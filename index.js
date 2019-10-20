#!/usr/bin/env node

// Load environment variables
require("dotenv").config();

// Setting the logs
const log4js = require("log4js");
const logger = log4js.getLogger("[index]");
logger.level = process.env.LOG_LEVEL || "INFO";

const DCDError = require("dcd-model/lib/Error");
const Property = require("dcd-model/entities/Property");
const Thing = require("dcd-model/entities/Thing");

const InfluxDb = require("dcd-model/dao/influxdb");
const influxHost = process.env.INFLUXDB_HOST || "influxdb";
const influxDatabase = process.env.INFLUXDB_NAME || "dcdhub";
const influxdb = new InfluxDb(influxHost, influxDatabase);

const host = process.env.KAFKA_HOST || "localhost";
const port =
  process.env.KAFKA_PORT !== undefined
    ? parseInt(process.env.KAFKA_PORT)
    : 9092;

const kafka = require("kafka-node");
const Consumer = kafka.Consumer;
const Offset = kafka.Offset;
const KafkaClient = kafka.KafkaClient;

const topics = [
  {
    topic: "persons",
    partition: 0
  },
  {
    topic: "things",
    partition: 0
  },
  {
    topic: "properties",
    partition: 0
  },
  {
    topic: "values",
    partition: 0
  }
];

connect(
  topics,
  onMessage
);

/**
 * Connect Kafka consumer
 * @param topics
 * @param onMessage
 */
function connect(topics, onMessage) {
  logger.debug("Connecting to kafka...");
  const client = new KafkaClient({
    kafkaHost: host + ":" + port
  });

  const options = {
    groupId: "kafka-node-influx",
    autoCommit: false,
    fetchMaxWaitMs: 1000,
    fetchMaxBytes: 1024 * 1024
  };

  const consumer = new Consumer(client, topics, options);
  const offset = new Offset(client);

  consumer.on("message", onMessage);

  consumer.on("error", error => {
    logger.error("error", error);
  });

  /*
   * If consumer get `offsetOutOfRange` event,
   * fetch data from the smallest(oldest) offset
   */
  consumer.on("offsetOutOfRange", topic => {
    topic.maxNum = 2;
    offset.fetch([topic], (error, offsets) => {
      if (error) {
        return logger.error(error);
      }
      const min = Math.min.apply(null, offsets[topic.topic][topic.partition]);
      consumer.setOffset(topic.topic, topic.partition, min);
    });
  });
}

/**
 * Handle Kafka messages
 * @param {Message} message
 */
function onMessage(message) {
  logger.debug(message);
  switch (message.topic) {
    case "things":
      try {
        if (message.value !== null) {
          const thing = new Thing(JSON.parse(message.value.toString()));
          influxdb.createThings([thing]);
        }
      } catch (e) {
        return logger.error(
          new DCDError(500, `Could not parse thing: ${message.value}`)
        );
      }
      break;
    case "properties":
      try {
        if (message.value !== null) {
          const property = new Property(JSON.parse(message.value.toString()));
          influxdb.createProperties(property.entityId, [property]);
        }
      } catch (e) {
        return logger.error(
          new DCDError(500, `Could not parse property: ${message.value}`)
        );
      }

      break;
    case "values":
      try {
        const property = new Property(JSON.parse(message.value.toString()));
        influxdb.createValues(property);
      } catch (e) {
        return logger.error(
          new DCDError(500, `Could not parse value: ${message.value}`)
        );
      }
      break;
    default:
  }
}
