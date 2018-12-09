'use strict';

// Setting the logs
const log4js = require('log4js');
const logger = log4js.getLogger('[lib:kafka]');
logger.level = process.env.LOG_LEVEL || 'INFO';

const host = process.env.KAFKA_HOST || 'localhost';
const port = process.env.KAFKA_PORT !== undefined
    ? parseInt(process.env.KAFKA_PORT) : 9092;

const kafka = require('kafka-node');
const Consumer = kafka.Consumer;
const Offset = kafka.Offset;
const KafkaClient = kafka.KafkaClient;

/**
 * Connect to Kafka server
 **/
exports.connect = (topics, onMessage) => {
    logger.debug('Connecting to kafka...');
    const client = new KafkaClient({
        kafkaHost: host + ':' + port
    });

    const options = {
        groupId: 'kafka-node-influx',
        autoCommit: false,
        fetchMaxWaitMs: 1000,
        fetchMaxBytes: 1024 * 1024
    };

    const consumer = new Consumer(client, topics, options);
    const offset = new Offset(client);

    consumer.on('message', onMessage);

    consumer.on('error', (error) => {
        logger.error('error', error);
    });

    /*
    * If consumer get `offsetOutOfRange` event,
    * fetch data from the smallest(oldest) offset
    */
    consumer.on('offsetOutOfRange', (topic) => {
        topic.maxNum = 2;
        offset.fetch([topic], (error, offsets) => {
            if (error) {
                return logger.error(error);
            }
            const min = Math.min.apply(null,
                offsets[topic.topic][topic.partition]);
            consumer.setOffset(topic.topic, topic.partition, min);
        });
    });

};