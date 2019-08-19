#!/usr/bin/env node

// Load environment variables
require("dotenv").config();

// Setting the logs
const log4js = require('log4js');
const logger = log4js.getLogger('[index]');
logger.level = process.env.LOG_LEVEL || 'INFO';

const host = process.env.INFLUXDB_HOST || 'influxdb';
const database = process.env.INFLUXDB_NAME || 'dcdhub';
const InfluxDb = require('./lib/influxdb');
const influxdb = new InfluxDb(host, database);

const kafka = require('./lib/kafka');

const topics = [
    {
        topic: 'persons',
        partition: 0
    },
    {
        topic: 'things',
        partition: 0
    },
    {
        topic: 'properties',
        partition: 0
    },
    {
        topic: 'values',
        partition: 0
    }
];

kafka.connect(topics, onMessage);
/**
 * Handle Kafka messages
 * @param {Message} message
 */
function onMessage(message) {
    logger.debug(message);
    let json;
    switch (message.topic) {
        case 'things':
            try {
                json = JSON.parse(message.value.toString());
            } catch (e) {
                return logger.error('Could not parse thing: ' + message.value);
            }

            if (message.value !== null) {
                const things = [json];
                influxdb.writeThings(things);
            }
            break;
        case 'properties':
            try {
                json = JSON.parse(message.value.toString());
            } catch (e) {
                return logger.error('Could not parse property: '
                    + message.value);
            }

            if (message.value !== null) {
                const properties = [json];
                influxdb.writeProperties(message.key, properties);
            }
            break;
        case 'values':
            try {
                json = JSON.parse(message.value.toString());
            } catch (e) {
                return logger.error('Could not parse value: ' + message.value);
            }

            if (message.value !== null) {
                influxdb.writeValues(message.key, json);
            }
            break;
        default:

    }

}