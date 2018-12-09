'use strict';

// Setting the logs
const log4js = require('log4js');
const logger = log4js.getLogger('[lib:influxdb]');
logger.level = process.env.LOG_LEVEL || 'INFO';


const Influx = require('influx');

const propertyMap = {};

class InfluxDB {

    /**
     * @param {String} host         Host of InfluxDB server
     * @param {String} database     Name of the database
     */
    constructor(host, database) {
        this.influx = new Influx.InfluxDB({
            host: host,
            database: database,
            schema: [
                {
                    measurement: 'thing',
                    fields: {
                        id: Influx.FieldType.STRING,
                        name: Influx.FieldType.STRING,
                        description: Influx.FieldType.STRING,
                        type: Influx.FieldType.STRING
                    },
                    tags: [
                        'user'
                    ]
                },
                {
                    measurement: 'property',
                    fields: {
                        id: Influx.FieldType.STRING,
                        name: Influx.FieldType.STRING,
                        description: Influx.FieldType.STRING,
                        type: Influx.FieldType.STRING
                    },
                    tags: [
                        'user'
                    ]
                }
            ]
        });
    }

    /**
     * @param {Thing[]} things
     */
    writeThings(things) {
        return this.influx.writePoints(thingsToPoints(things));
    }

    /**
     * @param {String} key
     * @param {Property[]} properties
     */
    writeProperties(key, properties) {
        properties.forEach((property) => {
            propertyMap[key] = property;
        });
        logger.debug(propertyMap);
        // return this.influx.writePoints(propertiesToPoints(properties));
    }

    /**
     * @param {String} key
     * @param {Array[]} values
     */
    writeValues(key, values) {
        return this.influx.writePoints(valuesToPoints(key, values));
    }
}

/**
 * @param {Thing[]} things
 * @returns {Array}
 */
function thingsToPoints(things) {
    const points = [];
    things.forEach((thing) => {
        const point = {
            measurement: 'thing',
            tags: {},
            fields: {
                id: thing.id,
                name: thing.name,
                description: thing.description,
                type: thing.type
            }
        };
        logger.debug(point);
        points.push(point);
    });
    return points;
}

// /**
//  * @param {Property[]} properties
//  * @returns {Array}
//  */
// function propertiesToPoints(properties) {
//     const points = [];
//     properties.forEach( (property) => {
//         const point = {
//             measurement: 'property',
//             tags: {},
//             fields: {
//                 id: property.id,
//                 name: property.name,
//                 description: property.description,
//                 type: property.type
//             }
//         };
//         logger.debug(point);
//         points.push(point);
//     });
//     return points;
// }

/**
 * @param {String} id
 * @param {Array} values
 * @returns {Array}
 */
function valuesToPoints(id, values) {
    logger.debug('++++ value to points, property map: '
        + JSON.stringify(propertyMap));
    logger.debug('++++ value to points, looking for: ' + id);
    const points = [];
    logger.debug(values);
    if (propertyMap[id] !== undefined) {
        const dimensions = propertyMap[id].dimensions;
        logger.debug('property defined in property map'
            + values.length + ' ' + dimensions.length);
        let ts;
        if (values.length - 1 === dimensions.length ||
            values.length === dimensions.length) {
            if (values.length === dimensions.length) {
                // missing time, take from server
                ts = Date.now();
            } else {
                ts = values[0];
            }
            logger.debug('dimensions: ' + JSON.stringify(dimensions));

            const fields = {};
            for (let i = 1; i < values.length; i++) {
                const name = dimensions[i - 1].name;
                fields[name] = values[i];
            }

            const point = {
                measurement: id,
                tags: {},
                fields: fields,
                time: ts
            };
            logger.debug(point);
            points.push(point);
        }
    }
    return points;
}

module.exports = InfluxDB;
