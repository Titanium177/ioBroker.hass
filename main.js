/* jshint -W097 */
/* jshint strict: false */
/* jslint node: true */

'use strict';

const utils       = require('@iobroker/adapter-core');
const HASS        = require('./lib/hass');
const adapterName = require('./package.json').name.split('.').pop();

let connected = false;
let hass;
let adapter;
const hassObjects = {};
let delayTimeout = null;
let stopped = false;
let syncDebounceTimeout = null;

function debouncedSync(callback) {
    if (syncDebounceTimeout) {
        clearTimeout(syncDebounceTimeout);
    }
    syncDebounceTimeout = setTimeout(() => {
        syncDebounceTimeout = null;
        hass.getStates((err, states) => {
            if (err) {
                adapter.log.error(`Cannot read states during resync: ${err}`);
                return;
            }
            hass.getServices((err, services) => {
                if (err) {
                    adapter.log.error(`Cannot read services during resync: ${err}`);
                    return;
                }
                parseStates(states, services, callback);
            });
        });
    }, 3000);
}

function startAdapter(options) {
    options = options || {};
    Object.assign(options, {name: adapterName, unload: stop});
    adapter = new utils.Adapter(options);

    // is called if a subscribed state changes
    adapter.on('stateChange', (id, state) => {
        // you can use the ack flag to detect if it is status (true) or command (false)
        if (state && !state.ack) {
            if (!connected) {
                return adapter.log.warn(`Cannot send command to "${id}", because not connected`);
            }
            /*if (id === adapter.namespace + '.' + '.info.resync') {
                queue.push({command: 'resync'});
                processQueue();
            } else */
            if (hassObjects[id]) {
                if (!hassObjects[id].common.write) {
                    adapter.log.warn(`Object ${id} is not writable!`);
                } else {
                    const serviceData = {};
                    const fields = hassObjects[id].native.fields;
                    const target = {};

                    let requestFields = {};
                    if (typeof state.val === 'string') {
                        state.val = state.val.trim();
                        if (state.val.startsWith('{') && state.val.endsWith('}')) {
                            try {
                                requestFields = JSON.parse(state.val) || {};
                            } catch (err) {
                                adapter.log.info(`Ignore data for service call ${id} is no valid JSON: ${err.message}`);
                                requestFields = {};
                            }
                        }
                    }

                    // If a non-JSON value was set, and we only have one relevant field, use this field as value
                    if (fields && Object.keys(requestFields).length === 0) {
                        const fieldList = Object.keys(fields);
                        if (fieldList.length === 1 && fieldList[0] !== 'entity_id') {
                            requestFields[fieldList[0]] = state.val;
                        } else if (fieldList.length === 2 && fields.entity_id) {
                            requestFields[fieldList[1 - fields.indexOf('entity_id')]] = state.val;
                        }
                    }

                    adapter.log.debug(`Prepare service call for ${id} with (mapped) request parameters ${JSON.stringify(requestFields)} from value: ${JSON.stringify(state.val)}`);
                    if (fields) {
                        for (const field in fields) {
                            if (!fields.hasOwnProperty(field)) {
                                continue;
                            }

                            if (field === 'entity_id') {
                                target.entity_id = hassObjects[id].native.entity_id
                            } else if (requestFields[field] !== undefined) {
                                serviceData[field] = requestFields[field];
                            }
                        }
                    }
                    const noFields = Object.keys(serviceData).length === 0;
                    serviceData.entity_id = hassObjects[id].native.entity_id

                    adapter.log.debug(`Send to HASS for service ${hassObjects[id].native.attr} with ${hassObjects[id].native.domain || hassObjects[id].native.type} and data ${JSON.stringify(serviceData)}`)
                    hass.callService(hassObjects[id].native.attr, hassObjects[id].native.domain || hassObjects[id].native.type, serviceData, target, err => {
                        err && adapter.log.error(`Cannot control ${id}: ${err}`);
                        if (err && fields && noFields) {
                            adapter.log.warn(`Please make sure to provide a stringified JSON as value to set relevant fields! Please refer to the Readme for details!`);
                            adapter.log.warn(`Allowed field keys are: ${Object.keys(fields).join(', ')}`);
                        }
                    });
                }
            }
        }
    });

    // is called when databases are connected and adapter received configuration.
    // start here!
    adapter.on('ready', main);

    return adapter;
}

function stop(callback) {
    stopped = true;
    delayTimeout && clearTimeout(delayTimeout);
    hass && hass.close();
    callback && callback();
}

function getUnit(name) {
    name = name.toLowerCase();
    if (name.indexOf('temperature') !== -1) {
        return '°C';
    } else if (name.indexOf('humidity') !== -1) {
        return '%';
    } else if (name.indexOf('pressure') !== -1) {
        return 'hPa';
    } else if (name.indexOf('degrees') !== -1) {
        return '°';
    } else if (name.indexOf('speed') !== -1) {
        return 'kmh';
    }
    return undefined;
}

function syncStates(states, cb) {
    if (!states || !states.length) {
        return cb();
    }
    const state = states.shift();
    const id = state.id;
    delete state.id;

    adapter.setForeignState(id, state, err => {
        err && adapter.log.error(err);
        setImmediate(syncStates, states, cb);
    });
}

function syncObjects(objects, stats, cb) {
    if (!objects || !objects.length) {
        return cb();
    }

    const groupedObjects = {};
    // Zähle zuerst alle neuen Objekte
    objects.forEach(obj => {
        const parts = obj._id.split('.');
        const entityIndex = parts.indexOf('entities') + 1;
        if (entityIndex > 0 && entityIndex + 1 < parts.length) {
            const entityId = parts[entityIndex] + '.' + parts[entityIndex + 1];
            const fullEntityPath = `${adapter.namespace}.entities.${entityId}`;
            if (!groupedObjects[fullEntityPath]) {
                groupedObjects[fullEntityPath] = {
                    new: [],
                    updated: []
                };
            }
            if (!hassObjects[obj._id]) {
                groupedObjects[fullEntityPath].new.push(obj);
                stats.new++; // Zähle neue Objekte direkt
            } else {
                groupedObjects[fullEntityPath].updated.push(obj);
            }
        }
    });

    const entityIds = Object.keys(groupedObjects);

    function processEntity() {
        if (!entityIds.length) {
            return cb();
        }
        const entityId = entityIds.shift();
        const entityObjects = groupedObjects[entityId];

        function processNext() {
            if (!entityObjects.new.length && !entityObjects.updated.length) {
                setImmediate(processEntity);
                return;
            }

            const obj = entityObjects.new.length ? entityObjects.new.shift() : entityObjects.updated.shift();
            adapter.getForeignObject(obj._id, (err, oldObj) => {
                err && adapter.log.error(err);

                if (!oldObj) {
                    adapter.log.debug(`Create "${obj._id}": ${JSON.stringify(obj.common)}`);
                    hassObjects[obj._id] = obj;
                    adapter.setForeignObject(obj._id, obj, err => {
                        err && adapter.log.error(err);
                        setImmediate(processNext);
                    });
                } else {
                    hassObjects[obj._id] = oldObj;
                    if (JSON.stringify(obj.native) !== JSON.stringify(oldObj.native)) {
                        oldObj.native = obj.native;
                        adapter.log.debug(`Update "${obj._id}": ${JSON.stringify(obj.common)}`);
                        adapter.setForeignObject(obj._id, oldObj, err => {
                            err && adapter.log.error(err);
                            stats.updated++; // Zähle Updates hier
                            setImmediate(processNext);
                        });
                    } else {
                        setImmediate(processNext);
                    }
                }
            });
        }
        processNext();
    }
    processEntity();
}

function syncRoom(room, members, cb) {
    adapter.getForeignObject(`enum.rooms.${room}`, (err, obj) => {
        if (!obj) {
            obj = {
                _id: `enum.rooms.${room}`,
                type: 'enum',
                common: {
                    name: room,
                    members: members
                },
                native: {}
            };
            adapter.log.debug(`Update "${obj._id}"`);
            adapter.setForeignObject(obj._id, obj, err => {
                err && adapter.log.error(err);
                cb();
            });
        } else {
            obj.common = obj.common || {};
            obj.common.members = obj.common.members || [];
            let changed = false;
            for (let m = 0; m < members.length; m++) {
                if (obj.common.members.indexOf(members[m]) === -1) {
                    changed = true;
                    obj.common.members.push(members[m]);
                }
            }
            if (changed) {
                adapter.log.debug(`Update "${obj._id}"`);
                adapter.setForeignObject(obj._id, obj, err => {
                    err && adapter.log.error(err);
                    cb();
                });
            } else {
                cb();
            }
        }
    });
}

const knownAttributes = {
    azimuth:   {write: false, read: true, unit: '°'},
    elevation: {write: false, read: true, unit: '°'}
};


const ERRORS = {
    1: 'ERR_CANNOT_CONNECT',
    2: 'ERR_INVALID_AUTH',
    3: 'ERR_CONNECTION_LOST'
};
const mapTypes = {
    'string': 'string',
    'number': 'number',
    'object': 'mixed',
    'boolean': 'boolean'
};
const skipServices = [
    'persistent_notification'
];

function parseStates(entities, services, callback) {
    const objs = [];
    const states = [];
    const newHassObjects = {};
    let obj;
    let channel;
    const expectedObjects = new Set();

    for (let e = 0; e < entities.length; e++) {
        const entity = entities[e];
        if (!entity) continue;

        const name = entity.name || (entity.attributes && entity.attributes.friendly_name ? entity.attributes.friendly_name : entity.entity_id);
        const desc = entity.attributes && entity.attributes.attribution ? entity.attributes.attribution : undefined;

        const channelId = `${adapter.namespace}.entities.${entity.entity_id}`;
        expectedObjects.add(channelId);
        
        channel = {
            _id: channelId,
            common: {
                name: name
            },
            type: 'channel',
            native: {
                object_id: entity.object_id,
                entity_id: entity.entity_id
            }
        };
        if (desc) channel.common.desc = desc;
        objs.push(channel);

        const lc = entity.last_changed ? new Date(entity.last_changed).getTime() : undefined;
        const ts = entity.last_updated ? new Date(entity.last_updated).getTime() : undefined;

        if (entity.state !== undefined) {
            const stateId = `${channelId}.state`;
            const boolStateId = `${channelId}.state_boolean`;
            expectedObjects.add(stateId);
            expectedObjects.add(boolStateId);

            obj = {
                _id: stateId,
                type: 'state',
                common: {
                    name: `${name} STATE`,
                    type: typeof entity.state,
                    read: true,
                    write: false
                },
                native: {
                    object_id: entity.object_id,
                    domain: entity.domain,
                    entity_id: entity.entity_id
                }
            };
            const booleanObj = {
                _id: boolStateId,
                type: 'state',
                common: {
                    name: `${name} state_BOOLEAN`,
                    type: 'boolean',
                    read: true,
                    write: false
                },
                native: {
                    object_id: entity.object_id,
                    domain: entity.domain,
                    entity_id: entity.entity_id
                }
            };
            objs.push(booleanObj);
            if (entity.attributes && entity.attributes.unit_of_measurement) {
                obj.common.unit = entity.attributes.unit_of_measurement;
            }
            objs.push(obj);

            let val = entity.state;
            if ((typeof val === 'object' && val !== null) || Array.isArray(val)) {
                val = JSON.stringify(val);
            }

            // Setze state und state_boolean
            states.push({id: obj._id, lc, ts, val, ack: true});
            let booleanState = null;
            if (entity.state === 'on') {
                booleanState = true;
            } else if (entity.state === 'off') {
                booleanState = false;
            }
            states.push({id: booleanObj._id, lc, ts, val: booleanState, ack: true});
        }

        if (entity.attributes) {
            for (const attr in entity.attributes) {
                if (!entity.attributes.hasOwnProperty(attr) || attr === 'friendly_name' || attr === 'unit_of_measurement' || attr === 'icon' || !attr.length) {
                    continue;
                }

                const attrId = attr.replace(adapter.FORBIDDEN_CHARS, '_').replace(/\.+$/, '_');
                const fullAttrId = `${channelId}.${attrId}`;
                expectedObjects.add(fullAttrId);

                let common;
                if (knownAttributes[attr]) {
                    common = Object.assign({}, knownAttributes[attr]);
                } else {
                    common = {};
                }

                obj = {
                    _id: fullAttrId,
                    type: 'state',
                    common: common,
                    native: {
                        object_id: entity.object_id,
                        domain: entity.domain,
                        entity_id: entity.entity_id,
                        attr: attr
                    }
                };
                if (!common.name) {
                    common.name = `${name} ${attr.replace(/_/g, ' ')}`;
                }
                if (common.read === undefined) {
                    common.read = true;
                }
                if (common.write === undefined) {
                    common.write = false;
                }
                if (common.type === undefined) {
                    common.type = mapTypes[typeof entity.attributes[attr]];
                }

                objs.push(obj);

                let val = entity.attributes[attr];
                if ((typeof val === 'object' && val !== null) || Array.isArray(val)) {
                    val = JSON.stringify(val);
                }

                states.push({id: obj._id, lc, ts, val, ack: true});
            }
        }

        const serviceType = entity.entity_id.split('.')[0];

        if (services[serviceType] && !skipServices.includes(serviceType)) {
            const service = services[serviceType];
            for (const s in service) {
                if (service.hasOwnProperty(s)) {
                    const serviceId = `${channelId}.${s}`;
                    expectedObjects.add(serviceId);
                    
                    obj = {
                        _id: serviceId,
                        type: 'state',
                        common: {
                            desc: service[s].description,
                            read: false,
                            write: true,
                            type: 'mixed'
                        },
                        native: {
                            object_id: entity.object_id,
                            domain: entity.domain,
                            fields: service[s].fields,
                            entity_id: entity.entity_id,
                            attr: s,
                            type: serviceType
                        }
                    };
                    objs.push(obj);
                }
            }
        }
    }

    const objectsToDelete = [];
    for (const id in hassObjects) {
        if (hassObjects.hasOwnProperty(id) && id.startsWith(`${adapter.namespace}.entities.`)) {
            if (!expectedObjects.has(id)) {
                objectsToDelete.push(id);
            }
        }
    }

    function deleteObjects(objects, cb) {
        if (!objects.length) {
            return cb();
        }

        const groupedObjects = {};
        objects.forEach(id => {
            const parts = id.split('.');
            const entityIndex = parts.indexOf('entities') + 1;
            if (entityIndex > 0 && entityIndex + 1 < parts.length) {
                const entityId = parts[entityIndex] + '.' + parts[entityIndex + 1];
                const fullEntityPath = `${adapter.namespace}.entities.${entityId}`;
                if (!groupedObjects[fullEntityPath]) {
                    groupedObjects[fullEntityPath] = [];
                }
                groupedObjects[fullEntityPath].push(id);
            }
        });

        const entityIds = Object.keys(groupedObjects);
        
        function deleteEntity() {
            if (!entityIds.length) {
                return cb();
            }
            const entityId = entityIds.shift();
            const objectsToDelete = groupedObjects[entityId];
            
            let deleted = 0;
            function deleteNext() {
                if (!objectsToDelete.length) {
                    if (deleted > 0) {
                        // Entferne die einzelnen Löschmeldungen
                        deleted++;
                    }
                    setImmediate(deleteEntity);
                    return;
                }
                const id = objectsToDelete.shift();
                adapter.delObject(id, err => {
                    if (err) {
                        adapter.log.error(`Error deleting object ${id}: ${err}`);
                    } else {
                        deleted++;
                        delete hassObjects[id];
                    }
                    setImmediate(deleteNext);
                });
            }
            deleteNext();
        }
        deleteEntity();
    }

    const stats = {
        new: 0,
        updated: 0,
        deleted: objectsToDelete.length
    };

    deleteObjects(objectsToDelete, () => {
        syncObjects(objs, stats, () => {
            // Gebe nur eine Zusammenfassung aus, wenn es Änderungen gab
            if (stats.new > 0 || stats.deleted > 0) {
                const changes = [];
                if (stats.new > 0) changes.push(`${stats.new} created`);
                if (stats.deleted > 0) changes.push(`${stats.deleted} deleted`);
                adapter.log.info(`Synchronization completed: ${changes.join(', ')}`);
            }
            syncStates(states, () => {
                callback && callback();
            });
        });
    });
}

function main() {
    adapter.config.host = adapter.config.host || '127.0.0.1';
    adapter.config.port = parseInt(adapter.config.port, 10) || 8123;

    adapter.setState('info.connection', false, true);

    hass = new HASS(adapter.config, adapter.log);

    hass.on('error', err =>
        adapter.log.error(err));

    hass.on('state_changed', entity => {
        adapter.log.debug(`HASS-Message: State Changed: ${JSON.stringify(entity)}`);
        if (!entity || typeof entity.entity_id !== 'string') {
            return;
        }

        const id = `entities.${entity.entity_id}.`;
        const lc = entity.last_changed ? new Date(entity.last_changed).getTime() : undefined;
        const ts = entity.last_updated ? new Date(entity.last_updated).getTime() : undefined;
        if (entity.state !== undefined) {
            // Map the state to a boolean value
            let booleanState = null;
            if (entity.state === 'on') {
                booleanState = true;
            } else if (entity.state === 'off') {
                booleanState = false;
            }
            
            if (hassObjects[`${adapter.namespace}.${id}state`]) {
                adapter.setState(`${id}state_boolean`, {val: booleanState, ack: true, lc: lc, ts: ts});
                adapter.setState(`${id}state`, {val: entity.state, ack: true, lc: lc, ts: ts});
            } else {
                adapter.log.info(`State changed for unknown object ${`${id}state`}. Triggering synchronization to resync the objects.`);
                debouncedSync();
            }
        }
        if (entity.attributes) {
            for (const attr in entity.attributes) {
                if (!entity.attributes.hasOwnProperty(attr) || attr === 'friendly_name' || attr === 'unit_of_measurement' || attr === 'icon'|| !attr.length) {
                    continue;
                }
                let val = entity.attributes[attr];
                if ((typeof val === 'object' && val !== null) || Array.isArray(val)) {
                    val = JSON.stringify(val);
                }
                const attrId = attr.replace(adapter.FORBIDDEN_CHARS, '_').replace(/\.+$/, '_');
                if (hassObjects[`${adapter.namespace}.${id}state`]) {
                    adapter.setState(id + attrId, {val, ack: true, lc, ts});
                } else {
                    adapter.log.info(`State changed for unknown object ${id + attrId}. Triggering synchronization to resync the objects.`);
                    debouncedSync();
                }
            }
        }
    });

    hass.on('connected', () => {
        if (!connected) {
            adapter.log.debug('Connected');
            connected = true;
            adapter.setState('info.connection', true, true);
            hass.getConfig((err, config) => {
                if (err) {
                    adapter.log.error(`Cannot read config: ${err}`);
                    return;
                }
                delayTimeout = setTimeout(() => {
                    delayTimeout = null;
                    !stopped && hass.getStates((err, states) => {
                        if (stopped) {
                            return;
                        }
                        if (err) {
                            return adapter.log.error(`Cannot read states: ${err}`);
                        }
                        delayTimeout = setTimeout(() => {
                            delayTimeout = null;
                            !stopped && hass.getServices((err, services) => {
                                if (stopped) {
                                    return;
                                }
                                if (err) {
                                    adapter.log.error(`Cannot read states: ${err}`);
                                } else {
                                    parseStates(states, services, () => {
                                        adapter.log.info('Initialization completed');
                                        adapter.subscribeStates('*');
                                    });
                                }
                            })}, 100);
                    })}, 100);
            });
        }
    });

    hass.on('disconnected', () => {
        if (connected) {
            adapter.log.debug('Disconnected');
            connected = false;
            adapter.setState('info.connection', false, true);
        }
    });

    hass.connect();
}

// If started as allInOne/compact mode => return function to create instance
if (module && module.parent) {
    module.exports = startAdapter;
} else {
    // or start the instance directly
    startAdapter();
}