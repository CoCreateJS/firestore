import {
    ObjectId,
    dotNotationToObject,
    searchData,
    isValidDate
} from "@cocreate/utils";

import { 
    dbClient, 
    createFilter, 
    createData,
    errorHandler 
} from "./utilities.js";

/**
 * "object" operations mapped to Firestore Documents.
 */
function object(method, data) {
    const storageName = data.storageName || data.provider || "firestore";
    return new Promise(
        async (resolve, reject) => {
            try {
                const db = await dbClient(data);
                if (!db || db.status === false) return resolve(data);

                let type = "object";
                let documents = [];

                if (!data["timeStamp"]) {
                    data["timeStamp"] = new Date().toISOString();
                }

                let databases = data.database;
                if (!Array.isArray(databases)) databases = [databases];

                for (let database of databases) {
                    let arrays = data.array;
                    if (!Array.isArray(arrays)) arrays = [arrays];

                    for (let array of arrays) {
                        const reference = {
                            $storage: storageName,
                            $database: database,
                            $array: array
                        };

                        if (!data[type]) data[type] = [];
                        else if (typeof data[type] === "string")
                            data[type] = [{ _id: data[type] }];
                        else if (!Array.isArray(data[type]))
                            data[type] = [data[type]];

                        let isFilter = !!data.$filter;
                        if ((isFilter && !data[type].length) || data.isFilter) {
                            data[type].splice(0, 0, { isFilter: "isEmptyObjectFilter" });
                        }

                        for (let i = 0; i < data[type].length; i++) {
                            let $storage = data[type][i].$storage || [];
                            let $database = data[type][i].$database || [];
                            let $array = data[type][i].$array || [];

                            if (!Array.isArray($storage)) $storage = [data[type][i].$storage];
                            if (!Array.isArray($database)) $database = [data[type][i].$database];
                            if (!Array.isArray($array)) $array = [data[type][i].$array];

                            if (!$storage.includes(storageName)) $storage.push(storageName);
                            if (!$database.includes(database)) $database.push(database);
                            if (!$array.includes(array)) $array.push(array);

                            delete data[type][i].$storage;
                            delete data[type][i].$database;
                            delete data[type][i].$array;

                            let queryFilter = isFilter ? data.$filter : (data[type][i].$filter || null);

                            if (method === "create") {
                                data[type][i] = replaceArray(data[type][i]);
                                data[type][i] = dotNotationToObject(data[type][i]);

                                const id = data[type][i]._id || `doc_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
                                const orgId = data.organization_id;
                                const created = {
                                    on: new Date(data.timeStamp).toISOString(),
                                    by: data.user_id || data.clientId
                                };

                                const recordPayload = { ...data[type][i] };
                                delete recordPayload._id;
                                delete recordPayload.organization_id;

                                const docRef = db.collection(array).doc(id);

                                process.emit("usage", {
                                    type: "egress",
                                    data: { api: "setDoc", path: `${array}/${id}`, payload: recordPayload },
                                    organization_id: data.organization_id
                                });

                                await docRef.set({
                                    _id: id,
                                    organization_id: orgId,
                                    created,
                                    modified: null,
                                    ...recordPayload
                                });

                                const savedDoc = await docRef.get();
                                if (savedDoc.exists) {
                                    data[type][i] = { ...savedDoc.data(), $storage, $database, $array };
                                }
                            }
                            else if (method === "read") {
                                let queryRef = db.collection(array);
                                queryRef = compileFilterToFirestore(queryRef, queryFilter, data.organization_id);

                                if (data.$filter && data.$filter.sort) {
                                    for (let s of data.$filter.sort) {
                                        let dir = s.direction === -1 || s.direction === "desc" ? "desc" : "asc";
                                        queryRef = queryRef.orderBy(s.key, dir);
                                    }
                                }
                                if (data.$filter && typeof data.$filter.limit === "number") {
                                    queryRef = queryRef.limit(data.$filter.limit);
                                }
                                if (data.$filter && typeof data.$filter.index === "number") {
                                    queryRef = queryRef.offset(data.$filter.index);
                                }

                                if (data[type][i]._id && !isFilter) {
                                    const docRef = db.collection(array).doc(data[type][i]._id);
                                    process.emit("usage", {
                                        type: "egress",
                                        data: { api: "getDoc", path: `${array}/${data[type][i]._id}` },
                                        organization_id: data.organization_id
                                    });
                                    const docSnap = await docRef.get();

                                    if (docSnap.exists) {
                                        let dbRecord = docSnap.data();

                                        // Offline synchronization check
                                        if ($storage.length && data[type][i].modified && data[type][i].modified.on) {
                                            let clientTime = new Date(data[type][i].modified.on);
                                            let dbTime = new Date(dbRecord.modified ? dbRecord.modified.on : 0);

                                            if (clientTime > dbTime) {
                                                const updatedRecord = { ...dbRecord, ...data[type][i] };
                                                const savedRecord = await executeUpdateTransaction(db, array, data[type][i]._id, updatedRecord, data);
                                                data[type][i] = { ...savedRecord, $storage, $database, $array };
                                            } else {
                                                data[type][i] = { ...data[type][i], ...dbRecord, $storage, $database, $array };
                                            }
                                        } else {
                                            data[type][i] = { ...data[type][i], ...dbRecord, $storage, $database, $array };
                                        }
                                    } else {
                                        data[type].splice(i, 1);
                                        i--;
                                    }
                                } else {
                                    process.emit("usage", {
                                        type: "egress",
                                        data: { api: "queryCollection", collection: array },
                                        organization_id: data.organization_id
                                    });
                                    const querySnapshot = await queryRef.get();
                                    
                                    for (let doc of querySnapshot.docs) {
                                        const parsedRow = doc.data();
                                        if (data.$filter && data.$filter.search) {
                                            let isMatch = searchData(parsedRow, data.$filter.search);
                                            if (!isMatch) continue;
                                        }
                                        documents.push({
                                            ...parsedRow,
                                            ...reference
                                        });
                                    }
                                }
                            }
                            else if (method === "update") {
                                const updatePayload = { ...data[type][i] };
                                delete updatePayload._id;
                                delete updatePayload.$filter;

                                if (data[type][i]._id) {
                                    const updatedDoc = await executeUpdateTransaction(db, array, data[type][i]._id, updatePayload, data);
                                    if (updatedDoc) {
                                        data[type][i] = { ...updatedDoc, $storage, $database, $array };
                                    }
                                } else if (isFilter) {
                                    let queryRef = db.collection(array);
                                    queryRef = compileFilterToFirestore(queryRef, queryFilter, data.organization_id);

                                    process.emit("usage", {
                                        type: "egress",
                                        data: { api: "queryForUpdate", collection: array },
                                        organization_id: data.organization_id
                                    });
                                    const querySnapshot = await queryRef.get();

                                    for (let doc of querySnapshot.docs) {
                                        const updatedDoc = await executeUpdateTransaction(db, array, doc.id, updatePayload, data);
                                        if (updatedDoc) {
                                            documents.push({ ...updatedDoc, ...reference });
                                        }
                                    }
                                }
                            }
                            else if (method === "delete") {
                                if (data[type][i]._id) {
                                    const docRef = db.collection(array).doc(data[type][i]._id);
                                    process.emit("usage", {
                                        type: "egress",
                                        data: { api: "deleteDoc", path: `${array}/${data[type][i]._id}` },
                                        organization_id: data.organization_id
                                    });
                                    await docRef.delete();
                                    data[type][i] = { _id: data[type][i]._id, $storage, $database, $array };
                                } else if (isFilter) {
                                    let queryRef = db.collection(array);
                                    queryRef = compileFilterToFirestore(queryRef, queryFilter, data.organization_id);

                                    process.emit("usage", {
                                        type: "egress",
                                        data: { api: "queryForDelete", collection: array },
                                        organization_id: data.organization_id
                                    });
                                    const querySnapshot = await queryRef.get();

                                    const batch = db.batch();
                                    for (let doc of querySnapshot.docs) {
                                        batch.delete(doc.ref);
                                        documents.push({ _id: doc.id, ...reference });
                                    }
                                    await batch.commit();
                                }
                            }
                        }
                    }
                }

                data = createData(data, documents, type);
                resolve(data);
            } catch (error) {
                errorHandler(data, error);
                console.log(method, "error", error);
                resolve(data);
            }
        },
        (error) => {
            errorHandler(data, error);
        }
    );
}

export default object