import {
    ObjectId,
    dotNotationToObject,
    searchData,
    isValidDate
} from "@cocreate/utils";

import { 
    dbClient, 
    createData,
    errorHandler 
} from "./utilities.js";

/**
 * "array" operations mapped to Firestore Collections.
 * @private
 * @environment server
 */
function array(method, data) {
    const storageName = data.storageConfig?.name;
    return new Promise(
        async (resolve, reject) => {
            let type = "array";
            let arrayArray = [];

            try {
                const db = await dbClient(data);
                if (!db || db.status === false) return resolve(data);

                if (data.request) data.array = data.request;

                let databases = data.database;
                if (!Array.isArray(databases)) databases = [databases];

                let databasesLength = databases.length;
                for (let database of databases) {
                    if (method === "read") {
                        process.emit("usage", {
                            type: "egress",
                            data: { api: "listCollections" },
                            organization_id: data.organization_id
                        });

                        const collections = await db.listCollections();
                        for (let col of collections) {
                            const tableObj = { name: col.id };

                            if (data.filter && data.filter.query) {
                                let isFilter = queryData(tableObj, data.filter.query);
                                if (isFilter) {
                                    arrayArray.push({
                                        name: col.id,
                                        database,
                                        storage: storageName
                                    });
                                }
                            } else {
                                arrayArray.push({
                                    name: col.id,
                                    database,
                                    storage: storageName
                                });
                            }
                        }

                        databasesLength -= 1;
                        if (!databasesLength) {
                            data = createData(data, arrayArray, type);
                            resolve(data);
                        }
                    } else {
                        let arrays = data.array;
                        if (method === "update") arrays = Object.entries(data.array);
                        if (!Array.isArray(arrays)) arrays = [arrays];

                        let arraysLength = arrays.length;
                        for (let array of arrays) {
                            if (method === "create") {
                                // Firestore creates collections implicitly. To guarantee visibility in list collections,
                                // we write a placeholder metadata document that is safely overwritten by standard CRUD later.
                                const placeholderDocRef = db.collection(array).doc("_placeholder_");
                                
                                process.emit("usage", {
                                    type: "egress",
                                    data: { api: "createCollection", collection: array },
                                    organization_id: data.organization_id
                                });

                                await placeholderDocRef.set({ isPlaceholder: true, organization_id: data.organization_id });

                                arrayArray.push({
                                    name: array,
                                    database,
                                    storage: storageName
                                });

                                arraysLength -= 1;
                                if (!arraysLength) databasesLength -= 1;
                                if (!databasesLength && !arraysLength) {
                                    data = createData(data, arrayArray, type);
                                    resolve(data);
                                }
                            } else {
                                if (method === "update") {
                                    let [oldName, newName] = array;
                                    process.emit("usage", {
                                        type: "egress",
                                        data: { api: "renameCollection", from: oldName, to: newName },
                                        organization_id: data.organization_id
                                    });

                                    // Firestore doesn't support atomic collection renames natively.
                                    // We migrate documents recursively in batches to fulfill expectations seamlessly.
                                    const oldCollectionRef = db.collection(oldName);
                                    const newCollectionRef = db.collection(newName);
                                    const snapshot = await oldCollectionRef.get();

                                    const batch = db.batch();
                                    snapshot.forEach((doc) => {
                                        batch.set(newCollectionRef.doc(doc.id), doc.data());
                                        batch.delete(doc.ref);
                                    });
                                    await batch.commit();

                                    arrayArray.push({
                                        name: newName,
                                        oldName: oldName,
                                        database,
                                        storage: storageName
                                    });

                                    arraysLength -= 1;
                                    if (!arraysLength) databasesLength -= 1;
                                    if (!databasesLength && !arraysLength) {
                                        data = createData(data, arrayArray, type);
                                        resolve(data);
                                    }
                                }

                                if (method === "delete") {
                                    process.emit("usage", {
                                        type: "egress",
                                        data: { api: "deleteCollection", collection: array },
                                        organization_id: data.organization_id
                                    });

                                    const collectionRef = db.collection(array);
                                    await db.recursiveDelete(collectionRef);

                                    arrayArray.push({
                                        name: array,
                                        database,
                                        storage: storageName
                                    });

                                    arraysLength -= 1;
                                    if (!arraysLength) databasesLength -= 1;
                                    if (!databasesLength && !arraysLength) {
                                        data = createData(data, arrayArray, type);
                                        resolve(data);
                                    }
                                }
                            }
                        }
                    }
                }
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

export default array