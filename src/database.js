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
 * "database" operations mapped to Firestore Databases.
 */
function database(method, data) {
    const storageName = data.storageConfig?.name;
    return new Promise(
        async (resolve, reject) => {
            let type = "database";
            let databaseArray = [];

            try {
                const db = await dbClient(data);
                if (!db || db.status === false) return resolve(data);

                if (method === "read") {
                    // Firestore is serverless and generally operates on a single default schema database per context.
                    // We construct catalog names dynamically matching the target structure.
                    const dbName = db.databaseId || data.database || "(default)";
                    const dbObj = { name: dbName };

                    process.emit("usage", {
                        type: "egress",
                        data: { api: "listDatabases", context: dbName },
                        organization_id: data.organization_id
                    });

                    if (data.$filter && data.$filter.query) {
                        let isFilter = queryData(dbObj, data.$filter.query);
                        if (isFilter) {
                            databaseArray.push({
                                database: dbObj,
                                storage: storageName
                            });
                        }
                    } else {
                        databaseArray.push({
                            database: dbObj,
                            storage: storageName
                        });
                    }
                    resolve(createData(data, databaseArray, type));
                }
                if (method === "delete") {
                    // Wiping an entire Firestore instance is emulated cleanly by deleting all collections recursively.
                    const collections = await db.listCollections();
                    for (let col of collections) {
                        process.emit("usage", {
                            type: "egress",
                            data: { api: "recursiveDelete", collection: col.id },
                            organization_id: data.organization_id
                        });
                        await db.recursiveDelete(col);
                    }
                    resolve({ status: true });
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

export default database