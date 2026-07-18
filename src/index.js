/********************************************************************************
 * Copyright (C) 2026 CoCreate and Contributors.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 ********************************************************************************/

import { Firestore } from "@google-cloud/firestore";
import {
	dotNotationToObject,
	queryData,
	searchData,
	sortData,
	isValidDate
} from "@cocreate/utils";

const organizations = {};

/**
 * Dynamically resolves or caches a long-lived Firestore database client connection.
 * Caches the connection promise to prevent duplicate authentication handshakes.
 * Normalizes custom credential protocols directly out of standard CoCreate storageUrls or urls.
 */
async function dbClient(data) {
	const storageUrl = data.storageUrl || data.url;

	if (storageUrl) {
		if (!organizations[data.organization_id]) {
			organizations[data.organization_id] = {};
		}
		try {
			if (!organizations[data.organization_id][storageUrl]) {
				const connectionPromise = (async () => {
					let config = {};

					// Option A: The Structured Credentials Object (Highly recommended)
					if (typeof storageUrl === "object" && storageUrl !== null) {
						const projectId = storageUrl.projectId || storageUrl.project_id;
						const clientEmail = storageUrl.clientEmail || storageUrl.client_email;
						const privateKey = storageUrl.privateKey || storageUrl.private_key;

						config = {
							projectId,
							credentials: {
								client_email: clientEmail,
								private_key: privateKey ? privateKey.replace(/\\n/g, "\n") : undefined
							}
						};
					}
					// Option B: The Credentials Path URL (e.g. firestore:///path/to/credentials.json)
					else if (typeof storageUrl === "string" && storageUrl.startsWith("firestore:///")) {
						const filePath = storageUrl.replace("firestore://", "");
						config = { keyFilename: filePath };
					}
					// Option C: Query Params Encoded URI (e.g. firestore://project-id?client_email=...&private_key=...)
					else if (typeof storageUrl === "string" && storageUrl.startsWith("firestore://")) {
						const urlParsed = new URL(storageUrl);
						const projectId = urlParsed.host || urlParsed.hostname;
						const clientEmail = urlParsed.searchParams.get("client_email");
						const privateKey = urlParsed.searchParams.get("private_key");

						config = {
							projectId,
							credentials: {
								client_email: clientEmail,
								private_key: privateKey ? privateKey.replace(/\\n/g, "\n") : undefined
							}
						};
					}

					// Safely map optional custom Firestore Database ID configurations
					if (data.database && data.database !== "(default)") {
						config.databaseId = data.database;
					}

					const client = new Firestore(config);
					
					// Run a dummy check to verify connection viability early
					await client.listCollections();
					return client;
				})();

				organizations[data.organization_id][storageUrl] = connectionPromise;
			}

			// Await connection resolution safely
			return await organizations[data.organization_id][storageUrl];
		} catch (error) {
			console.error(
				`${data.organization_id}: Firestore failed to connect`
			);
			errorHandler(data, error);
			return { status: false };
		}
	}

	errorHandler(data, "missing Firestore Connection URL/Credentials");
	return;
}

/**
 * Global Event Listener: orgDeleted
 * Gracefully terminates active Firestore client instances and frees up system memory.
 */
process.on("orgDeleted", (organization_id) => {
	const orgPools = organizations[organization_id];
	if (orgPools) {
		console.log(`[Firestore Utilities] Cleanup Scheduled: Initiating 5-second grace period for Org: ${organization_id}`);
		setTimeout(async () => {
			const currentPools = organizations[organization_id];
			if (!currentPools) return;

			console.log(`[Firestore Utilities] Cleanup Executing: Closing connection pools for Org: ${organization_id}`);
			const clientPromises = Object.values(currentPools);

			for (const clientPromise of clientPromises) {
				try {
					const client = await clientPromise;
					if (client && typeof client.terminate === "function") {
						await client.terminate();
					}
				} catch (err) {
					console.error(`[Firestore Utilities] Error closing Firestore client for Org ${organization_id}:`, err);
				}
			}

			delete organizations[organization_id];
			console.log(`[Firestore Utilities] Memory Cleared: Registry freed for Org: ${organization_id}`);
		}, 5000);
	}
});

/**
 * Universal router matching the standard CoCreate pipeline signature.
 */
export function send(data) {
	let [type, method] = data.method.split(".");
	if (type === "database") return database(method, data);
	if (type === "array") return array(method, data);
	if (type === "object") return object(method, data);
}

/**
 * "database" operations mapped to Firestore Databases.
 */
function database(method, data) {
	const storageName = data.storageName || data.provider || "firestore";
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

/**
 * "array" operations mapped to Firestore Collections.
 */
function array(method, data) {
	const storageName = data.storageName || data.provider || "firestore";
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

							if (data.$filter && data.$filter.query) {
								let isFilter = queryData(tableObj, data.$filter.query);
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

/**
 * Executes a state-safe transactional update pipeline matching MongoDB syntax operations inside Cloud Firestore.
 * Natively scales over concurrent race conditions utilizing Google Cloud's transactional isolation mechanics.
 */
async function executeUpdateTransaction(db, arrayName, docId, rawUpdateInput, requestData) {
	const docRef = db.collection(arrayName).doc(docId);
	try {
		return await db.runTransaction(async (transaction) => {
			const sfDoc = await transaction.get(docRef);
			let currentRecord = sfDoc.exists ? sfDoc.data() : null;

			if (!currentRecord) {
				if (requestData.upsert || rawUpdateInput.upsert || rawUpdateInput.$upsert) {
					currentRecord = { _id: docId, organization_id: requestData.organization_id };
				} else {
					return null;
				}
			}

			const modified = {
				on: new Date(requestData.timeStamp).toISOString(),
				by: requestData.user_id || requestData.clientId
			};

			const updatedPayload = applyMongoUpdate(currentRecord, rawUpdateInput);

			const savePayload = { ...updatedPayload };
			delete savePayload._id;
			delete savePayload.organization_id;
			delete savePayload.created;
			delete savePayload.modified;

			const recordToWrite = {
				_id: docId,
				organization_id: requestData.organization_id,
				created: currentRecord.created || modified,
				modified,
				...savePayload
			};

			process.emit("usage", {
				type: "egress",
				data: { api: "transactionalSetDoc", path: `${arrayName}/${docId}` },
				organization_id: requestData.organization_id
			});

			transaction.set(docRef, recordToWrite);
			return recordToWrite;
		});
	} catch (error) {
		throw error;
	}
}

/**
 * Applies deep MongoDB modification operators ($set, $unset, $push, $pull, $addToSet, $inc)
 * recursively on a target record to maintain absolute system synchronization behaviors.
 */
function applyMongoUpdate(record, updatePayload) {
	let target = { ...record };

	Object.keys(updatePayload).forEach((rawKey) => {
		if (rawKey.startsWith("$")) return;
		let cleanKey = rawKey.replace(/\[(\d+)\]/g, ".$1");
		setNestedValue(target, cleanKey, updatePayload[rawKey]);
	});

	if (updatePayload.$set) {
		Object.keys(updatePayload.$set).forEach((key) => {
			let cleanKey = key.replace(/\[(\d+)\]/g, ".$1");
			setNestedValue(target, cleanKey, updatePayload.$set[key]);
		});
	}

	if (updatePayload.$unset) {
		Object.keys(updatePayload.$unset).forEach((key) => {
			let cleanKey = key.replace(/\[(\d+)\]/g, ".$1");
			deleteNestedValue(target, cleanKey);
		});
	}

	if (updatePayload.$inc) {
		Object.keys(updatePayload.$inc).forEach((key) => {
			let cleanKey = key.replace(/\[(\d+)\]/g, ".$1");
			let currentVal = getNestedValue(target, cleanKey) || 0;
			setNestedValue(target, cleanKey, Number(currentVal) + Number(updatePayload.$inc[key]));
		});
	}

	if (updatePayload.$push) {
		Object.keys(updatePayload.$push).forEach((key) => {
			let cleanKey = key.replace(/\[(\d+)\]/g, ".$1");
			let arr = getNestedValue(target, cleanKey);
			if (!Array.isArray(arr)) arr = [];

			const pushValue = updatePayload.$push[key];
			if (pushValue && pushValue.$each) {
				let valuesToPush = pushValue.$each;
				let position = typeof pushValue.$position === "number" ? pushValue.$position : arr.length;
				arr.splice(position, 0, ...valuesToPush);
			} else {
				arr.push(pushValue);
			}
			setNestedValue(target, cleanKey, arr);
		});
	}

	if (updatePayload.$pull) {
		Object.keys(updatePayload.$pull).forEach((key) => {
			let cleanKey = key.replace(/\[(\d+)\]/g, ".$1");
			let arr = getNestedValue(target, cleanKey);
			if (Array.isArray(arr)) {
				const filterVal = updatePayload.$pull[key];
				arr = arr.filter(item => {
					if (typeof filterVal === "object" && filterVal !== null) {
						return !queryData(item, filterVal);
					}
					return item !== filterVal;
				});
				setNestedValue(target, cleanKey, arr);
			}
		});
	}

	if (updatePayload.$addToSet) {
		Object.keys(updatePayload.$addToSet).forEach((key) => {
			let cleanKey = key.replace(/\[(\d+)\]/g, ".$1");
			let arr = getNestedValue(target, cleanKey);
			if (!Array.isArray(arr)) arr = [];

			const addValue = updatePayload.$addToSet[key];
			if (addValue && addValue.$each) {
				addValue.$each.forEach(item => {
					if (!arr.includes(item)) arr.push(item);
				});
			} else {
				if (!arr.includes(addValue)) arr.push(addValue);
			}
			setNestedValue(target, cleanKey, arr);
		});
	}

	return target;
}

/**
 * Normalizes and compiles complex MongoDB logic filters cleanly into a chained Firestore Query reference structure.
 */
function compileFilterToFirestore(queryRef, filter, organizationId) {
	// Restrict and enforce organization scope limits uniformly
	let resolvedQuery = queryRef.where("organization_id", "==", organizationId);

	if (filter && filter.query) {
		for (let key in filter.query) {
			if (["$or", "$and", "organization_id"].includes(key)) continue;

			let value = filter.query[key];

			if (value && typeof value === "object" && !Array.isArray(value)) {
				for (let op in value) {
					let operand = value[op];
					if (op === "$gt") resolvedQuery = resolvedQuery.where(key, ">", operand);
					else if (op === "$gte") resolvedQuery = resolvedQuery.where(key, ">=", operand);
					else if (op === "$lt") resolvedQuery = resolvedQuery.where(key, "<", operand);
					else if (op === "$lte") resolvedQuery = resolvedQuery.where(key, "<=", operand);
					else if (op === "$ne") resolvedQuery = resolvedQuery.where(key, "!=", operand);
					else if (op === "$in") resolvedQuery = resolvedQuery.where(key, "in", operand);
				}
			} else {
				resolvedQuery = resolvedQuery.where(key, "==", value);
			}
		}
	}

	return resolvedQuery;
}

function replaceArray(data = {}) {
	let object = {};
	Object.keys(data).forEach((key) => {
		object[key.replace(/\[(\d+)\]/g, ".$1")] = data[key];
	});
	return object;
}

function getNestedValue(obj, path) {
	return path.split(".").reduce((acc, part) => acc && acc[part], obj);
}

function setNestedValue(obj, path, value) {
	const parts = path.split(".");
	const last = parts.pop();
	const target = parts.reduce((acc, part) => acc[part] = acc[part] || {}, obj);
	target[last] = value;
}

function deleteNestedValue(obj, path) {
	const parts = path.split(".");
	const last = parts.pop();
	const target = parts.reduce((acc, part) => acc && acc[part], obj);
	if (target) delete target[last];
}

/**
 * Syncs active state blocks and updates values cleanly inside CoCreate payload objects.
 */
function createData(data, array, type) {
	if (data[type] && data[type][0] && data[type][0].isFilter === "isEmptyObjectFilter") {
		data[type].shift();
		data.isFilter = true;
	}

	let key = type !== "object" ? "name" : "_id";

	if (!Array.isArray(data[type])) {
		console.log("data[type] is not an array", type);
	} else {
		for (let i = 0; i < array.length; i++) {
			const matchIndex = data[type].findIndex((item) => item[key] === array[i][key]);
			if (matchIndex !== -1) {
				for (let $type of ["$storage", "$database", "$array"]) {
					if (!data[type][matchIndex][$type]) data[type][matchIndex][$type] = [];
					if (!Array.isArray(data[type][matchIndex][$type])) {
						data[type][matchIndex][$type] = [data[type][matchIndex][$type]];
					}
					if (!data[type][matchIndex][$type].includes(array[i][$type])) {
						data[type][matchIndex][$type].push(array[i][$type]);
					}
					delete array[i][$type];
				}
				data[type][matchIndex] = { ...data[type][matchIndex], ...array[i] };
			} else {
				data[type].push(array[i]);
			}
		}
	}
	return data;
}

function errorHandler(data, error, database, array) {
	let errorMessage = typeof error === "object" && error.message ? error.message : error;
	let errorObject = {
		message: errorMessage,
		storage: "firestore"
	};

	if (database) errorObject.database = database;
	if (array) errorObject.array = array;

	if (Array.isArray(data.error)) {
		data.error.push(errorObject);
	} else {
		data.error = [errorObject];
	}
}

export default { send };