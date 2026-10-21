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
 * Accepts storage API credentials or a bootstrap URL.
 */
async function dbClient(data) {
	const storage = data.storageConfig;
	const storageUrl = storage?.projectId && storage?.clientEmail && storage?.privateKey
		? {
			projectId: storage.projectId,
			clientEmail: storage.clientEmail,
			privateKey: storage.privateKey
		}
		: storage?.url;

	if (storageUrl) {
		const connectionKey = typeof storageUrl === "object"
			? JSON.stringify(storageUrl)
			: storageUrl;
		if (!organizations[data.organization_id]) {
			organizations[data.organization_id] = {};
		}
		try {
			if (!organizations[data.organization_id][connectionKey]) {
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

				organizations[data.organization_id][connectionKey] = connectionPromise;
			}

			// Await connection resolution safely
			return await organizations[data.organization_id][connectionKey];
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

			const updated = {
				on: new Date(requestData.timeStamp).toISOString(),
				by: requestData.user_id || requestData.clientId
			};

			const updatedPayload = applyMongoUpdate(currentRecord, rawUpdateInput);

			const savePayload = { ...updatedPayload };
			delete savePayload._id;
			delete savePayload.organization_id;
			delete savePayload.created;
			delete savePayload.updated;
			delete savePayload.modified;

			const recordToWrite = {
				_id: docId,
				organization_id: requestData.organization_id,
				created: currentRecord.created || updated,
				updated,
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

export {
    dbClient,
    createData,
    errorHandler
};
