const aodb = require("../.");
const EthCrypto = require("eth-crypto");

const { privateKey, publicKey: writerAddress } = EthCrypto.createIdentity();
const { privateKey: privateKey2, publicKey: writerAddress2 } = EthCrypto.createIdentity();

const db = new aodb("./my.db", {
	valueEncoding: "json",
	reduce: (a, b) => a
});

/***** Add a Schema *****/
let schemaKey = "schema/%writerAddress%/profilePicture";
let schemaValue = {
	keySchema: "%writerAddress%/profilePicture",
	valueValidationKey: "",
	keyValidation: ""
};
let schemaKey2 = "schema/settings/profilePicture/%writerAddress%";
let schemaValue2 = {
	keySchema: "settings/profilePicture/%writerAddress%",
	valueValidationKey: "",
	keyValidation: ""
};

const batchList = [
	{
		type: "add-schema",
		key: schemaKey,
		value: schemaValue,
		writerSignature: EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue)),
		writerAddress: writerAddress
	},
	{
		type: "add-schema",
		key: schemaKey2,
		value: schemaValue2,
		writerSignature: EthCrypto.sign(privateKey, db.createSignHash(schemaKey2, schemaValue2)),
		writerAddress: writerAddress
	}
];

db.batch(batchList, (err) => {
	if (err) throw err;
	db.get(batchList[0].key, (err, node) => {
		if (err) throw err;
		console.log("Batch Insert:\n" + batchList[0].key + " --> " + JSON.stringify(node.value) + "\n");
	});
	db.get(batchList[1].key, (err, node) => {
		if (err) throw err;
		console.log("Batch Insert:\n" + batchList[1].key + " --> " + JSON.stringify(node.value) + "\n");
	});

	/***** Put *****/
	let key = writerAddress2 + "/profilePicture";
	let value = "https://upload.wikimedia.org/wikipedia/en/thumb/e/e0/Iron_Man_bleeding_edge.jpg/250px-Iron_Man_bleeding_edge.jpg";
	let writerSignature2 = EthCrypto.sign(privateKey2, db.createSignHash(key, value));
	let pointerKey = "settings/profilePicture/" + writerAddress2;
	let opts = {
		schemaKey,
		pointerKey,
		pointerSchemaKey: schemaKey2
	};
	db.put(key, value, writerSignature2, writerAddress2, opts, (err, node) => {
		if (err) throw err;
		db.get(key, (err, node) => {
			if (err) throw err;
			console.log("Put:\n" + key + " --> " + node.value + "\n");
			console.log("pointerKey --> " + node.pointerKey + "\n");
		});
		db.get(pointerKey, (err, node) => {
			if (err) throw err;
			console.log("Put Pointer Key:\n" + pointerKey + " --> " + node.value + "\n");
			console.log("pointer --> " + node.pointer + "\n");
		});
	});
});
