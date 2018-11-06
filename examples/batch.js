const aodb = require("../.");
const EthCrypto = require("eth-crypto");

const { privateKey, publicKey: writerAddress } = EthCrypto.createIdentity();
const { privateKey: privateKey2, publicKey: writerAddress2 } = EthCrypto.createIdentity();
const { privateKey: privateKey3, publicKey: writerAddress3 } = EthCrypto.createIdentity();
const { privateKey: privateKey4, publicKey: writerAddress4 } = EthCrypto.createIdentity();

const db = aodb("./my.db", {
	valueEncoding: "json",
	reduce: (a, b) => a
});

/***** Add a Schema *****/
let schemaKey = "schema/content/*/review/%writerAddress%";
let schemaValue = {
	keySchema: "content/*/review/%writerAddress%",
	valueValidationKey: "",
	keyValidation: ""
};

let schemaKey2 = "schema/%writerAddress%/content/*/review";
let schemaValue2 = {
	keySchema: "%writerAddress%/content/*/review",
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

	let schemaKey3 = "schema/setting/%writerAddress%/*";
	let schemaValue3 = {
		keySchema: "setting/%writerAddress%/*",
		valueValidationKey: "",
		keyValidation: ""
	};

	const batchList2 = [
		{
			type: "put",
			key: "content/0x123456789/review/" + writerAddress3,
			value: "Nice video",
			writerSignature: EthCrypto.sign(privateKey3, db.createSignHash("content/0x123456789/review/" + writerAddress3, "Nice video")),
			writerAddress: writerAddress3,
			schemaKey
		},
		{
			type: "put",
			key: "content/0x123456789/review/" + writerAddress4,
			value: "Please upload more video like this!",
			writerSignature: EthCrypto.sign(
				privateKey4,
				db.createSignHash("content/0x123456789/review/" + writerAddress4, "Please upload more video like this!")
			),
			writerAddress: writerAddress4,
			schemaKey,
			pointerKey: writerAddress4 + "/content/0x123456789/review",
			pointerSchemaKey: schemaKey2
		},
		{
			type: "add-schema",
			key: schemaKey3,
			value: schemaValue3,
			writerSignature: EthCrypto.sign(privateKey, db.createSignHash(schemaKey3, schemaValue3)),
			writerAddress: writerAddress
		}
	];

	db.batch(batchList2, (err) => {
		if (err) throw err;
		db.get(batchList2[0].key, (err, node) => {
			if (err) throw err;
			console.log("Batch Insert:\n" + batchList2[0].key + " --> " + node.value + "\n");
		});
		db.get(batchList2[1].key, (err, node) => {
			if (err) throw err;
			console.log("Batch Insert:\n" + batchList2[1].key + " --> " + node.value + "\n");
			console.log("pointerKey --> " + node.pointerKey + "\n");
			db.get(batchList2[1].pointerKey, (err, node) => {
				if (err) throw err;
				console.log("\tPointer Key:\n" + batchList2[1].pointerKey + " --> " + node.value + "\n");
				console.log("\tpointer --> " + node.pointer + "\n");
			});
		});
		db.get(batchList2[2].key, (err, node) => {
			if (err) throw err;
			console.log("Batch Insert:\n" + batchList2[2].key + " --> " + JSON.stringify(node.value) + "\n");
		});
	});
});

return;
db.addSchema(schemaKey, schemaValue, writerSignature, writerAddress, (err) => {
	if (err) throw err;
	db.get(schemaKey, (err, node) => {
		if (err) throw err;
		console.log("Add Schema:\n" + schemaKey + " --> " + JSON.stringify(node.value) + "\n");
		return;

		/***** Batch Insert *****/
		let schemaKey2 = "schema/setting/%writerAddress%/*";
		let schemaValue2 = {
			keySchema: "setting/%writerAddress%/*",
			valueValidationKey: "",
			keyValidation: ""
		};

		const batchList = [
			{
				type: "put",
				key: "content/0x123456789/review/" + writerAddress3,
				value: "Nice video",
				writerSignature: EthCrypto.sign(
					privateKey3,
					db.createSignHash("content/0x123456789/review/" + writerAddress3, "Nice video")
				),
				writerAddress: writerAddress3,
				schemaKey: contentSchemaKey
			},
			{
				type: "put",
				key: "content/0x123456789/review/" + writerAddress4,
				value: "Please upload more video like this!",
				writerSignature: EthCrypto.sign(
					privateKey4,
					db.createSignHash("content/0x123456789/review/" + writerAddress4, "Please upload more video like this!")
				),
				writerAddress: writerAddress4,
				schemaKey: contentSchemaKey
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
				console.log("Batch Insert:\n" + batchList[0].key + " --> " + node.value + "\n");
			});
			db.get(batchList[1].key, (err, node) => {
				if (err) throw err;
				console.log("Batch Insert:\n" + batchList[1].key + " --> " + node.value + "\n");
			});
			db.get(batchList[2].key, (err, node) => {
				if (err) throw err;
				console.log("Batch Insert:\n" + batchList[2].key + " --> " + JSON.stringify(node.value) + "\n");
			});
		});
	});
});
