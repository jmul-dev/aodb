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
let contentSchemaKey = "schema/content/*/review/%writerAddress%";
let contentSchemaValue = {
	keySchema: "content/*/review/%writerAddress%",
	valueValidationKey: "",
	keyValidation: ""
};
let contentWriterSignature = EthCrypto.sign(privateKey, db.createSignHash(contentSchemaKey, contentSchemaValue));

db.addSchema(contentSchemaKey, contentSchemaValue, contentWriterSignature, writerAddress, (err) => {
	if (err) throw err;
	db.get(contentSchemaKey, (err, node) => {
		if (err) throw err;
		console.log("Add Schema:\n" + contentSchemaKey + " --> " + JSON.stringify(node.value) + "\n");

		/***** Put *****/
		let key = "content/0x123456789/review/" + writerAddress2;
		let value = "Love the content";
		let writerSignature2 = EthCrypto.sign(privateKey2, db.createSignHash(key, value));

		db.put(key, value, writerSignature2, writerAddress2, { schemaKey: contentSchemaKey }, (err) => {
			if (err) throw err;
			db.get(key, (err, node) => {
				if (err) throw err;
				console.log("Put:\n" + key + " --> " + node.value + "\n");

				/***** Delete *****/
				// Create writerSignature of empty value
				writerSignature2 = EthCrypto.sign(privateKey2, db.createSignHash(key, ""));
				db.del(key, writerSignature2, writerAddress2, (err) => {
					if (err) throw err;
					db.get(key, (err, node) => {
						if (err) throw err;
						console.log("Delete:\n" + key + " --> " + node + "\n");
					});
				});
			});
		});

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
