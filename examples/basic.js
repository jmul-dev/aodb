const aodb = require("../.");
const EthCrypto = require("eth-crypto");

const { privateKey, publicKey: writerAddress } = EthCrypto.createIdentity();

const db = new aodb("./my.db", {
	valueEncoding: "json",
	reduce: (a, b) => a
});

/***** HyperDb example *****/
let schemaKey = "schema/hello";
let schemaValue = {
	keySchema: "hello",
	valueValidationKey: "",
	keyValidation: ""
};
let writerSignature = EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue));

db.addSchema(schemaKey, schemaValue, writerSignature, writerAddress, (err) => {
	if (err) throw err;
	db.get(schemaKey, (err, node) => {
		if (err) throw err;
		console.log("Add Schema:\n" + schemaKey + " --> " + JSON.stringify(node.value) + "\n");

		/***** Put *****/
		let key = "hello";
		let value = "world";
		writerSignature = EthCrypto.sign(privateKey, db.createSignHash(key, value));

		db.put(key, value, writerSignature, writerAddress, { schemaKey }, (err) => {
			if (err) throw err;
			db.get(key, (err, node) => {
				if (err) throw err;
				console.log("Put:\n" + key + " --> " + node.value + "\n");
			});
		});
	});
});
