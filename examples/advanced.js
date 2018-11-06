const aodb = require("../.");
const EthCrypto = require("eth-crypto");

const { privateKey, publicKey: writerAddress } = EthCrypto.createIdentity();
const { privateKey: privateKey2, publicKey: writerAddress2 } = EthCrypto.createIdentity();

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
let writerSignature = EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue));

db.addSchema(schemaKey, schemaValue, writerSignature, writerAddress, (err) => {
	if (err) throw err;
	db.get(schemaKey, (err, node) => {
		if (err) throw err;
		console.log("Add Schema:\n" + schemaKey + " --> " + JSON.stringify(node.value) + "\n");

		/***** Put *****/
		let key = "content/0x123456789/review/" + writerAddress2;
		let value = "Love the content";
		let writerSignature2 = EthCrypto.sign(privateKey2, db.createSignHash(key, value));

		db.put(key, value, writerSignature2, writerAddress2, { schemaKey }, (err) => {
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
	});
});
