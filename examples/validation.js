const aodb = require("../.");
const EthCrypto = require("eth-crypto");

const { privateKey, publicKey: writerAddress } = EthCrypto.createIdentity();
const { privateKey: privateKey2, publicKey: writerAddress2 } = EthCrypto.createIdentity();

const db = aodb("./my.db", {
	valueEncoding: "json",
	reduce: (a, b) => a
});

// Example of schema with validation
let validationSchemaKey = "schema/validate/type/*";
let validationSchemaValue = {
	keySchema: "validate/type/*",
	valueValidationKey: "",
	keyValidation: ""
};
let validationWriterSignature = EthCrypto.sign(privateKey, db.createSignHash(validationSchemaKey, validationSchemaValue));
db.addSchema(validationSchemaKey, validationSchemaValue, validationWriterSignature, writerAddress, (err) => {
	if (err) throw err;
	db.get(validationSchemaKey, (err, node) => {
		if (err) throw err;
		console.log("Add Schema:\n" + validationSchemaKey + " --> " + JSON.stringify(node.value) + "\n");

		let key = "validate/type/maxLength140";
		let value = "validateMaxLength140";
		let writerSignature = EthCrypto.sign(privateKey, db.createSignHash(key, value));

		db.put(key, value, writerSignature, writerAddress, { schemaKey: validationSchemaKey }, (err) => {
			if (err) throw err;
			db.get(key, (err, node) => {
				if (err) throw err;
				console.log("Put:\n" + key + " --> " + node.value + "\n");

				let schemaKey = "schema/profile/%writerAddress%/description";
				let schemaValue = {
					keySchema: "profile/%writerAddress%/description",
					valueValidationKey: "validate/type/maxLength140",
					keyValidation: ""
				};
				writerSignature = EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue));

				db.addSchema(schemaKey, schemaValue, writerSignature, writerAddress, (err) => {
					if (err) throw err;
					db.get(schemaKey, (err, node) => {
						if (err) throw err;
						console.log("Add Schema:\n" + schemaKey + " --> " + JSON.stringify(node.value) + "\n");

						/***** Success Put *****/
						let key2 = "profile/" + writerAddress2 + "/description";
						let value2 = "Stephen Strange, M.D., PhD is a selfish doctor who only cares about wealth from his career.";
						let writerSignature2 = EthCrypto.sign(privateKey2, db.createSignHash(key2, value2));

						db.put(key2, value2, writerSignature2, writerAddress2, { schemaKey }, (err) => {
							if (err) throw err;
							db.get(key2, (err, node) => {
								if (err) throw err;
								console.log("Put:\n" + key + " --> " + node.value + "\n");
							});
						});

						// Failed Put
						/*
						key = 'profile/' + writerAddress2 +'/description';
						value = 'A wealthy American business magnate, playboy, and ingenious scientist, Anthony Edward "Tony" Stark suffers a severe chest injury during a kidnapping. When his captors attempt to force him to build a weapon of mass destruction, he instead creates a powered suit of armor to save his life and escape captivity. Later, Stark develops his suit, adding weapons and other technological devices he designed through his company, Stark Industries. He uses the suit and successive versions to protect the world as Iron Man. Although at first concealing his true identity, Stark eventually declared that he was, in fact, Iron Man in a public announcement.';
						let writerSignature2 = EthCrypto.sign(privateKey2, db.createSignHash(key, value));

						db.put(key, value, writerSignature2, writerAddress2, { schemaKey }, (err) => {
							if (err) throw err
							db.get(key, (err, node) => {
								if (err) throw err
								console.log('Put:\n' + key + ' --> ' + node.value + '\n')
							})
						})
						*/
					});
				});
			});
		});
	});
});
