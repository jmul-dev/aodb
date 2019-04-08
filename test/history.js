const tape = require("tape");
const collect = require("stream-collector");
const create = require("./helpers/create");
const replicate = require("./helpers/replicate");
const EthCrypto = require("eth-crypto");
const { privateKey, publicKey: writerAddress } = EthCrypto.createIdentity();

tape("empty history", (t) => {
	const db = create.one();
	const expected = [];

	const rs = db.createHistoryStream();
	collect(rs, (err, actual) => {
		t.error(err, "no error");
		t.deepEqual(actual, expected, "diff as expected");
		t.end();
	});
});

tape("single value", (t) => {
	const db = create.one();
	const schemaKey = "schema/*";
	const schemaValue = {
		keySchema: "*",
		valueValidationKey: "",
		keyValidation: ""
	};
	db.addSchema(schemaKey, schemaValue, EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue)), writerAddress, (err) => {
		t.error(err, "no error");

		db.put("a", "2", EthCrypto.sign(privateKey, db.createSignHash("a", "2")), writerAddress, { schemaKey }, (err) => {
			t.error(err, "no error");
			const rs = db.createHistoryStream();
			collect(rs, (err, actual) => {
				t.error(err, "no error");
				t.equals(actual.length, 3);
				t.equals(actual[2].key, "a");
				t.equals(actual[2].value, "2");
				t.end();
			});
		});
	});
});

tape("multiple values", (t) => {
	const db = create.one();
	const schemaKey = "schema/*";
	const schemaValue = {
		keySchema: "*",
		valueValidationKey: "",
		keyValidation: ""
	};

	const schemaKey2 = "schema/*/*";
	const schemaValue2 = {
		keySchema: "*/*",
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
		t.error(err, "no error");
		db.put("a", "2", EthCrypto.sign(privateKey, db.createSignHash("a", "2")), writerAddress, { schemaKey }, (err) => {
			t.error(err, "no error");
			db.put(
				"b/0",
				"boop",
				EthCrypto.sign(privateKey, db.createSignHash("b/0", "boop")),
				writerAddress,
				{ schemaKey: schemaKey2 },
				(err) => {
					t.error(err, "no error");
					const rs = db.createHistoryStream();
					collect(rs, (err, actual) => {
						t.error(err, "no error");
						t.equals(actual.length,6);
						t.equals(actual[4].key, "a");
						t.equals(actual[4].value, "2");
						t.equals(actual[5].key, "b/0");
						t.equals(actual[5].value, "boop");
						t.end();
					});
				}
			);
		});
	});
});

tape("multiple values: same key", (t) => {
	const db = create.one();
	const schemaKey = "schema/*";
	const schemaValue = {
		keySchema: "*",
		valueValidationKey: "",
		keyValidation: ""
	};
	db.addSchema(schemaKey, schemaValue, EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue)), writerAddress, (err) => {
		t.error(err, "no error");
		db.put("a", "2", EthCrypto.sign(privateKey, db.createSignHash("a", "2")), writerAddress, { schemaKey }, (err) => {
			t.error(err, "no error");
			db.put("a", "boop", EthCrypto.sign(privateKey, db.createSignHash("a", "boop")), writerAddress, { schemaKey }, (err) => {
				t.error(err, "no error");
				const rs = db.createHistoryStream();
				collect(rs, (err, actual) => {
					t.error(err, "no error");
					t.equals(actual.length, 4);
					t.equals(actual[2].key, "a");
					t.equals(actual[2].value, "2");
					t.equals(actual[3].key, "a");
					t.equals(actual[3].value, "boop");
					t.end();
				});
			});
		});
	});
});

tape("2 feeds", (t) => {
	create.two((a, b) => {
		const schemaKey = "schema/*";
		const schemaValue = {
			keySchema: "*",
			valueValidationKey: "",
			keyValidation: ""
		};

		const validate = () => {
			const rs = b.createHistoryStream();
			const bi = b.feeds.indexOf(b.local);
			const ai = bi === 0 ? 1 : 0;

			collect(rs, (err, actual) => {
				t.error(err, "no error");
				t.equals(actual.length, 7);
				t.equals(actual[0].feed, ai);
				t.equals(actual[0].seq, 1);
				t.equals(actual[3].feed, ai);
				t.equals(actual[3].seq, 4);
				t.equals(actual[6].feed, bi);
				t.equals(actual[6].seq, 3);
				t.end();
			});
		};

		a.addSchema(schemaKey, schemaValue, EthCrypto.sign(privateKey, a.createSignHash(schemaKey, schemaValue)), writerAddress, (err) => {
			t.error(err, "no error");
			b.addSchema(
				schemaKey,
				schemaValue,
				EthCrypto.sign(privateKey, b.createSignHash(schemaKey, schemaValue)),
				writerAddress,
				(err) => {
					t.error(err, "no error");
					a.put("a", "a", EthCrypto.sign(privateKey, a.createSignHash("a", "a")), writerAddress, { schemaKey }, (err) => {
						b.put("b", "12", EthCrypto.sign(privateKey, b.createSignHash("b", "12")), writerAddress, { schemaKey }, (err) => {
							replicate(a, b, validate);
						});
					});
				}
			);
		});
	});
});

tape("reverse", (t) => {
	create.two((a, b) => {
		const schemaKey = "schema/*";
		const schemaValue = {
			keySchema: "*",
			valueValidationKey: "",
			keyValidation: ""
		};

		const validate = () => {
			const rs = b.createHistoryStream({ reverse: true });
			const bi = b.feeds.indexOf(b.local);
			const ai = bi === 0 ? 1 : 0;

			collect(rs, (err, actual) => {
				t.error(err, "no error");
				t.equals(actual.length, 7);
				t.equals(actual[0].feed, bi);
				t.equals(actual[0].seq, 3);
				t.equals(actual[3].feed, ai);
				t.equals(actual[3].seq, 4);
				t.equals(actual[6].feed, ai);
				t.equals(actual[6].seq, 1);
				t.end();
			});
		};

		a.addSchema(schemaKey, schemaValue, EthCrypto.sign(privateKey, a.createSignHash(schemaKey, schemaValue)), writerAddress, (err) => {
			t.error(err, "no error");
			b.addSchema(
				schemaKey,
				schemaValue,
				EthCrypto.sign(privateKey, b.createSignHash(schemaKey, schemaValue)),
				writerAddress,
				(err) => {
					t.error(err, "no error");
					a.put("a", "a", EthCrypto.sign(privateKey, a.createSignHash("a", "a")), writerAddress, { schemaKey }, (err) => {
						b.put("b", "12", EthCrypto.sign(privateKey, b.createSignHash("b", "12")), writerAddress, { schemaKey }, (err) => {
							replicate(a, b, validate);
						});
					});
				}
			);
		});
	});
});
