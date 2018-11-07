const tape = require("tape");
const create = require("./helpers/create");
const EthCrypto = require("eth-crypto");
const { privateKey, publicKey: writerAddress } = EthCrypto.createIdentity();

tape("two keys with same siphash", (t) => {
	t.plan(1 + 2 + 2);

	const db = create.one();
	const schemaKey = "schema/*";
	const schemaValue = {
		keySchema: "*",
		valueValidationKey: "",
		keyValidation: ""
	};
	let writerSignature = EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue));
	db.addSchema(schemaKey, schemaValue, writerSignature, writerAddress, (err) => {
		t.error(err);

		db.put("idgcmnmna", "a", EthCrypto.sign(privateKey, db.createSignHash("idgcmnmna", "a")), writerAddress, { schemaKey }, () => {
			db.put("mpomeiehc", "b", EthCrypto.sign(privateKey, db.createSignHash("mpomeiehc", "b")), writerAddress, { schemaKey }, () => {
				db.get("idgcmnmna", (err, node) => {
					t.error(err, "no error");
					t.same(node.value, "a");
				});
				db.get("mpomeiehc", (err, node) => {
					t.error(err, "no error");
					t.same(node.value, "b");
				});
			});
		});
	});
});

tape("two keys with same siphash (iterator)", (t) => {
	const db = create.one();
	const schemaKey = "schema/*";
	const schemaValue = {
		keySchema: "*",
		valueValidationKey: "",
		keyValidation: ""
	};
	let writerSignature = EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue));
	db.addSchema(schemaKey, schemaValue, writerSignature, writerAddress, (err) => {
		t.error(err);

		db.put("idgcmnmna", "a", EthCrypto.sign(privateKey, db.createSignHash("idgcmnmna", "a")), writerAddress, { schemaKey }, () => {
			db.put("mpomeiehc", "b", EthCrypto.sign(privateKey, db.createSignHash("mpomeiehc", "b")), writerAddress, { schemaKey }, () => {
				const ite = db.iterator();

				ite.next((err, node) => {
					t.error(err, "no error");
					t.same(node.value, "a");
				});
				ite.next((err, node) => {
					t.error(err, "no error");
					t.same(node.value, "b");
				});
				ite.next((err, node) => {
					t.error(err, "no error");
					t.same(node.value, schemaValue);
				});
				ite.next((err, node) => {
					t.error(err, "no error");
					t.same(node, null);
					t.end();
				});
			});
		});
	});
});

tape("two prefixes with same siphash (iterator)", (t) => {
	const db = create.one();
	const schemaKey = "schema/*/*";
	const schemaValue = {
		keySchema: "*/*",
		valueValidationKey: "",
		keyValidation: ""
	};
	let writerSignature = EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue));
	db.addSchema(schemaKey, schemaValue, writerSignature, writerAddress, (err) => {
		t.error(err);

		db.put("idgcmnmna/a", "a", EthCrypto.sign(privateKey, db.createSignHash("idgcmnmna/a", "a")), writerAddress, { schemaKey }, () => {
			db.put(
				"mpomeiehc/b",
				"b",
				EthCrypto.sign(privateKey, db.createSignHash("mpomeiehc/b", "b")),
				writerAddress,
				{ schemaKey },
				() => {
					const ite = db.iterator("idgcmnmna");

					ite.next((err, node) => {
						t.error(err, "no error");
						t.same(node.value, "a");
					});
					ite.next((err, node) => {
						t.error(err, "no error");
						t.same(node, null);
						t.end();
					});
				}
			);
		});
	});
});
