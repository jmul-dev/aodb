const tape = require("tape");
const create = require("./helpers/create");
const run = require("./helpers/run");
const EthCrypto = require("eth-crypto");
const { privateKey, publicKey: writerAddress } = EthCrypto.createIdentity();

tape("basic delete", (t) => {
	const db = create.one();
	const schemaKey = "schema/*";
	const schemaValue = {
		keySchema: "*",
		valueValidationKey: "",
		keyValidation: ""
	};
	db.addSchema(schemaKey, schemaValue, EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue)), writerAddress, (err) => {
		t.error(err, "no error");

		db.put("hello", "world", EthCrypto.sign(privateKey, db.createSignHash("hello", "world")), writerAddress, { schemaKey }, () => {
			db.get("hello", (err, node) => {
				t.error(err, "no error");
				t.same(node.value, "world");
				db.del("hello", EthCrypto.sign(privateKey, db.createSignHash("hello", "")), writerAddress, (err) => {
					t.error(err, "no error");
					db.get("hello", (err, node) => {
						t.error(err, "no error");
						t.ok(!node, "was deleted");
						t.end();
					});
				});
			});
		});
	});
});

tape("delete one in many", (t) => {
	t.plan(1 + 1 + 2 + 2);

	const db = create.one();
	const schemaKey = "schema/*";
	const schemaValue = {
		keySchema: "*",
		valueValidationKey: "",
		keyValidation: ""
	};
	db.addSchema(schemaKey, schemaValue, EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue)), writerAddress, (err) => {
		t.error(err, "no error");

		const keys = [];

		for (let i = 0; i < 50; i++) {
			keys.push("" + i);
		}

		const done = (err) => {
			t.error(err, "no error");
			db.get("42", (err, node) => {
				t.error(err, "no error");
				t.ok(!node, "was deleted");
			});
			db.get("43", (err, node) => {
				t.error(err, "no error");
				t.same(node.value, "43");
			});
		};

		run(
			keys.map((k) => (cb) => db.put(k, k, EthCrypto.sign(privateKey, db.createSignHash(k, k)), writerAddress, { schemaKey }, cb)),
			(cb) => db.del("42", EthCrypto.sign(privateKey, db.createSignHash("42", "")), writerAddress, cb),
			done
		);
	});
});

tape("delete one in many (iteration)", (t) => {
	const db = create.one();
	const schemaKey = "schema/*";
	const schemaValue = {
		keySchema: "*",
		valueValidationKey: "",
		keyValidation: ""
	};
	db.addSchema(schemaKey, schemaValue, EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue)), writerAddress, (err) => {
		t.error(err, "no error");

		const keys = [];

		for (let i = 0; i < 50; i++) {
			keys.push("" + i);
		}

		const done = (err) => {
			t.error(err, "no error");

			const ite = db.iterator();
			const actual = [];

			ite.next(function loop(err, node) {
				if (err) return t.error(err, "no error");

				if (!node) {
					const expected = keys.slice(0, 42).concat(keys.slice(43));
					t.same(actual.sort(), expected.sort(), "all except deleted one");
					t.end();
					return;
				}

				if (!node.isSchema) actual.push(node.value);
				ite.next(loop);
			});
		};

		run(
			keys.map((k) => (cb) => db.put(k, k, EthCrypto.sign(privateKey, db.createSignHash(k, k)), writerAddress, { schemaKey }, cb)),
			(cb) => db.del("42", EthCrypto.sign(privateKey, db.createSignHash("42", "")), writerAddress, cb),
			done
		);
	});
});

tape("delete marks node as deleted", (t) => {
	const db = create.one();
	const schemaKey = "schema/*";
	const schemaValue = {
		keySchema: "*",
		valueValidationKey: "",
		keyValidation: ""
	};
	db.addSchema(schemaKey, schemaValue, EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue)), writerAddress, (err) => {
		t.error(err, "no error");

		const expected = [{ key: "hello", value: "world", deleted: false }, { key: "hello", value: "", deleted: true }];

		db.put("hello", "world", EthCrypto.sign(privateKey, db.createSignHash("hello", "world")), writerAddress, { schemaKey }, () => {
			db.del("hello", EthCrypto.sign(privateKey, db.createSignHash("hello", "")), writerAddress, (err) => {
				db.createHistoryStream()
					.on("data", (data) => {
						if (!data.isSchema) t.same({ key: data.key, value: data.value, deleted: data.deleted }, expected.shift());
					})
					.on("end", () => {
						t.same(expected.length, 0);
						t.end();
					});
			});
		});
	});
});
