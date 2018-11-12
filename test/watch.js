const tape = require("tape");
const create = require("./helpers/create");
const replicate = require("./helpers/replicate");
const run = require("./helpers/run");
const EthCrypto = require("eth-crypto");
const { privateKey, publicKey: writerAddress } = EthCrypto.createIdentity();

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

const schemaKey3 = "schema/*/*/*";
const schemaValue3 = {
	keySchema: "*/*/*",
	valueValidationKey: "",
	keyValidation: ""
};

tape("basic watch", (t) => {
	const db = create.one();

	db.addSchema(schemaKey, schemaValue, EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue)), writerAddress, (err) => {
		t.error(err, "no error");

		db.watch(() => {
			t.pass("watch triggered");
			t.end();
		});

		db.put("hello", "world", EthCrypto.sign(privateKey, db.createSignHash("hello", "world")), writerAddress, { schemaKey });
	});
});

tape("watch prefix", (t) => {
	const db = create.one();
	let changed = false;
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

		db.watch("foo", () => {
			t.ok(changed);
			t.end();
		});

		db.put("hello", "world", EthCrypto.sign(privateKey, db.createSignHash("hello", "world")), writerAddress, { schemaKey }, (err) => {
			t.error(err, "you are here");
			setImmediate(() => {
				changed = true;
				db.put("foo/bar", "baz", EthCrypto.sign(privateKey, db.createSignHash("foo/bar", "baz")), writerAddress, {
					schemaKey: schemaKey2
				});
			});
		});
	});
});

tape("recursive watch", (t) => {
	t.plan(21);

	let i = 0;
	const db = create.one();

	db.addSchema(schemaKey, schemaValue, EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue)), writerAddress, (err) => {
		t.error(err, "no error");
		db.watch("foo", () => {
			if (i === 20) return;
			t.pass("watch triggered");
			++i;
			db.put("foo", "bar-" + i, EthCrypto.sign(privateKey, db.createSignHash("foo", "bar-" + i)), writerAddress, { schemaKey });
		});

		db.put("foo", "bar", EthCrypto.sign(privateKey, db.createSignHash("foo", "bar")), writerAddress, { schemaKey });
	});
});

tape("watch and stop watching", (t) => {
	const db = create.one();
	const batchList = [
		{
			type: "add-schema",
			key: schemaKey2,
			value: schemaValue2,
			writerSignature: EthCrypto.sign(privateKey, db.createSignHash(schemaKey2, schemaValue2)),
			writerAddress: writerAddress
		},
		{
			type: "add-schema",
			key: schemaKey3,
			value: schemaValue3,
			writerSignature: EthCrypto.sign(privateKey, db.createSignHash(schemaKey3, schemaValue3)),
			writerAddress: writerAddress
		}
	];
	db.batch(batchList, (err) => {
		t.error(err, "no error");

		let once = true;

		const w = db.watch("foo", () => {
			t.ok(once);
			once = false;
			w.destroy();
			db.put(
				"foo/bar/baz",
				"qux",
				EthCrypto.sign(privateKey, db.createSignHash("foo/bar/baz", "qux")),
				writerAddress,
				{ schemaKey: schemaKey3 },
				() => {
					t.end();
				}
			);
		});

		db.put("foo/bar", "baz", EthCrypto.sign(privateKey, db.createSignHash("foo/bar", "baz")), writerAddress, { schemaKey: schemaKey2 });
	});
});

tape("remote watch", (t) => {
	const db = create.one();

	db.ready(() => {
		db.addSchema(
			schemaKey,
			schemaValue,
			EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue)),
			writerAddress,
			(err) => {
				t.error(err, "no error");

				const clone = create.one(db.key);

				for (let i = 0; i < 100; i++)
					db.put(
						"hello-" + i,
						"world-" + i,
						EthCrypto.sign(privateKey, db.createSignHash("hello-" + i, "world-" + i)),
						writerAddress,
						{ schemaKey }
					);

				db.put(
					"flush",
					"flush",
					EthCrypto.sign(privateKey, db.createSignHash("flush", "flush")),
					writerAddress,
					{ schemaKey },
					() => {
						clone.watch(() => {
							t.pass("remote watch triggered");
							t.end();
						});

						replicate(db, clone);
					}
				);
			}
		);
	});
});

tape("watch with 3rd-party authorize", (t) => {
	create.two((a, b) => {
		t.plan(4); // once per writer updated in the namespace (b.auth and c.put) and .error

		a.addSchema(schemaKey, schemaValue, EthCrypto.sign(privateKey, a.createSignHash(schemaKey, schemaValue)), writerAddress, (err) => {
			t.error(err, "no error");

			a.watch(() => {
				t.pass("watch called");
			});

			const c = create.one(a.key);
			c.ready(() => {
				const done = (err) => {
					t.error(err, "no error");
				};

				run(
					(cb) => replicate(a, b, cb),
					(cb) => replicate(b, c, cb),
					(cb) => b.authorize(c.local.key, cb),
					(cb) => replicate(b, c, cb),
					(cb) =>
						c.put(
							"hello2",
							"world2",
							EthCrypto.sign(privateKey, c.createSignHash("hello2", "world2")),
							writerAddress,
							{ schemaKey },
							cb
						),
					(cb) => replicate(b, c, cb),
					(cb) => replicate(a, b, cb),
					done
				);
			});
		});
	});
});
