const tape = require("tape");
const create = require("./helpers/create");
const replicate = require("./helpers/replicate");
const run = require("./helpers/run");
const EthCrypto = require("eth-crypto");
const { privateKey: privateKeyA, publicKey: writerAddressA } = EthCrypto.createIdentity();
const { privateKey: privateKeyB, publicKey: writerAddressB } = EthCrypto.createIdentity();

tape('authorized writer passes "authorized" api', (t) => {
	create.two((a, b) => {
		const schemaKey = "schema/foo";
		const schemaValue = {
			keySchema: "foo",
			valueValidationKey: "",
			keyValidation: ""
		};
		let writerSignatureA = EthCrypto.sign(privateKeyA, a.createSignHash(schemaKey, schemaValue));
		a.addSchema(schemaKey, schemaValue, writerSignatureA, writerAddressA, (err) => {
			t.error(err);

			const key = "foo";
			const value = "bar";
			writerSignatureA = EthCrypto.sign(privateKeyA, a.createSignHash(key, value));
			a.put(key, value, writerSignatureA, writerAddressA, { schemaKey }, (err) => {
				t.error(err);
				a.authorized(a.local.key, (err, auth) => {
					t.error(err);
					t.equals(auth, true);
					b.authorized(b.local.key, (err, auth) => {
						t.error(err);
						t.equals(auth, true);
						t.end();
					});
				});
			});
		});
	});
});

tape('authorized writer passes "authorized" api', (t) => {
	create.two((a, b) => {
		const schemaKey = "schema/foo";
		const schemaValue = {
			keySchema: "foo",
			valueValidationKey: "",
			keyValidation: ""
		};
		let writerSignatureB = EthCrypto.sign(privateKeyB, b.createSignHash(schemaKey, schemaValue));
		b.addSchema(schemaKey, schemaValue, writerSignatureB, writerAddressB, (err) => {
			t.error(err);

			const key = "foo";
			const value = "bar";
			writerSignatureB = EthCrypto.sign(privateKeyB, b.createSignHash(key, value));
			b.put(key, value, writerSignatureB, writerAddressB, { schemaKey }, (err) => {
				t.error(err);
				a.authorized(a.local.key, (err, auth) => {
					t.error(err);
					t.equals(auth, true);
					b.authorized(b.local.key, (err, auth) => {
						t.error(err);
						t.equals(auth, true);
						t.end();
					});
				});
			});
		});
	});
});

tape('unauthorized writer fails "authorized" api', (t) => {
	const a = create.one();
	a.ready(() => {
		const b = create.one(a.key);
		b.ready(() => {
			b.authorized(b.local.key, (err, auth) => {
				t.error(err);
				t.equals(auth, false);
				t.end();
			});
		});
	});
});

tape("local unauthorized writes =/> authorized", (t) => {
	const a = create.one();
	a.ready(() => {
		const b = create.one(a.key);
		b.ready(() => {
			const schemaKey = "schema/foo";
			const schemaValue = {
				keySchema: "foo",
				valueValidationKey: "",
				keyValidation: ""
			};
			let writerSignatureB = EthCrypto.sign(privateKeyB, b.createSignHash(schemaKey, schemaValue));
			b.addSchema(schemaKey, schemaValue, writerSignatureB, writerAddressB, (err) => {
				t.error(err);

				const key = "foo";
				const value = "bar";
				writerSignatureB = EthCrypto.sign(privateKeyB, b.createSignHash(key, value));
				b.put(key, value, writerSignatureB, writerAddressB, { schemaKey }, (err) => {
					t.error(err);

					b.authorized(b.local.key, (err, auth) => {
						t.error(err);
						t.equals(auth, false);
						b.authorized(a.local.key, (err, auth) => {
							t.error(err);
							t.equals(auth, true);
							t.end();
						});
					});
				});
			});
		});
	});
});

tape("unauthorized writer doing a put after replication", (t) => {
	t.plan(2);
	const a = create.one();
	a.ready(() => {
		const b = create.one(a.key);
		b.ready(() => {
			replicate(a, b, () => {
				const schemaKey = "schema/foo";
				const schemaValue = {
					keySchema: "foo",
					valueValidationKey: "",
					keyValidation: ""
				};
				let writerSignatureB = EthCrypto.sign(privateKeyB, b.createSignHash(schemaKey, schemaValue));
				b.addSchema(schemaKey, schemaValue, writerSignatureB, writerAddressB, (err) => {
					t.error(err);

					const key = "foo";
					const value = "bar";
					writerSignatureB = EthCrypto.sign(privateKeyB, b.createSignHash(key, value));
					b.put(key, value, writerSignatureB, writerAddressB, { schemaKey }, (err) => {
						console.log("err", err);
						t.error(err);
					});
				});
			});
		});
	});
});

tape('unauthorized writer fails "authorized" after some writes', (t) => {
	const a = create.one();
	a.ready(() => {
		const schemaKey = "schema/foo";
		const schemaValue = {
			keySchema: "foo",
			valueValidationKey: "",
			keyValidation: ""
		};
		let writerSignatureA = EthCrypto.sign(privateKeyA, a.createSignHash(schemaKey, schemaValue));
		a.addSchema(schemaKey, schemaValue, writerSignatureA, writerAddressA, (err) => {
			t.error(err);

			const done = (err) => {
				t.error(err);
				const b = create.one(a.key);
				b.ready(() => {
					replicate(a, b, () => {
						b.authorized(b.local.key, (err, auth) => {
							t.error(err);
							t.equals(auth, false);
							t.end();
						});
					});
				});
			};

			run(
				(cb) => a.put("foo", "bar", EthCrypto.sign(privateKeyA, a.createSignHash("foo", "bar")), writerAddressA, { schemaKey }, cb),
				(cb) =>
					a.put("foo", "bar2", EthCrypto.sign(privateKeyA, a.createSignHash("foo", "bar2")), writerAddressA, { schemaKey }, cb),
				(cb) =>
					a.put("foo", "bar3", EthCrypto.sign(privateKeyA, a.createSignHash("foo", "bar3")), writerAddressA, { schemaKey }, cb),
				(cb) =>
					a.put("foo", "bar4", EthCrypto.sign(privateKeyA, a.createSignHash("foo", "bar4")), writerAddressA, { schemaKey }, cb),
				done
			);
		});
	});
});

tape("authorized is consistent", (t) => {
	t.plan(7);

	const a = create.one(null, { contentFeed: true });
	a.ready(() => {
		const schemaKeyA = "schema/foo";
		const schemaValueA = {
			keySchema: "foo",
			valueValidationKey: "",
			keyValidation: ""
		};
		let writerSignatureA = EthCrypto.sign(privateKeyA, a.createSignHash(schemaKeyA, schemaValueA));
		a.addSchema(schemaKeyA, schemaValueA, writerSignatureA, writerAddressA, (err) => {
			t.error(err);

			const b = create.one(a.key, { contentFeed: true, latency: 10 });

			const schemaKeyB = "schema/bar";
			const schemaValueB = {
				keySchema: "bar",
				valueValidationKey: "",
				keyValidation: ""
			};
			let writerSignatureB = EthCrypto.sign(privateKeyB, b.createSignHash(schemaKeyB, schemaValueB));
			b.addSchema(schemaKeyB, schemaValueB, writerSignatureB, writerAddressB, (err) => {
				t.error(err);

				const done = (err) => {
					t.error(err, "no error");
					a.authorized(b.local.key, (err, auth) => {
						t.error(err, "no error");
						t.ok(auth);
					});
					b.authorized(b.local.key, (err, auth) => {
						t.error(err, "no error");
						t.ok(auth);
					});
				};

				const auth = (cb) => {
					a.authorize(b.local.key, cb);
				};

				run(
					(cb) =>
						b.put(
							"bar",
							"foo",
							EthCrypto.sign(privateKeyB, b.createSignHash("bar", "foo")),
							writerAddressB,
							{ schemaKey: schemaKeyB },
							cb
						),
					(cb) =>
						a.put(
							"foo",
							"bar",
							EthCrypto.sign(privateKeyA, a.createSignHash("foo", "bar")),
							writerAddressA,
							{ schemaKey: schemaKeyA },
							cb
						),
					auth,
					replicate.bind(null, a, b),
					done
				);
			});
		});
	});
});
