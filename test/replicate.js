const tape = require("tape");
const cmp = require("compare");
const create = require("./helpers/create");
const run = require("./helpers/run");
const replicate = require("./helpers/replicate");
const EthCrypto = require("eth-crypto");
const { privateKey, publicKey: writerAddress } = EthCrypto.createIdentity();

const range = (n) => {
	return Array(n)
		.join(",")
		.split(",")
		.map((_, i) => "" + i);
};

const schemaKey = "schema/*";
const schemaValue = {
	keySchema: "*",
	valueValidationKey: "",
	keyValidation: ""
};

tape("two writers, no conflicts, many values", (t) => {
	t.plan(2 + 1 + 3 * 4);

	create.two((db1, db2, replicate) => {
		db1.addSchema(
			schemaKey,
			schemaValue,
			EthCrypto.sign(privateKey, db1.createSignHash(schemaKey, schemaValue)),
			writerAddress,
			(err) => {
				t.error(err, "no error");
				db2.addSchema(
					schemaKey,
					schemaValue,
					EthCrypto.sign(privateKey, db2.createSignHash(schemaKey, schemaValue)),
					writerAddress,
					(err) => {
						t.error(err, "no error");

						const r = [];
						for (let i = 0; i < 1000; i++) r.push("i" + i);

						const done = (err) => {
							t.error(err, "no error");

							const expect = (v) => {
								return function(err, nodes) {
									t.error(err, "no error");
									t.same(nodes.length, 1);
									t.same(nodes[0].key, v);
									t.same(nodes[0].value, v);
								};
							};

							db2.get("a", expect("a"));
							db1.get("0", expect("0"));
							db1.get("i424", expect("i424"));
						};

						run(
							(cb) =>
								db1.put(
									"0",
									"0",
									EthCrypto.sign(privateKey, db1.createSignHash("0", "0")),
									writerAddress,
									{ schemaKey },
									cb
								),
							(cb) => replicate(cb),
							(cb) =>
								db2.put(
									"a",
									"a",
									EthCrypto.sign(privateKey, db2.createSignHash("a", "a")),
									writerAddress,
									{ schemaKey },
									cb
								),
							(cb) => replicate(cb),
							(cb) =>
								db2.put(
									"2",
									"2",
									EthCrypto.sign(privateKey, db2.createSignHash("2", "2")),
									writerAddress,
									{ schemaKey },
									cb
								),
							(cb) =>
								db2.put(
									"3",
									"3",
									EthCrypto.sign(privateKey, db2.createSignHash("3", "3")),
									writerAddress,
									{ schemaKey },
									cb
								),
							(cb) =>
								db2.put(
									"4",
									"4",
									EthCrypto.sign(privateKey, db2.createSignHash("4", "4")),
									writerAddress,
									{ schemaKey },
									cb
								),
							(cb) =>
								db2.put(
									"5",
									"5",
									EthCrypto.sign(privateKey, db2.createSignHash("5", "5")),
									writerAddress,
									{ schemaKey },
									cb
								),
							(cb) =>
								db2.put(
									"6",
									"6",
									EthCrypto.sign(privateKey, db2.createSignHash("6", "6")),
									writerAddress,
									{ schemaKey },
									cb
								),
							(cb) =>
								db2.put(
									"7",
									"7",
									EthCrypto.sign(privateKey, db2.createSignHash("7", "7")),
									writerAddress,
									{ schemaKey },
									cb
								),
							(cb) =>
								db2.put(
									"8",
									"8",
									EthCrypto.sign(privateKey, db2.createSignHash("8", "8")),
									writerAddress,
									{ schemaKey },
									cb
								),
							(cb) =>
								db2.put(
									"9",
									"9",
									EthCrypto.sign(privateKey, db2.createSignHash("9", "9")),
									writerAddress,
									{ schemaKey },
									cb
								),
							(cb) => replicate(cb),
							(cb) =>
								db1.put(
									"b",
									"b",
									EthCrypto.sign(privateKey, db1.createSignHash("b", "b")),
									writerAddress,
									{ schemaKey },
									cb
								),
							(cb) =>
								db2.put(
									"c",
									"c",
									EthCrypto.sign(privateKey, db2.createSignHash("c", "c")),
									writerAddress,
									{ schemaKey },
									cb
								),
							(cb) => replicate(cb),
							(cb) =>
								db2.put(
									"d",
									"d",
									EthCrypto.sign(privateKey, db2.createSignHash("d", "d")),
									writerAddress,
									{ schemaKey },
									cb
								),
							(cb) => replicate(cb),
							r.map((i) => (cb) =>
								db1.put(i, i, EthCrypto.sign(privateKey, db1.createSignHash(i, i)), writerAddress, { schemaKey }, cb)
							),
							done
						);
					}
				);
			}
		);
	});
});

tape("two writers, one conflict", (t) => {
	t.plan(2 + 1 + 4 * 2 + 6 * 2);
	create.two((db1, db2, replicate) => {
		db1.addSchema(
			schemaKey,
			schemaValue,
			EthCrypto.sign(privateKey, db1.createSignHash(schemaKey, schemaValue)),
			writerAddress,
			(err) => {
				t.error(err, "no error");
				db2.addSchema(
					schemaKey,
					schemaValue,
					EthCrypto.sign(privateKey, db2.createSignHash(schemaKey, schemaValue)),
					writerAddress,
					(err) => {
						t.error(err, "no error");

						const done = (err) => {
							t.error(err, "no error");

							const onb = (err, nodes) => {
								t.error(err, "no error");
								nodes.sort((a, b) => cmp(a.value, b.value));
								t.same(nodes.length, 2);
								t.same(nodes[0].key, "b");
								t.same(nodes[0].value, "B");
								t.same(nodes[1].key, "b");
								t.same(nodes[1].value, "b");
							};

							const ona = (err, nodes) => {
								t.error(err, "no error");
								t.same(nodes.length, 1);
								t.same(nodes[0].key, "a");
								t.same(nodes[0].value, "A");
							};

							db1.get("a", ona);
							db2.get("a", ona);
							db1.get("b", onb);
							db2.get("b", onb);
						};

						run(
							(cb) =>
								db1.put(
									"a",
									"a",
									EthCrypto.sign(privateKey, db1.createSignHash("a", "a")),
									writerAddress,
									{ schemaKey },
									cb
								),
							(cb) => replicate(cb),
							(cb) =>
								db1.put(
									"b",
									"b",
									EthCrypto.sign(privateKey, db1.createSignHash("b", "b")),
									writerAddress,
									{ schemaKey },
									cb
								),
							(cb) =>
								db2.put(
									"b",
									"B",
									EthCrypto.sign(privateKey, db2.createSignHash("b", "B")),
									writerAddress,
									{ schemaKey },
									cb
								),
							(cb) => replicate(cb),
							(cb) =>
								db1.put(
									"a",
									"A",
									EthCrypto.sign(privateKey, db1.createSignHash("a", "A")),
									writerAddress,
									{ schemaKey },
									cb
								),
							(cb) => replicate(cb),
							done
						);
					}
				);
			}
		);
	});
});

tape("two writers, fork", (t) => {
	t.plan(2 + 4 * 2 + 1);

	create.two((a, b, replicate) => {
		a.addSchema(schemaKey, schemaValue, EthCrypto.sign(privateKey, a.createSignHash(schemaKey, schemaValue)), writerAddress, (err) => {
			t.error(err, "no error");
			b.addSchema(
				schemaKey,
				schemaValue,
				EthCrypto.sign(privateKey, b.createSignHash(schemaKey, schemaValue)),
				writerAddress,
				(err) => {
					t.error(err, "no error");

					const done = (err) => {
						t.error(err, "no error");
						a.get("a", ona);
						b.get("a", ona);
					};

					const ona = (err, nodes) => {
						t.error(err, "no error");
						t.same(nodes.length, 1);
						t.same(nodes[0].key, "a");
						t.same(nodes[0].value, "b");
					};

					run(
						(cb) => a.put("a", "a", EthCrypto.sign(privateKey, a.createSignHash("a", "a")), writerAddress, { schemaKey }, cb),
						replicate,
						(cb) => b.put("a", "b", EthCrypto.sign(privateKey, b.createSignHash("a", "b")), writerAddress, { schemaKey }, cb),
						(cb) => a.put("b", "c", EthCrypto.sign(privateKey, a.createSignHash("b", "c")), writerAddress, { schemaKey }, cb),
						replicate,
						done
					);
				}
			);
		});
	});
});

tape("three writers, two forks", (t) => {
	t.plan(3 + 4 * 3 + 1);

	create.three((a, b, c, replicateAll) => {
		a.addSchema(schemaKey, schemaValue, EthCrypto.sign(privateKey, a.createSignHash(schemaKey, schemaValue)), writerAddress, (err) => {
			t.error(err, "no error");
			b.addSchema(
				schemaKey,
				schemaValue,
				EthCrypto.sign(privateKey, b.createSignHash(schemaKey, schemaValue)),
				writerAddress,
				(err) => {
					t.error(err, "no error");
					c.addSchema(
						schemaKey,
						schemaValue,
						EthCrypto.sign(privateKey, c.createSignHash(schemaKey, schemaValue)),
						writerAddress,
						(err) => {
							t.error(err, "no error");

							const done = (err) => {
								t.error(err, "no error");

								const ona = (err, nodes) => {
									t.error(err, "no error");
									t.same(nodes.length, 1, "one node");
									t.same(nodes[0].key, "a");
									t.same(nodes[0].value, "ab");
								};

								a.get("a", ona);
								b.get("a", ona);
								c.get("a", ona);
							};

							run(
								(cb) =>
									a.put(
										"a",
										"a",
										EthCrypto.sign(privateKey, a.createSignHash("a", "a")),
										writerAddress,
										{ schemaKey },
										cb
									),
								replicateAll,
								(cb) =>
									b.put(
										"a",
										"ab",
										EthCrypto.sign(privateKey, b.createSignHash("a", "ab")),
										writerAddress,
										{ schemaKey },
										cb
									),
								(cb) =>
									a.put(
										"some",
										"some",
										EthCrypto.sign(privateKey, a.createSignHash("some", "some")),
										writerAddress,
										{ schemaKey },
										cb
									),
								(cb) => replicate(a, c, cb),
								(cb) =>
									c.put(
										"c",
										"c",
										EthCrypto.sign(privateKey, c.createSignHash("c", "c")),
										writerAddress,
										{ schemaKey },
										cb
									),
								replicateAll,
								done
							);
						}
					);
				}
			);
		});
	});
});

tape("two writers, simple fork", (t) => {
	t.plan(2 + 1 + 2 * (4 + 6) + 2 + 4);
	create.two((db1, db2, replicate) => {
		db1.addSchema(
			schemaKey,
			schemaValue,
			EthCrypto.sign(privateKey, db1.createSignHash(schemaKey, schemaValue)),
			writerAddress,
			(err) => {
				t.error(err, "no error");
				db2.addSchema(
					schemaKey,
					schemaValue,
					EthCrypto.sign(privateKey, db2.createSignHash(schemaKey, schemaValue)),
					writerAddress,
					(err) => {
						t.error(err, "no error");

						const on0 = (err, nodes) => {
							t.error(err, "no error");
							t.same(nodes.length, 1);
							t.same(nodes[0].key, "0");
							t.same(nodes[0].value, "0");
						};

						const on1 = (err, nodes) => {
							t.error(err, "no error");
							t.same(nodes.length, 2);
							nodes.sort((a, b) => cmp(a.value, b.value));
							t.same(nodes[0].key, "1");
							t.same(nodes[0].value, "1a");
							t.same(nodes[1].key, "1");
							t.same(nodes[1].value, "1b");
						};

						const on2db1 = (err, nodes) => {
							t.error(err, "no error");
							t.same(nodes.length, 1);
							t.same(nodes[0].key, "2");
							t.same(nodes[0].value, "2");
						};

						const on2db2 = (err, nodes) => {
							t.error(err, "no error");
							t.same(nodes.length, 0);
						};

						const done = (err) => {
							t.error(err, "no error");
							db1.get("0", on0);
							db1.get("1", on1);
							db1.get("2", on2db1);
							db2.get("0", on0);
							db2.get("1", on1);
							db2.get("2", on2db2);
						};

						run(
							(cb) =>
								db1.put(
									"0",
									"0",
									EthCrypto.sign(privateKey, db1.createSignHash("0", "0")),
									writerAddress,
									{ schemaKey },
									cb
								),
							replicate,
							(cb) =>
								db1.put(
									"1",
									"1a",
									EthCrypto.sign(privateKey, db1.createSignHash("1", "1a")),
									writerAddress,
									{ schemaKey },
									cb
								),
							(cb) =>
								db2.put(
									"1",
									"1b",
									EthCrypto.sign(privateKey, db2.createSignHash("1", "1b")),
									writerAddress,
									{ schemaKey },
									cb
								),
							replicate,
							(cb) =>
								db1.put(
									"2",
									"2",
									EthCrypto.sign(privateKey, db1.createSignHash("2", "2")),
									writerAddress,
									{ schemaKey },
									cb
								),
							done
						);
					}
				);
			}
		);
	});
});

tape("three writers, no conflicts, forks", (t) => {
	t.plan(3 + 1 + 4 * 3);

	create.three((a, b, c, replicateAll) => {
		a.addSchema(schemaKey, schemaValue, EthCrypto.sign(privateKey, a.createSignHash(schemaKey, schemaValue)), writerAddress, (err) => {
			t.error(err, "no error");
			b.addSchema(
				schemaKey,
				schemaValue,
				EthCrypto.sign(privateKey, b.createSignHash(schemaKey, schemaValue)),
				writerAddress,
				(err) => {
					t.error(err, "no error");
					c.addSchema(
						schemaKey,
						schemaValue,
						EthCrypto.sign(privateKey, c.createSignHash(schemaKey, schemaValue)),
						writerAddress,
						(err) => {
							t.error(err, "no error");

							const ona = (err, nodes) => {
								t.error(err, "no error");
								t.same(nodes.length, 1);
								t.same(nodes[0].key, "a");
								t.same(nodes[0].value, "aa");
							};

							const done = (err) => {
								t.error(err, "no error");
								a.get("a", ona);
								b.get("a", ona);
								c.get("a", ona);
							};

							run(
								(cb) =>
									c.put(
										"a",
										"ac",
										EthCrypto.sign(privateKey, c.createSignHash("a", "ac")),
										writerAddress,
										{ schemaKey },
										cb
									),
								replicateAll,
								(cb) =>
									a.put(
										"foo",
										"bar",
										EthCrypto.sign(privateKey, a.createSignHash("foo", "bar")),
										writerAddress,
										{ schemaKey },
										cb
									),
								replicateAll,
								(cb) =>
									a.put(
										"a",
										"aa",
										EthCrypto.sign(privateKey, a.createSignHash("a", "aa")),
										writerAddress,
										{ schemaKey },
										cb
									),
								(cb) => replicate(a, b, cb),
								range(50).map((key) => (cb) =>
									b.put(
										key,
										key,
										EthCrypto.sign(privateKey, b.createSignHash(key, key)),
										writerAddress,
										{ schemaKey },
										cb
									)
								),
								replicateAll,
								range(5).map((key) => (cb) =>
									c.put(
										key,
										"c" + key,
										EthCrypto.sign(privateKey, c.createSignHash(key, "c" + key)),
										writerAddress,
										{ schemaKey },
										cb
									)
								),
								done
							);
						}
					);
				}
			);
		});
	});
});

tape("replication to two new peers, only authorize one writer", (t) => {
	const a = create.one();
	a.ready(() => {
		a.addSchema(schemaKey, schemaValue, EthCrypto.sign(privateKey, a.createSignHash(schemaKey, schemaValue)), writerAddress, (err) => {
			t.error(err, "no error");

			const b = create.one(a.key);
			const c = create.one(a.key);

			const done = (err) => {
				t.error(err, "no error");
				c.authorized(c.local.key, (err, auth) => {
					t.error(err, "no error");
					t.notOk(auth);
					t.end();
				});
			};

			run(
				(cb) => b.ready(cb),
				(cb) => c.ready(cb),
				(cb) => a.put("foo", "bar", EthCrypto.sign(privateKey, a.createSignHash("foo", "bar")), writerAddress, { schemaKey }, cb),
				(cb) => a.authorize(b.local.key, cb),
				(cb) => replicate(a, b, cb),
				(cb) => replicate(a, c, cb),
				done
			);
		});
	});
});

tape("2 unauthed clones", (t) => {
	t.plan(1 + 1 + 2 * 2);

	const db = create.one(null);

	db.ready(() => {
		db.addSchema(
			schemaKey,
			schemaValue,
			EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue)),
			writerAddress,
			(err) => {
				t.error(err, "no error");

				const clone1 = create.one(db.key);
				const clone2 = create.one(db.key);

				const done = (err) => {
					t.error(err, "no error");
					const onhello = (err, node) => {
						t.error(err, "no error");
						t.same(node.value, "world");
					};

					clone1.get("hello", onhello);
					clone2.get("hello", onhello);
				};

				run(
					(cb) =>
						db.put(
							"hello",
							"world",
							EthCrypto.sign(privateKey, db.createSignHash("hello", "world")),
							writerAddress,
							{ schemaKey },
							cb
						),
					(cb) => clone1.ready(cb),
					(cb) => replicate(db, clone1, cb),
					(cb) => clone2.ready(cb),
					(cb) => replicate(clone1, clone2, cb),
					done
				);
			}
		);
	});
});

tape("opts is not mutated", (t) => {
	var db = create.one();
	var opts = {};
	db.replicate(opts);
	t.deepEqual(opts, {});
	t.end();
});
