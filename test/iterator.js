const tape = require("tape");
const create = require("./helpers/create");
const put = require("./helpers/put");
const run = require("./helpers/run");
const EthCrypto = require("eth-crypto");
const { privateKey, publicKey: writerAddress } = EthCrypto.createIdentity();

const range = (n, v) => {
	// #0, #1, #2, ...
	return new Array(n)
		.join(".")
		.split(".")
		.map((a, i) => v + i);
};

const toMap = (list) => {
	const map = {};
	for (let i = 0; i < list.length; i++) {
		map[list[i]] = list[i];
	}
	return map;
};

const appendSchema = (list, schemaKey) => {
	const listWithSchema = [];
	for (let i = 0; i < list.length; i++) {
		listWithSchema.push({ key: list[i], schemaKey: schemaKey });
	}
	return listWithSchema;
};

const all = (ite, cb) => {
	const vals = {};

	ite.next(function loop(err, node) {
		if (err) return cb(err);
		if (!node) return cb(null, vals);
		if (node.isSchema) {
			ite.next(loop);
		} else {
			const key = Array.isArray(node) ? node[0].key : node.key;
			if (vals[key]) return cb(new Error("duplicate node for " + key));
			vals[key] = Array.isArray(node) ? node.map((n) => n.value).sort() : node.value;
			ite.next(loop);
		}
	});
};

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

const schemaKey4 = "schema/*/*/*/*";
const schemaValue4 = {
	keySchema: "*/*/*/*",
	valueValidationKey: "",
	keyValidation: ""
};

tape("basic iteration", (t) => {
	const db = create.one();
	db.addSchema(schemaKey, schemaValue, EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue)), writerAddress, (err) => {
		t.error(err, "no error");

		const vals = ["a", "b", "c"];
		const expected = toMap(vals);

		put(db, privateKey, writerAddress, vals, { schemaKey }, (err) => {
			t.error(err, "no error");
			all(db.iterator(), (err, map) => {
				t.error(err, "no error");
				t.same(map, expected, "iterated all values");
				t.end();
			});
		});
	});
});

tape("iterate a big db", (t) => {
	const db = create.one();
	db.addSchema(schemaKey, schemaValue, EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue)), writerAddress, (err) => {
		t.error(err, "no error");

		const vals = range(1000, "#");
		const expected = toMap(vals);

		put(db, privateKey, writerAddress, vals, { schemaKey }, (err) => {
			t.error(err, "no error");
			all(db.iterator(), (err, map) => {
				t.error(err, "no error");
				t.same(map, expected, "iterated all values");
				t.end();
			});
		});
	});
});

tape("prefix basic iteration", (t) => {
	const db = create.one();
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

		let vals = [
			{ key: "foo/a", schemaKey: schemaKey2 },
			{ key: "foo/b", schemaKey: schemaKey2 },
			{ key: "foo/c", schemaKey: schemaKey2 }
		];
		const expected = toMap(["foo/a", "foo/b", "foo/c"]);

		vals = vals.concat([{ key: "a", schemaKey }, { key: "b", schemaKey }, { key: "c", schemaKey }]);

		put(db, privateKey, writerAddress, vals, {}, (err) => {
			t.error(err, "no error");
			all(db.iterator("foo"), (err, map) => {
				t.error(err, "no error");
				t.same(map, expected, "iterated all values");
				t.end();
			});
		});
	});
});

tape("empty prefix iteration", (t) => {
	const db = create.one();
	db.addSchema(
		schemaKey2,
		schemaValue2,
		EthCrypto.sign(privateKey, db.createSignHash(schemaKey2, schemaValue2)),
		writerAddress,
		(err) => {
			t.error(err, "no error");

			const vals = ["foo/a", "foo/b", "foo/c"];
			const expected = {};

			put(db, privateKey, writerAddress, vals, { schemaKey: schemaKey2 }, (err) => {
				t.error(err, "no error");
				all(db.iterator("bar"), (err, map) => {
					t.error(err, "no error");
					t.same(map, expected, "iterated all values");
					t.end();
				});
			});
		}
	);
});

tape("prefix iterate a big db", (t) => {
	const db = create.one();
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

		let vals = range(1000, "foo/#");
		const expected = toMap(vals);
		let valsWithSchema = appendSchema(vals, schemaKey2);

		valsWithSchema = valsWithSchema.concat(appendSchema(range(1000, "#"), schemaKey));
		put(db, privateKey, writerAddress, valsWithSchema, {}, (err) => {
			t.error(err, "no error");
			all(db.iterator("foo"), (err, map) => {
				t.error(err, "no error");
				t.same(map, expected, "iterated all values");
				t.end();
			});
		});
	});
});

tape("non recursive iteration", (t) => {
	const db = create.one();
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
		},
		{
			type: "add-schema",
			key: schemaKey3,
			value: schemaValue3,
			writerSignature: EthCrypto.sign(privateKey, db.createSignHash(schemaKey3, schemaValue3)),
			writerAddress: writerAddress
		},
		{
			type: "add-schema",
			key: schemaKey4,
			value: schemaValue4,
			writerSignature: EthCrypto.sign(privateKey, db.createSignHash(schemaKey4, schemaValue4)),
			writerAddress: writerAddress
		}
	];
	db.batch(batchList, (err) => {
		t.error(err, "no error");

		const vals = [
			{ key: "a", schemaKey },
			{ key: "a/b/c/d", schemaKey: schemaKey4 },
			{ key: "a/c", schemaKey: schemaKey2 },
			{ key: "b", schemaKey },
			{ key: "b/b/c", schemaKey: schemaKey3 },
			{ key: "c/a", schemaKey: schemaKey2 },
			{ key: "c", schemaKey }
		];

		put(db, privateKey, writerAddress, vals, {}, (err) => {
			t.error(err, "no error");
			all(db.iterator({ recursive: false }), (err, map) => {
				t.error(err, "no error");
				const keys = Object.keys(map).map((k) => k.split("/")[0]);
				t.same(keys.sort(), ["a", "b", "c"], "iterated all values");
				t.end();
			});
		});
	});
});

tape("mixed nested and non nexted iteration", (t) => {
	const db = create.one();
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

		const vals = [
			{ key: "a", schemaKey },
			{ key: "a/a", schemaKey: schemaKey2 },
			{ key: "a/b", schemaKey: schemaKey2 },
			{ key: "a/c", schemaKey: schemaKey2 },
			{ key: "a/a/a", schemaKey: schemaKey3 },
			{ key: "a/a/b", schemaKey: schemaKey3 },
			{ key: "a/a/c", schemaKey: schemaKey3 }
		];
		const expected = toMap(["a", "a/a", "a/b", "a/c", "a/a/a", "a/a/b", "a/a/c"]);

		put(db, privateKey, writerAddress, vals, {}, (err) => {
			t.error(err, "no error");
			all(db.iterator(), (err, map) => {
				t.error(err, "no error");
				t.same(map, expected, "iterated all values");
				t.end();
			});
		});
	});
});

tape("two writers, simple fork", (t) => {
	t.plan(2 + 2 * 2 + 1);

	create.two((db1, db2, replicate) => {
		const batchList1 = [
			{
				type: "add-schema",
				key: schemaKey,
				value: schemaValue,
				writerSignature: EthCrypto.sign(privateKey, db1.createSignHash(schemaKey, schemaValue)),
				writerAddress: writerAddress
			},
			{
				type: "add-schema",
				key: schemaKey2,
				value: schemaValue2,
				writerSignature: EthCrypto.sign(privateKey, db1.createSignHash(schemaKey2, schemaValue2)),
				writerAddress: writerAddress
			}
		];
		db1.batch(batchList1, (err) => {
			t.error(err, "no error");

			const batchList2 = [
				{
					type: "add-schema",
					key: schemaKey,
					value: schemaValue,
					writerSignature: EthCrypto.sign(privateKey, db2.createSignHash(schemaKey, schemaValue)),
					writerAddress: writerAddress
				},
				{
					type: "add-schema",
					key: schemaKey2,
					value: schemaValue2,
					writerSignature: EthCrypto.sign(privateKey, db2.createSignHash(schemaKey2, schemaValue2)),
					writerAddress: writerAddress
				}
			];
			db2.batch(batchList2, (err) => {
				t.error(err, "no error");

				const done = (err) => {
					t.error(err, "no error");
					all(db1.iterator(), ondb1all);
					all(db2.iterator(), ondb2all);
				};

				const ondb2all = (err, map) => {
					t.error(err, "no error");
					delete map["schema/*"];
					delete map["schema/*/*"];
					t.same(map, { "0": ["0"], "1": ["1a", "1b"], "10": ["10"] });
				};

				const ondb1all = (err, map) => {
					t.error(err, "no error");
					delete map["schema/*"];
					delete map["schema/*/*"];
					t.same(map, { "0": ["0"], "1": ["1a", "1b"], "10": ["10"], "2": ["2"], "1/0": ["1/0"] });
				};

				run(
					(cb) => db1.put("0", "0", EthCrypto.sign(privateKey, db1.createSignHash("0", "0")), writerAddress, { schemaKey }, cb),
					replicate,
					(cb) => db1.put("1", "1a", EthCrypto.sign(privateKey, db1.createSignHash("1", "1a")), writerAddress, { schemaKey }, cb),
					(cb) => db2.put("1", "1b", EthCrypto.sign(privateKey, db2.createSignHash("1", "1b")), writerAddress, { schemaKey }, cb),
					(cb) =>
						db1.put("10", "10", EthCrypto.sign(privateKey, db1.createSignHash("10", "10")), writerAddress, { schemaKey }, cb),
					replicate,
					(cb) => db1.put("2", "2", EthCrypto.sign(privateKey, db1.createSignHash("2", "2")), writerAddress, { schemaKey }, cb),
					(cb) =>
						db1.put(
							"1/0",
							"1/0",
							EthCrypto.sign(privateKey, db1.createSignHash("1/0", "1/0")),
							writerAddress,
							{ schemaKey: schemaKey2 },
							cb
						),
					done
				);
			});
		});
	});
});

tape("two writers, one fork", (t) => {
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
							all(db1.iterator(), (err, vals) => {
								t.error(err, "no error");
								delete vals["schema/*"];
								t.same(vals, {
									"0": ["00"],
									"1": ["1a", "1b"],
									"2": ["2"],
									"3": ["3"],
									"4": ["4"],
									"5": ["5"],
									"6": ["6"],
									"7": ["7"],
									"8": ["8"],
									"9": ["9"]
								});

								all(db2.iterator(), (err, vals) => {
									t.error(err, "no error");
									delete vals["schema/*"];
									t.same(vals, {
										"0": ["00"],
										"1": ["1a", "1b"],
										"2": ["2"],
										"3": ["3"],
										"4": ["4"],
										"5": ["5"],
										"6": ["6"],
										"7": ["7"],
										"8": ["8"],
										"9": ["9"],
										hi: ["ho"]
									});
									t.end();
								});
							});
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
							(cb) => replicate(cb),
							(cb) =>
								db1.put(
									"0",
									"00",
									EthCrypto.sign(privateKey, db1.createSignHash("0", "00")),
									writerAddress,
									{ schemaKey },
									cb
								),
							(cb) => replicate(cb),
							(cb) =>
								db2.put(
									"hi",
									"ho",
									EthCrypto.sign(privateKey, db2.createSignHash("hi", "ho")),
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

tape("two writers, one fork, many values", (t) => {
	const r = range(100, "i");

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

							const expected = {
								"0": ["00"],
								"1": ["1a", "1b"],
								"2": ["2"],
								"3": ["3"],
								"4": ["4"],
								"5": ["5"],
								"6": ["6"],
								"7": ["7"],
								"8": ["8"],
								"9": ["9"]
							};

							r.forEach((v) => {
								expected[v] = [v];
							});

							all(db1.iterator(), (err, vals) => {
								t.error(err, "no error");
								delete vals["schema/*"];
								t.same(vals, expected);
								all(db2.iterator(), (err, vals) => {
									t.error(err, "no error");
									delete vals["schema/*"];
									t.same(vals, expected);
									t.end();
								});
							});
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
							(cb) => replicate(cb),
							(cb) =>
								db1.put(
									"0",
									"00",
									EthCrypto.sign(privateKey, db1.createSignHash("0", "00")),
									writerAddress,
									{ schemaKey },
									cb
								),
							r.map((i) => (cb) =>
								db1.put(i, i, EthCrypto.sign(privateKey, db1.createSignHash(i, i)), writerAddress, { schemaKey }, cb)
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
	t.plan(2 + 2 * 2 + 1);

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

						const onall = (err, map) => {
							t.error(err, "no error");
							delete map["schema/*"];
							t.same(map, { b: ["c"], a: ["b"] });
						};

						all(a.iterator(), onall);
						all(b.iterator(), onall);
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
	t.plan(3 + 2 * 3 + 1);

	const replicate = require("./helpers/replicate");

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

								const onall = (err, map) => {
									t.error(err, "no error");
									delete map["schema/*"];
									t.same(map, { a: ["ab"], c: ["c"], some: ["some"] });
								};

								all(a.iterator(), onall);
								all(b.iterator(), onall);
								all(c.iterator(), onall);
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

tape("list buffers an iterator", (t) => {
	const db = create.one();

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

		const vals = [{ key: "a", schemaKey }, { key: "b", schemaKey }, { key: "b/c", schemaKey: schemaKey2 }];
		put(db, privateKey, writerAddress, vals, {}, (err) => {
			t.error(err, "no error");
			db.list((err, all) => {
				t.error(err, "no error");
				t.same(all.map((v) => v.key).sort(), ["a", "b", "b/c", "schema/*", "schema/*/*"]);
				db.list("b", { gt: true }, (err, all) => {
					t.error(err, "no error");
					t.same(all.length, 1);
					t.same(all[0].key, "b/c");
					t.end();
				});
			});
		});
	});
});

tape("options to get deleted keys", (t) => {
	const db = create.one();
	db.addSchema(schemaKey, schemaValue, EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue)), writerAddress, (err) => {
		t.error(err, "no error");

		const done = () => {
			all(db.iterator({ deletes: true }), (err, map) => {
				t.error(err, "no error");
				delete map["schema/*"];
				t.same(map, { a: "", b: "b", c: "c" }, "iterated all values");
				t.end();
			});
		};

		run(
			(cb) => put(db, privateKey, writerAddress, ["a", "b", "c"], { schemaKey }, cb),
			(cb) => db.del("a", EthCrypto.sign(privateKey, db.createSignHash("a", "")), writerAddress, cb),
			done
		);
	});
});

tape("three writers, two forks with deletes", (t) => {
	t.plan(3 + 2 * 3 + 1);

	const replicate = require("./helpers/replicate");

	create.three(function(a, b, c, replicateAll) {
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
								const onall = (err, map) => {
									t.error(err, "no error");
									delete map["schema/*"];
									t.same(map, { a: ["", "ab"], c: [""], some: [""] });
								};

								all(a.iterator({ deletes: true }), onall);
								all(b.iterator({ deletes: true }), onall);
								all(c.iterator({ deletes: true }), onall);
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
								(cb) => c.del("c", EthCrypto.sign(privateKey, c.createSignHash("c", "")), writerAddress, cb),
								(cb) => a.del("a", EthCrypto.sign(privateKey, a.createSignHash("a", "")), writerAddress, cb),
								(cb) => a.del("some", EthCrypto.sign(privateKey, a.createSignHash("some", "")), writerAddress, cb),
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
