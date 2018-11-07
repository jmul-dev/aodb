const tape = require("tape");
const replicate = require("./helpers/replicate");
const create = require("./helpers/create");
const put = require("./helpers/put");
const run = require("./helpers/run");
const EthCrypto = require("eth-crypto");
const { privateKey, publicKey: writerAddress } = EthCrypto.createIdentity();

const testHistory = (t, db, key, expected, cb) => {
	const results = expected.slice(0);
	const stream = db.createKeyHistoryStream(key);
	stream.on("data", (data) => {
		let expected = results.shift();
		t.notEqual(expected, undefined);
		if (!Array.isArray(expected)) expected = [expected];
		t.same(data.length, expected.length);
		expected.forEach((value, i) => {
			t.same(data[i].value, value);
		});
	});
	stream.on("end", () => {
		t.same(results.length, 0);
		cb();
	});
	stream.on("error", cb);
};

tape(
	"empty db",
	(t) => {
		const db = create.one();
		run((cb) => testHistory(t, db, "hello", [], cb), t.end);
	},
	{ timeout: 1000 }
);

tape(
	"single feed",
	(t) => {
		const db = create.one();
		const schemaKey = "schema/*";
		const schemaValue = {
			keySchema: "*",
			valueValidationKey: "",
			keyValidation: ""
		};
		db.addSchema(
			schemaKey,
			schemaValue,
			EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue)),
			writerAddress,
			(err) => {
				t.error(err, "no error");
				run(
					(cb) =>
						put(
							db,
							privateKey,
							writerAddress,
							[{ key: "hello", value: "welt" }, { key: "null", value: "void" }, { key: "hello", value: "world" }],
							{ schemaKey },
							cb
						),
					(cb) => testHistory(t, db, "hello", ["world", "welt"], cb),
					(cb) => testHistory(t, db, "null", ["void"], cb),
					t.end
				);
			}
		);
	},
	{ timeout: 1000 }
);

tape(
	"single feed (same value)",
	(t) => {
		const db = create.one();
		const schemaKey = "schema/*";
		const schemaValue = {
			keySchema: "*",
			valueValidationKey: "",
			keyValidation: ""
		};
		db.addSchema(
			schemaKey,
			schemaValue,
			EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue)),
			writerAddress,
			(err) => {
				t.error(err, "no error");
				run(
					(cb) =>
						put(
							db,
							privateKey,
							writerAddress,
							[{ key: "hello", value: "welt" }, { key: "hello", value: "darkness" }, { key: "hello", value: "world" }],
							{ schemaKey },
							cb
						),
					(cb) => testHistory(t, db, "hello", ["world", "darkness", "welt"], cb),
					t.end
				);
			}
		);
	},
	{ timeout: 1000 }
);

tape(
	"two feeds",
	(t) => {
		create.two((db1, db2, replicate) => {
			const schemaKey = "schema/*";
			const schemaValue = {
				keySchema: "*",
				valueValidationKey: "",
				keyValidation: ""
			};
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
							run(
								(cb) =>
									put(
										db1,
										privateKey,
										writerAddress,
										[{ key: "hello", value: "welt" }, { key: "null", value: "void" }],
										{ schemaKey },
										cb
									),
								replicate,
								(cb) => put(db2, privateKey, writerAddress, [{ key: "hello", value: "world" }], { schemaKey }, cb),
								replicate,
								(cb) => testHistory(t, db1, "hello", ["world", "welt"], cb),
								t.end
							);
						}
					);
				}
			);
		});
	},
	{ timeout: 1000 }
);

tape(
	"two feeds with conflict",
	(t) => {
		create.two((db1, db2, replicate) => {
			const schemaKey = "schema/*";
			const schemaValue = {
				keySchema: "*",
				valueValidationKey: "",
				keyValidation: ""
			};
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
							run(
								(cb) =>
									put(
										db1,
										privateKey,
										writerAddress,
										[{ key: "hello", value: "welt" }, { key: "null", value: "void" }],
										{ schemaKey },
										cb
									),
								(cb) => put(db2, privateKey, writerAddress, [{ key: "hello", value: "world" }], { schemaKey }, cb),
								replicate,
								(cb) => testHistory(t, db1, "hello", [["world", "welt"]], cb),
								t.end
							);
						}
					);
				}
			);
		});
	},
	{ timeout: 1000 }
);

tape(
	"three feeds with conflict",
	(t) => {
		create.three((db1, db2, db3, replicateAll) => {
			const schemaKey = "schema/*";
			const schemaValue = {
				keySchema: "*",
				valueValidationKey: "",
				keyValidation: ""
			};
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
							db3.addSchema(
								schemaKey,
								schemaValue,
								EthCrypto.sign(privateKey, db3.createSignHash(schemaKey, schemaValue)),
								writerAddress,
								(err) => {
									t.error(err, "no error");
									run(
										(cb) =>
											put(
												db1,
												privateKey,
												writerAddress,
												[{ key: "hello", value: "welt" }, { key: "null", value: "void" }],
												{ schemaKey },
												cb
											),
										(cb) => replicate(db1, db2, cb),
										(cb) => put(db2, privateKey, writerAddress, [{ key: "hello", value: "world" }], { schemaKey }, cb),
										(cb) => replicate(db1, db2, cb),
										(cb) => put(db3, privateKey, writerAddress, [{ key: "hello", value: "again" }], { schemaKey }, cb),
										replicateAll,
										(cb) => testHistory(t, db1, "hello", [["world", "again"], "welt"], cb),
										t.end
									);
								}
							);
						}
					);
				}
			);
		});
	},
	{ timeout: 1000 }
);

tape(
	"three feeds with all conflicting",
	(t) => {
		create.three((db1, db2, db3, replicateAll) => {
			const schemaKey = "schema/*";
			const schemaValue = {
				keySchema: "*",
				valueValidationKey: "",
				keyValidation: ""
			};
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
							db3.addSchema(
								schemaKey,
								schemaValue,
								EthCrypto.sign(privateKey, db3.createSignHash(schemaKey, schemaValue)),
								writerAddress,
								(err) => {
									t.error(err, "no error");
									run(
										(cb) =>
											put(
												db1,
												privateKey,
												writerAddress,
												[{ key: "hello", value: "welt" }, { key: "null", value: "void" }],
												{ schemaKey },
												cb
											),
										(cb) => put(db2, privateKey, writerAddress, [{ key: "hello", value: "world" }], { schemaKey }, cb),
										(cb) => put(db3, privateKey, writerAddress, [{ key: "hello", value: "again" }], { schemaKey }, cb),
										replicateAll,
										(cb) => testHistory(t, db1, "hello", [["world", "again", "welt"]], cb),
										t.end
									);
								}
							);
						}
					);
				}
			);
		});
	},
	{ timeout: 1000 }
);

tape(
	"three feeds (again)",
	(t) => {
		var toVersion = (v) => ({ key: "version", value: v });
		create.three((db1, db2, db3, replicateAll) => {
			const schemaKey = "schema/*";
			const schemaValue = {
				keySchema: "*",
				valueValidationKey: "",
				keyValidation: ""
			};
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
							db3.addSchema(
								schemaKey,
								schemaValue,
								EthCrypto.sign(privateKey, db3.createSignHash(schemaKey, schemaValue)),
								writerAddress,
								(err) => {
									t.error(err, "no error");

									const len = 5;
									const expected = [];
									for (let i = 0; i < len * 3; i++) {
										expected.push(i.toString());
									}
									run(
										(cb) =>
											put(db1, privateKey, writerAddress, expected.slice(0, len).map(toVersion), { schemaKey }, cb),
										replicateAll,
										(cb) =>
											put(
												db2,
												privateKey,
												writerAddress,
												expected.slice(len, len * 2).map(toVersion),
												{ schemaKey },
												cb
											),
										replicateAll,
										(cb) =>
											put(db3, privateKey, writerAddress, expected.slice(len * 2).map(toVersion), { schemaKey }, cb),
										replicateAll,
										(cb) => testHistory(t, db1, "version", expected.reverse(), cb),
										t.end
									);
								}
							);
						}
					);
				}
			);
		});
	},
	{ timeout: 1000 }
);
