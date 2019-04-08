const tape = require("tape");
const create = require("./helpers/create");
const replicate = require("./helpers/replicate");
const put = require("./helpers/put");
const EthCrypto = require("eth-crypto");
const { privateKey, publicKey: writerAddress } = EthCrypto.createIdentity();

const toKeyValuePairs = (value) => {
	return (k) => ({ key: k, value: value || k });
};

const indexWithKey = (key) => {
	return (v) => v.key === key;
};

const schemaKey = "schema/*";
const schemaValue = {
	keySchema: "*",
	valueValidationKey: "",
	keyValidation: ""
};
const wildStringSchema = "wildStringSchema/*";

const schemaKey2 = "schema/*/*";
const schemaValue2 = {
	keySchema: "*/*",
	valueValidationKey: "",
	keyValidation: ""
};
const wildStringSchema2 = "wildStringSchema/*/*";

const schemaKey3 = "schema/*/*/*";
const schemaValue3 = {
	keySchema: "*/*/*",
	valueValidationKey: "",
	keyValidation: ""
};
const wildStringSchema3 = "wildStringSchema/*/*/*";

tape("basic readStream", { timeout: 1000 }, (t) => {
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
			{ key: "foo", schemaKey },
			{ key: "foo/a", schemaKey: schemaKey2 },
			{ key: "foo/b", schemaKey: schemaKey2 },
			{ key: "a", schemaKey },
			{ key: "bar/a", schemaKey: schemaKey2 },
			{ key: "foo/abc", schemaKey: schemaKey2 },
			{ key: "foo/b", schemaKey: schemaKey2 },
			{ key: "bar/b", schemaKey: schemaKey2 },
			{ key: "foo/bar", schemaKey: schemaKey2 },
			{ key: "foo/a/b", schemaKey: schemaKey3 }
		];
		const expected = ["foo/a", "foo/abc", "foo/b", "foo/bar", "foo/a/b"];

		const validate = (err) => {
			t.error(err, "no error");
			const reader = db.createReadStream("foo/", { gt: true });
			reader.on("data", (data) => {
				const index = expected.indexOf(data.key);
				t.ok(index !== -1, "key is expected");
				if (index >= 0) expected.splice(index, 1);
			});
			reader.on("end", () => {
				t.equals(expected.length, 0);
				t.end();
			});
			reader.on("error", (err) => {
				t.fail(err.message);
				t.end();
			});
		};

		put(db, privateKey, writerAddress, vals, {}, validate);
	});
});

tape("basic readStream (again)", { timeout: 1000 }, (t) => {
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
			{ key: "foo/a", schemaKey: schemaKey2 },
			{ key: "foo/abc", schemaKey: schemaKey2 },
			{ key: "foo/a/b", schemaKey: schemaKey3 }
		];
		const expected = ["foo/a", "foo/a/b"];

		const validate = (err) => {
			t.error(err, "no error");
			const reader = db.createReadStream("foo/a");
			reader.on("data", (data) => {
				const index = expected.indexOf(data.key);
				t.ok(index !== -1, "key is expected");
				if (index >= 0) expected.splice(index, 1);
			});
			reader.on("end", () => {
				t.equals(expected.length, 0);
				t.end();
			});
			reader.on("error", (err) => {
				t.fail(err.message);
				t.end();
			});
		};

		put(db, privateKey, writerAddress, vals, {}, validate);
	});
});

tape("readStream with two feeds", { timeout: 1000 }, (t) => {
	create.two((a, b) => {
		const batchList1 = [
			{
				type: "add-schema",
				key: schemaKey2,
				value: schemaValue2,
				writerSignature: EthCrypto.sign(privateKey, a.createSignHash(schemaKey2, schemaValue2)),
				writerAddress: writerAddress
			},
			{
				type: "add-schema",
				key: schemaKey3,
				value: schemaValue3,
				writerSignature: EthCrypto.sign(privateKey, a.createSignHash(schemaKey3, schemaValue3)),
				writerAddress: writerAddress
			}
		];
		a.batch(batchList1, (err) => {
			t.error(err, "no error");

			const batchList2 = [
				{
					type: "add-schema",
					key: schemaKey2,
					value: schemaValue2,
					writerSignature: EthCrypto.sign(privateKey, b.createSignHash(schemaKey2, schemaValue2)),
					writerAddress: writerAddress
				},
				{
					type: "add-schema",
					key: schemaKey3,
					value: schemaValue3,
					writerSignature: EthCrypto.sign(privateKey, b.createSignHash(schemaKey3, schemaValue3)),
					writerAddress: writerAddress
				}
			];
			b.batch(batchList2, (err) => {
				t.error(err, "no error");

				const aValues = [
					{ key: "b/a", value: "A", schemaKey: schemaKey2 },
					{ key: "a/b/c", value: "A", schemaKey: schemaKey3 },
					{ key: "b/c", value: "A", schemaKey: schemaKey2 },
					{ key: "b/c/d", value: "A", schemaKey: schemaKey3 }
				];
				const bValues = [
					{ key: "a/b", value: "B", schemaKey: schemaKey2 },
					{ key: "a/b/c", value: "B", schemaKey: schemaKey3 },
					{ key: "b/c/d", value: "B", schemaKey: schemaKey3 },
					{ key: "b/c", value: "B", schemaKey: schemaKey2 }
				];

				const validate = (err) => {
					t.error(err, "no error");
					const reader = a.createReadStream("b/");
					const expected = [{ key: "b/c/d", value: "B" }, { key: "b/c", value: "B" }, { key: "b/a", value: "A" }];
					reader.on("data", (nodes) => {
						t.equals(nodes.length, 1);
						const index = expected.findIndex(indexWithKey(nodes[0].key));
						t.ok(index !== -1, "key is expected");
						if (index >= 0) {
							const found = expected.splice(index, 1);
							t.same(found[0].value, nodes[0].value);
						}
					});
					reader.on("end", () => {
						t.ok(expected.length === 0, "received all expected");
						t.pass("stream ended ok");
						t.end();
					});
					reader.on("error", (err) => {
						t.fail(err.message);
						t.end();
					});
				};

				put(a, privateKey, writerAddress, aValues, {}, (err) => {
					t.error(err, "no error");
					replicate(a, b, () => {
						put(b, privateKey, writerAddress, bValues, {}, (err) => {
							t.error(err, "no error");
							replicate(a, b, validate);
						});
					});
				});
			});
		});
	});
});

tape("readStream with two feeds (again)", { timeout: 1000 }, (t) => {
	const aValues = [
		{ key: "a/a", value: "A", schemaKey: schemaKey2 },
		{ key: "a/b", value: "A", schemaKey: schemaKey2 },
		{ key: "a/c", value: "A", schemaKey: schemaKey2 }
	];
	const bValues = [
		{ key: "b/a", value: "B", schemaKey: schemaKey2 },
		{ key: "b/b", value: "B", schemaKey: schemaKey2 },
		{ key: "b/c", value: "B", schemaKey: schemaKey2 },
		{ key: "a/a", value: "B", schemaKey: schemaKey2 },
		{ key: "a/b", value: "B", schemaKey: schemaKey2 },
		{ key: "a/c", value: "B", schemaKey: schemaKey2 }
	];

	create.two((a, b) => {
		a.addSchema(
			schemaKey2,
			schemaValue2,
			EthCrypto.sign(privateKey, a.createSignHash(schemaKey2, schemaValue2)),
			writerAddress,
			(err) => {
				t.error(err, "no error");
				b.addSchema(
					schemaKey2,
					schemaValue2,
					EthCrypto.sign(privateKey, b.createSignHash(schemaKey2, schemaValue2)),
					writerAddress,
					(err) => {
						t.error(err, "no error");

						const validate = () => {
							const reader = b.createReadStream("/");
							const expected = ["b/a", "b/b", "b/c", "a/a", "a/b", "a/c"];
							reader.on("data", (data) => {
								if (data[0].key !== schemaKey2 && data[0].key !== wildStringSchema2) {
									t.equals(data.length, 1);
									const index = expected.indexOf(data[0].key);
									t.ok(index !== -1, "key is expected");
									t.same(data[0].value, "B");
									if (index >= 0) expected.splice(index, 1);
								}
							});
							reader.on("end", () => {
								t.ok(expected.length === 0, "received all expected");
								t.pass("stream ended ok");
								t.end();
							});
							reader.on("error", (err) => {
								t.fail(err.message);
								t.end();
							});
						};

						put(a, privateKey, writerAddress, aValues, {}, (err) => {
							t.error(err);
							replicate(a, b, () => {
								put(b, privateKey, writerAddress, bValues, {}, (err) => {
									t.error(err);
									replicate(a, b, validate);
								});
							});
						});
					}
				);
			}
		);
	});
});

tape("readStream with conflicting feeds", { timeout: 2000 }, (t) => {
	const aValues = [
		{ key: "a/a", value: "A", schemaKey: schemaKey2 },
		{ key: "a/b", value: "A", schemaKey: schemaKey2 },
		{ key: "a/c", value: "A", schemaKey: schemaKey2 }
	];
	const bValues = [
		{ key: "b/a", value: "B", schemaKey: schemaKey2 },
		{ key: "b/b", value: "B", schemaKey: schemaKey2 },
		{ key: "b/c", value: "B", schemaKey: schemaKey2 }
	];
	const conflictingKeysValues = [
		{ key: "c/a", value: "A", schemaKey: schemaKey2 },
		{ key: "c/b", value: "A", schemaKey: schemaKey2 },
		{ key: "c/c", value: "A", schemaKey: schemaKey2 },
		{ key: "c/d", value: "A", schemaKey: schemaKey2 }
	];
	const reverseConflictingKeysValues = [
		{ key: "c/d", value: "B", schemaKey: schemaKey2 },
		{ key: "c/c", value: "B", schemaKey: schemaKey2 },
		{ key: "c/b", value: "B", schemaKey: schemaKey2 },
		{ key: "c/a", value: "B", schemaKey: schemaKey2 }
	];
	create.two((a, b) => {
		a.addSchema(
			schemaKey2,
			schemaValue2,
			EthCrypto.sign(privateKey, a.createSignHash(schemaKey2, schemaValue2)),
			writerAddress,
			(err) => {
				t.error(err, "no error");
				b.addSchema(
					schemaKey2,
					schemaValue2,
					EthCrypto.sign(privateKey, b.createSignHash(schemaKey2, schemaValue2)),
					writerAddress,
					(err) => {
						t.error(err, "no error");

						const validate = () => {
							const expected = ["a/a", "a/b", "a/c", "b/a", "b/b", "b/c", "c/b", "c/c", "c/a", "c/d"];
							const reader = a.createReadStream("/");
							reader.on("data", (data) => {
								if (data[0].key !== schemaKey2 && data[0].key !== wildStringSchema2) {
									let isConflicting = false;
									for (let i = 0; i < conflictingKeysValues.length; i++) {
										if (data[0].key === conflictingKeysValues[i].key) {
											isConflicting = true;
										}
									}
									if (isConflicting) {
										t.equals(data.length, 2);
									} else {
										t.equals(data.length, 1);
									}
									const index = expected.indexOf(data[0].key);

									t.ok(index !== -1, "key is expected");
									if (index >= 0) expected.splice(index, 1);
								}
							});
							reader.on("end", () => {
								t.ok(expected.length === 0, "received all expected");
								t.pass("stream ended ok");
								t.end();
							});
							reader.on("error", (err) => {
								t.fail(err.message);
								t.end();
							});
						};

						put(a, privateKey, writerAddress, aValues, {}, (err) => {
							t.error(err);
							replicate(a, b, () => {
								put(b, privateKey, writerAddress, bValues, {}, (err) => {
									t.error(err);
									replicate(a, b, (err) => {
										t.error(err);
										put(a, privateKey, writerAddress, conflictingKeysValues, {}, (err) => {
											t.error(err);
											put(b, privateKey, writerAddress, reverseConflictingKeysValues, {}, (err) => {
												t.error(err);
												replicate(a, b, validate);
											});
										});
									});
								});
							});
						});
					}
				);
			}
		);
	});
});

tape("returns no data if db is empty", (t) => {
	const db = create.one();
	const reader = db.createReadStream("foo/");

	reader.on("data", (data) => {
		t.fail("should be no data");
		t.end();
	});
	reader.on("end", () => {
		t.ok("everything is ok");
		t.end();
	});
	reader.on("error", (err) => {
		t.fail(err.message);
		t.end();
	});
});
