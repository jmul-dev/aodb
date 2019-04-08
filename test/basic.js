const tape = require("tape");
const Readable = require("stream").Readable;

const create = require("./helpers/create");
const run = require("./helpers/run");

const EthCrypto = require("eth-crypto");
const { privateKey, publicKey: writerAddress } = EthCrypto.createIdentity();

tape("put without schema", (t) => {
	const db = create.one();
	const key = "hello";
	const value = "world";

	db.put(key, value, EthCrypto.sign(privateKey, db.createSignHash(key, value)), writerAddress, {}, (err, node) => {
		console.log("no schemaKey", err);
		t.ok(err, "expected error");
		t.equal(err.message, "Error: missing the schemaKey option for this entry", "error message");
		t.end();
	});
});

tape("put without signature", (t) => {
	const db = create.one();
	const schemaKey = "schema/hello";
	const schemaValue = {
		keySchema: "hello",
		valueValidationKey: "",
		keyValidation: ""
	};
	let writerSignature = EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue));
	db.addSchema(schemaKey, schemaValue, writerSignature, writerAddress, (err) => {
		t.error(err);
		const key = "hello";
		const value = "world";

		db.put(key, value, "", writerAddress, { schemaKey }, (err, node) => {
			console.log("no writerSignature", err);
			t.ok(err, "expected error");
			t.equal(err.message, "Error: missing writerSignature", "error message");
			t.end();
		});
	});
});

tape("put without writerAddress", (t) => {
	const db = create.one();
	const schemaKey = "schema/hello";
	const schemaValue = {
		keySchema: "hello",
		valueValidationKey: "",
		keyValidation: ""
	};
	let writerSignature = EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue));
	db.addSchema(schemaKey, schemaValue, writerSignature, writerAddress, (err) => {
		t.error(err);
		const key = "hello";
		const value = "world";

		db.put(key, value, EthCrypto.sign(privateKey, db.createSignHash(key, value)), "", { schemaKey }, (err, node) => {
			console.log("no writerAddress", err);
			t.ok(err, "expected error");
			t.equal(err.message, "Error: missing writerAddress", "error message");
			t.end();
		});
	});
});

tape("put with non-existing schemaKey", (t) => {
	const db = create.one();
	const schemaKey = "schema/hello";
	const schemaValue = {
		keySchema: "hello",
		valueValidationKey: "",
		keyValidation: ""
	};
	let writerSignature = EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue));
	db.addSchema(schemaKey, schemaValue, writerSignature, writerAddress, (err) => {
		t.error(err);
		const key = "hello";
		const value = "world";

		db.put(
			key,
			value,
			EthCrypto.sign(privateKey, db.createSignHash(key, value)),
			writerAddress,
			{ schemaKey: "schema/hel" },
			(err, node) => {
				console.log("non-existing schemaKey", err);
				t.ok(err, "expected error");
				t.equal(err.message, "Error: unable to find this entry for the schemaKey", "error message");
				t.end();
			}
		);
	});
});

tape("put with incorrect schemaKey", (t) => {
	const db = create.one();
	const schemaKey = "schema/hello";
	const schemaValue = {
		keySchema: "hello",
		valueValidationKey: "",
		keyValidation: ""
	};
	let writerSignature = EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue));
	db.addSchema(schemaKey, schemaValue, writerSignature, writerAddress, (err) => {
		t.error(err);
		const key = "hello2";
		const value = "world";

		db.put(key, value, EthCrypto.sign(privateKey, db.createSignHash(key, value)), writerAddress, { schemaKey }, (err, node) => {
			console.log("incorrect schemaKey", err);
			t.ok(err, "expected error");
			t.equal(err.message, "Error: key's space not match. key => " + key + ". schema => " + schemaValue.keySchema, "error message");
			t.end();
		});
	});
});

tape("addSchema with invalid keySchema", (t) => {
	const db = create.one();
	const schemaKey = "schema/hello";
	const schemaValue = {
		keySchema: "hello2",
		valueValidationKey: "",
		keyValidation: ""
	};
	let writerSignature = EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue));
	db.addSchema(schemaKey, schemaValue, writerSignature, writerAddress, (err) => {
		console.log("incorrect keySchema", err);
		t.ok(err, "expected error");
		t.equal(err.message, "Error: key does not have the correct schema structure", "error message");
		t.end();
	});
});

tape("addSchema with invalid schemaValue", (t) => {
	const db = create.one();
	const schemaKey = "schema/hello";
	const schemaValue = "blah";
	let writerSignature = EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue));
	db.addSchema(schemaKey, schemaValue, writerSignature, writerAddress, (err) => {
		console.log("invalid schemaValue", err);
		t.ok(err, "expected error");
		t.equal(err.message, "Error: Invalid schemaValue object", "error message");
		t.end();
	});
});

tape("addSchema with schemaValue missing some properties", (t) => {
	const db = create.one();
	const schemaKey = "schema/hello";
	const schemaValue = {
		keySchema: "hello",
		keyValidation: ""
	};

	let writerSignature = EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue));
	db.addSchema(schemaKey, schemaValue, writerSignature, writerAddress, (err) => {
		console.log("schemaValue missing props", err);
		t.ok(err, "expected error");
		t.equal(err.message, "Error: val is missing keySchema / valueValidationKey / keyValidation property", "error message");
		t.end();
	});
});

tape("basic addSchema", (t) => {
	const db = create.one();
	const schemaKey = "schema/hello";
	const schemaValue = {
		keySchema: "hello",
		valueValidationKey: "",
		keyValidation: ""
	};
	let writerSignature = EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue));
	db.addSchema(schemaKey, schemaValue, writerSignature, writerAddress, (err) => {
		t.error(err);

		const schemaValue2 = {
			keySchema: "hello",
			valueValidationKey: "",
			keyValidation: "somekeyvalidation"
		};
		writerSignature = EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue2));
		db.addSchema(schemaKey, schemaValue2, writerSignature, writerAddress, (err) => {
			t.error(err);
			db.get(schemaKey, (err, node) => {
				t.error(err, "no error");
				t.same(node.key, schemaKey, "same key");
				t.same(node.value, schemaValue, "same value");
				t.same(node.isSchema, true);
				t.same(node.noUpdate, true);
				t.end();
			});
		});
	});
});

tape("basic put/get", (t) => {
	const db = create.one();
	const schemaKey = "schema/hello";
	const schemaValue = {
		keySchema: "hello",
		valueValidationKey: "",
		keyValidation: ""
	};
	let writerSignature = EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue));
	db.addSchema(schemaKey, schemaValue, writerSignature, writerAddress, (err) => {
		t.error(err);
		const key = "hello";
		const value = "world";

		db.put(key, value, EthCrypto.sign(privateKey, db.createSignHash(key, value)), writerAddress, { schemaKey }, (err, node) => {
			t.same(node.key, key);
			t.same(node.value, value);
			t.error(err, "no error");
			db.get(key, (err, node) => {
				t.error(err, "no error");
				t.same(node.key, key, "same key");
				t.same(node.value, value, "same value");
				t.end();
			});
		});
	});
});

tape("get on empty db", (t) => {
	const db = create.one();

	db.get("hello", (err, node) => {
		t.error(err, "no error");
		t.same(node, null, "node is not found");
		t.end();
	});
});

tape("not found", (t) => {
	const db = create.one();
	const schemaKey = "schema/hello";
	const schemaValue = {
		keySchema: "hello",
		valueValidationKey: "",
		keyValidation: ""
	};
	let writerSignature = EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue));
	db.addSchema(schemaKey, schemaValue, writerSignature, writerAddress, (err) => {
		t.error(err);

		const key = "hello";
		const value = "world";

		db.put(key, value, EthCrypto.sign(privateKey, db.createSignHash(key, value)), writerAddress, { schemaKey }, (err, node) => {
			t.error(err, "no error");
			db.get("hej", (err, node) => {
				t.error(err, "no error");
				t.same(node, null, "node is not found");
				t.end();
			});
		});
	});
});

tape("leading / is ignored", (t) => {
	t.plan(8);
	const db = create.one();
	const schemaKey = "schema/hello";
	const schemaValue = {
		keySchema: "hello",
		valueValidationKey: "",
		keyValidation: ""
	};
	let writerSignature = EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue));
	db.addSchema(schemaKey, schemaValue, writerSignature, writerAddress, (err) => {
		t.error(err);

		const key = "/hello";
		const value = "world";

		db.put(key, value, EthCrypto.sign(privateKey, db.createSignHash(key, value)), writerAddress, { schemaKey }, (err, node) => {
			t.error(err, "no error");
			db.get(key, (err, node) => {
				t.error(err, "no error");
				t.same(node.key, "hello", "same key");
				t.same(node.value, value, "same value");
			});
			db.get("/hello", (err, node) => {
				t.error(err, "no error");
				t.same(node.key, "hello", "same key");
				t.same(node.value, value, "same value");
			});
		});
	});
});

tape("multiple put/get", (t) => {
	t.plan(9);

	const db = create.one();
	const schemaKey = "schema/*";
	const schemaValue = {
		keySchema: "*",
		valueValidationKey: "",
		keyValidation: ""
	};
	let writerSignature = EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue));
	db.addSchema(schemaKey, schemaValue, writerSignature, writerAddress, (err) => {
		t.error(err, "no error");

		const key1 = "hello";
		const value1 = "world";
		const key2 = "world";
		const value2 = "hello";

		db.put(key1, value1, EthCrypto.sign(privateKey, db.createSignHash(key1, value1)), writerAddress, { schemaKey }, (err, node) => {
			t.error(err, "no error");
			db.put(key2, value2, EthCrypto.sign(privateKey, db.createSignHash(key2, value2)), writerAddress, { schemaKey }, (err, node) => {
				t.error(err, "no error");
				db.get(key1, (err, node) => {
					t.error(err, "no error");
					t.same(node.key, key1, "same key");
					t.same(node.value, value1, "same value");
				});
				db.get(key2, (err, node) => {
					t.error(err, "no error");
					t.same(node.key, key2, "same key");
					t.same(node.value, value2, "same value");
				});
			});
		});
	});
});

tape("overwrites", (t) => {
	const db = create.one();
	const schemaKey = "schema/hello";
	const schemaValue = {
		keySchema: "hello",
		valueValidationKey: "",
		keyValidation: ""
	};
	let writerSignature = EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue));
	db.addSchema(schemaKey, schemaValue, writerSignature, writerAddress, (err) => {
		t.error(err);

		const key = "hello";
		let value = "world";

		db.put(key, value, EthCrypto.sign(privateKey, db.createSignHash(key, value)), writerAddress, { schemaKey }, (err, node) => {
			t.error(err, "no error");
			db.get(key, (err, node) => {
				t.error(err, "no error");
				t.same(node.key, key, "same key");
				t.same(node.value, value, "same value");
				value = "verden";
				db.put(key, value, EthCrypto.sign(privateKey, db.createSignHash(key, value)), writerAddress, { schemaKey }, (err, node) => {
					t.error(err, "no error");
					db.get(key, (err, node) => {
						t.error(err, "no error");
						t.same(node.key, key, "same key");
						t.same(node.value, value, "same value");
						t.end();
					});
				});
			});
		});
	});
});

tape("put/gets namespaces", (t) => {
	t.plan(9);

	const db = create.one();
	const schemaKey = "schema/hello/world";
	const schemaValue = {
		keySchema: "hello/world",
		valueValidationKey: "",
		keyValidation: ""
	};

	const schemaKey2 = "schema/world";
	const schemaValue2 = {
		keySchema: "world",
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
		const key1 = "hello/world";
		const value1 = "world";

		const key2 = "world";
		const value2 = "hello";

		db.put(key1, value1, EthCrypto.sign(privateKey, db.createSignHash(key1, value1)), writerAddress, { schemaKey }, (err, node) => {
			t.error(err, "no error");
			db.put(
				key2,
				value2,
				EthCrypto.sign(privateKey, db.createSignHash(key2, value2)),
				writerAddress,
				{ schemaKey: schemaKey2 },
				(err, node) => {
					t.error(err, "no error");
					db.get(key1, (err, node) => {
						t.error(err, "no error");
						t.same(node.key, key1, "same key");
						t.same(node.value, value1, "same value");
					});
					db.get(key2, (err, node) => {
						t.error(err, "no error");
						t.same(node.key, key2, "same key");
						t.same(node.value, value2, "same value");
					});
				}
			);
		});
	});
});

tape("put in tree", (t) => {
	t.plan(9);

	const db = create.one();
	const schemaKey = "schema/hello";
	const schemaValue = {
		keySchema: "hello",
		valueValidationKey: "",
		keyValidation: ""
	};

	const schemaKey2 = "schema/hello/world";
	const schemaValue2 = {
		keySchema: "hello/world",
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

		const key1 = "hello";
		const value1 = "a";
		const key2 = "hello/world";
		const value2 = "b";

		db.put(key1, value1, EthCrypto.sign(privateKey, db.createSignHash(key1, value1)), writerAddress, { schemaKey }, (err, node) => {
			t.error(err, "no error");
			db.put(
				key2,
				value2,
				EthCrypto.sign(privateKey, db.createSignHash(key2, value2)),
				writerAddress,
				{ schemaKey: schemaKey2 },
				(err, node) => {
					t.error(err, "no error");
					db.get(key1, (err, node) => {
						t.error(err, "no error");
						t.same(node.key, key1, "same key");
						t.same(node.value, value1, "same value");
					});
					db.get(key2, (err, node) => {
						t.error(err, "no error");
						t.same(node.key, key2, "same key");
						t.same(node.value, value2, "same value");
					});
				}
			);
		});
	});
});

tape("put in tree reverse order", (t) => {
	t.plan(9);

	const db = create.one();
	const schemaKey = "schema/hello/world";
	const schemaValue = {
		keySchema: "hello/world",
		valueValidationKey: "",
		keyValidation: ""
	};

	const schemaKey2 = "schema/hello";
	const schemaValue2 = {
		keySchema: "hello",
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
		const key1 = "hello/world";
		const value1 = "b";
		const key2 = "hello";
		const value2 = "a";

		db.put(key1, value1, EthCrypto.sign(privateKey, db.createSignHash(key1, value1)), writerAddress, { schemaKey }, (err, node) => {
			t.error(err, "no error");
			db.put(
				key2,
				value2,
				EthCrypto.sign(privateKey, db.createSignHash(key2, value2)),
				writerAddress,
				{ schemaKey: schemaKey2 },
				(err, node) => {
					t.error(err, "no error");
					db.get(key2, (err, node) => {
						t.error(err, "no error");
						t.same(node.key, key2, "same key");
						t.same(node.value, value2, "same value");
					});
					db.get(key1, (err, node) => {
						t.error(err, "no error");
						t.same(node.key, key1, "same key");
						t.same(node.value, value1, "same value");
					});
				}
			);
		});
	});
});

tape("multiple put in tree", (t) => {
	t.plan(14);

	const db = create.one();
	const schemaKey = "schema/*/*";
	const schemaValue = {
		keySchema: "*/*",
		valueValidationKey: "",
		keyValidation: ""
	};

	const schemaKey2 = "schema/*";
	const schemaValue2 = {
		keySchema: "*",
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

		const key1 = "hello/world";
		const value1 = "b";
		const key2 = "hello";
		let value2 = "a";
		const key3 = "hello/verden";
		const value3 = "c";

		db.put(key1, value1, EthCrypto.sign(privateKey, db.createSignHash(key1, value1)), writerAddress, { schemaKey }, (err, node) => {
			t.error(err, "no error");
			db.put(
				key2,
				value2,
				EthCrypto.sign(privateKey, db.createSignHash(key2, value2)),
				writerAddress,
				{ schemaKey: schemaKey2 },
				(err, node) => {
					t.error(err, "no error");
					db.put(
						key3,
						value3,
						EthCrypto.sign(privateKey, db.createSignHash(key3, value3)),
						writerAddress,
						{ schemaKey },
						(err, node) => {
							t.error(err, "no error");
							value2 = "d";
							db.put(
								key2,
								value2,
								EthCrypto.sign(privateKey, db.createSignHash(key2, value2)),
								writerAddress,
								{ schemaKey: schemaKey2 },
								(err, node) => {
									t.error(err, "no error");
									db.get(key2, (err, node) => {
										t.error(err, "no error");
										t.same(node.key, key2, "same key");
										t.same(node.value, value2, "same value");
									});
									db.get(key1, (err, node) => {
										t.error(err, "no error");
										t.same(node.key, key1, "same key");
										t.same(node.value, value1, "same value");
									});
									db.get(key3, (err, node) => {
										t.error(err, "no error");
										t.same(node.key, key3, "same key");
										t.same(node.value, value3, "same value");
									});
								}
							);
						}
					);
				}
			);
		});
	});
});

tape("insert 100 values and get them all", (t) => {
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

		const max = 100;
		let i = 0;

		t.plan(3 * max + 1);

		const loop = () => {
			if (i === max) return validate();
			let key = "#" + i;
			let value = "#" + i++;
			db.put(key, value, EthCrypto.sign(privateKey, db.createSignHash(key, value)), writerAddress, { schemaKey }, loop);
		};

		const validate = () => {
			for (let i = 0; i < max; i++) {
				db.get("#" + i, same("#" + i, "#" + i));
			}
		};

		const same = (key, value) => {
			return (err, node) => {
				t.error(err, "no error");
				t.same(node.key, key, "same key");
				t.same(node.value, value, "same value");
			};
		};

		loop();
	});
});

tape("race works", (t) => {
	t.plan(41);

	let missing = 10;
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

		const done = (err) => {
			t.error(err, "no error");
			if (--missing) return;
			for (let i = 0; i < 10; i++) same("#" + i, "#" + i);
		};

		const same = (key, val) => {
			db.get(key, (err, node) => {
				t.error(err, "no error");
				t.same(node.key, key, "same key");
				t.same(node.value, val, "same value");
			});
		};

		for (let i = 0; i < 10; i++) {
			let key = "#" + i;
			let value = "#" + i;
			db.put(key, value, EthCrypto.sign(privateKey, db.createSignHash(key, value)), writerAddress, { schemaKey }, done);
		}
	});
});

tape("version", (t) => {
	const db = create.one();

	db.version((err, version) => {
		t.error(err, "no error");
		t.same(version, Buffer.alloc(0));

		const schemaKey = "schema/*";
		const schemaValue = {
			keySchema: "*",
			valueValidationKey: "",
			keyValidation: ""
		};
		let writerSignature = EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue));
		db.addSchema(schemaKey, schemaValue, writerSignature, writerAddress, (err) => {
			t.error(err);

			const key = "hello";
			const value = "world";
			db.put(key, value, EthCrypto.sign(privateKey, db.createSignHash(key, value)), writerAddress, { schemaKey }, () => {
				db.version((err, version) => {
					t.error(err, "no error");
					const newValue = "verden";
					db.put(
						key,
						newValue,
						EthCrypto.sign(privateKey, db.createSignHash(key, newValue)),
						writerAddress,
						{ schemaKey },
						() => {
							db.checkout(version).get(key, (err, node) => {
								t.error(err, "no error");
								t.same(node.value, value);
								t.end();
							});
						}
					);
				});
			});
		});
	});
});

tape("basic batch", (t) => {
	t.plan(1 + 1 + 3 + 3);

	const db = create.one();
	const schemaKey = "schema/*";
	const schemaValue = {
		keySchema: "*",
		valueValidationKey: "",
		keyValidation: ""
	};
	let writerSignature = EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue));
	db.addSchema(schemaKey, schemaValue, writerSignature, writerAddress, (err) => {
		t.error(err, "no error");

		db.batch(
			[
				{
					type: "put",
					key: "hello",
					value: "world",
					writerSignature: EthCrypto.sign(privateKey, db.createSignHash("hello", "world")),
					writerAddress,
					schemaKey
				},
				{
					type: "put",
					key: "hej",
					value: "verden",
					writerSignature: EthCrypto.sign(privateKey, db.createSignHash("hej", "verden")),
					writerAddress,
					schemaKey
				},
				{
					type: "put",
					key: "hello",
					value: "welt",
					writerSignature: EthCrypto.sign(privateKey, db.createSignHash("hello", "welt")),
					writerAddress,
					schemaKey
				}
			],
			(err) => {
				t.error(err, "no error");
				db.get("hello", (err, node) => {
					t.error(err, "no error");
					t.same(node.key, "hello");
					t.same(node.value, "welt");
				});
				db.get("hej", (err, node) => {
					t.error(err, "no error");
					t.same(node.key, "hej");
					t.same(node.value, "verden");
				});
			}
		);
	});
});

tape("batch with del", (t) => {
	t.plan(1 + 1 + 1 + 3 + 2);

	const db = create.one();
	const schemaKey = "schema/*";
	const schemaValue = {
		keySchema: "*",
		valueValidationKey: "",
		keyValidation: ""
	};
	let writerSignature = EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue));
	db.addSchema(schemaKey, schemaValue, writerSignature, writerAddress, (err) => {
		t.error(err, "no error");

		db.batch(
			[
				{
					type: "put",
					key: "hello",
					value: "world",
					writerSignature: EthCrypto.sign(privateKey, db.createSignHash("hello", "world")),
					writerAddress,
					schemaKey
				},
				{
					type: "put",
					key: "hej",
					value: "verden",
					writerSignature: EthCrypto.sign(privateKey, db.createSignHash("hej", "verden")),
					writerAddress,
					schemaKey
				},
				{
					type: "put",
					key: "hello",
					value: "welt",
					writerSignature: EthCrypto.sign(privateKey, db.createSignHash("hello", "welt")),
					writerAddress,
					schemaKey
				}
			],
			(err) => {
				t.error(err, "no error");
				db.batch(
					[
						{
							type: "put",
							key: "hello",
							value: "verden",
							writerSignature: EthCrypto.sign(privateKey, db.createSignHash("hello", "verden")),
							writerAddress,
							schemaKey
						},
						{
							type: "del",
							key: "hej",
							value: "",
							writerSignature: EthCrypto.sign(privateKey, db.createSignHash("hej", "")),
							writerAddress
						}
					],
					(err) => {
						t.error(err, "no error");
						db.get("hello", (err, node) => {
							t.error(err, "no error");
							t.same(node.key, "hello");
							t.same(node.value, "verden");
						});
						db.get("hej", (err, node) => {
							t.error(err, "no error");
							t.same(node, null);
						});
					}
				);
			}
		);
	});
});

tape("multiple batches", (t) => {
	t.plan(20);

	const db = create.one();
	const schemaKey = "schema/*";
	const schemaValue = {
		keySchema: "*",
		valueValidationKey: "",
		keyValidation: ""
	};
	let writerSignature = EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue));
	db.addSchema(schemaKey, schemaValue, writerSignature, writerAddress, (err) => {
		t.error(err, "no error");

		const same = (key, val) => {
			db.get(key, (err, node) => {
				t.error(err, "no error");
				t.same(node.key, key);
				t.same(node.value, val);
			});
		};

		db.batch(
			[
				{
					type: "put",
					key: "foo",
					value: "foo",
					writerSignature: EthCrypto.sign(privateKey, db.createSignHash("foo", "foo")),
					writerAddress,
					schemaKey
				},
				{
					type: "put",
					key: "bar",
					value: "bar",
					writerSignature: EthCrypto.sign(privateKey, db.createSignHash("bar", "bar")),
					writerAddress,
					schemaKey
				}
			],
			(err, nodes) => {
				t.error(err);
				t.same(2, nodes.length);
				same("foo", "foo");
				same("bar", "bar");
				db.batch(
					[
						{
							type: "put",
							key: "foo",
							value: "foo2",
							writerSignature: EthCrypto.sign(privateKey, db.createSignHash("foo", "foo2")),
							writerAddress,
							schemaKey
						},
						{
							type: "put",
							key: "bar",
							value: "bar2",
							writerSignature: EthCrypto.sign(privateKey, db.createSignHash("bar", "bar2")),
							writerAddress,
							schemaKey
						},
						{
							type: "put",
							key: "baz",
							value: "baz",
							writerSignature: EthCrypto.sign(privateKey, db.createSignHash("baz", "baz")),
							writerAddress,
							schemaKey
						}
					],
					(err, nodes) => {
						t.error(err);
						t.same(3, nodes.length);
						same("foo", "foo2");
						same("bar", "bar2");
						same("baz", "baz");
					}
				);
			}
		);
	});
});

tape("createWriteStream", (t) => {
	t.plan(11);

	const db = create.one();
	const schemaKey = "schema/*";
	const schemaValue = {
		keySchema: "*",
		valueValidationKey: "",
		keyValidation: ""
	};
	let writerSignature = EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue));
	db.addSchema(schemaKey, schemaValue, writerSignature, writerAddress, (err) => {
		t.error(err, "no error");

		const same = (key, val) => {
			db.get(key, (err, node) => {
				t.error(err, "no error");
				t.same(node.key, key);
				t.same(node.value, val);
			});
		};

		const writer = db.createWriteStream();

		writer.write([
			{
				type: "put",
				key: "foo",
				value: "foo",
				writerSignature: EthCrypto.sign(privateKey, db.createSignHash("foo", "foo")),
				writerAddress,
				schemaKey
			},
			{
				type: "put",
				key: "bar",
				value: "bar",
				writerSignature: EthCrypto.sign(privateKey, db.createSignHash("bar", "bar")),
				writerAddress,
				schemaKey
			}
		]);

		writer.write({
			type: "put",
			key: "baz",
			value: "baz",
			writerSignature: EthCrypto.sign(privateKey, db.createSignHash("baz", "baz")),
			writerAddress,
			schemaKey
		});

		writer.end((err) => {
			t.error(err, "no error");
			same("foo", "foo");
			same("bar", "bar");
			same("baz", "baz");
		});
	});
});

/*
tape('createWriteStream pipe', function (t) {
	t.plan(10)
	var db = create.one()
	var writer = db.createWriteStream()
	var index = 0
	var reader = new Readable({
		objectMode: true,
		read: function (size) {
			var value = (index < 1000) ? {
				type: 'put',
				key: 'foo' + index,
				value: index++
			} : null
			this.push(value)
		}
	})
	reader.pipe(writer)
	writer.on('finish', function (err) {
		t.error(err, 'no error')
		same('foo1', '1')
		same('foo50', '50')
		same('foo999', '999')
	})

	function same (key, val) {
		db.get(key, function (err, node) {
			t.error(err, 'no error')
			t.same(node.key, key)
			t.same(node.value, val)
		})
	}
})
*/

tape("create with precreated keypair", (t) => {
	const crypto = require("hypercore/lib/crypto");
	const keyPair = crypto.keyPair();

	const db = create.one(keyPair.publicKey, { secretKey: keyPair.secretKey });
	const schemaKey = "schema/*";
	const schemaValue = {
		keySchema: "*",
		valueValidationKey: "",
		keyValidation: ""
	};
	const writerSignature = EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue));
	db.addSchema(schemaKey, schemaValue, writerSignature, writerAddress, (err) => {
		t.error(err, "no error");

		const key = "hello";
		const value = "world";

		db.put(key, value, EthCrypto.sign(privateKey, db.createSignHash(key, value)), writerAddress, { schemaKey }, (err, node) => {
			t.same(node.value, value);
			t.error(err, "no error");
			t.same(db.key, keyPair.publicKey, "pubkey matches");
			db.source._storage.secretKey.read(0, keyPair.secretKey.length, (err, secretKey) => {
				t.error(err, "no error");
				t.same(secretKey, keyPair.secretKey, "secret key is stored");
			});
			db.get(key, (err, node) => {
				t.error(err, "no error");
				t.same(node.value, value, "same value");
				t.end();
			});
		});
	});
});

tape("can insert falsy values", (t) => {
	t.plan(1 + 2 * 2 + 4 + 1);

	const db = create.one(null, { valueEncoding: "json" });
	const schemaKey = "schema/*";
	const schemaValue = {
		keySchema: "*",
		valueValidationKey: "",
		keyValidation: ""
	};
	const writerSignature = EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue));
	db.addSchema(schemaKey, schemaValue, writerSignature, writerAddress, (err) => {
		t.error(err, "no error");

		const key1 = "hello";
		const value1 = 0;
		const key2 = "world";
		const value2 = false;

		db.put(key1, value1, EthCrypto.sign(privateKey, db.createSignHash(key1, value1)), writerAddress, { schemaKey }, () => {
			db.put(key2, value2, EthCrypto.sign(privateKey, db.createSignHash(key2, value2)), writerAddress, { schemaKey }, () => {
				db.get(key1, (err, node) => {
					t.error(err, "no error");
					t.same(node && node.value, value1);
				});
				db.get(key2, (err, node) => {
					t.error(err, "no error");
					t.same(node && node.value, value2);
				});

				const ite = db.iterator();
				const result = {};

				ite.next(function loop(err, node) {
					t.error(err, "no error");

					if (!node) {
						t.same(result, { ["hello"]: 0, ["world"]: false });
						return;
					}

					if (node.key !== schemaKey) result[node.key] = node.value;
					ite.next(loop);
				});
			});
		});
	});
});

tape("can put/get a null value", (t) => {
	t.plan(4);

	const db = create.one(null, { valueEncoding: "json" });
	const schemaKey = "schema/*";
	const schemaValue = {
		keySchema: "*",
		valueValidationKey: "",
		keyValidation: ""
	};
	const writerSignature = EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue));
	db.addSchema(schemaKey, schemaValue, writerSignature, writerAddress, (err) => {
		t.error(err, "no error");

		const key = "some key";
		const value = null;
		db.put(key, value, EthCrypto.sign(privateKey, db.createSignHash(key, value)), writerAddress, { schemaKey }, (err) => {
			t.error(err, "no error");
			db.get(key, (err, node) => {
				t.error(err, "no error");
				t.same(node.value, null);
			});
		});
	});
});

tape("does not reinsert if ifNotExists is true in put", (t) => {
	t.plan(5);

	const db = create.one(null, { valueEncoding: "json" });
	const schemaKey = "schema/*";
	const schemaValue = {
		keySchema: "*",
		valueValidationKey: "",
		keyValidation: ""
	};
	const writerSignature = EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue));
	db.addSchema(schemaKey, schemaValue, writerSignature, writerAddress, (err) => {
		t.error(err, "no error");

		const key = "some key";
		const value = "hello";
		db.put(key, value, EthCrypto.sign(privateKey, db.createSignHash(key, value)), writerAddress, { schemaKey }, (err) => {
			t.error(err, "no error");
			const value2 = "goodbye";
			db.put(
				key,
				value2,
				EthCrypto.sign(privateKey, db.createSignHash(key, value2)),
				writerAddress,
				{ schemaKey, ifNotExists: true },
				(err) => {
					t.error(err, "no error");
					db.get(key, (err, node) => {
						t.error(err, "no error");
						t.same(node.value, value);
					});
				}
			);
		});
	});
});

tape("normal insertions work with ifNotExists", (t) => {
	t.plan(6);

	const db = create.one();
	const schemaKey = "schema/*";
	const schemaValue = {
		keySchema: "*",
		valueValidationKey: "",
		keyValidation: ""
	};
	const writerSignature = EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue));
	db.addSchema(schemaKey, schemaValue, writerSignature, writerAddress, (err) => {
		t.error(err, "no error");

		const done = (err) => {
			t.error(err, "no error");
			db.get("some key", (err, node) => {
				t.error(err, "no error");
				t.same(node.value, "hello");
				db.get("another key", (err, node) => {
					t.error(err, "no error");
					t.same(node.value, "something else");
				});
			});
		};

		run(
			(cb) =>
				db.put(
					"some key",
					"hello",
					EthCrypto.sign(privateKey, db.createSignHash("some key", "hello")),
					writerAddress,
					{ schemaKey, ifNotExists: true },
					cb
				),
			(cb) =>
				db.put(
					"some key",
					"goodbye",
					EthCrypto.sign(privateKey, db.createSignHash("some key", "goodbye")),
					writerAddress,
					{ schemaKey, ifNotExists: true },
					cb
				),
			(cb) =>
				db.put(
					"another key",
					"something else",
					EthCrypto.sign(privateKey, db.createSignHash("another key", "something else")),
					writerAddress,
					{ schemaKey, ifNotExists: true },
					cb
				),
			done
		);
	});
});

tape("put with ifNotExists does not reinsert with conflict", (t) => {
	t.plan(7);

	create.two((db1, db2, replicate) => {
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

		db1.batch(batchList1, (err) => {
			t.error(err, "no error");
			db2.batch(batchList2, (err) => {
				t.error(err, "no error");

				const done = (err) => {
					t.error(err, "no error");
					db1.put(
						"1",
						"1c",
						EthCrypto.sign(privateKey, db1.createSignHash("1", "1c")),
						writerAddress,
						{ schemaKey, ifNotExists: true },
						(err) => {
							t.error(err, "no error");
							db1.get("1", (err, nodes) => {
								t.error(err, "no error");
								t.same(nodes.length, 2);
								var vals = nodes.map((n) => {
									return n.value;
								});
								t.same(vals, ["1b", "1a"]);
							});
						}
					);
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

tape("opts is not mutated", (t) => {
	const opts = { firstNode: true };
	create.one(opts);
	t.deepEqual(opts, { firstNode: true });
	t.end();
});

tape("put with pointer", (t) => {
	const db = create.one();

	const schemaKey = "schema/%writerAddress%/profilePicture";
	const schemaValue = {
		keySchema: "%writerAddress%/profilePicture",
		valueValidationKey: "",
		keyValidation: ""
	};
	const schemaKey2 = "schema/settings/profilePicture/%writerAddress%";
	const schemaValue2 = {
		keySchema: "settings/profilePicture/%writerAddress%",
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

		const key = writerAddress + "/profilePicture";
		const value = "https://upload.wikimedia.org/wikipedia/en/thumb/e/e0/Iron_Man_bleeding_edge.jpg/250px-Iron_Man_bleeding_edge.jpg";
		const writerSignature = EthCrypto.sign(privateKey, db.createSignHash(key, value));
		const pointerKey = "settings/profilePicture/" + writerAddress;
		const opts = {
			schemaKey,
			pointerKey,
			pointerSchemaKey: schemaKey2
		};
		db.put(key, value, writerSignature, writerAddress, opts, (err, node) => {
			t.error(err, "no error");
			db.get(key, (err, node) => {
				t.error(err, "no error");
				t.same(node.key, key, "same key");
				t.same(node.value, value, "same value");
				t.same(node.pointerKey, pointerKey, "same pointerKey");
				t.same(node.pointer, false, "correct pointer");
				db.get(pointerKey, (err, node) => {
					t.error(err, "no error");
					t.same(node.key, pointerKey, "same key");
					t.same(node.value, key, "same value");
					t.same(node.pointer, true, "correct pointer");
					t.end();
				});
			});
		});
	});
});
