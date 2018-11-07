const tape = require("tape");
const cmp = require("compare");
const collect = require("stream-collector");
const create = require("./helpers/create");
const replicate = require("./helpers/replicate");
const put = require("./helpers/put");
const EthCrypto = require("eth-crypto");
const { privateKey, publicKey: writerAddress } = EthCrypto.createIdentity();

tape("empty diff", (t) => {
	const db = create.one();

	const rs = db.createDiffStream(null, "a");
	collect(rs, (err, actual) => {
		t.error(err, "no error");
		t.deepEqual(actual, [], "diff as expected");
		t.end();
	});
});

tape("implicit checkout", (t) => {
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
			const rs = db.createDiffStream(null, "a");
			collect(rs, (err, actual) => {
				t.error(err, "no error");
				t.equals(actual.length, 1);
				// t.equals(actual[0].type, 'put')
				t.equals(actual[0].left.key, "a");
				t.equals(actual[0].left.value, "2");
				t.equals(actual[0].right, null);
				t.end();
			});
		});
	});
});

tape("new value", (t) => {
	const db = create.one();
	const schemaKey = "schema/*";
	const schemaValue = {
		keySchema: "*",
		valueValidationKey: "",
		keyValidation: ""
	};
	db.addSchema(schemaKey, schemaValue, EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue)), writerAddress, (err) => {
		t.error(err, "no error");

		db.put("a", "1", EthCrypto.sign(privateKey, db.createSignHash("a", "1")), writerAddress, { schemaKey }, (err) => {
			t.error(err, "no error");
			db.put("a", "2", EthCrypto.sign(privateKey, db.createSignHash("a", "2")), writerAddress, { schemaKey }, (err) => {
				t.error(err, "no error");
				const rs = db.createDiffStream(null, "a");
				collect(rs, (err, actual) => {
					t.error(err, "no error");
					t.equals(actual.length, 1);
					// t.equals(actual[0].type, 'put')
					t.equals(actual[0].left.key, "a");
					t.equals(actual[0].left.value, "2");
					t.equals(actual[0].right, null);
					t.end();
				});
			});
		});
	});
});

tape("two new nodes", (t) => {
	const db = create.one();
	const schemaKey = "schema/*/*";
	const schemaValue = {
		keySchema: "*/*",
		valueValidationKey: "",
		keyValidation: ""
	};
	db.addSchema(schemaKey, schemaValue, EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue)), writerAddress, (err) => {
		t.error(err, "no error");

		db.put("a/foo", "quux", EthCrypto.sign(privateKey, db.createSignHash("a/foo", "quux")), writerAddress, { schemaKey }, (err) => {
			t.error(err, "no error");
			db.put("a/bar", "baz", EthCrypto.sign(privateKey, db.createSignHash("a/bar", "baz")), writerAddress, { schemaKey }, (err) => {
				t.error(err, "no error");
				const rs = db.createDiffStream(null, "a");
				collect(rs, (err, actual) => {
					t.error(err, "no error");
					actual.sort(sort);
					t.equals(actual.length, 2);
					// t.equals(actual[0].type, 'put')
					t.equals(actual[0].left.key, "a/bar");
					t.equals(actual[0].left.value, "baz");
					t.equals(actual[0].right, null);
					// t.equals(actual[1].type, 'put')
					t.equals(actual[1].left.key, "a/foo");
					t.equals(actual[1].left.value, "quux");
					t.equals(actual[1].right, null);
					t.end();
				});
			});
		});
	});
});

tape("checkout === head", (t) => {
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
			const rs = db.createDiffStream(db, "a");
			collect(rs, (err, actual) => {
				t.error(err, "no error");
				t.equals(actual.length, 0);
				t.end();
			});
		});
	});
});

tape("new value, twice", (t) => {
	const db = create.one();
	const snap = db.snapshot();

	const schemaKey = "schema/*";
	const schemaValue = {
		keySchema: "*",
		valueValidationKey: "",
		keyValidation: ""
	};
	db.addSchema(schemaKey, schemaValue, EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue)), writerAddress, (err) => {
		t.error(err, "no error");

		db.put("a", "1", EthCrypto.sign(privateKey, db.createSignHash("a", "1")), writerAddress, { schemaKey }, (err) => {
			t.error(err, "no error");
			db.put("a", "2", EthCrypto.sign(privateKey, db.createSignHash("a", "2")), writerAddress, { schemaKey }, (err) => {
				t.error(err, "no error");
				const rs = db.createDiffStream(snap, "a");
				collect(rs, (err, actual) => {
					t.error(err, "no error");
					t.equals(actual.length, 1);
					t.equals(actual[0].left.key, "a");
					t.equals(actual[0].left.value, "2");
					t.equals(actual[0].right, null);
					t.end();
				});
			});
		});
	});
});

tape("untracked value", (t) => {
	const db = create.one();
	const schemaKey = "schema/*";
	const schemaValue = {
		keySchema: "*",
		valueValidationKey: "",
		keyValidation: ""
	};
	db.addSchema(schemaKey, schemaValue, EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue)), writerAddress, (err) => {
		t.error(err, "no error");

		db.put("a", "1", EthCrypto.sign(privateKey, db.createSignHash("a", "1")), writerAddress, { schemaKey }, (err) => {
			t.error(err, "no error");
			const snap = db.snapshot();
			db.put("a", "2", EthCrypto.sign(privateKey, db.createSignHash("a", "2")), writerAddress, { schemaKey }, (err) => {
				t.error(err, "no error");
				db.put("b", "17", EthCrypto.sign(privateKey, db.createSignHash("b", "17")), writerAddress, { schemaKey }, (err) => {
					t.error(err, "no error");
					const rs = db.createDiffStream(snap, "a");
					collect(rs, (err, actual) => {
						t.error(err, "no error");
						t.equals(actual.length, 1);
						t.equals(actual[0].left.key, "a");
						t.equals(actual[0].left.value, "2");
						t.equals(actual[0].right.key, "a");
						t.equals(actual[0].right.value, "1");
						t.end();
					});
				});
			});
		});
	});
});

tape("diff root", (t) => {
	const db = create.one();
	const schemaKey = "schema/*";
	const schemaValue = {
		keySchema: "*",
		valueValidationKey: "",
		keyValidation: ""
	};
	db.addSchema(schemaKey, schemaValue, EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue)), writerAddress, (err) => {
		t.error(err, "no error");

		db.put("a", "1", EthCrypto.sign(privateKey, db.createSignHash("a", "1")), writerAddress, { schemaKey }, (err) => {
			t.error(err, "no error");
			const snap = db.snapshot();
			db.put("a", "2", EthCrypto.sign(privateKey, db.createSignHash("a", "2")), writerAddress, { schemaKey }, (err) => {
				t.error(err, "no error");
				db.put("b", "17", EthCrypto.sign(privateKey, db.createSignHash("b", "17")), writerAddress, { schemaKey }, (err) => {
					t.error(err, "no error");
					const rs = db.createDiffStream(snap);
					collect(rs, (err, actual) => {
						t.error(err, "no error");
						actual.sort(sort);
						t.equals(actual.length, 2);
						t.equals(actual[0].left.key, "a");
						t.equals(actual[0].left.value, "2");
						t.equals(actual[0].right.key, "a");
						t.equals(actual[0].right.value, "1");
						t.equals(actual[1].left.key, "b");
						t.equals(actual[1].left.value, "17");
						t.equals(actual[1].right, null);
						t.end();
					});
				});
			});
		});
	});
});

tape("updated value", (t) => {
	const db = create.one();
	const schemaKey = "schema/*/*/*";
	const schemaValue = {
		keySchema: "*/*/*",
		valueValidationKey: "",
		keyValidation: ""
	};
	db.addSchema(schemaKey, schemaValue, EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue)), writerAddress, (err) => {
		t.error(err, "no error");

		db.put("a/d/r", "1", EthCrypto.sign(privateKey, db.createSignHash("a/d/r", "1")), writerAddress, { schemaKey }, (err) => {
			t.error(err, "no error");
			const snap = db.snapshot();
			db.put("a/d/r", "3", EthCrypto.sign(privateKey, db.createSignHash("a/d/r", "3")), writerAddress, { schemaKey }, (err) => {
				t.error(err, "no error");
				const rs = db.createDiffStream(snap, "a");
				collect(rs, (err, actual) => {
					t.error(err, "no error");
					t.equals(actual.length, 1);
					t.equals(actual[0].left.key, "a/d/r");
					t.equals(actual[0].left.value, "3");
					t.equals(actual[0].right.key, "a/d/r");
					t.equals(actual[0].right.value, "1");
					t.end();
				});
			});
		});
	});
});

tape("basic with 2 feeds", (t) => {
	create.two((a, b) => {
		const validate = () => {
			const rs = b.createDiffStream(null, "a", { reduce: (a, b) => a });
			collect(rs, (err, actual) => {
				t.error(err, "no error");
				t.equals(actual.length, 1);
				t.equals(actual[0].left.key, "a");
				t.equals(actual[0].left.value, "a");
				t.end();
			});
		};

		const schemaKey = "schema/*";
		const schemaValue = {
			keySchema: "*",
			valueValidationKey: "",
			keyValidation: ""
		};
		a.addSchema(schemaKey, schemaValue, EthCrypto.sign(privateKey, a.createSignHash(schemaKey, schemaValue)), writerAddress, (err) => {
			t.error(err, "no error");

			a.put("a", "a", EthCrypto.sign(privateKey, a.createSignHash("a", "a")), writerAddress, { schemaKey }, (err) => {
				replicate(a, b, validate);
			});
		});
	});
});

tape("two feeds /w competing for a value", (t) => {
	create.two((a, b) => {
		const schemaKey = "schema/*";
		const schemaValue = {
			keySchema: "*",
			valueValidationKey: "",
			keyValidation: ""
		};
		const validate = () => {
			const rs = b.createDiffStream(null, "a");
			collect(rs, (err, actual) => {
				t.error(err, "no error");
				t.equals(actual.length, 1);
				actual[0].left.sort(sortByValue);
				t.equals(actual[0].left[0].key, "a");
				t.equals(actual[0].left[0].value, "a");
				t.equals(actual[0].left[1].key, "a");
				t.equals(actual[0].left[1].value, "b");
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
						b.put("a", "b", EthCrypto.sign(privateKey, b.createSignHash("a", "b")), writerAddress, { schemaKey }, (err) => {
							replicate(a, b, validate);
						});
					});
				}
			);
		});
	});
});

tape("small diff on big db", (t) => {
	const db = create.one();
	const schemaKey = "schema/*";
	const schemaValue = {
		keySchema: "*",
		valueValidationKey: "",
		keyValidation: ""
	};
	db.addSchema(schemaKey, schemaValue, EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue)), writerAddress, (err) => {
		t.error(err, "no error");

		//module.exports = function (db, privateKey, writerAddress, list, opts, cb) {

		put(db, privateKey, writerAddress, range(1000), { schemaKey }, (err) => {
			t.error(err, "no error");
			const snap = db.snapshot();
			db.put("42", "42*", EthCrypto.sign(privateKey, db.createSignHash("42", "42*")), writerAddress, { schemaKey }, (err) => {
				t.error(err, "no error");
				const rs = db.createDiffStream(snap);
				collect(rs, (err, actual) => {
					t.error(err, "no error");
					t.equals(actual.length, 1);
					t.equals(actual[0].left.key, "42");
					t.equals(actual[0].left.value, "42*");
					t.equals(actual[0].right.key, "42");
					t.equals(actual[0].right.value, "42");
					t.end();
				});
			});
		});
	});
});

const range = (n) => {
	return Array(n)
		.join(".")
		.split(".")
		.map((_, i) => "" + i);
};

const sortByValue = (a, b) => {
	return cmp(a.value, b.value);
};

const sort = (a, b) => {
	const ak = (a.left || a.right).key;
	const bk = (b.left || b.right).key;
	return cmp(ak, bk);
};
