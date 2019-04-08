const tape = require("tape");
const cmp = require("compare");
const create = require("./helpers/create");
const put = require("./helpers/put");
const run = require("./helpers/run");
const hash = require("../lib/hash");
const EthCrypto = require("eth-crypto");
const { privateKey, publicKey: writerAddress } = EthCrypto.createIdentity();

const sortByHash = (a, b) => {
	const ha = hash(typeof a === "string" ? a : a.key).join("");
	const hb = hash(typeof b === "string" ? b : b.key).join("");
	return cmp(ha, hb);
};

const reverseSortByHash = (a, b) => {
	return -1 * sortByHash(a, b);
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

const testSingleFeedWithKeys = (t, keys, opts, cb) => {
	if (typeof opts === "function") return testSingleFeedWithKeys(t, keys, null, opts);
	opts = opts || {};

	const sortFunc = opts.sort || sortByHash;

	t.comment("with single feed");
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
		put(db, privateKey, writerAddress, keys, {}, (err) => {
			t.error(err, "no error");

			testIteratorOrder(t, db.iterator(opts), keys, sortFunc, cb);
		});
	});
};

const testTwoFeedsWithKeys = (t, keys, opts, cb) => {
	if (typeof opts === "function") return testTwoFeedsWithKeys(t, keys, null, opts);
	opts = opts || {};

	const sortFunc = opts.sort || sortByHash;

	t.comment("with values split across two feeds");
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
				writerSignature: EthCrypto.sign(privateKey, db2.createSignHash(schemaKey2, schemaValue2)),
				writerAddress: writerAddress
			},
			{
				type: "add-schema",
				key: schemaKey3,
				value: schemaValue3,
				writerSignature: EthCrypto.sign(privateKey, db1.createSignHash(schemaKey3, schemaValue3)),
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
				},
				{
					type: "add-schema",
					key: schemaKey3,
					value: schemaValue3,
					writerSignature: EthCrypto.sign(privateKey, db2.createSignHash(schemaKey3, schemaValue3)),
					writerAddress: writerAddress
				}
			];

			db2.batch(batchList2, (err) => {
				t.error(err, "no error");

				const half = Math.floor(keys.length / 2);
				const done = () => {
					if (!cb) t.end();
					else cb();
				};
				run(
					(cb) => put(db1, privateKey, writerAddress, keys.slice(0, half), {}, cb),
					(cb) => put(db2, privateKey, writerAddress, keys.slice(half), {}, cb),
					(cb) => replicate(cb),
					(cb) => testIteratorOrder(t, db1.iterator(opts), keys, sortFunc, cb),
					(cb) => testIteratorOrder(t, db2.iterator(opts), keys, sortFunc, cb),
					done
				);
			});
		});
	});
};

const testIteratorOrder = (t, iterator, expected, sortFunc, done) => {
	const sorted = expected.slice(0).sort(sortFunc);
	const onEach = (err, node) => {
		t.error(err, "no error");
		if (node.length) node = node[0];
		if (!node.isSchema && !node.isWildStringSchema) {
			const key = node.key;
			const expectedNode = sorted.shift();
			t.same(key, expectedNode.key);
		}
	};
	const onDone = () => {
		t.same(sorted.length, 0);
		if (done === undefined) t.end();
		else done();
	};
	each(iterator, onEach, onDone);
};

const each = (ite, cb, done) => {
	ite.next(function loop(err, node) {
		if (err) return cb(err);
		if (!node) return done();
		cb(null, node);
		ite.next(loop);
	});
};

const cases = {
	simple: [{ key: "a", schemaKey }, { key: "b", schemaKey }, { key: "c", schemaKey }],
	"mixed depth from root": [
		{ key: "a/a", schemaKey: schemaKey2 },
		{ key: "a/b", schemaKey: schemaKey2 },
		{ key: "a/c", schemaKey: schemaKey2 },
		{ key: "b", schemaKey },
		{ key: "c", schemaKey }
	],
	"3 paths deep": [
		{ key: "a", schemaKey },
		{ key: "a/a", schemaKey: schemaKey2 },
		{ key: "a/b", schemaKey: schemaKey2 },
		{ key: "a/c", schemaKey: schemaKey2 },
		{ key: "a/a/a", schemaKey: schemaKey3 },
		{ key: "a/a/b", schemaKey: schemaKey3 },
		{ key: "a/a/c", schemaKey: schemaKey3 }
	]
};

Object.keys(cases).forEach((key) => {
	tape("iterator is hash order sorted (" + key + ")", (t) => {
		const keysToTest = cases[key];
		run(
			(cb) => testSingleFeedWithKeys(t, keysToTest, cb),
			(cb) => testTwoFeedsWithKeys(t, keysToTest, cb),
			(cb) => testSingleFeedWithKeys(t, keysToTest, { reverse: true, sort: reverseSortByHash }, cb),
			(cb) => testTwoFeedsWithKeys(t, keysToTest, { reverse: true, sort: reverseSortByHash }, cb),
			(cb) => t.end()
		);
	});
});

tape("fully visit a folder before visiting the next one", (t) => {
	t.plan(19);
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
		put(
			db,
			privateKey,
			writerAddress,
			[
				{ key: "a", schemaKey },
				{ key: "a/b", schemaKey: schemaKey2 },
				{ key: "a/b/c", schemaKey: schemaKey3 },
				{ key: "b/c", schemaKey: schemaKey2 },
				{ key: "b/c/d", schemaKey: schemaKey3 }
			],
			{},
			(err) => {
				t.error(err, "no error");
				const ite = db.iterator();

				ite.next(function loop(err, val) {
					t.error(err, "no error");
					if (!val) return t.end();
					if (val.isSchema || val.isWildStringSchema) {
						ite.next(loop);
					}
					if (val.key[0] === "b") {
						t.same(val.key, "b/c");
						ite.next((err, val) => {
							t.error(err, "no error");
							t.same(val.key, "b/c/d");
							ite.next(loop);
						});
					} else if (val.key[0] === "a") {
						t.same(val.key, "a");
						ite.next((err, val) => {
							t.error(err, "no error");
							t.same(val.key, "a/b");
							ite.next((err, val) => {
								t.error(err, "no error");
								t.same(val.key, "a/b/c");
								ite.next(loop);
							});
						});
					}
				});
			}
		);
	});
});
