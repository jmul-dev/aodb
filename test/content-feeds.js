const tape = require("tape");
const create = require("./helpers/create");
const replicate = require("./helpers/replicate");
const EthCrypto = require("eth-crypto");
const { privateKey, publicKey: writerAddress } = EthCrypto.createIdentity();

tape("basic content feed", (t) => {
	const db = create.one(null, { contentFeed: true, valueEncoding: "json" });

	db.ready((err) => {
		t.error(err, "no error");
		db.localContent.append("lots of data");

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
				const key = "hello";
				const value = { start: 0, end: 1 };
				db.put(key, value, EthCrypto.sign(privateKey, db.createSignHash(key, value)), writerAddress, { schemaKey }, (err) => {
					t.error(err, "no error");
					db.get("hello", (err, node) => {
						t.error(err, "no error");
						t.ok(db.localContent === db.contentFeeds[node.feed]);
						db.contentFeeds[node.feed].get(node.value.start, (err, buf) => {
							t.error(err, "no error");
							t.same(buf, Buffer.from("lots of data"));
							t.end();
						});
					});
				});
			}
		);
	});
});

tape("replicating content feeds", (t) => {
	const db = create.one(null, { contentFeed: true });
	const schemaKey = "schema/*";
	const schemaValue = {
		keySchema: "*",
		valueValidationKey: "",
		keyValidation: ""
	};
	db.addSchema(schemaKey, schemaValue, EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue)), writerAddress, (err) => {
		t.error(err, "no error");

		db.put("hello", "world", EthCrypto.sign(privateKey, db.createSignHash("hello", "world")), writerAddress, { schemaKey }, (err) => {
			const clone = create.one(db.key, { contentFeed: true });
			db.localContent.append("data", () => {
				replicate(db, clone, () => {
					clone.get("hello", (err, node) => {
						t.error(err, "no error");
						t.same(node.value, "world");
						clone.contentFeeds[node.feed].get(0, (err, buf) => {
							t.error(err, "no error");
							t.same(buf, Buffer.from("data"));
							t.end();
						});
					});
				});
			});
		});
	});
});
