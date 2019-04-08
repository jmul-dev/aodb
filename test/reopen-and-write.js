const tape = require("tape");
const create = require("./helpers/create");
const run = require("./helpers/run");
const aodb = require("..");
const messages = require("../lib/messages");
const EthCrypto = require("eth-crypto");
const { privateKey, publicKey: writerAddress } = EthCrypto.createIdentity();

const schemaKey = "schema/*";
const schemaValue = {
	keySchema: "*",
	valueValidationKey: "",
	keyValidation: ""
};

const reuseStorage = (db) => {
	return function(name) {
		let match = name.match(/^source\/(.*)/);
		if (match) {
			name = match[1];
			if (name === "secret_key") return db.source._storage.secretKey;
			return db.source._storage[name];
		}
		match = name.match(/^peers\/([0-9a-f]+)\/(.*)/);
		if (match) {
			const hex = match[1];
			name = match[2];
			const peerWriter = db._writers.find((writer) => {
				return writer && writer._feed.discoveryKey.toString("hex") === hex;
			});
			if (!peerWriter) throw new Error("mismatch");
			const feed = peerWriter._feed;
			if (name === "secret_key") return feed._storage.secretKey;
			return feed._storage[name];
		} else {
			throw new Error("mismatch");
		}
	};
};

tape("3 writers, re-open and write, re-open again", (t) => {
	create.three((a, b, c) => {
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

							let reopened;

							const done = (err) => {
								t.error(err, "no error");
								t.end();
							};

							const testUncorrupted = (cb) => {
								t.equal(a._writers.length, 3, "correct number of writers");
								cb();
							};

							const reopenDb = (cb) => {
								reopened = new aodb(reuseStorage(a), { valueEncoding: "json" });
								reopened.ready((err) => {
									t.error(err, "no error");
									cb();
								});
							};

							const testInflateValue = (cb) => {
								t.equals(reopened.source.length, 7, "correct length");
								reopened.source.get(4, (err, data) => {
									t.error(err, "no error");
									var val = messages.Entry.decode(data);
									t.equal(val.inflate, 2, "correct inflate for new entry");
									cb();
								});
							};

							run(
								(cb) =>
									a.put(
										"foo",
										"bar",
										EthCrypto.sign(privateKey, a.createSignHash("foo", "bar")),
										writerAddress,
										{ schemaKey },
										cb
									),
								testUncorrupted,
								reopenDb,
								(cb) =>
									reopened.put(
										"foo2",
										"bar2",
										EthCrypto.sign(privateKey, reopened.createSignHash("foo2", "bar2")),
										writerAddress,
										{ schemaKey },
										cb
									),
								reopenDb,
								testInflateValue,
								done
							);
						}
					);
				}
			);
		});
	});
});
