const tape = require("tape");
const create = require("./helpers/create");
const run = require("./helpers/run");
const aodb = require("..");
const messages = require("../lib/messages");
const EthCrypto = require("eth-crypto");
const { privateKey, publicKey: writerAddress } = EthCrypto.createIdentity();

tape("feed with corrupted inflate generates error", (t) => {
	create.three((a, b, c) => {
		const schemaKey = "schema/*";
		const schemaValue = {
			keySchema: "*",
			valueValidationKey: "",
			keyValidation: ""
		};
		a.addSchema(schemaKey, schemaValue, EthCrypto.sign(privateKey, a.createSignHash(schemaKey, schemaValue)), writerAddress, (err) => {
			t.error(err, "no error");

			let corrupted;

			const done = (err) => {
				t.error(err, "no error");
				t.end();
			};

			const testUncorrupted = (cb) => {
				t.equal(a._writers.length, 3, "uncorrupted length");
				cb();
			};

			const corruptInflateRecord = (cb) => {
				const index = 5;
				a.source.get(index, (err, data) => {
					t.error(err, "no error");
					const val = messages.Entry.decode(data);
					val.inflate = 1; // Introduce corruption
					val.deleted = undefined; // To keep the same size
					const corruptData = messages.Entry.encode(val);
					const storage = a.source._storage;
					storage.dataOffset(index, [], (err, offset, size) => {
						t.error(err, "no error");
						storage.data.write(offset, corruptData, cb);
					});
				});
			};

			const openCorruptedDb = (cb) => {
				corrupted = new aodb(reuseStorage(a));
				corrupted.ready((err) => {
					console.log("corrupted", err);
					t.ok(err, "expected error");
					t.equal(err.message, "Missing feed mappings", "error message");
					t.equal(corrupted._writers.length, 2, "corrupted length");
					cb();
				});
			};

			run(
				(cb) => a.put("foo", "bar", EthCrypto.sign(privateKey, a.createSignHash("foo", "bar")), writerAddress, { schemaKey }, cb),
				testUncorrupted,
				corruptInflateRecord,
				openCorruptedDb,
				done
			);
		});
	});
});

const reuseStorage = (db) => {
	return (name) => {
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
