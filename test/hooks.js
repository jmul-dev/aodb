const tape = require("tape");
const create = require("./helpers/create");
const EthCrypto = require("eth-crypto");
const { privateKey, publicKey: writerAddress } = EthCrypto.createIdentity();

const schemaKey = "schema/*";
const schemaValue = {
	keySchema: "*",
	valueValidationKey: "",
	keyValidation: ""
};

tape("onlookup hook", (t) => {
	const db = create.one();
	db.addSchema(schemaKey, schemaValue, EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue)), writerAddress, (err) => {
		t.error(err, "no error");

		const batch = [];
		const path = [];

		for (let i = 0; i < 200; i++) {
			batch.push({
				type: "put",
				key: i,
				value: i,
				writerSignature: EthCrypto.sign(privateKey, db.createSignHash(i, i)),
				writerAddress,
				schemaKey
			});
		}

		db.batch(batch, (err) => {
			t.error(err, "no error");

			const inTrie = (node, ptr) => {
				return node.trie.some((bucket) => {
					if (!bucket) return false;
					return bucket.some((values) => {
						if (!values) return false;
						return values.some((val) => {
							return val.feed === ptr.feed && val.seq === ptr.seq;
						});
					});
				});
			};

			const onlookup = (ptr) => {
				path.push(ptr);
			};

			db.get(0, { onlookup }, (err, node) => {
				t.error(err, "no error");
				db._getAllPointers(path, false, (err, nodes) => {
					t.error(err, "no error");
					t.same(nodes[0].seq, db.feeds[0].length - 1, "first is head");
					for (let i = 1; i < nodes.length; i++) {
						t.ok(inTrie(nodes[i - 1], nodes[i]), "in trie");
					}
					t.same(nodes[nodes.length - 1].seq, node.seq, "last node is the found one");
					t.end();
				});
			});
		});
	});
});
