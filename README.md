# aodb

aodb is a fork of [HyperDB](https://github.com/mafintosh/hyperdb), a distributed scalable database with signature authentication and node validation on key writes.

```
npm install aodb
```

Read [ARCHITECTURE.md](ARCHITECTURE.md) for details on how hyperdb works.

## Usage

``` js
const aodb = require("aodb");
const EthCrypto = require("eth-crypto");

const { privateKey, publicKey: writerAddress } = EthCrypto.createIdentity();

const db = new aodb("./my.db", {
	valueEncoding: "json",
	reduce: (a, b) => a
});

const schemaKey = "schema/hello";
const schemaValue = {
	keySchema: "hello",
	valueValidationKey: "",
	keyValidation: ""
};
let writerSignature = EthCrypto.sign(privateKey, db.createSignHash(schemaKey, schemaValue));

db.addSchema(schemaKey, schemaValue, writerSignature, writerAddress, (err) => {
	if (err) throw err;
	db.get(schemaKey, (err, node) => {
		if (err) throw err;
		console.log("Add Schema:\n" + schemaKey + " --> " + JSON.stringify(node.value) + "\n");

		let key = "hello";
		let value = "world";
		writerSignature = EthCrypto.sign(privateKey, db.createSignHash(key, value));

		db.put(key, value, writerSignature, writerAddress, { schemaKey }, (err) => {
			if (err) throw err;
			db.get(key, (err, node) => {
				if (err) throw err;
				console.log("Put:\n" + key + " --> " + node.value + "\n");
			});
		});
	});
});
```

## API

#### `var db = aodb(storage, [key], [options])`

Create a new aodb.

`storage` can be a string or a function. If a string like the above example, the
[random-access-file](https://github.com/mafintosh/random-access-file) storage
module is used; the resulting folder with the data will be whatever `storage` is
set to.

If `storage` is a function, it will be called with every filename aodb needs
to operate on. There are many providers for the
[abstract-random-access](https://github.com/juliangruber/abstract-random-access)
interface. e.g.

```js
var ram = require('random-access-memory')
var feed = aodb(function (filename) {
	// filename will be one of: data, bitfield, tree, signatures, key, secret_key
	// the data file will contain all your data concattenated.

	// just store all files in ram by returning a random-access-memory instance
	return ram()
})
```

`key` is a `Buffer` containing the local feed's public key. If you do not set
this the public key will be loaded from storage. If no key exists a new key pair
will be generated.

Options include:

```js
{
    map: node => mappedNode, // map nodes before returning them
	reduce: (a, b) => someNode, // reduce the nodes array before returning it
	firstNode: false, // set to true to reduce the nodes array to the first node in it
	valueEncoding: 'binary' // set the value encoding of the db
}
```

#### `db.key`

Buffer containing the public key identifying this aodb.

Populated after `ready` has been emitted. May be `null` before the event.

#### `db.discoveryKey`

Buffer containing a key derived from the db.key.
In contrast to `db.key` this key does not allow you to verify the data but can be used to announce or look for peers that are sharing the same aodb, without leaking the aodb key.

Populated after `ready` has been emitted. May be `null` before the event.

#### `db.on('ready')`

Emitted exactly once: when the db is fully ready and all static properties have
been set. You do not need to wait for this when calling any async functions.

#### `db.version(callback)`

Get the current version identifier as a buffer for the db.

#### `var checkout = db.checkout(version)`

Checkout the db at an older version. The checkout is a DB instance as well.
Version should be a version identifier returned by the `db.version` api or an
array of nodes returned from `db.heads`.

#### `db.put(key, value, signature, publicKey, [callback])`

Insert a new value. Will merge any previous values seen for this key.
Check out [example.js](example.js) on how to create `signature` and `publicKey`.

#### `db.get(key, callback)`

Lookup a string `key`. Returns a nodes array with the current values for this key.
If there is no current conflicts for this key the array will only contain a single node.

#### `db.del(key, signature, publicKey, callback)`

Delete a string `key`.

#### `db.batch(batch, [callback])`

Insert a batch of values efficiently, in a single atomic transaction. A batch should be an array of objects that look like this:

``` js
{
    type: 'put',
    key: someKey,
    value: someValue,
    signature: signedValueByIdentity,
    writerAddress: publicKeyOfIdentity
}
```

`callback`'s parameters are `err, nodes`, where `nodes` is an array of the batched nodes.

#### `db.local`

Your local writable feed. You have to get an owner of the aodb to authorize you to have your
writes replicate. The first person to create the aodb is the first owner.

#### `db.authorize(key, [callback])`

Authorize another peer to write to the aodb.

To get another peer to authorize you you'd usually do something like

``` js
myDb.on('ready', function () {
	console.log('You local key is ' + myDb.local.key.toString('hex'))
	console.log('Tell an owner to authorize it')
})
```

#### `db.authorized(key, [callback])`

Check whether a key is authorized to write to the database.

```js
myDb.authorized(otherDb.local.key, function (err, auth) {
	if (err) console.log('err', err)
	else if (auth === true) console.log('authorized')
	else console.log('not authorized')
})
```

#### `watcher = db.watch(folderOrKey, onchange)`

Watch a folder and get notified anytime a key inside this folder
has changed.

```js
db.watch('foo/bar', function () {
	console.log('folder has changed')
})

...

db.put('foo/bar/baz', 'hi', 'someSignature', 'somePublicKey') // triggers the above
```

You can destroy the watcher by calling `watcher.destroy()`.

The watcher will emit `watching` when it starts watching and `change`
when a change has been detected.

If a critical error occurs an error will be emitted on the watcher.

#### `var stream = db.createReadStream(prefix[, options])`

Create a readable stream of nodes stored in the database.
Set `prefix` to only iterate nodes prefixed with that folder.

Options include:

```js
{
    recursive: true // visit all subfolders.
	// set to false to only visit the first node in each folder
	reverse: true   // read the records in reverse order.
	gt: false       // visit only strictly nodes that are > than the prefix
}
```

#### `var stream = db.createWriteStream()`

Create a writable stream.

Where `stream.write(data)` accepts data as an object or an array of objects with the same form as `db.batch()`.

#### `db.list(prefix[, options], callback)`

Same as `createReadStream` but buffers the result to a list that is passed to the
callback.

#### `var stream = db.createDiffStream(prefix[, checkout)`

Find out about changes in key/value pairs between the version `checkout` and
current version prefixed by `prefix`.

`stream` is a readable object stream that outputs modifications like

```js
{ left: nodes, right: nodes }
```

`left` are the nodes for a key found in the `db` and `right` are the nodes found in the `checkout`.
If no nodes exist in the `db` for the key `left` will be `null` and vice versa.

#### `var stream = db.createHistoryStream([options])`

Returns a readable stream of node objects covering all historic values since the beginning of time.

Nodes are emitted in topographic order, meaning if value `v2` was aware of value
`v1` at its insertion time, `v1` must be emitted before `v2`.

To emit the nodes in reverse order pass `{reverse: true}` as an option.

#### `var stream = db.createKeyHistoryStream(key)`

Returns a readable stream of node objects covering all historic values for a specific key.

Results are returned with the latest value first.

#### `var stream = db.replicate([options])`

Create a replication stream. Options include:

``` js
{
    live: false // set to true to keep replicating
}
```

## License

MIT
