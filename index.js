const hypercore = require('hypercore')
const protocol = require('hypercore-protocol')
const thunky = require('thunky')
const remove = require('unordered-array-remove')
const toStream = require('nanoiterator/to-stream')
const varint = require('varint')
const mutexify = require('mutexify')
const codecs = require('codecs')
const raf = require('random-access-file')
const path = require('path')
const util = require('util')
const bulk = require('bulk-write-stream')
const events = require('events')
const sodium = require('sodium-universal')
const alru = require('array-lru')
const inherits = require('inherits')
const hash = require('./lib/hash')
const iterator = require('./lib/iterator')
const differ = require('./lib/differ')
const history = require('./lib/history')
const keyHistory = require('./lib/key-history')
const get = require('./lib/get')
const put = require('./lib/put')
const messages = require('./lib/messages')
const trie = require('./lib/trie-encoding')
const watch = require('./lib/watch')
const normalizeKey = require('./lib/normalize')
const derive = require('./lib/derive')
const EthCrypto = require('eth-crypto');
const validate = require('validate.js');
const { promisify } = require('es6-promisify');
const promisifyGet = promisify(get);

module.exports = AODB

function AODB (storage, key, opts) {
	if (!(this instanceof AODB)) return new AODB(storage, key, opts)
	events.EventEmitter.call(this)

	if (isOptions(key)) {
		opts = key
		key = null
	}

	opts = Object.assign({}, opts)
	if (opts.firstNode) opts.reduce = reduceFirst

	const checkout = opts.checkout

	this.key = typeof key === 'string' ? Buffer.from(key, 'hex') : key
	this.discoveryKey = this.key ? hypercore.discoveryKey(this.key) : null
	this.source = checkout ? checkout.source : null
	this.local = checkout ? checkout.local : null
	this.localContent = checkout ? checkout.localContent : null
	this.feeds = checkout ? checkout.feeds : []
	this.contentFeeds = checkout ? checkout.contentFeeds : (opts.contentFeed ? [] : null)
	this.ready = thunky(this._ready.bind(this))
	this.opened = false
	this.sparse = !!opts.sparse
	this.sparseContent = opts.sparseContent !== undefined ? !!opts.sparseContent : this.sparse
	this.id = Buffer.alloc(32)
	sodium.randombytes_buf(this.id)

	this._storage = createStorage(storage)
	this._contentStorage = typeof opts.contentFeed === 'function'
		? opts.contentFeed
		: opts.contentFeed ? this._storage : null
	this._writers = checkout ? checkout._writers : []
	this._watching = checkout ? checkout._watching : []
	this._replicating = []
	this._localWriter = null
	this._byKey = new Map()
	this._heads = opts.heads || null
	this._version = opts.version || null
	this._checkout = checkout || null
	this._lock = mutexify()
	this._map = opts.map || null
	this._reduce = opts.reduce || null
	this._valueEncoding = codecs(opts.valueEncoding || 'binary')
	this._batching = null
	this._batchingNodes = null
	this._secretKey = opts.secretKey || null
	this._storeSecretKey = opts.storeSecretKey !== false
	this._onwrite = opts.onwrite || null
	this._authorized = []

	this.ready()
}

inherits(AODB, events.EventEmitter)

AODB.prototype.batch = function (batch, cb) {
	if (!cb) cb = noop

	const self = this

	this._lock(function (release) {
		const clock = self._clock()

		self._batching = []
		self._batchingNodes = []

		self.heads(function (err, heads) {
			if (err) return cb(err)

			let i = 0

			loop(null)

			async function loop (err, node) {
				if (err) throwError(cb, err)

				if (node) {
					node.path = hash(node.key, true)
					heads = [node]
				}

				if (i === batch.length) {
					self.local.append(self._batching, done)
					return
				}

				const next = batch[i++]

				const signer = EthCrypto.recoverPublicKey(next.writerSignature, self.createSignHash(next.key, next.value));
				if (signer !== next.writerAddress) {
					throwError(cb, 'Error: signer does not match address and therefore does not have access to this record');
				}

				if (next.type === 'add-schema') {
					// Validate the schema val
					try {
						const validation = await validateSchemaVal(self, heads, next.key, next.value);
						if (validation.error) {
							throwError(cb, 'Error: ' + validation.errorMessage);
						}
						put(self, clock, heads, normalizeKey(next.key), next.value, next.writerSignature, next.writerAddress, {isSchema: true, noUpdate: true}, loop)
					} catch (e) {
						throwError(cb, e);
					}
				} else if (next.type === 'del') {
					put(self, clock, heads, normalizeKey(next.key), next.value, next.writerSignature, next.writerAddress, {delete: next.type === 'del'}, loop)
				} else if (next.type === 'put') {
					// If not writing a schema, then a schemaKey for this key needs to be provided
					if (!next.schemaKey) {
						throwError(cb, 'Error: missing the schemaKey option for this entry');
					}

					// Validate pointerKey if exist
					let hasPointer = false;
					if (next.pointerKey) {
						if (!next.pointerSchemaKey) {
							throwError(cb, 'Error: missing the pointerSchemaKey option for this entry');
						}
						try {
							let pointerSchemaKeyNode = await promisifyGet(self, heads, normalizeKey(next.pointerSchemaKey));
							if (!pointerSchemaKeyNode) throwError(cb, 'Error: unable to find this entry for the pointerSchemaKey')
							if (pointerSchemaKeyNode.length) pointerSchemaKeyNode = pointerSchemaKeyNode[0];

							// Validate the pointerKey
							const validation = validateKeySchema(normalizeKey(next.pointerKey), pointerSchemaKeyNode.value.keySchema, next.writerAddress);
							if (validation.error) {
								throwError(cb, 'Error: ' + validation.errorMessage);
							}
							hasPointer = true;
						} catch (e) {
							throwError(cb, e);
						}
					}

					// Get the schema
					try {
						let schemaKeyNode = await promisifyGet(self, heads, normalizeKey(next.schemaKey));
						if (!schemaKeyNode) throwError(cb, 'Error: unable to find this entry for the schemaKey')
						if (schemaKeyNode.length) schemaKeyNode = schemaKeyNode[0];

						// Validate the key
						let validation = validateKeySchema(normalizeKey(next.key), schemaKeyNode.value.keySchema, next.writerAddress);
						if (validation.error) {
							throwError(cb, 'Error: ' + validation.errorMessage);
						}

						// Validate the val if there is valueValidationKey
						if (schemaKeyNode.value.valueValidationKey) {
							validation = await validateEntryValue(self, heads, val, schemaKeyNode.value.valueValidationKey);
							if (validation.error) {
								throwError(cb, 'Error: ' + validation.errorMessage);
							}
						}
					} catch (e) {
						throwError(cb, e);
					}

					// If there is a valid pointerKey
					if (hasPointer && next.pointerKey && next.pointerSchemaKey) {
						// Insert the pointerKey
						put(self, clock, heads, normalizeKey(next.pointerKey), next.key, next.writerSignature, next.writerAddress, {schemaKey: next.pointerSchemaKey, pointer: true}, (err, node) => {
							if (node) {
								node.path = hash(node.key, true)
								heads = [node]
							}
							// Insert the key
							put(self, clock, heads, normalizeKey(next.key), next.value, next.writerSignature, next.writerAddress, {schemaKey: next.schemaKey, pointerKey: next.pointerKey}, loop);
						})
					} else {
						put(self, clock, heads, normalizeKey(next.key), next.value, next.writerSignature, next.writerAddress, {schemaKey: next.schemaKey}, loop)
					}
				} else {
					throwError(cb, 'Error: missing the type option for this entry');
				}
			}

			function done (err) {
				const nodes = self._batchingNodes
				self._batching = null
				self._batchingNodes = null
				return release(cb, err, nodes)
			}
		})
	})
}

AODB.prototype.put = function (key, val, writerSignature, writerAddress, opts, cb) {
	if (typeof opts === 'function') return this.put(key, val, writerSignature, writerAddress, null, opts)
	if (!cb) cb = noop

	if (this._checkout) {
		throwError(cb, 'Cannot put on a checkout')
	}

	const self = this

	this._lock(function (release) {
		const clock = self._clock()
		self._getHeads(false, async function (err, heads) {
			if (err) throwError(cb, err)

			// Perform writerSignature validation IFF key, writerSignature and writerAddress exist
			if (key && writerSignature && writerAddress) {
				const signer = EthCrypto.recoverPublicKey(writerSignature, self.createSignHash(key, val));

				// Validate the writerSignature
				if (signer !== writerAddress) {
					throwError(cb, 'Error: signer does not match address and therefore does not have access to this record');
				}

				// If writing a schema
				if (opts && opts.isSchema === true) {
					// Schema is not rewriteable
					opts.noUpdate = true

					// Validate the schema val
					try {
						const validation = await validateSchemaVal(self, heads, key, val);
						if (validation.error) {
							throwError(cb, 'Error: ' + validation.errorMessage);
						}
						put(self, clock, heads, normalizeKey(key), val, writerSignature, writerAddress, opts, unlock)
					} catch (e) {
						throwError(cb, e);
					}
				} else if (opts && opts.delete === true) {
					// If deleting an entry
					put(self, clock, heads, normalizeKey(key), val, writerSignature, writerAddress, opts, unlock)
				} else {
					// If not writing a schema, then a schemaKey for this key needs to be provided
					if (opts && !opts.schemaKey) {
						throwError(cb, 'Error: missing the schemaKey option for this entry');
					}

					// Validate pointerKey if exist
					let hasPointer = false;
					if (opts && opts.pointerKey) {
						if (!opts.pointerSchemaKey) {
							throwError(cb, 'Error: missing the pointerSchemaKey option for this entry');
						}
						try {
							let pointerSchemaKeyNode = await promisifyGet(self, heads, normalizeKey(opts.pointerSchemaKey));
							if (!pointerSchemaKeyNode) throwError(cb, 'Error: unable to find this entry for the pointerSchemaKey')
							if (pointerSchemaKeyNode.length) pointerSchemaKeyNode = pointerSchemaKeyNode[0];

							// Validate the pointerKey
							const validation = validateKeySchema(normalizeKey(opts.pointerKey), pointerSchemaKeyNode.value.keySchema, writerAddress);
							if (validation.error) {
								throwError(cb, 'Error: ' + validation.errorMessage);
							}
							hasPointer = true;
						} catch (e) {
							throwError(cb, e);
						}
					}

					// Get the schema
					try {
						let schemaKeyNode = await promisifyGet(self, heads, normalizeKey(opts.schemaKey));
						if (!schemaKeyNode) throwError(cb, 'Error: unable to find this entry for the schemaKey')
						if (schemaKeyNode.length) schemaKeyNode = schemaKeyNode[0];

						// Validate the key
						let validation = validateKeySchema(normalizeKey(key), schemaKeyNode.value.keySchema, writerAddress);
						if (validation.error) {
							throwError(cb, 'Error: ' + validation.errorMessage);
						}

						// Validate the val if there is valueValidationKey
						if (schemaKeyNode.value.valueValidationKey) {
							validation = await validateEntryValue(self, heads, val, schemaKeyNode.value.valueValidationKey);
							if (validation.error) {
								throwError(cb, 'Error: ' + validation.errorMessage);
							}
						}
					} catch (e) {
						throwError(cb, e);
					}

					// If there is a valid pointerKey
					if (hasPointer && opts.pointerKey && opts.pointerSchemaKey) {
						// Insert the pointerKey
						put(self, clock, heads, normalizeKey(opts.pointerKey), key, writerSignature, writerAddress, {schemaKey: opts.pointerSchemaKey, pointer: true}, (err, node) => {
							self._getHeads(false, async function (err, heads) {
								if (err) throwError(cb, err)
								// Insert the key
								put(self, clock, heads, normalizeKey(key), val, writerSignature, writerAddress, opts, unlock);
							})
						})
					} else {
						put(self, clock, heads, normalizeKey(key), val, writerSignature, writerAddress, opts, unlock)
					}
				}
			} else {
				// We don't need writerSignature validation when authorizing empty key
				// i.e self.put('', null, '', '', cb) inside AODB.prototype.authorize
				put(self, clock, heads, '', null, '', '', opts, unlock)
			}
		})

		function unlock (err, node) {
			release(cb, err, node)
		}
	})
}

AODB.prototype.addSchema = function (key, val, writerSignature, writerAddress, cb) {
	this.put(key, val, writerSignature, writerAddress, { isSchema: true }, cb)
}

AODB.prototype.del = function (key, writerSignature, writerAddress, cb) {
	this.put(key, '', writerSignature, writerAddress, { delete: true }, cb)
}

AODB.prototype.watch = function (key, cb) {
	if (typeof key === 'function') return this.watch('', key)
	return watch(this, normalizeKey(key), cb)
}

AODB.prototype.get = function (key, opts, cb) {
	if (typeof opts === 'function') return this.get(key, null, opts)

	const self = this

	this._getHeads((opts && opts.update) !== false, function (err, heads) {
		if (err) return cb(err)
		get(self, heads, normalizeKey(key), opts, cb)
	})
}

AODB.prototype.version = function (cb) {
	const self = this

	this.heads(function (err, heads) {
		if (err) return cb(err)

		const buffers = []

		for (let i = 0; i < heads.length; i++) {
			buffers.push(self.feeds[heads[i].feed].key)
			buffers.push(Buffer.from(varint.encode(heads[i].seq)))
		}

		cb(null, Buffer.concat(buffers))
	})
}

AODB.prototype.checkout = function (version, opts) {
	if (!opts) opts = {}

	if (typeof version === 'string') {
		version = Buffer.from(version, 'hex')
	}

	if (Array.isArray(version)) {
		opts.heads = version
		version = null
	}

	return new AODB(this._storage, this.key, {
		checkout: this,
		version: version,
		map: opts.map !== undefined ? opts.map : this._map,
		reduce: opts.reduce !== undefined ? opts.reduce : this._reduce,
		heads: opts.heads
	})
}

AODB.prototype.snapshot = function (opts) {
	return this.checkout(null, opts)
}

AODB.prototype.heads = function (cb) {
	this._getHeads(true, cb)
}

AODB.prototype._getHeads = function (update, cb) {
	if (!this.opened) return readyAndHeads(this, update, cb)
	if (this._heads) return process.nextTick(cb, null, this._heads)

	// This is a bit of a hack. Basically when the db is empty
	// we wanna wait for data to come in. TODO: We should guarantee
	// that the db always has a single block of data (like a header)
	if (update && this._waitForUpdate()) {
		this.setMaxListeners(0)
		this.once('remote-update', this.heads.bind(this, cb))
		return
	}

	const self = this
	const len = this._writers.length
	let missing = len
	let error = null
	const nodes = new Array(len)

	for (let i = 0; i < len; i++) {
		this._writers[i].head(onhead)
	}

	function onhead (err, head, i) {
		if (err) error = err
		else nodes[i] = head

		if (--missing) return

		if (error) return cb(error)
		if (len !== self._writers.length) return self.heads(cb)

		if (nodes.length === 1) return cb(null, nodes[0] ? nodes : [])
		cb(null, filterHeads(nodes))
	}
}

AODB.prototype._waitForUpdate = function () {
	return !this._writers[0].length() && this.local.length < 2
}

AODB.prototype._index = function (key) {
	if (key.key) key = key.key
	for (let i = 0; i < this.feeds.length; i++) {
		if (this.feeds[i].key.equals(key)) return i
	}
	return -1
}

AODB.prototype.authorized = function (key, cb) {
	const self = this

	this._getHeads(false, function (err) {
		if (err) return cb(err)
		// writers[0] is the source, always authed
		cb(null, self._writers[0].authorizes(key, null))
	})
}

AODB.prototype.authorize = function (key, cb) {
	if (!cb) cb = noop

	const self = this

	this.heads(function (err) { // populates .feeds to be up to date
		if (err) return cb(err)
		self._addWriter(key, function (err) {
			if (err) return cb(err)
			self.put('', null, '', '', cb)
		})
	})
}

AODB.prototype.replicate = function (opts) {
	opts = Object.assign({}, opts)

	const self = this
	let expectedFeeds = Math.max(1, this._authorized.length)
	const factor = this.contentFeeds ? 2 : 1

	opts.expectedFeeds = expectedFeeds * factor
	if (!opts.id) opts.id = this.id

	if (!opts.stream) opts.stream = protocol(opts)
	const stream = opts.stream

	if (!opts.live) stream.on('prefinalize', prefinalize)

	this.ready(onready)

	return stream

	function onready (err) {
		if (err) return stream.destroy(err)
		if (stream.destroyed) return

		// bootstrap content feeds
		if (self.contentFeeds && !self.contentFeeds[0]) self._writers[0].get(1, noop)

		let i = 0

		self._replicating.push(replicate)
		stream.on('close', onclose)
		stream.on('end', onclose)

		replicate()

		function oncontent () {
			this._contentFeed.replicate(opts)
		}

		function replicate () {
			for (; i < self._authorized.length; i++) {
				const j = self._authorized[i]
				self.feeds[j].replicate(opts)
				if (!self.contentFeeds) continue
				const w = self._writers[j]
				if (w._contentFeed) w._contentFeed.replicate(opts)
				else w.once('content-feed', oncontent)
			}
		}

		function onclose () {
			let i = self._replicating.indexOf(replicate)
			if (i > -1) remove(self._replicating, i)
			for (i = 0; i < self._writers.length; i++) {
				self._writers[i].removeListener('content-feed', oncontent)
			}
		}
	}

	function prefinalize (cb) {
		self.heads(function (err) {
			if (err) return cb(err)
			stream.expectedFeeds += factor * (self._authorized.length - expectedFeeds)
			expectedFeeds = self._writers.length
			cb()
		})
	}
}

AODB.prototype._clock = function () {
	const clock = new Array(this._writers.length)

	for (let i = 0; i < clock.length; i++) {
		const w = this._writers[i]
		clock[i] = w === this._localWriter ? w._clock : w.length()
	}

	return clock
}

AODB.prototype._getPointer = function (feed, index, isPut, cb) {
	if (isPut && this._batching && feed === this._localWriter._id && index >= this._localWriter._feed.length) {
		process.nextTick(cb, null, this._batchingNodes[index - this._localWriter._feed.length])
		return
	}
	this._writers[feed].get(index, cb)
}

AODB.prototype._getAllPointers = function (list, isPut, cb) {
	let error = null
	const result = new Array(list.length)
	let missing = result.length

	if (!missing) return process.nextTick(cb, null, result)

	for (let i = 0; i < result.length; i++) {
		this._getPointer(list[i].feed, list[i].seq, isPut, done)
	}

	function done (err, node) {
		if (err) error = err
		else result[indexOf(list, node)] = node
		if (!--missing) cb(error, result)
	}
}

AODB.prototype._writer = function (dir, key, opts) {
	let writer = key && this._byKey.get(key.toString('hex'))
	if (writer) return writer

	opts = Object.assign({}, opts, {
		sparse: this.sparse,
		onwrite: this._onwrite ? onwrite : null
	})

	const self = this
	var feed = hypercore(storage, key, opts)

	writer = new Writer(self, feed)
	feed.on('append', onappend)
	feed.on('remote-update', onremoteupdate)
	feed.on('sync', onreloadhead)

	if (key) addWriter(null)
	else feed.ready(addWriter)

	return writer

	function onwrite (index, data, peer, cb) {
		if (!index) return cb(null) // do not intercept the header
		if (peer) peer.maxRequests++
		if (index >= writer._writeLength) writer._writeLength = index + 1
		writer._writes.set(index, data)
		writer._decode(index, data, function (err, entry) {
			if (err) return done(cb, index, peer, err)
			self._onwrite(entry, peer, function (err) {
				done(cb, index, peer, err)
			})
		})
	}

	function done (cb, index, peer, err) {
		if (peer) peer.maxRequests--
		writer._writes.delete(index)
		cb(err)
	}

	function onremoteupdate () {
		self.emit('remote-update', feed, writer._id)
	}

	function onreloadhead () {
		// read writer head to see if any new writers are added on full sync
		writer.head(noop)
	}

	function onappend () {
		for (let i = 0; i < self._watching.length; i++) self._watching[i]._kick()
		self.emit('append', feed, writer._id)
	}

	function addWriter (err) {
		if (!err) self._byKey.set(feed.key.toString('hex'), writer)
	}

	function storage (name) {
		return self._storage(dir + '/' + name, {feed})
	}
}

AODB.prototype._getWriter = function (key) {
	return this._byKey.get(key.toString('hex'))
}

AODB.prototype._addWriter = function (key, cb) {
	const self = this
	const writer = this._writer('peers/' + hypercore.discoveryKey(key).toString('hex'), key)

	writer._feed.ready(function (err) {
		if (err) return cb(err)
		if (self._index(key) <= -1) self._pushWriter(writer)
		cb(null)
	})
}

AODB.prototype._pushWriter = function (writer) {
	writer._id = this._writers.push(writer) - 1
	this.feeds.push(writer._feed)
	if (this.contentFeeds) this.contentFeeds.push(null)

	if (!this.opened) return

	for (let i = 0; i < this._replicating.length; i++) {
		this._replicating[i]()
	}
}

AODB.prototype.list = function (prefix, opts, cb) {
	if (typeof prefix === 'function') return this.list('', null, prefix)
	if (typeof opts === 'function') return this.list(prefix, null, opts)

	const ite = this.iterator(prefix, opts)
	const list = []

	ite.next(loop)

	function loop (err, nodes) {
		if (err) return cb(err)
		if (!nodes) return cb(null, list)
		list.push(nodes)
		ite.next(loop)
	}
}

AODB.prototype.history = function (opts) {
	return history(this, opts)
}

AODB.prototype.keyHistory = function (prefix, opts) {
	return keyHistory(this, prefix, opts)
}

AODB.prototype.diff = function (other, prefix, opts) {
	if (isOptions(prefix)) return this.diff(other, null, prefix)
	return differ(this, other || checkoutEmpty(this), prefix || '', opts)
}

AODB.prototype.iterator = function (prefix, opts) {
	if (isOptions(prefix)) return this.iterator('', prefix)
	return iterator(this, normalizeKey(prefix || ''), opts)
}

AODB.prototype.createHistoryStream = function (opts) {
	return toStream(this.history(opts))
}

AODB.prototype.createKeyHistoryStream = function (prefix, opts) {
	return toStream(this.keyHistory(prefix, opts))
}

AODB.prototype.createDiffStream = function (other, prefix, opts) {
	if (isOptions(prefix)) return this.createDiffStream(other, '', prefix)
	return toStream(this.diff(other, prefix, opts))
}

AODB.prototype.createReadStream = function (prefix, opts) {
	return toStream(this.iterator(prefix, opts))
}

AODB.prototype.createWriteStream = function (cb) {
	const self = this
	return bulk.obj(write)

	function write (batch, cb) {
		const flattened = []
		for (let i = 0; i < batch.length; i++) {
			const content = batch[i]
			if (Array.isArray(content)) {
				for (let j = 0; j < content.length; j++) {
					flattened.push(content[j])
				}
			} else {
				flattened.push(content)
			}
		}
		self.batch(flattened, cb)
	}
}

AODB.prototype._ready = function (cb) {
	const self = this

	if (this._checkout) {
		if (this._heads) oncheckout(null, this._heads)
		else if (this._version) this._checkout.heads(onversion)
		else this._checkout.heads(oncheckout)
		return
	}

	if (!this.source) {
		this.source = feed('source', this.key, {
			secretKey: this._secretKey,
			storeSecretKey: this._storeSecretKey
		})
	}

	this.source.ready(function (err) {
		if (err) return done(err)
		if (self.source.writable) self.local = self.source
		if (!self.local) self.local = feed('local')

		self.key = self.source.key
		self.discoveryKey = self.source.discoveryKey
		self._writers[0].authorize() // source is always authorized

		self.local.ready(function (err) {
			if (err) return done(err)

			self._localWriter = self._writers[self.feeds.indexOf(self.local)]

			if (self._contentStorage) {
				self._localWriter._ensureContentFeed(null)
				self.localContent = self._localWriter._contentFeed
			}

			self._localWriter.head(function (err) {
				if (err) return done(err)
				if (!self.localContent) return done(null)
				self.localContent.ready(done)
			})
		})
	})

	function done (err) {
		if (err) return cb(err)
		self._localWriter.ensureHeader(onheader)

		function onheader (err) {
			if (err) return cb(err)
			self.opened = true
			self.emit('ready')
			cb(null)
		}
	}

	function feed (dir, key, feedOpts) {
		const writer = self._writer(dir, key, feedOpts)
		self._pushWriter(writer)
		return writer._feed
	}

	function onversion (err) {
		if (err) return done(err)

		let offset = 0
		let missing = 0
		const nodes = []
		let error = null

		if (typeof self._version === 'number') {
			missing = 1
			self._checkout._writers[0].get(self._version, onnode)
			return
		}

		while (offset < self._version.length) {
			const key = self._version.slice(offset, offset + 32)
			const seq = varint.decode(self._version, offset + 32)
			offset += 32 + varint.decode.bytes
			const writer = self._checkout._byKey.get(key.toString('hex'))
			if (!writer) {
				error = new Error('Invalid version')
				continue
			}
			missing++
			writer.get(seq, onnode)
		}

		if (!missing) oncheckout(error, [])

		function onnode (err, node) {
			if (err) error = err
			else nodes.push(node)
			if (!--missing) oncheckout(error, nodes)
		}
	}

	function oncheckout (err, heads) {
		if (err) return done(err)

		self.opened = true
		self.source = self._checkout.source
		self.local = self._checkout.local
		self.localContent = self._checkout.localContent
		self._localWriter = self._checkout._localWriter
		self.key = self._checkout.key
		self.discoveryKey = self._checkout.discoveryKey
		self._heads = heads

		done(null)
	}
}

AODB.prototype.createSignHash = function (key, val) {
	if (val === null) val = '';
	const signData = [
		{	// prefix
			type: 'string',
			value: 'AODB Signature'
		},
		{	// key to be signed
			type: 'string',
			value: key
		},
		{	// value to be signed
			type: 'string',
			value: val
		}
	];
	return EthCrypto.hash.keccak256(signData);
}

/**
 * Validation functions
 */
AODB.prototype.validateMaxLength140 = function (value) {
	const constraints = {
		value: {
			length: { maximum: 140}
		}
	};
	const error = validate({value}, constraints);
	if (error && error.value) {
		return { error: true, errorMessage: error.value[0] };
	}
	return { error: false, errorMessage: '' };
}

function Writer (db, feed) {
	events.EventEmitter.call(this)

	this._id = 0
	this._db = db
	this._feed = feed
	this._contentFeed = null
	this._feeds = 0
	this._feedsMessage = null
	this._feedsLoaded = -1
	this._entry = 0
	this._clock = 0
	this._encodeMap = []
	this._decodeMap = []
	this._checkout = false
	this._length = 0
	this._authorized = false

	this._cache = alru(4096)

	this._writes = new Map()
	this._writeLength = 0

	this.setMaxListeners(0);
}

inherits(Writer, events.EventEmitter)

Writer.prototype.authorize = function () {
	if (this._authorized) return
	this._authorized = true
	this._db._authorized.push(this._id)
	if (this._feedsMessage) this._updateFeeds()
}

Writer.prototype.ensureHeader = function (cb) {
	if (this._feed.length) return cb(null)

	const header = {
		protocol: 'aodb'
	}

	this._feed.append(messages.Header.encode(header), cb)
}

Writer.prototype.append = function (entry, cb) {
	if (!this._clock) this._clock = this._feed.length

	let enc = messages.Entry
	this._entry = this._clock++

	entry.clock[this._id] = this._clock
	entry.seq = this._clock - 1
	entry.feed = this._id
	entry[util.inspect.custom] = inspect

	const mapped = {
		key: entry.key,
		pointerKey: entry.pointerKey,
		schemaKey: entry.schemaKey,
		value: null,
		deleted: entry.deleted,
		pointer: entry.pointer,
		noUpdate: entry.noUpdate,
		isSchema: entry.isSchema,
		inflate: 0,
		clock: null,
		trie: null,
		feeds: null,
		contentFeed: null,
		writerSignature: entry.writerSignature,
		writerAddress: entry.writerAddress,
		proofSignature: entry.proofSignature,
		proofPayload: entry.proofPayload,
		rootHash: entry.rootHash
	}

	if (this._needsInflate()) {
		enc = messages.InflatedEntry
		mapped.feeds = this._mapList(this._db.feeds, this._encodeMap, null)
		if (this._db.contentFeeds) mapped.contentFeed = this._db.contentFeeds[this._id].key
		this._feedsMessage = mapped
		this._feedsLoaded = this._feeds = this._entry
		this._updateFeeds()
	}

	mapped.clock = this._mapList(entry.clock, this._encodeMap, 0)
	mapped.inflate = this._feeds
	mapped.trie = trie.encode(entry.trie, this._encodeMap)
	if (!isNullish(entry.value)) mapped.value = this._db._valueEncoding.encode(entry.value)

	if (this._db._batching) {
		this._db._batching.push(enc.encode(mapped))
		this._db._batchingNodes.push(entry)
		return cb(null)
	}

	this._feed.append(enc.encode(mapped), cb)
}

Writer.prototype._needsInflate = function () {
	const msg = this._feedsMessage
	return !msg || msg.feeds.length !== this._db.feeds.length
}

Writer.prototype._maybeUpdateFeeds = function () {
	if (!this._feedsMessage) return
	const writers = this._feedsMessage.feeds || []
	if (
		this._decodeMap.length !== writers.length ||
		this._encodeMap.length !== this._db.feeds.length
	) {
		this._updateFeeds()
	}
}

Writer.prototype._decode = function (seq, buf, cb) {
	let val;
	try {
		val = messages.Entry.decode(buf)
	} catch (e) {
		return cb(e)
	}
	val[util.inspect.custom] = inspect
	val.seq = seq
	val.path = hash(val.key, true)
	try {
		val.value = val.value && this._db._valueEncoding.decode(val.value)
		val.proofPayload = val.proofPayload && this._db._valueEncoding.decode(val.proofPayload)
	} catch (e) {
		return cb(e)
	}

	if (this._feedsMessage && this._feedsLoaded === val.inflate) {
		this._maybeUpdateFeeds()
		val.feed = this._id
		if (val.clock.length > this._decodeMap.length) {
			return cb(new Error('Missing feed mappings'))
		}
		val.clock = this._mapList(val.clock, this._decodeMap, 0)
		val.trie = trie.decode(val.trie, this._decodeMap)
		this._cache.set(val.seq, val)
		return cb(null, val, this._id)
	}

	this._loadFeeds(val, buf, cb)
}

Writer.prototype.get = function (seq, cb) {
	const self = this
	const cached = this._cache.get(seq)
	if (cached) return process.nextTick(cb, null, cached, this._id)

	this._getFeed(seq, function (err, val) {
		if (err) return cb(err)
		self._decode(seq, val, cb)
	})
}

Writer.prototype._getFeed = function (seq, cb) {
	if (this._writes && this._writes.size) {
		const buf = this._writes.get(seq)
		if (buf) return process.nextTick(cb, null, buf)
	}
	this._feed.get(seq, cb)
}

Writer.prototype.head = function (cb) {
	let len = this.length()
	if (len < 2) return process.nextTick(cb, null, null, this._id)
	this.get(len - 1, cb)
}

Writer.prototype._mapList = function (list, map, def) {
	const mapped = []
	let i
	for (i = 0; i < map.length; i++) mapped[map[i]] = i < list.length ? list[i] : def
	for (; i < list.length; i++) mapped[i] = list[i]
	for (i = 0; i < mapped.length; i++) {
		if (!mapped[i]) mapped[i] = def
	}
	return mapped
}

Writer.prototype._loadFeeds = function (head, buf, cb) {
	const self = this

	if (head.feeds) done(head)
	else if (head.inflate === head.seq) onfeeds(null, buf)
	else this._getFeed(head.inflate, onfeeds)

	function onfeeds (err, buf) {
		if (err) return cb(err)
		let msg;
		try {
			msg = messages.InflatedEntry.decode(buf)
		} catch (e) {
			return cb(e)
		}
		done(msg)
	}

	function done (msg) {
		self._addWriters(head, msg, cb)
	}
}

Writer.prototype._addWriters = function (head, inflated, cb) {
	const self = this
	const id = this._id
	const writers = inflated.feeds || []
	let missing = writers.length + 1
	let error = null

	for (let i = 0; i < writers.length; i++) {
		this._db._addWriter(writers[i].key, done)
	}

	done(null)

	function done (err) {
		if (err) error = err
		if (--missing) return
		if (error) return cb(error)
		const seq = head.inflate
		if (seq > self._feedsLoaded) {
			self._feedsLoaded = self._feeds = seq
			self._feedsMessage = inflated
		}
		self._updateFeeds()
		head.feed = self._id
		if (head.clock.length > self._decodeMap.length) {
			return cb(new Error('Missing feed mappings'))
		}
		head.clock = self._mapList(head.clock, self._decodeMap, 0)
		head.trie = trie.decode(head.trie, self._decodeMap)
		self._cache.set(head.seq, head)
		cb(null, head, id)
	}
}

Writer.prototype._ensureContentFeed = function (key) {
	if (this._contentFeed) return

	const self = this
	let secretKey = null

	if (!key) {
		const pair = derive(this._db.local.secretKey)
		secretKey = pair.secretKey
		key = pair.publicKey
	}

	this._contentFeed = hypercore(storage, key, {
		sparse: this._db.sparseContent,
		storeSecretKey: false,
		secretKey
	})

	if (this._db.contentFeeds) this._db.contentFeeds[this._id] = this._contentFeed
	this.emit('content-feed')

	function storage (name) {
		name = 'content/' + self._feed.discoveryKey.toString('hex') + '/' + name
		return self._db._contentStorage(name, {
			metadata: self._feed,
			feed: self._contentFeed
		})
	}
}

Writer.prototype._updateFeeds = function () {
	let i
	let updateReplicates = false

	if (this._feedsMessage.contentFeed && this._db.contentFeeds && !this._contentFeed) {
		this._ensureContentFeed(this._feedsMessage.contentFeed)
		updateReplicates = true
	}

	const writers = this._feedsMessage.feeds || []
	const map = new Map()

	for (i = 0; i < this._db.feeds.length; i++) {
		map.set(this._db.feeds[i].key.toString('hex'), i)
	}

	for (i = 0; i < writers.length; i++) {
		const id = map.get(writers[i].key.toString('hex'))
		this._decodeMap[i] = id
		this._encodeMap[id] = i
		if (this._authorized) {
			this._db._writers[id].authorize()
			updateReplicates = true
		}
	}

	if (!updateReplicates) return

	for (i = 0; i < this._db._replicating.length; i++) {
		this._db._replicating[i]()
	}
}

Writer.prototype.authorizes = function (key, visited) {
	if (!visited) visited = new Array(this._db._writers.length)

	if (this._feed.key.equals(key)) return true
	if (!this._feedsMessage || visited[this._id]) return false
	visited[this._id] = true

	const feeds = this._feedsMessage.feeds || []
	for (let i = 0; i < feeds.length; i++) {
		const authedKey = feeds[i].key
		if (authedKey.equals(key)) return true
		const authedWriter = this._db._getWriter(authedKey)
		if (authedWriter.authorizes(key, visited)) return true
	}

	return false
}

Writer.prototype.length = function () {
	if (this._checkout) return this._length
	return Math.max(this._writeLength, Math.max(this._feed.length, this._feed.remoteLength))
}

function filterHeads (list) {
	const heads = []
	for (let i = 0; i < list.length; i++) {
		if (isHead(list[i], list)) heads.push(list[i])
	}
	return heads
}

function isHead (node, list) {
	if (!node) return false

	const clock = node.seq + 1

	for (let i = 0; i < list.length; i++) {
		const other = list[i]
		if (other === node || !other) continue
		if ((other.clock[node.feed] || 0) >= clock) return false
	}

	return true
}

function checkoutEmpty (db) {
	db = db.checkout(Buffer.from([]))
	return db
}

function readyAndHeads (self, update, cb) {
	self.ready(function (err) {
		if (err) return cb(err)
		self._getHeads(update, cb)
	})
}

function indexOf (list, ptr) {
	for (let i = 0; i < list.length; i++) {
		const p = list[i]
		if (ptr.feed === p.feed && ptr.seq === p.seq) return i
	}
	return -1
}

function isOptions (opts) {
	return typeof opts === 'object' && !!opts && !Buffer.isBuffer(opts)
}

function createStorage (st) {
	if (typeof st === 'function') return st
	return function (name) {
		return raf(path.join(st, name))
	}
}

function reduceFirst (a, b) {
	return a
}

function isNullish (v) {
	return v === null || v === undefined
}

function noop () {}

function inspect () {
	return `Node(key=${this.key}` +
		`, pointerKey=${this.pointerKey}` +
		`, schemaKey=${this.schemaKey}` +
		`, value=${util.inspect(this.value)}` +
		`, pointer=${this.pointer}` +
		`, noUpdate=${this.noUpdate}` +
		`, isSchema=${this.isSchema}` +
		`, seq=${this.seq}` +
		`, feed=${this.feed}` +
		`, writerSignature=${this.writerSignature}` +
		`, writerAddress=${this.writerAddress}` +
		`, proofSignature=${this.proofSignature}` +
		`, proofPayload=${util.inspect(this.proofPayload)}` +
		`, rootHash=${this.rootHash}` +
		`)`
}

/**
 * @dev Check whether or not the value of a schema entry is valid
 *		i.e, has to be in following structure
 *		val => {
 *			keySchema: someSchema,
 *			valueValidationKey: someValidationKey,
 *			keyValidation: pointerToLibraryWithRelevantVariables
 *		}
 */
const validateSchemaVal = async (self, heads, key, val) => {
	if (!key || typeof(val) !== 'object')
		return { error: true, errorMessage: 'Missing key/val value' };
	if (!val.hasOwnProperty('keySchema') || !val.hasOwnProperty('valueValidationKey') || !val.hasOwnProperty('keyValidation'))
		return { error: true, errorMessage: 'val is missing keySchema / valueValidationKey / keyValidation property' };
	if (normalizeKey(key) !== 'schema/' + normalizeKey(val.keySchema))
		return { error: true, errorMessage: 'key does not have the correct schema structure' };
	if (val.valueValidationKey) {
		try {
			const node = await promisifyGet(self, heads, normalizeKey(val.valueValidationKey));
			if (!node)
				return { error: true, errorMessage: 'Unable to find valueValidationKey entry' };
			return { error: false, errorMessage: '' };
		} catch (e) {
			return { error: true, errorMessage: e };
		}
	} else {
		return { error: false, errorMessage: '' };
	}
}

/**
 * @dev Check whether or not a key follows the schema structure
 */
const validateKeySchema = (key, keySchema, writerAddress) => {
	const splitKey = key.split('/');
	const splitKeySchema = keySchema.split('/');

	if (splitKey.length != splitKeySchema.length)
		return { error: true, errorMessage: 'key has incorrect space length' };

	for (let i=0; i < splitKeySchema.length; i++) {
		if (splitKeySchema[i] === '*') continue
		if (splitKeySchema[i] === '%writerAddress%' && splitKey[i] !== writerAddress) {
			return { error: true, errorMessage: 'key\'s writerAddress does not match the address' };
		}
		if (splitKeySchema[i] !== splitKey[i] && splitKeySchema[i] !== '%writerAddress%')
			return { error: true, errorMessage: 'key\'s space not match. key => ' + splitKey[i] + '. schema => ' + splitKeySchema[i] };
	}
	return { error: false, errorMessage: '' };
};

/**
 * @dev Check whether or not the entry value needs specific validation
 */
const validateEntryValue = async (self, heads, value, valueValidationKey) => {
	try {
		let node = await promisifyGet(self, heads, normalizeKey(valueValidationKey));
		if (!node)
			return { error: true, errorMessage: 'Unable to find valueValidationKey entry' };
		if (node.length) node = node[0];
		const {error, errorMessage} = self[node.value](value);
		return { error, errorMessage };
	} catch (e) {
		return {error: true, errorMessage: e};
	}
};

/**
 * @dev helper function to exit when there is an error
 */
const throwError = (cb, errorMessage) => {
	return process.nextTick(cb, new Error(errorMessage));
};
