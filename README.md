# level-cluster

Use consistent-hashing with hash-rings to distribute reads and writes across
multiple multilevel nodes.

[![build status](https://secure.travis-ci.org/eugeneware/level-cluster.png)](http://travis-ci.org/eugeneware/level-cluster)

**NB: Work in progress. Not currently suitable for production.**

`level-cluster` tries to implement the [levelup](https://github.com/rvagg/node-levelup) API
but on top of a [multilevel](https://github.com/juliangruber/multilevel) cluster.

Currently assumes [bytewise/hex](https://github.com/deanlandolt/bytewise) as a keyEncoding.

## Installation

This module is installed via npm:

``` bash
$ npm install level-cluster
```

## Example Usage

Assuming some [multilevel](https://github.com/juliangruber/multilevel) servers listening
on `['127.0.0.1:3000', '127.0.0.1:3001', '127.0.0.1:3002']`:

``` js
var servers = ['127.0.0.1:3000', '127.0.0.1:3001', '127.0.0.1:3002'];
var db = new LevelCluster(servers);

 // will consistently hash the write to a server based on the key
db.put(...);

// will consistently hash all the writes and deletes to the right servers
db.batch(...);

// will retrieve the right data from the right server
db.get(...);

// will delete the right data from the right server
db.del(...);

db.createReadStream();
// will stream the data from the different servers and create a unified stream
// AND make sure it's sorted

generatorStream().pipe(db.createWriteStream());
// will distribute writes and deletes across the cluster based on the hashes of
// keys
```
