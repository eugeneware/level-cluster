var multilevel = require('multilevel'),
    HashRing = require('hashring'),
    bytewise = require('bytewise/hex'),
    net = require('net'),
    after = require('after'),
    joiner = require('joiner-stream'),
    setImmediate = global.setImmediate || process.nextTick;

module.exports = LevelCluster;

function LevelCluster(_servers, options) {
  options = options || {};
  this._servers = _servers;
  this.ring = _servers.reduce(function (acc, item) {
    acc.add(item);
    return acc;
  }, new HashRing());
  this.servers = {};
}

LevelCluster.prototype.put = function (key, value, cb) {
  var db = this.getServerByKey(key);
  return db.put(key, value, cb);
};

LevelCluster.prototype.get = function (key, cb) {
  var db = this.getServerByKey(key);
  return db.get(key, cb);
};

LevelCluster.prototype.batch = function (batch, cb) {
  var self = this;
  var batches = batch.reduce(function (acc, item) {
    var serverName = self.getServerNameByKey(item.key);
    acc[serverName] = acc[serverName] || [];
    acc[serverName].push(item);
    return acc;
  }, {});
  var keys = Object.keys(batches);
  var next = after(keys.length, cb);
  keys.forEach(function (serverName) {
    var server = self.getServerByName(serverName);
    var batch = batches[serverName];
    server.batch(batch, next);
  });
};

LevelCluster.prototype.createReadStream = function (options) {
  this.connectAllServers();
  var serverKeys = Object.keys(this.servers);
  var aggregator = joiner();
  var self = this;
  serverKeys.forEach(function (serverKey) {
    var server = self.servers[serverKey];
    var s = server.createReadStream(options);
    s.on('error', function (err) {
      // for some reason we get this error
      if (err.toString() !== 'Error: unexpected disconnection') 
        aggregator.emit('error', err);
    });
    s.pipe(aggregator);
  });
  return aggregator;
};

LevelCluster.prototype.close = function (cb) {
  var serverKeys = Object.keys(this.servers);
  var next = after(serverKeys.length, cb);
  var self = this;
  serverKeys.forEach(function (key) {
    var db = self.servers[key];
    db.close(next);
  });
};

LevelCluster.prototype.connectAllServers = function () {
  var self = this;
  self._servers.forEach(function (serverName) {
    var server = self.getServerByName(serverName);
  });
};

LevelCluster.prototype.getServerNameByKey = function (key) {
  return this.ring.get(bytewise.encode(key));
};

LevelCluster.prototype.getServerByKey = function (key) {
  var server = this.getServerNameByKey(key);
  return this.getServerByName(server);
};

LevelCluster.prototype.getServerByName = function (server) {
  if (typeof this.servers[server] === 'undefined') {
    this.servers[server] = getServer(server);
  }
  return this.servers[server];
};

function getServer(server) {
  var parts = server.split(':');
  var port = parts[1];
  var db = multilevel.client();
  var con = net.connect(port);
  con.pipe(db.createRpcStream()).pipe(con);
  db.serverName = server;
  return db;
}
