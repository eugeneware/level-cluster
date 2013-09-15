var expect = require('expect.js'),
    multilevel = require('multilevel'),
    levelup = require('levelup'),
    net = require('net'),
    path = require('path'),
    bytewise = require('bytewise/hex'),
    rimraf = require('rimraf'),
    after = require('after'),
    range = require('range').range,
    HashRing = require('hashring'),
    levelCluster = require('..');

describe('level-cluster', function() {
  var dbPath = path.join(__dirname, '..', 'data');
  function server(id, port, cb) {
    var dbOptions = { keyEncoding: bytewise, valueEncoding: 'json' };
    var _dbPath = path.join(dbPath, 'db' + id);
    rimraf.sync(_dbPath);

    var serverDb = levelup(_dbPath, dbOptions);
    net.createServer(function (con) {
      con.pipe(multilevel.server(serverDb)).pipe(con);
    }).listen(port, function (err) {
      if (err) return cb(err);
      cb(null, serverDb);
    });
  }

  it('should be able to spin up a multilevel instance', function(done) {
    var serverDb, clientDb;
    server(1, 3000, connect);
    function connect(err, _serverDb) {
      if (err) return done(err);
      serverDb = _serverDb;
      clientDb = multilevel.client();
      var con = net.connect(3000);
      con.pipe(clientDb.createRpcStream()).pipe(con);
      clientDb.put(['mykey', 123], { please: 'work' }, get);
    }

    function get(err) {
      if (err) return done(err);
      clientDb.get(['mykey', 123], check);
    }

    function check(err, value) {
      if (err) return done(err);
      expect(value).to.eql({ please: 'work' });
      cleanup();
    }

    function cleanup() {
      var next = after(2, done);
      clientDb.close(next);
      serverDb.close(next);
    }
  });

  it('should be able to spin up multiple servers', function(done) {
    var numServers = 3, servers = [];
    var next = after(numServers, cleanup);
    range(0, numServers).forEach(function (i) {
      server(i, 3000 + i, function (err, serverDb) {
        if (err) return next(err);
        servers[i] = serverDb;
        next();
      });
    });

    function cleanup(err) {
      var next = after(numServers, done);
      servers.forEach(function (serverDb) {
        serverDb.close(next);
      });
    }
  });

  it.only('should be able to write to multiple servers', function(done) {
    var numServers = 3, servers = [];
    var next = after(numServers, write);
    var ring = new HashRing();
    range(0, numServers).forEach(function (i) {
      var port = 3000 + i;
      server(i, port, function (err, serverDb) {
        if (err) return next(err);
        servers[i] = serverDb;
        ring.add('127.0.0.1:' + port);
        next();
      });
    });

    var key = ['mykey', 123];
    var value = { please: 'work' };
    var db;

    function write(err) {
      if (err) return done(err);
      var server = ring.get(bytewise.encode(key)).split(':');
      var port = server[1];
      db = multilevel.client();
      var con = net.connect(port);
      con.pipe(db.createRpcStream()).pipe(con);
      db.put(key, value, function (err) {
        if (err) return done(err);
        db.close(get);
      });
    }

    function get(err) {
      if (err) return done(err);
      var server = ring.get(bytewise.encode(key)).split(':');
      var port = server[1];
      db = multilevel.client();
      var con = net.connect(port);
      con.pipe(db.createRpcStream()).pipe(con);
      db.get(key, check);
    }

    function check(err, _value) {
      if (err) return done(err);
      expect(value).to.eql(_value);
      cleanup();
    }

    function cleanup(err) {
      var next = after(numServers, done);
      servers.forEach(function (serverDb) {
        serverDb.close(next);
      });
    }
  });
});
