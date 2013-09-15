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
    through = require('through'),
    LevelCluster = require('..');

describe('level-cluster', function() {
  var dbPath = path.join(__dirname, '..', 'data');
  var cluster;
  var numServers = 3;
  var clusterPortStart = 3000;

  function server(id, port, cb) {
    var dbOptions = { keyEncoding: bytewise, valueEncoding: 'json' };
    var _dbPath = path.join(dbPath, 'db' + (id + port));
    rimraf.sync(_dbPath);

    var serverDb = levelup(_dbPath, dbOptions);
    var tcpServer = net.createServer(function (con) {
      con.pipe(multilevel.server(serverDb)).pipe(con);
    }).listen(port, function (err) {
      if (err) return cb(err);
      cb(null, {
        db: serverDb,
        server: tcpServer,
        close: cleanup
      });
    });

    function cleanup(cb) {
      var next = after(2, cb);
      serverDb.close(next);
      tcpServer.close(next);
    }
  }

  function startCluster(numServers, cb) {
    var servers = [];
    var next = after(numServers, finish);
    range(0, numServers).forEach(function (i) {
      var port = clusterPortStart + i;
      server(i, port, function (err, server) {
        if (err) return next(err);
        server.serverName = '127.0.0.1:' + port;
        servers[i] = server;
        next();
      });
    });

    function finish(err) {
      if (err) return cb(err);
      cb(null, { close: cleanup });
    }

    function cleanup(cb) {
      var next = after(servers.length, cb);
      servers.forEach(function (server) {
        server.close(next);
      });
    }
  }

  beforeEach(function(done) {
    startCluster(numServers, function (err, _cluster) {
      if (err) return done(err);
      cluster = _cluster;
      done();
    });
  });

  afterEach(function(done) {
    cluster.close(done);
  });

  it('should be able to spin up a multilevel instance', function(done) {
    var clientDb, dbServer;
    server(1, 4000, connect);
    function connect(err, server) {
      if (err) return done(err);
      clientDb = multilevel.client();
      dbServer = server;
      var con = net.connect(4000);
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
      dbServer.close(next);
    }
  });

  it('should be able to write to multiple servers', function(done) {
    var ring = new HashRing();
    range(0, numServers).forEach(function (i) {
      ring.add('127.0.0.1:' + (clusterPortStart + i));
    });

    var key = ['mykey', 123];
    var value = { please: 'work' };
    var db;

    write();

    function write() {
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
      db.close(done);
    }
  });

  it('should use the level cluster object', function(done) {
    var servers = range(0, numServers).map(function (i) {
      return '127.0.0.1:' + (clusterPortStart + i);
    });
    var key = ['mykey', 123];
    var value = { please: 'work' };
    var db = new LevelCluster(servers);
    db.put(key, value, get);

    function get(err) {
      if (err) return done(err);
      db.get(key, check);
    }

    function check(err, _value) {
      if (err) return done(err);
      expect(value).to.eql(_value);
      cleanup();
    }

    function cleanup() {
      db.close(done);
    }
  });

  it('should be able to cluster batch operations', function(done) {
    var servers = range(0, numServers).map(function (i) {
      return '127.0.0.1:' + (clusterPortStart + i);
    });
    var db = new LevelCluster(servers);

    var batch = range(0, 10).map(function (i) {
      return {
        type: 'put',
        key: ['key', i],
        value: {
          val: 'value ' + i
        }
      };
    });

    db.batch(batch, get);

    function get(err) {
      if (err) return done(err);
      db.get(['key', 3], check);
    }

    function check(err, _value) {
      if (err) return done(err);
      expect(_value).to.eql({ val: 'value 3' });
      db.close(done);
    }
  });

  it('should be able to create a read stream', function(done) {
    var servers = range(0, numServers).map(function (i) {
      return '127.0.0.1:' + (clusterPortStart + i);
    });
    var db = new LevelCluster(servers);

    var batch = range(0, 10).map(function (i) {
      return {
        type: 'put',
        key: ['key', i],
        value: {
          val: 'value ' + i
        }
      };
    });

    db.batch(batch, stream);

    var count = 0;
    function stream(err) {
      if (err) return done(err);
      var s = db.createReadStream();
      s.pipe(through(write, finish));
    }

    function write(data) {
      expect(data.key[0]).to.equal('key');
      expect(data.key[1]).to.not.be.below(0);
      expect(data.value.val).to.match(/^value [0-9]+$/);
      count++;
    }

    function finish() {
      expect(count).to.equal(10);
      db.close(done);
    }
  });
});
