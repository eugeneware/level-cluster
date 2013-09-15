var expect = require('expect.js'),
    multilevel = require('multilevel'),
    levelup = require('levelup'),
    net = require('net'),
    path = require('path'),
    bytewise = require('bytewise/hex'),
    rimraf = require('rimraf'),
    after = require('after'),
    levelCluster = require('..');

describe('level-cluster', function() {
  var dbPath = path.join(__dirname, '..', 'data');

  it('should be able to spin up a multilevel instance', function(done) {
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
});
