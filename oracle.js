var oracledb = require('orawrap');//require('oracledb');
var Q = require('q');

oracledb.fetchAsString = [oracledb.DATE];

module.exports = function (logger) {
    var oracle = {
        pool: null,

        getConnectionPool: function (params) {
            var start_time = Date.now(),
                self = this;

            return Q.Promise(function (resolve, reject) {
                if (self.pool === null) {
                    logger.debug(params.sid + " Oracle: creating connection pool - " + params['connectStr']);
                    oracledb.createPool({
                            user: params['user'],
                            password: params['pass'],
                            connectString: params['connectStr'],
                            poolMax: 10,
                            poolMin: 0,
                            poolIncrement: 1,
                            poolTimeout: 180
                        }, function (err, connectionPool) {
                            if (err) {
                                return reject(err);
                            }

                            self.pool = connectionPool;
                            var connection_time = Date.now();
                            logger.debug(params.sid + " Oracle: preparing pool time: " + (connection_time - start_time) + "ms");

                            return resolve(connectionPool);
                        }
                    );
                } else {
                    return resolve(self.pool);
                }
            });
        },

        releaseConnection: function (params, connection) {
            connection.release(function (err) {
                if (err) {
                    return logger.log('error', err, params.sid);
                }
                logger.debug(params.sid + " Oracle: releasing connection ...");
            });
        },

        executeQuery: function (params, sql, connectionPool) {
            var self = this;

            return Q.Promise(function (resolve, reject) {
                connectionPool.getConnection(function (err, connection) {
                    if (err) {
                        return reject(err);
                    }

                    connection.execute(sql, function (error, data) {
                        if (error) {
                            self.releaseConnection(params, connection);
                            return reject(error);
                        }

                        self.releaseConnection(params, connection);

                        return resolve(data);
                    });
                });
            });
        },

        init: function (params, sql) {
            return Q.Promise(function (resolve, reject) {
                this.getConnectionPool(params)
                    .then(this.executeQuery.bind(this, params, sql))
                    .then(resolve).catch(reject).done();
            }.bind(this));
        }
    };

    return function (params) {
        var sql = "Some Select Query";
        return oracle.init(params, sql);
    }
};

