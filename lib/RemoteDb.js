var $, Collection, RemoteDb, async, jQueryHttpClient, utils, _;

_ = require('lodash');

$ = require('jquery');

async = require('async');

utils = require('./utils');

jQueryHttpClient = require('./jQueryHttpClient');

module.exports = RemoteDb = (function() {
  function RemoteDb(url, client, httpClient) {
    this.url = url;
    this.client = client;
    this.collections = {};
    this.httpClient = httpClient || jQueryHttpClient;
  }

  RemoteDb.prototype.addCollection = function(name, options, success, error) {
    var collection, url, _ref;
    if (options == null) {
      options = {};
    }
    if (_.isFunction(options)) {
      _ref = [{}, options, success], options = _ref[0], success = _ref[1], error = _ref[2];
    }
    url = options.url || (this.url + name);
    collection = new Collection(name, url, this.client, this.httpClient);
    this[name] = collection;
    this.collections[name] = collection;
    if (success != null) {
      return success();
    }
  };

  RemoteDb.prototype.removeCollection = function(name, success, error) {
    delete this[name];
    delete this.collections[name];
    if (success != null) {
      return success();
    }
  };

  return RemoteDb;

})();

Collection = (function() {
  function Collection(name, url, client, httpClient) {
    this.name = name;
    this.url = url;
    this.client = client;
    this.httpClient = httpClient;
  }

  Collection.prototype.find = function(selector, options) {
    if (options == null) {
      options = {};
    }
    return {
      fetch: (function(_this) {
        return function(success, error) {
          var params;
          params = {};
          if (options.sort) {
            params.sort = JSON.stringify(options.sort);
          }
          if (options.limit) {
            params.limit = options.limit;
          }
          if (options.skip) {
            params.skip = options.skip;
          }
          if (options.fields) {
            params.fields = JSON.stringify(options.fields);
          }
          if (_this.client) {
            params.client = _this.client;
          }
          params.selector = JSON.stringify(selector || {});
          if ((typeof navigator !== "undefined" && navigator !== null) && navigator.userAgent.toLowerCase().indexOf('android 2.3') !== -1) {
            params._ = new Date().getTime();
          }
          return _this.httpClient("GET", _this.url, params, null, success, error);
        };
      })(this)
    };
  };

  Collection.prototype.findOne = function(selector, options, success, error) {
    var params, _ref;
    if (options == null) {
      options = {};
    }
    if (_.isFunction(options)) {
      _ref = [{}, options, success], options = _ref[0], success = _ref[1], error = _ref[2];
    }
    params = {};
    if (options.sort) {
      params.sort = JSON.stringify(options.sort);
    }
    params.limit = 1;
    if (this.client) {
      params.client = this.client;
    }
    params.selector = JSON.stringify(selector || {});
    if ((typeof navigator !== "undefined" && navigator !== null) && navigator.userAgent.toLowerCase().indexOf('android 2.3') !== -1) {
      params._ = new Date().getTime();
    }
    return this.httpClient("GET", this.url, params, null, function(results) {
      if (results && results.length > 0) {
        return success(results[0]);
      } else {
        return success(null);
      }
    }, error);
  };

  Collection.prototype.upsert = function(docs, bases, success, error) {
    var items, results, _ref;
    _ref = utils.regularizeUpsert(docs, bases, success, error), items = _ref[0], success = _ref[1], error = _ref[2];
    if (!this.client) {
      throw new Error("Client required to upsert");
    }
    results = [];
    return async.eachLimit(items, 8, (function(_this) {
      return function(item, cb) {
        var params;
        if (!item.doc._id) {
          item.doc._id = utils.createUid();
        }
        params = {
          client: _this.client
        };
        if ((typeof navigator !== "undefined" && navigator !== null) && navigator.userAgent.toLowerCase().indexOf('android 2.3') !== -1) {
          params._ = new Date().getTime();
        }
        if (item.base) {
          return _this.httpClient("PATCH", _this.url + "/" + item.doc._id, params, item, function(result) {
            results.push(result);
            return cb();
          }, function(err) {
            return cb(err);
          });
        } else {
          return _this.httpClient("POST", _this.url, params, item.doc, function(result) {
            results.push(result);
            return cb();
          }, function(err) {
            return cb(err);
          });
        }
      };
    })(this), function(err) {
      if (err) {
        if (error) {
          error(err);
        }
        return;
      }
      if (_.isArray(docs)) {
        if (success) {
          return success(results);
        }
      } else {
        if (success) {
          return success(results[0]);
        }
      }
    });
  };

  Collection.prototype.remove = function(id, success, error) {
    var params;
    if (!this.client) {
      throw new Error("Client required to remove");
    }
    params = {
      client: this.client
    };
    return this.httpClient("DELETE", this.url + "/" + id, params, null, success, function(err) {
      if (err.status === 410) {
        return success();
      } else {
        return error(err);
      }
    });
  };

  return Collection;

})();
