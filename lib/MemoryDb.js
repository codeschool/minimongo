var Collection, MemoryDb, compileSort, processAggregate, processFind, utils, _,
  __slice = [].slice;

_ = require('lodash');

utils = require('./utils');

processFind = require('./utils').processFind;

processAggregate = require('./utils').processAggregate;

compileSort = require('./selector').compileSort;

module.exports = MemoryDb = (function() {
  function MemoryDb(options, success) {
    this.collections = {};
    if (success) {
      success(this);
    }
  }

  MemoryDb.prototype.addCollection = function(name, success, error) {
    var collection;
    collection = new Collection(name);
    this[name] = collection;
    this.collections[name] = collection;
    if (success != null) {
      return success();
    }
  };

  MemoryDb.prototype.createCollection = function(name, success, error) {
    var collection;
    collection = new Collection(name);
    this[name] = collection;
    this.collections[name] = collection;
    if (success != null) {
      return success();
    }
  };

  MemoryDb.prototype.removeCollection = function(name, success, error) {
    delete this[name];
    delete this.collections[name];
    if (success != null) {
      return success();
    }
  };

  return MemoryDb;

})();

Collection = (function() {
  function Collection(name) {
    this.name = name;
    this.items = {};
    this.upserts = {};
    this.removes = {};
  }

  Collection.prototype.find = function(selector, options) {
    return this._findFetch(selector, options);
  };

  Collection.prototype.findOne = function(selector, options, success, error) {
    var results, _ref;
    if (_.isFunction(options)) {
      _ref = [{}, options, success], options = _ref[0], success = _ref[1], error = _ref[2];
    }
    results = this.find(selector, options);
    if (!!results) {
      return results[0];
    } else {
      return null;
    }
  };

  Collection.prototype._findFetch = function(selector, options, success, error) {
    return processFind(this.items, selector, options);
  };

  Collection.prototype.upsert = function(docs, bases, success, error) {
    var item, items, _i, _len, _ref;
    _ref = utils.regularizeUpsert(docs, bases, success, error), items = _ref[0], success = _ref[1], error = _ref[2];
    for (_i = 0, _len = items.length; _i < _len; _i++) {
      item = items[_i];
      if (item.base === void 0) {
        if (this.upserts[item.doc._id]) {
          item.base = this.upserts[item.doc._id].base;
        } else {
          item.base = this.items[item.doc._id] || null;
        }
      }
      item = _.cloneDeep(item);
      this.items[item.doc._id] = item.doc;
      this.upserts[item.doc._id] = item;
    }
    if (success) {
      return success(docs);
    }
  };

  Collection.prototype.insert = function(docs, bases, success, error) {
    var item, items, _i, _len, _ref, _results;
    _ref = utils.regularizeUpsert(docs, bases, success, error), items = _ref[0], success = _ref[1], error = _ref[2];
    _results = [];
    for (_i = 0, _len = items.length; _i < _len; _i++) {
      item = items[_i];
      if (item.base === void 0) {
        item.base = this.items[item.doc._id] || null;
      }
      item = _.cloneDeep(item);
      if (!this.items[item.doc._id]) {
        _results.push(this.items[item.doc._id] = item.doc);
      } else {
        throw 'Duplicate ID';
      }
    }
    return _results;
  };

  Collection.prototype.update = function(selector, docs, bases, success, error) {
    var item, k, theItems, v, _i, _len, _results;
    theItems = processFind(this.items, selector);
    _results = [];
    for (_i = 0, _len = theItems.length; _i < _len; _i++) {
      item = theItems[_i];
      if (item.docs === void 0) {
        item.doc = docs;
      }
      if (item.base === void 0) {
        item.base = this.items[item.doc._id] || null;
      }
      item = _.cloneDeep(item);
      _results.push((function() {
        var _results1;
        _results1 = [];
        for (k in docs) {
          v = docs[k];
          _results1.push(this.items[item._id][k] = docs[k]);
        }
        return _results1;
      }).call(this));
    }
    return _results;
  };

  Collection.prototype.aggregate = function() {
    var remaining, select, selectors, _i, _len;
    selectors = 1 <= arguments.length ? __slice.call(arguments, 0) : [];
    remaining = [];
    if (selectors.length > 1) {
      for (_i = 0, _len = selectors.length; _i < _len; _i++) {
        select = selectors[_i];
        remaining.push(select);
      }
      selectors = remaining;
    } else {
      selectors = selectors[0];
    }
    return processAggregate(this.items, selectors);
  };

  Collection.prototype.remove = function(id, success, error) {
    if (_.has(this.items, id)) {
      this.removes[id] = this.items[id];
      delete this.items[id];
      delete this.upserts[id];
    } else {
      this.removes[id] = {
        _id: id
      };
    }
    if (success != null) {
      return success();
    }
  };

  Collection.prototype.cache = function(docs, selector, options, success, error) {
    var doc, docsMap, sort, _i, _len;
    for (_i = 0, _len = docs.length; _i < _len; _i++) {
      doc = docs[_i];
      this.cacheOne(doc);
    }
    docsMap = _.object(_.pluck(docs, "_id"), docs);
    if (options.sort) {
      sort = compileSort(options.sort);
    }
    return this.find(selector, options).fetch((function(_this) {
      return function(results) {
        var result, _j, _len1;
        for (_j = 0, _len1 = results.length; _j < _len1; _j++) {
          result = results[_j];
          if (!docsMap[result._id] && !_.has(_this.upserts, result._id)) {
            if (options.sort && options.limit && docs.length === options.limit) {
              if (sort(result, _.last(docs)) >= 0) {
                continue;
              }
            }
            delete _this.items[result._id];
          }
        }
        if (success != null) {
          return success();
        }
      };
    })(this), error);
  };

  Collection.prototype.pendingUpserts = function(success) {
    return success(_.values(this.upserts));
  };

  Collection.prototype.pendingRemoves = function(success) {
    return success(_.pluck(this.removes, "_id"));
  };

  Collection.prototype.resolveUpserts = function(upserts, success) {
    var id, upsert, _i, _len;
    for (_i = 0, _len = upserts.length; _i < _len; _i++) {
      upsert = upserts[_i];
      id = upsert.doc._id;
      if (this.upserts[id]) {
        if (_.isEqual(upsert.doc, this.upserts[id].doc)) {
          delete this.upserts[id];
        } else {
          this.upserts[id].base = upsert.doc;
        }
      }
    }
    if (success != null) {
      return success();
    }
  };

  Collection.prototype.resolveRemove = function(id, success) {
    delete this.removes[id];
    if (success != null) {
      return success();
    }
  };

  Collection.prototype.seed = function(docs, success) {
    var doc, _i, _len;
    if (!_.isArray(docs)) {
      docs = [docs];
    }
    for (_i = 0, _len = docs.length; _i < _len; _i++) {
      doc = docs[_i];
      if (!_.has(this.items, doc._id) && !_.has(this.removes, doc._id)) {
        this.items[doc._id] = doc;
      }
    }
    if (success != null) {
      return success();
    }
  };

  Collection.prototype.cacheOne = function(doc, success) {
    var existing;
    if (!_.has(this.upserts, doc._id) && !_.has(this.removes, doc._id)) {
      existing = this.items[doc._id];
      if (!existing || !doc._rev || !existing._rev || doc._rev >= existing._rev) {
        this.items[doc._id] = doc;
      }
    }
    if (success != null) {
      return success();
    }
  };

  return Collection;

})();
