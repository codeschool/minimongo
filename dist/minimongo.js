require=(function e(t,n,r){function s(o,u){if(!n[o]){if(!t[o]){var a=typeof require=="function"&&require;if(!u&&a)return a(o,!0);if(i)return i(o,!0);throw new Error("Cannot find module '"+o+"'")}var f=n[o]={exports:{}};t[o][0].call(f.exports,function(e){var n=t[o][1][e];return s(n?n:e)},f,f.exports,e,t,n,r)}return n[o].exports}var i=typeof require=="function"&&require;for(var o=0;o<r.length;o++)s(r[o]);return s})({"Focm2+":[function(require,module,exports){
exports.MemoryDb = require('./lib/MemoryDb');
exports.utils = require('./lib/utils');

},{"./lib/MemoryDb":6,"./lib/utils":8}],"minimongo":[function(require,module,exports){
module.exports=require('Focm2+');
},{}],"jquery":[function(require,module,exports){
module.exports=require('/nayFu');
},{}],"/nayFu":[function(require,module,exports){
module.exports = window.$;

},{}],5:[function(require,module,exports){
var _ = require('lodash');

EJSON = {}; // Global!
var customTypes = {};
// Add a custom type, using a method of your choice to get to and
// from a basic JSON-able representation.  The factory argument
// is a function of JSON-able --> your object
// The type you add must have:
// - A clone() method, so that Meteor can deep-copy it when necessary.
// - A equals() method, so that Meteor can compare it
// - A toJSONValue() method, so that Meteor can serialize it
// - a typeName() method, to show how to look it up in our type table.
// It is okay if these methods are monkey-patched on.
EJSON.addType = function (name, factory) {
  if (_.has(customTypes, name))
    throw new Error("Type " + name + " already present");
  customTypes[name] = factory;
};

var builtinConverters = [
  { // Date
    matchJSONValue: function (obj) {
      return _.has(obj, '$date') && _.size(obj) === 1;
    },
    matchObject: function (obj) {
      return obj instanceof Date;
    },
    toJSONValue: function (obj) {
      return {$date: obj.getTime()};
    },
    fromJSONValue: function (obj) {
      return new Date(obj.$date);
    }
  },
  { // Binary
    matchJSONValue: function (obj) {
      return _.has(obj, '$binary') && _.size(obj) === 1;
    },
    matchObject: function (obj) {
      return typeof Uint8Array !== 'undefined' && obj instanceof Uint8Array
        || (obj && _.has(obj, '$Uint8ArrayPolyfill'));
    },
    toJSONValue: function (obj) {
      return {$binary: EJSON._base64Encode(obj)};
    },
    fromJSONValue: function (obj) {
      return EJSON._base64Decode(obj.$binary);
    }
  },
  { // Escaping one level
    matchJSONValue: function (obj) {
      return _.has(obj, '$escape') && _.size(obj) === 1;
    },
    matchObject: function (obj) {
      if (_.isEmpty(obj) || _.size(obj) > 2) {
        return false;
      }
      return _.any(builtinConverters, function (converter) {
        return converter.matchJSONValue(obj);
      });
    },
    toJSONValue: function (obj) {
      var newObj = {};
      _.each(obj, function (value, key) {
        newObj[key] = EJSON.toJSONValue(value);
      });
      return {$escape: newObj};
    },
    fromJSONValue: function (obj) {
      var newObj = {};
      _.each(obj.$escape, function (value, key) {
        newObj[key] = EJSON.fromJSONValue(value);
      });
      return newObj;
    }
  },
  { // Custom
    matchJSONValue: function (obj) {
      return _.has(obj, '$type') && _.has(obj, '$value') && _.size(obj) === 2;
    },
    matchObject: function (obj) {
      return EJSON._isCustomType(obj);
    },
    toJSONValue: function (obj) {
      return {$type: obj.typeName(), $value: obj.toJSONValue()};
    },
    fromJSONValue: function (obj) {
      var typeName = obj.$type;
      var converter = customTypes[typeName];
      return converter(obj.$value);
    }
  }
];

EJSON._isCustomType = function (obj) {
  return obj &&
    typeof obj.toJSONValue === 'function' &&
    typeof obj.typeName === 'function' &&
    _.has(customTypes, obj.typeName());
};


//for both arrays and objects, in-place modification.
var adjustTypesToJSONValue =
EJSON._adjustTypesToJSONValue = function (obj) {
  if (obj === null)
    return null;
  var maybeChanged = toJSONValueHelper(obj);
  if (maybeChanged !== undefined)
    return maybeChanged;
  _.each(obj, function (value, key) {
    if (typeof value !== 'object' && value !== undefined)
      return; // continue
    var changed = toJSONValueHelper(value);
    if (changed) {
      obj[key] = changed;
      return; // on to the next key
    }
    // if we get here, value is an object but not adjustable
    // at this level.  recurse.
    adjustTypesToJSONValue(value);
  });
  return obj;
};

// Either return the JSON-compatible version of the argument, or undefined (if
// the item isn't itself replaceable, but maybe some fields in it are)
var toJSONValueHelper = function (item) {
  for (var i = 0; i < builtinConverters.length; i++) {
    var converter = builtinConverters[i];
    if (converter.matchObject(item)) {
      return converter.toJSONValue(item);
    }
  }
  return undefined;
};

EJSON.toJSONValue = function (item) {
  var changed = toJSONValueHelper(item);
  if (changed !== undefined)
    return changed;
  if (typeof item === 'object') {
    item = EJSON.clone(item);
    adjustTypesToJSONValue(item);
  }
  return item;
};

//for both arrays and objects. Tries its best to just
// use the object you hand it, but may return something
// different if the object you hand it itself needs changing.
var adjustTypesFromJSONValue =
EJSON._adjustTypesFromJSONValue = function (obj) {
  if (obj === null)
    return null;
  var maybeChanged = fromJSONValueHelper(obj);
  if (maybeChanged !== obj)
    return maybeChanged;
  _.each(obj, function (value, key) {
    if (typeof value === 'object') {
      var changed = fromJSONValueHelper(value);
      if (value !== changed) {
        obj[key] = changed;
        return;
      }
      // if we get here, value is an object but not adjustable
      // at this level.  recurse.
      adjustTypesFromJSONValue(value);
    }
  });
  return obj;
};

// Either return the argument changed to have the non-json
// rep of itself (the Object version) or the argument itself.

// DOES NOT RECURSE.  For actually getting the fully-changed value, use
// EJSON.fromJSONValue
var fromJSONValueHelper = function (value) {
  if (typeof value === 'object' && value !== null) {
    if (_.size(value) <= 2
        && _.all(value, function (v, k) {
          return typeof k === 'string' && k.substr(0, 1) === '$';
        })) {
      for (var i = 0; i < builtinConverters.length; i++) {
        var converter = builtinConverters[i];
        if (converter.matchJSONValue(value)) {
          return converter.fromJSONValue(value);
        }
      }
    }
  }
  return value;
};

EJSON.fromJSONValue = function (item) {
  var changed = fromJSONValueHelper(item);
  if (changed === item && typeof item === 'object') {
    item = EJSON.clone(item);
    adjustTypesFromJSONValue(item);
    return item;
  } else {
    return changed;
  }
};

EJSON.stringify = function (item) {
  return JSON.stringify(EJSON.toJSONValue(item));
};

EJSON.parse = function (item) {
  return EJSON.fromJSONValue(JSON.parse(item));
};

EJSON.isBinary = function (obj) {
  return (typeof Uint8Array !== 'undefined' && obj instanceof Uint8Array) ||
    (obj && obj.$Uint8ArrayPolyfill);
};

EJSON.equals = function (a, b, options) {
  var i;
  var keyOrderSensitive = !!(options && options.keyOrderSensitive);
  if (a === b)
    return true;
  if (!a || !b) // if either one is falsy, they'd have to be === to be equal
    return false;
  if (!(typeof a === 'object' && typeof b === 'object'))
    return false;
  if (a instanceof Date && b instanceof Date)
    return a.valueOf() === b.valueOf();
  if (EJSON.isBinary(a) && EJSON.isBinary(b)) {
    if (a.length !== b.length)
      return false;
    for (i = 0; i < a.length; i++) {
      if (a[i] !== b[i])
        return false;
    }
    return true;
  }
  if (typeof (a.equals) === 'function')
    return a.equals(b, options);
  if (a instanceof Array) {
    if (!(b instanceof Array))
      return false;
    if (a.length !== b.length)
      return false;
    for (i = 0; i < a.length; i++) {
      if (!EJSON.equals(a[i], b[i], options))
        return false;
    }
    return true;
  }
  // fall back to structural equality of objects
  var ret;
  if (keyOrderSensitive) {
    var bKeys = [];
    _.each(b, function (val, x) {
        bKeys.push(x);
    });
    i = 0;
    ret = _.all(a, function (val, x) {
      if (i >= bKeys.length) {
        return false;
      }
      if (x !== bKeys[i]) {
        return false;
      }
      if (!EJSON.equals(val, b[bKeys[i]], options)) {
        return false;
      }
      i++;
      return true;
    });
    return ret && i === bKeys.length;
  } else {
    i = 0;
    ret = _.all(a, function (val, key) {
      if (!_.has(b, key)) {
        return false;
      }
      if (!EJSON.equals(val, b[key], options)) {
        return false;
      }
      i++;
      return true;
    });
    return ret && _.size(b) === i;
  }
};

EJSON.clone = function (v) {
  var ret;
  if (typeof v !== "object")
    return v;
  if (v === null)
    return null; // null has typeof "object"
  if (v instanceof Date)
    return new Date(v.getTime());
  if (EJSON.isBinary(v)) {
    ret = EJSON.newBinary(v.length);
    for (var i = 0; i < v.length; i++) {
      ret[i] = v[i];
    }
    return ret;
  }
  if (_.isArray(v) || _.isArguments(v)) {
    // For some reason, _.map doesn't work in this context on Opera (weird test
    // failures).
    ret = [];
    for (i = 0; i < v.length; i++)
      ret[i] = EJSON.clone(v[i]);
    return ret;
  }
  // handle general user-defined typed Objects if they have a clone method
  if (typeof v.clone === 'function') {
    return v.clone();
  }
  // handle other objects
  ret = {};
  _.each(v, function (value, key) {
    ret[key] = EJSON.clone(value);
  });
  return ret;
};

module.exports = EJSON;

},{"lodash":"nJZoxB"}],6:[function(require,module,exports){
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

  MemoryDb.prototype.addCollection = function(name) {
    var collection;
    collection = new Collection(name);
    this[name] = collection;
    return this.collections[name] = collection;
  };

  MemoryDb.prototype.createCollection = function(name) {
    var collection;
    collection = new Collection(name);
    this[name] = collection;
    return this.collections[name] = collection;
  };

  MemoryDb.prototype.removeCollection = function(name) {
    delete this[name];
    return delete this.collections[name];
  };

  return MemoryDb;

})();

Collection = (function() {
  function Collection(name) {
    this.name = name;
    this.items = {};
    this.founds = {};
    this.updates = {};
    this.upserts = {};
    this.removes = {};
  }

  Collection.prototype.find = function(selector, options) {
    return this._findFetch(selector, options);
  };

  Collection.prototype.findOne = function(selector, options) {
    var results;
    results = this.find(selector, options);
    if (!!results) {
      return results[0];
    } else {
      return null;
    }
  };

  Collection.prototype._findFetch = function(selector, options) {
    return processFind(this.items, selector, options);
  };

  Collection.prototype.upsert = function(docs, bases) {
    var item, items, _i, _len, _results;
    items = utils.regularizeUpsert(docs, bases)[0];
    _results = [];
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
      _results.push(this.upserts[item.doc._id] = item);
    }
    return _results;
  };

  Collection.prototype.insert = function(docs, bases) {
    var item, items, _i, _len;
    if ((_.isEmpty(docs) && typeof docs !== 'object') || typeof docs === 'string') {
      throw {
        message: "Error: no object passed to insert"
      };
    }
    items = utils.regularizeUpsert(docs, bases)[0];
    for (_i = 0, _len = items.length; _i < _len; _i++) {
      item = items[_i];
      if (item.base === void 0) {
        item.base = this.items[item.doc._id] || null;
      }
      item = _.cloneDeep(item);
      if (!this.items[item.doc._id]) {
        this.items[item.doc._id] = item.doc;
      } else {
        throw 'Duplicate ID';
      }
    }
    return null;
  };

  Collection.prototype.update = function(selector, docs, bases) {
    var me, theItems;
    if ((_.isEmpty(selector) && _.isEmpty(docs) && typeof selector !== 'object' && typeof docs !== 'object') || !docs) {
      throw {
        message: "Error: no object passed to update"
      };
    }
    theItems = processFind(this.items, selector);
    me = this;
    _.map(theItems, function(item) {
      return me.founds[item._id] = docs;
    });
    if (!_.isEmpty(docs)) {
      return utils.processUpdate(theItems, selector, docs, bases, this);
    }
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

  Collection.prototype.remove = function(selector, options) {
    var found, id, ids, _i, _len, _results;
    found = processFind(this.items, selector, options);
    ids = _.collect(found, '_id');
    _results = [];
    for (_i = 0, _len = ids.length; _i < _len; _i++) {
      id = ids[_i];
      this.removes[id] = this.items[id];
      _results.push(this.items = _.omit(this.items, id));
    }
    return _results;
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

},{"./selector":7,"./utils":8,"lodash":"nJZoxB"}],7:[function(require,module,exports){
/*
========================================
Meteor is licensed under the MIT License
========================================

Copyright (C) 2011--2012 Meteor Development Group

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.


====================================================================
This license applies to all code in Meteor that is not an externally
maintained library. Externally maintained libraries have their own
licenses, included below:
====================================================================

*/

LocalCollection = {};
EJSON = require("./EJSON");
var _ = require('lodash');


// Like _.isArray, but doesn't regard polyfilled Uint8Arrays on old browsers as
// arrays.
var isArray = function (x) {
  return _.isArray(x) && !EJSON.isBinary(x);
};

var _anyIfArray = function (x, f) {
  if (isArray(x))
    return _.any(x, f);
  return f(x);
};

var _anyIfArrayPlus = function (x, f) {
  if (f(x))
    return true;
  return isArray(x) && _.any(x, f);
};

var hasOperators = function(valueSelector) {
  var theseAreOperators = undefined;
  for (var selKey in valueSelector) {
    var thisIsOperator = selKey.substr(0, 1) === '$';
    if (theseAreOperators === undefined) {
      theseAreOperators = thisIsOperator;
    } else if (theseAreOperators !== thisIsOperator) {
      throw new Error("Inconsistent selector: " + valueSelector);
    }
  }
  return !!theseAreOperators;  // {} has no operators
};

var compileValueSelector = function (valueSelector) {
  if (valueSelector == null) {  // undefined or null
    return function (value) {
      return _anyIfArray(value, function (x) {
        return x == null;  // undefined or null
      });
    };
  }

  // Selector is a non-null primitive (and not an array or RegExp either).
  if (!_.isObject(valueSelector)) {
    return function (value) {
      return _anyIfArray(value, function (x) {
        return x === valueSelector;
      });
    };
  }

  if (valueSelector instanceof RegExp) {
    return function (value) {
      if (value === undefined)
        return false;
      return _anyIfArray(value, function (x) {
        return valueSelector.test(x);
      });
    };
  }

  // Arrays match either identical arrays or arrays that contain it as a value.
  if (isArray(valueSelector)) {
    return function (value) {
      if (!isArray(value))
        return false;
      return _anyIfArrayPlus(value, function (x) {
        return LocalCollection._f._equal(valueSelector, x);
      });
    };
  }

  // It's an object, but not an array or regexp.
  if (hasOperators(valueSelector)) {
    var operatorFunctions = [];
    _.each(valueSelector, function (operand, operator) {
      if (!_.has(VALUE_OPERATORS, operator))
        throw new Error("Unrecognized operator: " + operator);
      operatorFunctions.push(VALUE_OPERATORS[operator](
        operand, valueSelector.$options));
    });
    return function (value) {
      return _.all(operatorFunctions, function (f) {
        return f(value);
      });
    };
  }

  // It's a literal; compare value (or element of value array) directly to the
  // selector.
  return function (value) {
    return _anyIfArray(value, function (x) {
      return LocalCollection._f._equal(valueSelector, x);
    });
  };
};

// XXX can factor out common logic below
var LOGICAL_OPERATORS = {
  "$and": function(subSelector) {
    if (!isArray(subSelector) || _.isEmpty(subSelector))
      throw Error("$and/$or/$nor must be nonempty array");
    var subSelectorFunctions = _.map(
      subSelector, compileDocumentSelector);
    return function (doc) {
      return _.all(subSelectorFunctions, function (f) {
        return f(doc);
      });
    };
  },

  "$or": function(subSelector) {
    if (!isArray(subSelector) || _.isEmpty(subSelector))
      throw Error("$and/$or/$nor must be nonempty array");
    var subSelectorFunctions = _.map(
      subSelector, compileDocumentSelector);
    return function (doc) {
      return _.any(subSelectorFunctions, function (f) {
        return f(doc);
      });
    };
  },

  "$nor": function(subSelector) {
    if (!isArray(subSelector) || _.isEmpty(subSelector))
      throw Error("$and/$or/$nor must be nonempty array");
    var subSelectorFunctions = _.map(
      subSelector, compileDocumentSelector);
    return function (doc) {
      return _.all(subSelectorFunctions, function (f) {
        return !f(doc);
      });
    };
  },

  "$where": function(selectorValue) {
    if (!(selectorValue instanceof Function)) {
      selectorValue = Function("return " + selectorValue);
    }
    return function (doc) {
      return selectorValue.call(doc);
    };
  }
};

var VALUE_OPERATORS = {

  "$group": function (operand) {
    if (!isArray(operand))
      throw new Error("Argument to $in must be array");
    return function (value) {
      return _anyIfArrayPlus(value, function (x) {
        return _.any(operand, function (operandElt) {
          return LocalCollection._f._equal(operandElt, x);
        });
      });
    };
  },

  "$match": function (operand) {
    if (!isArray(operand))
      throw new Error("Argument to $in must be array");
    return function (value) {
      return _anyIfArrayPlus(value, function (x) {
        return _.any(operand, function (operandElt) {
          return LocalCollection._f._equal(operandElt, x);
        });
      });
    };
  },

  "$in": function (operand) {
    if (!isArray(operand))
      throw new Error("Argument to $in must be array");
    return function (value) {
      return _anyIfArrayPlus(value, function (x) {
        return _.any(operand, function (operandElt) {
          return LocalCollection._f._equal(operandElt, x);
        });
      });
    };
  },

  "$all": function (operand) {
    if (!isArray(operand))
      throw new Error("Argument to $all must be array");
    return function (value) {
      if (!isArray(value))
        return false;
      return _.all(operand, function (operandElt) {
        return _.any(value, function (valueElt) {
          return LocalCollection._f._equal(operandElt, valueElt);
        });
      });
    };
  },

  "$lt": function (operand) {
    return function (value) {
      return _anyIfArray(value, function (x) {
        return LocalCollection._f._cmp(x, operand) < 0;
      });
    };
  },

  "$lte": function (operand) {
    return function (value) {
      return _anyIfArray(value, function (x) {
        return LocalCollection._f._cmp(x, operand) <= 0;
      });
    };
  },

  "$gt": function (operand) {
    return function (value) {
      return _anyIfArray(value, function (x) {
        return LocalCollection._f._cmp(x, operand) > 0;
      });
    };
  },

  "$gte": function (operand) {
    return function (value) {
      return _anyIfArray(value, function (x) {
        return LocalCollection._f._cmp(x, operand) >= 0;
      });
    };
  },

  "$ne": function (operand) {
    return function (value) {
      return ! _anyIfArrayPlus(value, function (x) {
        return LocalCollection._f._equal(x, operand);
      });
    };
  },

  "$nin": function (operand) {
    if (!isArray(operand))
      throw new Error("Argument to $nin must be array");
    var inFunction = VALUE_OPERATORS.$in(operand);
    return function (value) {
      // Field doesn't exist, so it's not-in operand
      if (value === undefined)
        return true;
      return !inFunction(value);
    };
  },

  "$exists": function (operand) {
    return function (value) {
      return operand === (value !== undefined);
    };
  },

  "$mod": function (operand) {
    var divisor = operand[0],
        remainder = operand[1];
    return function (value) {
      return _anyIfArray(value, function (x) {
        return x % divisor === remainder;
      });
    };
  },

  "$size": function (operand) {
    return function (value) {
      return isArray(value) && operand === value.length;
    };
  },

  "$type": function (operand) {
    return function (value) {
      // A nonexistent field is of no type.
      if (value === undefined)
        return false;
      // Definitely not _anyIfArrayPlus: $type: 4 only matches arrays that have
      // arrays as elements according to the Mongo docs.
      return _anyIfArray(value, function (x) {
        return LocalCollection._f._type(x) === operand;
      });
    };
  },

  "$regex": function (operand, options) {
    if (options !== undefined) {
      // Options passed in $options (even the empty string) always overrides
      // options in the RegExp object itself.

      // Be clear that we only support the JS-supported options, not extended
      // ones (eg, Mongo supports x and s). Ideally we would implement x and s
      // by transforming the regexp, but not today...
      if (/[^gim]/.test(options))
        throw new Error("Only the i, m, and g regexp options are supported");

      var regexSource = operand instanceof RegExp ? operand.source : operand;
      operand = new RegExp(regexSource, options);
    } else if (!(operand instanceof RegExp)) {
      operand = new RegExp(operand);
    }

    return function (value) {
      if (value === undefined)
        return false;
      return _anyIfArray(value, function (x) {
        return operand.test(x);
      });
    };
  },

  "$options": function (operand) {
    // evaluation happens at the $regex function above
    return function (value) { return true; };
  },

  "$elemMatch": function (operand) {
    if(_.isEmpty(operand)){
      return function(value){ return false  }
    }
    var matcher = compileDocumentSelector(operand);
    return function (value) {
      if (!isArray(value))
        return false;
      return _.any(value, function (x) {
        return matcher(x);
      });
    };
  },

  "$not": function (operand) {
    var matcher = compileValueSelector(operand);
    return function (value) {
      return !matcher(value);
    };
  },

  "$near": function (operand) {
    // Always returns true. Must be handled in post-filter/sort/limit
    return function (value) {
      return true;
    }
  },

  "$geoIntersects": function (operand) {
    // Always returns true. Must be handled in post-filter/sort/limit
    return function (value) {
      return true;
    }
  }

};

// helpers used by compiled selector code
LocalCollection._f = {
  // XXX for _all and _in, consider building 'inquery' at compile time..

  _type: function (v) {
    if (typeof v === "number")
      return 1;
    if (typeof v === "string")
      return 2;
    if (typeof v === "boolean")
      return 8;
    if (isArray(v))
      return 4;
    if (v === null)
      return 10;
    if (v instanceof RegExp)
      return 11;
    if (typeof v === "function")
      // note that typeof(/x/) === "function"
      return 13;
    if (v instanceof Date)
      return 9;
    if (EJSON.isBinary(v))
      return 5;
    return 3; // object

    // XXX support some/all of these:
    // 14, symbol
    // 15, javascript code with scope
    // 16, 18: 32-bit/64-bit integer
    // 17, timestamp
    // 255, minkey
    // 127, maxkey
  },

  // deep equality test: use for literal document and array matches
  _equal: function (a, b) {
    return EJSON.equals(a, b, {keyOrderSensitive: true});
  },

  // maps a type code to a value that can be used to sort values of
  // different types
  _typeorder: function (t) {
    // http://www.mongodb.org/display/DOCS/What+is+the+Compare+Order+for+BSON+Types
    // XXX what is the correct sort position for Javascript code?
    // ('100' in the matrix below)
    // XXX minkey/maxkey
    return [-1,  // (not a type)
            1,   // number
            2,   // string
            3,   // object
            4,   // array
            5,   // binary
            -1,  // deprecated
            6,   // ObjectID
            7,   // bool
            8,   // Date
            0,   // null
            9,   // RegExp
            -1,  // deprecated
            100, // JS code
            2,   // deprecated (symbol)
            100, // JS code
            1,   // 32-bit int
            8,   // Mongo timestamp
            1    // 64-bit int
           ][t];
  },

  // compare two values of unknown type according to BSON ordering
  // semantics. (as an extension, consider 'undefined' to be less than
  // any other value.) return negative if a is less, positive if b is
  // less, or 0 if equal
  _cmp: function (a, b) {
    if(typeof a == 'object'){
      return -1
    }
    if (a === undefined)
      return b === undefined ? 0 : -1;
    if (b === undefined)
      return 1;
    var ta = LocalCollection._f._type(a);
    var tb = LocalCollection._f._type(b);
    var oa = LocalCollection._f._typeorder(ta);
    var ob = LocalCollection._f._typeorder(tb);
    if (oa !== ob)
      return oa < ob ? -1 : 1;
    if (ta !== tb)
      // XXX need to implement this if we implement Symbol or integers, or
      // Timestamp
      throw Error("Missing type coercion logic in _cmp");
    if (ta === 7) { // ObjectID
      // Convert to string.
      ta = tb = 2;
      a = a.toHexString();
      b = b.toHexString();
    }
    if (ta === 9) { // Date
      // Convert to millis.
      ta = tb = 1;
      a = a.getTime();
      b = b.getTime();
    }

    if (ta === 1) // double
      return a - b;
    if (tb === 2) // string
      return a < b ? -1 : (a === b ? 0 : 1);
    if (ta === 3) { // Object
      // this could be much more efficient in the expected case ...
      var to_array = function (obj) {
        var ret = [];
        for (var key in obj) {
          ret.push(key);
          ret.push(obj[key]);
        }
        return ret;
      };
      return LocalCollection._f._cmp(to_array(a), to_array(b));
    }
    if (ta === 4) { // Array
      for (var i = 0; ; i++) {
        if (i === a.length)
          return (i === b.length) ? 0 : -1;
        if (i === b.length)
          return 1;
        var s = LocalCollection._f._cmp(a[i], b[i]);
        if (s !== 0)
          return s;
      }
    }
    if (ta === 5) { // binary
      // Surprisingly, a small binary blob is always less than a large one in
      // Mongo.
      if (a.length !== b.length)
        return a.length - b.length;
      for (i = 0; i < a.length; i++) {
        if (a[i] < b[i])
          return -1;
        if (a[i] > b[i])
          return 1;
      }
      return 0;
    }
    if (ta === 8) { // boolean
      if (a) return b ? 0 : 1;
      return b ? -1 : 0;
    }
    if (ta === 10) // null
      return 0;
    if (ta === 11) // regexp
      throw Error("Sorting not supported on regular expression"); // XXX
    // 13: javascript code
    // 14: symbol
    // 15: javascript code with scope
    // 16: 32-bit integer
    // 17: timestamp
    // 18: 64-bit integer
    // 255: minkey
    // 127: maxkey
    if (ta === 13) // javascript code
      throw Error("Sorting not supported on Javascript code"); // XXX
    throw Error("Unknown type to sort");
  }
};

// For unit tests. True if the given document matches the given
// selector.
LocalCollection._matches = function (selector, doc) {
  return (LocalCollection._compileSelector(selector))(doc);
};

// _makeLookupFunction(key) returns a lookup function.
//
// A lookup function takes in a document and returns an array of matching
// values.  This array has more than one element if any segment of the key other
// than the last one is an array.  ie, any arrays found when doing non-final
// lookups result in this function "branching"; each element in the returned
// array represents the value found at this branch. If any branch doesn't have a
// final value for the full key, its element in the returned list will be
// undefined. It always returns a non-empty array.
//
// _makeLookupFunction('a.x')({a: {x: 1}}) returns [1]
// _makeLookupFunction('a.x')({a: {x: [1]}}) returns [[1]]
// _makeLookupFunction('a.x')({a: 5})  returns [undefined]
// _makeLookupFunction('a.x')({a: [{x: 1},
//                                 {x: [2]},
//                                 {y: 3}]})
//   returns [1, [2], undefined]
LocalCollection._makeLookupFunction = function (key) {
  var dotLocation = key.indexOf('.');
  var first, lookupRest, nextIsNumeric;
  if (dotLocation === -1) {
    first = key;
  } else {
    first = key.substr(0, dotLocation);
    var rest = key.substr(dotLocation + 1);
    lookupRest = LocalCollection._makeLookupFunction(rest);
    // Is the next (perhaps final) piece numeric (ie, an array lookup?)
    nextIsNumeric = /^\d+(\.|$)/.test(rest);
  }

  return function (doc) {
    if (doc == null)  // null or undefined
      return [undefined];
    var firstLevel = doc[first];

    // We don't "branch" at the final level.
    if (!lookupRest)
      return [firstLevel];

    // It's an empty array, and we're not done: we won't find anything.
    if (isArray(firstLevel) && firstLevel.length === 0)
      return [undefined];

    // For each result at this level, finish the lookup on the rest of the key,
    // and return everything we find. Also, if the next result is a number,
    // don't branch here.
    //
    // Technically, in MongoDB, we should be able to handle the case where
    // objects have numeric keys, but Mongo doesn't actually handle this
    // consistently yet itself, see eg
    // https://jira.mongodb.org/browse/SERVER-2898
    // https://github.com/mongodb/mongo/blob/master/jstests/array_match2.js
    if (!isArray(firstLevel) || nextIsNumeric)
      firstLevel = [firstLevel];
    return Array.prototype.concat.apply([], _.map(firstLevel, lookupRest));
  };
};

// The main compilation function for a given selector.
var compileDocumentSelector = function (docSelector) {
  var perKeySelectors = [];
  _.each(docSelector, function (subSelector, key) {
    if (key.substr(0, 1) === '$') {
      // Outer operators are either logical operators (they recurse back into
      // this function), or $where.
      if (_.has(VALUE_OPERATORS, key))
        perKeySelectors.push(VALUE_OPERATORS[key](subSelector));
      else if (_.has(LOGICAL_OPERATORS, key))
        perKeySelectors.push(LOGICAL_OPERATORS[key](subSelector));
      else
        throw new Error("Unrecognized logical operator: " + key);
    } else {
      var lookUpByIndex = LocalCollection._makeLookupFunction(key);
      var valueSelectorFunc = compileValueSelector(subSelector);
      perKeySelectors.push(function (doc) {
        var branchValues = lookUpByIndex(doc);
        // We apply the selector to each "branched" value and return true if any
        // match. This isn't 100% consistent with MongoDB; eg, see:
        // https://jira.mongodb.org/browse/SERVER-8585
        return _.any(branchValues, valueSelectorFunc);
      });
    }
  });


  return function (doc) {
    return _.all(perKeySelectors, function (f) {
      return f(doc);
    });
  };
};

// Given a selector, return a function that takes one argument, a
// document, and returns true if the document matches the selector,
// else false.
LocalCollection._compileSelector = function (selector) {
  // you can pass a literal function instead of a selector
  if (selector instanceof Function)
    return function (doc) {return selector.call(doc);};

  // shorthand -- scalars match _id
  if (LocalCollection._selectorIsId(selector)) {
    return function (doc) {
      return EJSON.equals(doc._id, selector);
    };
  }

  // protect against dangerous selectors.  falsey and {_id: falsey} are both
  // likely programmer error, and not what you want, particularly for
  // destructive operations.
  if (!selector || (('_id' in selector) && !selector._id))
    return function (doc) {return false;};

  // Top level can't be an array or true or binary.
  if (typeof(selector) === 'boolean' || isArray(selector) ||
      EJSON.isBinary(selector))
    throw new Error("Invalid selector: " + selector);

  return compileDocumentSelector(selector);
};

// Give a sort spec, which can be in any of these forms:
//   {"key1": 1, "key2": -1}
//   [["key1", "asc"], ["key2", "desc"]]
//   ["key1", ["key2", "desc"]]
//
// (.. with the first form being dependent on the key enumeration
// behavior of your javascript VM, which usually does what you mean in
// this case if the key names don't look like integers ..)
//
// return a function that takes two objects, and returns -1 if the
// first object comes first in order, 1 if the second object comes
// first, or 0 if neither object comes before the other.

LocalCollection._compileSort = function (spec) {
  var sortSpecParts = [];

  if (spec instanceof Array) {
    for (var i = 0; i < spec.length; i++) {
      if (typeof spec[i] === "string") {
        sortSpecParts.push({
          lookup: LocalCollection._makeLookupFunction(spec[i]),
          ascending: true
        });
      } else {
        sortSpecParts.push({
          lookup: LocalCollection._makeLookupFunction(spec[i][0]),
          ascending: spec[i][1] !== "desc"
        });
      }
    }
  } else if (typeof spec === "object") {
    for (var key in spec) {
      sortSpecParts.push({
        lookup: LocalCollection._makeLookupFunction(key),
        ascending: spec[key] >= 0
      });
    }
  } else {
    throw Error("Bad sort specification: ", JSON.stringify(spec));
  }

  if (sortSpecParts.length === 0)
    return function () {return 0;};

  // reduceValue takes in all the possible values for the sort key along various
  // branches, and returns the min or max value (according to the bool
  // findMin). Each value can itself be an array, and we look at its values
  // too. (ie, we do a single level of flattening on branchValues, then find the
  // min/max.)
  var reduceValue = function (branchValues, findMin) {
    var reduced;
    var first = true;
    // Iterate over all the values found in all the branches, and if a value is
    // an array itself, iterate over the values in the array separately.
    _.each(branchValues, function (branchValue) {
      // Value not an array? Pretend it is.
      if (!isArray(branchValue))
        branchValue = [branchValue];
      // Value is an empty array? Pretend it was missing, since that's where it
      // should be sorted.
      if (isArray(branchValue) && branchValue.length === 0)
        branchValue = [undefined];
      _.each(branchValue, function (value) {
        // We should get here at least once: lookup functions return non-empty
        // arrays, so the outer loop runs at least once, and we prevented
        // branchValue from being an empty array.
        if (first) {
          reduced = value;
          first = false;
        } else {
          // Compare the value we found to the value we found so far, saving it
          // if it's less (for an ascending sort) or more (for a descending
          // sort).
          var cmp = LocalCollection._f._cmp(reduced, value);
          if ((findMin && cmp > 0) || (!findMin && cmp < 0))
            reduced = value;
        }
      });
    });
    return reduced;
  };

  return function (a, b) {
    for (var i = 0; i < sortSpecParts.length; ++i) {
      var specPart = sortSpecParts[i];
      var aValue = reduceValue(specPart.lookup(a), specPart.ascending);
      var bValue = reduceValue(specPart.lookup(b), specPart.ascending);
      var compare = LocalCollection._f._cmp(aValue, bValue);
      if (compare !== 0)
        return specPart.ascending ? compare : -compare;
    };
    return 0;
  };
};

exports.compileDocumentSelector = compileDocumentSelector;
exports.compileSort = LocalCollection._compileSort;

},{"./EJSON":5,"lodash":"nJZoxB"}],8:[function(require,module,exports){
var SortedObjectArray, addMethods, async, bowser, compileDocumentSelector, compileSort, deg2rad, getDistanceFromLatLngInM, isLocalStorageSupported, pointInPolygon, processGeoIntersectsOperator, processNearOperator, _;

_ = require('lodash');

async = require('async');

bowser = require('bowser');

SortedObjectArray = require('sorted-object-array');

compileDocumentSelector = require('./selector').compileDocumentSelector;

compileSort = require('./selector').compileSort;

isLocalStorageSupported = function() {
  var e;
  if (!window.localStorage) {
    return false;
  }
  try {
    window.localStorage.setItem("test", "test");
    window.localStorage.removeItem("test");
    return true;
  } catch (_error) {
    e = _error;
    return false;
  }
};

exports.compileDocumentSelector = compileDocumentSelector;

exports.autoselectLocalDb = function(options, success, error) {
  var MemoryDb, browser;
  MemoryDb = require('./MemoryDb');
  browser = bowser.browser;
  return new MemoryDb(options, success);
};

exports.migrateLocalDb = function(fromDb, toDb, success, error) {};

exports.processUpdate = function(theItems, selector, docs, bases, database) {
  var docUpdate, id, item, k, v, _i, _len, _ref, _ref1;
  if (bases && bases['upsert'] && theItems.length < 1) {
    if (_.include(Object.keys(docs), '$set')) {
      database.insert(_.merge(selector, docs['$set']));
    } else if (Object.keys(docs)[0].indexOf('$') >= 0) {
      database.insert(_.merge(selector, docs[Object.keys(docs)[0]]));
    } else {
      database.insert(_.merge(selector, docs));
    }
    database.upserts[docs._id] = docs;
  }
  if ((!!bases && !!bases.multi) || theItems.length < 1) {
    theItems;
  } else {
    theItems = [_.first(theItems)];
  }
  for (_i = 0, _len = theItems.length; _i < _len; _i++) {
    item = theItems[_i];
    if (item.docs === void 0) {
      item.doc = docs;
    }
    if (item.base === void 0) {
      item.base = database.items[item.doc._id] || null;
    }
    item = _.cloneDeep(item);
    docUpdate = true;
    if (_.include(Object.keys(docs), "$inc")) {
      docUpdate = false;
      database.updates[item._id] = docs;
      _ref = docs['$inc'];
      for (k in _ref) {
        v = _ref[k];
        database.items[item._id][k] = database.items[item._id][k] + v;
      }
    }
    if (_.include(Object.keys(docs), "$set")) {
      docUpdate = exports.update$set(selector, database, docs, item);
    }
    if (_.include(Object.keys(docs), "$unset")) {
      docUpdate = exports.update$unset(database, docs, item);
    }
    if (_.include(Object.keys(docs), "$rename")) {
      database.updates[item._id] = docs;
      docUpdate = false;
      _ref1 = docs['$rename'];
      for (k in _ref1) {
        v = _ref1[k];
        database.items[item._id][v] = database.items[item._id][k];
        database.items[item._id] = _.omit(database.items[item._id], k);
      }
    }
    if (_.include(Object.keys(docs), "$max")) {
      docUpdate = exports.update$max(database, docs, item);
    }
    if (_.include(Object.keys(docs), "$min")) {
      docUpdate = exports.update$min(database, docs, item);
    }
    if (_.include(Object.keys(docs), "$mul")) {
      docUpdate = exports.update$mul(database, docs, item);
    }
    if (_.include(Object.keys(docs), "$addToSet")) {
      docUpdate = exports.update$addToSet(database, docs, item);
    }
    if (_.include(Object.keys(docs), "$push")) {
      docUpdate = exports.update$push(database, docs, item);
    }
    if (docUpdate) {
      database.updates[item._id] = docs;
      for (k in docs) {
        v = docs[k];
        id = database.items[item._id]._id;
        database.items[item._id] = docs;
      }
      database.items[item._id]._id = id;
    }
  }
  return '';
};

exports.update$push = function(database, docs, item) {
  var docUpdate, hit, k, v, _ref;
  docUpdate = false;
  hit = false;
  _ref = docs['$push'];
  for (k in _ref) {
    v = _ref[k];
    if (database.items[item._id][k]) {
      hit = true;
      database.items[item._id][k].push(v);
    }
  }
  if (hit) {
    database.updates[item._id] = docs;
  }
  return docUpdate;
};

exports.update$addToSet = function(database, docs, item) {
  var docUpdate, hit, k, v, _ref;
  docUpdate = false;
  hit = false;
  _ref = docs['$addToSet'];
  for (k in _ref) {
    v = _ref[k];
    if (database.items[item._id][k] && !_.include(database.items[item._id][k], v)) {
      hit = true;
      database.items[item._id][k].push(v);
    }
  }
  if (hit) {
    database.updates[item._id] = docs;
  }
  return docUpdate;
};

exports.update$unset = function(database, docs, item) {
  var docUpdate, hit, k, v, _ref;
  docUpdate = false;
  hit = false;
  _ref = docs['$unset'];
  for (k in _ref) {
    v = _ref[k];
    if (database.items[item._id][k]) {
      hit = true;
    }
    database.items[item._id] = _.omit(database.items[item._id], k);
  }
  if (hit) {
    database.updates[item._id] = docs;
  }
  return docUpdate;
};

exports.update$set = function(selector, database, docs, item) {
  var arr, docUpdate, index, k, placeholder, v, _ref;
  database.updates[item._id] = docs;
  docUpdate = false;
  _ref = docs['$set'];
  for (k in _ref) {
    v = _ref[k];
    placeholder = k.split('.');
    if (placeholder[placeholder.length - 1] === '$') {
      arr = database.items[item._id][placeholder[0]];
      index = arr.indexOf(_.values(selector)[0]);
      database.items[item._id][placeholder[0]][index] = _.values(docs['$set'])[0];
    } else if (!isNaN(placeholder[placeholder.length - 1])) {
      arr = database.items[item._id][placeholder[0]];
      index = parseInt(placeholder[placeholder.length - 1]);
      database.items[item._id][placeholder[0]][index] = v;
    } else {
      if (!database.items[item._id][k] && _.include(k, '$')) {
        throw {
          message: "The dollar ($) prefixed field " + k + " in " + k + " is not valid for storage."
        };
      }
      database.items[item._id][k] = v;
    }
  }
  return docUpdate;
};

exports.update$max = function(database, docs, item) {
  var data, docUpdate, hit, k, keys, v, _ref;
  docUpdate = false;
  hit = false;
  _ref = docs['$max'];
  for (k in _ref) {
    v = _ref[k];
    if (_.include(k, '.')) {
      keys = exports.prepareDot(k);
      data = exports.convertDot(item, keys[0])[keys[1]];
      if (data < v) {
        exports.convertDot(item, keys[0])[keys[1]] = v;
        database.items[item._id] = _.omit(item, 'doc', 'base');
        hit = true;
      }
    } else if (!database.items[item._id][k] && _.include(k, '$')) {
      throw {
        message: "The dollar ($) prefixed field " + k + " in " + k + " is not valid for storage."
      };
    } else if (database.items[item._id][k] < v) {
      database.items[item._id][k] = v;
      hit = true;
    }
  }
  if (hit) {
    database.updates[item._id] = docs;
  }
  return docUpdate;
};

exports.update$min = function(database, docs, item) {
  var data, docUpdate, hit, k, keys, v, _ref;
  database.updates[item._id] = docs;
  docUpdate = false;
  hit = false;
  _ref = docs['$min'];
  for (k in _ref) {
    v = _ref[k];
    if (_.include(k, '.')) {
      keys = exports.prepareDot(k);
      data = exports.convertDot(item, keys[0])[keys[1]];
      if (data > v) {
        exports.convertDot(item, keys[0])[keys[1]] = v;
        database.items[item._id] = _.omit(item, 'doc', 'base');
        hit = true;
      }
    } else if (!database.items[item._id][k] && _.include(k, '$')) {
      throw {
        message: "The dollar ($) prefixed field " + k + " in " + k + " is not valid for storage."
      };
    } else if (database.items[item._id][k] > v) {
      database.items[item._id][k] = v;
      hit = true;
    }
  }
  if (hit) {
    database.updates[item._id] = docs;
  }
  return docUpdate;
};

exports.update$mul = function(database, docs, item) {
  var docUpdate, k, keys, v, _ref;
  database.updates[item._id] = docs;
  docUpdate = false;
  _ref = docs['$mul'];
  for (k in _ref) {
    v = _ref[k];
    if (_.include(k, '.')) {
      keys = exports.prepareDot(k);
      exports.convertDot(item, keys[0])[keys[1]] = exports.convertDot(item, keys[0])[keys[1]] * v;
      database.items[item._id] = _.omit(item, 'doc', 'base');
    } else if (!database.items[item._id][k] && _.include(k, '$')) {
      throw {
        message: "The dollar ($) prefixed field " + k + " in " + k + " is not valid for storage."
      };
    } else {
      database.items[item._id][k] = database.items[item._id][k] * v;
    }
  }
  return docUpdate;
};

exports.prepareDot = function(k) {
  var arr, final_key, keys;
  arr = k.split('.');
  final_key = arr.pop();
  keys = arr.join('.');
  return [keys, final_key];
};

exports.convertDot = function(obj, _is, value) {
  if (typeof _is === 'string') {
    return exports.convertDot(obj, _is.split('.'), value);
  } else if (_is.length === 1 && value !== void 0) {
    return obj[_is[0]] = value;
  } else if (_is.length === 0) {
    return obj;
  } else {
    return exports.convertDot(obj[_is[0]], _is.slice(1), value);
  }
};

exports.processFind = function(items, selector, options) {
  var filtered, me;
  if (_.isArray(selector)) {
    return [];
  }
  filtered = _.filter(_.values(items), compileDocumentSelector(selector));
  filtered = processNearOperator(selector, filtered);
  filtered = processGeoIntersectsOperator(selector, filtered);
  if (options) {
    filtered = exports.filterFields(filtered, options);
  } else {
    filtered = _.map(filtered, function(doc) {
      return _.cloneDeep(doc);
    });
  }
  me = filtered;
  filtered['skip'] = function(amount) {
    var data;
    if (me.preLimit) {
      me.preLimit.splice(0, amount);
      data = me.preLimit;
    } else {
      me.splice(0, amount);
      data = me;
    }
    return addMethods(data);
  };
  filtered['limit'] = function(max) {
    var obj;
    obj = addMethods(me.slice(0, max));
    obj.preLimit = filtered;
    return obj;
  };
  filtered['sort'] = function(options) {
    var direction, sorted;
    direction = options[Object.keys(options)[0]];
    sorted = addMethods(_.sortBy(me, Object.keys(options)[0]));
    if (direction === -1) {
      sorted = sorted.reverse();
    } else if (direction === 1 || _.isEmpty(Object.keys(options))) {
      sorted = sorted;
    } else {
      throw {
        message: 'BadValue bad sort specification'
      };
    }
    return sorted;
  };
  filtered['count'] = function() {
    return _.size(me);
  };
  return filtered;
};

addMethods = function(filtered) {
  var me;
  me = filtered;
  me['skip'] = function(amount) {
    var data;
    if (me.preLimit) {
      me.preLimit.splice(0, amount);
      data = me.preLimit;
    } else {
      me.splice(0, amount);
      data = me;
    }
    return addMethods(data);
  };
  me['limit'] = function(max) {
    var obj;
    obj = addMethods(me.slice(0, max));
    obj.preLimit = filtered;
    return obj;
  };
  me['sort'] = function(options) {
    var direction, sorted;
    direction = options[Object.keys(options)[0]];
    sorted = addMethods(_.sortBy(me, Object.keys(options)[0]));
    if (direction < 0) {
      sorted = sorted.reverse();
    }
    return sorted;
  };
  filtered['count'] = function() {
    return _.size(me);
  };
  return me;
};

exports.convertToObject = function(selectors) {
  var key, obj, select, _i, _len;
  if (selectors.constructor !== Array) {
    return selectors;
  }
  obj = {};
  for (_i = 0, _len = selectors.length; _i < _len; _i++) {
    select = selectors[_i];
    key = Object.keys(select)[0];
    obj[key] = select[key];
  }
  return obj;
};

exports.aggregateGroup = function(filtered, items, selector, options) {
  var counter, h, i, item, keys, operation, taken, temp_filtered, values, _filter, _i, _id, _items, _j, _k, _len, _len1, _len2;
  temp_filtered = [];
  _items = filtered;
  keys = _.keys(selector['$group']);
  values = _.values(selector['$group']);
  _id = null;
  if (!_.include(keys, '_id')) {
    throw {
      message: '_id field was not supplied'
    };
  }
  if (typeof selector['$group']['_id'] === 'string' && selector['$group']['_id'].trim() === "$") {
    throw {
      message: "'$' by itself is not a valid FieldPath"
    };
  }
  for (_i = 0, _len = _items.length; _i < _len; _i++) {
    item = _items[_i];
    h = {};
    counter = 0;
    for (_j = 0, _len1 = keys.length; _j < _len1; _j++) {
      i = keys[_j];
      if (typeof values[counter] === 'string') {
        if (i === '_id') {
          _id = values[counter].replace('$', '');
        }
        h[i] = exports.compileDollar(item, values[counter]);
        taken = false;
        for (_k = 0, _len2 = temp_filtered.length; _k < _len2; _k++) {
          _filter = temp_filtered[_k];
          if (_.include(_.values(_filter), h._id)) {
            taken = true;
          }
        }
        if (!taken) {
          temp_filtered.push(h);
        }
      } else {
        if (typeof values[counter] !== 'object') {
          return [
            {
              _id: values[counter]
            }
          ];
        }
        operation = Object.keys(values[counter])[0];
        if (operation === '$max') {
          exports.aggregateMax(values, temp_filtered, _items, counter, _id, i);
        }
        if (operation === '$min') {
          exports.aggregateMin(values, temp_filtered, _items, counter, _id, i);
        }
        if (operation === '$sum') {
          exports.aggregateAdd(values, temp_filtered, _items, counter, _id, i);
        } else if (operation === '$avg') {
          exports.aggregateAvg(values, temp_filtered, _items, counter, _id, i);
        }
      }
      counter += 1;
    }
  }
  return filtered = temp_filtered;
};

exports.compileDollar = function(item, value) {
  var k;
  if (_.include(value, '$')) {
    if (_.include(value, '.')) {
      k = value.replace('$', '').split('.');
      return item[k[0]][k[1]];
    } else {
      return item[value.replace('$', '')];
    }
  } else {
    return value;
  }
};

exports.processOperation = function(it, sum_key) {
  var k, parts, val;
  if (typeof sum_key === 'number') {
    return sum_key;
  } else if (_.include(sum_key, '.')) {
    parts = sum_key.replace('$', '').split('.');
    if (it[parts[0]]) {
      val = it[parts[0]][parts[1]];
    } else {
      return null;
    }
  } else if (!_.include(sum_key, '$')) {
    0;
  } else {
    val = it[sum_key.replace('$', '')] || it[sum_key];
  }
  if (typeof val === 'object') {
    k = sum_key.replace('$', '').split('.');
    return it[sum_key][k[1]];
  } else if (typeof val === 'string' || typeof val === 'number') {
    return val;
  }
};

exports.aggregateMax = function(values, temp_filtered, _items, counter, _id, i) {
  var filt, it, max, opt_keys, sum_key, _i, _j, _len, _len1, _results;
  opt_keys = Object.keys(values[counter]);
  sum_key = values[counter][opt_keys];
  _results = [];
  for (_i = 0, _len = temp_filtered.length; _i < _len; _i++) {
    filt = temp_filtered[_i];
    max = 0;
    for (_j = 0, _len1 = _items.length; _j < _len1; _j++) {
      it = _items[_j];
      if (exports.checkIfMatch(filt, it, _id, sum_key)) {
        if (max < exports.processOperation(it, sum_key)) {
          max = exports.processOperation(it, sum_key);
        }
      }
    }
    _results.push(filt[i] = max);
  }
  return _results;
};

exports.aggregateMin = function(values, temp_filtered, _items, counter, _id, i) {
  var filt, it, max, opt_keys, sum_key, _i, _j, _len, _len1, _results;
  opt_keys = Object.keys(values[counter]);
  sum_key = values[counter][opt_keys];
  _results = [];
  for (_i = 0, _len = temp_filtered.length; _i < _len; _i++) {
    filt = temp_filtered[_i];
    max = null;
    for (_j = 0, _len1 = _items.length; _j < _len1; _j++) {
      it = _items[_j];
      if (exports.checkIfMatch(filt, it, _id, sum_key)) {
        if (!max) {
          max = exports.processOperation(it, sum_key);
        }
        if (max > exports.processOperation(it, sum_key)) {
          max = exports.processOperation(it, sum_key);
        }
      }
    }
    _results.push(filt[i] = max);
  }
  return _results;
};

exports.aggregateAdd = function(values, temp_filtered, _items, counter, _id, i) {
  var filt, it, opt_keys, sum, sum_key, _i, _j, _len, _len1, _results;
  opt_keys = Object.keys(values[counter]);
  sum_key = values[counter][opt_keys];
  _results = [];
  for (_i = 0, _len = temp_filtered.length; _i < _len; _i++) {
    filt = temp_filtered[_i];
    sum = 0;
    for (_j = 0, _len1 = _items.length; _j < _len1; _j++) {
      it = _items[_j];
      if (exports.checkIfMatch(filt, it, _id, sum_key)) {
        sum += exports.processOperation(it, sum_key);
      }
    }
    _results.push(filt[i] = sum);
  }
  return _results;
};

exports.aggregateAvg = function(values, temp_filtered, _items, counter, _id, i) {
  var filt, it, length, opt_keys, sum, sum_key, _i, _j, _len, _len1;
  opt_keys = Object.keys(values[counter]);
  sum_key = values[counter][opt_keys];
  for (_i = 0, _len = temp_filtered.length; _i < _len; _i++) {
    filt = temp_filtered[_i];
    sum = 0;
    length = 0;
    for (_j = 0, _len1 = _items.length; _j < _len1; _j++) {
      it = _items[_j];
      if (exports.checkIfMatch(filt, it, _id, sum_key)) {
        sum += exports.processOperation(it, sum_key);
        length += 1;
      }
    }
    if (sum < 1) {
      length = 1;
    }
    filt[i] = sum / length;
  }
  return temp_filtered;
};

exports.aggregateSort = function(selector, filtered) {
  var compare, direction, key;
  direction = selector['$sort'][Object.keys(selector['$sort'])[0]];
  key = Object.keys(selector['$sort'])[0].replace('$', '');
  compare = function(a, b) {
    if (a[key] < b[key]) {
      return -1;
    }
    if (a[key] > b[key]) {
      return 1;
    }
    return 0;
  };
  filtered = filtered.sort(compare);
  if (direction < 0) {
    filtered = filtered.reverse();
  }
  return filtered;
};

exports.checkIfMatch = function(filt, it, _id, sum_key) {
  return filt['_id'] === exports.processOperation(it, _id) || _id === filt['_id'] || (it[_id] === filt['_id'] && (typeof sum_key === 'number' || _.include(sum_key, '$')));
};

exports.aggregateProject = function(selector, filtered) {
  var filt, k, keep, key, keys, strippedKey, temp, _i, _j, _k, _len, _len1, _len2, _temp_filtered;
  keys = Object.keys(selector['$project']);
  keep = [];
  for (_i = 0, _len = keys.length; _i < _len; _i++) {
    k = keys[_i];
    if (selector['$project'][k]) {
      keep.push(k);
    }
  }
  if (selector['$project']['_id']) {
    keep.push('_id');
  }
  _temp_filtered = [];
  for (_j = 0, _len1 = filtered.length; _j < _len1; _j++) {
    filt = filtered[_j];
    temp = {};
    for (_k = 0, _len2 = keep.length; _k < _len2; _k++) {
      key = keep[_k];
      strippedKey = key.replace('$', '');
      if (!filt[key] && !filt[strippedKey]) {
        k = key.split('.');
        temp[k[0]] = filt[k[0]];
      } else if (filt[strippedKey]) {
        temp[strippedKey] = filt[strippedKey];
      }
    }
    _temp_filtered.push(temp);
  }
  return filtered = _temp_filtered;
};

exports.throwPipelineOperators = function(key) {
  if (_.intersection(['$match', '$project', '$group', '$limit', '$sort', '$skip'], Object.keys(key)).length < 1) {
    if (_.intersection(['match', 'project', 'group', 'limit', 'sort', 'skip'], Object.keys(key)).length > 0) {
      throw {
        message: "The pipeline operator " + (Object.keys(key)[0]) + " requires a $ in front."
      };
    } else {
      throw {
        message: 'This pipeline operator is not supported with this browser version of MongoDB'
      };
    }
  }
};

exports.processAggregate = function(items, selector, options) {
  var filtered, key, _i, _len;
  if (!_.isArray(selector)) {
    selector = [selector];
  }
  filtered = _.values(items);
  for (_i = 0, _len = selector.length; _i < _len; _i++) {
    key = selector[_i];
    exports.throwPipelineOperators(key);
    if (key['$match']) {
      filtered = _.filter(filtered, compileDocumentSelector(key['$match']));
    }
    if (key['$project']) {
      filtered = exports.aggregateProject(key, filtered);
    }
    if (key['$group']) {
      filtered = exports.aggregateGroup(filtered, items, key, options);
    }
    if (key['$limit']) {
      filtered = filtered.slice(0, key['$limit']);
    }
    if (key['$sort']) {
      filtered = exports.aggregateSort(key, filtered);
    }
    if (key['$skip']) {
      filtered.splice(0, key['$skip']);
    }
  }
  return filtered;
};

exports.filterFields = function(items, fields) {
  if (fields == null) {
    fields = {};
  }
  if (_.keys(fields).length === 0) {
    return items;
  }
  return _.map(items, function(item) {
    var availKeys, field, from, newItem, obj, path, pathElem, to, _i, _j, _k, _l, _len, _len1, _len2, _len3, _len4, _m, _ref, _ref1, _ref2;
    item = _.cloneDeep(item);
    newItem = {};
    if (_.include(_.values(fields), 1) || _.include(_.values(fields), true)) {
      if (_.include(_.keys(fields), '_id') && !fields['_id']) {
        availKeys = _.keys(fields);
        fields['_id'] = false;
      } else {
        availKeys = _.keys(fields).concat(["_id"]);
        fields['_id'] = true;
      }
      for (_i = 0, _len = availKeys.length; _i < _len; _i++) {
        field = availKeys[_i];
        path = field.split(".");
        obj = item;
        for (_j = 0, _len1 = path.length; _j < _len1; _j++) {
          pathElem = path[_j];
          if (obj) {
            obj = obj[pathElem];
          }
        }
        if (obj == null) {
          continue;
        }
        from = item;
        to = newItem;
        _ref = _.initial(path);
        for (_k = 0, _len2 = _ref.length; _k < _len2; _k++) {
          pathElem = _ref[_k];
          to[pathElem] = to[pathElem] || {};
          to = to[pathElem];
          from = from[pathElem];
        }
        if (fields[field]) {
          to[_.last(path)] = from[_.last(path)];
        }
      }
      return newItem;
    } else {
      _ref1 = _.keys(fields);
      for (_l = 0, _len3 = _ref1.length; _l < _len3; _l++) {
        field = _ref1[_l];
        path = field.split(".");
        obj = item;
        _ref2 = _.initial(path);
        for (_m = 0, _len4 = _ref2.length; _m < _len4; _m++) {
          pathElem = _ref2[_m];
          if (obj) {
            obj = obj[pathElem];
          }
        }
        if (obj == null) {
          continue;
        }
        delete obj[_.last(path)];
      }
      return item;
    }
  });
};

exports.createUid = function() {
  return 'xxxxxxxxxxxx4xxxyxxxxxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
    var r, v;
    r = Math.random() * 16 | 0;
    v = c === 'x' ? r : r & 0x3 | 0x8;
    return v.toString(16);
  });
};

processNearOperator = function(selector, list) {
  var distances, geo, key, value;
  for (key in selector) {
    value = selector[key];
    if ((value != null) && value['$near']) {
      geo = value['$near']['$geometry'];
      if (geo.type !== 'Point') {
        break;
      }
      list = _.filter(list, function(doc) {
        return doc[key] && doc[key].type === 'Point';
      });
      distances = _.map(list, function(doc) {
        return {
          doc: doc,
          distance: getDistanceFromLatLngInM(geo.coordinates[1], geo.coordinates[0], doc[key].coordinates[1], doc[key].coordinates[0])
        };
      });
      distances = _.filter(distances, function(item) {
        return item.distance >= 0;
      });
      distances = _.sortBy(distances, 'distance');
      if (value['$near']['$maxDistance']) {
        distances = _.filter(distances, function(item) {
          return item.distance <= value['$near']['$maxDistance'];
        });
      }
      list = _.pluck(distances, 'doc');
    }
  }
  return list;
};

pointInPolygon = function(point, polygon) {
  if (!_.isEqual(_.first(polygon.coordinates[0]), _.last(polygon.coordinates[0]))) {
    throw new Error("First must equal last");
  }
  if (point.coordinates[0] < Math.min.apply(this, _.map(polygon.coordinates[0], function(coord) {
    return coord[0];
  }))) {
    return false;
  }
  if (point.coordinates[1] < Math.min.apply(this, _.map(polygon.coordinates[0], function(coord) {
    return coord[1];
  }))) {
    return false;
  }
  if (point.coordinates[0] > Math.max.apply(this, _.map(polygon.coordinates[0], function(coord) {
    return coord[0];
  }))) {
    return false;
  }
  if (point.coordinates[1] > Math.max.apply(this, _.map(polygon.coordinates[0], function(coord) {
    return coord[1];
  }))) {
    return false;
  }
  return true;
};

getDistanceFromLatLngInM = function(lat1, lng1, lat2, lng2) {
  var R, a, c, d, dLat, dLng;
  R = 6370986;
  dLat = deg2rad(lat2 - lat1);
  dLng = deg2rad(lng2 - lng1);
  a = Math.sin(dLat / 2) * Math.sin(dLat / 2) + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.sin(dLng / 2) * Math.sin(dLng / 2);
  c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  d = R * c;
  return d;
};

deg2rad = function(deg) {
  return deg * (Math.PI / 180);
};

processGeoIntersectsOperator = function(selector, list) {
  var geo, key, value;
  for (key in selector) {
    value = selector[key];
    if ((value != null) && value['$geoIntersects']) {
      geo = value['$geoIntersects']['$geometry'];
      if (geo.type !== 'Polygon') {
        break;
      }
      list = _.filter(list, function(doc) {
        if (!doc[key] || doc[key].type !== 'Point') {
          return false;
        }
        return pointInPolygon(doc[key], geo);
      });
    }
  }
  return list;
};

exports.regularizeUpsert = function(docs, bases, success, error) {
  var item, items, _i, _len, _ref;
  if (_.isFunction(bases)) {
    _ref = [void 0, bases, success], bases = _ref[0], success = _ref[1], error = _ref[2];
  }
  if (!_.isArray(docs)) {
    docs = [docs];
    bases = [bases];
  } else {
    bases = bases || [];
  }
  items = _.map(docs, function(doc, i) {
    return {
      doc: doc,
      base: i < bases.length ? bases[i] : void 0
    };
  });
  for (_i = 0, _len = items.length; _i < _len; _i++) {
    item = items[_i];
    if (!item.doc._id) {
      item.doc._id = exports.createUid();
    }
    if (item.base && !item.base._id) {
      throw new Error("Base needs _id");
    }
    if (item.base && item.base._id !== item.doc._id) {
      throw new Error("Base needs same _id");
    }
  }
  return [items, success, error];
};

},{"./MemoryDb":6,"./selector":7,"async":11,"bowser":12,"lodash":"nJZoxB","sorted-object-array":14}],"lodash":[function(require,module,exports){
module.exports=require('nJZoxB');
},{}],"nJZoxB":[function(require,module,exports){
module.exports = window._;

},{}],11:[function(require,module,exports){
(function (process){
/*!
 * async
 * https://github.com/caolan/async
 *
 * Copyright 2010-2014 Caolan McMahon
 * Released under the MIT license
 */
/*jshint onevar: false, indent:4 */
/*global setImmediate: false, setTimeout: false, console: false */
(function () {

    var async = {};

    // global on the server, window in the browser
    var root, previous_async;

    root = this;
    if (root != null) {
      previous_async = root.async;
    }

    async.noConflict = function () {
        root.async = previous_async;
        return async;
    };

    function only_once(fn) {
        var called = false;
        return function() {
            if (called) throw new Error("Callback was already called.");
            called = true;
            fn.apply(root, arguments);
        }
    }

    //// cross-browser compatiblity functions ////

    var _toString = Object.prototype.toString;

    var _isArray = Array.isArray || function (obj) {
        return _toString.call(obj) === '[object Array]';
    };

    var _each = function (arr, iterator) {
        if (arr.forEach) {
            return arr.forEach(iterator);
        }
        for (var i = 0; i < arr.length; i += 1) {
            iterator(arr[i], i, arr);
        }
    };

    var _map = function (arr, iterator) {
        if (arr.map) {
            return arr.map(iterator);
        }
        var results = [];
        _each(arr, function (x, i, a) {
            results.push(iterator(x, i, a));
        });
        return results;
    };

    var _reduce = function (arr, iterator, memo) {
        if (arr.reduce) {
            return arr.reduce(iterator, memo);
        }
        _each(arr, function (x, i, a) {
            memo = iterator(memo, x, i, a);
        });
        return memo;
    };

    var _keys = function (obj) {
        if (Object.keys) {
            return Object.keys(obj);
        }
        var keys = [];
        for (var k in obj) {
            if (obj.hasOwnProperty(k)) {
                keys.push(k);
            }
        }
        return keys;
    };

    //// exported async module functions ////

    //// nextTick implementation with browser-compatible fallback ////
    if (typeof process === 'undefined' || !(process.nextTick)) {
        if (typeof setImmediate === 'function') {
            async.nextTick = function (fn) {
                // not a direct alias for IE10 compatibility
                setImmediate(fn);
            };
            async.setImmediate = async.nextTick;
        }
        else {
            async.nextTick = function (fn) {
                setTimeout(fn, 0);
            };
            async.setImmediate = async.nextTick;
        }
    }
    else {
        async.nextTick = process.nextTick;
        if (typeof setImmediate !== 'undefined') {
            async.setImmediate = function (fn) {
              // not a direct alias for IE10 compatibility
              setImmediate(fn);
            };
        }
        else {
            async.setImmediate = async.nextTick;
        }
    }

    async.each = function (arr, iterator, callback) {
        callback = callback || function () {};
        if (!arr.length) {
            return callback();
        }
        var completed = 0;
        _each(arr, function (x) {
            iterator(x, only_once(done) );
        });
        function done(err) {
          if (err) {
              callback(err);
              callback = function () {};
          }
          else {
              completed += 1;
              if (completed >= arr.length) {
                  callback();
              }
          }
        }
    };
    async.forEach = async.each;

    async.eachSeries = function (arr, iterator, callback) {
        callback = callback || function () {};
        if (!arr.length) {
            return callback();
        }
        var completed = 0;
        var iterate = function () {
            iterator(arr[completed], function (err) {
                if (err) {
                    callback(err);
                    callback = function () {};
                }
                else {
                    completed += 1;
                    if (completed >= arr.length) {
                        callback();
                    }
                    else {
                        iterate();
                    }
                }
            });
        };
        iterate();
    };
    async.forEachSeries = async.eachSeries;

    async.eachLimit = function (arr, limit, iterator, callback) {
        var fn = _eachLimit(limit);
        fn.apply(null, [arr, iterator, callback]);
    };
    async.forEachLimit = async.eachLimit;

    var _eachLimit = function (limit) {

        return function (arr, iterator, callback) {
            callback = callback || function () {};
            if (!arr.length || limit <= 0) {
                return callback();
            }
            var completed = 0;
            var started = 0;
            var running = 0;

            (function replenish () {
                if (completed >= arr.length) {
                    return callback();
                }

                while (running < limit && started < arr.length) {
                    started += 1;
                    running += 1;
                    iterator(arr[started - 1], function (err) {
                        if (err) {
                            callback(err);
                            callback = function () {};
                        }
                        else {
                            completed += 1;
                            running -= 1;
                            if (completed >= arr.length) {
                                callback();
                            }
                            else {
                                replenish();
                            }
                        }
                    });
                }
            })();
        };
    };


    var doParallel = function (fn) {
        return function () {
            var args = Array.prototype.slice.call(arguments);
            return fn.apply(null, [async.each].concat(args));
        };
    };
    var doParallelLimit = function(limit, fn) {
        return function () {
            var args = Array.prototype.slice.call(arguments);
            return fn.apply(null, [_eachLimit(limit)].concat(args));
        };
    };
    var doSeries = function (fn) {
        return function () {
            var args = Array.prototype.slice.call(arguments);
            return fn.apply(null, [async.eachSeries].concat(args));
        };
    };


    var _asyncMap = function (eachfn, arr, iterator, callback) {
        arr = _map(arr, function (x, i) {
            return {index: i, value: x};
        });
        if (!callback) {
            eachfn(arr, function (x, callback) {
                iterator(x.value, function (err) {
                    callback(err);
                });
            });
        } else {
            var results = [];
            eachfn(arr, function (x, callback) {
                iterator(x.value, function (err, v) {
                    results[x.index] = v;
                    callback(err);
                });
            }, function (err) {
                callback(err, results);
            });
        }
    };
    async.map = doParallel(_asyncMap);
    async.mapSeries = doSeries(_asyncMap);
    async.mapLimit = function (arr, limit, iterator, callback) {
        return _mapLimit(limit)(arr, iterator, callback);
    };

    var _mapLimit = function(limit) {
        return doParallelLimit(limit, _asyncMap);
    };

    // reduce only has a series version, as doing reduce in parallel won't
    // work in many situations.
    async.reduce = function (arr, memo, iterator, callback) {
        async.eachSeries(arr, function (x, callback) {
            iterator(memo, x, function (err, v) {
                memo = v;
                callback(err);
            });
        }, function (err) {
            callback(err, memo);
        });
    };
    // inject alias
    async.inject = async.reduce;
    // foldl alias
    async.foldl = async.reduce;

    async.reduceRight = function (arr, memo, iterator, callback) {
        var reversed = _map(arr, function (x) {
            return x;
        }).reverse();
        async.reduce(reversed, memo, iterator, callback);
    };
    // foldr alias
    async.foldr = async.reduceRight;

    var _filter = function (eachfn, arr, iterator, callback) {
        var results = [];
        arr = _map(arr, function (x, i) {
            return {index: i, value: x};
        });
        eachfn(arr, function (x, callback) {
            iterator(x.value, function (v) {
                if (v) {
                    results.push(x);
                }
                callback();
            });
        }, function (err) {
            callback(_map(results.sort(function (a, b) {
                return a.index - b.index;
            }), function (x) {
                return x.value;
            }));
        });
    };
    async.filter = doParallel(_filter);
    async.filterSeries = doSeries(_filter);
    // select alias
    async.select = async.filter;
    async.selectSeries = async.filterSeries;

    var _reject = function (eachfn, arr, iterator, callback) {
        var results = [];
        arr = _map(arr, function (x, i) {
            return {index: i, value: x};
        });
        eachfn(arr, function (x, callback) {
            iterator(x.value, function (v) {
                if (!v) {
                    results.push(x);
                }
                callback();
            });
        }, function (err) {
            callback(_map(results.sort(function (a, b) {
                return a.index - b.index;
            }), function (x) {
                return x.value;
            }));
        });
    };
    async.reject = doParallel(_reject);
    async.rejectSeries = doSeries(_reject);

    var _detect = function (eachfn, arr, iterator, main_callback) {
        eachfn(arr, function (x, callback) {
            iterator(x, function (result) {
                if (result) {
                    main_callback(x);
                    main_callback = function () {};
                }
                else {
                    callback();
                }
            });
        }, function (err) {
            main_callback();
        });
    };
    async.detect = doParallel(_detect);
    async.detectSeries = doSeries(_detect);

    async.some = function (arr, iterator, main_callback) {
        async.each(arr, function (x, callback) {
            iterator(x, function (v) {
                if (v) {
                    main_callback(true);
                    main_callback = function () {};
                }
                callback();
            });
        }, function (err) {
            main_callback(false);
        });
    };
    // any alias
    async.any = async.some;

    async.every = function (arr, iterator, main_callback) {
        async.each(arr, function (x, callback) {
            iterator(x, function (v) {
                if (!v) {
                    main_callback(false);
                    main_callback = function () {};
                }
                callback();
            });
        }, function (err) {
            main_callback(true);
        });
    };
    // all alias
    async.all = async.every;

    async.sortBy = function (arr, iterator, callback) {
        async.map(arr, function (x, callback) {
            iterator(x, function (err, criteria) {
                if (err) {
                    callback(err);
                }
                else {
                    callback(null, {value: x, criteria: criteria});
                }
            });
        }, function (err, results) {
            if (err) {
                return callback(err);
            }
            else {
                var fn = function (left, right) {
                    var a = left.criteria, b = right.criteria;
                    return a < b ? -1 : a > b ? 1 : 0;
                };
                callback(null, _map(results.sort(fn), function (x) {
                    return x.value;
                }));
            }
        });
    };

    async.auto = function (tasks, callback) {
        callback = callback || function () {};
        var keys = _keys(tasks);
        var remainingTasks = keys.length
        if (!remainingTasks) {
            return callback();
        }

        var results = {};

        var listeners = [];
        var addListener = function (fn) {
            listeners.unshift(fn);
        };
        var removeListener = function (fn) {
            for (var i = 0; i < listeners.length; i += 1) {
                if (listeners[i] === fn) {
                    listeners.splice(i, 1);
                    return;
                }
            }
        };
        var taskComplete = function () {
            remainingTasks--
            _each(listeners.slice(0), function (fn) {
                fn();
            });
        };

        addListener(function () {
            if (!remainingTasks) {
                var theCallback = callback;
                // prevent final callback from calling itself if it errors
                callback = function () {};

                theCallback(null, results);
            }
        });

        _each(keys, function (k) {
            var task = _isArray(tasks[k]) ? tasks[k]: [tasks[k]];
            var taskCallback = function (err) {
                var args = Array.prototype.slice.call(arguments, 1);
                if (args.length <= 1) {
                    args = args[0];
                }
                if (err) {
                    var safeResults = {};
                    _each(_keys(results), function(rkey) {
                        safeResults[rkey] = results[rkey];
                    });
                    safeResults[k] = args;
                    callback(err, safeResults);
                    // stop subsequent errors hitting callback multiple times
                    callback = function () {};
                }
                else {
                    results[k] = args;
                    async.setImmediate(taskComplete);
                }
            };
            var requires = task.slice(0, Math.abs(task.length - 1)) || [];
            var ready = function () {
                return _reduce(requires, function (a, x) {
                    return (a && results.hasOwnProperty(x));
                }, true) && !results.hasOwnProperty(k);
            };
            if (ready()) {
                task[task.length - 1](taskCallback, results);
            }
            else {
                var listener = function () {
                    if (ready()) {
                        removeListener(listener);
                        task[task.length - 1](taskCallback, results);
                    }
                };
                addListener(listener);
            }
        });
    };

    async.retry = function(times, task, callback) {
        var DEFAULT_TIMES = 5;
        var attempts = [];
        // Use defaults if times not passed
        if (typeof times === 'function') {
            callback = task;
            task = times;
            times = DEFAULT_TIMES;
        }
        // Make sure times is a number
        times = parseInt(times, 10) || DEFAULT_TIMES;
        var wrappedTask = function(wrappedCallback, wrappedResults) {
            var retryAttempt = function(task, finalAttempt) {
                return function(seriesCallback) {
                    task(function(err, result){
                        seriesCallback(!err || finalAttempt, {err: err, result: result});
                    }, wrappedResults);
                };
            };
            while (times) {
                attempts.push(retryAttempt(task, !(times-=1)));
            }
            async.series(attempts, function(done, data){
                data = data[data.length - 1];
                (wrappedCallback || callback)(data.err, data.result);
            });
        }
        // If a callback is passed, run this as a controll flow
        return callback ? wrappedTask() : wrappedTask
    };

    async.waterfall = function (tasks, callback) {
        callback = callback || function () {};
        if (!_isArray(tasks)) {
          var err = new Error('First argument to waterfall must be an array of functions');
          return callback(err);
        }
        if (!tasks.length) {
            return callback();
        }
        var wrapIterator = function (iterator) {
            return function (err) {
                if (err) {
                    callback.apply(null, arguments);
                    callback = function () {};
                }
                else {
                    var args = Array.prototype.slice.call(arguments, 1);
                    var next = iterator.next();
                    if (next) {
                        args.push(wrapIterator(next));
                    }
                    else {
                        args.push(callback);
                    }
                    async.setImmediate(function () {
                        iterator.apply(null, args);
                    });
                }
            };
        };
        wrapIterator(async.iterator(tasks))();
    };

    var _parallel = function(eachfn, tasks, callback) {
        callback = callback || function () {};
        if (_isArray(tasks)) {
            eachfn.map(tasks, function (fn, callback) {
                if (fn) {
                    fn(function (err) {
                        var args = Array.prototype.slice.call(arguments, 1);
                        if (args.length <= 1) {
                            args = args[0];
                        }
                        callback.call(null, err, args);
                    });
                }
            }, callback);
        }
        else {
            var results = {};
            eachfn.each(_keys(tasks), function (k, callback) {
                tasks[k](function (err) {
                    var args = Array.prototype.slice.call(arguments, 1);
                    if (args.length <= 1) {
                        args = args[0];
                    }
                    results[k] = args;
                    callback(err);
                });
            }, function (err) {
                callback(err, results);
            });
        }
    };

    async.parallel = function (tasks, callback) {
        _parallel({ map: async.map, each: async.each }, tasks, callback);
    };

    async.parallelLimit = function(tasks, limit, callback) {
        _parallel({ map: _mapLimit(limit), each: _eachLimit(limit) }, tasks, callback);
    };

    async.series = function (tasks, callback) {
        callback = callback || function () {};
        if (_isArray(tasks)) {
            async.mapSeries(tasks, function (fn, callback) {
                if (fn) {
                    fn(function (err) {
                        var args = Array.prototype.slice.call(arguments, 1);
                        if (args.length <= 1) {
                            args = args[0];
                        }
                        callback.call(null, err, args);
                    });
                }
            }, callback);
        }
        else {
            var results = {};
            async.eachSeries(_keys(tasks), function (k, callback) {
                tasks[k](function (err) {
                    var args = Array.prototype.slice.call(arguments, 1);
                    if (args.length <= 1) {
                        args = args[0];
                    }
                    results[k] = args;
                    callback(err);
                });
            }, function (err) {
                callback(err, results);
            });
        }
    };

    async.iterator = function (tasks) {
        var makeCallback = function (index) {
            var fn = function () {
                if (tasks.length) {
                    tasks[index].apply(null, arguments);
                }
                return fn.next();
            };
            fn.next = function () {
                return (index < tasks.length - 1) ? makeCallback(index + 1): null;
            };
            return fn;
        };
        return makeCallback(0);
    };

    async.apply = function (fn) {
        var args = Array.prototype.slice.call(arguments, 1);
        return function () {
            return fn.apply(
                null, args.concat(Array.prototype.slice.call(arguments))
            );
        };
    };

    var _concat = function (eachfn, arr, fn, callback) {
        var r = [];
        eachfn(arr, function (x, cb) {
            fn(x, function (err, y) {
                r = r.concat(y || []);
                cb(err);
            });
        }, function (err) {
            callback(err, r);
        });
    };
    async.concat = doParallel(_concat);
    async.concatSeries = doSeries(_concat);

    async.whilst = function (test, iterator, callback) {
        if (test()) {
            iterator(function (err) {
                if (err) {
                    return callback(err);
                }
                async.whilst(test, iterator, callback);
            });
        }
        else {
            callback();
        }
    };

    async.doWhilst = function (iterator, test, callback) {
        iterator(function (err) {
            if (err) {
                return callback(err);
            }
            var args = Array.prototype.slice.call(arguments, 1);
            if (test.apply(null, args)) {
                async.doWhilst(iterator, test, callback);
            }
            else {
                callback();
            }
        });
    };

    async.until = function (test, iterator, callback) {
        if (!test()) {
            iterator(function (err) {
                if (err) {
                    return callback(err);
                }
                async.until(test, iterator, callback);
            });
        }
        else {
            callback();
        }
    };

    async.doUntil = function (iterator, test, callback) {
        iterator(function (err) {
            if (err) {
                return callback(err);
            }
            var args = Array.prototype.slice.call(arguments, 1);
            if (!test.apply(null, args)) {
                async.doUntil(iterator, test, callback);
            }
            else {
                callback();
            }
        });
    };

    async.queue = function (worker, concurrency) {
        if (concurrency === undefined) {
            concurrency = 1;
        }
        function _insert(q, data, pos, callback) {
          if (!q.started){
            q.started = true;
          }
          if (!_isArray(data)) {
              data = [data];
          }
          if(data.length == 0) {
             // call drain immediately if there are no tasks
             return async.setImmediate(function() {
                 if (q.drain) {
                     q.drain();
                 }
             });
          }
          _each(data, function(task) {
              var item = {
                  data: task,
                  callback: typeof callback === 'function' ? callback : null
              };

              if (pos) {
                q.tasks.unshift(item);
              } else {
                q.tasks.push(item);
              }

              if (q.saturated && q.tasks.length === q.concurrency) {
                  q.saturated();
              }
              async.setImmediate(q.process);
          });
        }

        var workers = 0;
        var q = {
            tasks: [],
            concurrency: concurrency,
            saturated: null,
            empty: null,
            drain: null,
            started: false,
            paused: false,
            push: function (data, callback) {
              _insert(q, data, false, callback);
            },
            kill: function () {
              q.drain = null;
              q.tasks = [];
            },
            unshift: function (data, callback) {
              _insert(q, data, true, callback);
            },
            process: function () {
                if (!q.paused && workers < q.concurrency && q.tasks.length) {
                    var task = q.tasks.shift();
                    if (q.empty && q.tasks.length === 0) {
                        q.empty();
                    }
                    workers += 1;
                    var next = function () {
                        workers -= 1;
                        if (task.callback) {
                            task.callback.apply(task, arguments);
                        }
                        if (q.drain && q.tasks.length + workers === 0) {
                            q.drain();
                        }
                        q.process();
                    };
                    var cb = only_once(next);
                    worker(task.data, cb);
                }
            },
            length: function () {
                return q.tasks.length;
            },
            running: function () {
                return workers;
            },
            idle: function() {
                return q.tasks.length + workers === 0;
            },
            pause: function () {
                if (q.paused === true) { return; }
                q.paused = true;
                q.process();
            },
            resume: function () {
                if (q.paused === false) { return; }
                q.paused = false;
                q.process();
            }
        };
        return q;
    };

    async.cargo = function (worker, payload) {
        var working     = false,
            tasks       = [];

        var cargo = {
            tasks: tasks,
            payload: payload,
            saturated: null,
            empty: null,
            drain: null,
            drained: true,
            push: function (data, callback) {
                if (!_isArray(data)) {
                    data = [data];
                }
                _each(data, function(task) {
                    tasks.push({
                        data: task,
                        callback: typeof callback === 'function' ? callback : null
                    });
                    cargo.drained = false;
                    if (cargo.saturated && tasks.length === payload) {
                        cargo.saturated();
                    }
                });
                async.setImmediate(cargo.process);
            },
            process: function process() {
                if (working) return;
                if (tasks.length === 0) {
                    if(cargo.drain && !cargo.drained) cargo.drain();
                    cargo.drained = true;
                    return;
                }

                var ts = typeof payload === 'number'
                            ? tasks.splice(0, payload)
                            : tasks.splice(0, tasks.length);

                var ds = _map(ts, function (task) {
                    return task.data;
                });

                if(cargo.empty) cargo.empty();
                working = true;
                worker(ds, function () {
                    working = false;

                    var args = arguments;
                    _each(ts, function (data) {
                        if (data.callback) {
                            data.callback.apply(null, args);
                        }
                    });

                    process();
                });
            },
            length: function () {
                return tasks.length;
            },
            running: function () {
                return working;
            }
        };
        return cargo;
    };

    var _console_fn = function (name) {
        return function (fn) {
            var args = Array.prototype.slice.call(arguments, 1);
            fn.apply(null, args.concat([function (err) {
                var args = Array.prototype.slice.call(arguments, 1);
                if (typeof console !== 'undefined') {
                    if (err) {
                        if (console.error) {
                            console.error(err);
                        }
                    }
                    else if (console[name]) {
                        _each(args, function (x) {
                            console[name](x);
                        });
                    }
                }
            }]));
        };
    };
    async.log = _console_fn('log');
    async.dir = _console_fn('dir');
    /*async.info = _console_fn('info');
    async.warn = _console_fn('warn');
    async.error = _console_fn('error');*/

    async.memoize = function (fn, hasher) {
        var memo = {};
        var queues = {};
        hasher = hasher || function (x) {
            return x;
        };
        var memoized = function () {
            var args = Array.prototype.slice.call(arguments);
            var callback = args.pop();
            var key = hasher.apply(null, args);
            if (key in memo) {
                async.nextTick(function () {
                    callback.apply(null, memo[key]);
                });
            }
            else if (key in queues) {
                queues[key].push(callback);
            }
            else {
                queues[key] = [callback];
                fn.apply(null, args.concat([function () {
                    memo[key] = arguments;
                    var q = queues[key];
                    delete queues[key];
                    for (var i = 0, l = q.length; i < l; i++) {
                      q[i].apply(null, arguments);
                    }
                }]));
            }
        };
        memoized.memo = memo;
        memoized.unmemoized = fn;
        return memoized;
    };

    async.unmemoize = function (fn) {
      return function () {
        return (fn.unmemoized || fn).apply(null, arguments);
      };
    };

    async.times = function (count, iterator, callback) {
        var counter = [];
        for (var i = 0; i < count; i++) {
            counter.push(i);
        }
        return async.map(counter, iterator, callback);
    };

    async.timesSeries = function (count, iterator, callback) {
        var counter = [];
        for (var i = 0; i < count; i++) {
            counter.push(i);
        }
        return async.mapSeries(counter, iterator, callback);
    };

    async.seq = function (/* functions... */) {
        var fns = arguments;
        return function () {
            var that = this;
            var args = Array.prototype.slice.call(arguments);
            var callback = args.pop();
            async.reduce(fns, args, function (newargs, fn, cb) {
                fn.apply(that, newargs.concat([function () {
                    var err = arguments[0];
                    var nextargs = Array.prototype.slice.call(arguments, 1);
                    cb(err, nextargs);
                }]))
            },
            function (err, results) {
                callback.apply(that, [err].concat(results));
            });
        };
    };

    async.compose = function (/* functions... */) {
      return async.seq.apply(null, Array.prototype.reverse.call(arguments));
    };

    var _applyEach = function (eachfn, fns /*args...*/) {
        var go = function () {
            var that = this;
            var args = Array.prototype.slice.call(arguments);
            var callback = args.pop();
            return eachfn(fns, function (fn, cb) {
                fn.apply(that, args.concat([cb]));
            },
            callback);
        };
        if (arguments.length > 2) {
            var args = Array.prototype.slice.call(arguments, 2);
            return go.apply(this, args);
        }
        else {
            return go;
        }
    };
    async.applyEach = doParallel(_applyEach);
    async.applyEachSeries = doSeries(_applyEach);

    async.forever = function (fn, callback) {
        function next(err) {
            if (err) {
                if (callback) {
                    return callback(err);
                }
                throw err;
            }
            fn(next);
        }
        next();
    };

    // Node.js
    if (typeof module !== 'undefined' && module.exports) {
        module.exports = async;
    }
    // AMD / RequireJS
    else if (typeof define !== 'undefined' && define.amd) {
        define([], function () {
            return async;
        });
    }
    // included directly via <script> tag
    else {
        root.async = async;
    }

}());

}).call(this,require("FWaASH"))
},{"FWaASH":13}],12:[function(require,module,exports){
/*!
  * Bowser - a browser detector
  * https://github.com/ded/bowser
  * MIT License | (c) Dustin Diaz 2014
  */

!function (name, definition) {
  if (typeof module != 'undefined' && module.exports) module.exports['browser'] = definition()
  else if (typeof define == 'function' && define.amd) define(definition)
  else this[name] = definition()
}('bowser', function () {
  /**
    * See useragents.js for examples of navigator.userAgent
    */

  var t = true

  function detect(ua) {

    function getFirstMatch(regex) {
      var match = ua.match(regex);
      return (match && match.length > 1 && match[1]) || '';
    }

    function getSecondMatch(regex) {
      var match = ua.match(regex);
      return (match && match.length > 1 && match[2]) || '';
    }

    var iosdevice = getFirstMatch(/(ipod|iphone|ipad)/i).toLowerCase()
      , likeAndroid = /like android/i.test(ua)
      , android = !likeAndroid && /android/i.test(ua)
      , edgeVersion = getFirstMatch(/edge\/(\d+(\.\d+)?)/i)
      , versionIdentifier = getFirstMatch(/version\/(\d+(\.\d+)?)/i)
      , tablet = /tablet/i.test(ua)
      , mobile = !tablet && /[^-]mobi/i.test(ua)
      , result

    if (/opera|opr/i.test(ua)) {
      result = {
        name: 'Opera'
      , opera: t
      , version: versionIdentifier || getFirstMatch(/(?:opera|opr)[\s\/](\d+(\.\d+)?)/i)
      }
    }
    else if (/windows phone/i.test(ua)) {
      result = {
        name: 'Windows Phone'
      , windowsphone: t
      }
      if (edgeVersion) {
        result.msedge = t
        result.version = edgeVersion
      }
      else {
        result.msie = t
        result.version = getFirstMatch(/iemobile\/(\d+(\.\d+)?)/i)
      }
    }
    else if (/msie|trident/i.test(ua)) {
      result = {
        name: 'Internet Explorer'
      , msie: t
      , version: getFirstMatch(/(?:msie |rv:)(\d+(\.\d+)?)/i)
      }
    }
    else if (/chrome.+? edge/i.test(ua)) {
      result = {
        name: 'Microsoft Edge'
      , msedge: t
      , version: edgeVersion
      }
    }
    else if (/chrome|crios|crmo/i.test(ua)) {
      result = {
        name: 'Chrome'
      , chrome: t
      , version: getFirstMatch(/(?:chrome|crios|crmo)\/(\d+(\.\d+)?)/i)
      }
    }
    else if (iosdevice) {
      result = {
        name : iosdevice == 'iphone' ? 'iPhone' : iosdevice == 'ipad' ? 'iPad' : 'iPod'
      }
      // WTF: version is not part of user agent in web apps
      if (versionIdentifier) {
        result.version = versionIdentifier
      }
    }
    else if (/sailfish/i.test(ua)) {
      result = {
        name: 'Sailfish'
      , sailfish: t
      , version: getFirstMatch(/sailfish\s?browser\/(\d+(\.\d+)?)/i)
      }
    }
    else if (/seamonkey\//i.test(ua)) {
      result = {
        name: 'SeaMonkey'
      , seamonkey: t
      , version: getFirstMatch(/seamonkey\/(\d+(\.\d+)?)/i)
      }
    }
    else if (/firefox|iceweasel/i.test(ua)) {
      result = {
        name: 'Firefox'
      , firefox: t
      , version: getFirstMatch(/(?:firefox|iceweasel)[ \/](\d+(\.\d+)?)/i)
      }
      if (/\((mobile|tablet);[^\)]*rv:[\d\.]+\)/i.test(ua)) {
        result.firefoxos = t
      }
    }
    else if (/silk/i.test(ua)) {
      result =  {
        name: 'Amazon Silk'
      , silk: t
      , version : getFirstMatch(/silk\/(\d+(\.\d+)?)/i)
      }
    }
    else if (android) {
      result = {
        name: 'Android'
      , version: versionIdentifier
      }
    }
    else if (/phantom/i.test(ua)) {
      result = {
        name: 'PhantomJS'
      , phantom: t
      , version: getFirstMatch(/phantomjs\/(\d+(\.\d+)?)/i)
      }
    }
    else if (/blackberry|\bbb\d+/i.test(ua) || /rim\stablet/i.test(ua)) {
      result = {
        name: 'BlackBerry'
      , blackberry: t
      , version: versionIdentifier || getFirstMatch(/blackberry[\d]+\/(\d+(\.\d+)?)/i)
      }
    }
    else if (/(web|hpw)os/i.test(ua)) {
      result = {
        name: 'WebOS'
      , webos: t
      , version: versionIdentifier || getFirstMatch(/w(?:eb)?osbrowser\/(\d+(\.\d+)?)/i)
      };
      /touchpad\//i.test(ua) && (result.touchpad = t)
    }
    else if (/bada/i.test(ua)) {
      result = {
        name: 'Bada'
      , bada: t
      , version: getFirstMatch(/dolfin\/(\d+(\.\d+)?)/i)
      };
    }
    else if (/tizen/i.test(ua)) {
      result = {
        name: 'Tizen'
      , tizen: t
      , version: getFirstMatch(/(?:tizen\s?)?browser\/(\d+(\.\d+)?)/i) || versionIdentifier
      };
    }
    else if (/safari/i.test(ua)) {
      result = {
        name: 'Safari'
      , safari: t
      , version: versionIdentifier
      }
    }
    else {
      result = {
        name: getFirstMatch(/^(.*)\/(.*) /),
        version: getSecondMatch(/^(.*)\/(.*) /)
     };
   }

    // set webkit or gecko flag for browsers based on these engines
    if (!result.msedge && /(apple)?webkit/i.test(ua)) {
      result.name = result.name || "Webkit"
      result.webkit = t
      if (!result.version && versionIdentifier) {
        result.version = versionIdentifier
      }
    } else if (!result.opera && /gecko\//i.test(ua)) {
      result.name = result.name || "Gecko"
      result.gecko = t
      result.version = result.version || getFirstMatch(/gecko\/(\d+(\.\d+)?)/i)
    }

    // set OS flags for platforms that have multiple browsers
    if (!result.msedge && (android || result.silk)) {
      result.android = t
    } else if (iosdevice) {
      result[iosdevice] = t
      result.ios = t
    }

    // OS version extraction
    var osVersion = '';
    if (result.windowsphone) {
      osVersion = getFirstMatch(/windows phone (?:os)?\s?(\d+(\.\d+)*)/i);
    } else if (iosdevice) {
      osVersion = getFirstMatch(/os (\d+([_\s]\d+)*) like mac os x/i);
      osVersion = osVersion.replace(/[_\s]/g, '.');
    } else if (android) {
      osVersion = getFirstMatch(/android[ \/-](\d+(\.\d+)*)/i);
    } else if (result.webos) {
      osVersion = getFirstMatch(/(?:web|hpw)os\/(\d+(\.\d+)*)/i);
    } else if (result.blackberry) {
      osVersion = getFirstMatch(/rim\stablet\sos\s(\d+(\.\d+)*)/i);
    } else if (result.bada) {
      osVersion = getFirstMatch(/bada\/(\d+(\.\d+)*)/i);
    } else if (result.tizen) {
      osVersion = getFirstMatch(/tizen[\/\s](\d+(\.\d+)*)/i);
    }
    if (osVersion) {
      result.osversion = osVersion;
    }

    // device type extraction
    var osMajorVersion = osVersion.split('.')[0];
    if (tablet || iosdevice == 'ipad' || (android && (osMajorVersion == 3 || (osMajorVersion == 4 && !mobile))) || result.silk) {
      result.tablet = t
    } else if (mobile || iosdevice == 'iphone' || iosdevice == 'ipod' || android || result.blackberry || result.webos || result.bada) {
      result.mobile = t
    }

    // Graded Browser Support
    // http://developer.yahoo.com/yui/articles/gbs
    if (result.msedge ||
        (result.msie && result.version >= 10) ||
        (result.chrome && result.version >= 20) ||
        (result.firefox && result.version >= 20.0) ||
        (result.safari && result.version >= 6) ||
        (result.opera && result.version >= 10.0) ||
        (result.ios && result.osversion && result.osversion.split(".")[0] >= 6) ||
        (result.blackberry && result.version >= 10.1)
        ) {
      result.a = t;
    }
    else if ((result.msie && result.version < 10) ||
        (result.chrome && result.version < 20) ||
        (result.firefox && result.version < 20.0) ||
        (result.safari && result.version < 6) ||
        (result.opera && result.version < 10.0) ||
        (result.ios && result.osversion && result.osversion.split(".")[0] < 6)
        ) {
      result.c = t
    } else result.x = t

    return result
  }

  var bowser = detect(typeof navigator !== 'undefined' ? navigator.userAgent : '')

  bowser.test = function (browserList) {
    for (var i = 0; i < browserList.length; ++i) {
      var browserItem = browserList[i];
      if (typeof browserItem=== 'string') {
        if (browserItem in bowser) {
          return true;
        }
      }
    }
    return false;
  }

  /*
   * Set our detect method to the main bowser object so we can
   * reuse it to test other user agents.
   * This is needed to implement future tests.
   */
  bowser._detect = detect;

  return bowser
});

},{}],13:[function(require,module,exports){
// shim for using process in browser

var process = module.exports = {};

process.nextTick = (function () {
    var canSetImmediate = typeof window !== 'undefined'
    && window.setImmediate;
    var canPost = typeof window !== 'undefined'
    && window.postMessage && window.addEventListener
    ;

    if (canSetImmediate) {
        return function (f) { return window.setImmediate(f) };
    }

    if (canPost) {
        var queue = [];
        window.addEventListener('message', function (ev) {
            var source = ev.source;
            if ((source === window || source === null) && ev.data === 'process-tick') {
                ev.stopPropagation();
                if (queue.length > 0) {
                    var fn = queue.shift();
                    fn();
                }
            }
        }, true);

        return function nextTick(fn) {
            queue.push(fn);
            window.postMessage('process-tick', '*');
        };
    }

    return function nextTick(fn) {
        setTimeout(fn, 0);
    };
})();

process.title = 'browser';
process.browser = true;
process.env = {};
process.argv = [];

function noop() {}

process.on = noop;
process.addListener = noop;
process.once = noop;
process.off = noop;
process.removeListener = noop;
process.removeAllListeners = noop;
process.emit = noop;

process.binding = function (name) {
    throw new Error('process.binding is not supported');
}

// TODO(shtylman)
process.cwd = function () { return '/' };
process.chdir = function (dir) {
    throw new Error('process.chdir is not supported');
};

},{}],14:[function(require,module,exports){
if (typeof module === "object") module.exports = SortedObjectArray;

SortedObjectArray.prototype.insert = function (element) {
    var array = this.array;
    var index = array.length;
    array.push(element);

    while (index) {
        var i = index, j = --index;

        if (this.getKey(i) < this.getKey(j)) {
            var temp = array[i];
            array[i] = array[j];
            array[j] = temp;
        }
    }

    return this;
};

SortedObjectArray.prototype.getKey = function(index) {
    return this.array[index][this.key];
};

SortedObjectArray.prototype.search = function (element) {
    var elementKey = (typeof(element) === 'object')? element[this.key]: element;

    var low = 0;
    var array = this.array;
    var high = array.length;

    while (high > low) {
        var index = (high + low) / 2 >>> 0;
        var cursor = this.getKey(index);

        if (cursor < elementKey) low = index + 1;
        else if (cursor > elementKey) high = index;
        else return index;
    }

    return -1;
};

SortedObjectArray.prototype.remove = function (element) {
    var index = this.search(element);
    if (index >= 0) this.array.splice(index, 1);
    return this;
};

function SortedObjectArray() {
    var index = 0;

    if (typeof(arguments[0]) != 'object' && arguments[0] != null){
        this.key = arguments[0];
        index = 1;
    } else {
        this.key = 'id';
    }
    
    this.array = [];
    var length = arguments.length;
    
    while (index < length) this.insert(arguments[index++]);
}
},{}]},{},[])