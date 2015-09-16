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
  var IndexedDb, LocalStorageDb, MemoryDb, WebSQLDb, browser;
  IndexedDb = require('./IndexedDb');
  WebSQLDb = require('./WebSQLDb');
  LocalStorageDb = require('./LocalStorageDb');
  MemoryDb = require('./MemoryDb');
  browser = bowser.browser;
  return new MemoryDb(options, success);
};

exports.migrateLocalDb = function(fromDb, toDb, success, error) {
  var HybridDb, col, hybridDb, name, _ref;
  HybridDb = require('./HybridDb');
  hybridDb = new HybridDb(fromDb, toDb);
  _ref = fromDb.collections;
  for (name in _ref) {
    col = _ref[name];
    if (toDb[name]) {
      hybridDb.addCollection(name);
    }
  }
  return hybridDb.upload(success, error);
};

exports.processFind = function(items, selector, options) {
  var filtered, me;
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
    sorted = new SortedObjectArray(Object.keys(options)[0], me)['array'][0];
    if (direction < 0) {
      sorted = sorted.reverse();
    }
    return sorted = addMethods(sorted);
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
    return addMethods(_.first(filtered, max));
  };
  me['sort'] = function(options) {
    var direction, sorted;
    direction = options[Object.keys(options)[0]];
    sorted = new SortedObjectArray(Object.keys(options)[0], me)['array'][0];
    if (direction < 0) {
      sorted = sorted.reverse();
    }
    return addMethods(sorted);
  };
  return me;
};

exports.convertToObject = function(selectors) {
  var key, obj, select, _i, _len;
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
        h[i] = item[values[counter].replace('$', '')];
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
        operation = Object.keys(values[counter])[0];
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
      if (it[_id] === filt['_id']) {
        sum += it[sum_key.replace('$', '')];
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
      if (it[_id] === filt['_id']) {
        sum += it[sum_key.replace('$', '')];
        length += 1;
      }
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

exports.aggregateProject = function(selector, filtered) {
  var filt, k, keep, key, keys, _i, _j, _k, _len, _len1, _len2, _temp_filtered;
  keys = Object.keys(selector['$project']);
  keep = [];
  for (_i = 0, _len = keys.length; _i < _len; _i++) {
    k = keys[_i];
    if (selector['$project'][k]) {
      keep.push(k);
    }
  }
  _temp_filtered = [];
  for (_j = 0, _len1 = filtered.length; _j < _len1; _j++) {
    filt = filtered[_j];
    for (_k = 0, _len2 = keep.length; _k < _len2; _k++) {
      key = keep[_k];
      if (filt[key.replace('$', '')]) {
        filt = _.pick(filt, key.replace('$', ''));
      }
    }
    _temp_filtered.push(filt);
  }
  return filtered = _temp_filtered;
};

exports.processAggregate = function(items, selector, options) {
  var filtered;
  filtered = _.values(items);
  selector = exports.convertToObject(selector);
  if (selector['$match']) {
    filtered = _.filter(filtered, compileDocumentSelector(selector['$match']));
  }
  if (selector['$group']) {
    filtered = exports.aggregateGroup(filtered, items, selector, options);
  }
  if (selector['$limit']) {
    filtered = filtered.slice(0, selector['$limit']);
  }
  if (selector['$sort']) {
    filtered = exports.aggregateSort(selector, filtered);
  }
  if (selector['$project']) {
    filtered = exports.aggregateProject(selector, filtered);
  }
  if (selector['$skip']) {
    filtered.splice(0, selector['$skip']);
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
    var field, from, newItem, obj, path, pathElem, to, _i, _j, _k, _l, _len, _len1, _len2, _len3, _len4, _m, _ref, _ref1, _ref2, _ref3;
    item = _.cloneDeep(item);
    newItem = {};
    if (_.first(_.values(fields)) === 1) {
      _ref = _.keys(fields).concat(["_id"]);
      for (_i = 0, _len = _ref.length; _i < _len; _i++) {
        field = _ref[_i];
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
        _ref1 = _.initial(path);
        for (_k = 0, _len2 = _ref1.length; _k < _len2; _k++) {
          pathElem = _ref1[_k];
          to[pathElem] = to[pathElem] || {};
          to = to[pathElem];
          from = from[pathElem];
        }
        to[_.last(path)] = from[_.last(path)];
      }
      return newItem;
    } else {
      _ref2 = _.keys(fields);
      for (_l = 0, _len3 = _ref2.length; _l < _len3; _l++) {
        field = _ref2[_l];
        path = field.split(".");
        obj = item;
        _ref3 = _.initial(path);
        for (_m = 0, _len4 = _ref3.length; _m < _len4; _m++) {
          pathElem = _ref3[_m];
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
