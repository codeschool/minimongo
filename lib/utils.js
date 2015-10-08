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
      index = placeholder[placeholder.length - 1];
      database.items[item._id][placeholder[0]][index] = _.values(docs['$set'])[index];
    } else {
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
  var filt, k, keep, key, keys, temp, _i, _j, _k, _len, _len1, _len2, _temp_filtered;
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
      if (!filt[key] && !filt[key.replace('$', '')]) {
        k = key.split('.');
        temp[k[0]] = filt[k[0]];
      } else if (filt[key.replace('$', '')]) {
        temp[key.replace('$', '')] = filt[key.replace('$', '')];
      }
    }
    _temp_filtered.push(temp);
  }
  return filtered = _temp_filtered;
};

exports.processAggregate = function(items, selector, options) {
  var filtered, key, _i, _len;
  if (!_.isArray(selector)) {
    selector = [selector];
  }
  filtered = _.values(items);
  for (_i = 0, _len = selector.length; _i < _len; _i++) {
    key = selector[_i];
    if (_.intersection(['$match', '$project', '$group', '$limit', '$sort', '$skip'], Object.keys(key)).length < 1) {
      throw {
        message: 'This pipeline operator is not supported with this browser version of MongoDB'
      };
    }
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
