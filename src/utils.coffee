# Utilities for db handling
_ = require 'lodash'
async = require 'async'
bowser = require 'bowser'
SortedObjectArray = require 'sorted-object-array'

compileDocumentSelector = require('./selector').compileDocumentSelector
compileSort = require('./selector').compileSort

# Test window.localStorage
isLocalStorageSupported = ->
  if not window.localStorage
    return false
  try
    window.localStorage.setItem("test", "test")
    window.localStorage.removeItem("test")
    return true
  catch e
    return false


# Compile a document selector (query) to a lambda function
exports.compileDocumentSelector = compileDocumentSelector

# Select appropriate local database, prefering IndexedDb, then WebSQLDb, then LocalStorageDb, then MemoryDb
exports.autoselectLocalDb = (options, success, error) ->
  # Here due to browserify circularity quirks
  # IndexedDb = require './IndexedDb'
  # WebSQLDb = require './WebSQLDb'
  # LocalStorageDb = require './LocalStorageDb'
  MemoryDb = require './MemoryDb'

  # Get browser capabilities
  browser = bowser.browser

  # Browsers with no localStorage support don't deserve anything better than a MemoryDb
  # if not isLocalStorageSupported()
  return new MemoryDb(options, success)

#   # Always use WebSQL in cordova
#   if window.cordova
#     console.log "Selecting WebSQLDb for Cordova"
#     # WebSQLDb must success in Cordova
#     return new WebSQLDb options, success, error
#
#   # Use WebSQL in Android, iOS, Chrome, Safari, Opera, Blackberry
#   if browser.android or browser.ios or browser.chrome or browser.safari or browser.opera or browser.blackberry
#     console.log "Selecting WebSQLDb for browser"
#     return new WebSQLDb options, success, (err) =>
#       console.log "Failed to create WebSQLDb: " + (if err then err.message)
#       # Create memory db instead
#       return new MemoryDb(options, success)
#
#   # Use IndexedDb on Firefox >= 16
#   if browser.firefox and browser.version >= 16
#     console.log "Selecting IndexedDb for browser"
#     return new IndexedDb options, success, (err) =>
#       console.log "Failed to create IndexedDb: " + (if err then err.message)
#       # Create memory db instead
#       return new MemoryDb(options, success)
#
#   # Use Local Storage otherwise
#   console.log "Selecting LocalStorageDb for fallback"
#   return new LocalStorageDb(options, success, error)

# Migrates a local database's pending upserts and removes from one database to another
# Useful for upgrading from one type of database to another
exports.migrateLocalDb = (fromDb, toDb, success, error) ->
  # Migrate collection using a HybridDb
  # Here due to browserify circularity quirks
#   HybridDb = require './HybridDb'
#   hybridDb = new HybridDb(fromDb, toDb)
#   for name, col of fromDb.collections
#     if toDb[name]
#       hybridDb.addCollection(name)
#
#   hybridDb.upload(success, error)

exports.processUpdate = (theItems, selector, docs, bases, database) ->

  if bases && bases['upsert'] && theItems.length < 1
    if _.include(Object.keys(docs), '$set')
      database.insert(_.merge(selector, docs['$set']))

    #upsert doesnt find but operation used
    else if Object.keys(docs)[0].indexOf('$') >= 0
      database.insert(_.merge(selector, docs[Object.keys(docs)[0]]))

    else
      database.insert(_.merge(selector, docs))
    database.upserts[docs._id] = docs

  if (!!bases and !!bases.multi) or theItems.length < 1
    theItems
  else
    theItems = [_.first(theItems)]

  for item in theItems
    if item.docs == undefined
      item.doc = docs
    if item.base == undefined
      item.base = database.items[item.doc._id] or null
    item = _.cloneDeep(item)

    #keep track of found records for writeResult
    database.founds[item._id] = docs

    docUpdate = true
    if _.include(Object.keys(docs), "$inc")
      docUpdate = false
      database.updates[item._id] = docs
      for k,v of docs['$inc']
        database.items[item._id][k] = database.items[item._id][k] + v

    if _.include(Object.keys(docs), "$set")
      docUpdate = exports.update$set(selector, database, docs, item)

    if _.include(Object.keys(docs), "$unset")
      docUpdate = exports.update$unset(database, docs, item)

    if _.include(Object.keys(docs), "$rename")
      database.updates[item._id] = docs
      docUpdate = false
      for k,v of docs['$rename']
        database.items[item._id][v] = database.items[item._id][k]
        database.items[item._id] = _.omit(database.items[item._id], k)

    if _.include(Object.keys(docs), "$max")
      docUpdate = exports.update$max(database, docs, item)

    if _.include(Object.keys(docs), "$min")
      docUpdate = exports.update$min(database, docs, item)

    if _.include(Object.keys(docs), "$mul")
      docUpdate = exports.update$mul(database, docs, item)

    if _.include(Object.keys(docs), "$addToSet")
      docUpdate = exports.update$addToSet(database, docs, item)

    if _.include(Object.keys(docs), "$push")
      docUpdate = exports.update$push(database, docs, item)

    if docUpdate
      database.updates[docs._id] = docs
      for k,v of docs
        id = database.items[item._id]._id
        database.items[item._id] = docs
      database.items[item._id]._id = id
  return ''

exports.update$push = (database, docs, item) ->
  docUpdate = false
  hit = false
  for k,v of docs['$push']
    if database.items[item._id][k]
      hit = true
      database.items[item._id][k].push(v)
  #ensure actually removed for writeResult
  if hit
    database.updates[item._id] = docs
  docUpdate


exports.update$addToSet = (database, docs, item) ->
  docUpdate = false
  hit = false
  for k,v of docs['$addToSet']
    if database.items[item._id][k] && !_.include(database.items[item._id][k], v)
      hit = true
      database.items[item._id][k].push(v)
  #ensure actually removed for writeResult
  if hit
    database.updates[item._id] = docs
  docUpdate


exports.update$unset = (database, docs, item) ->
  docUpdate = false
  hit = false
  for k,v of docs['$unset']
    if database.items[item._id][k]
      hit = true
    database.items[item._id] = _.omit(database.items[item._id], k)
  #ensure actually removed for writeResult
  if hit
    database.updates[item._id] = docs
  docUpdate

exports.update$set = (selector, database, docs, item) ->
  database.updates[item._id] = docs
  docUpdate = false
  for k,v of docs['$set']
    placeholder = k.split('.')
    if(placeholder[placeholder.length - 1] == '$')
      arr = database.items[item._id][placeholder[0]]
      index = arr.indexOf(_.values(selector)[0])
      database.items[item._id][placeholder[0]][index] = _.values(docs['$set'])[0]
    else if !isNaN(placeholder[placeholder.length - 1])
      arr = database.items[item._id][placeholder[0]]
      index = placeholder[placeholder.length - 1]
      database.items[item._id][placeholder[0]][index] = _.values(docs['$set'])[index]
    else
      database.items[item._id][k] = v
  docUpdate

exports.update$max = (database, docs, item) ->
  docUpdate = false
  hit = false
  for k,v of docs['$max']
    if _.include(k, '.')
      keys = exports.prepareDot(k)
      data = exports.convertDot(item, keys[0])[keys[1]]
      if data < v
        exports.convertDot(item, keys[0])[keys[1]] = v
        database.items[item._id] = _.omit(item, 'doc', 'base')
        hit = true
    else if database.items[item._id][k] < v
      database.items[item._id][k] = v
      hit = true

  if(hit)
    database.updates[item._id] = docs
  docUpdate

exports.update$min = (database, docs, item) ->
  database.updates[item._id] = docs
  docUpdate = false
  hit = false
  for k,v of docs['$min']
    if _.include(k, '.')
      keys = exports.prepareDot(k)
      data = exports.convertDot(item, keys[0])[keys[1]]
      if data > v
        exports.convertDot(item, keys[0])[keys[1]] = v
        database.items[item._id] = _.omit(item, 'doc', 'base')
        hit = true
    else if database.items[item._id][k] > v
      database.items[item._id][k] = v
      hit = true

  if(hit)
    database.updates[item._id] = docs
  docUpdate

exports.update$mul = (database, docs, item) ->
  database.updates[item._id] = docs
  docUpdate = false
  for k,v of docs['$mul']
    if _.include(k, '.')
      keys = exports.prepareDot(k)
      exports.convertDot(item, keys[0])[keys[1]] = exports.convertDot(item, keys[0])[keys[1]] * v
      database.items[item._id] = _.omit(item, 'doc', 'base')
    else
      database.items[item._id][k] = database.items[item._id][k] * v
  docUpdate

exports.prepareDot = (k) ->
  arr = k.split('.')
  final_key = arr.pop()
  keys = arr.join('.')
  return [keys, final_key]

exports.convertDot = (obj, _is, value) ->
  if typeof _is == 'string'
    exports.convertDot obj, _is.split('.'), value
  else if _is.length == 1 and value != undefined
    obj[_is[0]] = value
  else if _is.length == 0
    obj
  else
    exports.convertDot obj[_is[0]], _is.slice(1), value


# exports.processDot = (item, docs, database) ->
    
    # database.items[item._id][k] = database.items[item._id][k] * v


# Processes a find with sorting and filtering and limiting
exports.processFind = (items, selector, options) ->
  filtered = _.filter(_.values(items), compileDocumentSelector(selector))

  # Handle geospatial operators
  filtered = processNearOperator(selector, filtered)
  filtered = processGeoIntersectsOperator(selector, filtered)

  # if options and options.sort
  #   filtered.sort(compileSort(options.sort))

  # if options and options.skip
  #   filtered = _.rest filtered, options.skip

  # if options and options.limit
  #   filtered = _.first filtered, options.limit

  # Clone to prevent accidental updates, or apply fields if present
  if options #and options.fields
    filtered = exports.filterFields(filtered, options)
  else
    filtered = _.map filtered, (doc) -> _.cloneDeep(doc)

  me = filtered
  #need to readd the methods here becaues they are getting lost #addMethods
  filtered['skip'] = (amount) ->
    if me.preLimit
      me.preLimit.splice 0, amount
      data = me.preLimit
    else
      me.splice 0, amount
      data = me
    return addMethods(data)

  filtered['limit'] = (max) ->
    obj = addMethods(me.slice(0, max))
    obj.preLimit = filtered
    return obj

  filtered['sort'] = (options) ->
    direction = options[Object.keys(options)[0]]
    sorted = addMethods(_.sortBy me, Object.keys(options)[0])
    if direction == -1
      sorted = sorted.reverse()
    else if direction == 1 or _.isEmpty(Object.keys(options))
      sorted = sorted
    else
      throw {message: 'BadValue bad sort specification'}
    return sorted

  filtered['count'] = () ->
    return _.size(me)

  return filtered

addMethods = (filtered) ->
  me = filtered

  me['skip'] = (amount) ->
    if me.preLimit
      me.preLimit.splice 0, amount
      data = me.preLimit
    else
      me.splice 0, amount
      data = me
    return addMethods(data)

  me['limit'] = (max) ->
    obj = addMethods(me.slice(0, max))
    obj.preLimit = filtered
    return obj

  me['sort'] = (options) ->
    direction = options[Object.keys(options)[0]]
    sorted = addMethods(_.sortBy me, Object.keys(options)[0])
    if direction < 0
      sorted = sorted.reverse()
    return sorted

  filtered['count'] = () ->
    return _.size(me)

  return me

exports.convertToObject = (selectors) ->
  if selectors.constructor != Array
    return selectors
  obj = {}
  for select in selectors
    key = Object.keys(select)[0]
    obj[key] = select[key]
  obj

exports.aggregateGroup = (filtered, items, selector, options) ->
  temp_filtered = []
  _items = filtered
  keys = _.keys(selector['$group'])
  values  = _.values(selector['$group'])
  _id = null
  # throw if no _id
  if !_.include(keys, '_id')
    throw {message: '_id field was not supplied'}

  for item in _items
    h = {}
    counter = 0
    for i in keys
      if typeof values[counter] == 'string'
        if i == '_id'
          _id = values[counter].replace('$', '')
        h[i] = exports.compileDollar(item, values[counter])
        taken = false
        for _filter in temp_filtered
          if _.include(_.values(_filter), h._id)
            taken = true
        if !taken
          temp_filtered.push h
      else
        operation = Object.keys(values[counter])[0]
        if operation == '$max'
          exports.aggregateMax(values, temp_filtered, _items, counter, _id, i)
        if operation == '$min'
          exports.aggregateMin(values, temp_filtered, _items, counter, _id, i)
        if operation == '$sum'
          exports.aggregateAdd(values, temp_filtered, _items, counter, _id, i)
        else if operation == '$avg'
          exports.aggregateAvg(values, temp_filtered, _items, counter, _id, i)
      counter += 1
  filtered = temp_filtered

exports.compileDollar = (item, value) ->
  if _.include value, '$'
    if _.include value, '.'
      k = value.replace('$', '').split('.')
      item[k[0]][k[1]]
    else
      item[value.replace('$', '')]
  else
    value



exports.processOperation = (it, sum_key) ->
  if typeof sum_key == 'number'
    return sum_key
  else if _.include(sum_key, '.')
    parts = sum_key.replace('$', '').split('.')
    if it[parts[0]]
      val = it[parts[0]][parts[1]]
    else
      return null
  else if !_.include(sum_key, '$')
    0
  else
    val = it[sum_key.replace('$', '')] || it[sum_key]
  if typeof val == 'object'
    k = sum_key.replace('$', '').split('.')
    return it[sum_key][k[1]]
  else if typeof val == 'string' or typeof val == 'number'
    val

exports.aggregateMax = (values, temp_filtered, _items, counter, _id, i) ->
  opt_keys = Object.keys(values[counter])
  sum_key = values[counter][opt_keys]
  for filt in temp_filtered
    max = 0
    for it in _items
      if exports.checkIfMatch(filt, it, _id, sum_key)
        if max < exports.processOperation(it, sum_key)
          max = exports.processOperation(it, sum_key)
    filt[i] = max

exports.aggregateMin = (values, temp_filtered, _items, counter, _id, i) ->
  opt_keys = Object.keys(values[counter])
  sum_key = values[counter][opt_keys]
  for filt in temp_filtered
    max = null
    for it in _items
      if exports.checkIfMatch(filt, it, _id, sum_key)
        if !max then max = exports.processOperation(it, sum_key)
        if max > exports.processOperation(it, sum_key)
          max = exports.processOperation(it, sum_key)
    filt[i] = max

exports.aggregateAdd = (values, temp_filtered, _items, counter, _id, i) ->
  opt_keys = Object.keys(values[counter])
  sum_key = values[counter][opt_keys]
  for filt in temp_filtered
    sum = 0
    for it in _items
      if exports.checkIfMatch(filt, it, _id, sum_key)
        sum += exports.processOperation(it, sum_key)
    filt[i] = sum

exports.aggregateAvg = (values, temp_filtered, _items, counter, _id, i) ->
  opt_keys = Object.keys(values[counter])
  sum_key = values[counter][opt_keys]
  for filt in temp_filtered
    sum = 0
    length = 0
    for it in _items
      if exports.checkIfMatch(filt, it, _id, sum_key)
        sum += exports.processOperation(it, sum_key)
        length += 1
    #ensure no divide by zero
    if sum < 1
      length = 1
    filt[i] = sum / length
  temp_filtered

exports.aggregateSort = (selector, filtered) ->
  direction = selector['$sort'][Object.keys(selector['$sort'])[0]]
  key = Object.keys(selector['$sort'])[0].replace('$', '')
  compare = (a, b) ->
    if a[key] < b[key]
      return -1
    if a[key] > b[key]
      return 1
    return 0
  filtered = filtered.sort(compare)
  if direction < 0
    filtered = filtered.reverse()
  filtered

exports.checkIfMatch = (filt, it, _id, sum_key) ->
  filt['_id'] == exports.processOperation(it, _id) || _id == filt['_id'] or (it[_id] == filt['_id'] and (typeof sum_key == 'number' || _.include(sum_key, '$')))


exports.aggregateProject = (selector, filtered) ->
  keys = Object.keys(selector['$project'])
  keep = []
  for k in keys
    if selector['$project'][k]
      keep.push k


  #need to check if id is wanted
  if selector['$project']['_id']
    keep.push '_id'

  _temp_filtered = []
  for filt in filtered
    temp = {}
    for key in keep
      if !filt[key] and !filt[key.replace('$', '')]
        k = key.split('.')
        temp[k[0]] = filt[k[0]]
      else if filt[key.replace('$', '')]
        temp[key.replace('$', '')] = filt[key.replace('$', '')]
    _temp_filtered.push temp
  filtered = _temp_filtered


exports.processAggregate = (items, selector, options) ->
  if not _.isArray(selector)
    selector = [selector]
  filtered = _.values(items)

  for key in selector
    if key['$match']
      filtered = _.filter(filtered, compileDocumentSelector(key['$match']))
    if key['$project']
      filtered = exports.aggregateProject(key, filtered)
    if key['$group']
      filtered = exports.aggregateGroup(filtered, items, key, options)
    if key['$limit']
      filtered = filtered.slice(0, key['$limit'])
    if key['$sort']
      filtered = exports.aggregateSort(key, filtered)
    if key['$skip']
      filtered.splice(0, key['$skip'])
  return filtered



exports.filterFields = (items, fields={}) ->
  # Handle trivial case
  if _.keys(fields).length == 0
    return items

  # For each item
  return _.map items, (item) ->
    item = _.cloneDeep(item)

    newItem = {}

    if _.include(_.values(fields), 1) or _.include(_.values(fields), true)
      # Include fields
      if _.include(_.keys(fields), '_id') and !fields['_id']
        availKeys = _.keys(fields)
        fields['_id'] = false
      else
        availKeys = _.keys(fields).concat(["_id"])
        fields['_id'] = true

      for field in availKeys
        path = field.split(".")

        # Determine if path exists
        obj = item
        for pathElem in path
          if obj
            obj = obj[pathElem]

        if not obj?
          continue

        # Go into path, creating as necessary
        from = item
        to = newItem
        for pathElem in _.initial(path)
          to[pathElem] = to[pathElem] or {}

          # Move inside
          to = to[pathElem]
          from = from[pathElem]

        # Copy value
        if fields[field]
          to[_.last(path)] = from[_.last(path)]
      return newItem
    else
      # Exclude fields
      for field in _.keys(fields)
        path = field.split(".")

        # Go inside path
        obj = item
        for pathElem in _.initial(path)
          if obj
            obj = obj[pathElem]

        # If not there, don't exclude
        if not obj?
          continue

        delete obj[_.last(path)]

      return item


# Creates a unique identifier string
exports.createUid = ->
  'xxxxxxxxxxxx4xxxyxxxxxxxxxxxxxxx'.replace(/[xy]/g, (c) ->
    r = Math.random()*16|0
    v = if c == 'x' then r else (r&0x3|0x8)
    return v.toString(16)
   )

processNearOperator = (selector, list) ->
  for key, value of selector
    if value? and value['$near']
      geo = value['$near']['$geometry']
      if geo.type != 'Point'
        break

      list = _.filter list, (doc) ->
        return doc[key] and doc[key].type == 'Point'

      # Get distances
      distances = _.map list, (doc) ->
        return { doc: doc, distance: getDistanceFromLatLngInM(
            geo.coordinates[1], geo.coordinates[0],
            doc[key].coordinates[1], doc[key].coordinates[0])
        }

      # Filter non-points
      distances = _.filter distances, (item) -> item.distance >= 0

      # Sort by distance
      distances = _.sortBy distances, 'distance'

      # Filter by maxDistance
      if value['$near']['$maxDistance']
        distances = _.filter distances, (item) -> item.distance <= value['$near']['$maxDistance']

      # Extract docs
      list = _.pluck distances, 'doc'
  return list

# Very simple polygon check. Assumes that is a square
pointInPolygon = (point, polygon) ->
  # Check that first == last
  if not _.isEqual(_.first(polygon.coordinates[0]), _.last(polygon.coordinates[0]))
    throw new Error("First must equal last")

  # Check bounds
  if point.coordinates[0] < Math.min.apply(this,
      _.map(polygon.coordinates[0], (coord) -> coord[0]))
    return false
  if point.coordinates[1] < Math.min.apply(this,
      _.map(polygon.coordinates[0], (coord) -> coord[1]))
    return false
  if point.coordinates[0] > Math.max.apply(this,
      _.map(polygon.coordinates[0], (coord) -> coord[0]))
    return false
  if point.coordinates[1] > Math.max.apply(this,
      _.map(polygon.coordinates[0], (coord) -> coord[1]))
    return false
  return true

# From http://www.movable-type.co.uk/scripts/latlong.html
getDistanceFromLatLngInM = (lat1, lng1, lat2, lng2) ->
  R = 6370986 # Radius of the earth in m
  dLat = deg2rad(lat2 - lat1) # deg2rad below
  dLng = deg2rad(lng2 - lng1)
  a = Math.sin(dLat / 2) * Math.sin(dLat / 2) + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.sin(dLng / 2) * Math.sin(dLng / 2)
  c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
  d = R * c # Distance in m
  return d

deg2rad = (deg) ->
  deg * (Math.PI / 180)

processGeoIntersectsOperator = (selector, list) ->
  for key, value of selector
    if value? and value['$geoIntersects']
      geo = value['$geoIntersects']['$geometry']
      if geo.type != 'Polygon'
        break

      # Check within for each
      list = _.filter list, (doc) ->
        # Reject non-points
        if not doc[key] or doc[key].type != 'Point'
          return false

        # Check polygon
        return pointInPolygon(doc[key], geo)

  return list

# Tidy up upsert parameters to always be a list of { doc: <doc>, base: <base> },
# doing basic error checking and making sure that _id is present
# Returns [items, success, error]
exports.regularizeUpsert = (docs, bases, success, error) ->
  # Handle case of bases not present
  if _.isFunction(bases)
    [bases, success, error] = [undefined, bases, success]

  # Handle single upsert
  if not _.isArray(docs)
    docs = [docs]
    bases = [bases]
  else
    bases = bases or []

  # Make into list of { doc: .., base: }
  items = _.map(docs, (doc, i) -> { doc: doc, base: if i < bases.length then bases[i] else undefined})

  # Set _id
  for item in items
    if not item.doc._id
      item.doc._id = exports.createUid()
    if item.base and not item.base._id
      throw new Error("Base needs _id")
    if item.base and item.base._id != item.doc._id
      throw new Error("Base needs same _id")

  return [items, success, error]
