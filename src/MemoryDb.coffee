_ = require 'lodash'
utils = require('./utils')
processFind = require('./utils').processFind
processAggregate = require('./utils').processAggregate
compileSort = require('./selector').compileSort

module.exports = class MemoryDb
  constructor: (options, success) ->
    @collections = {}

    if success then success(this)

  addCollection: (name, success, error) ->
    collection = new Collection(name)
    @[name] = collection
    @collections[name] = collection
    if success? then success()

  createCollection: (name, success, error) ->
    collection = new Collection(name)
    @[name] = collection
    @collections[name] = collection
    if success? then success()

  removeCollection: (name, success, error) ->
    delete @[name]
    delete @collections[name]
    if success? then success()

# Stores data in memory
class Collection
  constructor: (name) ->
    @name = name

    @items = {}
    @upserts = {}  # Pending upserts by _id. Still in items
    @removes = {}  # Pending removes by _id. No longer in items

  find: (selector, options) ->
    return @_findFetch(selector, options)

  findOne: (selector, options, success, error) ->
    if _.isFunction(options)
      [options, success, error] = [{}, options, success]

    results = @find(selector, options)
    if !!results then return results[0] else return null

  _findFetch: (selector, options, success, error) ->
    return processFind(@items, selector, options)

  upsert: (docs, bases, success, error) ->
    [items, success, error] = utils.regularizeUpsert(docs, bases, success, error)

    for item in items
      # Fill in base if undefined
      if item.base == undefined
        # Use existing base
        if @upserts[item.doc._id]
          item.base = @upserts[item.doc._id].base
        else
          item.base = @items[item.doc._id] or null

      # Keep independent copies
      item = _.cloneDeep(item)

      # Replace/add
      @items[item.doc._id] = item.doc
      @upserts[item.doc._id] = item

    # dont remove right now!
    if success then success(docs)

  insert: (docs, bases, success, error) ->
    [items, success, error] = utils.regularizeUpsert(docs, bases, success, error)

    for item in items
      # Fill in base if undefined
      if item.base == undefined
        item.base = @items[item.doc._id] or null

      # Keep independent copies
      item = _.cloneDeep(item)
      if !@items[item.doc._id]
        @items[item.doc._id] = item.doc
      else
        throw 'Duplicate ID'

  update: (selector, docs, bases, success, error) ->
    theItems = processFind(@items, selector)
    if bases && bases['upsert'] && theItems.length < 1
      this.insert(docs)
      @upserts[docs._id] = docs

    for item in theItems
      if item.docs == undefined
        item.doc = docs
      if item.base == undefined
        item.base = @items[item.doc._id] or null
      item = _.cloneDeep(item)
      if _.include(Object.keys(docs), "$set")
        for k,v of docs['$set']
          @items[item._id][k] = v
      else
        for k,v of docs
          id = @items[item._id]._id
          @items[item._id] = docs
        @items[item._id]._id = id

  aggregate: (selectors...) ->
    remaining = []
    if selectors.length > 1
      for select in selectors
        remaining.push select
      selectors = remaining
    else
      selectors = selectors[0]
    return processAggregate(@items, selectors)
    # for selector in selectors
    #   if remaining.length < 1
    #   then remaining = processAggregate(@items, selector)
    #   else remaining.push(processAggregate(@items, selector))
    # return remaining


  remove: (id, success, error) ->
    if _.has(@items, id)
      @removes[id] = @items[id]
      delete @items[id]
      delete @upserts[id]
    else
      @removes[id] = { _id: id }

    if success? then success()

  cache: (docs, selector, options, success, error) ->
    # Add all non-local that are not upserted or removed
    for doc in docs
      @cacheOne(doc)

    docsMap = _.object(_.pluck(docs, "_id"), docs)

    if options.sort
      sort = compileSort(options.sort)

    # Perform query, removing rows missing in docs from local db
    @find(selector, options).fetch (results) =>
      for result in results
        if not docsMap[result._id] and not _.has(@upserts, result._id)
          # If past end on sorted limited, ignore
          if options.sort and options.limit and docs.length == options.limit
            if sort(result, _.last(docs)) >= 0
              continue
          delete @items[result._id]

      if success? then success()
    , error

  pendingUpserts: (success) ->
    success _.values(@upserts)

  pendingRemoves: (success) ->
    success _.pluck(@removes, "_id")

  resolveUpserts: (upserts, success) ->
    for upsert in upserts
      id = upsert.doc._id
      if @upserts[id]
        # Only safely remove upsert if doc is unchanged
        if _.isEqual(upsert.doc, @upserts[id].doc)
          delete @upserts[id]
        else
          # Just update base
          @upserts[id].base = upsert.doc

    if success? then success()

  resolveRemove: (id, success) ->
    delete @removes[id]
    if success? then success()

  # Add but do not overwrite or record as upsert
  seed: (docs, success) ->
    if not _.isArray(docs)
      docs = [docs]

    for doc in docs
      if not _.has(@items, doc._id) and not _.has(@removes, doc._id)
        @items[doc._id] = doc
    if success? then success()

  # Add but do not overwrite upserts or removes
  cacheOne: (doc, success) ->
    if not _.has(@upserts, doc._id) and not _.has(@removes, doc._id)
      existing = @items[doc._id]

      # If _rev present, make sure that not overwritten by lower _rev
      if not existing or not doc._rev or not existing._rev or doc._rev >= existing._rev
        @items[doc._id] = doc
    if success? then success()
