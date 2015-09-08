_ = require 'lodash'
chai = require 'chai'
assert = chai.assert
expect = chai.expect

error = (err) ->
  console.log err
  assert.fail(JSON.stringify(err))

# Runs queries on @col which must be a collection (with a:<string>, b:<integer>, c:<json>, geo:<geojson>, stringarr: <json array of strings>)
# When present:
# c.arrstr is an array of string values
# c.arrint is an array of integer values
# @reset(done) must truncate the collection
module.exports = ->
  before ->
    # Test a filter to return specified rows (in order)
    @testFilter = (filter, ids, done) ->
      results = @col.find(filter, { sort:["_id"]})
      assert.deepEqual _.pluck(results, '_id'), ids
      done()

  context 'With sample rows', ->
    beforeEach (done) ->
      @reset =>
        @col.upsert { _id:"1", a:"Alice", b:1, c: { d: 1, e: 2 } }
        @col.upsert { _id:"2", a:"Charlie", b:2, c: { d: 2, e: 3 } }
        @col.upsert { _id:"3", a:"Bob", b:3 }
        done()

    it 'finds all rows', (done) ->
      results = @col.find({}) 
      assert.equal results.length, 3
      done()

    it 'finds all rows with options', (done) ->
      results = @col.find({}, {})
      assert.equal 3, results.length
      done()

    it 'filters by id', (done) ->
      @testFilter { _id: "1" }, ["1"], done

    it 'filters by string', (done) ->
      @testFilter { a: "Alice" }, ["1"], done

    it 'filters by $in string', (done) ->
      @testFilter { a: { $in: ["Alice", "Charlie"]} }, ["1", "2"], done

    it 'filters by number', (done) ->
      @testFilter { b: 2 }, ["2"], done

    it 'filters by $in number', (done) ->
      @testFilter { b: { $in: [2, 3]} }, ["2", "3"], done

    it 'filters by $regex', (done) ->
      @testFilter { a: { $regex: "li"} }, ["1", "2"], done

    it 'filters by $regex case-sensitive', (done) ->
      @testFilter { a: { $regex: "A"} }, ["1"], done

    it 'filters by $regex case-insensitive', (done) ->
      @testFilter { a: { $regex: "A", $options: 'i' } }, ["1", "2"], done

    it 'filters by $or', (done) ->
      @testFilter { "$or": [{b:1}, {b:2}]}, ["1","2"], done

    it 'filters by path', (done) ->
      @testFilter { "c.d": 2 }, ["2"], done

    it 'filters by $ne', (done) ->
      @testFilter { "b": { $ne: 2 }}, ["1","3"], done

    it 'filters by $gt', (done) ->
      @testFilter { "b": { $gt: 1 }}, ["2","3"], done

    it 'filters by $lt', (done) ->
      @testFilter { "b": { $lt: 3 }}, ["1","2"], done

    it 'filters by $gte', (done) ->
      @testFilter { "b": { $gte: 2 }}, ["2","3"], done

    it 'filters by $lte', (done) ->
      @testFilter { "b": { $lte: 2 }}, ["1","2"], done

    it 'filters by $not', (done) ->
      @testFilter { "b": { $not: { $lt: 3 }}}, ["3"], done

    it 'filters by $or', (done) ->
      @testFilter { $or: [{b: 3},{b: 1}]}, ["1", "3"], done

    it 'filters by $exists: true', (done) ->
      @testFilter { c: { $exists: true }}, ["1", "2"], done

    it 'filters by $exists: false', (done) ->
      @testFilter { c: { $exists: false }}, ["3"], done

    it 'includes fields', (done) ->
      results = @col.find({ _id: "1" }, {a: 1})
      assert.deepEqual results[0], { _id: "1",  a: "Alice" }
      done()

    it 'sorts by proper method asc', (done) ->
      results = @col.find().sort({a: 1})
      assert.deepEqual results[0], { _id: "1",  a: "Alice", b: 1, c: { d: 1, e: 2 } }
      done()

    it 'sorts by proper method asc with no values', (done) ->
      results = @col.find().sort({a: 1})
      assert.deepEqual results[0], { _id: "1",  a: "Alice", b: 1, c: { d: 1, e: 2 } }
      done()

    it 'sorts by proper method desc', (done) ->
      results = @col.find().sort({a: -1})
      assert.deepEqual results[0], { _id:"3", a:"Bob", b:3 } 
      done()


    it 'includes subfields', (done) ->
      results = @col.find({ _id: "1" }, { "c.d":1 })
      assert.deepEqual results[0], { _id: "1",  c: { d: 1 } }
      done()

    it 'ignores non-existent subfields', (done) ->
      results = @col.find({ _id: "1" }, { "x.y":1 })
      assert.deepEqual results[0], { _id: "1" }
      done()

    it 'excludes fields', (done) ->
      results = @col.find({ _id: "1" }, {a: 0})
      assert.isUndefined results[0].a
      assert.equal results[0].b, 1
      done()

    it 'excludes subfields', (done) ->
      results = @col.find({ _id: "1" }, {"c.d": 0})
      assert.deepEqual results[0].c, { e: 2 }
      done()

    it 'finds one row', (done) ->
      result = @col.findOne { _id: "2" }
      assert.equal 'Charlie', result.a
      done()

    it 'removes item', (done) ->
      @col.remove "2"
      results = @col.find({})
      assert.equal 2, results.length
      assert "1" in (result._id for result in results)
      assert "2" not in (result._id for result in results)
      done()

    it 'removes non-existent item', (done) ->
      @col.remove "999"
      results = @col.find({})
      assert.equal 3, results.length
      done()

    it 'sorts ascending', (done) ->
      results = @col.find({}).sort({'_id': 1})
      assert.deepEqual _.pluck(results, '_id'), ["1","2","3"]
      done()

    it 'sorts descending', (done) ->
      results = @col.find({}).sort({'_id': -1})
      assert.deepEqual _.pluck(results, '_id'), ["3","2","1"]
      done()

    it 'limits', (done) ->
      results = @col.find({}).sort({a: 1}).limit(2)
      assert.deepEqual _.pluck(results, '_id'), ["1","2"]
      done()

    it 'skips', (done) ->
      results = @col.find({}).sort({'a': 1}).skip(1)
      assert.deepEqual _.pluck(results, '_id'), ["2", "3"]
      done()

    it 'skips with chained limit', (done) ->
      results = @col.find({}).sort({'a': 1}).skip(1).limit(1)
      assert.deepEqual _.pluck(results, '_id'), ["2"]
      done()

    it 'skips with chained limit reversed', (done) ->
      results = @col.find({}).sort({'a': 1}).limit(1).skip(1)
      assert.deepEqual _.pluck(results, '_id'), ["2"]
      done()

    it 'fetches independent copies', (done) ->
      result1 = @col.findOne { _id: "2" }
      result2 = @col.findOne { _id: "2" }
      assert result1 != result2
      done()

    it 'upsert keeps independent copies', (done) ->
      doc = { _id: "2" }
      item = @col.upsert doc
      doc.a = "xyz"
      doc2 = @col.findOne { _id:"2" }
      assert doc != doc2
      assert doc2.a != "xyz"
      done()

    it 'adds _id to rows', (done) ->
      @col.upsert { a: "1" }, (item) ->
        assert.property item, '_id'
        assert.lengthOf item._id, 32
        done()

    it 'returns array if called with array', (done) ->
      @col.upsert [{ a: "1" }], (items) ->
        assert.equal items[0].a, "1"
        done()

    it 'updates by id', (done) ->
      @col.upsert { _id:"1", a:"1" }, (item) =>
        @col.upsert { _id:"1", a:"2", b: 1 }, (item) =>
          assert.equal item.a, "2"

          results = @col.find({ _id: "1" })
          assert.equal 1, results.length, "Should be only one document"
          done()

    it 'call upsert with upserted row', (done) ->
      @col.upsert { _id:"1", a:"1" }, (item) ->
        assert.equal item._id, "1"
        assert.equal item.a, "1"
        done()

  it 'upserts multiple rows', (done) ->
    @timeout(10000)
    @reset =>
      docs = []
      for i in [0...100]
        docs.push { b: i }

      @col.upsert docs, =>
        results = @col.find({})
        assert.equal results.length, 100
        done()
      , error

  context 'With sample with capitalization', ->
    beforeEach (done) ->
      @reset =>
        @col.upsert { _id:"1", a:"Alice", b:1, c: { d: 1, e: 2 } }
        @col.upsert { _id:"2", a:"AZ", b:2, c: { d: 2, e: 3 } }
        done()

    it 'finds sorts in Javascript order', (done) ->
      results = @col.find({}).sort({'a': -1})
      assert.deepEqual _.pluck(results, '_id'), ["2","1"]
      done()

  context 'With integer array in json rows', ->
    beforeEach (done) ->
      @reset =>
        @col.upsert { _id:"1", c: { arrint: [1, 2] }}
        @col.upsert { _id:"2", c: { arrint: [2, 3] }}
        @col.upsert { _id:"3", c: { arrint: [1, 3] }}
        done()

    it 'filters by $in', (done) ->
      @testFilter { "c.arrint": { $in: [3] }}, ["2", "3"], done

    it 'filters by list $in with multiple', (done) ->
      @testFilter { "c.arrint": { $in: [1, 3] }}, ["1", "2", "3"], done

  context 'With object array rows', ->
    beforeEach (done) ->
      @reset =>
        @col.upsert { _id:"1", c: [{ x: 1, y: 1 }, { x:1, y:2 }] }
        @col.upsert { _id:"2", c: [{ x: 2, y: 1 }] }
        @col.upsert { _id:"3", c: [{ x: 2, y: 2 }] }
        done()

    it 'filters by $elemMatch', (done) ->
      @testFilter { "c": { $elemMatch: { y:1 }}}, ["1", "2"], =>
        @testFilter { "c": { $elemMatch: { x:1 }}}, ["1"], done

  context 'With array rows with inner string arrays', ->
    beforeEach (done) ->
      @reset =>
        @col.upsert { _id:"1", c: [{ arrstr: ["a", "b"]}, { arrstr: ["b", "c"]}] }
        @col.upsert { _id:"2", c: [{ arrstr: ["b"]}] }
        @col.upsert { _id:"3", c: [{ arrstr: ["c", "d"]}, { arrstr: ["e", "f"]}] }
        done()

    it 'filters by $elemMatch', (done) ->
      @testFilter { "c": { $elemMatch: { "arrstr": { $in: ["b"]} }}}, ["1", "2"], =>
        @testFilter { "c": { $elemMatch: { "arrstr": { $in: ["d", "e"]} }}}, ["3"], done

  context 'With text array rows', ->
    beforeEach (done) ->
      @reset =>
        @col.upsert { _id:"1", textarr: ["a", "b"]}
        @col.upsert { _id:"2", textarr: ["b", "c"]}
        @col.upsert { _id:"3", textarr: ["c", "d"]}
        done()

    it 'filters by $in', (done) ->
      @testFilter { "textarr": { $in: ["b"] }}, ["1", "2"], done

    it 'filters by direct reference', (done) ->
      @testFilter { "textarr": "b" }, ["1", "2"], done

    it 'filters by both item and complete array', (done) ->
      @testFilter { "textarr": { $in: ["a", ["b", "c"]] } }, ["1", "2"], done

  geopoint = (lng, lat) ->
    return {
      type: 'Point'
      coordinates: [lng, lat]
    }

  context 'With geolocated rows', ->
    beforeEach (done) ->
      @col.upsert { _id:"1", geo:geopoint(90, 45) }
      @col.upsert { _id:"2", geo:geopoint(90, 46) }
      @col.upsert { _id:"3", geo:geopoint(91, 45) }
      @col.upsert { _id:"4", geo:geopoint(91, 46) }
      done()

    it 'finds points near', (done) ->
      selector = geo:
        $near:
          $geometry: geopoint(90, 45)

      results = @col.find(selector)
      assert.deepEqual _.pluck(results, '_id'), ["1","3","2","4"]
      done()

    it 'finds points near maxDistance', (done) ->
      selector = geo:
        $near:
          $geometry: geopoint(90, 45)
          $maxDistance: 111180

      results = @col.find(selector)
      assert.deepEqual _.pluck(results, '_id'), ["1","3"]
      done()

    it 'finds points near maxDistance just above', (done) ->
      selector = geo:
        $near:
          $geometry: geopoint(90, 45)
          $maxDistance: 111410

      results = @col.find(selector)
      assert.deepEqual _.pluck(results, '_id'), ["1","3","2"]
      done()

    it 'finds points within simple box', (done) ->
      selector = geo:
        $geoIntersects:
          $geometry:
            type: 'Polygon'
            coordinates: [[
              [89.5, 45.5], [89.5, 46.5], [90.5, 46.5], [90.5, 45.5], [89.5, 45.5]
            ]]
      results = @col.find(selector)
      assert.deepEqual _.pluck(results, '_id'), ["2"]
      done()

    it 'finds points within big box', (done) ->
      selector = geo:
        $geoIntersects:
          $geometry:
            type: 'Polygon'
            coordinates: [[
              [0, -89], [0, 89], [179, 89], [179, -89], [0, -89]
            ]]
      results = @col.find(selector).sort({'_id': 1})
      assert.deepEqual _.pluck(results, '_id'), ["1", "2", "3", "4"]
      done()

    it 'handles undefined', (done) ->
      selector = geo:
        $geoIntersects:
          $geometry:
            type: 'Polygon'
            coordinates: [[
              [89.5, 45.5], [89.5, 46.5], [90.5, 46.5], [90.5, 45.5], [89.5, 45.5]
            ]]
      @col.upsert { _id:5 }, =>
        results = @col.find(selector)
        assert.deepEqual _.pluck(results, '_id'), ["2"]
        done()

  context 'With insert and update', ->
    beforeEach (done) ->
      @col.items = {}
      done()

    afterEach (done) ->
      @col.items = {}
      done()
    #insert

    it 'inserts new row', (done) ->
      item = @col.insert { a: "xxx" }
      results = @col.find({a: 'xxx'})
      assert results[0].a == 'xxx'
      done()

    it 'throws duplicate id error', (done) ->
      item = @col.insert { _id: '123', a: "xxx" }
      expect(() =>
        @col.insert { _id: '123', a: "hello" }).to.throw 'Duplicate ID'
      done()


    #update
  
    it 'updates correct record', (done) ->
      item = @col.insert { a: "xxx", b: 'test' }
      results = @col.find({b: 'test'})
      assert results[0].a == 'xxx'
      item = @col.update {b: 'test'}, { a: "123", b: 'test' }
      results = @col.find({b: 'test'})
      assert.equal results[0].a, '123'
      done()

    it 'doesnt add record on update', (done) ->
      item = @col.insert { a: "xxx", b: 'test' }
      results = @col.find({b: 'test'})
      assert results.length == 1
      item = @col.update {b: 'test'}, { a: "123", b: 'test' }
      assert results.length == 1
      done()


    #aggregates
    
    it 'uses $match to limit results', (done) ->
      item = @col.insert { name: 'jack', status: 'awesome', age: 20 }
      item = @col.insert { name: 'bob', status: 'ok', age: 2 }
      results = @col.aggregate([{$match: {age: {$gte: 10}}}])
      assert.equal results[0].name, 'jack'
      assert.equal results[1], undefined
      done()

    it 'uses $match to return all results', (done) ->
      item = @col.insert { name: 'jack', status: 'awesome', age: 20 }
      item = @col.insert { name: 'bob', status: 'ok', age: 2 }
      results = @col.aggregate([{$match: {age: {$gte: 1}}}])
      assert.equal results[0].name, 'jack'
      assert.equal results[1].name, 'bob'
      done()

    it 'uses $group to group together by criteria', (done) ->
      item = @col.insert { name: 'jack', status: 'awesome', age: 20 }
      item = @col.insert { name: 'bob', status: 'ok', age: 2 }
      results = @col.aggregate([{$group: {_id: '$age'}}])
      assert.equal results[0]['_id'], 20
      assert.equal results[1]['_id'], 2
      done()

#     it 'uses $group with $sum accumulator', (done) ->
#       item = @col.insert { name: 'jack', status: 'awesome', age: 20 }
#       item = @col.insert { name: 'bob', status: 'ok', age: 2 }
#       results = @col.aggregate([{$group: {_id: '$_id', total: '$sum': '$age'}}])
#       assert.equal results[0]['_id'], 20
#       assert.equal results[1]['_id'], 2
#       done()
#
