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
        @col.insert { _id:"1", a:"Alice", b:1, c: { d: 1, e: 2 }, lengths: [1,3,2] }
        @col.insert { _id:"2", a:"Charlie", b:2, c: { d: 2, e: 3 } , lengths: [2,3,3]}
        @col.insert { _id:"3", a:"Bob", b:3 , lengths: [5,3,4]}
        done()

    it 'doesnt return results when array passed into find', (done) ->
      results = @col.find([{_id: 1}])
      assert.equal results.length, 0
      done()

    it 'doesnt return results when using elemMatch without any operators', (done) ->
      results = @col.find({"lengths": {$elemMatch: {}}})
      assert.equal results.length, 0
      done()

    it 'find handles bad query not using dot notation to get to doc with operator', (done) ->
      results = @col.find({"c": {$gt: 1}})
      assert.equal results.length, 0
      done()

    it 'finds 2 with $elemMatch', (done) ->
      results = @col.find({lengths: {$elemMatch: {$gt: 1, $lt: 3}}})
      assert.equal results.length, 2
      done()

    it 'finds 3 with $elemMatch', (done) ->
      results = @col.find({lengths: {$elemMatch: {$gt: 1, $lt: 4}}})
      assert.equal results.length, 3
      done()

    it 'projects multiple fields', (done) ->
      results = @col.find({}, {"_id": false, a: true})
      assert.equal results[0].a, 'Alice'
      assert.equal results[0]._id, undefined
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

    it 'it throws when sort value not -1 or 1', (done) ->
      expect(() =>
        results = @col.find().sort({a: 2})
      ).to.throw 'BadValue bad sort specification'
      done()

    it 'sorts with {}', (done) ->
      results = @col.find().sort({})
      assert results.length > 0
      done()


    it 'sorts by proper method asc', (done) ->
      results = @col.find().sort({a: 1})
      assert.equal results[0]._id, '1'
      assert.equal results[1]._id, '3'
      assert.equal results[2]._id, '2'
      # assert.deepEqual results[0], { _id:"1", a:"Alice", b:1, c: { d: 1, e: 2 }, lengths: [1,3,2] }
      done()

    it 'sorts by proper method asc with no values', (done) ->
      results = @col.find().sort({a: 1})
      assert.deepEqual results[0], { _id:"1", a:"Alice", b:1, c: { d: 1, e: 2 }, lengths: [1,3,2] }
      done()

    it 'sorts by proper method desc', (done) ->
      results = @col.find().sort({a: -1})
      assert.equal results[0]._id, '2'
      assert.equal results[1]._id, '3'
      assert.equal results[2]._id, '1'
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
      @col.remove _id: "2"
      results = @col.find({})
      assert.equal 2, results.length
      assert "1" in (result._id for result in results)
      assert "2" not in (result._id for result in results)
      done()

    it 'removes non-existent item', (done) ->
      @col.remove _id: "999"
      results = @col.find({})
      assert.equal 3, results.length
      done()

    it 'counts records in pipeline', (done) ->
      count =  @col.find({}).count()
      assert.equal count, 3
      done()

    it 'sorts ascending', (done) ->
      results = @col.find({}).sort({'_id': 1})
      assert.deepEqual _.pluck(results, '_id'), ["1","2","3"]
      done()

    it 'sorts descending', (done) ->
      results = @col.find({}).sort({'_id': -1})
      assert.deepEqual _.pluck(results, '_id'), ["3","2","1"]
      done()

    it 'limits by self', (done) ->
      results = @col.find({}).limit(2)
      assert.deepEqual _.pluck(results, '_id'), ["1", "2"]
      done()

    it 'limits can return full set', (done) ->
      results = @col.find({}).limit(50)
      assert.deepEqual _.pluck(results, '_id'), ["1", "2", "3"]
      done()

    it 'limits with sort', (done) ->
      results = @col.find({}).sort({a: 1}).limit(2)
      assert.deepEqual _.pluck(results, '_id'), ["1","3"]
      done()

    it 'limits with sort to 3 records', (done) ->
      results = @col.find({}).sort({a: 1}).limit(3)
      assert.deepEqual _.pluck(results, '_id'), ["1","3","2"]
      done()

    it 'skips', (done) ->
      results = @col.find({}).sort({'a': 1}).skip(1)
      assert.deepEqual _.pluck(results, '_id'), ["3", "2"]
      done()

    it 'skips with chained limit', (done) ->
      results = @col.find({}).sort({'a': 1}).skip(1).limit(1)
      assert.deepEqual _.pluck(results, '_id'), ["3"]
      done()

    it 'skips with chained limit reversed', (done) ->
      results = @col.find({}).sort({'a': 1}).limit(3).skip(2)
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
      assert.deepEqual _.pluck(results, '_id'), ["1","2"]
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
      @reset =>
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

    it 'throws if no first arg insert', (done) ->
      expect(() =>
        @col.insert()).to.throw 'Error: no object passed to insert'
      done()

    it 'does not throw if first arg empty object', (done) ->
      expect(() =>
        @col.insert({})).to.not.throw 'Error: no object passed to insert'
      done()


    #update
    it 'throws if update with $set field using dollar', (done) ->
      item = @col.insert { name: "xxx", price: 123 }
      expect(() =>
        @col.update({name: 'xxx'}, {$set: {$price: 0}})).to.throw 'The dollar ($) prefixed field $price in $price is not valid for storage.'
      done()

    it 'throws if update with $max field using dollar', (done) ->
      item = @col.insert { name: "xxx", price: 123 }
      expect(() =>
        @col.update({name: 'xxx'}, {$max: {$price: 0}})).to.throw 'The dollar ($) prefixed field $price in $price is not valid for storage.'
      done()

    it 'throws if update with $min field using dollar', (done) ->
      item = @col.insert { name: "xxx", price: 123 }
      expect(() =>
        @col.update({name: 'xxx'}, {$min: {$price: 0}})).to.throw 'The dollar ($) prefixed field $price in $price is not valid for storage.'
      done()

    it 'throws if update with $mul field using dollar', (done) ->
      item = @col.insert { name: "xxx", price: 123 }
      expect(() =>
        @col.update({name: 'xxx'}, {$mul: {$price: 0}})).to.throw 'The dollar ($) prefixed field $price in $price is not valid for storage.'
      done()

    it 'throws if no args update', (done) ->
      expect(() =>
        @col.update()).to.throw 'Error: no object passed to update'
      done()

    it 'does not throw if args both args are empty objects', (done) ->
      expect(() =>
        @col.update({}, {})).to.not.throw 'Error: no object passed to update'
      done()

    it 'does not count an update if empty passed in', (done) ->
      @col.update({}, {})
      assert.equal(Object.keys(@col.updates).length, 0)
      done()

    it 'updates first record when selector is {}', (done) ->
      item = @col.insert { name: 'dan', car: ['honda', 'ford'], age: 12}
      item = @col.insert { name: 'dan', car: ['honda', 'ford'], age: 12}
      item = @col.insert { name: 'dan', car: ['honda', 'ford'], age: 12}
      item = @col.update {}, {$inc: {age: 2}}
      results = @col.find()
      assert.equal results[0].age, 14
      assert.equal results[1].age, 12
      assert.equal results[2].age, 12
      done()
  
    it 'updates inserts with $inc and upsert', (done) ->
      item = @col.insert { name: 'dan', car: ['honda', 'ford'], age: 12}
      item = @col.update {name: 'danxx'}, { $inc: {age: 2}}, {upsert: true}
      results = @col.find({name: 'danxx'})
      assert.equal results[0].age, 2
      done()
  
    it 'update with chained pipeline', (done) ->
      item = @col.insert { name: 'dan', car: ['honda', 'ford'], age: 12}
      item = @col.update {name: 'dan'}, { $set: {"car.0": 'ford', "car.1": 'fiat'}, $inc: {age: 2}}
      results = @col.find({name: 'dan'})
      assert.equal results[0].car[0], 'ford'
      assert.equal results[0].car[1], 'fiat'
      assert.equal results[0].age, 14
      done()
  
    it 'update uses $set with array second index to update array', (done) ->
      item = @col.insert { name: 'dan', car: ['honda', 'ford']}
      item = @col.update {name: 'dan'}, { $set: {"car.0": 'ford', "car.1": 'fiat'}}
      results = @col.find({name: 'dan'})
      assert.equal results[0].car[0], 'ford'
      assert.equal results[0].car[1], 'fiat'
      done()
  
    it 'update uses $set with array third index to update array', (done) ->
      item = @col.insert { name: 'dan', car: ['honda', 'ford', 'hyundai']}
      item = @col.update {name: 'dan'}, { $set: {"car.2": 'ferrari'}}
      results = @col.find({name: 'dan'})
      assert.equal results[0].car[0], 'honda'
      assert.equal results[0].car[1], 'ford'
      assert.equal results[0].car[2], 'ferrari'
      done()
  
    it 'update uses $set with array index to update array', (done) ->
      item = @col.insert { name: 'dan', car: ['honda', 'ford']}
      item = @col.update {name: 'dan'}, { $set: {"car.0": 'ford'}}
      results = @col.find({name: 'dan'})
      assert.equal results[0].car[0], 'ford'
      assert.equal results[0].car[1], 'ford'
      done()
  
    it 'update uses $push to push onto array', (done) ->
      item = @col.insert { name: 'dan', car: ['honda', 'ford']}
      item = @col.update {name: 'dan'}, { $push: {car: 'ford'}}
      results = @col.find({name: 'dan'})
      assert.equal results[0].car[0], 'honda'
      assert.equal results[0].car[1], 'ford'
      assert.equal results[0].car[2], 'ford'
      done()
  
    it 'update doesnt add to array with $addToSet if in array', (done) ->
      item = @col.insert { name: 'dan', car: ['honda', 'ford']}
      item = @col.update {name: 'dan'}, { $addToSet: {car: 'ford'}}
      results = @col.find({name: 'dan'})
      assert.equal results[0].car[0], 'honda'
      assert.equal results[0].car[1], 'ford'
      assert.equal results[0].car[2], undefined
      done()
  
    it 'update adds to array with $addToSet', (done) ->
      item = @col.insert { name: 'dan', car: ['honda', 'ford']}
      item = @col.update {name: 'dan'}, { $addToSet: {car: 'toyota'}}
      results = @col.find({name: 'dan'})
      assert.equal results[0].car[0], 'honda'
      assert.equal results[0].car[1], 'ford'
      assert.equal results[0].car[2], 'toyota'
      done()
  
    it 'uses positional operator with array', (done) ->
      item = @col.insert { name: 'dan', car: ['honda', 'ford']}
      item = @col.update {'car': 'honda'}, { $set: {"car.$": 'ferrari'}}
      results = @col.find({name: 'dan'})
      assert.equal results[0].car[0], 'ferrari'
      done()
  
    it 'updates nested documents with $max dot notation', (done) ->
      item = @col.insert { name: 'dan', car: {age: 12}}
      item = @col.update {name: 'dan'}, { $max: {"car.age": 22}}
      results = @col.find({name: 'dan'})
      assert.equal results[0].car.age, 22
      done()
  
    it 'updates nested documents with $min dot notation', (done) ->
      item = @col.insert { name: 'dan', car: {age: 12}}
      item = @col.update {name: 'dan'}, { $min: {"car.age": 2}}
      results = @col.find({name: 'dan'})
      assert.equal results[0].car.age, 2
      done()
  
    it 'updates nested documents with $mul dot notation', (done) ->
      item = @col.insert { name: 'dan', car: {age: 12}}
      item = @col.update {name: 'dan'}, { $mul: {"car.age": 2}}
      results = @col.find({name: 'dan'})
      assert.equal results[0].car.age, 24
      done()
  
    it 'upserts with $set using the set parameters as document', (done) ->
      item = @col.insert { name: 'dan', age: 12, car: 'honda' }
      item = @col.update {name: 'dan'}, { $set: {name: 'dan', age: 2}}, {upsert: true}
      results = @col.find({name: 'dan'})
      assert.equal results[0].age, 2
      done()
  
    it 'multiples specified field', (done) ->
      item = @col.insert { name: 'dan', age: 12, car: 'honda' }
      item = @col.update {name: 'dan'}, { $mul: {age: 2}}
      results = @col.find({name: 'dan'})
      assert.equal results[0].age, 24
      done()

    it 'updates only if above max', (done) ->
      item = @col.insert { name: 'dan', age: 12, car: 'honda' }
      item = @col.update {name: 'dan'}, { $max: {age: 15}}
      results = @col.find({name: 'dan'})
      assert.equal results[0].age, 15
      done()

    it 'doesnt update if less than max', (done) ->
      item = @col.insert { name: 'dan', age: 12, car: 'honda' }
      item = @col.update {name: 'dan'}, { $max: {age: 10}}
      results = @col.find({name: 'dan'})
      assert.equal results[0].age, 12
      done()

    it 'updates only if below min', (done) ->
      item = @col.insert { name: 'dan', age: 12, car: 'honda' }
      item = @col.update {name: 'dan'}, { $min: {age: 5}}
      results = @col.find({name: 'dan'})
      assert.equal results[0].age, 5
      done()

    it 'doesnt update if greater than min', (done) ->
      item = @col.insert { name: 'dan', age: 12, car: 'honda' }
      item = @col.update {name: 'dan'}, { $min: {age: 55}}
      results = @col.find({name: 'dan'})
      assert.equal results[0].age, 12
      done()

    it 'unsets a field', (done) ->
      item = @col.insert { name: 'dan', age: 12, car: 'honda' }
      item = @col.update {name: 'dan'}, { $unset: {car: ''}}
      results = @col.find({name: 'dan'})
      assert.equal results[0].car, undefined
      done()

    it 'renames all fields', (done) ->
      item = @col.insert { name: 'dan', age: 12, car: 'honda' }
      item = @col.update {name: 'dan'}, { $rename: {car: 'make', age: 'oldness'}}
      results = @col.find({name: 'dan'})
      assert.equal results[0].make, 'honda'
      assert.equal results[0].oldness, 12
      assert.equal results[0].car, undefined
      assert.equal results[0].age, undefined
      done()

    it 'uses $inc to increment age', (done) ->
      item = @col.insert { name: 'dan', age: 12, car: 'honda' }
      item = @col.update {name: 'dan'}, { $set: {car: 'ford'}, $inc: {age: 1}}
      results = @col.find({name: 'dan'})
      assert.equal results[0].age, 13
      done()

    it 'only updates first record without multi true', (done) ->
      item = @col.insert { name: 'dan', age: 12, car: 'honda' }
      item = @col.insert { name: 'dan', age: 22, car: 'honda' }
      item = @col.update {name: 'dan'}, { $set: {car: 'ford'}, $inc: {age: 1}}
      results = @col.find()
      assert.equal results[0].age, 13
      assert.equal results[1].age, 22
      done()

    it 'updates all records with multi true', (done) ->
      item = @col.insert { name: 'dan', age: 12, car: 'honda' }
      item = @col.insert { name: 'dan', age: 22, car: 'honda' }
      item = @col.update {name: 'dan'}, { $set: {car: 'ford'}, $inc: {age: 1}}, {multi: true}
      results = @col.find()
      assert.equal results[0].age, 13
      assert.equal results[1].age, 23
      done()

    it 'updates correct record', (done) ->
      item = @col.insert { a: "xxx", b: 'test' }
      results = @col.find({b: 'test'})
      assert results[0].a == 'xxx'
      item = @col.update {b: 'test'}, { a: "123", b: 'test' }
      results = @col.find({b: 'test'})
      assert.equal results[0].a, '123'
      done()

    it 'updates using set', (done) ->
      item = @col.insert { a: "xxx", b: 'test' }
      results = @col.find({b: 'test'})
      assert results[0].a == 'xxx'
      item = @col.update {b: 'test'}, { $set: {a: "123" }}
      results = @col.find({b: 'test'})
      assert.equal results[0].a, '123'
      done()

    it 'replaces document when doc is second arg', (done) ->
      item = @col.insert { a: "xxx", b: 'test' }
      results = @col.find({b: 'test'})
      assert.equal results[0].a, 'xxx'
      item = @col.update {b: 'test'}, { a: "123" }
      results = @col.find({a: "123"})
      assert.equal results[0].a, 123
      assert.equal results[0].b, undefined
      assert results.length == 1
      done()

    it 'inserts a new doc with upsert specified and not matching doc', (done) ->
      item = @col.insert { a: "xxx", b: 'test' }
      results = @col.find({b: 'test'})
      assert.equal results[0].a, 'xxx'
      item = @col.update {b: 'notfound'}, { a: "123", b: 'testing' }, {upsert: true}
      results = @col.find()
      assert.equal results[1].a, "123"
      assert.equal results[1].b, "testing"
      assert results.length == 2
      done()

    it 'updates doc with upsert when found', (done) ->
      item = @col.insert { a: "xxx", b: 'test' }
      results = @col.find({b: 'test'})
      assert.equal results[0].a, 'xxx'
      item = @col.update {b: 'test'}, { a: "123", b: 'testing' }, {upsert: true}
      results = @col.find()
      assert.equal results[0].a, "123"
      assert.equal results[0].b, "testing"
      assert results.length == 1
      done()

    it 'inserts on update with complete document', (done) ->
      item = @col.insert { a: "xxx", b: 'test' }
      results = @col.find({b: 'test'})
      assert.equal results[0].a, 'xxx'
      item = @col.update {c: 'test'}, { a: "123", b: 'testing' }, {upsert: true}
      results = @col.find({c: 'test'})
      assert.equal results[0].a, "123"
      assert.equal results[0].b, "testing"
      assert.equal results[0].c, "test"
      assert results.length == 1
      done()

  context 'With aggregates', ->
    #aggregates
    beforeEach (done) ->
      @reset =>
        done()
    
    it 'returns _id: <item> when not objected passed into $group', (done) ->
      item = @col.insert { name: 'jack', status: 'awesome', age: 20, car: {'make': 12} }
      item = @col.insert { name: 'jack', status: 'awesome', age: 2, car: {'make': 12} }
      item = @col.insert { name: 'bob', status: 'ok', age: 2 , car: {'make': 22}}
      item = @col.insert { name: 'sam', status: 'eh', age: 12 , car: {'make': 4}}
      result = @col.aggregate({$group: {_id: 1}})
      assert.equal result[0]._id, 1
      done()

    it 'throws without $ pipeline operator', (done) ->
      item = @col.insert { name: 'jack', status: 'awesome', age: 20, car: {'make': 12} }
      item = @col.insert { name: 'jack', status: 'awesome', age: 2, car: {'make': 12} }
      item = @col.insert { name: 'bob', status: 'ok', age: 2 , car: {'make': 22}}
      item = @col.insert { name: 'sam', status: 'eh', age: 12 , car: {'make': 4}}
      expect(() =>
        @col.aggregate({group: {_id: '$name', max: {$sum: 1}}})
      ).to.throw('The pipeline operator group requires a $ in front.')
      done()

    it 'throws without valid pipeline operator', (done) ->
      item = @col.insert { name: 'jack', status: 'awesome', age: 20, car: {'make': 12} }
      item = @col.insert { name: 'jack', status: 'awesome', age: 2, car: {'make': 12} }
      item = @col.insert { name: 'bob', status: 'ok', age: 2 , car: {'make': 22}}
      item = @col.insert { name: 'sam', status: 'eh', age: 12 , car: {'make': 4}}
      expect(() =>
        @col.aggregate({$xxx: {_id: '$name', max: {$sum: 1}}})
      ).to.throw('This pipeline operator is not supported with this browser version of MongoDB')
      done()

    it 'throws with just a string', (done) ->
      expect(() =>
        @col.insert("test")
      ).to.throw('Error: no object passed to insert')
      done()

    it 'aggregate $sum to variable ', (done) ->
      item = @col.insert { name: 'jack', status: 'awesome', age: 20, car: {'make': 12} }
      item = @col.insert { name: 'jack', status: 'awesome', age: 2, car: {'make': 12} }
      item = @col.insert { name: 'bob', status: 'ok', age: 2 , car: {'make': 22}}
      item = @col.insert { name: 'sam', status: 'eh', age: 12 , car: {'make': 4}}
      results = @col.aggregate({$group: {_id: '$name', max: {$sum: 1}}})
      assert.equal results[0]._id, 'jack'
      assert.equal results[0].max, 2
      done()

    it 'can group _id of nested doc and get $sum', (done) ->
      item = @col.insert { name: 'jack', status: 'awesome', age: 20, car: {'make': 12} }
      item = @col.insert { name: 'jack', status: 'awesome', age: 2, car: {'make': 12} }
      item = @col.insert { name: 'bob', status: 'ok', age: 2 , car: {'make': 22}}
      item = @col.insert { name: 'sam', status: 'eh', age: 12 , car: {'make': 4}}
      results = @col.aggregate({$group: {_id: '$car.make', max: {$sum: 1}}})
      assert.equal results[0]._id, 12
      assert.equal results[0].max, 2
      done()

    it 'can group _id of nested doc and get $max', (done) ->
      item = @col.insert { name: 'jack', status: 'awesome', age: 20, car: {'make': 12} }
      item = @col.insert { name: 'jack', status: 'awesome', age: 2, car: {'make': 12} }
      item = @col.insert { name: 'bob', status: 'ok', age: 2 , car: {'make': 22}}
      item = @col.insert { name: 'sam', status: 'eh', age: 12 , car: {'make': 4}}
      results = @col.aggregate({$group: {_id: '$car.make', max: {$max: 1}}})
      assert.equal results[0]._id, 12
      assert.equal results[0].max, 1
      done()

    it 'can group _id of nested doc and get $min', (done) ->
      item = @col.insert { name: 'jack', status: 'awesome', age: 20, car: {'make': 12} }
      item = @col.insert { name: 'jack', status: 'awesome', age: 2, car: {'make': 12} }
      item = @col.insert { name: 'bob', status: 'ok', age: 2 , car: {'make': 22}}
      item = @col.insert { name: 'sam', status: 'eh', age: 12 , car: {'make': 4}}
      results = @col.aggregate({$group: {_id: '$car.make', max: {$min: 1}}})
      assert.equal results[0]._id, 12
      assert.equal results[0].max, 1
      done()

    it 'can group _id of nested doc and get $avg', (done) ->
      item = @col.insert { name: 'jack', status: 'awesome', age: 20, car: {'make': 12} }
      item = @col.insert { name: 'jack', status: 'awesome', age: 2, car: {'make': 12} }
      item = @col.insert { name: 'bob', status: 'ok', age: 2 , car: {'make': 22}}
      item = @col.insert { name: 'sam', status: 'eh', age: 12 , car: {'make': 4}}
      results = @col.aggregate({$group: {_id: '$car.make', max: {$avg: 1}}})
      assert.equal results[0]._id, 12
      assert.equal results[0].max, 1
      done()

    it 'can group _id of nested doc', (done) ->
      item = @col.insert { name: 'jack', status: 'awesome', age: 20, car: {'make': 'honda'} }
      item = @col.insert { name: 'jack', status: 'awesome', age: 2, car: {'make': 'ford'} }
      item = @col.insert { name: 'bob', status: 'ok', age: 2 , car: {'make': 'toyota'}}
      item = @col.insert { name: 'sam', status: 'eh', age: 12 , car: {'make': 'nissan'}}
      results = @col.aggregate({$group: {_id: '$car.make'}})
      assert.equal results[0]._id, 'honda'
      done()

    it 'aggregates without array and single object', (done) ->
      item = @col.insert { name: 'jack', status: 'awesome', age: 20, car: {'make': 1} }
      item = @col.insert { name: 'jack', status: 'awesome', age: 2, car: {'make': 11} }
      item = @col.insert { name: 'bob', status: 'ok', age: 2 , car: {'make': 12}}
      item = @col.insert { name: 'sam', status: 'eh', age: 12 , car: {'make': 22}}
      results = @col.aggregate({$group: {_id: '$name'}})
      assert.equal results[0]._id, 'jack'
      assert.equal results[0].status, undefined
      assert.equal results[0].age, undefined
      assert.equal results[0].car, undefined
      assert.equal results[1]._id, 'bob'
      assert.equal results[2]._id, 'sam'
      done()

    it 'throws error if $group without _id', (done) ->
      item = @col.insert { name: 'jack', status: 'awesome', age: 20, car: {'make': 1} }
      item = @col.insert { name: 'jack', status: 'awesome', age: 2, car: {'make': 11} }
      item = @col.insert { name: 'bob', status: 'ok', age: 2 , car: {'make': 12}}
      item = @col.insert { name: 'sam', status: 'eh', age: 12 , car: {'make': 22}}
      expect(() =>
        @col.aggregate([{$group: {name: '$age'}}])
      ).to.throw '_id field was not supplied'
      done()

    it 'sets $ defined to null if not found from $project', (done) ->
      item = @col.insert { name: 'jack', status: 'awesome', age: 20, car: {'make': 1} }
      item = @col.insert { name: 'jack', status: 'awesome', age: 2, car: {'make': 11} }
      item = @col.insert { name: 'bob', status: 'ok', age: 2 , car: {'make': 12}}
      item = @col.insert { name: 'sam', status: 'eh', age: 12 , car: {'make': 22}}
      results = @col.aggregate([{$match: {age: {$gte: 10}}}, {$project: {"name": true}}, {$group: {_id: 'name', total: {$max: '$car.make'}}}])
      assert.equal results[0]._id, 'name'
      assert.equal results[0].total, 0
      done()

    it 'handles aggregation with project only value false', (done) ->
      item = @col.insert { name: 'jack', status: 'awesome', age: 20, car: {'make': 1} }
      item = @col.insert { name: 'jack', status: 'awesome', age: 2, car: {'make': 11} }
      item = @col.insert { name: 'bob', status: 'ok', age: 2 , car: {'make': 12}}
      item = @col.insert { name: 'sam', status: 'eh', age: 12 , car: {'make': 22}}
      results = @col.aggregate([{$match: {age: {$gte: 10}}}, {$project: {"name": false}}, {$group: {_id: 'name', total: {$max: '$car.make'}}}])
      assert.equal results[0]._id, 'name'
      assert.equal results[0].total, 0
      done()

    it 'runs order correctly with $limit then $match', (done) ->
      item = @col.insert { name: 'jack', status: 'awesome', age: 20, car: {'make': 1} }
      item = @col.insert { name: 'jack', status: 'awesome', age: 2, car: {'make': 11} }
      item = @col.insert { name: 'bob', status: 'ok', age: 2 , car: {'make': 12}}
      item = @col.insert { name: 'sam', status: 'eh', age: 12 , car: {'make': 22}}
      results = @col.aggregate([{$limit: 1}, $match: {age: {$gte: 10}}])
      assert.equal results[0].name, 'jack'
      assert.equal results[1], undefined
      done()

    it 'returns nothing if limit greater than what is available', (done) ->
      item = @col.insert { name: 'jack', status: 'awesome', age: 2, car: {'make': 1} }
      item = @col.insert { name: 'jack', status: 'awesome', age: 20, car: {'make': 11} }
      item = @col.insert { name: 'bob', status: 'ok', age: 2 , car: {'make': 12}}
      item = @col.insert { name: 'sam', status: 'eh', age: 12 , car: {'make': 22}}
      results = @col.aggregate([{$limit: 1}, $match: {age: {$gte: 10}}])
      assert.equal results[0], undefined
      done()

    it 'runs $match, $project, $group, $sort then limit with $max', (done) ->
      item = @col.insert { name: 'jack', status: 'awesome', age: 20, car: {'make': 1} }
      item = @col.insert { name: 'jack', status: 'awesome', age: 20, car: {'make': 11} }
      item = @col.insert { name: 'bob', status: 'ok', age: 2 , car: {'make': 12}}
      item = @col.insert { name: 'sam', status: 'eh', age: 12 , car: {'make': 22}}
      results = @col.aggregate([{$match: {age: {$gte: 10}}}, {$project: {"name": true, "car.make": true}}, {$group: {_id: '$name', total: {$max: '$car.make'}}}])
      assert.equal results[0]._id, 'jack'
      assert.equal results[0].total, 11
      assert.equal results[1]._id, 'sam'
      assert.equal results[1].total, 22
      done()

    it 'groups by field name without $ and $max', (done) ->
      item = @col.insert { name: 'jack', status: 'awesome', age: 20, car: {'make': 1} }
      item = @col.insert { name: 'jack', status: 'awesome', age: 20, car: {'make': 11} }
      item = @col.insert { name: 'bob', status: 'ok', age: 2 , car: {'make': 12}}
      item = @col.insert { name: 'sam', status: 'eh', age: 12 , car: {'make': 22}}
      results = @col.aggregate([{$match: {age: {$gte: 10}}}, {$project: {"name": true, "car.make": true}}, {$group: {_id: 'name', total: {$max: '$car.make'}}}])
      assert.equal results[0]._id, 'name'
      assert.equal results[0].total, 22
      done()

    it 'groups by field name without $ and $min', (done) ->
      item = @col.insert { name: 'jack', status: 'awesome', age: 20, car: {'make': 1} }
      item = @col.insert { name: 'jack', status: 'awesome', age: 20, car: {'make': 11} }
      item = @col.insert { name: 'bob', status: 'ok', age: 2 , car: {'make': 12}}
      item = @col.insert { name: 'sam', status: 'eh', age: 12 , car: {'make': 22}}
      results = @col.aggregate([{$match: {age: {$gte: 10}}}, {$project: {"name": true, "car.make": true}}, {$group: {_id: 'name', total: {$min: '$car.make'}}}])
      assert.equal results[0]._id, 'name'
      assert.equal results[0].total, 1
      done()

    it 'groups by field name without $ and $avg', (done) ->
      item = @col.insert { name: 'jack', status: 'awesome', age: 20, car: {'make': 1} }
      item = @col.insert { name: 'jack', status: 'awesome', age: 20, car: {'make': 11} }
      item = @col.insert { name: 'bob', status: 'ok', age: 2 , car: {'make': 12}}
      item = @col.insert { name: 'sam', status: 'eh', age: 12 , car: {'make': 22}}
      results = @col.aggregate([{$match: {age: {$gte: 10}}}, {$project: {"name": true, "car.make": true}}, {$group: {_id: 'name', total: {$avg: '$car.make'}}}])
      assert.equal results[0]._id, 'name'
      assert.equal results[0].total, 11.333333333333334
      done()

    it 'runs $match, $project, $group, $sort then limit with $sum', (done) ->
      item = @col.insert { name: 'jack', status: 'awesome', age: 20, car: {'make': 1} }
      item = @col.insert { name: 'jack', status: 'awesome', age: 20, car: {'make': 1} }
      item = @col.insert { name: 'bob', status: 'ok', age: 2 , car: {'make': 12}}
      item = @col.insert { name: 'sam', status: 'eh', age: 12 , car: {'make': 22}}
      results = @col.aggregate([{$match: {age: {$gte: 10}}}, {$project: {"name": true, "car.make": true}}, {$group: {_id: '$name', total: {$sum: '$car.make'}}}])
      assert.equal results[0]._id, 'jack'
      assert.equal results[0].total, 2
      assert.equal results[1]._id, 'sam'
      assert.equal results[1].total, 22
      done()

    it 'runs $match, $project, $group, $sort then limit with $min', (done) ->
      item = @col.insert { name: 'jack', status: 'awesome', age: 20, car: {'make': 1} }
      item = @col.insert { name: 'jack', status: 'awesome', age: 20, car: {'make': 11} }
      item = @col.insert { name: 'bob', status: 'ok', age: 2 , car: {'make': 12}}
      item = @col.insert { name: 'sam', status: 'eh', age: 12 , car: {'make': 22}}
      results = @col.aggregate([{$match: {age: {$gte: 10}}}, {$project: {"name": true, "car.make": true}}, {$group: {_id: '$name', total: {$min: '$car.make'}}}])
      assert.equal results[0]._id, 'jack'
      assert.equal results[0].total, 1
      assert.equal results[1]._id, 'sam'
      assert.equal results[1].total, 22
      done()

    it 'runs $match, $project, $group, $sort then limit with $avg', (done) ->
      item = @col.insert { name: 'jack', status: 'awesome', age: 20, car: {'make': 1} }
      item = @col.insert { name: 'jack', status: 'awesome', age: 20, car: {'make': 11} }
      item = @col.insert { name: 'bob', status: 'ok', age: 2 , car: {'make': 12}}
      item = @col.insert { name: 'sam', status: 'eh', age: 12 , car: {'make': 22}}
      results = @col.aggregate([{$match: {age: {$gte: 10}}}, {$project: {"name": true, "car.make": true}}, {$group: {_id: '$name', total: {$avg: '$car.make'}}}])
      assert.equal results[0]._id, 'jack'
      assert.equal results[0].total, 6
      assert.equal results[1]._id, 'sam'
      assert.equal results[1].total, 22
      done()

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

    it 'uses $group with $sum accumulator', (done) ->
      item = @col.insert { name: 'jack', status: 'awesome', age: 20 }
      item = @col.insert { name: 'bob', status: 'ok', age: 2 }
      item = @col.insert { name: 'bob', status: 'ok', age: 12 }
      results = @col.aggregate([{$group: {_id: '$name', total: {$sum: '$age'}}}])
      assert.equal results[0]['total'], 20
      assert.equal results[1]['total'], 14
      assert.equal results.length, 2
      done()

    it 'uses $group with $sum accumulator but returns 0 if no dollar', (done) ->
      item = @col.insert { name: 'jack', status: 'awesome', age: 20 }
      item = @col.insert { name: 'bob', status: 'ok', age: 2 }
      item = @col.insert { name: 'bob', status: 'ok', age: 12 }
      results = @col.aggregate([{$group: {_id: '$name', total: {$sum: 'age'}}}])
      assert.equal results[0]['total'], 0
      assert.equal results[1]['total'], 0
      assert.equal results.length, 2
      done()

    it 'uses $match with $group with $sum accumulator', (done) ->
      item = @col.insert { name: 'jack', status: 'awesome', age: 20 }
      item = @col.insert { name: 'bob', status: 'ok', age: 2 }
      item = @col.insert { name: 'bob', status: 'ok', age: 12 }
      results = @col.aggregate([{$match: {name: 'bob'}}, {$group: {_id: '$name', total: {$sum: '$age'}}}])
      assert.equal results[0]['total'], 14
      done()

    it 'uses $group with $avg accumulator', (done) ->
      item = @col.insert { name: 'jack', status: 'awesome', age: 20 }
      item = @col.insert { name: 'bob', status: 'ok', age: 2 }
      item = @col.insert { name: 'bob', status: 'ok', age: 12 }
      results = @col.aggregate([{$group: {_id: '$name', total: {$avg: '$age'}}}])
      assert.equal results[0]['total'], 20
      assert.equal results[1]['total'], 7
      done()
  
    it 'uses $group with $avg accumulator with age without dollar returns 0', (done) ->
      item = @col.insert { name: 'jack', status: 'awesome', age: 20 }
      item = @col.insert { name: 'bob', status: 'ok', age: 2 }
      item = @col.insert { name: 'bob', status: 'ok', age: 12 }
      results = @col.aggregate([{$group: {_id: '$name', total: {$avg: 'age'}}}])
      assert.equal results[0]['total'], 0
      assert.equal results[1]['total'], 0
      done()
  
    it 'uses $limit to return only 1', (done) ->
      item = @col.insert { name: 'jack', status: 'awesome', age: 20 }
      item = @col.insert { name: 'bob', status: 'ok', age: 2 }
      item = @col.insert { name: 'bob', status: 'ok', age: 12 }
      results = @col.aggregate([{$group: {_id: '$name', total: {$avg: '$age'}}}, {$limit: 1}])
      assert.equal results[0]['total'], 20
      assert.equal results.length, 1
      # assert.equal results[1]['total'], 7
      done()

    it 'uses $limit to return only 2', (done) ->
      item = @col.insert { name: 'jack', status: 'awesome', age: 20 }
      item = @col.insert { name: 'bob', status: 'ok', age: 2 }
      item = @col.insert { name: 'bob', status: 'ok', age: 12 }
      results = @col.aggregate([{$group: {_id: '$name', total: {$avg: '$age'}}}, {$limit: 2}])
      assert.equal results.length, 2
      done()

    it 'uses sorts by name', (done) ->
      item = @col.insert { name: 'jack', status: 'awesome', age: 20 }
      item = @col.insert { name: 'bob', status: 'ok', age: 2 }
      item = @col.insert { name: 'nick', status: 'ok', age: 12 }
      results = @col.aggregate([{$sort: {'$name': 1}}])
      assert.equal results[0]['name'], 'bob'
      assert.equal results[1]['name'], 'jack'
      assert.equal results[2]['name'], 'nick'
      done()
  
    it 'uses sorts by name reversed', (done) ->
      item = @col.insert { name: 'jack', status: 'awesome', age: 20 }
      item = @col.insert { name: 'bob', status: 'ok', age: 2 }
      item = @col.insert { name: 'nick', status: 'ok', age: 12 }
      results = @col.aggregate([{$sort: {'$name': -1}}])
      assert.equal results[2]['name'], 'bob'
      assert.equal results[1]['name'], 'jack'
      assert.equal results[0]['name'], 'nick'
      done()
  
    it 'returns correct fields with project', (done) ->
      item = @col.insert { name: 'jack', status: 'awesome', age: 20 }
      item = @col.insert { name: 'bob', status: 'ok', age: 2 }
      item = @col.insert { name: 'nick', status: 'ok', age: 12 }
      results = @col.aggregate([{$project: {'$name': true}}])
      assert.equal results[0]['name'], 'jack'
      assert.equal results[0]['status'], undefined
      done()

    #([{$match: {a: {"$gt": 12}}}, {$project: {_id: false, b: true}}])
    it 'returns correct fields with project and group', (done) ->
      item = @col.insert { name: 'jack', status: 'awesome', age: 20 }
      item = @col.insert { name: 'bob', status: 'ok', age: 2 }
      item = @col.insert { name: 'nick', status: 'ok', age: 12 }
      results = @col.aggregate([$match: {age: {$gt: 5}}, {$project: {_id: false, '$name': true}}])
      assert.equal results[0]['name'], 'jack'
      assert.equal results[0]['status'], undefined
      assert.equal results[0]['_id'], undefined
      assert.equal results[1]['name'], 'nick'
      done()

    it 'handles for non array passed in ', (done) ->
      item = @col.insert { name: 'jack', status: 'awesome', age: 20 }
      item = @col.insert { name: 'bob', status: 'ok', age: 2 }
      item = @col.insert { name: 'nick', status: 'ok', age: 12 }
      results = @col.aggregate($match: {age: {$gt: 5}}, {$project: {_id: false, '$name': true}})
      assert.equal results[0]['name'], 'jack'
      assert.equal results[0]['status'], undefined
      assert.equal results[0]['_id'], undefined
      done()

    it 'skips first record', (done) ->
      item = @col.insert { name: 'jack', status: 'awesome', age: 20 }
      item = @col.insert { name: 'bob', status: 'ok', age: 2 }
      item = @col.insert { name: 'nick', status: 'ok', age: 12 }
      results = @col.aggregate($match: {age: {$gt: 5}},
        {$project: {_id: false, '$name': true}},
        {$skip: 1})
      assert.equal results[0]['name'], 'nick'
      assert.equal results[0]['status'], undefined
      assert.equal results[0]['_id'], undefined
      done()

  context 'With remove', ->
    beforeEach (done) ->
      @reset =>
        done()

    it 'removes all records', (done) ->
      item = @col.insert { name: 'jack', status: 'awesome', age: 20 }
      item = @col.insert { name: 'nick', status: 'awesome', age: 20 }
      @col.remove()
      results = @col.find()

      assert.equal results.length, 0
      done()

    it 'removes only specified record', (done) ->
      item = @col.insert { name: 'jack', status: 'awesome', age: 20 }
      item = @col.insert { name: 'nick', status: 'awesome', age: 20 }
      @col.remove({name: 'jack'})
      results = @col.find()

      assert.equal results.length, 1
      done()


