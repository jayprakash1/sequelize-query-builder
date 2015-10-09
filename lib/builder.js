var _ = require('lodash');

module.exports = Builder;

function Builder() {
  if (!(this instanceof Builder)) return new Builder();

  this._select = []
  this._where  = {}
  this._order  = []
  this._limit  = null;
  this._skip = null;
}

Builder.prototype.build = function(db) {
  var result = {};

  if (!_.isEmpty(this._select)) {
    result.attributes = this._select;
  }

  if (!_.isEmpty(this._where)) {
    if(!_.isEmpty(this._where['q'])){
      // Special treatment for 'q' keyword, we would like a full text search on full text index columns
      var value = this._where['q']['$eq'];
      delete this._where['q'];
      result.where = {$and: [this._where, db.sequelize.literal("match(name) against('" + value + "')")]};
    } else {
      result.where = this._where;
    }
  }

  if (!_.isEmpty(this._order)) {
    result.order = this._order;
  }

  if (this._limit) {
    result.limit = this._limit;
  }

  if (this._skip) {
    result.skip = this._skip;
  }

  return result;
}

Builder.prototype.select = function() {
  this._select = this._select.concat(_.flatten(_.toArray(arguments), true));
  return this;
}

Builder.prototype.order = function(a, b) {
  if (b) {
    this._order.push([a, b]);
  } else {
    this._order.push(a);
  }

  return this;
}

Builder.prototype.limit = function(limit) {
  this._limit = limit;
  return this;
}
Builder.prototype.skip = function(skip) {
  this._skip = skip;
  return this;
}

Builder.prototype.where = function(a, b) {
  this._context = null;

  if (_.isObject(a)) {
    this._where = _.assign(this._where, a);
  } else {
    this._context = a;
    this._where[a] = this._where[a] || {};

    if (b) return this.eq(b);
  }

  return this;
}

var comparators = {
  equals:   "$eq",
  ne:       "$ne",
  gt:       "$gt",
  gte:      "$gte",
  lt:       "$lt",
  lte:      "$lte",
  is:       "$is",
  not:      "$not",
  like:     "$like",
  notLike:  "$notLike",
  ilike:    "$iLike",
  notIlike: "$notILike",
}

_.forEach(comparators, function(comparator, method) {
  Builder.prototype[method] = function(value) {
    if (this._context) {
      this._where[this._context][comparator] = value;
    }

    return this;
  }
});

var arrays = {
  in:         "$in",
  notIn:      "$notIn",
  between:    "$between",
  notBetween: "$notBetween",
  overlap:    "$overlap",
  contains:   "$contains",
  contained:  "$contained"
}

_.forEach(arrays, function(comparator, method) {
  Builder.prototype[method] = function() {
    if (this._context) {
      var arr;

      if (_.isArray(arguments[0])) {
        arr = arguments[0];
      } else {
        arr = _.toArray(arguments);
      }

      this._where[this._context][comparator] = arr;
    }

    return this;
  }
})
