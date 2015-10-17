var _ = require('lodash');

module.exports = Builder;

function Builder() {
  if (!(this instanceof Builder)) return new Builder();

  this._select = []
  this._where  = {}
  this._order  = []
  this._limit  = null;
  this._skip = null;
  this._include = [];
}

Builder.prototype.getEmbeddedIncludeModels = function(include, prefix){
  if(prefix == null)
    prefix = "";
  if(include == null) return [];
  if(_.isArray(include)){
    var mappedArray = _.map(include, function(model){
      if(model.model){
        var arr = [{model: model.model, prefix: "`" + prefix + model.model.name + "`" + "."}].concat(this.getEmbeddedIncludeModels(model.include, prefix + model.model.name + "." ));
        return arr;
      }
      else{
        return [{model: model, prefix: "`" + prefix + model.name + "`" + "."}]
      }
    }, this);
    var res = _.flatten(mappedArray, true);
    return res;
  }
  else{
    this.getEmbeddedIncludeModels([include]);
  }
}

Builder.prototype.build = function(sequelizeModel, options) {
  var result = {};

  if (!_.isEmpty(this._select)) {
    result.attributes = this._select;
  }

  if (!_.isEmpty(this._where)) {
    if(!_.isEmpty(this._where['q'])){
      // Special treatment for 'q' keyword, we would like a full text search on full text index columns
      var value = this._where['q']['$eq'];
      delete this._where['q'];
      var matchColNames = (_.map(sequelizeModel.getFullTextSearchColNames(), function(col){return ("`" + sequelizeModel.name + "`" + "." + "`" +col + "`")})).toString();
      var againstCluasePart = ") against('" + value + "')";
      var fullTextOrCond = [sequelizeModel.sequelize.literal("match(" +  matchColNames + againstCluasePart)];
      _.forEach(this.getEmbeddedIncludeModels(sequelizeModel.getFullTextIncludeModels(sequelizeModel.sequelize.models)), function(model){
        matchColNames = (_.map(model.model.getFullTextSearchColNames(), function(col){return (model.prefix + "`" + col + "`" )})).toString();
        fullTextOrCond.push(sequelizeModel.sequelize.literal("match(" +  matchColNames + againstCluasePart)); 
      });
      result.where = {$and: [this._where, {$or: fullTextOrCond}]};
      result.include = sequelizeModel.getFullTextIncludeModels(sequelizeModel.sequelize.models); 
    } else {
      result.where = this._where;
    }
  }

  // allow addition of extra custom attributes if the model expects it
  if(_.isFunction(sequelizeModel.getAttributesInclude)){
    result.attributes = {include: [sequelizeModel.getAttributesInclude(options)]};
  } 
 
  if(_.isFunction(sequelizeModel.getDefaultWhereCluase)){
    if(_.isObject(result.where)){
      result.where = _.extend(result.where, sequelizeModel.getDefaultWhereCluase(options));
    } else {
      result.where = sequelizeModel.getDefaultWhereCluase(options);
    }
  }

  if (!_.isEmpty(this._order)) {
    result.order = this._order;
  } else if(_.isFunction(sequelizeModel.getDefaultOrder)) {// allow addition of default ordering expected by model 
    result.order = sequelizeModel.getDefaultOrder();
  } 

  if (this._limit) {
    result.limit = this._limit;
  }

  if (this._skip) {
    result.skip = this._skip;
  }

  if(this._include){
    // change include to actual model objects
    var assignModelObj = function(arr){
      _.forEach(arr, function(obj){
        obj.model = sequelizeModel.sequelize.models[obj.model];
        if(_.isArray(obj.include)){
          assignModelObj(obj.include);
        }
      });
    }
    assignModelObj(this._include);
    // merge with include from free text search recursively
    var mergeIncludeObjects = function(objArr1, objArr2){
      if((objArr1 == null && objArr2 == null) || ((objArr1 && objArr1.length === 0) && (objArr2 && objArr2.length === 0))){
        return []
      }
      if(objArr1 == null || objArr1.length === 0){
        return objArr2
      }
      if(objArr2 == null || objArr2.length === 0){
        return objArr1
      }
      // find objects in array 1 only 
      // modify objects in both array in array 2
      // concat with array 2
      var objArrHash2 = _.indexBy(objArr2, function(obj){return obj.model.name});
      var onlyInArr1 = _.filter(objArr1, function(obj){
        if(objArrHash2[obj.model.name] == null){
          return true;
        }
      }); 
      _.forEach(objArr1, function(obj){
        if(objArrHash2[obj.model.name] != null){
          //assuming only one will have where clauses resulting in inner joins
          objArrHash2[obj.model.name].where = obj.where || objArrHash2[obj.model.name].where;
          objArrHash2[obj.model.name].include = mergeIncludeObjects(obj.include, objArrHash2[obj.model.name].include);
        }
      });
      return onlyInArr1.concat(objArr2)
    }
    
    result.include = mergeIncludeObjects(result.include, this._include);
    // let's include default includes for the model as that will be required to show list 
    result.include = mergeIncludeObjects(result.include, sequelizeModel.getIncludes(sequelizeModel.sequelize.models));
  }

  return result;
}

Builder.prototype.select = function() {
  this._select = this._select.concat(_.flatten(_.toArray(arguments), true));
  return this;
}

Builder.prototype.sort = function(a, b) {
  if (b) {
    this._order.push([a, b]);
  } else {
    if(_.isString(a)){
      this._order.push(a);
    } else if(_.isObject(a)){
      this._order = this._order.concat(_.pairs(a));
    } else {
      throw "unexpected argument for sort function"
    }
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
Builder.prototype.include = function(include) {
  this._include = include;
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
