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
  this._model = null;
  this._search = null;
  // Ugly way of finding out if subquery got created and the main query may not have all the includes for search it needs even if there is no limit applied on the main model itself
  this._subquery = false;
}

Builder.prototype.getEmbeddedIncludeModels = function(include, prefix){
  if(prefix == null)
    prefix = "";
  if(include == null) return [];
  if(_.isArray(include)){
    var mappedArray = _.map(include, function(includeModel){
      var tmpPrefix = "";
      if(includeModel.model){
        // keep it as we might override the value in case of limit clause
        var existingModelIncludes = includeModel.include; 
        //  handle included models wth limits which are to be used for fullText Search also 
        if(includeModel.limit){
          //TODO: the lines below should not be handled here as this is a undesired side effect in function 
          this._subquery = true;
          var res = this.getFullTextSearchClauses(includeModel.model, includeModel.include);
          includeModel.where = res.where;
          includeModel.include = res.include;
        }
        tmpPrefix = prefix + (includeModel.as || includeModel.model.name); 
        //TODO: fix the 'as' logic below in a better way. it should be not need model.as
        var arr = [{model: includeModel.model, prefix: this._model.sequelize.queryInterface.quoteIdentifier(tmpPrefix) + "."}].concat(this.getEmbeddedIncludeModels(existingModelIncludes, tmpPrefix + "." ));
        return arr;
      }
      else{
        tmpPrefix = prefix + includeModel.name;
        return [{model: this._model.sequelize.models[includeModel.name], prefix: this._model.queryInterface.queryInterface(tmpPrefix) + "."}]
      }
    }, this);
    var res = _.flatten(mappedArray, true);
    return res;
  }
  else{
    this.getEmbeddedIncludeModels([include]);
  }
}

Builder.prototype.getFullTextSearchClauses = function(model, include){
  if(include == null)
    include = model.getFullTextIncludeModels(model.sequelize.models)
  var res = {where: {$or: [this.getFullTextSearchClause(model, "`" + model.name + "`" + ".")]}, include: include}; 
  // TODO: https://bitbucket.org/vidyahub/vidyahub/issues/42/how-does-one-pass-this-argument-to-lodash resolve
  this.getEmbeddedIncludeModels(res.include).forEach(function(includeModel){
    if(includeModel && includeModel.model && includeModel.model.getFullTextSearchColNames && _.isFunction(includeModel.model.getFullTextSearchColNames)) {
      res.where.$or.push(this.getFullTextSearchClause(includeModel.model, includeModel.prefix)); 
    }
  }, this);
  if(this._subquery){
    //TODO: needs to be fixed with better code. currently just hacking to get search queries working with limit scenarios
    return {where: [model.getSubQueryFullTextSearchSQL(_.pluck(res.where.$or, 'val').join(" OR "))], include: res.include};
  } else {
    return res;
  }
}

Builder.prototype.buildVirtualFieldWhereClauses = function(whereNode, options){
  // check for virtual fields in the query. they need to be handled separately
  var key = _.keys(whereNode)[0];
  if(["$and", "$or"].indexOf(key) >= 0){
    whereNode[key][0] = this.buildVirtualFieldWhereClauses(whereNode[key][0], options); 
    whereNode[key][1] = this.buildVirtualFieldWhereClauses(whereNode[key][1], options);
    return whereNode;
  }
  if(_.isArray(this._model._virtualAttributes)
      && this._model._virtualAttributes.length > 0
      && this._model._virtualAttributes.indexOf(key) >= 0){
    return this._model.sequelize.where(this._model.attributes[key].rawQuery(options), whereNode[key]);
  } else {
    return whereNode;
  }
}

Builder.prototype.getFullTextSearchClause = function(model, prefix){
  var matchColNames = (_.map(model.getFullTextSearchColNames(), function(col){return (prefix + "`" +col + "`")})).toString();
  var againstCluasePart = ") against('" + this._search + "')";
  return this._model.sequelize.literal("match(" +  matchColNames + againstCluasePart);
}

Builder.prototype.build = function(sequelizeModel, options) {
  this._model = sequelizeModel;
  var result = {};

  if (!_.isEmpty(this._select)) {
    result.attributes = this._select;
  }

  if (!_.isEmpty(this._where)) {
    var fullTextIncludeWhereClauses = null;
    if(!_.isEmpty(this._where['q'])){
      // Special treatment for 'q' keyword, we would like a full text search on full text index columns
      this._search = this._where['q']['$eq'];
      delete this._where['q'];
      fullTextIncludeWhereClauses = this.getFullTextSearchClauses(sequelizeModel);
    }
    this._where = this.buildVirtualFieldWhereClauses(this._where, options);
    result.include = fullTextIncludeWhereClauses ? fullTextIncludeWhereClauses.include : null; 
    result.where = {$and: [this._where, (fullTextIncludeWhereClauses ? fullTextIncludeWhereClauses.where : null)]};
  }

  // allow addition of extra custom attributes if the model expects it
  if(_.isFunction(sequelizeModel.getAttributesInclude)){
    result.attributes = {include: sequelizeModel.getAttributesInclude(options)};
  } 
 
  if(_.isFunction(sequelizeModel.getDefaultWhereCluase)){
    if(_.isObject(result.where)){
      result.where = _.extend(result.where, sequelizeModel.getDefaultWhereCluase(options));
    } else {
      result.where = sequelizeModel.getDefaultWhereCluase(options);
    }
  }

  if (!_.isEmpty(this._order)) {
    // check for virtual fields here. they need to be handled separately
    this._order = _.map(this._order, function(orderEle){
      if(sequelizeModel._virtualAttributes.indexOf(orderEle[0]) >= 0){
        return [sequelizeModel.sequelize.col(orderEle[0]), orderEle[1]];
      } else {
        return orderEle;
      }
    });
    result.order = this._order;
  } else if(_.isFunction(sequelizeModel.getDefaultOrder)) {// allow addition of default ordering expected by model 
    result.order = sequelizeModel.getDefaultOrder();
  } 

  if (this._limit) {
    result.limit = this._limit;
  }

  if(_.isFunction(sequelizeModel.getMaxRows)){
    result.limit = result.limit || sequelizeModel.getMaxRows();
  }

  if (this._skip) {
    result.offset = this._skip;
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
      if(_.isEmpty(objArr1) && _.isEmpty(objArr2)){
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
          objArrHash2[obj.model.name].where = {$and: [obj.where, objArrHash2[obj.model.name].where]};
          objArrHash2[obj.model.name].include = mergeIncludeObjects(obj.include, objArrHash2[obj.model.name].include);
        }
      });
      return onlyInArr1.concat(objArr2)
    }
    
    result.include = mergeIncludeObjects(result.include, this._include);
    // let's include default includes for the model as that will be required to show list 
    result.include = mergeIncludeObjects(result.include, sequelizeModel.getFindAllIncludes(sequelizeModel.sequelize.models));
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
