/*globals SproutCouch*/

SproutCouch.DataSource = SC.DataSource.extend({
  database: '',

  fetch: function(store, query) {
    // Note, these properties are temporary and are going to change!
    if (!query.temprary_url_fragment) throw SC.$error('temporary_url_fragment was not provided!');
    if (!query.temporary_url_method) throw  SC.$error('temporary_url_method was not provided!');
    var needs_body = ['PUT','POST'].indexOf(query.temporary_url_method)>-1;
    if (needs_body && !query.temporary_url_body) throw SC.$error('temporary_url_body was not provided!');
    if (!query.respondsTo('sproutCouchDataSourceDidComplete')) throw SC.$error('sproutCouchDataSourceDidComplete callback was not provided!');

    var req = SC.Request.create()
                        .set('address', '/'+this.database+'/'+query.temporary_url_fragment)
                        .set('type', query.temporary_url_method)
                        .notify(this, 'fetchDidComplete', {
                          store: store,
                          query: query
                        })
                        .send(query.temporary_url_body); // Will be undefined for GET and DELETE, but that's okay. :)
    return YES;
  },
  fetchDidComplete: function(request, params) {
    var response = request.get('response');
    var callbackResult = params.query.sproutCouchDataSourceDidComplete({
      temporary_store: params.store
    });
    if (callbackResult.error) {
      params.store.dataSourceDidErrorQuery(params.query, callbackResult.error);
    } else if (callbackResult.remoteStoreKeys || params.query.isRemote) {
      if (!callbackResult.remoteStoreKeys) throw SC.$error('remote queries need remoteStoreKeys!');
      if (!params.query.isRemote)          throw SC.$error('local queries don\'t need remoteStoreKeys!');
      params.store.loadQueryResults(params.store, callbackResult.remoteStoreKeys);
      // loadQueryResults calls dataSourceDidFetchQuery for us. :)
    } else {
      params.store.dataSourceDidFetchQuery(params.query);
    }
  },

  retrieveRecords: function(store, storeKeys, ids) {
    if (storeKeys.length !== ids.length) {
      throw SC.$error('storeKeys and ids were not the same length!');
    }

    SC.Request.postUrl('/'+this.database+'/_all_docs?include_docs=true')
              .json()
              .notify(this, 'retrieveRecordsRequestDidComplete', {
                store: store,
                storeKeys: storeKeys
              })
              .send({keys: ids});
    return YES;
  },
  retrieveRecordsRequestDidComplete: function(request, params) {
    var response = request.get('response'),
        dataHashes = response.rows.map(function(row){return row.doc;});
    if (dataHashes.length !== params.storeKeys.length) throw SC.$error('lengths did not match');
    // Note: This appears to call dataSourceDidComplete, not pushRetreive; this might be useful for when we add error handling
    params.store.loadRecords(params.storeKeys.map(function(storeKey){return params.store.recordTypeFor(storeKey);}), dataHashes);
  },

  commitRecords: function(store, createStoreKeys, updateStoreKeys, destroyStoreKeys, params) {
    var allStoreKeys = [].concat(createStoreKeys, updateStoreKeys, destroyStoreKeys);
    if (params) throw SC.$error('params was provided!');

    destroyStoreKeys.forEach(function(destroyStoreKey) {
      store.readDataHash(destroyStoreKey)._deleted = true;
    });

    SC.Request.postUrl('/'+this.database+'/_bulk_docs')
              .json()
              .notify(this, 'commitRecordsRequestDidComplete', {
                store: store,
                createStoreKeys: createStoreKeys,
                updateStoreKeys: updateStoreKeys,
                destroyStoreKeys: destroyStoreKeys,
                allStoreKeys: allStoreKeys
              })
              .send({
                docs: allStoreKeys.map(function(storeKey){return store.readDataHash(storeKey);})
              });
  },
  commitRecordsRequestDidComplete: function(request, params) {
    var response = request.get('response'),
        storeKeys = params.allStoreKeys;
    if (response.length !== params.allStoreKeys.length) throw SC.$error('lengths did not match');

    for (var i=0,l=storeKeys.length;i<l;i++) {
      var storeKey = storeKeys[i],
          responseDataHash = response[i],
          dataHash = params.store.readDataHash(storeKey);

      if (params.destroyStoreKeys.indexOf(storeKey)==-1) {
        dataHash._id  = responseDataHash.id;
        dataHash._rev = responseDataHash.rev;
        params.store.dataSourceDidComplete(storeKey, dataHash, responseDataHash.id);
      } else {
        params.store.dataSourceDidDestroy(storeKey);
      }
    }
  }
});
