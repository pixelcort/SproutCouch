/*globals SproutCouch*/

SproutCouch.DataSource = SC.DataSource.extend({
  database: '',

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
