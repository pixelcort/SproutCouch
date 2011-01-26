/*globals SproutCouch, SCD*/

SCD = SC.Object.create({
  store: SC.Store.create({
    commitRecordsAutomatically: YES
  }).from(SproutCouch.DataSource.create({
    database: 'scd'
  }))
});
