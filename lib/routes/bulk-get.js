'use strict';

// NOTE: The _bulk_get end point does not return its results line-by-line
// as e.g. _changes.
//
// NOTE: The response format is convoluted, and seemingly undocumented.
//
//  "results": [
// {
//   "id": "1c43dd76fee5036c0cb360648301a710",
//   "docs": [
//     {
//       "ok": { ..doc body here...
//
//         }
//       }
//     }
//   ]
// },
//
// Not sure if the "docs" array can ever contain multiple items.

var express = require('express'),
  router = express.Router(),
  app = require('../../app'),
  auth = require('../auth'),
  access = require('../access'),
  uuid = require('uuid'),
  async = require('async'),
  utils = require('../utils');

// Pouch does this to check it exists
router.get('/:db/_bulk_get', auth.isAuthenticated, function(req, res) {
  app.cloudant.request({
    db: app.dbName,
    qs: req.query || {},
    path: '_bulk_get'
  }).pipe(res);
});

// simulate POST /db/_bulk_get with lots of GETs
var simulatedBulkGet = function(req,res) {
    if (req.body && req.body.docs) {

    // add ownerids to incoming ids
    req.body.docs = req.body.docs.map(function(doc) {
      doc.id = access.addOwnerId(doc.id, req.session.user.name);
      return doc;
    });
    
    // build up an array of individual get requests to be made in parallel
    var tasks = [];
    req.body.docs.forEach(function(doc) {
      (function(d){
        tasks.push(function(callback) {
          var opts = req.query;
          opts.revs = true;
          app.db.get(d.id, opts, function(err, data) {
            if (err) {
              data = { id: d.id, rev: opts.rev, error: err.error,  }
            }
            var result = { id: access.removeOwnerId(d.id), docs:[ ] };
            var item = {};
            if (data.error) {
              item.error = access.strip(data);
            } else {
              item.ok = access.strip(data);
            }
            result.docs.push(item);
            callback(null, result);
          });
        });
      })(doc);
    });
    
    // run the tasks in parallel (up to 10 at a time)
    async.parallelLimit(tasks, 10, function(err, data) {
      res.send({results: data});
    });

  } else {
    res.status(400).send({error:'missing docs parameter'});
  }
};

// use real POST /db/_bulk_get
var realBulkGet = function(req, res) {
  // add ownerids to incoming ids
  if (req.body && req.body.docs) {
    req.body.docs = req.body.docs.map(function(doc) {
      doc.id = access.addOwnerId(doc.id, req.session.user.name);
      return doc;
    });
  }
  app.cloudant.request({
    db: app.dbName,
    qs: req.query || {},
    path: '_bulk_get',
    method: 'POST',
    body: req.body
  }, function (err, data) {
    if (err) {
      return utils.sendError(err, res);
    }
    res.send({ results: data.results.map(function (row) {
      var stripped = Object.assign({}, row);
      stripped.id = access.removeOwnerId(stripped.id);
      stripped.docs.forEach(function (item) {
        if (item.ok) {
          access.strip(item.ok);
        }
        if (item.error) {
          access.strip(item.error);
        }  
      });        
      return stripped;
    })});
  });
};

// use real or simulated bulk_get
router.post('/:db/_bulk_get',auth.isAuthenticated, function(req, res) {
  if (app.bulkGet) {
    realBulkGet(req, res);
  } else {
    simulatedBulkGet(req, res);
  }
});

module.exports = router;