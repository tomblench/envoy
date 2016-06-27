var envoy = window.location.origin;
var db = new PouchDB('envoydemo');

var logout = function() {
  var r = {
    url: envoy + '/_logout',
    xhrFields: {
      withCredentials: true
     }
  };
  $.ajax(r).done(function (data) {
    location.reload();
  });
}

var onEdit = function(e) {
  e.preventDefault();
  var x = $(this).attr('data-id');
  var self = this;
  db.get(x).then(function(doc) {
    console.log(doc);
    var form = '<form onsubmit="return onEditSubmit(this)">';
    form += '<input id="update{{id}}" data-id="{{id}}" type="text" class="validate" value="{{todo}}">';
    form += '</form>';
    var html = Mustache.render(form, {id: x, todo: doc.todo});;
    $(self).parent().html(html);
    $('#delete' + x).addClass('hide');
  });
};

var onEditSubmit = function(f) {
  var input = $(f).children()[0];
  var id = $(input).attr('data-id');
  var todo = $(input).val();
  db.get(id).then(function(doc) {
    doc.todo = todo;
    return db.post(doc);
  }).then(function(data) {
    renderList();
  })
  return false;
}

var onDelete = function(e) {
  e.preventDefault();
  var x = $(this).attr('data-id');
  var self = this;
  db.get(x).then(function(doc) {
    console.log(doc);
    return db.remove(doc._id, doc._rev);
  }).then(function(data){
    console.log(data);
    renderList();
  });
};

var sync = function() {
  var remote = new PouchDB(envoy + '/mbaas');
  $('#syncprogress').removeClass('hide');
  db.sync(remote).on('complete', function (info) {
    $('#syncprogress').addClass('hide');
    Materialize.toast('Sync complete', 4000);
    renderList();
  }).on('error', function (err) {
    $('#syncprogress').addClass('hide');
    Materialize.toast('Sync error', 4000);
    console.log(err);
  });

}
var renderList = function() {
  var fun = function(doc) {
    emit(doc.ts, null);
  }
  db.query(fun, {descending:true, include_docs:true}).then(function(data) {
    if (data.rows.length > 0) {
      var html = '<ul class="collection">\n';
      var li = '<li class="collection-item">';
      li += '<span><a href="#!" class="edit" data-id="{{id}}">{{todo}}</a></span>';
      li += '<span><a id="delete{{id}}" data-id="{{id}}" href="#!" class="delete secondary-content"><i class="material-icons">delete</i></a></span>';
      li += '</li>';
      for(var i in data.rows) {
        html += Mustache.render(li, {id: data.rows[i].id, todo: data.rows[i].doc.todo});
      }
      html += '</ul>';
      $('#list').html(html);
      $('.edit').on("click", onEdit);
      $('.delete').on("click", onDelete);
    }
  });
}
var checkLogin = function () {
  console.log("URL",envoy + '/_auth')
  var r = {
    url: envoy + '/_auth',
    xhrFields: {
      withCredentials: true
    }
  };
  $.ajax(r).done(function (data) {
    $('#addformcontainer').removeClass('hide');
    $('.synclink').removeClass('hide');
    $('.logoutlink').removeClass('hide');
    $('.loginlink').addClass('hide');
    $('.registerlink').addClass('hide');
    renderList();
    sync();
  }).fail(function () {
    $('.loginlink').removeClass('hide');
    $('.registerlink').removeClass('hide');
    renderList();
  });
};

$(document).ready(function () {
  $(".button-collapse").sideNav();
  console.log("ready");
  checkLogin();

  // handle login submission
  $('#addform').submit(function (e) {
    e.preventDefault();
    var todo = $('#todo').val();
    if (!todo) {
      return Materialize.toast('Missing to do', 4000)
    }
    $('#todo').val('');
    var date = new Date();
    var doc = {
      todo: todo,
      date: date.toISOString(),
      ts: date.getTime()
    };
    db.post(doc).then(function() {
      renderList();
    });
  });

});
