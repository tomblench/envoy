var envoy = window.location.origin;

$(document).ready(function () {

  // handle login submission
  $('#login').submit(function (e) {
    e.preventDefault();
    var username = $('#username').val();
    var password = $('#password').val();
    var r = {
      url: envoy + '/_auth',
      json: true,
      beforeSend: function (xhr) {
        xhr.setRequestHeader("Authorization", "Basic " + btoa(username + ":" + password));
      },
      xhrFields: {
        withCredentials: true
      }
    };
    $.ajax(r).done(function (data) {
      window.location.href = '/demo/';
    }).fail(function () {
      Materialize.toast('Incorrect username and password', 4000)
    });
  });



});
