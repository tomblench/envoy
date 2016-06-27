var envoy = window.location.origin;

$(document).ready(function () {
  // handle login submission
  $('#register').submit(function (e) {
    e.preventDefault();
    var username = $('#username').val();
    var password = $('#password').val();
    if (!username || !password) {
      return Materialize.toast('Please supply username and password', 4000)
    }
    var r = {
      method: 'post',
      url: envoy + '/_adduser',
      data: {
        username: username,
        password: password
      }
    };
    $.ajax(r).done(function (data) {
      window.location.href = '/demo/login.html';
    }).fail(function () {
      Materialize.toast('Invalid username or password', 4000)
    });
  });
});
