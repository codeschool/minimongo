module.exports = function(method, url, params, data, success, error) {
  var fullUrl, req;
  fullUrl = url + "?" + $.param(params);
  if (method === "GET") {
    req = $.getJSON(fullUrl);
  } else if (method === "DELETE") {
    req = $.ajax(fullUrl, {
      type: 'DELETE'
    });
  } else if (method === "POST" || method === "PATCH") {
    req = $.ajax(fullUrl, {
      data: JSON.stringify(data),
      contentType: 'application/json',
      type: method
    });
  }
  req.done(function(response, textStatus, jqXHR) {
    if (response == null) {
      console.error(("Empty response: " + fullUrl + ":" + method + " returned ") + jqXHR.responseText + " as JSON " + JSON.stringify(response));
    }
    return success(response || null);
  });
  return req.fail(function(jqXHR, textStatus, errorThrown) {
    if (error) {
      return error(jqXHR);
    }
  });
};
