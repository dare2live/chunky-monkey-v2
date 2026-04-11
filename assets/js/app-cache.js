(function (global) {
  'use strict';

  var SHORT_CACHE_TTL_MS = 15 * 1000;
  var _shortApiCache = {};

  function apiCached(apiFn, path, ttlMs, opts) {
    var method = ((opts && opts.method) || 'GET').toUpperCase();
    if (method !== 'GET') return apiFn(path, opts);
    var ttl = Number(ttlMs || SHORT_CACHE_TTL_MS);
    var now = Date.now();
    var entry = _shortApiCache[path];
    if (entry && entry.data != null && (now - entry.ts) < ttl) return entry.data;
    if (entry && entry.promise) return entry.promise;
    var promise = apiFn(path, opts).then(function (data) {
      _shortApiCache[path] = { ts: Date.now(), data: data, promise: null };
      return data;
    }).catch(function () {
      delete _shortApiCache[path];
      return null;
    });
    _shortApiCache[path] = { ts: now, data: entry ? entry.data : null, promise: promise };
    return promise;
  }

  function clearShortApiCache(path) {
    if (path) {
      delete _shortApiCache[path];
      return;
    }
    _shortApiCache = {};
  }

  global.AppCache = {
    SHORT_CACHE_TTL_MS: SHORT_CACHE_TTL_MS,
    apiCached: apiCached,
    clearShortApiCache: clearShortApiCache,
  };
})(window);