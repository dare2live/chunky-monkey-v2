(function (global) {
  'use strict';

  var _inst = {
    dim: 'overview',
    data: []
  };
  var _stock = {
    data: [],
    filterSignal: 'all',
    filterGate: 'all'
  };
  var _industry = {
    data: [],
    summary: null
  };

  function setInstData(data) {
    _inst.data = Array.isArray(data) ? data : [];
    return _inst.data;
  }

  function setStockData(data) {
    _stock.data = Array.isArray(data) ? data : [];
    return _stock.data;
  }

  function setIndustryData(data) {
    _industry.data = Array.isArray(data) ? data : [];
    return _industry.data;
  }

  global.AppListState = {
    inst: {
      getDim: function () { return _inst.dim; },
      setDim: function (dim) { _inst.dim = dim || 'overview'; return _inst.dim; },
      getData: function () { return _inst.data; },
      setData: setInstData,
    },
    stock: {
      getData: function () { return _stock.data; },
      setData: setStockData,
      getFilterSignal: function () { return _stock.filterSignal; },
      setFilterSignal: function (value) { _stock.filterSignal = value || 'all'; return _stock.filterSignal; },
      getFilterGate: function () { return _stock.filterGate; },
      setFilterGate: function (value) { _stock.filterGate = value || 'all'; return _stock.filterGate; },
    },
    industry: {
      getData: function () { return _industry.data; },
      setData: setIndustryData,
      getSummary: function () { return _industry.summary; },
      setSummary: function (summary) { _industry.summary = summary || null; return _industry.summary; },
    }
  };
})(window);