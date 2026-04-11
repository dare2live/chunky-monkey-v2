(function (global) {
  'use strict';

  var _currentGroup = 'holder';
  var _etfState = {
    currentTab: 'workbench',
    dataCache: null,
    categoryFilter: 'all',
    strategyFilter: 'all'
  };

  function getCurrentGroup() {
    return _currentGroup;
  }

  function setCurrentGroup(groupName) {
    _currentGroup = groupName || 'holder';
    return _currentGroup;
  }

  function getEtfState() {
    return _etfState;
  }

  function setEtfTab(tabName) {
    _etfState.currentTab = tabName || 'workbench';
    return _etfState.currentTab;
  }

  global.AppNav = {
    getCurrentGroup: getCurrentGroup,
    setCurrentGroup: setCurrentGroup,
    getEtfState: getEtfState,
    setEtfTab: setEtfTab,
  };
})(window);