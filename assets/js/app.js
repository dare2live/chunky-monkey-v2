/**
 * Chunky Monkey v2 — 前端主逻辑
 */
(function () {
  'use strict';
  const BASE = location.origin;
  const SHORT_CACHE_TTL_MS = 15 * 1000;
  var _shortApiCache = {};

  // ─── Toast 通知 ───────────────────────────────────────────
  var _toastContainer = null;
  function showToast(msg, type) {
    if (!_toastContainer) {
      _toastContainer = document.createElement('div');
      _toastContainer.id = 'toast-container';
      _toastContainer.style.cssText = 'position:fixed;top:16px;right:16px;z-index:99999;display:flex;flex-direction:column;gap:8px;pointer-events:none;';
      document.body.appendChild(_toastContainer);
    }
    var el = document.createElement('div');
    var bg = type === 'error' ? '#ef4444' : type === 'warn' ? '#f59e0b' : '#10b981';
    el.style.cssText = 'padding:10px 18px;border-radius:8px;color:#fff;font-size:13px;background:' + bg + ';opacity:0;transition:opacity .3s;pointer-events:auto;max-width:360px;box-shadow:0 4px 12px rgba(0,0,0,.15);';
    el.textContent = msg;
    _toastContainer.appendChild(el);
    requestAnimationFrame(function () { el.style.opacity = '1'; });
    setTimeout(function () {
      el.style.opacity = '0';
      setTimeout(function () { el.remove(); }, 350);
    }, 4000);
  }

  // ─── API 层（带重试 + 用户提示）────────────────────────────
  async function api(path, opts) {
    var maxRetries = 1;
    for (var attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        var r = await fetch(BASE + path, {
          cache: 'no-store',
          headers: { 'Content-Type': 'application/json' },
          ...opts
        });
        if (!r.ok) throw new Error('HTTP ' + r.status);
        return await r.json();
      } catch (e) {
        console.warn('[API]', path, 'attempt', attempt + 1, e.message);
        if (attempt < maxRetries) {
          await new Promise(function (resolve) { setTimeout(resolve, 1000 * (attempt + 1)); });
          continue;
        }
        showToast('请求失败: ' + path.split('?')[0] + ' (' + e.message + ')', 'error');
        return null;
      }
    }
  }

  async function apiCached(path, ttlMs, opts) {
    var method = ((opts && opts.method) || 'GET').toUpperCase();
    if (method !== 'GET') return api(path, opts);
    var ttl = Number(ttlMs || SHORT_CACHE_TTL_MS);
    var now = Date.now();
    var entry = _shortApiCache[path];
    if (entry && entry.data != null && (now - entry.ts) < ttl) return entry.data;
    if (entry && entry.promise) return entry.promise;
    var promise = api(path, opts).then(function (data) {
      _shortApiCache[path] = { ts: Date.now(), data: data, promise: null };
      return data;
    }).catch(function () {
      delete _shortApiCache[path];
      return null;
    });
    _shortApiCache[path] = { ts: now, data: entry ? entry.data : null, promise: promise };
    return promise;
  }

  // ============================================================
  // Navigation
  // ============================================================
  // ============================================================
  // Navigation — 两级：股东挖掘 / ETF研究
  // ============================================================
  var _currentGroup = 'holder';
  var _currentEtfTab = 'overview';
  var _etfDataCache = null; // cache ETF list data for category filtering

  function showGroup(name) {
    _currentGroup = name;
    document.querySelectorAll('.nav-group-btn').forEach(b => b.classList.toggle('active', b.dataset.group === name));
    var subHolder = el('nav-sub-holder');
    var subEtf = el('nav-sub-etf');
    if (subHolder) subHolder.style.display = name === 'holder' ? '' : 'none';
    if (subEtf) subEtf.style.display = name === 'etf' ? '' : 'none';
    if (name === 'holder') {
      // 进入股东挖掘默认显示工作台
      showView('dashboard');
    } else if (name === 'etf') {
      showView('etf');
    }
  }

  function showView(name) {
    document.querySelectorAll('.view').forEach(v => v.classList.toggle('active', v.id === 'view-' + name));
    // 更新子导航按钮的 active 状态（仅更新当前板块的子导航）
    var subBar = _currentGroup === 'etf' ? el('nav-sub-etf') : el('nav-sub-holder');
    if (subBar) {
      subBar.querySelectorAll('.nav-btn').forEach(b => {
        if (name === 'etf') {
          // ETF 子导航 active 状态由 etftab 控制
          b.classList.toggle('active', b.dataset.etftab === _currentEtfTab);
        } else {
          b.classList.toggle('active', b.dataset.view === name);
        }
      });
    }
    ({ dashboard: loadDashboard, research: loadResearch, stocks: loadStockView, qlib: loadQlib, etf: loadEtf })[name]?.();
  }

  function showEtfTab(tabName) {
    _currentEtfTab = tabName;
    document.querySelectorAll('.etf-tab-content').forEach(c => c.classList.toggle('active', c.id === 'etftab-' + tabName));
    // Update ETF sub-nav active state
    var subEtf = el('nav-sub-etf');
    if (subEtf) {
      subEtf.querySelectorAll('.nav-btn').forEach(b => b.classList.toggle('active', b.dataset.etftab === tabName));
    }
    // Lazy-load content for the tab (if cache missing, fetch first)
    if (!_etfDataCache && (tabName === 'overview' || tabName === 'list')) {
      loadEtf();
      return;
    }
    if (tabName === 'overview') loadEtfOverview();
    if (tabName === 'list') loadEtfList();
    if (tabName === 'etf-qlib') loadEtfQlib();
  }

  // 顶层板块切换
  document.querySelectorAll('.nav-group-btn').forEach(b => b.addEventListener('click', () => showGroup(b.dataset.group)));

  // 子导航按钮
  document.querySelectorAll('.nav-sub-bar .nav-btn').forEach(b => {
    b.addEventListener('click', () => {
      if (b.dataset.etftab) {
        // ETF 子标签
        showView('etf');
        showEtfTab(b.dataset.etftab);
      } else {
        showView(b.dataset.view);
      }
    });
  });

  // Dashboard sub-tabs
  // 工作台不再有 tabs，排除规则和网络检测在页面加载时一起执行

  // Research sub-tabs
  document.querySelectorAll('.research-tabs .tab-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.research-tabs .tab-btn').forEach(b => b.classList.remove('active'));
      document.querySelectorAll('.rtab-content').forEach(c => c.classList.remove('active'));
      btn.classList.add('active');
      var t = document.getElementById('rtab-' + btn.dataset.rtab);
      if (t) t.classList.add('active');
      if (btn.dataset.rtab === 'mgmt') loadInstMgmt();
      if (btn.dataset.rtab === 'scorecard') loadInstScorecard();
    });
  });

  // Stock sub-tabs
  document.querySelectorAll('.stock-tabs .tab-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.stock-tabs .tab-btn').forEach(b => b.classList.remove('active'));
      document.querySelectorAll('.stab-content').forEach(c => c.classList.remove('active'));
      btn.classList.add('active');
      var t = document.getElementById('stab-' + btn.dataset.stab);
      if (t) t.classList.add('active');
      setStockSearchContext(btn.dataset.stab);
      loadActiveStockSubtab();
    });
  });

  // ============================================================
  // Health
  // ============================================================
  async function checkHealth() {
    var badge = el('statusBadge'), r = await api('/health');
    if (r && r.status === 'ok') {
      badge.textContent = 'Online'; badge.className = 'logo-status online';

      // Update dynamic modules nav
      var modules = r.enabled_modules || [];
      var navQlib = el('nav-qlib'), navGroupEtf = el('nav-group-etf');
      if (navQlib) navQlib.style.display = modules.includes('qlib') ? '' : 'none';
      if (navGroupEtf) navGroupEtf.style.display = modules.includes('etf') ? '' : 'none';

      // Update checkboxes in settings
      var chkQlib = el('chkModuleQlib'), chkEtf = el('chkModuleEtf');
      if (chkQlib) chkQlib.checked = modules.includes('qlib');
      if (chkEtf) chkEtf.checked = modules.includes('etf');
    }
    else { badge.textContent = 'Offline'; badge.className = 'logo-status'; }
  }

  async function saveModuleSettings() {
    if (!confirm('保存模块配置后，后端可能需要重启以重新注册路由。继续吗？')) return;
    var settings = {
      qlib: el('chkModuleQlib')?.checked,
      etf: el('chkModuleEtf')?.checked
    };
    var r = await api('/api/settings/modules', { method: 'POST', body: JSON.stringify(settings) });
    if (r && r.status === 'ok') {
      alert(r.message);
      checkHealth();
    } else {
      alert('保存失败: ' + (r?.message || '未知错误'));
    }
  }

  // ============================================================
  // Dashboard — Overview
  // ============================================================
  async function loadDashboard() {
    await refreshDashboardStatus(true);
  }

  async function refreshDashboardStatus(includeConnectivity) {
    // 并行拉 audit，避免 renderUpdatePanel 二次往返
    var auditPromise = _lastAuditSnapshot ? Promise.resolve(_lastAuditSnapshot) : refreshAuditSnapshot();
    var [status, inst, ev, up] = await Promise.all([
      api('/api/inst/market/status'),
      api('/api/inst/institutions'),
      api('/api/inst/events?limit=1'),
      api('/api/inst/update/status'),
    ]);
    await auditPromise;
    if (status) {
      el('statRaw').className = 'stat-value'; el('statRaw').textContent = fmt(status.total_records);
      el('statStocks').className = 'stat-value'; el('statStocks').textContent = fmt(status.matched_stocks != null ? status.matched_stocks : status.total_stocks);
      el('statLatest').className = 'stat-value'; el('statLatest').textContent = fmtDate(status.latest_notice_date);
    }
    if (inst?.data) { el('statInst').className = 'stat-value'; el('statInst').textContent = inst.data.filter(i => i.enabled && !i.blacklisted).length; }
    if (ev) { el('statEvents').className = 'stat-value'; el('statEvents').textContent = fmt(ev.total); }
    // 先渲染 step grid（不等 audit），再异步补充审计信息
    await renderUpdatePanel(up);
    // connectivity 异步加载，不阻塞主流程
    if (includeConnectivity !== false) checkNetwork();
  }

  async function refreshWorkbenchStatus() {
    var btn = el('btnRefreshStatus');
    if (!btn) return;
    var originalText = btn.textContent;
    btn.disabled = true;
    btn.textContent = '刷新中...';
    try {
      await refreshDashboardStatus(false);
    } finally {
      btn.disabled = false;
      btn.textContent = originalText;
    }
  }

  // ============================================================
  // Research — Cards & List
  // ============================================================
  async function loadResearch() {
    var [profiles, inst] = await Promise.all([api('/api/inst/profiles'), api('/api/inst/institutions')]);

    var types = [...new Set((inst?.data || []).map(i => i.type).filter(Boolean))];
    var filterEl = el('instTypeFilter');
    filterEl.innerHTML = typeTag('all', '全部') + types.map(t => typeTag(t)).join('');
    filterEl.querySelectorAll('.type-tag').forEach(tag => {
      tag.addEventListener('click', () => {
        filterEl.querySelectorAll('.type-tag').forEach(t => t.classList.remove('active'));
        tag.classList.add('active');
        renderInstList(profiles?.data || [], tag.dataset.type);
      });
    });
    filterEl.querySelector('.type-tag')?.classList.add('active');
    renderInstList(profiles?.data || [], 'all');
  }

  function buildReturnsSvg(gains, width, height) {
    if (!gains || gains.length < 2) return '<div class="muted" style="height:' + height + 'px;display:flex;align-items:center;justify-content:center;font-size:11px">数据不足</div>';
    var vals = gains.map(function (g) { return g.gain_30d || 0; });
    // 数据点过多时采样，保持曲线平滑
    if (vals.length > 60) {
      var step = Math.ceil(vals.length / 60), sampled = [];
      for (var si = 0; si < vals.length; si += step) {
        var chunk = vals.slice(si, Math.min(si + step, vals.length));
        sampled.push(chunk.reduce(function (a, b) { return a + b }, 0) / chunk.length);
      }
      vals = sampled;
    }
    var mn = Math.min.apply(null, vals), mx = Math.max.apply(null, vals);
    if (mx === mn) { mx = mn + 1; }
    var pad = 4, w = width - pad * 2, h = height - pad * 2;
    // 生成坐标点
    var coords = vals.map(function (v, i) {
      return { x: pad + i / (vals.length - 1) * w, y: pad + (1 - (v - mn) / (mx - mn)) * h };
    });
    // 贝塞尔平滑曲线
    var pathD = 'M ' + coords[0].x.toFixed(1) + ' ' + coords[0].y.toFixed(1);
    for (var ci = 1; ci < coords.length; ci++) {
      var prev = coords[ci - 1], curr = coords[ci];
      var cpx = (prev.x + curr.x) / 2;
      pathD += ' C ' + cpx.toFixed(1) + ' ' + prev.y.toFixed(1) + ', ' + cpx.toFixed(1) + ' ' + curr.y.toFixed(1) + ', ' + curr.x.toFixed(1) + ' ' + curr.y.toFixed(1);
    }
    // 零线
    var zeroY = (pad + (1 - (0 - mn) / (mx - mn)) * h).toFixed(1);
    // 最大最小点
    var maxIdx = 0, minIdx = 0;
    vals.forEach(function (v, i) { if (v > vals[maxIdx]) maxIdx = i; if (v < vals[minIdx]) minIdx = i; });

    return '<svg viewBox="0 0 ' + width + ' ' + height + '" style="width:100%;height:' + height + 'px">' +
      '<line x1="' + pad + '" y1="' + zeroY + '" x2="' + (width - pad) + '" y2="' + zeroY + '" stroke="#e2e8f0" stroke-dasharray="3"/>' +
      '<path d="' + pathD + '" fill="none" stroke="#3b82f6" stroke-width="1.5"/>' +
      '<circle cx="' + coords[maxIdx].x.toFixed(1) + '" cy="' + coords[maxIdx].y.toFixed(1) + '" r="3" fill="#10b981"/>' +
      '<text x="' + (coords[maxIdx].x + 4).toFixed(1) + '" y="' + (coords[maxIdx].y - 3).toFixed(1) + '" font-size="9" fill="#10b981">+' + vals[maxIdx].toFixed(1) + '%</text>' +
      '<circle cx="' + coords[minIdx].x.toFixed(1) + '" cy="' + coords[minIdx].y.toFixed(1) + '" r="3" fill="#ef4444"/>' +
      '<text x="' + (coords[minIdx].x + 4).toFixed(1) + '" y="' + (coords[minIdx].y + 10).toFixed(1) + '" font-size="9" fill="#ef4444">' + vals[minIdx].toFixed(1) + '%</text>' +
      '</svg>';
  }

  function renderCards(data, tf) {
    var d = tf === 'all' ? data : data.filter(p => p.inst_type === tf);
    var c = el('instCardsContainer');
    if (!d.length) { c.innerHTML = '<div class="muted">暂无机构画像数据。</div>'; return; }
    c.innerHTML = d.map(p =>
      '<div class="inst-card"><div class="inst-card-head">' +
      '<span class="inst-card-name clickable-name" onclick="event.stopPropagation();App.toggleInstDetail(\'' + esc(p.institution_id) + '\',this)">' + esc(p.display_name || p.institution_name || '') + '</span>' +
      (p.quality_score != null ? '<span style="font-size:11px;background:#eef2ff;color:#4f46e5;padding:2px 8px;border-radius:10px;margin-left:6px;font-weight:600">实力 ' + Number(p.quality_score).toFixed(1) + '</span>' : '') +
      (p.followability_score != null ? '<span style="font-size:11px;background:#ecfdf5;color:#059669;padding:2px 8px;border-radius:10px;margin-left:6px;font-weight:600">可跟 ' + Number(p.followability_score).toFixed(1) + '</span>' : '') +
      typeTag(p.inst_type) + '</div>' +
      '<div class="inst-card-chart" id="chart-' + esc(p.institution_id) + '" style="height:64px;margin:6px 0;border:1px solid #f1f5f9;border-radius:6px;padding:4px;background:#fafbfc"><div class="muted" style="font-size:11px;text-align:center;line-height:56px">加载中...</div></div>' +
      '<div class="inst-card-metrics">' +
      metric('历史胜率', pct(p.total_win_rate)) +
      metric('30日胜率', pct(p.win_rate_30d)) +
      metric('60日胜率', pct(p.win_rate_60d)) +
      metric('最大回撤', p.median_max_drawdown_30d != null ? '-' + p.median_max_drawdown_30d.toFixed(1) + '%' : '-') +
      metric('持仓股票', (p.current_stock_count || 0) + '只') +
      metric('持仓资金', compactNum(p.current_total_cap)) +
      '<div class="metric" style="display:flex;gap:4px;align-items:center">' +
      (p.recent_new_entry_count ? evTag('new_entry') + ' ' + p.recent_new_entry_count : '') +
      (p.recent_increase_count ? ' ' + evTag('increase') + ' ' + p.recent_increase_count : '') +
      (p.recent_exit_count ? ' ' + evTag('exit') + ' ' + p.recent_exit_count : '') +
      '</div>' +
      '</div></div>'
    ).join('');
    // 异步加载收益曲线
    d.forEach(function (p) {
      api('/api/inst/profiles/returns-history/' + encodeURIComponent(p.institution_id)).then(function (r) {
        var chartEl = document.getElementById('chart-' + p.institution_id);
        if (chartEl && r?.ok && r.data?.length) {
          chartEl.innerHTML = buildReturnsSvg(r.data, 280, 56);
        } else if (chartEl) {
          chartEl.innerHTML = '<div class="muted" style="font-size:11px;text-align:center;line-height:56px">暂无收益数据</div>';
        }
      });
    });
  }

  // Phase 3: 机构列表维度切换
  var _instListDim = 'overview';
  var _instListData = [];
  function renderInstList(data, tf) {
    _instListData = tf === 'all' ? data : data.filter(function (p) { return p.inst_type === tf });
    var c = el('instListContainer');
    // 维度切换栏
    var dims = [
      { id: 'overview', label: '概览' },
      { id: 'returns', label: '收益' },
      { id: 'risk', label: '风险与行业' }
    ];
    var dimBar = '<div style="display:flex;gap:6px;margin-bottom:8px">' +
      dims.map(function (dim) {
        return '<button class="btn-sm ' + (dim.id === _instListDim ? 'primary' : '') + '" onclick="App.switchInstDim(\'' + dim.id + '\')" style="font-size:12px">' + dim.label + '</button>';
      }).join('') + '</div>';

    var head, row;
    if (_instListDim === 'overview') {
      head = '<tr><th>机构</th><th>类型</th><th>实力分</th><th>可跟分</th><th>置信</th><th>胜率</th><th>持仓</th><th>资金</th><th>公告日</th><th>距今</th></tr>';
      row = function (p) {
        var confBadge = p.score_confidence === 'high' ? '<span style="color:#10b981;font-size:10px">高</span>' :
          p.score_confidence === 'medium' ? '<span style="color:#f59e0b;font-size:10px">中</span>' :
            p.score_confidence === 'low' ? '<span style="color:#ef4444;font-size:10px">低</span>' : '-';
        return '<td><b class="clickable-name" onclick="App.toggleInstDetail(\'' + esc(p.institution_id) + '\',this)">' + esc(p.display_name || p.institution_name || '') + '</b></td><td>' + typeTag(p.inst_type) + '</td><td>' + (p.quality_score != null ? Number(p.quality_score).toFixed(1) : '-') + '</td><td>' + (p.followability_score != null ? Number(p.followability_score).toFixed(1) : '-') + '</td><td>' + confBadge + '</td><td>' + pct(p.win_rate_30d) + '</td><td>' + (p.current_stock_count || 0) + '</td><td>' + compactNum(p.current_total_cap) + '</td><td>' + fmtDate(p.latest_notice_date) + '</td><td>' + (p.latest_notice_date ? daysAgo(p.latest_notice_date) : '-') + '</td>';
      };
    } else if (_instListDim === 'returns') {
      head = '<tr><th>机构</th><th>买入事件</th><th>30日胜率</th><th>60日胜率</th><th>120日胜率</th><th>30日均收</th><th>60日均收</th><th>120日均收</th><th>依据</th></tr>';
      row = function (p) {
        var basisTag = p.score_basis === 'buy' ? '<span style="color:#3b82f6;font-size:10px">买入</span>' : '<span style="color:#94a3b8;font-size:10px">全事件</span>';
        return '<td><b class="clickable-name" onclick="App.toggleInstDetail(\'' + esc(p.institution_id) + '\',this)">' + esc(p.display_name || p.institution_name || '') + '</b></td><td>' + (p.buy_event_count || p.total_events || 0) + '</td><td>' + pct(p.buy_win_rate_30d || p.win_rate_30d) + '</td><td>' + pct(p.buy_win_rate_60d || p.win_rate_60d) + '</td><td>' + pct(p.buy_win_rate_120d || p.win_rate_90d) + '</td><td>' + fmtGain(p.buy_avg_gain_30d || p.avg_gain_30d) + '</td><td>' + fmtGain(p.buy_avg_gain_60d || p.avg_gain_60d) + '</td><td>' + fmtGain(p.buy_avg_gain_120d || p.avg_gain_120d) + '</td><td>' + basisTag + '</td>';
      };
    } else {
      head = '<tr><th>机构</th><th>回撤30d</th><th>回撤60d</th><th>主要行业</th><th>优势行业</th><th>集中度</th><th>完整性</th></tr>';
      row = function (p) {
        var dcTag = p.data_completeness === 'partial' ? '<span style="color:#f59e0b;font-size:10px" title="收益或行业数据不完整">部分</span>' : '<span style="color:#10b981;font-size:10px">完整</span>';
        return '<td><b class="clickable-name" onclick="App.toggleInstDetail(\'' + esc(p.institution_id) + '\',this)">' + esc(p.display_name || p.institution_name || '') + '</b></td><td>' + (p.buy_median_max_drawdown_30d != null ? '-' + Number(p.buy_median_max_drawdown_30d).toFixed(1) + '%' : (p.median_max_drawdown_30d != null ? '-' + p.median_max_drawdown_30d.toFixed(1) + '%' : '-')) + '</td><td>' + (p.buy_median_max_drawdown_60d != null ? '-' + Number(p.buy_median_max_drawdown_60d).toFixed(1) + '%' : (p.median_max_drawdown_60d != null ? '-' + p.median_max_drawdown_60d.toFixed(1) + '%' : '-')) + '</td><td>' + esc(p.main_industry_1 || '-') + '</td><td>' + esc(p.best_industry_1 || '-') + '</td><td>' + (p.concentration != null ? p.concentration + '%' : '-') + '</td><td>' + dcTag + '</td>';
      };
    }

    c.innerHTML = dimBar + '<table class="data-table data-table-cards"><thead>' + head + '</thead><tbody>' +
      (_instListData.length ? _instListData.map(function (p, i) { return '<tr data-inst-idx="' + i + '">' + row(p) + '</tr>' }).join('') : '<tr><td class="empty-row" colspan="11">暂无数据</td></tr>') +
      '</tbody></table>';
    scheduleSortableTables('instListContainer');
  }
  function switchInstDim(dim) { _instListDim = dim; renderInstList(_instListData, 'all'); }
  function filterInstList() {
    var keyword = (el('instSearch')?.value || '').trim().toLowerCase();
    if (!keyword) { renderInstList(_instListData, 'all'); return; }
    var filtered = _instListData.filter(function (p) {
      return [p.institution_name, p.display_name, p.inst_type, p.institution_id]
        .some(function (v) { return String(v || '').toLowerCase().includes(keyword); });
    });
    var c = el('instListContainer');
    var rows = c?.querySelectorAll('tbody tr');
    if (!rows) return;
    rows.forEach(function (tr) {
      var text = tr.textContent.toLowerCase();
      tr.style.display = text.includes(keyword) ? '' : 'none';
    });
  }
  function daysAgo(dateStr) {
    if (!dateStr) return '-';
    var s = String(dateStr).replace(/[^0-9]/g, '');
    if (s.length !== 8) return '-';
    var d = new Date(s.substring(0, 4) + '-' + s.substring(4, 6) + '-' + s.substring(6, 8));
    return isNaN(d) ? '-' : Math.floor((new Date() - d) / 86400000) + '天';
  }

  // ============================================================
  // Stocks — Phase 4: 维度切换
  // ============================================================
  var _stockListData = [];
  var _stockListLoadedAt = 0;
  var _stockListLoadingPromise = null;
  var _stockListRenderSeq = 0;
  var _stockSearchTimer = null;
  var _industryViewData = [];
  var _industryViewSummary = null;
  var _stockValidationSector = '';
  function activeStockSubtab() {
    return document.querySelector('.stock-tabs .tab-btn.active')?.dataset.stab || 'list';
  }
  function setStockSearchContext(stab) {
    var search = el('stockSearch');
    var filterArea = el('stockFilterArea');
    if (!search) return;
    var mode = stab || 'list';
    search.placeholder = mode === 'industry'
      ? '搜索行业名称/代码/候选股'
      : mode === 'list'
        ? '搜索股票代码/名称/行业'
        : '当前子页不支持顶部搜索';
    if (filterArea) {
      if (mode === 'industry') filterArea.innerHTML = '<div class="muted" style="font-size:12px">行业背景加载中...</div>';
      else if (mode === 'list') filterArea.innerHTML = '';
      else filterArea.innerHTML = '';
    }
  }
  async function loadActiveStockSubtab() {
    var stab = activeStockSubtab();
    setStockSearchContext(stab);
    if (stab === 'watchlist') return loadWatchlist();
    if (stab === 'industry') return loadIndustryView();
    if (stab === 'validation') return loadStockValidation();
    if (stab === 'scorecard') return loadStockScorecard();
    if (stab === 'exclusions') return loadExclusions();
    return loadStockList();
  }
  async function loadStockView() {
    await loadActiveStockSubtab();
  }
  async function loadStockList(force) {
    var now = Date.now();
    if (!force && _stockListData.length && (now - _stockListLoadedAt) < 15000) {
      renderStockList();
      return;
    }
    if (_stockListLoadingPromise) {
      await _stockListLoadingPromise;
      renderStockList();
      return;
    }
    _stockListLoadingPromise = (async function () {
      var r = await api('/api/inst/stock-trends');
      _stockListData = r?.data || [];
      // 并行加载选股结果、板块动量和双重确认，再合并到股票列表
      try {
        var [screenR, sectorR, dualConfirmR] = await Promise.all([
          api('/api/screening/results?limit=5000'),
          api('/api/screening/sector-momentum'),
          api('/api/screening/dual-confirm')
        ]);
        if (screenR?.ok && screenR.data) {
          var screenMap = {};
          screenR.data.forEach(function (s) { screenMap[s.stock_code] = s; });
          _stockListData.forEach(function (s) { s._screen = screenMap[s.stock_code] || null; });
        }
        if (sectorR?.ok && sectorR.data) {
          var sectorMap = {};
          sectorR.data.forEach(function (s) { sectorMap[s.sector_name] = s; });
          _stockListData.forEach(function (s) {
            var ind = s.sw_level1 || s.setup_industry_name;
            var sm = ind ? sectorMap[ind] : null;
            s._sector_trend = sm ? sm.trend_state : null;
          });
        }
        if (dualConfirmR?.ok && dualConfirmR.data) {
          var dualMap = {};
          dualConfirmR.data.forEach(function (d) { if (d.dual_confirm) dualMap[d.stock_code] = true; });
          _stockListData.forEach(function (s) { s._dual_confirm = dualMap[s.stock_code] || false; });
        }
      } catch (e) { }
      _stockListData.forEach(decorateStockSearchBlob);
      _stockListLoadedAt = Date.now();
    })();
    try {
      await _stockListLoadingPromise;
    } finally {
      _stockListLoadingPromise = null;
    }
    renderStockList();
  }
  var _stockFilterSignal = 'all';
  var _stockFilterGate = 'all';

  function renderStockFilters() {
    var signals = [
      { key: 'all', label: '全部' },
      { key: 'a1', label: 'A1' },
      { key: 'a2', label: 'A2' },
      { key: 'a3', label: 'A3' },
      { key: 'a45', label: 'A4/A5' },
      { key: 'none', label: '无信号' }
    ];
    var gates = [
      { key: 'all', label: '全部' },
      { key: 'follow', label: '可跟' },
      { key: 'watch', label: '关注' },
      { key: 'observe', label: '观察' },
      { key: 'avoid', label: '回避' }
    ];
    function chip(group, key, label, active) {
      return '<span class="type-tag stock-filter-chip' + (active ? ' active' : '') + '" data-filter-group="' + group + '" data-filter-key="' + key + '">' + label + '</span>';
    }
    return '<div class="stock-filter-bar">' +
      '<div class="stock-filter-group"><span class="stock-filter-label-pill">信号</span>' + signals.map(function (f) { return chip('signal', f.key, f.label, f.key === _stockFilterSignal) }).join('') + '</div>' +
      '<div class="stock-filter-group"><span class="stock-filter-label-pill">执行</span>' + gates.map(function (f) { return chip('gate', f.key, f.label, f.key === _stockFilterGate) }).join('') + '</div>' +
      '</div>';
  }

  function bindStockFilters() {
    var area = el('stockFilterArea');
    if (!area) return;
    area.querySelectorAll('.stock-filter-chip').forEach(function (chip) {
      chip.addEventListener('click', function () {
        var group = chip.dataset.filterGroup;
        var key = chip.dataset.filterKey;
        if (group === 'signal') _stockFilterSignal = key;
        if (group === 'gate') _stockFilterGate = key;
        chip.closest('.stock-filter-group').querySelectorAll('.stock-filter-chip').forEach(function (c) { c.classList.remove('active') });
        chip.classList.add('active');
        applyStockFilters();
      });
    });
  }

  function matchSignalFilter(s) {
    if (_stockFilterSignal === 'all') return true;
    if (_stockFilterSignal === 'none') return !s.setup_tag;
    if (_stockFilterSignal === 'a1') return s.setup_priority === 1;
    if (_stockFilterSignal === 'a2') return s.setup_priority === 2;
    if (_stockFilterSignal === 'a3') return s.setup_priority === 3;
    if (_stockFilterSignal === 'a45') return s.setup_priority >= 4;
    return true;
  }

  function matchGateFilter(s) {
    // 与显示列共用同一份 stockGateInfo 解析，杜绝「各算各的」
    if (_stockFilterGate === 'all') return true;
    return stockGateInfo(s).key === _stockFilterGate;
  }

  function decorateStockSearchBlob(s) {
    if (!s) return s;
    s._search_blob = [
      s.stock_code,
      s.stock_name,
      s.sw_level1,
      s.sw_level2,
      s.sw_level3,
      s.setup_industry_name,
      s.stock_archetype,
      s.priority_pool,
      s.display_inst_name,
      s.setup_inst_name
    ].map(function (v) {
      return String(v || '').toLowerCase();
    }).join(' ');
    return s;
  }

  function applyStockFilters() {
    var keyword = ((el('stockSearch')?.value) || '').trim().toLowerCase();
    var rows = el('stockListContainer')?.querySelectorAll('tbody tr[data-stock-idx]');
    if (!rows) return;
    var data = _stockListData || [];
    rows.forEach(function (tr) {
      var idx = parseInt(tr.dataset.stockIdx);
      var s = data[idx];
      if (!s) { tr.style.display = 'none'; return; }
      var show = matchSignalFilter(s) && matchGateFilter(s);
      if (show && keyword) show = String(s._search_blob || '').includes(keyword);
      tr.style.display = show ? '' : 'none';
    });
  }

  function trendStateMeta(state) {
    return {
      bullish: { label: '看多', bg: '#dcfce7', fg: '#166534', border: '#bbf7d0' },
      recovering: { label: '回升', bg: '#dbeafe', fg: '#1d4ed8', border: '#bfdbfe' },
      consolidating: { label: '震荡', bg: '#f8fafc', fg: '#475569', border: '#e2e8f0' },
      weakening: { label: '转弱', bg: '#fef3c7', fg: '#b45309', border: '#fde68a' },
      bearish: { label: '看空', bg: '#fee2e2', fg: '#b91c1c', border: '#fecaca' }
    }[state || ''] || { label: state || '未判定', bg: '#f8fafc', fg: '#64748b', border: '#e2e8f0' };
  }

  function trendStateTag(state, macdCross) {
    var meta = trendStateMeta(state);
    var label = meta.label + (macdCross ? ' · MACD金叉' : '');
    return '<span class="industry-trend-tag" style="background:' + meta.bg + ';color:' + meta.fg + ';border-color:' + meta.border + '">' + esc(label) + '</span>';
  }

  function industryRotationTag(bucket, score) {
    var meta = {
      leader: { label: '轮动前排', bg: '#dcfce7', fg: '#166534', border: '#bbf7d0' },
      neutral: { label: '中性观察', bg: '#eff6ff', fg: '#1d4ed8', border: '#bfdbfe' },
      blacklist: { label: '回避名单', bg: '#fee2e2', fg: '#b91c1c', border: '#fecaca' }
    }[bucket || 'neutral'] || { label: bucket || '观察', bg: '#f8fafc', fg: '#475569', border: '#e2e8f0' };
    var scoreText = score != null ? ' · ' + scoreNum(score) : '';
    return '<span class="industry-trend-tag" style="background:' + meta.bg + ';color:' + meta.fg + ';border-color:' + meta.border + '">' + esc(meta.label + scoreText) + '</span>';
  }

  function industryMiniMetric(label, value, sub) {
    return '<div class="industry-mini-metric">' +
      '<div class="industry-mini-label">' + esc(label) + '</div>' +
      '<div class="industry-mini-value">' + value + '</div>' +
      (sub ? '<div class="industry-mini-sub">' + esc(sub) + '</div>' : '') +
      '</div>';
  }

  function industryPoolPill(label, value, tone) {
    var palette = {
      a: { bg: '#dcfce7', fg: '#166534' },
      b: { bg: '#dbeafe', fg: '#1d4ed8' },
      c: { bg: '#fef3c7', fg: '#b45309' },
      d: { bg: '#fee2e2', fg: '#b91c1c' }
    }[tone || 'c'] || { bg: '#f8fafc', fg: '#475569' };
    return '<span class="industry-pool-pill" style="background:' + palette.bg + ';color:' + palette.fg + '">' + esc(label) + ' ' + fmt(value || 0) + '</span>';
  }

  function renderIndustrySummaryBar(summary) {
    if (!summary) return '';
    return '<div class="stock-filter-bar industry-summary-bar">' +
      '<div class="stock-filter-group"><span class="stock-filter-label-pill">行业背景</span>' +
      '<span class="industry-summary-chip">行业 ' + fmt(summary.sector_count || 0) + '</span>' +
      '<span class="industry-summary-chip">多头/回升 ' + fmt(summary.positive_trend_count || 0) + '</span>' +
      '<span class="industry-summary-chip">A池 ' + fmt(summary.a_pool_total || 0) + '</span>' +
      '<span class="industry-summary-chip">Setup ' + fmt(summary.setup_total || 0) + '</span>' +
      '<span class="industry-summary-chip">双确 ' + fmt(summary.dual_confirm_total || 0) + '</span>' +
      (summary.strongest_sector ? '<span class="industry-summary-chip">最强 ' + esc(summary.strongest_sector) + '</span>' : '') +
      '</div>' +
      '</div>';
  }

  function renderIndustryTopStocks(stocks) {
    if (!stocks || !stocks.length) return '<div class="muted" style="font-size:12px">暂无候选股票。</div>';
    return '<div class="industry-top-stock-list">' + stocks.map(function (stock) {
      return '<div class="industry-top-stock">' +
        '<div class="industry-top-stock-head">' +
        '<div class="industry-top-stock-name">' + stockCell(stock.stock_code, stock.stock_name) + '</div>' +
        '<div class="industry-top-stock-tags">' +
        priorityPoolTag(stock.priority_pool) +
        '<span class="industry-top-stock-score">综合 ' + scoreNum(stock.composite_priority_score) + '</span>' +
        '</div>' +
        '</div>' +
        '<div class="industry-top-stock-sub">' +
        esc((stock.stock_archetype || '待分类') + ' · 质量 ' + scoreNum(stock.company_quality_score) + ' · 阶段 ' + scoreNum(stock.stage_score)) +
        (stock.setup_tag ? '<span class="industry-top-stock-setup">Setup</span>' : '') +
        '</div>' +
        '</div>';
    }).join('') + '</div>';
  }

  function industryFeedbackCard(item) {
    var currentSampleCount = Number(item.feedback_20d_count || 0);
    var snapshotSampleCount = Number(item.snapshot_feedback_30d_count || 0);
    var snapshotAnySampleCount =
      Number(item.snapshot_feedback_10d_count || 0) +
      Number(item.snapshot_feedback_30d_count || 0) +
      Number(item.snapshot_feedback_60d_count || 0);
    var snapshotPendingText = (Number(item.snapshot_date_count || 0) > 0 || Number(item.snapshot_scored_count || 0) > 0)
      ? (
        '快照样本仍在积累中'
        + (Number(item.snapshot_date_count || 0) > 0 ? ' · 快照日 ' + fmt(item.snapshot_date_count || 0) : '')
        + (Number(item.snapshot_scored_date_count || 0) > 0 ? ' · 评分日 ' + fmt(item.snapshot_scored_date_count || 0) : '')
        + (item.snapshot_last_date ? ' · 最新 ' + fmtDate(item.snapshot_last_date) : '')
      )
      : '';
    if (!currentSampleCount && !snapshotAnySampleCount) {
      return '<div class="industry-feedback-card">' +
        '<div class="industry-feedback-title">近期反馈</div>' +
        '<div class="industry-feedback-empty">' + esc(snapshotPendingText || '当前行业暂无可用的当前反馈或快照后验样本。') + '</div>' +
        '</div>';
    }
    var currentBlock = currentSampleCount
      ? '<div class="industry-feedback-block">' +
      '<div class="industry-feedback-head">' +
      '<div class="industry-feedback-title">当前反馈</div>' +
      '<div class="industry-feedback-note">当前池子近 20 日价格反馈，不当作严格前瞻回放</div>' +
      '</div>' +
      '<div class="industry-feedback-grid">' +
      industryMiniMetric('全候选20日', fmtGain(item.avg_price_20d_pct), '胜率 ' + pct(item.win_rate_20d) + ' · 样本 ' + fmt(currentSampleCount)) +
      industryMiniMetric('A/B池20日', fmtGain(item.ab_avg_price_20d_pct), '胜率 ' + pct(item.ab_win_rate_20d) + ' · 样本 ' + fmt(item.ab_feedback_20d_count || 0)) +
      industryMiniMetric('A池20日', fmtGain(item.a_avg_price_20d_pct), '胜率 ' + pct(item.a_win_rate_20d) + ' · 样本 ' + fmt(item.a_feedback_20d_count || 0)) +
      industryMiniMetric('A池超额', fmtGain(
        item.a_avg_price_20d_pct != null && item.avg_price_20d_pct != null
          ? Number(item.a_avg_price_20d_pct) - Number(item.avg_price_20d_pct)
          : null
      ), 'A池相对全候选的均值差') +
      '</div>' +
      '</div>'
      : '<div class="industry-feedback-block">' +
      '<div class="industry-feedback-head">' +
      '<div class="industry-feedback-title">当前反馈</div>' +
      '<div class="industry-feedback-note">当前池子近 20 日价格反馈，不当作严格前瞻回放</div>' +
      '</div>' +
      '<div class="industry-feedback-empty">当前行业暂无可用的 20 日反馈样本。</div>' +
      '</div>';
    var snapshotBlock = snapshotSampleCount
      ? '<div class="industry-feedback-block industry-feedback-block-historical">' +
      '<div class="industry-feedback-head">' +
      '<div class="industry-feedback-title">快照后验</div>' +
      '<div class="industry-feedback-note">基于 Setup 快照的成熟 30 日后验，更接近前瞻口径</div>' +
      '</div>' +
      '<div class="industry-feedback-grid">' +
      industryMiniMetric('全候选30日', fmtGain(item.snapshot_avg_gain_30d), '胜率 ' + pct(item.snapshot_win_rate_30d) + ' · 样本 ' + fmt(snapshotSampleCount)) +
      industryMiniMetric('A/B池30日', fmtGain(item.snapshot_ab_avg_gain_30d), '胜率 ' + pct(item.snapshot_ab_win_rate_30d) + ' · 样本 ' + fmt(item.snapshot_ab_feedback_30d_count || 0)) +
      industryMiniMetric('A池30日', fmtGain(item.snapshot_a_avg_gain_30d), '胜率 ' + pct(item.snapshot_a_win_rate_30d) + ' · 样本 ' + fmt(item.snapshot_a_feedback_30d_count || 0)) +
      industryMiniMetric('A池超额', fmtGain(
        item.snapshot_a_avg_gain_30d != null && item.snapshot_avg_gain_30d != null
          ? Number(item.snapshot_a_avg_gain_30d) - Number(item.snapshot_avg_gain_30d)
          : null
      ), '快照A池相对全候选的均值差 · 覆盖日 ' + fmt(item.snapshot_scored_date_count || 0)) +
      '</div>' +
      '</div>'
      : '<div class="industry-feedback-block industry-feedback-block-historical">' +
      '<div class="industry-feedback-head">' +
      '<div class="industry-feedback-title">快照后验</div>' +
      '<div class="industry-feedback-note">基于 Setup 快照的成熟 30 日后验，更接近前瞻口径</div>' +
      '</div>' +
      '<div class="industry-feedback-empty">' + esc(snapshotPendingText || '当前行业暂无可用的快照 30 日成熟样本。') + '</div>' +
      '</div>';
    var historyBlock = snapshotAnySampleCount
      ? '<div class="industry-feedback-block industry-feedback-block-history">' +
      '<div class="industry-feedback-head">' +
      '<div class="industry-feedback-title">快照历史面板</div>' +
      '<div class="industry-feedback-note">按成熟 10/30/60 日后验展开，区分全候选与 A 池</div>' +
      '</div>' +
      '<div class="industry-history-coverage">' +
      '<span class="industry-summary-chip">快照日 ' + fmt(item.snapshot_date_count || 0) + '</span>' +
      '<span class="industry-summary-chip">评分日 ' + fmt(item.snapshot_scored_date_count || 0) + '</span>' +
      '<span class="industry-summary-chip">样本 ' + fmt(item.snapshot_scored_count || 0) + '</span>' +
      '<span class="industry-summary-chip">区间 ' + esc((item.snapshot_first_date || '-') + ' ~ ' + (item.snapshot_last_date || '-')) + '</span>' +
      '</div>' +
      '<div class="industry-history-grid">' +
      industryMiniMetric('全候选10日', fmtGain(item.snapshot_avg_gain_10d), '胜率 ' + pct(item.snapshot_win_rate_10d) + ' · 样本 ' + fmt(item.snapshot_feedback_10d_count || 0)) +
      industryMiniMetric('全候选30日', fmtGain(item.snapshot_avg_gain_30d), '胜率 ' + pct(item.snapshot_win_rate_30d) + ' · 样本 ' + fmt(item.snapshot_feedback_30d_count || 0)) +
      industryMiniMetric('全候选60日', fmtGain(item.snapshot_avg_gain_60d), '胜率 ' + pct(item.snapshot_win_rate_60d) + ' · 样本 ' + fmt(item.snapshot_feedback_60d_count || 0)) +
      industryMiniMetric('A池10日', fmtGain(item.snapshot_a_avg_gain_10d), '胜率 ' + pct(item.snapshot_a_win_rate_10d) + ' · 样本 ' + fmt(item.snapshot_a_feedback_10d_count || 0)) +
      industryMiniMetric('A池30日', fmtGain(item.snapshot_a_avg_gain_30d), '胜率 ' + pct(item.snapshot_a_win_rate_30d) + ' · 样本 ' + fmt(item.snapshot_a_feedback_30d_count || 0)) +
      industryMiniMetric('A池60日', fmtGain(item.snapshot_a_avg_gain_60d), '胜率 ' + pct(item.snapshot_a_win_rate_60d) + ' · 样本 ' + fmt(item.snapshot_a_feedback_60d_count || 0)) +
      '</div>' +
      '</div>'
      : '<div class="industry-feedback-block industry-feedback-block-history">' +
      '<div class="industry-feedback-head">' +
      '<div class="industry-feedback-title">快照历史面板</div>' +
      '<div class="industry-feedback-note">按成熟 10/30/60 日后验展开，区分全候选与 A 池</div>' +
      '</div>' +
      '<div class="industry-feedback-empty">' + esc(snapshotPendingText || '当前行业快照样本还未成熟到可展示 10/30/60 日后验。') + '</div>' +
      '</div>';
    return '<div class="industry-feedback-card">' +
      '<div class="industry-feedback-stack">' +
      currentBlock +
      snapshotBlock +
      historyBlock +
      '</div>' +
      '</div>';
  }

  function industryDistributionBar(title, total, items) {
    total = Number(total || 0);
    if (!total) {
      return '<div class="industry-dist-card">' +
        '<div class="industry-dist-head"><span class="industry-dist-title">' + esc(title) + '</span><span class="industry-dist-total">暂无样本</span></div>' +
        '<div class="industry-dist-empty">当前行业暂无可计算分布。</div>' +
        '</div>';
    }
    return '<div class="industry-dist-card">' +
      '<div class="industry-dist-head"><span class="industry-dist-title">' + esc(title) + '</span><span class="industry-dist-total">样本 ' + fmt(total) + '</span></div>' +
      '<div class="industry-dist-track">' + items.map(function (item) {
        var value = Number(item.value || 0);
        var width = Math.max(value > 0 ? (value / total) * 100 : 0, value > 0 ? 4 : 0);
        return '<span class="industry-dist-segment tone-' + esc(item.tone || 'slate') + '" style="width:' + width.toFixed(2) + '%" title="' + esc(item.label + ' ' + value + ' / ' + total) + '"></span>';
      }).join('') + '</div>' +
      '<div class="industry-dist-legend">' + items.map(function (item) {
        var value = Number(item.value || 0);
        var pct = total ? ((value / total) * 100).toFixed(0) : '0';
        return '<div class="industry-dist-legend-item">' +
          '<span class="industry-dist-dot tone-' + esc(item.tone || 'slate') + '"></span>' +
          '<span class="industry-dist-label">' + esc(item.label) + '</span>' +
          '<span class="industry-dist-value">' + fmt(value) + ' / ' + pct + '%</span>' +
          '</div>';
      }).join('') + '</div>' +
      '</div>';
  }

  function renderIndustryValidationButton(item) {
    if (!item || !item.sector_name) return '';
    return '<button class="btn-sm" type="button" onclick="App.openSectorValidation(' + JSON.stringify(String(item.sector_name)) + ')" style="font-size:11px">验证视角</button>';
  }

  function renderIndustryView(summary, data) {
    var filterArea = el('stockFilterArea');
    if (filterArea) filterArea.innerHTML = renderIndustrySummaryBar(summary);

    var c = el('industryViewContainer');
    if (!c) return;
    if (!data || !data.length) {
      c.innerHTML = '<div class="panel"><div class="muted">暂无行业研究背景数据。</div></div>';
      return;
    }

    var hasSnapshotCoverage = data.some(function (item) { return Number(item.snapshot_date_count || 0) > 0; });
    var noMaturedSnapshotYet =
      Number(summary?.snapshot_feedback_ready_10d_total || 0) === 0 &&
      Number(summary?.snapshot_feedback_ready_total || 0) === 0 &&
      Number(summary?.snapshot_feedback_ready_60d_total || 0) === 0;

    var summaryHtml = '<div class="industry-overview-summary">' +
      industryMiniMetric('覆盖行业', fmt(summary?.sector_count || data.length), '可按顶部搜索过滤') +
      industryMiniMetric('多头/回升', fmt(summary?.positive_trend_count || 0), '趋势状态为看多/回升') +
      industryMiniMetric('A池候选', fmt(summary?.a_pool_total || 0), '优先池中的候选总数') +
      industryMiniMetric('Setup候选', fmt(summary?.setup_total || 0), '当前命中 Setup 的候选') +
      industryMiniMetric('双重确认', fmt(summary?.dual_confirm_total || 0), '近180天行业双确股票数') +
      industryMiniMetric('反馈样本', fmt(summary?.feedback_ready_total || 0), '已有 20 日价格反馈的候选') +
      industryMiniMetric('快照10日', fmt(summary?.snapshot_feedback_ready_10d_total || 0), '成熟 10 日快照样本') +
      industryMiniMetric('快照30日', fmt(summary?.snapshot_feedback_ready_total || 0), '成熟 30 日快照样本 · 覆盖行业 ' + fmt(summary?.snapshot_feedback_sector_count || 0)) +
      industryMiniMetric('快照60日', fmt(summary?.snapshot_feedback_ready_60d_total || 0), '成熟 60 日快照样本') +
      industryMiniMetric('最强行业', esc(summary?.strongest_sector || '-'), '按行业动量排序') +
      '</div>' +
      (hasSnapshotCoverage && noMaturedSnapshotYet
        ? '<div class="industry-feedback-empty" style="margin-top:10px">快照历史已经开始积累，但当前还没有成熟到 10/30/60 日窗口的行业级样本，这不是链路异常。</div>'
        : '');

    var cardHtml = data.map(function (item, idx) {
      var returnsText = [
        '1月 ' + fmtGain(item.return_1m),
        '3月 ' + fmtGain(item.return_3m),
        '6月 ' + fmtGain(item.return_6m),
        '12月 ' + fmtGain(item.return_12m)
      ].join(' · ');
      var excessText = [
        '1月 ' + fmtGain(item.excess_1m),
        '3月 ' + fmtGain(item.excess_3m),
        '6月 ' + fmtGain(item.excess_6m),
        '12月 ' + fmtGain(item.excess_12m)
      ].join(' · ');
      var distributionHtml = '<div class="industry-distribution-grid">' +
        industryDistributionBar('质量分布', item.candidate_count, [
          { label: '80+', value: item.quality_band_80_plus, tone: 'green' },
          { label: '65-80', value: item.quality_band_65_80, tone: 'blue' },
          { label: '50-65', value: item.quality_band_50_65, tone: 'amber' },
          { label: '<50', value: item.quality_band_below_50, tone: 'red' }
        ]) +
        industryDistributionBar('阶段分布', item.candidate_count, [
          { label: '80+', value: item.stage_band_80_plus, tone: 'green' },
          { label: '60-80', value: item.stage_band_60_80, tone: 'blue' },
          { label: '40-60', value: item.stage_band_40_60, tone: 'amber' },
          { label: '<40', value: item.stage_band_below_40, tone: 'red' }
        ]) +
        industryDistributionBar('综合分布', item.candidate_count, [
          { label: '75+', value: item.composite_band_75_plus, tone: 'green' },
          { label: '60-75', value: item.composite_band_60_75, tone: 'blue' },
          { label: '45-60', value: item.composite_band_45_60, tone: 'amber' },
          { label: '<45', value: item.composite_band_below_45, tone: 'red' }
        ]) +
        '</div>';
      return '<article class="industry-sector-card" data-sector-idx="' + idx + '">' +
        '<div class="industry-sector-head">' +
        '<div>' +
        '<div class="industry-sector-title">' + esc(item.sector_name || '-') + '</div>' +
        '<div class="industry-sector-code">' + esc(item.sector_code || '-') + '</div>' +
        '</div>' +
        '<div class="industry-sector-head-tags">' +
        renderIndustryValidationButton(item) +
        trendStateTag(item.trend_state, item.macd_cross) +
        industryRotationTag(item.rotation_bucket, item.rotation_score) +
        '<span class="industry-momentum-pill">动量 ' + scoreNum(item.momentum_score) + '</span>' +
        '</div>' +
        '</div>' +
        '<div class="industry-sector-returns">' +
        '<div><span class="industry-section-label">行业收益</span>' + returnsText + '</div>' +
        '<div><span class="industry-section-label">相对强弱</span>' + excessText + '</div>' +
        '</div>' +
        '<div class="industry-sector-metrics">' +
        industryMiniMetric('机构活跃', fmt(item.active_institution_count || 0), '当前持仓股 ' + fmt(item.current_stock_count || 0)) +
        industryMiniMetric('新进入', fmt(item.recent_new_entry_count || 0), '涉及股票 ' + fmt(item.recent_new_entry_stock_count || 0)) +
        industryMiniMetric('买入信号', fmt(item.recent_buy_signal_count || 0), '涉及股票 ' + fmt(item.recent_buy_signal_stock_count || 0)) +
        industryMiniMetric('候选股票', fmt(item.candidate_count || 0), 'Setup ' + fmt(item.setup_candidate_count || 0)) +
        industryMiniMetric('质量强', fmt(item.quality_strong_count || 0), '阶段强 ' + fmt(item.stage_strong_count || 0)) +
        industryMiniMetric('轮动分', scoreNum(item.rotation_score), '短排 ' + fmt(item.rotation_rank_1m || 0) + ' · 长排 ' + fmt(item.rotation_rank_3m || 0)) +
        industryMiniMetric('行业顺风', scoreNum(item.avg_tailwind_score), '均综合 ' + scoreNum(item.avg_composite_score)) +
        '</div>' +
        '<div class="industry-pool-strip">' +
        industryPoolPill('A池', item.a_pool_count, 'a') +
        industryPoolPill('B池', item.b_pool_count, 'b') +
        industryPoolPill('C池', item.c_pool_count, 'c') +
        industryPoolPill('D池', item.d_pool_count, 'd') +
        '<span class="industry-pool-note">发现 ' + scoreNum(item.avg_discovery_score) + ' · 质量 ' + scoreNum(item.avg_quality_score) + ' · 阶段 ' + scoreNum(item.avg_stage_score) + '</span>' +
        '</div>' +
        '<div class="industry-card-section">' +
        industryFeedbackCard(item) +
        '</div>' +
        '<div class="industry-card-section">' +
        '<div class="industry-card-section-title">分布结构</div>' +
        distributionHtml +
        '</div>' +
        '<div class="industry-card-section">' +
        '<div class="industry-card-section-title">前排候选</div>' +
        renderIndustryTopStocks(item.top_stocks || []) +
        '</div>' +
        '</article>';
    }).join('');

    c.innerHTML = '<div class="industry-view-wrap">' +
      summaryHtml +
      '<div id="industryEmptyState" class="industry-empty" style="display:none">没有匹配的行业。</div>' +
      '<div class="industry-sector-grid">' + cardHtml + '</div>' +
      '</div>';
    applyIndustryFilters();
  }

  async function loadIndustryView() {
    var c = el('industryViewContainer');
    if (!c) return;
    c.innerHTML = '<div class="panel"><div class="muted">加载行业研究背景中...</div></div>';
    var r = await apiCached('/api/screening/industry-overview?topn=4', SHORT_CACHE_TTL_MS);
    _industryViewData = r?.data || [];
    _industryViewSummary = r?.summary || null;
    renderIndustryView(_industryViewSummary, _industryViewData);
  }

  function applyIndustryFilters() {
    var keyword = ((el('stockSearch')?.value) || '').trim().toLowerCase();
    var cards = el('industryViewContainer')?.querySelectorAll('.industry-sector-card[data-sector-idx]');
    if (!cards) return;
    var visible = 0;
    cards.forEach(function (card) {
      var idx = parseInt(card.dataset.sectorIdx, 10);
      var item = _industryViewData[idx];
      if (!item) {
        card.style.display = 'none';
        return;
      }
      var tokens = [
        item.sector_name,
        item.sector_code,
        item.trend_state,
        item.rotation_bucket
      ];
      (item.top_stocks || []).forEach(function (stock) {
        tokens.push(stock.stock_code, stock.stock_name, stock.stock_archetype);
      });
      var show = !keyword || tokens.some(function (value) {
        return String(value || '').toLowerCase().includes(keyword);
      });
      card.style.display = show ? '' : 'none';
      if (show) visible += 1;
    });
    var empty = el('industryEmptyState');
    if (empty) empty.style.display = visible ? 'none' : '';
  }

  function handleStockSearchInput() {
    if (_stockSearchTimer) clearTimeout(_stockSearchTimer);
    _stockSearchTimer = setTimeout(function () {
      if (activeStockSubtab() === 'industry') applyIndustryFilters();
      else applyStockFilters();
    }, 120);
  }

  function renderStockList() {
    // 筛选栏放到 banner 区
    var filterArea = el('stockFilterArea');
    if (filterArea) { filterArea.innerHTML = renderStockFilters(); bindStockFilters(); }

    var c = el('stockListContainer');
    var emptyCols = 10;
    var colgroup = '<colgroup><col style="width:13%"><col style="width:10%"><col style="width:7%"><col style="width:14%"><col style="width:11%"><col style="width:11%"><col style="width:6%"><col style="width:9%"><col style="width:10%"><col style="width:8%"></colgroup>';
    var head = '<tr><th>股票</th><th>信号</th><th title="股票级执行档：取持仓机构 follow_gate 的最高优先级（与上方筛选、机构页 follow_gate 共用同一口径）">执行档</th><th>来源 / 行业</th><th>报告期</th><th>公告</th><th>综合 / 四分</th><th title="当前持仓机构总数 (其中 follow_gate=可跟 的家数)，来自 mart_current_relationship">机构 (可跟)</th><th>选股</th><th>板块</th></tr>';
    var row = function (s, idx) {
      // 执行档：用单一 resolver（与顶部筛选共用同一份 stockGateInfo）
      var gateHtml = stockGateTag(s);
      var signalHtml = s.setup_tag
        ? setupBadge(s.setup_tag, s.setup_priority, s.setup_confidence)
        : '<span class="muted" style="font-size:11px">' + esc(s.path_state || '-') + '</span>';
      var source = stockSourceName(s);
      var industry = s.setup_industry_name || s.sw_level3 || s.sw_level2 || s.sw_level1 || '';
      var sourceIndustryHtml = '<div style="line-height:1.5"><div style="font-weight:600;font-size:12px">' + esc(source) + '</div>' +
        (industry ? '<div style="font-size:11px;color:#94a3b8">' + esc(industry) + '</div>' : '') + '</div>';
      var reportHtml = fmtDate(s.latest_report_date) + ' ' + daysAgoPill(s.latest_report_date);
      var noticeHtml = fmtDate(s.latest_notice_date) + ' ' + daysAgoPill(s.latest_notice_date);
      var scoreHtml = stockCompositeCell(s);
      // 选股信号标签
      var screenHtml = '';
      if (s._screen) {
        var badges = [];
        if (s._screen.f1_hit) badges.push('<span class="tag tag-sm" style="background:#3b82f6;color:#fff" title="MA突破">F1</span>');
        if (s._screen.f3_hit) badges.push('<span class="tag tag-sm" style="background:#8b5cf6;color:#fff" title="趋势跟踪">F3</span>');
        if (s._screen.f5_hit) badges.push('<span class="tag tag-sm" style="background:#f59e0b;color:#fff" title="MACD金叉">F5</span>');
        screenHtml = badges.length ? badges.join(' ') : '<span class="muted">-</span>';
      } else { screenHtml = '<span class="muted">-</span>'; }
      // 板块动量 + 双重确认
      var sectorHtml = '';
      if (s._dual_confirm) {
        sectorHtml = '<span class="tag tag-sm" style="background:#10b981;color:#fff" title="机构+板块双重确认">双确</span>';
      } else if (s._sector_trend) {
        var tc = s._sector_trend === 'bullish' ? '#10b981' : s._sector_trend === 'recovering' ? '#3b82f6' : s._sector_trend === 'weakening' ? '#f59e0b' : '#94a3b8';
        var trendLabel = { bullish: '\u770b\u591a', recovering: '\u56de\u5347', weakening: '\u8f6c\u5f31', bearish: '\u770b\u7a7a', consolidating: '\u9707\u8361' }[s._sector_trend] || s._sector_trend;
        sectorHtml = '<span style="color:' + tc + ';font-size:11px">' + esc(trendLabel) + '</span>';
      } else { sectorHtml = '<span class="muted">-</span>'; }
      // 「机构 (可跟)」单元格：当前持仓家数 + 其中 follow_gate=follow 的家数
      var holderTotal = (s.holder_total != null) ? s.holder_total : (s.inst_count_t0 || 0);
      var holderFollow = s.holder_follow_count || 0;
      var holderHtml = '<span style="font-size:12px;color:#0f172a">' + holderTotal + '</span>' +
        (holderFollow > 0
          ? ' <span style="font-size:11px;color:#059669;font-weight:600" title="该股票现有持仓机构中 follow_gate=follow 的家数">(' + holderFollow + ')</span>'
          : '');
      return '<tr data-stock-idx="' + idx + '">' +
        '<td>' + stockCell(s.stock_code, s.stock_name) + '</td>' +
        '<td>' + signalHtml + '</td>' +
        '<td>' + gateHtml + '</td>' +
        '<td>' + sourceIndustryHtml + '</td>' +
        '<td>' + reportHtml + '</td>' +
        '<td>' + noticeHtml + '</td>' +
        '<td>' + scoreHtml + '</td>' +
        '<td>' + holderHtml + '</td>' +
        '<td>' + screenHtml + '</td>' +
        '<td>' + sectorHtml + '</td>' +
        '</tr>';
    };
    var data = _stockListData || [];
    if (!data.length) {
      c.innerHTML = '<table class="data-table data-table-compact data-table-cards" style="table-layout:fixed">' + colgroup + '<thead>' + head + '</thead><tbody><tr><td class="empty-row" colspan="' + emptyCols + '">暂无数据</td></tr></tbody></table>';
      return;
    }
    var renderSeq = ++_stockListRenderSeq;
    c.innerHTML =
      '<div id="stockListRenderHint" class="muted" style="font-size:11px;margin-bottom:8px">正在渲染 0 / ' + data.length + ' ...</div>' +
      '<table class="data-table data-table-compact data-table-cards" style="table-layout:fixed">' +
      colgroup + '<thead>' + head + '</thead><tbody></tbody></table>';
    var tbody = c.querySelector('tbody');
    var hint = el('stockListRenderHint');
    var chunkSize = 180;
    var offset = 0;

    function flushChunk() {
      if (renderSeq !== _stockListRenderSeq || !tbody) return;
      var slice = data.slice(offset, Math.min(offset + chunkSize, data.length));
      tbody.insertAdjacentHTML('beforeend', slice.map(function (s, i) {
        return row(s, offset + i);
      }).join(''));
      offset += slice.length;
      if (hint) {
        hint.textContent = offset < data.length
          ? ('正在渲染 ' + offset + ' / ' + data.length + ' ...')
          : ('共 ' + data.length + ' 只股票，已完成渲染');
      }
      if (offset < data.length) {
        requestAnimationFrame(flushChunk);
        return;
      }
      applyStockFilters();
      scheduleSortableTables('stockListContainer');
    }

    requestAnimationFrame(flushChunk);
  }
  function switchStockDim() { renderStockList(); }

  // ============================================================
  // Watchlist
  // ============================================================
  function setupReplayGroupLabel(name) {
    return {
      baseline_all_buy: '全量买入基线',
      setup_hit_all: 'Setup 命中',
      priority_1: 'Setup A1',
      priority_2: 'Setup A2',
      priority_3: 'Setup A3',
      priority_4: 'Setup A4',
      priority_5: 'Setup A5'
    }[name] || String(name || '-');
  }
  function setupValidationGateCell(gate) {
    if (!gate) return '<span class="muted">-</span>';
    if (['follow', 'watch', 'observe', 'avoid'].indexOf(gate) >= 0) return followGateTag(gate, '');
    return esc(String(gate));
  }
  function renderSetupValidationPanel(report) {
    if (!report) return '';
    var forward = report.forward || {};
    var replay = report.replay || {};
    var latest = forward.latest_snapshot || {};
    var overall = forward.overall || {};
    var chain = forward.tracking_chain || {};
    var decision = report.decision || {};
    var insights = report.insights || [];
    var snapshotHistory = forward.snapshot_history || [];
    var priorityGroups = forward.latest_priority_groups || [];
    var gateGroups = forward.latest_gate_groups || [];
    var coreReplayGroups = [];
    if (replay.baseline) coreReplayGroups.push(replay.baseline);
    if (replay.setup_hit) coreReplayGroups.push(replay.setup_hit);
    (replay.priority_groups || []).forEach(function (item) {
      if (['1', '2', '3'].indexOf(String(item.factor_value || '')) >= 0) coreReplayGroups.push(item);
    });
    var setupSnapshotPendingText = snapshotMaturityPendingText(
      {
        total_snapshot_days: forward.total_snapshot_days,
        latest_snapshot_total: latest.total
      },
      {
        matured_10d_count: overall.h10 && overall.h10.matured_count,
        matured_30d_count: overall.h30 && overall.h30.matured_count,
        matured_60d_count: overall.h60 && overall.h60.matured_count
      }
    );
    return '<div class="setup-validation-panel">' +
      '<div class="setup-validation-head">' +
      '<div>' +
      '<div class="setup-validation-title">Setup 验证面板</div>' +
      '<div class="muted" style="font-size:11px;margin-top:4px">把前瞻快照、历史 replay 和评分决策放在同一个判断面里。先确认证据，再决定是否提权。</div>' +
      '</div>' +
      '<div class="setup-validation-badge ' + esc(decision.phase3_status || 'defer') + '">' + esc(decision.phase3_status === 'defer' ? 'Phase 3 暂缓' : 'Phase 3 可推进') + '</div>' +
      '</div>' +
      '<div class="setup-validation-metrics">' +
      metric('最新快照', esc(fmtDate(forward.latest_snapshot_date))) +
      metric('市场最新交易日', esc(fmtDate(chain.market_latest_trade_date))) +
      metric('快照是否最新', chain.snapshot_is_current ? '是' : '否') +
      metric('快照日数', fmt(forward.total_snapshot_days || 0)) +
      metric('最新候选数', fmt(latest.total || 0)) +
      metric('已成熟10日', fmt(overall.h10 && overall.h10.matured_count || 0)) +
      metric('已成熟30日', fmt(overall.h30 && overall.h30.matured_count || 0)) +
      metric('已成熟60日', fmt(overall.h60 && overall.h60.matured_count || 0)) +
      '</div>' +
      '<div class="setup-validation-callout ' + esc(decision.phase3_status || 'defer') + '">' +
      '<div class="setup-validation-callout-title">当前结论</div>' +
      '<div class="setup-validation-callout-text">' + esc(decision.recommended_action || '-') + '</div>' +
      ((decision.reasons || []).length ? '<ul class="setup-validation-list">' +
        decision.reasons.map(function (text) { return '<li>' + esc(text) + '</li>'; }).join('') +
        '</ul>' : '') +
      '</div>' +
      ((insights || []).length ? '<div class="setup-validation-section">' +
        '<div class="setup-validation-section-title">关键发现</div>' +
        '<ul class="setup-validation-list">' +
        insights.map(function (text) { return '<li>' + esc(text) + '</li>'; }).join('') +
        '</ul>' +
        '</div>' : '') +
      '<div class="setup-validation-grid">' +
      '<div class="setup-validation-card">' +
      '<div class="setup-validation-section-title">最新快照按优先级</div>' +
      '<table class="data-table"><thead><tr><th>优先级</th><th>样本</th><th>均综合分</th><th>均Setup分</th><th>成熟30日</th></tr></thead><tbody>' +
      (priorityGroups.length ? priorityGroups.map(function (item) {
        return '<tr><td>' + setupBadge('industry_expert_entry', item.group_value, null) + '</td><td>' + fmt(item.total) + '</td><td>' + scoreNum(item.avg_composite_score) + '</td><td>' + (item.avg_setup_score != null ? Number(item.avg_setup_score).toFixed(1) : '-') + '</td><td>' + fmt(item.h30 && item.h30.matured_count || 0) + '</td></tr>';
      }).join('') : '<tr><td class="empty-row" colspan="5">暂无数据</td></tr>') +
      '</tbody></table>' +
      '</div>' +
      '<div class="setup-validation-card">' +
      '<div class="setup-validation-section-title">最新快照按执行建议</div>' +
      '<table class="data-table"><thead><tr><th>执行</th><th>样本</th><th>均综合分</th><th>均Setup分</th><th>成熟30日</th></tr></thead><tbody>' +
      (gateGroups.length ? gateGroups.map(function (item) {
        return '<tr><td>' + setupValidationGateCell(item.group_value) + '</td><td>' + fmt(item.total) + '</td><td>' + scoreNum(item.avg_composite_score) + '</td><td>' + (item.avg_setup_score != null ? Number(item.avg_setup_score).toFixed(1) : '-') + '</td><td>' + fmt(item.h30 && item.h30.matured_count || 0) + '</td></tr>';
      }).join('') : '<tr><td class="empty-row" colspan="5">暂无数据</td></tr>') +
      '</tbody></table>' +
      '</div>' +
      '</div>' +
      '<div class="setup-validation-section">' +
      '<div class="setup-validation-section-title">历史 replay 对照</div>' +
      '<table class="data-table"><thead><tr><th>组别</th><th>样本</th><th>30日均收益</th><th>30日胜率</th><th>30日回撤</th><th>vs基线</th></tr></thead><tbody>' +
      (coreReplayGroups.length ? coreReplayGroups.map(function (item) {
        var label = item.group_name ? setupReplayGroupLabel(item.group_name) : ('Setup A' + String(item.factor_value || ''));
        return '<tr><td>' + esc(label) + '</td><td>' + fmt(item.sample_count) + '</td><td>' + fmtGain(item.avg_gain_30d) + '</td><td>' + pct(item.win_rate_30d) + '</td><td>' + (item.avg_drawdown_30d != null ? '-' + Number(item.avg_drawdown_30d).toFixed(1) + '%' : '-') + '</td><td>' + fmtGain(item.uplift_vs_baseline_30d) + '</td></tr>';
      }).join('') : '<tr><td class="empty-row" colspan="6">暂无 replay 数据</td></tr>') +
      '</tbody></table>' +
      '</div>' +
      '<div class="setup-validation-section">' +
      '<div class="setup-validation-section-title">执行建议历史表现</div>' +
      '<table class="data-table"><thead><tr><th>执行</th><th>样本</th><th>30日均收益</th><th>30日胜率</th><th>30日回撤</th><th>vs基线</th></tr></thead><tbody>' +
      ((replay.gate_groups || []).length ? replay.gate_groups.map(function (item) {
        return '<tr><td>' + setupValidationGateCell(item.factor_value) + '</td><td>' + fmt(item.sample_count) + '</td><td>' + fmtGain(item.avg_gain_30d) + '</td><td>' + pct(item.win_rate_30d) + '</td><td>' + (item.avg_drawdown_30d != null ? '-' + Number(item.avg_drawdown_30d).toFixed(1) + '%' : '-') + '</td><td>' + fmtGain(item.uplift_vs_baseline_30d) + '</td></tr>';
      }).join('') : '<tr><td class="empty-row" colspan="6">暂无历史 gate 数据</td></tr>') +
      '</tbody></table>' +
      '</div>' +
      '<div class="setup-validation-section">' +
      '<div class="setup-validation-section-title">前瞻快照历史</div>' +
      (setupSnapshotPendingText ? '<div class="validation-overlap-note">' + esc(setupSnapshotPendingText) + '</div>' : '') +
      '<table class="data-table"><thead><tr><th>快照日</th><th>样本</th><th>成熟10日</th><th>成熟30日</th><th>成熟60日</th><th>30日均收益</th><th>30日胜率</th></tr></thead><tbody>' +
      (snapshotHistory.length ? snapshotHistory.map(function (item) {
        return '<tr><td>' + esc(fmtDate(item.snapshot_date)) + '</td><td>' + fmt(item.total) + '</td><td>' + fmt(item.h10 && item.h10.matured_count || 0) + '</td><td>' + fmt(item.h30 && item.h30.matured_count || 0) + '</td><td>' + fmt(item.h60 && item.h60.matured_count || 0) + '</td><td>' + fmtGain(item.h30 && item.h30.avg_gain) + '</td><td>' + pct(item.h30 && item.h30.win_rate) + '</td></tr>';
      }).join('') : '<tr><td class="empty-row" colspan="7">暂无快照历史</td></tr>') +
      '</tbody></table>' +
      '</div>' +
      '</div>';
  }
  async function loadWatchlist() {
    var rs = await Promise.all([
      apiCached('/api/inst/watchlist', SHORT_CACHE_TTL_MS),
      apiCached('/api/inst/candidate-setups', SHORT_CACHE_TTL_MS),
      apiCached('/api/inst/setup-validation/report', SHORT_CACHE_TTL_MS),
      apiCached('/api/inst/setup-tracking/snapshots?limit=80', SHORT_CACHE_TTL_MS)
    ]);
    var r = rs[0], cands = rs[1], validationReport = rs[2], trackingSnapshots = rs[3];
    var c = el('watchlistContainer');
    var sections = [];
    if (validationReport?.data) sections.push(renderSetupValidationPanel(validationReport.data));
    var candHead = '<table class="data-table"><thead><tr><th>股票</th><th>核心判断</th><th>执行</th><th>来源机构</th><th>报告期</th><th>池子 / 综合</th></tr></thead><tbody>';
    if (cands?.data?.length) {
      sections.push('<div style="margin-bottom:18px"><div style="font-size:14px;font-weight:700;margin-bottom:8px">研究候选</div>' +
        candHead + cands.data.map(function (s) {
          return '<tr><td>' + stockCell(s.stock_code, s.stock_name) + '</td><td>' + stockSignalCell(s) + '</td><td>' + stockExecutionCell(s) + '</td><td>' + sourceInstitutionCell(s) + '</td><td>' + stockReportCell(s) + '</td><td>' + stockCompositeCell(s) + '</td></tr>';
        }).join('') + '</tbody></table></div>');
    }
    var head = '<table class="data-table"><thead><tr><th>股票</th><th>入池日期</th><th>入池价</th><th>理由</th><th>当前Setup</th><th>来源</th><th>至今</th><th>最大涨</th><th>最大撤</th><th>状态</th></tr></thead><tbody>';
    if (r?.data?.length) {
      sections.push('<div><div style="font-size:14px;font-weight:700;margin-bottom:8px">手工股票池</div>' +
        head + r.data.map(function (w) {
          return '<tr><td>' + stockCell(w.stock_code, w.stock_name) + '</td><td>' + fmtDate(w.added_date) + '</td><td>' + (w.added_price || '-') + '</td><td>' + esc(w.added_reason || '') + '</td><td>' + setupSummaryCell(w) + '</td><td>' + esc(w.source_institution || '') + '</td><td>' + fmtGain(w.gain_since_added) + '</td><td>' + fmtGain(w.max_gain) + '</td><td>' + (w.max_drawdown != null ? '-' + w.max_drawdown.toFixed(1) + '%' : '-') + '</td><td>' + esc(w.status || '') + '</td></tr>';
        }).join('') + '</tbody></table></div>');
    } else {
      sections.push('<div><div style="font-size:14px;font-weight:700;margin-bottom:8px">手工股票池</div>' +
        head + '<tr><td class=”empty-row” colspan=”10”>暂无股票。</td></tr></tbody></table></div>');
    }
    var trackHead = '<table class="data-table"><thead><tr><th>快照日</th><th>股票</th><th>池子 / 综合</th><th>Setup</th><th>来源机构</th><th>10日</th><th>30日</th><th>60日</th><th>至今</th></tr></thead><tbody>';
    if (trackingSnapshots?.data?.length) {
      sections.push('<div style="margin-top:18px"><div style="font-size:14px;font-weight:700;margin-bottom:8px">最近跟踪快照</div>' +
        trackHead + trackingSnapshots.data.map(function (s) {
          var gain10 = s.matured_10d ? fmtGain(s.gain_10d) : '<span class="muted">待成熟</span>';
          var gain30 = s.matured_30d ? fmtGain(s.gain_30d) : '<span class="muted">待成熟</span>';
          var gain60 = s.matured_60d ? fmtGain(s.gain_60d) : '<span class="muted">待成熟</span>';
          return '<tr><td>' + fmtDate(s.snapshot_date) + '</td><td>' + stockCell(s.stock_code, s.stock_name) + '</td><td>' + stockCompositeCell(s) + '</td><td>' + setupSummaryCell(s) + '</td><td>' + esc(s.setup_inst_name || '-') + '</td><td>' + gain10 + '</td><td>' + gain30 + '</td><td>' + gain60 + '</td><td>' + fmtGain(s.gain_to_now) + '</td></tr>';
        }).join('') + '</tbody></table></div>');
    }
    c.innerHTML = sections.join('');
    scheduleSortableTables('watchlistContainer');
  }

  function validationMetricCard(label, value, sub) {
    return '<div class="validation-kpi-card">' +
      '<div class="validation-kpi-label">' + esc(label) + '</div>' +
      '<div class="validation-kpi-value">' + value + '</div>' +
      (sub ? '<div class="validation-kpi-sub">' + esc(sub) + '</div>' : '') +
      '</div>';
  }

  function validationStockTable(rows, reasonField) {
    if (!rows || !rows.length) return '<div class="muted" style="font-size:12px">暂无样本。</div>';
    return '<table class="data-table data-table-compact"><thead><tr><th>股票</th><th>池子</th><th>新 / 旧</th><th>质量 / 阶段 / 预测</th><th>原因</th></tr></thead><tbody>' +
      rows.map(function (item) {
        var reason = item[reasonField] || item.priority_pool_reason || item.composite_cap_reason || '-';
        return '<tr>' +
          '<td>' + stockCell(item.stock_code, item.stock_name) + '</td>' +
          '<td>' + priorityPoolTag(item.priority_pool) + '</td>' +
          '<td><div style="font-size:12px;color:#0f172a;font-weight:600">综合 ' + scoreNum(item.composite_priority_score) + '</div><div class="muted" style="font-size:10px">Legacy ' + scoreNum(item.action_score) + '</div></td>' +
          '<td><div style="font-size:12px;color:#0f172a">质 ' + scoreNum(item.company_quality_score) + ' · 阶 ' + scoreNum(item.stage_score) + ' · 测 ' + scoreNum(item.forecast_score) + '</div><div class="muted" style="font-size:10px">' + esc(item.stock_archetype || '待分类') + '</div></td>' +
          '<td><div style="font-size:11px;color:#475569;line-height:1.5">' + esc(reason) + '</div></td>' +
          '</tr>';
      }).join('') +
      '</tbody></table>';
  }

  function validationRankTable(rows, mode) {
    if (!rows || !rows.length) return '<div class="muted" style="font-size:12px">暂无明显位次变化。</div>';
    return '<table class="data-table data-table-compact"><thead><tr><th>股票</th><th>新排序</th><th>旧排序</th><th>位次变化</th><th>当前状态</th></tr></thead><tbody>' +
      rows.map(function (item) {
        var delta = Number(item.rank_delta || 0);
        var deltaText = (delta >= 0 ? '+' : '') + delta;
        var deltaColor = mode === 'promoted' ? '#166534' : '#b91c1c';
        return '<tr>' +
          '<td>' + stockCell(item.stock_code, item.stock_name) + '</td>' +
          '<td>#' + fmt(item.composite_rank) + '</td>' +
          '<td>#' + fmt(item.legacy_rank) + '</td>' +
          '<td><span style="font-size:12px;font-weight:700;color:' + deltaColor + '">' + esc(deltaText) + '</span></td>' +
          '<td><div style="font-size:12px;color:#0f172a">' + priorityPoolTag(item.priority_pool) + '</div><div class="muted" style="font-size:10px;margin-top:3px">综合 ' + scoreNum(item.composite_priority_score) + ' · Legacy ' + scoreNum(item.action_score) + '</div></td>' +
          '</tr>';
      }).join('') +
      '</tbody></table>';
  }

  function validationReplayCell(item, horizon) {
    var matured = Number(item['matured_' + horizon + 'd_count'] || 0);
    var gain = item['avg_gain_' + horizon + 'd'];
    var winRate = item['win_rate_' + horizon + 'd'];
    var drawdown = item['avg_drawdown_' + horizon + 'd'];
    if (!matured) return '<span class="muted">待成熟</span>';
    return '<div style="line-height:1.45"><div>' + fmtGain(gain) + '</div><div class="muted" style="font-size:10px">样本 ' + fmt(matured) + ' · 胜率 ' + pct(winRate) + (drawdown != null ? ' · 回撤 -' + Number(drawdown).toFixed(1) + '%' : '') + '</div></div>';
  }

  function validationSnapshotHistoryTable(rows) {
    if (!rows || !rows.length) return '<div class="muted" style="font-size:12px">暂无快照池历史。</div>';
    return '<table class="data-table data-table-compact"><thead><tr><th>快照日</th><th>池子</th><th>样本</th><th>均综合</th><th>成熟30日</th><th>30日均收益</th><th>30日胜率</th></tr></thead><tbody>' +
      rows.map(function (item) {
        return '<tr>' +
          '<td>' + esc(fmtDate(item.snapshot_date)) + '</td>' +
          '<td>' + priorityPoolTag(item.priority_pool) + '</td>' +
          '<td>' + fmt(item.total) + '</td>' +
          '<td>' + scoreNum(item.avg_composite_score) + '</td>' +
          '<td>' + fmt(item.matured_30d_count || 0) + '</td>' +
          '<td>' + fmtGain(item.avg_gain_30d) + '</td>' +
          '<td>' + pct(item.win_rate_30d) + '</td>' +
          '</tr>';
      }).join('') +
      '</tbody></table>';
  }

  function validationSnapshotRankSummaryTable(rows) {
    if (!rows || !rows.length) return '<div class="muted" style="font-size:12px">暂无成熟快照日，暂时还不能做快照期后验对比。</div>';
    var grouped = {};
    rows.forEach(function (item) {
      grouped[item.topn] = grouped[item.topn] || {};
      grouped[item.topn][item.method] = item;
    });
    return '<table class="data-table data-table-compact"><thead><tr><th>TopN</th><th>新综合排序</th><th>旧 action_score</th><th>重合样本</th></tr></thead><tbody>' +
      Object.keys(grouped).sort(function (a, b) { return Number(a) - Number(b); }).map(function (key) {
        var composite = grouped[key].composite || {};
        var legacy = grouped[key].legacy || {};
        return '<tr>' +
          '<td>Top' + esc(String(key)) + '</td>' +
          '<td><div>' + fmtGain(composite.avg_gain_30d) + '</div><div class="muted" style="font-size:10px">样本 ' + fmt(composite.sample_count || 0) + ' · 胜率 ' + pct(composite.win_rate_30d) + ' · 回撤 ' + (composite.avg_drawdown_30d != null ? '-' + Number(composite.avg_drawdown_30d).toFixed(1) + '%' : '-') + '</div></td>' +
          '<td><div>' + fmtGain(legacy.avg_gain_30d) + '</div><div class="muted" style="font-size:10px">样本 ' + fmt(legacy.sample_count || 0) + ' · 胜率 ' + pct(legacy.win_rate_30d) + ' · 回撤 ' + (legacy.avg_drawdown_30d != null ? '-' + Number(legacy.avg_drawdown_30d).toFixed(1) + '%' : '-') + '</div></td>' +
          '<td>' + fmt(composite.overlap_count || 0) + '</td>' +
          '</tr>';
      }).join('') +
      '</tbody></table>';
  }

  function validationSnapshotRankHistoryTable(rows) {
    if (!rows || !rows.length) return '<div class="muted" style="font-size:12px">暂无成熟快照日。</div>';
    return '<table class="data-table data-table-compact"><thead><tr><th>快照日</th><th>新综合 Top20</th><th>旧 action_score Top20</th><th>重合</th></tr></thead><tbody>' +
      rows.map(function (item) {
        return '<tr>' +
          '<td>' + esc(fmtDate(item.snapshot_date)) + '</td>' +
          '<td><div>' + fmtGain(item.composite_avg_gain_30d) + '</div><div class="muted" style="font-size:10px">样本 ' + fmt(item.composite_count || 0) + ' · 胜率 ' + pct(item.composite_win_rate_30d) + '</div></td>' +
          '<td><div>' + fmtGain(item.legacy_avg_gain_30d) + '</div><div class="muted" style="font-size:10px">样本 ' + fmt(item.legacy_count || 0) + ' · 胜率 ' + pct(item.legacy_win_rate_30d) + '</div></td>' +
          '<td>' + fmt(item.top20_overlap || 0) + '</td>' +
          '</tr>';
      }).join('') +
      '</tbody></table>';
  }

  function renderValidationScopeBanner(scope) {
    if (!scope?.sector) return '';
    return '<div class="panel validation-scope-banner">' +
      '<div class="validation-scope-head">' +
      '<div>' +
      '<div class="validation-scope-title">行业验证视角：' + esc(scope.sector) + '</div>' +
      '<div class="validation-scope-sub">当前分池反馈、快照回放、新旧排序对比和异常样本已按该行业过滤。审计摘要仍保持全市场口径。</div>' +
      '</div>' +
      '<button class="btn-sm" type="button" onclick="App.clearStockValidationFilter()">查看全市场</button>' +
      '</div>' +
      '</div>';
  }

  function snapshotMaturityPendingText(coverage, baseline, extra) {
    coverage = coverage || {};
    baseline = baseline || {};
    var matured10 = Number(baseline.matured_10d_count || 0);
    var matured30 = Number(baseline.matured_30d_count || 0);
    var matured60 = Number(baseline.matured_60d_count || 0);
    var snapshotDays = Number(
      coverage.scored_snapshot_dates != null ? coverage.scored_snapshot_dates
        : coverage.snapshot_dates != null ? coverage.snapshot_dates
          : coverage.total_snapshot_days != null ? coverage.total_snapshot_days
            : 0
    );
    var totalRows = Number(
      coverage.scored_rows != null ? coverage.scored_rows
        : coverage.total_rows != null ? coverage.total_rows
          : coverage.latest_snapshot_total != null ? coverage.latest_snapshot_total
            : 0
    );
    if ((matured10 + matured30 + matured60) > 0 || (!snapshotDays && !totalRows)) return '';
    var parts = ['快照样本仍在积累中'];
    if (snapshotDays > 0) parts.push('快照日 ' + fmt(snapshotDays));
    if (totalRows > 0) parts.push('样本 ' + fmt(totalRows));
    if (extra) parts.push(extra);
    return parts.join(' · ');
  }

  function renderStockValidation(report) {
    var scope = report.scope || {};
    var summary = report.summary || {};
    var pools = report.pool_feedback || [];
    var snapshotReplay = report.snapshot_pool_replay || {};
    var snapshotCoverage = snapshotReplay.coverage || {};
    var snapshotBaseline = snapshotReplay.baseline || {};
    var snapshotPools = snapshotReplay.by_pool || [];
    var snapshotHistory = snapshotReplay.history || [];
    var snapshotRankCompare = report.snapshot_rank_compare || {};
    var compare = report.legacy_compare || {};
    var anomalies = report.anomalies || {};
    var audit = report.audit || {};
    var qlibSummary = report.qlib_summary || {};
    var overlap = compare.overlap || {};
    var snapshotPendingText = snapshotMaturityPendingText(
      {
        scored_snapshot_dates: summary.snapshot_scored_dates,
        scored_rows: summary.snapshot_scored_rows
      },
      {
        matured_10d_count: snapshotBaseline.matured_10d_count,
        matured_30d_count: snapshotBaseline.matured_30d_count,
        matured_60d_count: snapshotBaseline.matured_60d_count
      },
      scope.sector ? ('行业 ' + scope.sector) : '全市场'
    );
    var rankComparePendingText =
      Number(summary.snapshot_scored_dates || 0) > 0 && Number(summary.snapshot_rank_matured_dates || 0) === 0
        ? '快照期排序对比样本仍在积累中 · 已评分快照日 ' + fmt(summary.snapshot_scored_dates || 0)
        : '';
    var poolTable = '<table class="data-table data-table-compact"><thead><tr><th>池子</th><th>股票数</th><th>Setup</th><th>封顶</th><th>均综合</th><th>均发现</th><th>均质量</th><th>均阶段</th><th>均预测</th><th>近20日反馈</th></tr></thead><tbody>' +
      (pools.length ? pools.map(function (item) {
        return '<tr>' +
          '<td>' + priorityPoolTag(item.priority_pool) + '</td>' +
          '<td>' + fmt(item.total) + '</td>' +
          '<td>' + fmt(item.setup_count) + '</td>' +
          '<td>' + fmt(item.capped_count) + '</td>' +
          '<td>' + scoreNum(item.avg_composite_score) + '</td>' +
          '<td>' + scoreNum(item.avg_discovery_score) + '</td>' +
          '<td>' + scoreNum(item.avg_quality_score) + '</td>' +
          '<td>' + scoreNum(item.avg_stage_score) + '</td>' +
          '<td>' + scoreNum(item.avg_forecast_score) + '</td>' +
          '<td>' + fmtGain(item.avg_price_20d_pct) + '</td>' +
          '</tr>';
      }).join('') : '<tr><td class="empty-row" colspan="10">暂无数据</td></tr>') +
      '</tbody></table>';
    var snapshotPoolTable = '<table class="data-table data-table-compact"><thead><tr><th>池子</th><th>快照样本</th><th>快照日数</th><th>均综合</th><th>10日后验</th><th>30日后验</th><th>vs基线</th><th>60日后验</th><th>vs基线</th></tr></thead><tbody>' +
      (snapshotPools.length ? snapshotPools.map(function (item) {
        return '<tr>' +
          '<td>' + priorityPoolTag(item.priority_pool) + '</td>' +
          '<td>' + fmt(item.total) + '</td>' +
          '<td>' + fmt(item.snapshot_days) + '</td>' +
          '<td>' + scoreNum(item.avg_composite_score) + '</td>' +
          '<td>' + validationReplayCell(item, 10) + '</td>' +
          '<td>' + validationReplayCell(item, 30) + '</td>' +
          '<td>' + fmtGain(item.uplift_vs_baseline_30d) + '</td>' +
          '<td>' + validationReplayCell(item, 60) + '</td>' +
          '<td>' + fmtGain(item.uplift_vs_baseline_60d) + '</td>' +
          '</tr>';
      }).join('') : '<tr><td class="empty-row" colspan="9">暂无快照池回放数据</td></tr>') +
      '</tbody></table>';

    return renderValidationScopeBanner(scope) +
      '<div class="validation-kpi-grid">' +
      validationMetricCard('评分覆盖', fmt(summary.stock_count || 0), '当前已分池股票总数') +
      validationMetricCard('A池', fmt(summary.a_pool_count || 0), '重点优先池') +
      validationMetricCard('Top20重合', fmt(summary.overlap_top20 || 0), '新旧排序共同入围') +
      validationMetricCard('封顶股票', fmt(summary.capped_total || 0), '触发 Stage / Quality 封顶') +
      validationMetricCard('异常项', fmt(summary.anomaly_total || 0), '高分冲突或阶段错配') +
      validationMetricCard('审计分', scoreNum(summary.audit_score), '来自数据质量审计') +
      validationMetricCard('快照已评分', fmt(summary.snapshot_scored_rows || 0), '带四层主分的快照样本') +
      validationMetricCard('快照评分日', fmt(summary.snapshot_scored_dates || 0), '可用于历史分池回放') +
      validationMetricCard('快照成熟日', fmt(summary.snapshot_rank_matured_dates || 0), '可用于新旧排序后验对比') +
      validationMetricCard('Qlib覆盖', fmt(summary.qlib_prediction_count || 0), esc(fmtDate(summary.qlib_predict_date) || '-')) +
      '</div>' +
      '<div class="validation-section-grid">' +
      '<div class="panel validation-table-card">' +
      '<div class="validation-section-head"><div><div class="validation-section-title">当前分池反馈</div><div class="validation-section-sub">这里展示的是当前池子的结构和近20日价格反馈，不把它当成前瞻回测。</div></div></div>' +
      poolTable +
      '</div>' +
      '<div class="panel validation-table-card">' +
      '<div class="validation-section-head"><div><div class="validation-section-title">审计摘要</div><div class="validation-section-sub">把评分闭环最关键的数据完备性问题收进同一页。</div></div></div>' +
      '<div class="validation-audit-list">' +
      '<div class="validation-audit-item">最新公告：' + esc(fmtDate(audit.latest_notice)) + '</div>' +
      '<div class="validation-audit-item">股票评分：' + fmt(audit.trend_scored || 0) + ' / ' + fmt(audit.trend_count || 0) + '</div>' +
      '<div class="validation-audit-item">财务历史就绪：' + fmt(audit.financial_research_ready || 0) + '</div>' +
      '<div class="validation-audit-item">财务历史缺口：' + fmt(audit.financial_research_gap || 0) + '</div>' +
      '<div class="validation-audit-item">扩展指标就绪：' + fmt(audit.indicator_research_ready || 0) + '</div>' +
      '<div class="validation-audit-item">扩展指标缺口：' + fmt(audit.indicator_research_gap || 0) + '</div>' +
      '<div class="validation-audit-item">阶段特征：' + fmt(audit.stage_feature_count || 0) + '</div>' +
      '<div class="validation-audit-item">预测特征：' + fmt(audit.forecast_feature_count || 0) + '</div>' +
      '<div class="validation-audit-item">行业上下文：' + fmt(audit.industry_context_count || 0) + '</div>' +
      '<div class="validation-audit-item">当前关系缺行业：' + fmt(audit.industry_missing_current || 0) + '</div>' +
      '<div class="validation-audit-item">快照已评分日：' + fmt(snapshotCoverage.scored_snapshot_dates || 0) + '</div>' +
      '<div class="validation-audit-item">首个评分快照：' + esc(fmtDate(snapshotCoverage.first_scored_snapshot_date)) + '</div>' +
      '<div class="validation-audit-item">最新评分快照：' + esc(fmtDate(snapshotCoverage.last_scored_snapshot_date)) + '</div>' +
      '<div class="validation-audit-item">快照总行数：' + fmt(snapshotCoverage.total_rows || 0) + '</div>' +
      '</div>' +
      '</div>' +
      '</div>' +
      '<div class="panel validation-table-card" style="margin-top:14px">' +
      '<div class="validation-section-head"><div><div class="validation-section-title">Qlib 模型验证上下文</div><div class="validation-section-sub">Forecast 和与预测有关的验证，优先直接消费完整版 Qlib 的模型指标、预测覆盖和因子结构。</div></div></div>' +
      renderQlibSummaryBlock(qlibSummary, { title: '当前 Qlib 模型', note: '这里显示的是当前正在回流到 Forecast 的真实模型，而不是额外拼出来的并行预测口径。' }) +
      '</div>' +
      '<div class="validation-section-grid">' +
      '<div class="panel validation-table-card">' +
      '<div class="validation-section-head"><div><div class="validation-section-title">快照分池后验</div><div class="validation-section-sub">这里开始用 `fact_setup_snapshot` 的真实快照口径汇总 A/B/C/D 的后验表现，后续会继续积累成熟样本。</div></div></div>' +
      '<div class="validation-overlap-note">全量快照基线：30日 ' + fmtGain(snapshotBaseline.avg_gain_30d) + ' · 60日 ' + fmtGain(snapshotBaseline.avg_gain_60d) + ' · 成熟样本 30日 ' + fmt(snapshotBaseline.matured_30d_count || 0) + ' / 60日 ' + fmt(snapshotBaseline.matured_60d_count || 0) + '</div>' +
      (snapshotPendingText ? '<div class="validation-overlap-note">' + esc(snapshotPendingText) + '</div>' : '') +
      snapshotPoolTable +
      '</div>' +
      '<div class="panel validation-table-card">' +
      '<div class="validation-section-head"><div><div class="validation-section-title">快照池历史</div><div class="validation-section-sub">按快照日和池子展开，方便观察历史分池的样本变化与 30 日后验。</div></div></div>' +
      validationSnapshotHistoryTable(snapshotHistory) +
      '</div>' +
      '</div>' +
      '<div class="validation-section-grid">' +
      '<div class="panel validation-table-card">' +
      '<div class="validation-section-head"><div><div class="validation-section-title">快照期后验对比</div><div class="validation-section-sub">只看已经成熟 30 日的快照日，比较新综合排序和旧 action_score 排序在同一批候选中的后验表现。</div></div></div>' +
      (rankComparePendingText ? '<div class="validation-overlap-note">' + esc(rankComparePendingText) + '</div>' : '') +
      validationSnapshotRankSummaryTable(snapshotRankCompare.summary || []) +
      '</div>' +
      '<div class="panel validation-table-card">' +
      '<div class="validation-section-head"><div><div class="validation-section-title">快照期历史对比</div><div class="validation-section-sub">按快照日展开 Top20 的新旧排序后验，用来观察两套排序是否逐步分化。</div></div></div>' +
      (rankComparePendingText ? '<div class="validation-overlap-note">' + esc(rankComparePendingText) + '</div>' : '') +
      validationSnapshotRankHistoryTable(snapshotRankCompare.history || []) +
      '</div>' +
      '</div>' +
      '<div class="validation-section-grid">' +
      '<div class="panel validation-table-card">' +
      '<div class="validation-section-head"><div><div class="validation-section-title">新排序上调样本</div><div class="validation-section-sub">对比口径：新排序 = 分池优先级 + 综合分；旧排序 = legacy action_score。</div></div></div>' +
      '<div class="validation-overlap-note">Top20 重合 ' + fmt(overlap.top20 || 0) + ' · Top50 重合 ' + fmt(overlap.top50 || 0) + ' · Top100 重合 ' + fmt(overlap.top100 || 0) + '</div>' +
      validationRankTable(compare.promoted || [], 'promoted') +
      '</div>' +
      '<div class="panel validation-table-card">' +
      '<div class="validation-section-head"><div><div class="validation-section-title">旧排序更靠前样本</div><div class="validation-section-sub">这批股票说明新体系在主动压缩部分旧高分股的优先级。</div></div></div>' +
      validationRankTable(compare.demoted || [], 'demoted') +
      '</div>' +
      '</div>' +
      '<div class="validation-section-grid validation-section-grid-3">' +
      '<div class="panel validation-table-card">' +
      '<div class="validation-section-head"><div><div class="validation-section-title">高原始分但未进 A 池</div><div class="validation-section-sub">重点检查是否被阶段门槛、质量门槛或封顶规则压住。</div></div></div>' +
      validationStockTable(anomalies.capped_high_raw || [], 'composite_cap_reason') +
      '</div>' +
      '<div class="panel validation-table-card">' +
      '<div class="validation-section-head"><div><div class="validation-section-title">预测强但阶段差</div><div class="validation-section-sub">这类股票不应让 Qlib 把总分硬拉上去。</div></div></div>' +
      validationStockTable(anomalies.forecast_stage_conflict || [], 'priority_pool_reason') +
      '</div>' +
      '<div class="panel validation-table-card">' +
      '<div class="validation-section-head"><div><div class="validation-section-title">质量门槛冲突</div><div class="validation-section-sub">非周期/事件型里，质量偏弱但综合仍偏高的样本。</div></div></div>' +
      validationStockTable(anomalies.quality_gate_conflict || [], 'priority_pool_reason') +
      '</div>' +
      '</div>';
  }

  async function loadStockValidation() {
    var c = el('stockValidationContainer');
    if (!c) return;
    c.innerHTML = '<div class="panel"><div class="muted">加载评分验证报告中...</div></div>';
    var path = '/api/inst/stock-validation/report' + (_stockValidationSector ? ('?sector=' + encodeURIComponent(_stockValidationSector)) : '');
    var r = await apiCached(path, SHORT_CACHE_TTL_MS);
    if (!r?.ok || !r.data) {
      c.innerHTML = '<div class="panel"><div class="muted">暂无评分验证数据。</div></div>';
      return;
    }
    c.innerHTML = renderStockValidation(r.data);
    scheduleSortableTables('stockValidationContainer');
  }

  function openSectorValidation(sectorName) {
    _stockValidationSector = String(sectorName || '').trim();
    document.querySelectorAll('.stock-tabs .tab-btn').forEach(function (b) {
      b.classList.toggle('active', b.dataset.stab === 'validation');
    });
    document.querySelectorAll('.stab-content').forEach(function (c) {
      c.classList.toggle('active', c.id === 'stab-validation');
    });
    setStockSearchContext('validation');
    loadStockValidation();
  }

  function clearStockValidationFilter() {
    _stockValidationSector = '';
    loadStockValidation();
  }



  // ============================================================
  // Update Pipeline
  // ============================================================
  var STATUS_COLORS = {
    completed: '#10b981',
    partial: '#f59e0b',
    failed: '#ef4444',
    blocked: '#f59e0b',
    running: '#3b82f6',
    pending: '#94a3b8',
    skipped: '#cbd5e1',
    stopped: '#f59e0b',
    idle: '#e2e8f0'
  };
  var _uiRunning = false;
  var _lastAuditSnapshot = null;
  var _auditRefreshPromise = null;
  var _activeRunContext = null;
  var _lastRunContext = null;
  var _stopRequestedUi = false;
  var QLIB_DEFAULT_PARAMS = {
    train_start: '2023-01-01',
    train_end: '2025-03-31',
    valid_end: '2025-09-30',
    test_end: '2026-01-31',
    num_boost_round: 500,
    early_stopping_rounds: 50,
    num_leaves: 64,
    learning_rate: 0.05,
    subsample: 0.8,
    colsample_bytree: 0.8,
    use_alpha158: true,
    use_financial: true,
    use_institution: true
  };
  var DOWNSTREAM_LABELS = {
    sync_market_data: ['计算收益', '构建当前关系', '机构画像', '行业统计', '生成股票列表', 'TDX选股筛选', '板块动量分析', '机构评分', '股票评分'],
    sync_financial: ['计算财务指标', 'TDX选股筛选'],
    gen_events: ['计算收益', '构建当前关系', '机构画像', '行业统计', '生成股票列表', '机构评分', '股票评分'],
    calc_returns: ['构建当前关系', '机构画像', '行业统计', '生成股票列表', '机构评分', '股票评分'],
    sync_industry: ['构建当前关系', '机构画像', '行业统计', '生成股票列表', '板块动量分析', '机构评分', '股票评分'],
    calc_financial_derived: ['TDX选股筛选'],
    build_current_rel: ['机构画像', '行业统计', '生成股票列表', '机构评分', '股票评分'],
    build_profiles: ['机构评分', '股票评分'],
    build_industry_stat: ['机构评分', '股票评分'],
    build_trends: ['股票评分'],
    calc_screening: ['股票评分'],
    calc_sector_momentum: ['股票评分'],
    calc_inst_scores: ['股票评分'],
  };
  var STEP_DEFAULTS = [
    { step_id: 'sync_raw', step_name: '下载十大股东', status: 'idle', desc: '从全市场拉取最新十大股东入驻数据' },
    { step_id: 'match_inst', step_name: '匹配跟踪机构', status: 'idle', desc: '将原始数据与跟踪名单匹配' },
    { step_id: 'sync_market_data', step_name: '同步行情数据', status: 'idle', desc: '补齐持仓股月K/日K数据' },
    { step_id: 'sync_financial', step_name: '同步财务数据', status: 'idle', desc: '从通达信同步 gpcw 财务数据' },
    { step_id: 'gen_events', step_name: '生成事件', status: 'idle', desc: '比对持仓变动，生成新进/增持/减持事件' },
    { step_id: 'calc_returns', step_name: '计算收益', status: 'idle', desc: '计算每个事件公告后的收益与回撤' },
    { step_id: 'sync_industry', step_name: '申万行业', status: 'idle', desc: '给持仓股补充申万三级行业分类' },
    { step_id: 'calc_financial_derived', step_name: '计算财务指标', status: 'idle', desc: '计算 ROE、毛利率等财务派生指标' },
    { step_id: 'build_current_rel', step_name: '构建当前关系', status: 'idle', desc: '构建“机构→股票”当前持仓关系' },
    { step_id: 'build_profiles', step_name: '机构画像', status: 'idle', desc: '生成机构历史胜率、收益、行业画像' },
    { step_id: 'build_industry_stat', step_name: '行业统计', status: 'idle', desc: '汇总机构在各行业的历史表现' },
    { step_id: 'build_trends', step_name: '生成股票列表', status: 'idle', desc: '汇总当前持仓股的信号与趋势' },
    { step_id: 'calc_screening', step_name: 'TDX选股筛选', status: 'idle', desc: '运行通达信 F1/F3/F5 选股公式' },
    { step_id: 'calc_sector_momentum', step_name: '板块动量分析', status: 'idle', desc: '计算板块技术态势与双重确认信号' },
    { step_id: 'calc_inst_scores', step_name: '机构评分', status: 'idle', desc: '多维度评分机构实力、胜率、稳定性' },
    { step_id: 'calc_stock_scores', step_name: '股票评分', status: 'idle', desc: '综合评分每只股票的行动价值' }
  ];

  function cloneStepDefinitions() {
    return STEP_DEFAULTS.map(function (step) { return Object.assign({}, step); });
  }

  function makePlannedSteps(stepIds, skipReasons) {
    var selected = new Set(stepIds || []);
    var reasons = skipReasons || {};
    return cloneStepDefinitions().map(function (step) {
      if (selected.has(step.step_id)) {
        return Object.assign(step, { status: 'pending' });
      }
      return Object.assign(step, {
        status: 'skipped',
        error: reasons[step.step_id] || '数据已是最新，无需更新'
      });
    });
  }

  function fmtTime(ts) {
    if (!ts) return '';
    var d = new Date(ts);
    if (isNaN(d)) return '';
    return (d.getMonth() + 1) + '-' + d.getDate() + ' ' + d.getHours() + ':' + String(d.getMinutes()).padStart(2, '0');
  }

  function gapStatusLabel(status) {
    return {
      blocked: '阻断',
      retrying: '重试中',
      pending: '待补齐',
      resolved: '已补齐'
    }[status] || (status || '');
  }

  function renderGapItems(items, maxItems) {
    var list = Array.isArray(items) ? items.slice(0, maxItems || items.length) : [];
    if (!list.length) return '';
    return '<div class="gap-queue-list">' + list.map(function (item) {
      var title = item.stock_name ? (item.stock_name + ' · ' + item.stock_code) : (item.stock_code || '-');
      var meta = [];
      if (item.source_attempts > 0) meta.push('尝试' + fmt(item.source_attempts) + '次');
      var ts = item.last_attempt_at || item.updated_at || item.first_seen_at;
      if (ts) meta.push(fmtTime(ts));
      return '<div class="gap-queue-item">' +
        '<div class="gap-queue-item-head">' +
        '<span>' + esc(title) + '</span>' +
        '<span class="gap-chip ' + esc(item.status || 'pending') + '">' + esc(gapStatusLabel(item.status)) + '</span>' +
        '</div>' +
        (item.reason ? '<div class="gap-queue-item-reason">' + esc(item.reason) + '</div>' : '') +
        (meta.length ? '<div class="gap-queue-item-reason">' + esc(meta.join(' · ')) + '</div>' : '') +
        '</div>';
    }).join('') + '</div>';
  }

  function renderGapSummaryInline(summary, maxItems) {
    if (!summary || typeof summary.unresolved !== 'number') return '';
    var meta = '未补齐 ' + fmt(summary.unresolved);
    if (summary.blocked) meta += ' · 阻断 ' + fmt(summary.blocked);
    if (summary.retrying) meta += ' · 重试中 ' + fmt(summary.retrying);
    if (summary.pending) meta += ' · 待补齐 ' + fmt(summary.pending);
    var html = '<div class="gap-queue-meta" style="margin-top:4px">' + esc(meta) + '</div>';
    if ((summary.unresolved || 0) > 0) html += renderGapItems(summary.items, maxItems || 3);
    return html;
  }

  function renderMarketSyncDetail(step) {
    var detail = step.detail;
    if (!detail) return '';
    var lines = [];
    var progressObj = null;
    function stageLine(label, obj) {
      if (!obj) return;
      var text = label + ' ';
      if (typeof obj.done_codes === 'number' && typeof obj.total_codes === 'number') {
        text += obj.done_codes + '/' + obj.total_codes;
      } else if (typeof obj.total_codes === 'number') {
        text += '0/' + obj.total_codes;
      } else {
        text += obj.status || '';
      }
      if (typeof obj.rows === 'number') text += ' · ' + fmt(obj.rows) + '行';
      if (typeof obj.failed_count === 'number' && obj.failed_count > 0) text += ' · 失败' + obj.failed_count;
      if (typeof obj.before_missing === 'number' && typeof obj.after_missing === 'number') {
        text += ' · 缺口' + fmt(obj.before_missing) + '→' + fmt(obj.after_missing);
      }
      lines.push('<div style="font-size:10px;color:#64748b;margin-top:2px">' + esc(text) + '</div>');
      if (obj.reason) {
        lines.push('<div style="font-size:10px;color:#b45309;margin-top:2px">' + esc(obj.reason) + '</div>');
      }
      if (obj.gap_summary && obj.gap_summary.unresolved > 0) {
        lines.push(renderGapSummaryInline(obj.gap_summary, 2));
      }
      if (typeof obj.total_codes === 'number' && obj.total_codes > 0 && obj.status === 'running') {
        progressObj = obj;
      }
    }
    stageLine('月K', detail.monthly_sync);
    stageLine('日K', detail.daily_sync);
    if (_stopRequestedUi && step && step.status === 'running') {
      lines.push('<div style="font-size:10px;color:#f59e0b;margin-top:2px">正在等待当前请求结束后停止</div>');
    }
    if (!progressObj) return lines.join('');
    var done = typeof progressObj.done_codes === 'number' ? progressObj.done_codes : 0;
    var total = Math.max(progressObj.total_codes || 0, 1);
    var pct = Math.max(0, Math.min(100, Math.round(done / total * 100)));
    return lines.join('') + '<div class="step-mini-progress"><div class="step-mini-progress-fill" style="width:' + pct + '%"></div></div>';
  }

  function renderIndustrySyncDetail(step) {
    var detail = step.detail;
    var obj = detail && detail.industry_sync;
    if (!obj) return '';
    var text = '申万三级';
    if (typeof obj.updated_rows === 'number') text += ' · 更新' + fmt(obj.updated_rows) + '只';
    if (typeof obj.before_missing === 'number' && typeof obj.after_missing === 'number') {
      text += ' · 缺口' + fmt(obj.before_missing) + '→' + fmt(obj.after_missing);
    } else if (obj.gap_summary && typeof obj.gap_summary.unresolved === 'number') {
      text += ' · 未补齐' + fmt(obj.gap_summary.unresolved);
    }
    var lines = [
      '<div style="font-size:10px;color:#64748b;margin-top:2px">' + esc(text) + '</div>'
    ];
    if (obj.reason) {
      lines.push('<div style="font-size:10px;color:#b45309;margin-top:2px">' + esc(obj.reason) + '</div>');
    }
    if (obj.gap_summary && obj.gap_summary.unresolved > 0) {
      lines.push(renderGapSummaryInline(obj.gap_summary, 2));
    }
    return lines.join('');
  }

  function isStepActiveInCurrentRun(stepId) {
    if (!_uiRunning || !_activeRunContext || !stepId) return false;
    var stepIds = _activeRunContext.step_ids || [];
    if (Array.isArray(stepIds) && stepIds.length) return stepIds.indexOf(stepId) >= 0;
    return _activeRunContext.step_id === stepId;
  }

  function normalizeStepReason(step) {
    var msg = step && step.error ? String(step.error) : '';
    if (!msg) return '';
    if (msg === '原始数据已保留，恢复时未重拉') return '已复用现有原始数据';
    if (msg === '持仓数据已保留，恢复时未重匹配') return '已复用现有持仓数据';
    if (msg === '现有K线保留，恢复时未补拉') return '已复用现有K线数据';
    if (msg === '现有行业数据保留，恢复时未补拉') return '已复用现有行业数据';
    return msg;
  }

  function parseTsMs(ts) {
    if (!ts) return 0;
    var d = new Date(ts);
    return isNaN(d) ? 0 : d.getTime();
  }

  function hasStepHistory(steps) {
    return !!(steps && steps.some(function (s) {
      return !!(s && ((s.status && s.status !== 'idle') || s.started_at || s.finished_at || s.records));
    }));
  }

  function buildAuditMeta(step, audit) {
    var layers = audit?.layers || {};
    var raw = layers.raw || {};
    var institutions = layers.institutions || {};
    var holdings = layers.holdings || {};
    var events = layers.events || {};
    var kline = layers.kline || {};
    var returns = layers.returns || {};
    var industry = layers.industry || {};
    var currentRel = layers.current_relationship || {};
    var profiles = layers.profiles || {};
    var industryStat = layers.industry_stat || {};
    var trends = layers.trends || {};
    var notes = [];
    var actionable = false;
    var actionLabel = '';
    var issueCount = 0;
    var hasData = false;
    var stepId = step.step_id;
    var status = step.status;
    function addNote(text, tone) {
      if (!text) return;
      notes.push({ text: text, tone: tone || 'info' });
    }
    function addIssue(text) {
      addNote(text, 'issue');
    }

    if (stepId === 'sync_raw') {
      if (typeof raw.count === 'number') addNote('审计 ' + fmt(raw.count) + ' 条原始记录 · 覆盖 ' + fmt(raw.stocks || 0) + ' 只全市场股票');
      if (raw.latest_notice) addNote('最新公告 ' + fmtDate(raw.latest_notice));
      hasData = (raw.count || 0) > 0;
      actionable = ['failed', 'skipped', 'stopped'].includes(status);
      actionLabel = '单独补拉';
    } else if (stepId === 'match_inst') {
      if (typeof holdings.institutions === 'number') {
        addNote('审计 ' + fmt(holdings.institutions) + '/' + fmt(holdings.tracked_institutions || 0) + ' 家跟踪机构匹配到持仓 · ' + fmt(holdings.stocks || 0) + ' 只历史持仓股');
      }
      if ((holdings.missing_institutions || 0) > 0) {
        addNote('另有 ' + fmt(holdings.missing_institutions) + ' 家跟踪机构当前未见持仓披露，可能为空仓或未进前十大');
      }
      hasData = (holdings.count || 0) > 0 || (holdings.institutions || 0) > 0;
      actionable = ['failed', 'skipped', 'stopped'].includes(status);
      actionLabel = '单独重匹配';
    } else if (stepId === 'sync_market_data') {
      if (typeof kline.expected_stocks === 'number') {
        addNote('审计 ' + fmt(kline.covered_stocks || 0) + '/' + fmt(kline.expected_stocks) + ' 只匹配持仓股有日K · 覆盖' + (kline.coverage != null ? kline.coverage : 0) + '%');
        if ((kline.missing || 0) > 0) addIssue('仍缺 ' + fmt(kline.missing) + ' 只无任何日K');
        if ((kline.stale_stocks || 0) > 0) {
          var parts = [];
          if (kline.suspended_stocks) parts.push(fmt(kline.suspended_stocks) + ' 只停牌');
          if (kline.delisted_stocks) parts.push(fmt(kline.delisted_stocks) + ' 只退市');
          addNote(fmt(kline.stale_stocks) + ' 只日K未更新到最新交易日（' + parts.join('、') + '）' + (kline.latest_trade_date ? ' · ' + fmtDate(kline.latest_trade_date) : ''));
        }
      }
      hasData = (kline.expected_stocks || 0) === 0 || (kline.covered_stocks || 0) > 0;
      issueCount = kline.missing || 0;
      actionable = (kline.missing || 0) > 0 || ['failed', 'skipped', 'stopped'].includes(status);
      actionLabel = (kline.missing || 0) > 0 ? '补齐缺失K线' : '单独补跑';
    } else if (stepId === 'gen_events') {
      if (typeof events.count === 'number') addNote('审计 ' + fmt(events.count) + ' 条事件 · 来源 ' + fmt(holdings.count || 0) + ' 条匹配持仓');
      if (typeof events.expected_stocks === 'number') addNote('覆盖 ' + fmt(events.stocks || 0) + '/' + fmt(events.expected_stocks) + ' 只匹配持仓股');
      hasData = (events.expected_stocks || 0) === 0 || (events.count || 0) > 0;
      actionable = ['failed', 'skipped', 'stopped'].includes(status);
      actionLabel = '重新生成事件';
    } else if (stepId === 'calc_returns') {
      if (typeof returns.total === 'number') {
        addNote('审计 ' + fmt(returns.count || 0) + '/' + fmt(returns.total) + ' 条事件有收益 · 覆盖' + (returns.coverage != null ? returns.coverage : 0) + '%');
        if (typeof returns.expected_institutions === 'number') addNote(fmt(returns.institutions || 0) + '/' + fmt(returns.expected_institutions) + ' 家有事件机构具备收益样本');
        if ((returns.ineligible_events || 0) > 0) addNote('另有 ' + fmt(returns.ineligible_events) + ' 条退出事件不参与收益计算');
        if ((returns.not_ready_events || 0) > 0) {
          addNote(
            '另有 ' + fmt(returns.not_ready_events || 0) + ' 条事件尚待成熟'
            + '（锚点未到 ' + fmt(returns.not_ready_future_events || 0)
            + ' · 路径待成熟 ' + fmt(returns.not_ready_path_events || 0) + '）'
          );
        }
        // 停牌事件是正常现象，灰色提示不算缺口
        if ((returns.suspended_waiting_events || 0) > 0) {
          addNote(fmt(returns.suspended_waiting_events) + ' 只股票停牌中，恢复交易后自动补算');
        }
        // 真正的数据缺失才标红
        var realMissing = Math.max((returns.missing_entry_price_events || 0) - (returns.suspended_waiting_events || 0), 0);
        if (realMissing > 0) {
          addIssue('仍有 ' + fmt(realMissing) + ' 条成熟事件缺K线入口价');
        }
        if ((returns.other_missing_events || 0) > 0) {
          addIssue('仍有 ' + fmt(returns.other_missing_events) + ' 条收益路径未补齐');
        }
        // issueCount 不计入停牌事件
        issueCount = Math.max(realMissing, returns.other_missing_events || 0, returns.missing_institutions || 0);
      }
      hasData = (returns.total || 0) === 0 || (returns.count || 0) > 0 || (returns.not_ready_events || 0) > 0;
      actionable = ((returns.actionable_missing_events || 0) > 0) || ['failed', 'skipped', 'stopped'].includes(status);
      actionLabel = ((returns.actionable_missing_events || 0) > 0) ? '补算缺失收益' : '单独补跑';
    } else if (stepId === 'sync_industry') {
      if (typeof industry.expected_stocks === 'number') {
        addNote('股票维度：给匹配持仓股补申万三级分类');
        addNote('审计 ' + fmt(industry.complete_three_level_stocks || 0) + '/' + fmt(industry.expected_stocks) + ' 只匹配持仓股有三级行业 · 覆盖' + (industry.coverage != null ? industry.coverage : 0) + '%');
        addNote('三级分类齐全：L1 ' + fmt(industry.level1_stocks || 0) + ' / L2 ' + fmt(industry.level2_stocks || 0) + ' / L3 ' + fmt(industry.level3_stocks || 0));
        if ((industry.missing || 0) > 0) addIssue('仍缺 ' + fmt(industry.missing) + ' 只，可单独补齐');
      }
      hasData = (industry.expected_stocks || 0) === 0 || (industry.complete_three_level_stocks || 0) > 0;
      issueCount = industry.missing || 0;
      actionable = (industry.missing || 0) > 0 || ['failed', 'skipped', 'stopped'].includes(status);
      actionLabel = (industry.missing || 0) > 0 ? '补齐缺失行业' : '单独补跑';
    } else if (stepId === 'build_current_rel') {
      if (typeof currentRel.expected_count === 'number') addNote('审计 ' + fmt(currentRel.count || 0) + '/' + fmt(currentRel.expected_count) + ' 条当前关系已对齐');
      addNote('口径：每只股票取全市场最新报告期中的跟踪机构');
      addNote('当前集合：' + fmt(currentRel.institutions || 0) + '/' + fmt(currentRel.expected_institutions || 0) + ' 家机构 · ' + fmt(currentRel.stocks || 0) + '/' + fmt(currentRel.expected_stocks || 0) + ' 只股票');
      if (typeof currentRel.industry_stocks === 'number' && typeof currentRel.stocks === 'number') addNote('其中 ' + fmt(currentRel.industry_stocks) + '/' + fmt(currentRel.stocks) + ' 只当前股已带行业');
      if (Math.abs(currentRel.row_gap || 0) > 0) addIssue('当前关系条数仍差 ' + fmt(Math.abs(currentRel.row_gap || 0)) + ' 条');
      if (Math.abs(currentRel.institution_gap || 0) > 0) addIssue('当前关系机构口径仍差 ' + fmt(Math.abs(currentRel.institution_gap || 0)) + ' 家');
      if (Math.abs(currentRel.stock_gap || 0) > 0) addIssue('当前关系股票口径仍差 ' + fmt(Math.abs(currentRel.stock_gap || 0)) + ' 只');
      if ((currentRel.industry_missing_stocks || 0) > 0) addIssue('仍有 ' + fmt(currentRel.industry_missing_stocks) + ' 只当前股缺行业');
      hasData = (currentRel.expected_count || 0) === 0 || (currentRel.count || 0) > 0;
      issueCount = Math.max(
        Math.abs(currentRel.row_gap || 0),
        Math.abs(currentRel.institution_gap || 0),
        Math.abs(currentRel.stock_gap || 0),
        currentRel.industry_missing_stocks || 0
      );
      actionable = issueCount > 0 || ['failed', 'skipped', 'stopped'].includes(status);
      actionLabel = '重建当前关系';
    } else if (stepId === 'build_profiles') {
      if (typeof profiles.count === 'number') addNote('审计 ' + fmt(profiles.count) + '/' + fmt(profiles.expected_institutions || institutions.tracked || profiles.count) + ' 家跟踪机构画像已生成');
      if (typeof profiles.current_institutions === 'number') addNote('其中 ' + fmt(profiles.current_institutions) + ' 家当前有持仓 · ' + fmt(profiles.tracked_without_current || 0) + ' 家当前空仓');
      if (Math.max((profiles.expected_institutions || 0) - (profiles.count || 0), 0) > 0) {
        addIssue('仍缺 ' + fmt(Math.max((profiles.expected_institutions || 0) - (profiles.count || 0), 0)) + ' 家机构画像');
      }
      hasData = (profiles.expected_institutions || institutions.tracked || 0) === 0 || (profiles.count || 0) > 0;
      issueCount = Math.max((profiles.expected_institutions || 0) - (profiles.count || 0), 0);
      actionable = (profiles.count || 0) < (institutions.tracked || profiles.count || 0) || ['failed', 'skipped', 'stopped'].includes(status);
      actionLabel = (profiles.count || 0) < (institutions.tracked || profiles.count || 0) ? '补齐机构画像' : '重算机构画像';
    } else if (stepId === 'build_industry_stat') {
      if (typeof industryStat.expected_institutions === 'number') addNote('审计 ' + fmt(industryStat.institutions || 0) + '/' + fmt(industryStat.expected_institutions) + ' 家有收益样本的机构已生成行业统计');
      addNote('机构维度：按行业汇总历史收益表现，不等同于股票行业映射');
      addNote('三级行业统计齐全 ' + fmt(industryStat.complete_three_level_institutions || 0) + '/' + fmt(industryStat.expected_institutions || 0) + ' 家');
      if ((industryStat.tracked_without_holdings || 0) > 0 || (industryStat.matched_without_returns || 0) > 0) {
        addNote('另有 ' + fmt(industryStat.tracked_without_holdings || 0) + ' 家跟踪机构暂无持仓，' + fmt(industryStat.matched_without_returns || 0) + ' 家匹配机构暂无收益样本');
      }
      if ((industryStat.inactive_or_merged_institutions || 0) > 0) {
        addNote('另有 ' + fmt(industryStat.inactive_or_merged_institutions) + ' 家已合并/停用别名机构保留历史收益，不计入行业统计');
      }
      if ((industryStat.missing_institutions || 0) > 0) addIssue('仍缺 ' + fmt(industryStat.missing_institutions) + ' 家机构行业统计');
      var incompleteIndustryStat = Math.max((industryStat.expected_institutions || 0) - (industryStat.complete_three_level_institutions || 0), 0);
      if (incompleteIndustryStat > 0) addIssue('仍有 ' + fmt(incompleteIndustryStat) + ' 家机构行业层级未补齐');
      hasData = (industryStat.expected_institutions || 0) === 0 || (industryStat.institutions || 0) > 0;
      issueCount = Math.max(
        industryStat.missing_institutions || 0,
        incompleteIndustryStat
      );
      actionable = issueCount > 0 || ['failed', 'skipped', 'stopped'].includes(status);
      actionLabel = '重算行业统计';
    } else if (stepId === 'build_trends') {
      if (typeof trends.expected_stocks === 'number') addNote('审计 ' + fmt(trends.count || 0) + '/' + fmt(trends.expected_stocks) + ' 只当前持仓股已入榜');
      if (typeof trends.scored === 'number') addNote('其中 ' + fmt(trends.scored) + '/' + fmt(trends.count || 0) + ' 只已有评分');
      if ((trends.missing_stocks || 0) > 0) addIssue('仍缺 ' + fmt(trends.missing_stocks) + ' 只当前股未入榜');
      if ((trends.extra_stocks || 0) > 0) addIssue('仍有 ' + fmt(trends.extra_stocks) + ' 只股票未对齐当前关系');
      if (Math.max((trends.count || 0) - (trends.scored || 0), 0) > 0) addIssue('仍有 ' + fmt(Math.max((trends.count || 0) - (trends.scored || 0), 0)) + ' 只入榜股票未评分');
      hasData = (trends.expected_stocks || 0) === 0 || (trends.count || 0) > 0;
      issueCount = Math.max(trends.missing_stocks || 0, trends.extra_stocks || 0, Math.max((trends.count || 0) - (trends.scored || 0), 0));
      actionable = issueCount > 0 || ['failed', 'skipped', 'stopped'].includes(status);
      actionLabel = '重生成股票列表';
    } else if (stepId === 'sync_financial') {
      var financial = layers.financial || {};
      if (typeof financial.raw_count === 'number') addNote('审计 ' + fmt(financial.raw_count) + ' 条原始财务记录');
      if (typeof financial.latest_count === 'number') addNote(fmt(financial.latest_count) + '/' + fmt(financial.expected_stocks || 0) + ' 只股票有最新财务快照');
      hasData = (financial.raw_count || 0) > 0 || (financial.latest_count || 0) > 0;
      actionable = ['failed', 'skipped', 'stopped'].includes(status);
      actionLabel = '单独补拉';
    } else if (stepId === 'calc_financial_derived') {
      var financial = layers.financial || {};
      if (typeof financial.derived_count === 'number') addNote('审计 ' + fmt(financial.derived_count) + ' 条派生财务指标');
      if (typeof financial.latest_count === 'number') addNote(fmt(financial.latest_count) + ' 只股票有最新快照');
      hasData = (financial.derived_count || 0) > 0;
      actionable = ['failed', 'skipped', 'stopped'].includes(status);
      actionLabel = '单独补算';
    } else if (stepId === 'calc_screening') {
      var screening = layers.screening || {};
      if (typeof screening.count === 'number') addNote('审计 ' + fmt(screening.count) + ' 只股票已筛选');
      if (typeof screening.hits === 'number' && screening.count > 0) addNote('命中 ' + fmt(screening.hits) + ' 只（F1/F3/F5 任一命中）');
      if (screening.screen_date) addNote('最近筛选：' + fmtDate(screening.screen_date));
      hasData = (screening.count || 0) > 0;
      actionable = (screening.count || 0) === 0 || ['failed', 'skipped', 'stopped'].includes(status);
      actionLabel = '单独补跑';
    } else if (stepId === 'calc_sector_momentum') {
      var sectorMom = layers.sector_momentum || {};
      if (typeof sectorMom.count === 'number') addNote('审计 ' + fmt(sectorMom.count) + ' 个板块动量已计算');
      if (typeof sectorMom.dual_confirm_count === 'number' && sectorMom.count > 0) addNote('双重确认信号 ' + fmt(sectorMom.dual_confirm_count) + ' 个');
      hasData = (sectorMom.count || 0) > 0;
      actionable = (sectorMom.count || 0) === 0 || ['failed', 'skipped', 'stopped'].includes(status);
      actionLabel = '单独补跑';
    } else if (stepId === 'calc_inst_scores') {
      if (typeof profiles.scored === 'number') addNote('审计 ' + fmt(profiles.scored) + '/' + fmt(profiles.count || institutions.tracked || 0) + ' 家机构已评分');
      if (Math.max((profiles.count || 0) - (profiles.scored || 0), 0) > 0) addIssue('仍缺 ' + fmt(Math.max((profiles.count || 0) - (profiles.scored || 0), 0)) + ' 家机构评分');
      hasData = (profiles.count || institutions.tracked || 0) === 0 || (profiles.scored || 0) > 0;
      issueCount = Math.max((profiles.count || 0) - (profiles.scored || 0), 0);
      actionable = (profiles.scored || 0) < (profiles.count || institutions.tracked || 0) || ['failed', 'skipped', 'stopped'].includes(status);
      actionLabel = (profiles.scored || 0) < (profiles.count || institutions.tracked || 0) ? '补算剩余机构评分' : '重算机构评分';
    } else if (stepId === 'calc_stock_scores') {
      if (typeof trends.scored === 'number') addNote('审计 ' + fmt(trends.scored) + '/' + fmt(trends.count || 0) + ' 只入榜股票已评分');
      if (Math.max((trends.count || 0) - (trends.scored || 0), 0) > 0) addIssue('仍缺 ' + fmt(Math.max((trends.count || 0) - (trends.scored || 0), 0)) + ' 只股票评分');
      hasData = (trends.count || 0) === 0 || (trends.scored || 0) > 0;
      issueCount = Math.max((trends.count || 0) - (trends.scored || 0), 0);
      actionable = (trends.scored || 0) < (trends.count || 0) || ['failed', 'skipped', 'stopped'].includes(status);
      actionLabel = (trends.scored || 0) < (trends.count || 0) ? '补算剩余股票评分' : '重算股票评分';
    }

    if (!actionable && ['failed', 'skipped', 'stopped'].includes(status)) {
      actionable = true;
      actionLabel = '单独补跑';
    }

    var downstream = DOWNSTREAM_LABELS[stepId] || [];
    if (actionable && downstream.length) {
      addNote('执行后将自动续跑：' + downstream.slice(0, 3).join('、') + (downstream.length > 3 ? ' 等' : ''));
    }

    return { audit_notes: notes, actionable: actionable, action_label: actionLabel, issue_count: issueCount, has_data: hasData };
  }

  function isBlockedStep(step) {
    var reason = normalizeStepReason(step) || '';
    return /不可用|阻断|中断|连接失败/.test(reason);
  }

  function deriveDisplayStatus(step, meta) {
    var rawStatus = step.status || 'idle';
    var issueCount = meta.issue_count || 0;
    var hasData = !!meta.has_data;
    var blocked = isBlockedStep(step);

    if (_uiRunning) {
      if (rawStatus === 'running' || rawStatus === 'pending' || rawStatus === 'failed' || rawStatus === 'stopped') return rawStatus;
      if (rawStatus === 'skipped' && blocked) return 'blocked';
      if (rawStatus === 'completed' && issueCount > 0) return 'partial';
      return rawStatus;
    }

    if (rawStatus === 'failed' && !hasData) return 'failed';
    if (rawStatus === 'stopped' && !hasData) return 'stopped';
    if (blocked && !hasData) return 'blocked';
    if (issueCount > 0) return 'partial';
    if (hasData) return 'completed';
    return 'idle';
  }

  function formatAuditNote(note) {
    var text = typeof note === 'string' ? note : (note?.text || '');
    var tone = typeof note === 'string' ? 'info' : (note?.tone || 'info');
    var html = esc(text);
    if (tone === 'issue') {
      html = html.replace(/(\d+(?:,\d{3})*(?:\.\d+)?)/g, '<span class="audit-issue-num">$1</span>');
    }
    return { html: html, tone: tone };
  }

  function withAuditNotes(steps, audit) {
    return (steps || []).map(function (s) {
      var item = Object.assign({}, s);
      var meta = buildAuditMeta(item, audit);
      item.audit_notes = meta.audit_notes || [];
      item.actionable = !!meta.actionable;
      item.action_label = meta.action_label || '';
      item.audit_issue = !!meta.issue_count;
      item.audit_issue_count = meta.issue_count || 0;
      item.display_status = deriveDisplayStatus(item, meta);
      return item;
    });
  }

  function renderWorkbenchSummary(summary) {
    var progressBar = document.querySelector('#progressArea .progress-bar');
    if (!summary || summary.kind === 'idle') {
      el('progressArea').style.display = 'none';
      el('progressFill').style.width = '0%';
      el('progressText').textContent = '';
      if (progressBar) progressBar.style.display = '';
      return;
    }
    var pctVal = Math.max(0, Math.min(100, Number(summary.pct || 0)));
    var latestAt = fmtTime(summary?.counts?.latest_at);
    var message = summary.message || '';
    if ((summary.kind === 'last' || summary.kind === 'noop') && latestAt) {
      message += ' · ' + latestAt;
    }
    el('progressArea').style.display = '';
    if (progressBar) progressBar.style.display = summary.show_progress === false ? 'none' : '';
    el('progressFill').style.width = (summary.kind === 'noop' ? '0%' : pctVal + '%');
    el('progressText').textContent = message;
  }

  function stepGridSignature(steps) {
    if (!steps || !steps.length) return '';
    return JSON.stringify(steps.map(function (s) {
      return [
        s.step_id,
        s.display_status || s.status || '',
        s.records || 0,
        s.started_at || '',
        s.finished_at || '',
        s.error || '',
        s.actionable ? 1 : 0,
        s.audit_issue_count || 0
      ];
    }));
  }

  function workbenchSummarySignature(summary) {
    if (!summary) return '';
    return JSON.stringify({
      kind: summary.kind || '',
      mode: summary.mode || '',
      pct: summary.pct || 0,
      message: summary.message || '',
      show_progress: summary.show_progress !== false,
      latest_at: summary?.counts?.latest_at || '',
      running: summary?.counts?.running || 0,
      pending: summary?.counts?.pending || 0,
      active: (summary.active_step_ids || []).join(',')
    });
  }

  function maybeRenderStepGrid(steps) {
    if (!steps || !steps.length) {
      _lastStepGridSig = '';
      renderStepGrid(steps);
      return;
    }
    var sig = stepGridSignature(steps);
    if (sig && sig === _lastStepGridSig) return;
    _lastStepGridSig = sig;
    renderStepGrid(steps);
  }

  function maybeRenderWorkbenchSummary(summary) {
    if (!summary) {
      _lastWorkbenchSummarySig = '';
      renderWorkbenchSummary(summary);
      return;
    }
    var sig = workbenchSummarySignature(summary);
    if (sig && sig === _lastWorkbenchSummarySig) return;
    _lastWorkbenchSummarySig = sig;
    renderWorkbenchSummary(summary);
  }

  async function refreshAuditSnapshot() {
    if (_auditRefreshPromise) return _auditRefreshPromise;
    _auditRefreshPromise = (async function () {
      var audit = await api('/api/inst/update/audit');
      if (audit && audit.layers) _lastAuditSnapshot = audit;
      return _lastAuditSnapshot;
    })();
    try {
      return await _auditRefreshPromise;
    } finally {
      _auditRefreshPromise = null;
    }
  }

  async function renderUpdatePanel(up) {
    if (up?.run_context) _activeRunContext = up.run_context;
    if (up?.last_run_context) _lastRunContext = up.last_run_context;
    if (up?.running) {
      _uiRunning = true;
      _stopRequestedUi = !!up.stop_requested;
      if (!_lastAuditSnapshot && !_auditRefreshPromise) refreshAuditSnapshot();
      var runningSteps = withAuditNotes(up.steps || STEP_DEFAULTS, _lastAuditSnapshot || {});
      maybeRenderStepGrid(runningSteps);
      maybeRenderWorkbenchSummary(up.summary);
      syncServerLogs(up.logs || [], true);
      el('btnUpdateAll').disabled = true;
      el('btnUpdateAll').textContent = up.stop_requested ? '停止中...' : '更新中...';
      el('btnStop').disabled = !!up.stop_requested;
      el('btnStop').textContent = up.stop_requested ? '停止中...' : '停止';
      el('btnStop').style.display = '';
      if (!_poll) _poll = setInterval(pollStatus, 1000);
      return;
    }

    _uiRunning = false;
    _stopRequestedUi = false;
    _activeRunContext = null;
    // 一次性渲染：等 audit 拿到再画 grid，杜绝「无 audit → 有 audit」闪烁
    // 后续 polling 时 _lastAuditSnapshot 已存在，不会再阻塞
    var baseSteps = hasStepHistory(up?.steps) ? up.steps : STEP_DEFAULTS;
    if (!_lastAuditSnapshot) await refreshAuditSnapshot();
    var enrichedSteps = withAuditNotes(baseSteps, _lastAuditSnapshot);
    maybeRenderStepGrid(enrichedSteps);
    maybeRenderWorkbenchSummary(up?.summary);
    syncServerLogs(up?.logs || [], true);

    el('btnUpdateAll').disabled = false;
    el('btnUpdateAll').textContent = '智能更新';
    el('btnStop').disabled = false;
    el('btnStop').textContent = '停止';
    el('btnStop').style.display = 'none';
  }

  function renderStepGrid(steps) {
    if (!steps || !steps.length) { el('stepGrid').innerHTML = ''; return; }

    var GROUP_MAP = {
      sync_raw: 'data', match_inst: 'data', sync_market_data: 'data', sync_financial: 'data', sync_industry: 'data',
      gen_events: 'calc', calc_returns: 'calc', calc_financial_derived: 'calc',
      build_current_rel: 'mart', build_profiles: 'mart', build_industry_stat: 'mart', build_trends: 'mart', calc_screening: 'mart', calc_sector_momentum: 'mart', calc_inst_scores: 'mart', calc_stock_scores: 'mart'
    };
    // 单点定义：每列的身份（序号 + 名称 + 操作动词 + 步骤总数）
    // CSS 颜色由 .data-col / .calc-col / .mart-col 各自的 --col-color 提供
    var GROUP_DEF = {
      data: { name: '数据获取', verb: '同步数据', count: 5, badge: '①' },
      calc: { name: '事实计算', verb: '全量计算', count: 3, badge: '②' },
      mart: { name: '集市构建', verb: '重构集市', count: 8, badge: '③' }
    };
    var grouped = { data: [], calc: [], mart: [] };

    steps.forEach(function (s) {
      var g = GROUP_MAP[s.step_id] || 'mart';
      grouped[g].push(s);
    });

    var html = '';
    ['data', 'calc', 'mart'].forEach(function (gId) {
      var cardsHtml = grouped[gId].map(function (s) {
        var cardStatus = s.display_status || s.status || 'idle';
        var color = STATUS_COLORS[cardStatus] || '#e2e8f0';
        var statusText = { completed: '完成', partial: '有缺口', failed: '失败', blocked: '阻断', running: '运行中', pending: '等待执行', skipped: '本轮跳过', stopped: '已停止', idle: '' }[cardStatus] || '';
        if (_stopRequestedUi && cardStatus === 'running') statusText = '停止中';
        var timeStr = fmtTime(s.finished_at || s.started_at);
        var detailHtml = s.step_id === 'sync_market_data'
          ? renderMarketSyncDetail(s)
          : (s.step_id === 'sync_industry' ? renderIndustrySyncDetail(s) : '');
        var canShowAction = !!s.actionable;
        var actionable = canShowAction && !_uiRunning;
        var reasonStr = '';
        if (s.error && !s.detail && !['completed', 'partial', 'idle'].includes(cardStatus)) {
          reasonStr = '<div style="font-size:10px;color:' + ((cardStatus === 'failed') ? '#ef4444' : '#f59e0b') + ';margin-top:2px">' + esc(normalizeStepReason(s)).substring(0, 40) + '</div>';
        }
        var auditHtml = (s.audit_notes || []).map(function (note) {
          var formatted = formatAuditNote(note);
          var noteColor = formatted.tone === 'issue' ? '#ef4444' : '#64748b';
          return '<div style="font-size:10px;color:' + noteColor + ';margin-top:2px">' + formatted.html + '</div>';
        }).join('');
        var actionHtml = canShowAction
          ? '<button type="button" class="btn-sm step-card-action-btn" data-step-id="' + esc(s.step_id) + '" data-step-name="' + esc(s.step_name || s.step_id) + '"' + (_uiRunning ? ' disabled title="当前有任务在运行，暂不可并行执行"' : '') + '>' + esc(s.action_label || '单独补跑') + '</button>'
          : '';
        var toneClass = '';
        if (cardStatus === 'partial' || cardStatus === 'failed') toneClass = ' audit-issue';
        else if (cardStatus === 'completed') toneClass = ' audit-ok';
        else if (cardStatus === 'blocked' || cardStatus === 'skipped' || cardStatus === 'stopped') toneClass = ' audit-warn';
        var stepDesc = s.desc || '';
        return '<div class="step-card ' + esc(cardStatus) + toneClass + (actionable ? ' actionable' : '') + '" style="border-top:3px solid ' + color + '"' +
          (actionable ? ' data-step-id="' + esc(s.step_id) + '" data-step-name="' + esc(s.step_name || s.step_id) + '" tabindex="0" role="button" title="' + esc(s.action_label || '单独补跑') + '"' : '') + '>' +
          '<div class="step-card-head"><div>' +
          '<div style="font-size:12px;font-weight:600;min-width:0">' + esc(s.step_name || s.step_id) + '</div>' +
          '</div>' + actionHtml + '</div>' +
          '<div style="font-size:11px;color:' + color + '">' + statusText + '</div>' +
          (cardStatus === 'idle' && stepDesc ? '<div class="step-card-desc">' + esc(stepDesc) + '</div>' : '') +
          (s.records ? '<div style="font-size:11px;color:#64748b">' + fmt(s.records) + '条</div>' : '') +
          detailHtml +
          auditHtml +
          (timeStr ? '<div style="font-size:10px;color:#94a3b8;margin-top:2px">' + timeStr + '</div>' : '') +
          reasonStr + '</div>';
      }).join('');

      var def = GROUP_DEF[gId];
      var total = def.count;
      var completedCount = grouped[gId].filter(function (s) { return s.status === 'completed'; }).length;
      var statText = total + '步 · ' + completedCount + '/' + total + ' 完成';

      html += '<div class="step-group-col ' + gId + '-col">' +
        '<div class="step-group-header">' +
        '<div class="group-title">' +
        '<span class="group-badge" aria-hidden="true">' + def.badge + '</span>' +
        '<span class="group-name">' + def.name + '</span>' +
        '<button type="button" class="step-group-run-btn" data-group="' + gId + '"' + (_uiRunning ? ' disabled' : '') + '>' + def.verb + '</button>' +
        '</div>' +
        '<div class="group-stats">' + statText + '</div>' +
        '</div>' +
        '<div class="step-group-cards">' + cardsHtml + '</div>' +
        '</div>';
    });

    el('stepGrid').innerHTML = html;

    el('stepGrid').querySelectorAll('.step-group-run-btn').forEach(function (btn) {
      btn.addEventListener('click', function () {
        if (_uiRunning) return;
        runGroup(btn.dataset.group, btn.textContent);
      });
    });

    el('stepGrid').querySelectorAll('.step-card.actionable').forEach(function (card) {
      card.addEventListener('click', function (e) {
        if (e.target.closest('.step-card-action-btn') || e.target.closest('.step-group-run-btn')) return;
        runSingleStep(card.dataset.stepId, card.dataset.stepName);
      });
      card.addEventListener('keydown', function (e) {
        if (e.key === 'Enter' || e.key === ' ') {
          e.preventDefault();
          runSingleStep(card.dataset.stepId, card.dataset.stepName);
        }
      });
    });
    el('stepGrid').querySelectorAll('.step-card-action-btn').forEach(function (btn) {
      btn.addEventListener('click', function (e) {
        e.stopPropagation();
        runSingleStep(btn.dataset.stepId, btn.dataset.stepName);
      });
    });
  }
  function renderIdleStepGrid() {
    renderStepGrid(STEP_DEFAULTS);
  }

  async function runGroup(groupId, groupName) {
    if (!groupId) return;
    if (_uiRunning) {
      await modalAlert('当前已有更新任务在运行，请稍候。');
      return;
    }

    // groupId is one of 'data' (sync), 'calc' (calc), 'mart' (mart)
    var endpoint = groupId === 'data' ? '/api/inst/update/sync' : '/api/inst/update/' + groupId;
    var r = await api(endpoint, { method: 'POST' });
    if (!r || !r.ok) {
      addLog('启动失败: ' + (r ? r.message || r.error : '未知错误'), true);
      return;
    }

    el('btnUpdateAll').disabled = true; el('btnUpdateAll').textContent = '更新中...';
    el('btnStop').disabled = false; el('btnStop').textContent = '停止';
    el('btnStop').style.display = ''; el('progressArea').style.display = '';

    _activeRunContext = { mode: groupId, step_ids: r.step_ids || [] };
    _uiRunning = true;
    _stopRequestedUi = false;

    if (_poll) clearInterval(_poll);
    _poll = setInterval(pollStatus, 1000);
    pollStatus();
  }

  var _poll = null;
  var _lastServerLogId = 0;
  var _pollRequestSeq = 0;
  var _pollAppliedSeq = 0;
  var _pollInFlight = false;
  var _lastStepGridSig = '';
  var _lastWorkbenchSummarySig = '';
  async function startUpdate() {
    _activeRunContext = null;
    _lastRunContext = null;
    _uiRunning = true;
    _stopRequestedUi = false;
    el('btnUpdateAll').disabled = true; el('btnUpdateAll').textContent = '分析中...';
    el('btnStop').disabled = false; el('btnStop').textContent = '停止';
    el('btnStop').style.display = ''; el('progressArea').style.display = '';
    el('updateLog').innerHTML = ''; _lastServerLogId = 0; addLog('正在分析数据状态...');
    // 使用智能更新：先审计再决定跑什么
    var r = await api('/api/inst/update/smart', { method: 'POST' });
    if (!r || !r.ok) {
      if (r && r.message) addLog(r.message);
      else addLog('启动失败', true);
      el('btnUpdateAll').disabled = false; el('btnUpdateAll').textContent = '智能更新';
      el('btnStop').style.display = 'none';
      return;
    }
    if (r.noop || !r.steps) {
      _uiRunning = false;
      _activeRunContext = null;
      el('btnUpdateAll').disabled = false;
      el('btnUpdateAll').textContent = '智能更新';
      el('btnStop').style.display = 'none';
      await refreshDashboardStatus(true);
      addLog(r.message || '数据已是最新，无需更新');
      return;
    }
    _activeRunContext = { mode: 'smart', step_ids: Array.isArray(r.step_ids) ? r.step_ids : [] };
    // 显示计划摘要
    if (r.plan) {
      var reasons = r.plan.reason || [];
      reasons.forEach(function (reason) { addLog('计划: ' + reason); });
      var skipReasons = r.plan.skip_reasons || {};
      Object.keys(skipReasons).forEach(function (stepId) {
        var step = STEP_DEFAULTS.find(function (item) { return item.step_id === stepId; });
        var name = step ? step.step_name : stepId;
        addLog('跳过: ' + name + ' · ' + skipReasons[stepId]);
      });
      addLog('开始执行 ' + (r.steps || 0) + ' 个步骤...');
    }
    el('btnUpdateAll').textContent = '更新中...';
    renderStepGrid(makePlannedSteps(r.step_ids, r.plan?.skip_reasons));
    renderWorkbenchSummary({
      kind: 'running',
      pct: 0,
      message: '智能更新 · 0/' + (r.steps || 0) + ' · 0%',
      counts: { total: r.steps || 0, done: 0 },
      show_progress: true
    });
    if (_poll) clearInterval(_poll);
    await pollStatus();
    if (_uiRunning) _poll = setInterval(pollStatus, 1000);
  }
  async function pollStatus() {
    if (_pollInFlight) return;
    var requestSeq = ++_pollRequestSeq;
    _pollInFlight = true;
    var r = await api('/api/inst/update/status');
    _pollInFlight = false;
    if (!r) return;
    if (requestSeq < _pollAppliedSeq) return;
    _pollAppliedSeq = requestSeq;
    if (r.run_context) _activeRunContext = r.run_context;
    if (r.last_run_context) _lastRunContext = r.last_run_context;
    _stopRequestedUi = !!r.stop_requested;
    var effectiveRunning = !!r.running;
    if (
      effectiveRunning &&
      r.summary?.counts?.total > 0 &&
      r.summary?.counts?.done >= r.summary?.counts?.total &&
      (!r.summary?.active_step_ids || !r.summary.active_step_ids.length)
    ) {
      effectiveRunning = false;
    }
    if (r.steps) {
      if (effectiveRunning) {
        if (!_lastAuditSnapshot && !_auditRefreshPromise) refreshAuditSnapshot();
      } else if (!_lastAuditSnapshot) {
        await refreshAuditSnapshot();
      }
      var steps = withAuditNotes(r.steps, _lastAuditSnapshot || {});
      maybeRenderStepGrid(steps);
      maybeRenderWorkbenchSummary(r.summary);
      syncServerLogs(r.logs || [], false);
      if (effectiveRunning) {
        el('btnUpdateAll').textContent = r.stop_requested ? '停止中...' : '更新中...';
        el('btnStop').disabled = !!r.stop_requested;
        el('btnStop').textContent = r.stop_requested ? '停止中...' : '停止';
        return;
      }
      clearInterval(_poll);
      _poll = null;
      _uiRunning = false;
      _lastRunContext = r.last_run_context || _activeRunContext || _lastRunContext;
      _activeRunContext = null;
      el('btnUpdateAll').disabled = false; el('btnUpdateAll').textContent = '智能更新';
      el('btnStop').style.display = 'none';
      await renderUpdatePanel(r);
    }
  }

  async function runSingleStep(stepId, stepName) {
    if (!stepId) return;
    if (_uiRunning) {
      await modalAlert('当前已有更新任务在运行，请稍候。');
      return;
    }
    var r = await api('/api/inst/update/step/' + encodeURIComponent(stepId), { method: 'POST' });
    if (!r || !r.ok) {
      await modalAlert((r && (r.message || r.error)) || '单步执行启动失败');
      return;
    }
    _activeRunContext = { mode: 'single', step_id: stepId, step_name: stepName || stepId };
    if (Array.isArray(r.steps) && r.steps.length) _activeRunContext.step_ids = r.steps;
    _lastRunContext = null;
    _uiRunning = true;
    el('btnUpdateAll').disabled = true;
    el('btnUpdateAll').textContent = '更新中...';
    el('btnStop').disabled = false;
    el('btnStop').textContent = '停止';
    el('btnStop').style.display = '';
    el('progressArea').style.display = '';
    el('updateLog').innerHTML = '';
    _lastServerLogId = 0;
    addLog('开始单独执行: ' + (stepName || stepId));
    if (Array.isArray(r.steps) && r.steps.length > 1) {
      var names = r.steps.slice(1).map(function (id) {
        var step = STEP_DEFAULTS.find(function (s) { return s.step_id === id; });
        return step ? step.step_name : id;
      });
      if (names.length) addLog('将自动续跑下游: ' + names.join(' -> '));
    }
    var status = await api('/api/inst/update/status');
    if (status?.steps) {
      if (!_lastAuditSnapshot && !_auditRefreshPromise) refreshAuditSnapshot();
      var steps = withAuditNotes(status.steps, _lastAuditSnapshot || {});
      maybeRenderStepGrid(steps);
      maybeRenderWorkbenchSummary(status.summary);
      syncServerLogs(status.logs || [], Array.isArray(status.logs) && status.logs.length > 0);
    }
    if (_poll) clearInterval(_poll);
    _poll = setInterval(pollStatus, 1000);
  }

  function logClass(level) {
    if (level === 'error') return 'log-err';
    if (level === 'warning' || level === 'warn') return 'log-warn';
    return 'log-ok';
  }

  function appendLogEntry(entry) {
    var box = el('updateLog');
    var t = entry.ts
      ? new Date(entry.ts).toLocaleTimeString('zh-CN', { hour12: false })
      : new Date().toLocaleTimeString('zh-CN', { hour12: false });
    box.innerHTML += '<div class="log-line"><span class="log-time">' + esc(t) + '</span><span class="' + logClass(entry.level) + '">' + esc(entry.message) + '</span></div>';
    box.scrollTop = box.scrollHeight;
  }

  function syncServerLogs(logs, replace) {
    if (!Array.isArray(logs)) return;
    if (replace) {
      if (!logs.length && el('updateLog').innerHTML.trim()) return;
      el('updateLog').innerHTML = '';
      _lastServerLogId = 0;
    }
    logs.forEach(function (log) {
      if (!replace && typeof log.id === 'number' && log.id <= _lastServerLogId) return;
      appendLogEntry(log);
      if (typeof log.id === 'number' && log.id > _lastServerLogId) _lastServerLogId = log.id;
    });
  }

  function addLog(msg, isErr) {
    appendLogEntry({
      ts: new Date().toISOString(),
      level: isErr ? 'error' : 'info',
      message: msg
    });
  }

  // ============================================================
  // Institution Management (inside Research)
  // ============================================================
  var _mgmtData = [];
  var _mgmtActiveType = 'all';  // 记住当前选中的分类
  var _chunkTableRenderSeq = 0;

  function renderChunkedTable(containerEl, options) {
    if (!containerEl) return;
    var rows = options.rows || [];
    var head = options.head || '';
    var emptyColspan = options.emptyColspan || 1;
    var emptyText = options.emptyText || '暂无数据';
    var chunkSize = options.chunkSize || 120;
    var progressLabel = options.progressLabel || '正在渲染';
    if (!rows.length) {
      containerEl.innerHTML = head + '<tr><td class="empty-row" colspan="' + emptyColspan + '">' + emptyText + '</td></tr></tbody></table>';
      if (typeof options.afterRender === 'function') options.afterRender(containerEl, true);
      return;
    }
    var seq = ++_chunkTableRenderSeq;
    containerEl.innerHTML =
      '<div class="muted" style="font-size:11px;margin-bottom:8px">' + progressLabel + ' 0 / ' + rows.length + ' ...</div>' +
      head + '</tbody></table>';
    var hint = containerEl.querySelector('.muted');
    var tbody = containerEl.querySelector('tbody');
    var offset = 0;

    function flushChunk() {
      if (seq !== _chunkTableRenderSeq || !tbody) return;
      var slice = rows.slice(offset, Math.min(offset + chunkSize, rows.length));
      tbody.insertAdjacentHTML('beforeend', slice.join(''));
      offset += slice.length;
      if (hint) {
        hint.textContent = offset < rows.length
          ? (progressLabel + ' ' + offset + ' / ' + rows.length + ' ...')
          : ('共 ' + rows.length + ' 行，已完成渲染');
      }
      if (offset < rows.length) {
        requestAnimationFrame(flushChunk);
        return;
      }
      if (typeof options.afterRender === 'function') options.afterRender(containerEl, false);
    }

    requestAnimationFrame(flushChunk);
  }

  async function loadInstMgmt() {
    var r = await api('/api/inst/institutions?show=all');
    if (!r?.data) return;
    _mgmtData = r.data;
    var types = [...new Set(r.data.map(i => i.type).filter(Boolean))];
    var filterEl = el('mgmtTypeFilter');
    filterEl.innerHTML = typeTag('all', '全部 ' + r.data.length) + types.map(t => typeTag(t, t + ' ' + r.data.filter(i => i.type === t).length)).join('');
    filterEl.querySelectorAll('.type-tag').forEach(tag => {
      tag.addEventListener('click', () => {
        filterEl.querySelectorAll('.type-tag').forEach(t => t.classList.remove('active'));
        tag.classList.add('active');
        _mgmtActiveType = tag.dataset.type;
        renderMgmtTable(r.data, tag.dataset.type);
      });
    });
    // 恢复之前选中的分类
    var activeTag = filterEl.querySelector('.type-tag[data-type="' + _mgmtActiveType + '"]') || filterEl.querySelector('.type-tag');
    if (activeTag) activeTag.classList.add('active');
    renderMgmtTable(r.data, _mgmtActiveType);
  }

  function renderMgmtTable(data, tf) {
    var d = tf === 'all' ? data : data.filter(i => i.type === tf);
    var head = '<table class="data-table"><thead><tr><th style="width:30px"><input type="checkbox" class="row-check" id="checkAll"></th><th>机构名称</th><th>简称</th><th>类型</th><th>状态</th><th>操作</th></tr></thead><tbody>';
    renderChunkedTable(el('instMgmtTable'), {
      head: head,
      rows: d.map(function (i) {
        var st = i.blacklisted ? evTag('exit', '已拉黑') : i.merged_into ? evTag('unchanged', '已合并') : !i.enabled ? evTag('unchanged', '禁用') : evTag('new_entry', '正常');
        var ops = '';
        if (i.merged_into || i.blacklisted || !i.enabled) {
          ops = '<button class="btn-sm success" onclick="App.restoreInst(\'' + esc(i.id) + '\')">恢复</button> ' +
            '<button class="btn-sm danger" onclick="App.deleteInst(\'' + esc(i.id) + '\')">删除</button>';
        } else {
          ops = '<button class="btn-sm" onclick="App.setAlias(\'' + esc(i.id) + '\')">简称</button> ' +
            '<button class="btn-sm" onclick="App.setType(\'' + esc(i.id) + '\')">类型</button> ' +
            '<button class="btn-sm warn" onclick="App.toggleBlack(\'' + esc(i.id) + '\',1)">拉黑</button> ' +
            '<button class="btn-sm danger" onclick="App.deleteInst(\'' + esc(i.id) + '\')">删除</button>';
        }
        return '<tr' + (i.blacklisted || i.merged_into ? ' style="opacity:0.6"' : '') + '>' +
          '<td><input type="checkbox" class="row-check" data-id="' + esc(i.id) + '"></td>' +
          '<td><span class="clickable-name" onclick="App.toggleInstDetail(\'' + esc(i.id) + '\',this)">' + esc(i.name || '') + '</span></td><td>' + esc(i.display_name || '') + '</td>' +
          '<td>' + typeTag(i.type) + '</td><td>' + st + '</td>' +
          '<td style="white-space:nowrap">' + ops + '</td></tr>';
      }),
      emptyColspan: 6,
      emptyText: '暂无机构',
      chunkSize: 80,
      progressLabel: '正在渲染机构',
      afterRender: function () {
        document.getElementById('checkAll')?.addEventListener('change', function () {
          document.querySelectorAll('#instMgmtTable .row-check[data-id]').forEach(cb => cb.checked = this.checked);
        });
        scheduleSortableTables('instMgmtTable');
      }
    });
  }

  function getCheckedIds() {
    return [...document.querySelectorAll('#instMgmtTable .row-check[data-id]:checked')].map(cb => cb.dataset.id);
  }

  async function restoreInst(id) {
    if (!await modalConfirm('恢复该机构为正常状态？')) return;
    await api('/api/inst/institutions/' + id, { method: 'PUT', body: JSON.stringify({ blacklisted: 0, enabled: 1, merged_into: null }) });
    loadInstMgmt();
  }
  async function setAlias(id) { var v = await modalPrompt('设置简称'); if (v === null) return; await api('/api/inst/institutions/' + id, { method: 'PUT', body: JSON.stringify({ display_name: v }) }); loadInstMgmt(); }
  async function setType(id) { var v = await modalTypeSelect('设置类型'); if (!v) return; await api('/api/inst/institutions/' + id, { method: 'PUT', body: JSON.stringify({ type: v }) }); loadInstMgmt(); }
  async function toggleBlack(id, val) { await api('/api/inst/institutions/' + id, { method: 'PUT', body: JSON.stringify({ blacklisted: val }) }); loadInstMgmt(); }
  async function deleteInst(id) { if (!await modalConfirm('确定删除？')) return; await api('/api/inst/institutions/' + id, { method: 'DELETE' }); loadInstMgmt(); }
  async function batchAlias() {
    var ids = getCheckedIds(); if (!ids.length) { await modalAlert('请先勾选'); return; }
    var fr = await modalFindReplace('批量简称 — 查找替换');
    if (!fr) return;
    var changed = 0;
    for (var id of ids) {
      var inst = _mgmtData.find(function (i) { return i.id === id });
      if (!inst) continue;
      // 从当前简称（有则用）或原名开始替换
      var name = inst.display_name || inst.name || '';
      var newName = name;
      // 逐个替换每个查找词
      for (var fi = 0; fi < fr.finds.length; fi++) {
        newName = newName.split(fr.finds[fi]).join(fr.replace);
      }
      newName = newName.trim();
      if (newName && newName !== (inst.display_name || '')) {
        await api('/api/inst/institutions/' + id, { method: 'PUT', body: JSON.stringify({ display_name: newName }) });
        changed++;
      }
    }
    await modalAlert('已更新 ' + changed + ' 个机构的简称');
    loadInstMgmt();
  }
  async function batchBlack() {
    var ids = getCheckedIds(); if (!ids.length) { await modalAlert('请先勾选'); return; }
    if (!await modalConfirm('确定拉黑选中的 ' + ids.length + ' 个机构？')) return;
    for (var id of ids) await api('/api/inst/institutions/' + id, { method: 'PUT', body: JSON.stringify({ blacklisted: 1 }) });
    loadInstMgmt();
  }
  async function searchInst() {
    var kw = el('mgmtSearch').value.trim();
    if (!kw) { modalAlert('请输入关键词'); return; }
    var holderType = el('searchHolderType')?.value || '';
    el('searchResultArea').style.display = '';
    el('searchResults').innerHTML = '<div class="muted">搜索中...</div>';
    var url = '/api/inst/institutions/search?keywords=' + encodeURIComponent(kw);
    if (holderType) url += '&holder_type=' + encodeURIComponent(holderType);
    var r = await api(url);
    if (!r?.ok || !r.data) { el('searchResults').innerHTML = '<div class="muted">搜索失败</div>'; return; }
    el('searchResultCount').textContent = '共 ' + r.total + ' 个结果' + (r.keywords ? '（关键词: ' + r.keywords.join(' + ') + '）' : '');
    if (!r.data.length) { el('searchResults').innerHTML = '<div class="muted">未找到匹配的机构</div>'; return; }
    var head = '<table class="data-table"><thead><tr><th style="width:30px"><input type="checkbox" id="searchCheckAll"></th><th>机构名称</th><th>东财分类</th><th>当前持仓数</th><th>最新公告</th><th>状态</th></tr></thead><tbody>';
    renderChunkedTable(el('searchResults'), {
      head: head,
      rows: r.data.map(function (item) {
        var st = item.tracked ? '<span style="color:#10b981">已跟踪</span>' : '<span style="color:#94a3b8">未跟踪</span>';
        var disabled = item.tracked ? ' disabled' : '';
        return '<tr><td><input type="checkbox" class="search-check" data-name="' + esc(item.holder_name) + '"' + disabled + '></td>' +
          '<td>' + esc(item.holder_name) + '</td><td>' + esc(item.holder_type || '') + '</td><td>' + item.stock_count + '</td>' +
          '<td>' + fmtDate(item.latest_notice) + '</td><td>' + st + '</td></tr>';
      }),
      emptyColspan: 6,
      emptyText: '未找到匹配的机构',
      chunkSize: 80,
      progressLabel: '正在渲染搜索结果',
      afterRender: function () {
        document.getElementById('searchCheckAll')?.addEventListener('change', function () {
          document.querySelectorAll('.search-check:not(:disabled)').forEach(function (cb) { cb.checked = this.checked; }.bind(this));
        });
        scheduleSortableTables('searchResults');
      }
    });
  }

  async function importChecked() {
    // 获取标签
    var typeVal = el('addInstType').value;
    if (!typeVal) { await modalAlert('请选择标签类型（必填）'); return; }
    // 获取选中的机构
    var names = [];
    document.querySelectorAll('.search-check:checked:not(:disabled)').forEach(function (cb) { names.push(cb.dataset.name); });
    if (!names.length) { modalAlert('请勾选要导入的机构'); return; }
    if (!await modalConfirm('确认导入 ' + names.length + ' 个机构，标签: ' + typeVal + '？')) return;
    // 批量添加
    var items = names.map(function (n) { return { name: n, type: typeVal }; });
    var r = await api('/api/inst/institutions/batch', { method: 'POST', body: JSON.stringify({ institutions: items }) });
    if (r?.ok) {
      modalAlert('已导入 ' + (r.created || names.length) + ' 个机构，系统正在后台自动匹配持仓和计算数据...');
      el('searchResultArea').style.display = 'none';
      el('mgmtSearch').value = '';
      loadInstMgmt();
    } else { modalAlert('导入失败'); }
  }
  async function batchType() {
    var ids = getCheckedIds(); if (!ids.length) { await modalAlert('请先勾选'); return; }
    var type = await modalTypeSelect('批量设类型'); if (!type) return;
    for (var id of ids) await api('/api/inst/institutions/' + id, { method: 'PUT', body: JSON.stringify({ type: type }) });
    loadInstMgmt();
  }
  async function batchMerge() {
    var ids = getCheckedIds(); if (ids.length < 2) { modalAlert('至少勾选2个'); return; }
    var target = prompt('合并到哪个？输入目标机构ID（第一个勾选的ID: ' + ids[0] + '）:', ids[0]); if (!target) return;
    for (var id of ids) { if (id !== target) await api('/api/inst/institutions/' + id, { method: 'PUT', body: JSON.stringify({ merged_into: target, enabled: 0 }) }); }
    loadInstMgmt();
  }
  async function batchDelete() {
    var ids = getCheckedIds(); if (!ids.length) { modalAlert('请先勾选'); return; }
    if (!await modalConfirm('确定删除 ' + ids.length + ' 个机构？')) return;
    for (var id of ids) await api('/api/inst/institutions/' + id, { method: 'DELETE' });
    loadInstMgmt();
  }

  // ============================================================
  // Lightweight Panels
  // ============================================================
  function togglePanel(buttonId, panelId, openText, closeText) {
    var btn = el(buttonId), panel = el(panelId);
    if (!btn || !panel) return;
    var isOpen = panel.style.display !== 'none';
    var nextOpen = !isOpen;
    panel.style.display = nextOpen ? '' : 'none';
    btn.textContent = nextOpen ? closeText : openText;
    btn.classList.toggle('active', nextOpen);
    if (!btn.classList.contains('pill-tab-btn')) {
      btn.style.borderColor = nextOpen ? '#93c5fd' : '';
      btn.style.color = nextOpen ? '#2563eb' : '';
      btn.style.background = nextOpen ? '#eff6ff' : '';
    }
  }

  async function resetDerivedData() {
    if (!await modalConfirm('确认清空事件、收益、画像、关系和股票列表等派生层，并在之后重新计算吗？')) return;
    var r = await api('/api/inst/update/reset-derived', { method: 'POST' });
    if (r?.ok) {
      await modalAlert(r.message || '已重置派生数据');
      loadDashboard();
    } else {
      await modalAlert((r && (r.message || r.error)) || '重置失败');
    }
  }

  async function copyLogs() {
    var lines = Array.from(document.querySelectorAll('#updateLog .log-line')).map(function (line) {
      return line.innerText.trim();
    }).filter(Boolean);
    if (!lines.length) {
      await modalAlert('当前没有可复制的日志');
      return;
    }
    var text = lines.join('\n');
    try {
      await navigator.clipboard.writeText(text);
      await modalAlert('运行日志已复制到剪贴板');
    } catch (e) {
      var textarea = document.createElement('textarea');
      textarea.value = text;
      document.body.appendChild(textarea);
      textarea.select();
      document.execCommand('copy');
      document.body.removeChild(textarea);
      await modalAlert('运行日志已复制到剪贴板');
    }
  }

  // ============================================================
  // Exclusions
  // ============================================================
  async function loadExclusions() {
    var r = await api('/api/inst/exclusions/categories');
    var cats = r?.data?.length ? r.data : [
      { category: 'ST', label: 'ST/*ST — 退市风险警示', enabled: 1 }, { category: 'DELISTED', label: '退市股', enabled: 1 },
      { category: 'OLD_OTC', label: '老三板 (400xxx)', enabled: 1 }, { category: 'BSE', label: '北交所', enabled: 1 },
      { category: 'NEEQ', label: '新三板', enabled: 1 }, { category: 'B_SHARE', label: 'B股', enabled: 1 },
      { category: 'CDR', label: 'CDR存托凭证 (689xxx)', enabled: 1 },
      { category: 'SCIENCE', label: '科创板 (688xxx)', enabled: 0 }, { category: 'GEM', label: '创业板 (300xxx)', enabled: 0 },
    ];
    el('exclusionCategories').innerHTML = cats.map(c =>
      '<div class="excl-category"><button class="excl-toggle' + (c.enabled ? ' on' : '') + '" data-cat="' + esc(c.category) + '"></button><span class="excl-label">' + esc(c.label || c.category) + '</span></div>'
    ).join('');
    el('exclusionCategories').querySelectorAll('.excl-toggle').forEach(btn => btn.addEventListener('click', () => btn.classList.toggle('on')));
  }

  // ============================================================
  // Detail Panels (click to expand)
  // ============================================================
  function removeDetailPanel(panelEl) {
    if (panelEl && panelEl.parentNode) panelEl.parentNode.removeChild(panelEl);
  }

  function toggleInstDetail(instId, clickedEl) {
    // If already open, close it
    var existing = document.querySelector('.detail-panel[data-detail-inst="' + instId + '"]');
    if (existing) { removeDetailPanel(existing.closest('tr.detail-row') || existing); clickedEl.removeAttribute('data-detail-open'); return; }
    // Close any other open inst detail
    document.querySelectorAll('.detail-panel[data-detail-inst]').forEach(function (p) { removeDetailPanel(p.closest('tr.detail-row') || p); });
    document.querySelectorAll('[data-detail-open="inst"]').forEach(function (e) { e.removeAttribute('data-detail-open'); });
    clickedEl.setAttribute('data-detail-open', 'inst');

    // Determine context: inside a table row or inside a card grid
    var tr = clickedEl.closest('tr');
    var cardEl = clickedEl.closest('.inst-card');
    var panel = document.createElement('div');
    panel.className = 'detail-panel';
    panel.setAttribute('data-detail-inst', instId);
    panel.innerHTML = '<div class="detail-loading">加载中...</div>';

    if (tr) {
      // Table context: insert a new tr below
      var colCount = tr.children.length;
      var detailTr = document.createElement('tr');
      detailTr.className = 'detail-row';
      var td = document.createElement('td');
      td.setAttribute('colspan', colCount);
      td.appendChild(panel);
      detailTr.appendChild(td);
      tr.parentNode.insertBefore(detailTr, tr.nextSibling);
    } else if (cardEl) {
      // Card grid context: insert after the card, spanning full width
      cardEl.parentNode.insertBefore(panel, cardEl.nextSibling);
    } else {
      // Fallback: insert after clicked element
      clickedEl.parentNode.insertBefore(panel, clickedEl.nextSibling);
    }

    // Fetch detail + profile + chart data in parallel
    Promise.all([
      api('/api/inst/profiles/detail/' + encodeURIComponent(instId)),
      api('/api/inst/profiles'),
      api('/api/inst/profiles/returns-history/' + encodeURIComponent(instId))
    ]).then(function (results) {
      var r = results[0], profilesResp = results[1], chartResp = results[2];
      if (!r || !r.ok) { panel.innerHTML = '<div class="detail-loading">加载失败</div>'; return; }
      var holdings = r.data || [];

      // 从 profiles 中找到该机构的画像
      var p = (profilesResp?.data || []).find(function (x) { return x.institution_id === instId }) || {};

      // 画像摘要 + 收益曲线
      var chartSvg = (chartResp?.ok && chartResp.data?.length) ? buildReturnsSvg(chartResp.data, 400, 60) : '<span class="muted">暂无收益数据</span>';
      var html = '<div style="display:flex;gap:16px;margin-bottom:12px;flex-wrap:wrap;align-items:flex-start">' +
        '<div style="flex:1;min-width:280px">' +
        '<div style="display:flex;flex-wrap:wrap;gap:12px;font-size:12px">' +
        metric('实力分', p.quality_score != null ? Number(p.quality_score).toFixed(1) : '-') +
        metric('可跟分', p.followability_score != null ? Number(p.followability_score).toFixed(1) : '-') +
        metric('历史胜率', pct(p.total_win_rate)) + metric('30日胜率', pct(p.win_rate_30d)) +
        metric('60日胜率', pct(p.win_rate_60d)) + metric('最大回撤', p.median_max_drawdown_30d != null ? '-' + p.median_max_drawdown_30d.toFixed(1) + '%' : '-') +
        metric('30日均', fmtGain(p.avg_gain_30d)) + metric('60日均', fmtGain(p.avg_gain_60d)) + metric('120日均', fmtGain(p.avg_gain_120d)) +
        metric('持仓', (p.current_stock_count || 0) + '只') + metric('资金', compactNum(p.current_total_cap)) +
        '</div>' +
        '<div style="display:flex;flex-wrap:wrap;gap:12px;font-size:12px;margin-top:12px">' +
        metric('平均跟随溢价', premiumText(p.avg_premium_pct)) +
        metric('安全跟随样本', fmt(p.safe_follow_event_count || 0)) +
        metric('安全跟随30日胜率', pct(p.safe_follow_win_rate_30d)) +
        metric('安全跟随30日均', fmtGain(p.safe_follow_avg_gain_30d)) +
        metric('安全跟随平均回撤', p.safe_follow_avg_drawdown_30d != null ? '-' + Number(p.safe_follow_avg_drawdown_30d).toFixed(1) + '%' : '-') +
        metric('信号传递效率', p.signal_transfer_efficiency_30d != null ? Number(p.signal_transfer_efficiency_30d).toFixed(0) + '%' : '-') +
        metric('可跟性提示', esc(p.followability_hint || '-')) +
        '</div>' +
        '</div>' +
        '<div style="flex:0 0 420px;border:1px solid #f1f5f9;border-radius:6px;padding:6px;background:#fafbfc">' + chartSvg + '</div>' +
        '</div>';

      // Phase 4: 评分拆解入口
      html += '<div style="margin:8px 0"><button class="btn-sm" style="font-size:11px" onclick="App.toggleInstBreakdown(\'' + esc(instId) + '\')">评分拆解</button></div>' +
        '<div id="breakdown-' + esc(instId) + '" style="display:none"></div>';

      // 行业分布 + 业绩表现
      var indSummary = r.industry_summary || [];
      if (indSummary.length) {
        html += '<div style="margin-bottom:14px">' +
          '<table class="data-table" style="font-size:12px"><thead><tr>' +
          '<th style="text-align:left">行业</th><th>持仓</th><th>占比</th><th>胜率</th><th>30日均</th>' +
          '</tr></thead><tbody>';
        indSummary.forEach(function (l1) {
          // 一级行业 = 分组行
          var barW = Math.min(l1.pct, 100);
          html += '<tr style="background:#f8fafc;cursor:pointer" onclick="var s=this.nextElementSibling;while(s&&!s.classList.contains(\'ind-l1\')){s.style.display=s.style.display===\'none\'?\'table-row\':\'none\';s=s.nextElementSibling;}">' +
            '<td style="font-weight:600;padding-left:8px">' + esc(l1.level1) + ' <div style="background:#e2e8f0;height:4px;border-radius:2px;width:100px;display:inline-block;vertical-align:middle;margin-left:6px"><div style="background:#3b82f6;height:100%;border-radius:2px;width:' + barW + '%"></div></div></td>' +
            '<td style="text-align:center">' + l1.stock_count + '只</td>' +
            '<td style="text-align:center">' + l1.pct + '%</td>' +
            '<td style="text-align:center">' + (l1.win_rate_30d != null ? ('<span style="color:' + (Number(l1.win_rate_30d) >= 60 ? '#10b981' : Number(l1.win_rate_30d) >= 50 ? '#f59e0b' : '#ef4444') + '">' + Number(l1.win_rate_30d).toFixed(0) + '%</span>') : '-') + '</td>' +
            '<td style="text-align:center">' + (l1.avg_gain_30d != null ? fmtGain(l1.avg_gain_30d) : '-') + '</td></tr>';
          // 二级行业 = 数据行（默认隐藏）
          (l1.children || []).forEach(function (l2) {
            var wrHtml = '-', gainHtml = '-';
            if (l2.win_rate_30d != null) {
              var wr = Number(l2.win_rate_30d);
              var wrColor = wr >= 60 ? '#10b981' : wr >= 50 ? '#f59e0b' : '#ef4444';
              wrHtml = '<span style="color:' + wrColor + ';font-weight:600">' + wr.toFixed(0) + '%</span>';
            }
            if (l2.avg_gain_30d != null) gainHtml = fmtGain(l2.avg_gain_30d);
            // 三级子行业（紧凑展示）
            var l3Str = (l2.children || []).map(function (l3) { return l3.level3 + '(' + l3.stock_count + ')'; }).join(' ');
            html += '<tr style="display:none">' +
              '<td style="padding-left:24px">' + esc(l2.level2) +
              (l3Str ? '<div style="font-size:10px;color:#94a3b8;margin-top:1px">' + esc(l3Str) + '</div>' : '') +
              '</td>' +
              '<td style="text-align:center">' + l2.stock_count + '只</td><td></td>' +
              '<td style="text-align:center">' + wrHtml + '</td>' +
              '<td style="text-align:center">' + gainHtml + '</td></tr>';
          });
        });
        html += '</tbody></table></div>';
      }

      // 分隔线
      html += '<hr style="border:none;border-top:1px solid #e2e8f0;margin:14px 0">';

      // 持仓明细表
      if (!holdings.length) { html += '<div class="muted">暂无持仓明细</div>'; panel.innerHTML = html; return; }
      html += '<table class="data-table"><thead><tr><th>股票</th><th>行业</th><th>报告期</th><th>事件</th><th>机构成本</th><th>跟随溢价</th><th>门槛</th><th>持仓市值</th><th>其他机构</th></tr></thead><tbody>';
      html += holdings.map(function (h) {
        var others = (h.other_institutions || []).map(function (o) { return instLink(o.id, o.name, o.type); }).join(' ') || '-';
        var indFull = (h.sw_level1 || '') + (h.sw_level2 ? ' > ' + h.sw_level2 : '') + (h.sw_level3 ? ' > ' + h.sw_level3 : '') || '-';
        var cost = h.inst_ref_cost != null ? Number(h.inst_ref_cost).toFixed(2) + '<div class="muted" style="font-size:10px">' + esc(costMethodText(h.inst_cost_method)) + '</div>' : '-';
        var premium = h.premium_pct != null ? premiumText(h.premium_pct) + '<div class="muted" style="font-size:10px">' + esc(h.premium_bucket || '') + '</div>' : '-';
        return '<tr><td>' + stockCell(h.stock_code, h.stock_name) + '</td><td style="font-size:11px;color:#64748b">' + esc(indFull) + '</td><td>' + fmtDate(h.report_date) + '</td><td>' + (h.event_type ? evTag(h.event_type) : '-') + '</td><td>' + cost + '</td><td>' + premium + '</td><td>' + followGateTag(h.follow_gate, h.follow_gate_reason) + '</td><td>' + compactNum(h.hold_market_cap) + '</td><td>' + others + '</td></tr>';
      }).join('');
      html += '</tbody></table>';
      panel.innerHTML = html;
    });
  }

  async function toggleInstBreakdown(instId) {
    var el = document.getElementById('breakdown-' + instId);
    if (!el) return;
    if (el.style.display !== 'none') {
      el.style.display = 'none';
      return;
    }
    var bd = await App._api('/api/inst/scoring/breakdown/institution/' + encodeURIComponent(instId));
    if (!bd || !bd.ok) return;
    var h = '<div style="padding:10px;background:#f8fafc;border-radius:6px;font-size:12px">';
    h += '<b>实力分公式</b>: ' + esc(bd.formula) + '<br>';
    h += '<b>实力分</b>: ' + (bd.quality_score != null ? Number(bd.quality_score).toFixed(1) : '-') + ' | ';
    h += '<b>可跟分</b>: ' + (bd.followability_score != null ? Number(bd.followability_score).toFixed(1) : '-') + '<br>';
    h += '<b>实力置信</b>: ' + esc(bd.score_confidence || '-') + ' | ';
    h += '<b>可跟置信</b>: ' + esc(bd.followability_confidence || '-') + '<br>';
    h += '<b>评分依据</b>: ' + (bd.score_basis === 'buy' ? '买入类事件' : '全事件回退') + '<br><br>';
    h += '<table style="width:100%;font-size:11px"><tr><th>因子</th><th>原始值</th><th>权重</th><th>来源</th></tr>';
    (bd.factors || []).forEach(function (f) {
      h += '<tr><td>' + esc(f.label || '-') + '</td><td>' + (f.raw_value != null ? esc(String(f.raw_value)) : '-') + '</td><td>' + esc(String(f.weight || 0)) + '</td><td style="color:#64748b">' + esc(f.source || '-') + '</td></tr>';
    });
    h += '</table></div>';
    if (bd.followability) {
      h += '<div style="margin-top:8px;padding:10px;background:#fff;border:1px solid #e2e8f0;border-radius:6px;font-size:12px">';
      h += '<b>可跟性画像</b>';
      h += '<div style="display:flex;flex-wrap:wrap;gap:10px;margin-top:8px">';
      h += '<span>平均溢价: ' + esc(premiumText(bd.followability.avg_premium_pct)) + '</span>';
      h += '<span>安全样本: ' + esc(String(bd.followability.safe_follow_event_count || 0)) + '</span>';
      h += '<span>安全胜率: ' + esc(pct(bd.followability.safe_follow_win_rate_30d)) + '</span>';
      h += '<span>安全30日均: ' + (bd.followability.safe_follow_avg_gain_30d != null ? fmtGain(bd.followability.safe_follow_avg_gain_30d) : '-') + '</span>';
      h += '<span>安全平均回撤: ' + (bd.followability.safe_follow_avg_drawdown_30d != null ? '-' + Number(bd.followability.safe_follow_avg_drawdown_30d).toFixed(1) + '%' : '-') + '</span>';
      h += '<span>传递效率: ' + (bd.followability.signal_transfer_efficiency_30d != null ? esc(Number(bd.followability.signal_transfer_efficiency_30d).toFixed(0) + '%') : '-') + '</span>';
      h += '<span>提示: ' + esc(bd.followability.followability_hint || '-') + '</span>';
      h += '</div></div>';
    }
    el.innerHTML = h;
    el.style.display = 'block';
  }

  function toggleStockDetail(stockCode, clickedEl) {
    var existing = document.querySelector('.detail-panel[data-detail-stock="' + stockCode + '"]');
    if (existing) { removeDetailPanel(existing.closest('tr.detail-row') || existing); clickedEl.removeAttribute('data-detail-open'); return; }
    // Close any other open stock detail
    document.querySelectorAll('.detail-panel[data-detail-stock]').forEach(function (p) { removeDetailPanel(p.closest('tr.detail-row') || p); });
    document.querySelectorAll('[data-detail-open="stock"]').forEach(function (e) { e.removeAttribute('data-detail-open'); });
    clickedEl.setAttribute('data-detail-open', 'stock');

    var tr = clickedEl.closest('tr');
    var panel = document.createElement('div');
    panel.className = 'detail-panel';
    panel.setAttribute('data-detail-stock', stockCode);
    panel.innerHTML = '<div class="detail-loading">加载中...</div>';

    if (tr) {
      var colCount = tr.children.length;
      var detailTr = document.createElement('tr');
      detailTr.className = 'detail-row';
      var td = document.createElement('td');
      td.setAttribute('colspan', colCount);
      td.appendChild(panel);
      detailTr.appendChild(td);
      tr.parentNode.insertBefore(detailTr, tr.nextSibling);
    } else {
      clickedEl.parentNode.insertBefore(panel, clickedEl.nextSibling);
    }

    api('/api/inst/stocks/detail/' + encodeURIComponent(stockCode)).then(function (r) {
      if (!r || !r.ok) { panel.innerHTML = '<div class="detail-loading">加载失败</div>'; return; }
      var insts = r.institutions || [];
      var indStr = r.industry ? (r.industry.sw_level1 || '') + (r.industry.sw_level2 ? ' > ' + r.industry.sw_level2 : '') + (r.industry.sw_level3 ? ' > ' + r.industry.sw_level3 : '') : '';
      var html = '';
      if (indStr) html += '<div style="margin-bottom:8px"><span class="muted" style="font-size:12px">行业：' + esc(indStr) + '</span></div>';
      html += renderSetupBlock(r.setup, insts, r);
      html += renderStockDetailCardGrid(r.setup, r.stage, r.forecast, insts, r);
      if (!insts.length) { html += '<div class="muted">暂无机构持仓</div>'; panel.innerHTML = html; return; }
      html += '<table class="data-table data-table-compact"><thead><tr><th>机构</th><th>类型</th><th>事件</th><th>机构成本</th><th>跟随溢价</th><th>执行</th><th>持仓市值</th><th>报告至今</th><th>公告至今</th></tr></thead><tbody>';
      html += insts.map(function (inst) {
        var cost = inst.inst_ref_cost != null ? Number(inst.inst_ref_cost).toFixed(2) + '<span class="muted" style="font-size:10px;margin-left:3px">' + esc(costMethodText(inst.inst_cost_method)) + '</span>' : '-';
        var premium = inst.premium_pct != null ? premiumText(inst.premium_pct) + '<span class="muted" style="font-size:10px;margin-left:3px">' + esc(premiumBucketText(inst.premium_bucket)) + '</span>' : '-';
        var noticeToNow = inst.notice_return_to_now != null ? fmtGain(inst.notice_return_to_now) : '<span class="muted">' + esc(inst.notice_return_status === '待最新收盘' ? '待K线' : (inst.notice_return_status || '-')) + '</span>';
        return '<tr><td><b class="clickable-name" onclick="App.toggleInstDetail(\'' + esc(inst.institution_id) + '\',this)">' + esc(inst.inst_name || '') + '</b></td><td>' + typeTag(inst.inst_type) + '</td><td>' + (inst.event_type ? evTag(inst.event_type) : '-') + '</td><td>' + cost + '</td><td>' + premium + '</td><td>' + followGateTag(inst.follow_gate, inst.follow_gate_reason) + '</td><td>' + compactNum(inst.hold_market_cap) + '</td><td>' + fmtGain(inst.report_return_to_now) + '</td><td>' + noticeToNow + '</td></tr>';
      }).join('');
      html += '</tbody></table>';
      panel.innerHTML = html;
    });
  }

  // ============================================================
  // 自定义模态框（替代 prompt/confirm/alert）
  // ============================================================
  var INST_TYPES = ['QFII', '社保', '保险', '国家队', '北向', '牛散', '基金', '券商'];

  function showModal(title, bodyHtml) {
    return new Promise(function (resolve) {
      var overlay = document.createElement('div');
      overlay.className = 'modal-overlay';
      overlay.innerHTML = '<div class="modal-box"><div class="modal-title">' + title + '</div><div class="modal-body">' + bodyHtml + '</div><div class="modal-actions"><button class="modal-cancel">取消</button><button class="primary modal-ok">确定</button></div></div>';
      document.body.appendChild(overlay);
      overlay.querySelector('.modal-cancel').onclick = function () { document.body.removeChild(overlay); resolve(null); };
      overlay.querySelector('.modal-ok').onclick = function () { resolve(overlay); };
      overlay.addEventListener('click', function (e) { if (e.target === overlay) { document.body.removeChild(overlay); resolve(null); } });
      var firstInput = overlay.querySelector('input,select');
      if (firstInput) firstInput.focus();
    });
  }
  function closeModal(overlay) { if (overlay && overlay.parentNode) overlay.parentNode.removeChild(overlay); }

  async function modalPrompt(title, defaultVal) {
    var ov = await showModal(title, '<input type="text" id="modalInput" value="' + (defaultVal || '') + '">');
    if (!ov) return null;
    var val = ov.querySelector('#modalInput').value;
    closeModal(ov);
    return val;
  }
  async function modalConfirm(msg) {
    var ov = await showModal('确认', '<p style="font-size:13px;color:#475569">' + msg + '</p>');
    if (!ov) return false;
    closeModal(ov);
    return true;
  }
  async function modalAlert(msg) {
    var ov = await showModal('提示', '<p style="font-size:13px;color:#475569">' + msg + '</p>');
    if (ov) closeModal(ov);
  }
  async function modalTypeSelect(title) {
    var opts = INST_TYPES.map(function (t) { return '<option value="' + t + '">' + t + '</option>'; }).join('');
    var ov = await showModal(title || '选择类型', '<select id="modalSelect">' + opts + '</select>');
    if (!ov) return null;
    var val = ov.querySelector('#modalSelect').value;
    closeModal(ov);
    return val;
  }
  async function modalFindReplace(title) {
    var body = '<label>查找（多个词用逗号或空格分隔，将逐个替换）</label><input type="text" id="modalFind" placeholder="例如：有限责任公司,股份有限公司,-自有资金">' +
      '<label style="margin-top:10px">替换为（留空则删除匹配的文字）</label><input type="text" id="modalReplace" placeholder="替换后的文字，留空=删除">';
    var ov = await showModal(title || '查找替换', body);
    if (!ov) return null;
    var findStr = ov.querySelector('#modalFind').value;
    var replace = ov.querySelector('#modalReplace').value;
    closeModal(ov);
    if (!findStr) return null;
    // 拆分多个查找词（逗号、顿号、空格）
    var finds = findStr.split(/[,，、\s]+/).filter(function (s) { return s.trim(); });
    if (!finds.length) return null;
    return { finds: finds, replace: replace };
  }

  function el(id) { return document.getElementById(id) }
  function esc(s) { var d = document.createElement('div'); d.textContent = s || ''; return d.innerHTML }
  function fmt(n) { return n != null ? Number(n).toLocaleString() : '-' }
  function pct(v) { return v != null ? Number(v).toFixed(1) + '%' : '-' }
  function premiumText(v) { if (v == null) return '-'; var n = Number(v); return (n > 0 ? '+' : '') + n.toFixed(1) + '%' }
  function premiumBucketText(v) {
    if (!v) return '-';
    return String(v)
      .replace('discount', '折价')
      .replace('near_cost', '近成本')
      .replace('premium', '溢价')
      .replace('high_premium', '高溢价');
  }
  function fmtDate(d) { if (!d) return '-'; var s = String(d).replace(/[^0-9]/g, '').slice(0, 8); return s.length === 8 ? s.slice(0, 4) + '-' + s.slice(4, 6) + '-' + s.slice(6, 8) : String(d) }
  function gainText(v) { if (v == null) return '-'; var n = Number(v); return (n >= 0 ? '+' : '') + n.toFixed(1) + '%' }
  function fmtGain(v) { if (v == null) return '-'; var n = Number(v); return '<span class="' + (n >= 0 ? 'gain-pos' : 'gain-neg') + '">' + (n >= 0 ? '+' : '') + n.toFixed(1) + '%</span>' }
  function compactNum(v) { if (v == null) return '-'; var n = Number(v); return n >= 1e8 ? (n / 1e8).toFixed(1) + '亿' : n >= 1e4 ? (n / 1e4).toFixed(0) + '万' : n.toFixed(0) }
  function metric(l, v) { return '<div class="metric"><div class="metric-value">' + v + '</div><div class="metric-label">' + l + '</div></div>' }
  function setupPriorityMeta(priority) {
    var p = Number(priority || 0);
    if (p === 1) return { label: 'A1', bg: '#dcfce7', fg: '#166534' };
    if (p === 2) return { label: 'A2', bg: '#dbeafe', fg: '#1d4ed8' };
    if (p === 3) return { label: 'A3', bg: '#fef3c7', fg: '#b45309' };
    if (p === 4) return { label: 'A4', bg: '#f1f5f9', fg: '#475569' };
    if (p === 5) return { label: 'A5', bg: '#f8fafc', fg: '#64748b' };
    return { label: '-', bg: '#f8fafc', fg: '#94a3b8' };
  }
  function setupBadge(tag, priority, confidence) {
    if (!tag) return '<span class="muted">-</span>';
    var meta = setupPriorityMeta(priority);
    var conf = confidence ? '<span style="font-size:10px;color:#94a3b8;margin-left:4px">' + esc(confidence) + '</span>' : '';
    return '<span style="display:inline-flex;align-items:center;padding:2px 8px;border-radius:999px;background:' + meta.bg + ';color:' + meta.fg + ';font-size:11px;font-weight:700">Setup ' + meta.label + '</span>' + conf;
  }
  function setupSummaryCell(s) {
    if (!s || !s.setup_tag) return '<span class="muted">-</span>';
    var badge = setupBadge(s.setup_tag, s.setup_priority, s.setup_confidence);
    var sub = [];
    var scoreLine = [];
    if (s.priority_pool) scoreLine.push(s.priority_pool);
    if (s.composite_priority_score != null) scoreLine.push('综合 ' + scoreNum(s.composite_priority_score));
    else if (s.discovery_score != null) scoreLine.push('发现 ' + scoreNum(s.discovery_score));
    if (scoreLine.length) sub.push(scoreLine.join(' · '));
    var name = stockSourceName(s);
    if (name && name !== '-') sub.push(name);
    if (s.score_highlights) sub.push(s.score_highlights);
    else if (s.setup_reason) sub.push(s.setup_reason);
    return badge + (sub.length ? '<div class="muted" style="font-size:10px;line-height:1.4;margin-top:4px">' + esc(sub.join(' · ')) + '</div>' : '');
  }
  function renderSetupBlock(setup, institutions, payload) {
    var scoreTarget = Object.assign({}, payload || {}, (payload && payload.stage) || {}, (payload && payload.forecast) || {}, setup || {});
    var scoreStrip = '<div style="display:flex;flex-wrap:wrap;gap:10px;margin:0 0 12px 0">' +
      focusMetric('综合优先分', scoreNum(scoreTarget.composite_priority_score), scoreTarget.priority_pool || '-') +
      focusMetric('发现分', scoreNum(scoreTarget.discovery_score), '机构发现') +
      focusMetric('质量分', scoreNum(scoreTarget.company_quality_score), scoreTarget.stock_archetype || '公司质量') +
      focusMetric('阶段分', scoreNum(scoreTarget.stage_score), scoreTarget.stage_reason || scoreTarget.path_state || '-') +
      focusMetric('预测分', scoreNum(scoreTarget.forecast_score), scoreTarget.forecast_reason || scoreTarget.score_highlights || '-') +
      '</div>';
    var focusConstraint = renderFocusConstraintLine(scoreTarget);
    if (!setup || !setup.setup_tag) {
      return '<div class="stock-focus-card stock-focus-card--empty">' +
        scoreStrip +
        '<div class="stock-focus-headline" style="color:#94a3b8;font-size:13px">暂无当前核心信号</div>' +
        '<div style="color:#cbd5e1;font-size:12px;margin-top:6px">未命中行业高手切入 Setup，可继续观察。</div>' +
        (focusConstraint ? '<div style="color:#cbd5e1;font-size:11px;margin-top:8px;line-height:1.6">' + esc(focusConstraint) + '</div>' : '') +
        '</div>';
    }
    var meta = setupPriorityMeta(setup.setup_priority);
    var confText = setup.setup_confidence ? ' · ' + esc(setup.setup_confidence) : '';
    var grades = [
      gradeInline('行业', setup.industry_skill_grade),
      gradeInline('可跟', setup.followability_grade),
      gradeInline('收益', setup.crowding_yield_grade),
      gradeInline('稳健', setup.crowding_stability_grade),
      gradeInline('溢价', setup.premium_grade),
      gradeInline('时效', setup.report_recency_grade),
      gradeInline('可靠', setup.reliability_grade)
    ].filter(Boolean).join('<span style="color:#e2e8f0;margin:0 6px">|</span>');
    return '<div class="stock-focus-card">' +
      scoreStrip +
      '<div style="display:flex;align-items:center;gap:12px;margin-bottom:12px">' +
      '<span class="setup-badge-lg" style="background:' + meta.bg + ';color:' + meta.fg + '">Setup ' + meta.label + '</span>' +
      '<span class="stock-focus-headline">' + esc(stockSignalHeadline(setup)) + '</span>' +
      '</div>' +
      '<div class="stock-focus-narrative" title="' + esc(setup.setup_reason || '') + '">' + esc(stockSignalNarrative(setup)) + confText + '</div>' +
      '<div class="stock-focus-grade-bar">' + grades + '</div>' +
      (focusConstraint ? '<div style="font-size:12px;color:#cbd5e1;line-height:1.6;margin-top:10px">' + esc(focusConstraint) + '</div>' : '') +
      '</div>';
  }
  function gradeInline(label, value) {
    if (value == null) return '';
    var v = Number(value);
    var color = v <= 2 ? '#059669' : v <= 3 ? '#d97706' : '#94a3b8';
    return '<span style="white-space:nowrap"><span style="color:#94a3b8">' + label + '</span> <span style="color:' + color + ';font-weight:600">' + v + '</span></span>';
  }
  function followGateTag(gate, reason) {
    // 用于「机构-股票对」级 follow_gate（loose：基于 event_type + premium_pct 两输入）
    if (!gate) return '<span class="muted">-</span>';
    var color = gate === 'follow' ? '#059669' : gate === 'watch' ? '#10b981' : gate === 'observe' ? '#f59e0b' : '#ef4444';
    var label = gate === 'follow' ? '可跟' : gate === 'watch' ? '关注' : gate === 'observe' ? '观察' : gate === 'avoid' ? '回避' : gate;
    return '<span style="display:inline-flex;align-items:center;gap:4px;padding:2px 8px;border-radius:999px;background:' + color + '14;color:' + color + ';font-size:11px;font-weight:600" title="' + esc(reason || '') + '">' + label + '</span>';
  }
  // 单一真相源：股票级 gate 解析器
  // 优先用后端预算的 stock_gate（从 MCR holder follow_gate 聚合）
  // 兜底用 holder_xxx_count 重算同一逻辑，保证显示列与筛选列共用一份判定
  function stockGateInfo(s) {
    if (!s) return { key: null, label: '-', color: '#94a3b8', reason: '' };
    var gate = s.stock_gate;
    var reason = s.stock_gate_reason || '';
    if (!gate) {
      var f = s.holder_follow_count || 0;
      var w = s.holder_watch_count || 0;
      var o = s.holder_observe_count || 0;
      var a = s.holder_avoid_count || 0;
      if (f > 0) { gate = 'follow'; reason = f + ' 家持仓机构可跟'; }
      else if (w > 0) { gate = 'watch'; reason = w + ' 家持仓机构关注'; }
      else if (o > 0) { gate = 'observe'; reason = o + ' 家持仓机构观察'; }
      else if (a > 0) { gate = 'avoid'; reason = a + ' 家持仓机构回避'; }
    }
    var meta = {
      follow: { label: '可跟', color: '#059669' },
      watch: { label: '关注', color: '#10b981' },
      observe: { label: '观察', color: '#f59e0b' },
      avoid: { label: '回避', color: '#ef4444' }
    }[gate];
    if (!meta) return { key: null, label: '-', color: '#94a3b8', reason: reason };
    return { key: gate, label: meta.label, color: meta.color, reason: reason };
  }
  // 单一真相源：股票级 gate 渲染器（display 列与详情共用同一份样式）
  function stockGateTag(s) {
    var info = stockGateInfo(s);
    if (!info.key) return '<span class="muted" title="' + esc(info.reason) + '">-</span>';
    return '<span style="display:inline-flex;align-items:center;gap:4px;padding:2px 8px;border-radius:999px;background:' + info.color + '14;color:' + info.color + ';font-size:11px;font-weight:600" title="' + esc(info.reason) + '">' + info.label + '</span>';
  }
  // 单一真相源：来源机构简称（后端解析后的 display_inst_name 优先；前端不再 fallback 到 leader_inst 全名）
  function stockSourceName(s) {
    if (!s) return '-';
    return s.display_inst_name || s.setup_inst_name || '-';
  }
  function costMethodText(v) {
    if (!v) return '-';
    return String(v)
      .replace('holding_chain_weighted_avg', '持仓链加权均价')
      .replace('holding_chain_carry_forward', '沿用上次持仓成本')
      .replace('holding_chain_new_entry', '本轮新进成本')
      .replace('holding_chain_event_fallback', '回退到本次事件成本')
      .replace('daily_vwap_qfq_volume_hand_adjusted', '日线VWAP(手数修正)')
      .replace('daily_vwap_qfq', '日线VWAP')
      .replace('daily_close_mean_qfq', '日线均价')
      .replace('monthly_vwap_qfq', '月线VWAP')
      .replace('monthly_close_mean_qfq', '月线均价')
      .replace(/_/g, ' ');
  }
  function xqLink(code) { if (!code) return ''; var p = code.startsWith('6') ? 'SH' : 'SZ'; return '<a class="xq-link" href="https://xueqiu.com/S/' + p + code + '" target="_blank">' + p + ':' + esc(code) + '</a>' }
  function stockCell(code, name) {
    return '<div class="stock-name-cell"><div><span class="clickable-name" onclick="App.toggleStockDetail(\'' + esc(code) + '\',this)">' + esc(name || code) + '</span> ' + xqLink(code) + '</div></div>';
  }
  function shortInstName(v) {
    if (!v) return '';
    return String(v).replace(/^inst_/, '').replace(/_/g, ' ').trim();
  }
  function parseDateDigits(v) {
    var s = String(v || '').replace(/[^0-9]/g, '').slice(0, 8);
    if (s.length !== 8) return null;
    var d = new Date(s.substring(0, 4) + '-' + s.substring(4, 6) + '-' + s.substring(6, 8));
    return isNaN(d) ? null : d;
  }
  function daysFromDateDigits(v) {
    var d = parseDateDigits(v);
    if (!d) return '-';
    return Math.floor((new Date() - d) / 86400000) + '天';
  }
  function daysAgoPill(v) {
    var d = parseDateDigits(v);
    if (!d) return '';
    var days = Math.floor((new Date() - d) / 86400000);
    var color = days <= 7 ? '#059669' : days <= 30 ? '#d97706' : '#94a3b8';
    var bg = days <= 7 ? '#ecfdf5' : days <= 30 ? '#fffbeb' : '#f8fafc';
    return '<span style="display:inline-flex;padding:1px 6px;border-radius:999px;font-size:10px;font-weight:600;background:' + bg + ';color:' + color + ';border:1px solid ' + color + '22;margin-left:4px;white-space:nowrap">' + days + '天</span>';
  }
  function daysBetweenDates(v1, v2) {
    var d1 = parseDateDigits(v1), d2 = parseDateDigits(v2);
    if (!d1 || !d2) return '-';
    return Math.floor((d2 - d1) / 86400000) + '天';
  }
  function setupLevelText(level) {
    if (level === 'level3') return '细分行业';
    if (level === 'level2') return '二级行业';
    if (level === 'level1') return '一级行业';
    return '行业';
  }
  function setupEventText(eventType) {
    return {
      new_entry: '新进',
      increase: '增持',
      unchanged: '持仓不变',
      decrease: '减持'
    }[eventType] || '动作';
  }
  function setupTimelinessText(grade) {
    return {
      1: '时效佳',
      2: '披露较快',
      3: '披露适中',
      4: '披露偏慢',
      5: '披露过慢'
    }[Number(grade)] || '披露适中';
  }
  function stockSignalHeadline(s) {
    if (!s || !s.setup_tag) return '暂无当前核心信号';
    return setupLevelText(s.setup_level) + '高手' + setupEventText(s.setup_event_type);
  }
  function stockSignalNarrative(s) {
    if (!s || !s.setup_tag) return s && (s.path_state || s.price_trend) ? String(s.path_state || s.price_trend) : '等待新的机构动作';
    var parts = [];
    var name = stockSourceName(s);
    if (name && name !== '-') parts.push(name);
    if (s.setup_industry_name) parts.push(s.setup_industry_name);
    if (s.report_recency_grade != null && Number(s.report_recency_grade) <= 2) parts.push(setupTimelinessText(s.report_recency_grade));
    if (s.premium_grade === 1) parts.push('低溢价');
    return parts.join(' · ') || (s.setup_reason || '-');
  }
  function scoreNum(v) {
    return v != null ? Number(v).toFixed(1) : '-';
  }
  function signedScore(v) {
    if (v == null) return '-';
    var n = Number(v);
    return (n >= 0 ? '+' : '') + n.toFixed(1);
  }
  function signedPct(v) {
    if (v == null) return '-';
    var n = Number(v);
    return (n >= 0 ? '+' : '') + n.toFixed(1) + '%';
  }
  function stockStageStructText(s) {
    var parts = [];
    if (s && s.path_state) parts.push(String(s.path_state));
    if (s && s.generic_stage_raw != null) parts.push('通用 ' + scoreNum(s.generic_stage_raw));
    if (s && s.stage_type_adjust_raw != null) parts.push('型修 ' + signedScore(s.stage_type_adjust_raw));
    return parts.join(' · ');
  }
  function stockForecastStructText(s) {
    var parts = [];
    if (s && s.forecast_20d_score != null) parts.push('预20 ' + scoreNum(s.forecast_20d_score));
    if (s && s.forecast_60d_excess_score != null) parts.push('预60 ' + scoreNum(s.forecast_60d_excess_score));
    if (s && s.forecast_risk_adjusted_score != null) parts.push('性价比 ' + scoreNum(s.forecast_risk_adjusted_score));
    return parts.join(' · ');
  }
  function renderFocusConstraintLine(s) {
    if (!s) return '';
    var parts = [];
    if (s.setup_execution_reason) parts.push(s.setup_execution_reason);
    else if (s.priority_pool_reason) parts.push(s.priority_pool_reason);
    if (s.composite_cap_reason) parts.push(s.composite_cap_reason);
    if (s.raw_composite_priority_score != null && s.composite_priority_score != null && Number(s.raw_composite_priority_score) !== Number(s.composite_priority_score)) {
      parts.push('综合裁决 ' + scoreNum(s.raw_composite_priority_score) + ' -> ' + scoreNum(s.composite_priority_score));
    }
    return parts.join(' · ');
  }
  function detailMetricItem(label, value, tone) {
    var cls = tone ? ' stock-detail-item--' + tone : '';
    return '<div class="stock-detail-item' + cls + '"><div class="stock-detail-item-label">' + esc(label) + '</div><div class="stock-detail-item-value">' + esc(value == null ? '-' : String(value)) + '</div></div>';
  }
  function detailTextBlock(label, text, tone) {
    if (!text) return '';
    var cls = tone ? ' stock-detail-text--' + tone : '';
    return '<div class="stock-detail-text' + cls + '"><div class="stock-detail-text-label">' + esc(label) + '</div><div class="stock-detail-text-body">' + esc(text) + '</div></div>';
  }
  function renderStockBehaviorCard(setup, insts, base) {
    var s = Object.assign({}, base || {}, setup || {});
    var rows = Array.isArray(insts) ? insts : [];
    var totalInst = rows.length;
    var totalCap = rows.reduce(function (sum, item) {
      return sum + (Number(item.hold_market_cap || 0) || 0);
    }, 0);
    var avgPremium = rows.reduce(function (sum, item) {
      return item.premium_pct == null ? sum : sum + Number(item.premium_pct || 0);
    }, 0);
    var premiumCount = rows.filter(function (item) { return item.premium_pct != null; }).length;
    var leaderName = s.leader_inst ? shortInstName(s.leader_inst) : (rows[0] && rows[0].inst_name ? rows[0].inst_name : '-');
    var summary = [
      s.setup_tag ? ('当前由 ' + stockSourceName(s) + ' 的 ' + setupEventText(s.setup_event_type) + ' 信号触发') : '',
      s.setup_industry_name || '',
      s.priority_pool_reason || ''
    ].filter(Boolean).join(' · ');
    var behaviorBreakdown = renderStockBehaviorBreakdown(rows);
    var behaviorSignals = renderStockBehaviorSignals(s, rows);
    var behaviorSummary = renderStockBehaviorSummary(s, rows, leaderName, premiumCount ? (avgPremium / premiumCount) : null);
    return '<div class="stock-detail-card">' +
      '<div class="stock-detail-card-head"><div class="stock-detail-card-title">机构行为卡</div><div class="stock-detail-card-sub">谁在买、怎么买、最近是否有新动作</div></div>' +
      '<div class="stock-detail-metrics">' +
      detailMetricItem('来源机构', stockSourceName(s) || '-') +
      detailMetricItem('领头机构', leaderName || '-') +
      detailMetricItem('触发事件', setupEventText(s.setup_event_type) || '-') +
      detailMetricItem('发现分', scoreNum(s.discovery_score), 'accent') +
      detailMetricItem('机构数', fmt(totalInst)) +
      detailMetricItem('共识度', fmt(s.consensus_count || 0)) +
      detailMetricItem('平均溢价', premiumCount ? premiumText(avgPremium / premiumCount) : '-') +
      detailMetricItem('合计市值', compactNum(totalCap)) +
      detailMetricItem('最新报告', fmtDate(s.latest_report_date)) +
      detailMetricItem('最新公告', fmtDate(s.latest_notice_date)) +
      '</div>' +
      detailTextBlock('机构行为结论', summary || (s.setup_reason || '等待新的机构动作')) +
      behaviorBreakdown +
      behaviorSignals +
      behaviorSummary +
      (s.setup_reason ? detailTextBlock('触发说明', s.setup_reason) : '') +
      (s.setup_execution_reason ? detailTextBlock('执行建议', s.setup_execution_reason, 'neutral') : '') +
      '</div>';
  }

  function renderStockBehaviorBreakdown(insts) {
    var rows = Array.isArray(insts) ? insts : [];
    if (!rows.length) return '';
    var gateCounts = { follow: 0, watch: 0, observe: 0, avoid: 0 };
    var eventCounts = { new_entry: 0, increase: 0, decrease: 0, unchanged: 0 };
    rows.forEach(function (inst) {
      if (inst.follow_gate && gateCounts[inst.follow_gate] != null) gateCounts[inst.follow_gate] += 1;
      if (inst.event_type && eventCounts[inst.event_type] != null) eventCounts[inst.event_type] += 1;
    });
    var items = [
      detailMetricItem('可跟', fmt(gateCounts.follow), gateCounts.follow > 0 ? 'good' : ''),
      detailMetricItem('关注', fmt(gateCounts.watch)),
      detailMetricItem('观察', fmt(gateCounts.observe), gateCounts.observe > 0 ? 'warn' : ''),
      detailMetricItem('回避', fmt(gateCounts.avoid), gateCounts.avoid > 0 ? 'warn' : ''),
      detailMetricItem('新进', fmt(eventCounts.new_entry), eventCounts.new_entry > 0 ? 'good' : ''),
      detailMetricItem('增持', fmt(eventCounts.increase), eventCounts.increase > 0 ? 'good' : ''),
      detailMetricItem('减持', fmt(eventCounts.decrease), eventCounts.decrease > 0 ? 'warn' : ''),
      detailMetricItem('不变', fmt(eventCounts.unchanged))
    ];
    return '<div class="stock-detail-text stock-detail-text--neutral">' +
      '<div class="stock-detail-text-label">行为拆解</div>' +
      '<div class="stock-detail-metrics" style="margin-top:8px">' + items.join('') + '</div>' +
      '</div>';
  }

  function renderStockBehaviorSignals(s, insts) {
    var rows = Array.isArray(insts) ? insts : [];
    var premiumValues = rows.filter(function (inst) { return inst.premium_pct != null; }).map(function (inst) { return Number(inst.premium_pct || 0); });
    var avgPremium = premiumValues.length ? premiumValues.reduce(function (sum, value) { return sum + value; }, 0) / premiumValues.length : null;
    var nearCostCount = premiumValues.filter(function (value) { return value > 0 && value <= 5; }).length;
    var discountCount = premiumValues.filter(function (value) { return value <= 0; }).length;
    var highPremiumCount = premiumValues.filter(function (value) { return value > 20; }).length;
    var avgHeldDays = rows.filter(function (inst) { return inst.current_held_days != null; }).map(function (inst) { return Number(inst.current_held_days || 0); });
    var avgDisclosureLag = rows.filter(function (inst) { return inst.disclosure_lag_days != null; }).map(function (inst) { return Number(inst.disclosure_lag_days || 0); });
    var topInst = rows[0] || {};
    var items = [
      s.setup_follow_gate ? detailMetricItem('源头执行', stockGateInfo({ stock_gate: s.setup_follow_gate, stock_gate_reason: s.setup_follow_gate_reason }).label) : '',
      s.setup_premium_pct != null ? detailMetricItem('源头溢价', premiumText(s.setup_premium_pct)) : '',
      avgPremium != null ? detailMetricItem('平均溢价', premiumText(avgPremium)) : '',
      nearCostCount ? detailMetricItem('近成本机构', fmt(nearCostCount), 'good') : '',
      discountCount ? detailMetricItem('折价机构', fmt(discountCount), 'good') : '',
      highPremiumCount ? detailMetricItem('高溢价机构', fmt(highPremiumCount), 'warn') : '',
      topInst.hold_ratio != null ? detailMetricItem('首位持股比', pct(topInst.hold_ratio)) : '',
      topInst.holder_rank != null ? detailMetricItem('首位席次', '#' + fmt(topInst.holder_rank)) : '',
      avgHeldDays.length ? detailMetricItem('平均持有天数', fmt(Math.round(avgHeldDays.reduce(function (sum, value) { return sum + value; }, 0) / avgHeldDays.length)) + '天') : '',
      avgDisclosureLag.length ? detailMetricItem('平均披露时差', fmt(Math.round(avgDisclosureLag.reduce(function (sum, value) { return sum + value; }, 0) / avgDisclosureLag.length)) + '天') : ''
    ].filter(Boolean);
    if (!items.length) return '';
    return '<div class="stock-detail-text stock-detail-text--info">' +
      '<div class="stock-detail-text-label">机构行为信号</div>' +
      '<div class="stock-detail-metrics" style="margin-top:8px">' + items.join('') + '</div>' +
      '</div>';
  }

  function renderStockBehaviorSummary(s, insts, leaderName, avgPremium) {
    var rows = Array.isArray(insts) ? insts : [];
    if (!rows.length && !s.setup_reason) return '';
    var parts = [];
    var topNames = rows.slice(0, 3).map(function (inst) { return inst.inst_name; }).filter(Boolean);
    var followCount = rows.filter(function (inst) { return inst.follow_gate === 'follow'; }).length;
    var watchCount = rows.filter(function (inst) { return inst.follow_gate === 'watch'; }).length;
    if (leaderName && leaderName !== '-') parts.push('当前由 ' + leaderName + ' 领头');
    if (topNames.length) parts.push('前排机构为 ' + topNames.join(' / '));
    if (followCount || watchCount) parts.push('可执行机构 ' + fmt(followCount) + ' 家，关注机构 ' + fmt(watchCount) + ' 家');
    if (avgPremium != null) {
      if (avgPremium <= 5) parts.push('整体仍接近机构成本区');
      else if (avgPremium >= 20) parts.push('整体已进入高溢价区');
      else parts.push('整体位于正常溢价区');
    }
    return parts.length ? detailTextBlock('行为结构', parts.join('；'), 'neutral') : '';
  }

  function renderStockQualityCard(setup, base) {
    var s = Object.assign({}, base || {}, setup || {});
    var chips = [
      gradeChip('行业', s.industry_skill_grade),
      gradeChip('可跟', s.followability_grade),
      gradeChip('溢价', s.premium_grade),
      gradeChip('时效', s.report_recency_grade),
      gradeChip('可靠', s.reliability_grade)
    ].filter(Boolean).join('');
    var qualityBreakdown = renderStockQualityBreakdown(s);
    var qualityMetrics = renderStockQualityMetrics(s);
    var qualitySummary = renderStockQualitySummary(s);
    return '<div class="stock-detail-card">' +
      '<div class="stock-detail-card-head"><div class="stock-detail-card-title">质量卡</div><div class="stock-detail-card-sub">公司质量、股票类型和当前主要优劣势</div></div>' +
      '<div class="stock-detail-metrics">' +
      detailMetricItem('质量分', scoreNum(s.company_quality_score), 'good') +
      detailMetricItem('股票类型', s.stock_archetype || '待分类') +
      detailMetricItem('行业技能', s.industry_skill_grade != null ? String(s.industry_skill_grade) + '级' : '-') +
      detailMetricItem('可跟等级', s.followability_grade != null ? String(s.followability_grade) + '级' : '-') +
      detailMetricItem('溢价等级', s.premium_grade != null ? String(s.premium_grade) + '级' : '-') +
      detailMetricItem('可靠等级', s.reliability_grade != null ? String(s.reliability_grade) + '级' : '-') +
      '</div>' +
      (chips ? '<div class="stock-detail-chip-row">' + chips + '</div>' : '') +
      qualityBreakdown +
      qualityMetrics +
      qualitySummary +
      (s.score_highlights ? detailTextBlock('加分主因', s.score_highlights, 'positive') : '') +
      (s.score_risks ? detailTextBlock('扣分主因', s.score_risks, 'warning') : '') +
      '</div>';
  }

  function renderStockQualityBreakdown(s) {
    var items = [
      { label: '利润', value: s.quality_profit_raw },
      { label: '现金', value: s.quality_cash_raw },
      { label: '负债', value: s.quality_balance_raw },
      { label: '利润率', value: s.quality_margin_raw },
      { label: '合同/库存', value: s.quality_contract_raw },
      { label: '新鲜度', value: s.quality_freshness_raw },
      { label: '资本纪律', value: s.quality_capital_raw },
      { label: '效率', value: s.quality_efficiency_raw },
      { label: '增长', value: s.quality_growth_raw }
    ].filter(function (item) {
      return item.value != null;
    });
    if (!items.length) return '';
    return '<div class="stock-detail-text stock-detail-text--neutral">' +
      '<div class="stock-detail-text-label">质量子分拆解</div>' +
      '<div class="stock-detail-metrics" style="margin-top:8px">' +
      items.map(function (item) {
        return detailMetricItem(item.label, scoreNum(item.value));
      }).join('') +
      '</div>' +
      '</div>';
  }

  function renderStockQualityMetrics(s) {
    var items = [
      s.roe != null ? detailMetricItem('ROE', pct(s.roe)) : '',
      s.roa_ak != null ? detailMetricItem('ROA', pct(s.roa_ak)) : '',
      s.gross_margin != null ? detailMetricItem('毛利率', pct(s.gross_margin)) : '',
      s.ocf_to_profit != null ? detailMetricItem('现利比', scoreNum(s.ocf_to_profit)) : '',
      s.debt_ratio != null ? detailMetricItem('负债率', pct(s.debt_ratio)) : '',
      s.current_ratio != null ? detailMetricItem('流动比率', scoreNum(s.current_ratio)) : '',
      s.total_shares_growth_3y != null ? detailMetricItem('3年股本变动', signedPct(s.total_shares_growth_3y)) : '',
      s.holder_count_change_pct != null ? detailMetricItem('股东人数变动', signedPct(s.holder_count_change_pct)) : '',
      s.net_profit_positive_8q != null ? detailMetricItem('8期净利为正', scoreNum(s.net_profit_positive_8q) + '/8') : '',
      s.operating_cashflow_positive_8q != null ? detailMetricItem('8期现金流为正', scoreNum(s.operating_cashflow_positive_8q) + '/8') : '',
      s.revenue_yoy_positive_4q != null ? detailMetricItem('4期营收同比为正', scoreNum(s.revenue_yoy_positive_4q) + '/4') : '',
      s.profit_yoy_positive_4q != null ? detailMetricItem('4期利润同比为正', scoreNum(s.profit_yoy_positive_4q) + '/4') : '',
      s.revenue_growth_yoy_ak != null ? detailMetricItem('营收同比', signedPct(s.revenue_growth_yoy_ak)) : '',
      s.net_profit_growth_yoy_ak != null ? detailMetricItem('利润同比', signedPct(s.net_profit_growth_yoy_ak)) : ''
    ].filter(Boolean);
    if (!items.length) return '';
    var meta = [
      s.quality_latest_financial_report_date ? ('财务 ' + fmtDate(s.quality_latest_financial_report_date)) : '',
      s.quality_latest_indicator_report_date ? ('指标 ' + fmtDate(s.quality_latest_indicator_report_date)) : ''
    ].filter(Boolean).join(' · ');
    return '<div class="stock-detail-text stock-detail-text--info">' +
      '<div class="stock-detail-text-label">关键财务信号' + (meta ? ' · ' + meta : '') + '</div>' +
      '<div class="stock-detail-metrics" style="margin-top:8px">' +
      items.join('') +
      '</div>' +
      '</div>';
  }

  function renderStockQualitySummary(s) {
    var components = [
      { label: '利润', value: s.quality_profit_raw },
      { label: '现金', value: s.quality_cash_raw },
      { label: '负债', value: s.quality_balance_raw },
      { label: '利润率', value: s.quality_margin_raw },
      { label: '合同/库存', value: s.quality_contract_raw },
      { label: '新鲜度', value: s.quality_freshness_raw },
      { label: '资本纪律', value: s.quality_capital_raw },
      { label: '效率', value: s.quality_efficiency_raw },
      { label: '增长', value: s.quality_growth_raw }
    ].filter(function (item) {
      return item.value != null;
    });
    if (!components.length) return '';
    components.sort(function (a, b) {
      return Number(b.value || 0) - Number(a.value || 0);
    });
    var strongest = components.slice(0, 3).map(function (item) { return item.label; }).join(' / ');
    var weakest = components.slice(-2).reverse().map(function (item) { return item.label; }).join(' / ');
    var extra = [];
    if (s.future_unlock_ratio_180d != null) extra.push('180日解禁 ' + pct(s.future_unlock_ratio_180d));
    if (s.dividend_financing_ratio != null) extra.push('分红/融资 ' + scoreNum(s.dividend_financing_ratio));
    if (s.total_shares_growth_3y != null) extra.push('3年股本 ' + signedPct(s.total_shares_growth_3y));
    if (s.holder_count_change_pct != null) extra.push('股东人数 ' + signedPct(s.holder_count_change_pct));
    if (s.net_profit_positive_8q != null) extra.push('8期净利正 ' + scoreNum(s.net_profit_positive_8q) + '/8');
    if (s.operating_cashflow_positive_8q != null) extra.push('8期现金正 ' + scoreNum(s.operating_cashflow_positive_8q) + '/8');
    if (s.contract_to_revenue != null) extra.push('合同/收入 ' + scoreNum(s.contract_to_revenue));
    var text = '当前质量分主要由 ' + strongest + ' 支撑';
    if (weakest) text += '；相对偏弱的是 ' + weakest;
    if (extra.length) text += '。补充信号：' + extra.join(' · ');
    return detailTextBlock('质量结构', text, 'neutral');
  }
  function renderStockStageCard(stage, base) {
    var s = Object.assign({}, base || {}, stage || {});
    var stageBreakdown = renderStockStageBreakdown(s);
    var stageSignals = renderStockStageSignals(s);
    var stageSummary = renderStockStageSummary(s);
    return '<div class="stock-detail-card">' +
      '<div class="stock-detail-card-head"><div class="stock-detail-card-title">阶段卡</div><div class="stock-detail-card-sub">当前阶段是否仍适合买，以及是否触发限制</div></div>' +
      '<div class="stock-detail-metrics">' +
      detailMetricItem('阶段分', scoreNum(s.stage_score), 'warn') +
      detailMetricItem('路径状态', s.path_state || '-') +
      detailMetricItem('通用阶段', scoreNum(s.generic_stage_raw)) +
      detailMetricItem('类型修正', signedScore(s.stage_type_adjust_raw)) +
      detailMetricItem('60日回撤', pct(s.max_drawdown_60d)) +
      detailMetricItem('距250日线', signedPct(s.dist_ma250_pct)) +
      detailMetricItem('站上250日线', s.above_ma250 ? '是' : (s.above_ma250 === 0 ? '否' : '-')) +
      '</div>' +
      stageBreakdown +
      stageSignals +
      stageSummary +
      (s.stage_reason ? detailTextBlock('阶段判断', s.stage_reason) : '') +
      (s.priority_pool_reason ? detailTextBlock('分池约束', s.priority_pool_reason, 'neutral') : '') +
      (s.composite_cap_reason ? detailTextBlock('封顶约束', s.composite_cap_reason, 'warning') : '') +
      '</div>';
  }

  function renderStockStageBreakdown(s) {
    var items = [];
    if (s.stock_archetype === '高质量稳健型') {
      items = [
        { label: '续航', value: s.stage_quality_continuity_raw },
        { label: '趋势健康', value: s.stage_quality_trend_raw },
        { label: '过热惩罚', value: s.stage_quality_overheat_penalty }
      ];
    } else if (s.stock_archetype === '成长兑现型') {
      items = [
        { label: '增长延续', value: s.stage_growth_continuity_raw },
        { label: '放缓惩罚', value: s.stage_growth_slowdown_penalty },
        { label: '透支惩罚', value: s.stage_growth_stretch_penalty }
      ];
    } else {
      items = [
        { label: '修复验证', value: s.stage_cycle_recovery_raw },
        { label: '兑现惩罚', value: s.stage_cycle_realization_penalty },
        { label: '不确定惩罚', value: s.stage_cycle_uncertainty_penalty }
      ];
    }
    items = items.filter(function (item) { return item.value != null; });
    if (!items.length) return '';
    return '<div class="stock-detail-text stock-detail-text--neutral">' +
      '<div class="stock-detail-text-label">阶段子分拆解</div>' +
      '<div class="stock-detail-metrics" style="margin-top:8px">' +
      items.map(function (item) {
        var value = Number(item.value || 0);
        return detailMetricItem(item.label, (value >= 0 ? '+' : '') + value.toFixed(1), value >= 0 ? 'good' : 'warn');
      }).join('') +
      '</div>' +
      '</div>';
  }

  function renderStockStageSignals(s) {
    var items = [
      s.return_1m != null ? detailMetricItem('1月收益', signedPct(s.return_1m)) : '',
      s.return_3m != null ? detailMetricItem('3月收益', signedPct(s.return_3m)) : '',
      s.return_6m != null ? detailMetricItem('6月收益', signedPct(s.return_6m)) : '',
      s.return_12m != null ? detailMetricItem('12月收益', signedPct(s.return_12m)) : '',
      s.path_max_gain_pct != null ? detailMetricItem('路径最高涨幅', signedPct(s.path_max_gain_pct)) : '',
      s.path_max_drawdown_pct != null ? detailMetricItem('路径最大回撤', pct(s.path_max_drawdown_pct)) : '',
      s.amount_ratio_20_120 != null ? detailMetricItem('量能放大', scoreNum(s.amount_ratio_20_120)) : '',
      s.volatility_20d != null ? detailMetricItem('20日波动', pct(s.volatility_20d)) : '',
      s.amplitude_20d != null ? detailMetricItem('20日振幅', pct(s.amplitude_20d)) : '',
      s.stock_gate ? detailMetricItem('执行门槛', s.stock_gate) : ''
    ].filter(Boolean);
    if (!items.length) return '';
    var gateMeta = [];
    if (s.gate_follow_count != null) gateMeta.push('follow ' + fmt(s.gate_follow_count || 0));
    if (s.gate_watch_count != null) gateMeta.push('watch ' + fmt(s.gate_watch_count || 0));
    if (s.gate_observe_count != null) gateMeta.push('observe ' + fmt(s.gate_observe_count || 0));
    if (s.gate_avoid_count != null) gateMeta.push('avoid ' + fmt(s.gate_avoid_count || 0));
    return '<div class="stock-detail-text stock-detail-text--info">' +
      '<div class="stock-detail-text-label">路径与量价信号' + (gateMeta.length ? ' · ' + gateMeta.join(' / ') : '') + '</div>' +
      '<div class="stock-detail-metrics" style="margin-top:8px">' +
      items.join('') +
      '</div>' +
      '</div>';
  }

  function renderStockStageSummary(s) {
    var parts = [];
    if (s.stock_archetype) parts.push(s.stock_archetype);
    if (s.path_state) parts.push('当前处于' + s.path_state);
    if (s.return_12m != null) {
      if (Number(s.return_12m) > 80) parts.push('过去12个月涨幅偏高');
      else if (Number(s.return_12m) < 0) parts.push('过去12个月仍未明显修复');
      else parts.push('过去12个月仍在可跟踪区间');
    }
    if (s.amount_ratio_20_120 != null && Number(s.amount_ratio_20_120) >= 2) parts.push('近期量能明显放大');
    if (s.volatility_20d != null && Number(s.volatility_20d) >= 5) parts.push('短期波动偏大');
    if (!parts.length) return '';
    return detailTextBlock('阶段结构', parts.join('；'), 'neutral');
  }
  function renderStockForecastCard(forecast, base) {
    var s = Object.assign({}, base || {}, forecast || {});
    var meta = [
      s.forecast_snapshot_date ? ('快照 ' + fmtDate(s.forecast_snapshot_date)) : '',
      s.forecast_predict_date ? ('预测日 ' + fmtDate(s.forecast_predict_date)) : '',
      s.forecast_industry_relative_group || '',
      s.forecast_model_id ? ('模型 ' + s.forecast_model_id) : ''
    ].filter(Boolean).join(' · ');
    var forecastBreakdown = renderStockForecastBreakdown(s);
    var forecastSignals = renderStockForecastSignals(s);
    var forecastSummary = renderStockForecastSummary(s);
    return '<div class="stock-detail-card">' +
      '<div class="stock-detail-card-head"><div class="stock-detail-card-title">预测卡</div><div class="stock-detail-card-sub">Qlib 排序增强结果，以及当前生效后的预测贡献</div></div>' +
      '<div class="stock-detail-metrics">' +
      detailMetricItem('预测分', scoreNum(s.forecast_score), 'accent') +
      detailMetricItem('生效预测', scoreNum(s.forecast_score_effective)) +
      detailMetricItem('Qlib分', scoreNum(s.qlib_score)) +
      detailMetricItem('AI排行', s.qlib_rank != null ? ('#' + fmt(s.qlib_rank)) : '-') +
      detailMetricItem('市场分位', pct(s.qlib_percentile)) +
      detailMetricItem('行业分位', pct(s.industry_qlib_percentile)) +
      detailMetricItem('波动排位', pct(s.volatility_rank)) +
      detailMetricItem('回撤排位', pct(s.drawdown_rank)) +
      '</div>' +
      forecastBreakdown +
      forecastSignals +
      forecastSummary +
      (s.forecast_reason ? detailTextBlock('预测判断', s.forecast_reason, 'info') : '') +
      (meta ? detailTextBlock('模型信息', meta, 'neutral') : '') +
      '</div>';
  }

  function renderStockForecastBreakdown(s) {
    var items = [
      { label: '预20', value: s.forecast_20d_score },
      { label: '预60', value: s.forecast_60d_excess_score },
      { label: '性价比', value: s.forecast_risk_adjusted_score }
    ].filter(function (item) { return item.value != null; });
    if (!items.length) return '';
    return '<div class="stock-detail-text stock-detail-text--neutral">' +
      '<div class="stock-detail-text-label">预测子分拆解</div>' +
      '<div class="stock-detail-metrics" style="margin-top:8px">' +
      items.map(function (item) { return detailMetricItem(item.label, scoreNum(item.value)); }).join('') +
      '</div>' +
      '</div>';
  }

  function renderStockForecastSignals(s) {
    var items = [
      s.forecast_20d_score != null ? detailMetricItem('20日概率', scoreNum(s.forecast_20d_score)) : '',
      s.forecast_60d_excess_score != null ? detailMetricItem('60日超额', scoreNum(s.forecast_60d_excess_score)) : '',
      s.forecast_risk_adjusted_score != null ? detailMetricItem('风险收益', scoreNum(s.forecast_risk_adjusted_score)) : '',
      s.qlib_percentile != null ? detailMetricItem('全市场分位', pct(s.qlib_percentile)) : '',
      s.industry_qlib_percentile != null ? detailMetricItem('行业内分位', pct(s.industry_qlib_percentile)) : '',
      s.volatility_rank != null ? detailMetricItem('低波优势', pct(s.volatility_rank)) : '',
      s.drawdown_rank != null ? detailMetricItem('回撤优势', pct(s.drawdown_rank)) : ''
    ].filter(Boolean);
    if (!items.length) return '';
    return '<div class="stock-detail-text stock-detail-text--info">' +
      '<div class="stock-detail-text-label">预测结构信号</div>' +
      '<div class="stock-detail-metrics" style="margin-top:8px">' +
      items.join('') +
      '</div>' +
      '</div>';
  }

  function renderStockForecastSummary(s) {
    var parts = [];
    if (s.forecast_score != null && s.forecast_score_effective != null && Number(s.forecast_score_effective) < Number(s.forecast_score)) {
      parts.push('阶段过滤压缩了预测贡献');
    } else if (s.forecast_score_effective != null) {
      parts.push('阶段过滤未明显压缩预测贡献');
    }
    if (s.qlib_percentile != null) {
      if (Number(s.qlib_percentile) >= 80) parts.push('全市场分位进入前 20%');
      else if (Number(s.qlib_percentile) <= 30) parts.push('全市场分位仍偏后');
    }
    if (s.industry_qlib_percentile != null) {
      if (Number(s.industry_qlib_percentile) >= 80) parts.push('行业内排序靠前');
      else if (Number(s.industry_qlib_percentile) <= 30) parts.push('行业内排序仍偏后');
    }
    if (s.forecast_industry_relative_group) parts.push('行业相对组别 ' + s.forecast_industry_relative_group);
    return parts.length ? detailTextBlock('预测结构', parts.join('；'), 'neutral') : '';
  }
  function renderStockDetailCardGrid(setup, stage, forecast, insts, payload) {
    var base = Object.assign({}, payload || {}, setup || {}, stage || {}, forecast || {});
    return '<div class="stock-detail-grid">' +
      renderStockBehaviorCard(setup, insts, base) +
      renderStockQualityCard(setup, base) +
      renderStockStageCard(stage, base) +
      renderStockForecastCard(forecast, base) +
      '</div>';
  }
  function priorityPoolTag(pool) {
    var meta = {
      'A池': { bg: '#dcfce7', fg: '#166534' },
      'B池': { bg: '#dbeafe', fg: '#1d4ed8' },
      'C池': { bg: '#fef3c7', fg: '#b45309' },
      'D池': { bg: '#fee2e2', fg: '#b91c1c' }
    }[pool || ''] || { bg: '#f1f5f9', fg: '#64748b' };
    return '<span style="display:inline-flex;align-items:center;padding:2px 8px;border-radius:999px;background:' + meta.bg + ';color:' + meta.fg + ';font-size:11px;font-weight:700">' + esc(pool || '未分池') + '</span>';
  }
  function stockCompositeSummary(s) {
    return [
      '发' + scoreNum(s.discovery_score),
      '质' + scoreNum(s.company_quality_score),
      '阶' + scoreNum(s.stage_score),
      '测' + scoreNum(s.forecast_score)
    ].join(' / ');
  }
  function stockCompositeCell(s) {
    var composite = s.composite_priority_score != null ? Number(s.composite_priority_score).toFixed(1) : '-';
    var poolHtml = priorityPoolTag(s.priority_pool);
    var archetype = s.stock_archetype || '待分类';
    var summary = stockCompositeSummary(s);
    var extra = [];
    if (s.qlib_rank != null) extra.push('AI#' + s.qlib_rank);
    if (s.forecast_score_effective != null) extra.push('有效测 ' + Number(s.forecast_score_effective).toFixed(1));
    var stageStruct = stockStageStructText(s);
    var forecastStruct = stockForecastStructText(s);
    var structureLine = [stageStruct, forecastStruct].filter(Boolean).join(' · ');
    var constraintLine = [];
    if (s.raw_composite_priority_score != null && s.composite_priority_score != null && Number(s.raw_composite_priority_score) !== Number(s.composite_priority_score)) {
      constraintLine.push('原始 ' + scoreNum(s.raw_composite_priority_score) + ' -> 封顶 ' + scoreNum(s.composite_priority_score));
    }
    if (s.priority_pool_reason) constraintLine.push(s.priority_pool_reason);
    if (s.composite_cap_reason) constraintLine.push(s.composite_cap_reason);
    return '<div style="line-height:1.45">' +
      '<div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap">' + poolHtml + '<span style="font-size:13px;font-weight:700;color:#0f172a">综合 ' + composite + '</span></div>' +
      '<div style="font-size:11px;color:#475569;margin-top:3px">' + esc(summary) + '</div>' +
      (structureLine ? '<div style="font-size:10px;color:#64748b;margin-top:2px">' + esc(structureLine) + '</div>' : '') +
      '<div style="font-size:10px;color:#94a3b8;margin-top:2px">' + esc(archetype + (extra.length ? ' · ' + extra.join(' · ') : '')) + '</div>' +
      (constraintLine.length ? '<div style="font-size:10px;color:#475569;margin-top:2px">' + esc(constraintLine.join(' · ')) + '</div>' : '') +
      '</div>';
  }
  function stockSignalCell(s) {
    var scoreText = s.score_highlights ? '加分：' + s.score_highlights : (s.discovery_score != null ? '发现层 ' + scoreNum(s.discovery_score) : '发现层 -');
    if (s.score_risks) scoreText += ' · 风险：' + s.score_risks;
    else if (s.qlib_rank != null) scoreText += ' · AI排行 #' + s.qlib_rank;
    if (!s || !s.setup_tag) {
      return '<div class="stock-signal-cell"><div class="stock-signal-title">暂无当前核心信号</div><div class="stock-signal-sub">' + esc(s.path_state || s.price_trend || '等待新的机构动作') + '</div><div class="stock-signal-meta">' + esc(scoreText) + '</div></div>';
    }
    return '<div class="stock-signal-cell">' +
      '<div>' + setupBadge(s.setup_tag, s.setup_priority, s.setup_confidence) + '</div>' +
      '<div class="stock-signal-title">' + esc(stockSignalHeadline(s)) + '</div>' +
      '<div class="stock-signal-sub">' + esc(stockSignalNarrative(s)) + '</div>' +
      '<div class="stock-signal-meta">' + esc(scoreText) + '</div>' +
      '</div>';
  }
  function stockExecutionCell(s) {
    // 与表格列、筛选共用同一份 stockGateInfo（可跟/关注/观察/回避）
    var info = stockGateInfo(s);
    var main = stockGateTag(s);
    var sub = info.reason || s.setup_execution_reason || s.priority_pool_reason || s.stage_reason || '';
    return '<div class="stock-exec-cell">' + main + (sub ? '<div class="stock-exec-sub">' + esc(sub) + '</div>' : '') + '</div>';
  }
  function sourceInstitutionCell(s) {
    var name = stockSourceName(s);
    var sub = s.setup_industry_name || s.sw_level3 || s.sw_level2 || s.sw_level1 || '-';
    return '<div class="stock-source-cell"><div class="stock-source-main">' + esc(name) + '</div><div class="stock-source-sub">' + esc(sub) + '</div></div>';
  }
  function stockReportCell(s) {
    return '<div class="stock-source-cell"><div class="stock-source-main">' + fmtDate(s.latest_report_date) + '</div><div class="stock-source-sub">' + esc(daysBetweenDates(s.latest_report_date, s.latest_notice_date)) + ' 披露</div></div>';
  }
  function stockReportAgeCell(s) {
    return '<div class="stock-source-cell"><div class="stock-source-main">' + esc(daysFromDateDigits(s.latest_report_date)) + '</div><div class="stock-source-sub">' + esc(gainText(s.setup_report_return_to_now != null ? s.setup_report_return_to_now : s.price_20d_pct)) + '</div></div>';
  }
  function focusMetric(label, value, sub) {
    return '<div class="stock-focus-metric"><div class="stock-focus-metric-label">' + label + '</div><div class="stock-focus-metric-value">' + value + '</div>' + (sub && sub !== '-' ? '<div class="stock-focus-metric-sub">' + esc(sub) + '</div>' : '') + '</div>';
  }
  function gradeChip(label, value) {
    return '<span class="stock-grade-chip">' + label + ' ' + (value != null ? String(value) + '级' : '-') + '</span>';
  }
  function instLink(id, name, type) { return '<span class="type-tag clickable-name" data-type="' + esc(type || 'other') + '" onclick="App.toggleInstDetail(\'' + esc(id) + '\',this)" style="cursor:pointer;font-size:11px">' + esc(name || '') + '</span>' }
  function typeTag(type, label) { return '<span class="type-tag" data-type="' + esc(type || 'other') + '">' + (label || esc(type || 'other')) + '</span>' }
  function evTag(type, label) { var cls = { new_entry: 'new', increase: 'up', decrease: 'down', exit: 'exit', unchanged: 'unchanged' }[type] || 'unchanged'; return '<span class="event-tag event-' + (cls) + '">' + esc(label || { new_entry: '新进', increase: '增持', decrease: '减持', exit: '退出', unchanged: '不变' }[type] || type) + '</span>' }

  // ============================================================
  // Table Sorting
  // ============================================================
  function makeSortable(tableEl) {
    if (!tableEl || tableEl.dataset.sortableReady === '1') return;
    tableEl.dataset.sortableReady = '1';
    var heads = tableEl.querySelectorAll('thead th');
    heads.forEach(function (th, idx) {
      // 跳过包含复选框或宽度很小的列（如全选列）
      if (th.querySelector('input[type="checkbox"]') || th.style.width === '30px') return;
      th.style.cursor = 'pointer';
      th.addEventListener('click', function () {
        var tbody = tableEl.querySelector('tbody');
        var rows = Array.from(tbody.querySelectorAll('tr'));
        var asc = th.dataset.sortDir !== 'asc';
        heads.forEach(h => { h.dataset.sortDir = ''; h.textContent = h.textContent.replace(/ [▲▼]/, ''); });
        th.dataset.sortDir = asc ? 'asc' : 'desc';
        th.textContent += asc ? ' ▲' : ' ▼';
        rows.sort(function (a, b) {
          var ca = a.children[idx], cb = b.children[idx];
          var va = (ca.textContent || '').trim(), vb = (cb.textContent || '').trim();
          var na = parseFloat(va.replace(/[,%+]/g, '')), nb = parseFloat(vb.replace(/[,%+]/g, ''));
          if (!isNaN(na) && !isNaN(nb)) return asc ? na - nb : nb - na;
          return asc ? va.localeCompare(vb, 'zh') : vb.localeCompare(va, 'zh');
        });
        rows.forEach(function (r) { tbody.appendChild(r); });
      });
    });
  }

  function scheduleSortableTables(root, selector) {
    var container = typeof root === 'string' ? el(root) : root;
    if (!container) return;
    var tables = Array.from(container.querySelectorAll(selector || '.data-table'));
    if (!tables.length) return;
    var idx = 0;
    function runBatch() {
      var startedAt = (window.performance && performance.now) ? performance.now() : Date.now();
      while (idx < tables.length) {
        makeSortable(tables[idx]);
        idx++;
        var now = (window.performance && performance.now) ? performance.now() : Date.now();
        if ((now - startedAt) > 8) break;
      }
      if (idx < tables.length) {
        if (typeof window.requestIdleCallback === 'function') {
          window.requestIdleCallback(runBatch, { timeout: 300 });
        } else {
          setTimeout(runBatch, 16);
        }
      }
    }
    if (typeof window.requestIdleCallback === 'function') {
      window.requestIdleCallback(runBatch, { timeout: 150 });
    } else {
      setTimeout(runBatch, 16);
    }
  }

  // ============================================================
  // Network Connectivity Check
  // ============================================================
  function setSourcePill(id, name, online, detail, pending) {
    var pill = el(id);
    if (!pill) return;
    pill.classList.remove('online', 'offline', 'pending');
    pill.classList.add(pending ? 'pending' : (online ? 'online' : 'offline'));
    var textEl = pill.querySelector('.source-pill-text');
    if (!textEl) return;
    var statusText = pending ? '检测中' : (online ? '在线' : '离线');
    var text = name + ' · ' + statusText;
    if (!pending && online && detail) text += ' · ' + detail;
    textEl.textContent = text;
  }
  function primeNetworkPills() {
    setSourcePill('sourcePillHoldings', '股东源', false, '', false);
    setSourcePill('sourcePillKline', 'K线源', false, '', false);
    setSourcePill('sourcePillIndustry', '行业源', false, '', false);
  }
  async function checkNetwork() {
    setSourcePill('sourcePillHoldings', '股东源', false, '', true);
    setSourcePill('sourcePillKline', 'K线源', false, '', true);
    setSourcePill('sourcePillIndustry', '行业源', false, '', true);
    var r = await api('/api/inst/update/connectivity');
    if (!r) {
      primeNetworkPills();
      return;
    }
    setSourcePill('sourcePillHoldings', '股东源', !!r.holdings_source, r.holdings_source_detail, false);
    setSourcePill('sourcePillKline', 'K线源', !!r.kline_source, r.kline_source_detail, false);
    setSourcePill('sourcePillIndustry', '行业源', !!r.industry_source, r.industry_source_detail, false);
  }

  // ============================================================
  // Scorecard
  // ============================================================
  function renderScoreParamCard(containerId, framework, config, defaults) {
    var items = framework?.editable_factors || [];
    return '<div class="score-rule-card" id="' + esc(containerId) + '">' +
      '<div class="score-rule-title">' + esc(framework?.title || '评分参数') + '</div>' +
      '<div class="score-param-list">' + items.map(function (item) {
        var current = config && config[item.key] != null ? config[item.key] : (defaults && defaults[item.key] != null ? defaults[item.key] : 0);
        var def = defaults && defaults[item.key] != null ? defaults[item.key] : 0;
        return '<div class="score-param-item">' +
          '<div class="score-param-head">' +
          '<div class="score-param-title">' + esc(item.label || item.key) + '</div>' +
          '<input type="number" class="score-input" data-key="' + esc(item.key) + '" value="' + esc(String(current)) + '" min="0" max="100">' +
          '</div>' +
          '<div class="score-param-desc">' + esc(item.description || '-') + '</div>' +
          '<div class="score-param-sub">默认 ' + esc(String(def)) + (item.source ? ' · 来源 ' + esc(item.source) : '') + '</div>' +
          '</div>';
      }).join('') + '</div>' +
      '</div>';
  }

  function renderInstFrameworkRules(framework) {
    return '<div class="score-rule-grid">' +
      '<div class="score-rule-card">' +
      '<div class="score-rule-title">固定口径</div>' +
      '<div class="score-rule-list">' +
      '<div class="score-rule-item"><b>评分公式</b>：' + esc(framework?.formula || '-') + '</div>' +
      '<div class="score-rule-item"><b>置信因子</b>：' + esc(framework?.confidence || '-') + '</div>' +
      '</div>' +
      '</div>' +
      '<div class="score-rule-card">' +
      '<div class="score-rule-title">当前定位</div>' +
      '<div class="score-rule-list">' +
      '<div class="score-rule-item">' + esc(framework?.summary || '-') + '</div>' +
      '</div>' +
      '</div>' +
      '</div>';
  }

  function renderInstScorecardStats(stats) {
    if (!stats) return '';
    var summary = stats.summary || {};
    var typeTop = Array.isArray(stats.type_top) ? stats.type_top : [];
    var hintTop = Array.isArray(stats.hint_top) ? stats.hint_top : [];
    var confidence = stats.confidence || {};

    var summaryCards = '<div class="scorecard-stats-grid">' +
      renderScorecardMiniCard('机构样本', fmt(summary.total || 0), '当前已生成画像的机构数') +
      renderScorecardMiniCard('买入口径', fmt(summary.buy_basis_count || 0), 'fallback ' + fmt(summary.fallback_basis_count || 0)) +
      renderScorecardMiniCard('高置信机构分', fmt(summary.quality_high_conf_count || 0), '均分 ' + scoreNum(summary.avg_quality_score)) +
      renderScorecardMiniCard('高置信可跟分', fmt(summary.follow_high_conf_count || 0), '均分 ' + scoreNum(summary.avg_followability_score)) +
      renderScorecardMiniCard('高分机构', fmt(summary.quality_strong_count || 0), 'quality ≥ 65') +
      renderScorecardMiniCard('高可跟机构', fmt(summary.followability_strong_count || 0), 'followability ≥ 65') +
      renderScorecardMiniCard('安全跟随机构', fmt(summary.safe_follow_inst_count || 0), '均安全样本 ' + fmt(summary.avg_safe_follow_event_count || 0)) +
      renderScorecardMiniCard('平均溢价', fmtGain(summary.avg_premium_pct), '均买入样本 ' + fmt(summary.avg_buy_event_count || 0)) +
      '</div>';

    var typeTable = typeTop.length
      ? '<div class="score-rule-card">' +
      '<div class="score-rule-title">机构类型分布</div>' +
      '<table class="score-pool-table"><thead><tr><th>类型</th><th>机构数</th><th>均机构分</th><th>均可跟分</th></tr></thead><tbody>' +
      typeTop.map(function (item) {
        return '<tr><td>' + esc(item.inst_type || '未分类') + '</td><td>' + fmt(item.total) + '</td><td>' + scoreNum(item.avg_quality_score) + '</td><td>' + scoreNum(item.avg_followability_score) + '</td></tr>';
      }).join('') +
      '</tbody></table>' +
      '</div>'
      : '';

    var confidenceCard = '<div class="score-rule-card">' +
      '<div class="score-rule-title">置信分层</div>' +
      '<div class="score-rule-list">' +
      '<div class="score-rule-item"><b>机构分</b>：' + renderConfidenceSummary(confidence.quality || []) + '</div>' +
      '<div class="score-rule-item"><b>可跟分</b>：' + renderConfidenceSummary(confidence.followability || []) + '</div>' +
      '</div>' +
      '</div>';

    var hintCard = hintTop.length
      ? '<div class="score-rule-card">' +
      '<div class="score-rule-title">可跟性提示分布</div>' +
      '<div class="score-rule-list">' + hintTop.map(function (item) {
        return '<div class="score-rule-item">' + esc(item.followability_hint || '未标注') + ' · ' + fmt(item.total) + ' 家</div>';
      }).join('') + '</div>' +
      '</div>'
      : '';

    return '<div class="score-rule-card" style="margin-bottom:12px">' +
      '<div class="score-rule-title">当前样本摘要</div>' +
      '<div class="scorecard-note">机构评分卡现在会直接展示真实机构画像分布，帮助判断这套机构评分与可跟性评分在当前样本中的覆盖、置信和主流提示结构。</div>' +
      '</div>' +
      summaryCards +
      '<div class="score-rule-grid" style="margin-top:12px">' + typeTable + confidenceCard + '</div>' +
      hintCard;
  }

  function renderConfidenceSummary(items) {
    if (!items || !items.length) return '-';
    return items.map(function (item) {
      return (item.confidence || '未标注') + ' ' + fmt(item.total || 0);
    }).join(' · ');
  }

  function renderStockFrameworkLayer(layer) {
    return '<div class="score-framework-card">' +
      '<div class="score-framework-head">' +
      '<div class="score-framework-title">' + esc(layer.label || '-') + '</div>' +
      '<span class="score-framework-weight">' + esc(String(layer.weight || 0)) + '%</span>' +
      '</div>' +
      '<div class="score-framework-role">' + esc(layer.role || '-') + '</div>' +
      '<div class="score-framework-summary">' + esc(layer.summary || '-') + '</div>' +
      '<div class="score-framework-list">' + (layer.items || []).map(function (item) {
        return '<div class="score-framework-item">' + esc(item) + '</div>';
      }).join('') + '</div>' +
      '</div>';
  }

  function renderStockFrameworkRules(framework) {
    var formulaCard = '<div class="score-rule-card">' +
      '<div class="score-rule-title">固定口径</div>' +
      '<div class="score-rule-list">' +
      '<div class="score-rule-item"><b>综合优先分</b>：' + esc(framework.formula || '-') + '</div>' +
      '<div class="score-rule-item"><b>' + esc((framework.effective_forecast || {}).label || '生效预测分') + '</b>：' + esc((framework.effective_forecast || {}).formula || '-') + '</div>' +
      '<div class="score-rule-item">' + esc((framework.effective_forecast || {}).meaning || '-') + '</div>' +
      '</div>' +
      '</div>';
    var capsCard = '<div class="score-rule-card">' +
      '<div class="score-rule-title">封顶与门槛</div>' +
      '<div class="score-rule-list">' + (framework.caps || []).map(function (item) {
        return '<div class="score-rule-item">' + esc(item) + '</div>';
      }).join('') + '</div>' +
      '</div>';
    var poolRows = (framework.pools || []).map(function (item) {
      return '<tr><td>' + priorityPoolTag(item.label) + '</td><td>' + esc(item.gate || '-') + '</td><td>' + esc(item.meaning || '-') + '</td></tr>';
    }).join('');
    var poolCard = '<div class="score-rule-card">' +
      '<div class="score-rule-title">池子规则</div>' +
      '<table class="score-pool-table"><thead><tr><th>池子</th><th>门槛</th><th>含义</th></tr></thead><tbody>' + poolRows + '</tbody></table>' +
      '</div>';
    return '<div class="score-rule-grid">' + formulaCard + capsCard + '</div>' + poolCard;
  }

  function renderQlibFeatureStackLabel(params) {
    if (!params) return '-';
    var parts = [];
    if (params.use_alpha158) parts.push('Alpha158');
    if (params.use_financial) parts.push('financial');
    if (params.use_institution) parts.push('institution');
    return parts.length ? parts.join(' + ') : '-';
  }

  function renderQlibFactorGroupSummary(groups) {
    if (!groups || !groups.length) return '<div class="muted" style="font-size:12px">暂无因子组摘要。</div>';
    return '<div class="score-rule-list">' + groups.map(function (item) {
      return '<div class="score-rule-item"><b>' + esc(item.factor_group || 'unknown') + '</b> · 因子 ' +
        fmt(item.factor_count || 0) + ' · 总重要性 ' + scoreNum(item.total_importance) +
        ' · 均值 ' + scoreNum(item.avg_importance) + '</div>';
    }).join('') + '</div>';
  }

  function renderQlibTopFactors(topFactors) {
    if (!topFactors || !topFactors.length) return '<div class="muted" style="font-size:12px">暂无 Top 因子。</div>';
    return '<div class="score-rule-list">' + topFactors.map(function (item) {
      return '<div class="score-rule-item">' + esc(item.factor_name || '-') + ' · ' +
        esc(item.factor_group || 'unknown') + ' · ' + scoreNum(item.importance) + '</div>';
    }).join('') + '</div>';
  }

  function renderQlibSummaryBlock(summary, opts) {
    if (!summary || !summary.model_id) {
      return '<div class="score-rule-card">' +
        '<div class="score-rule-title">Qlib 模型摘要</div>' +
        '<div class="scorecard-note">当前还没有可复用的 Qlib 训练模型，Forecast 会退回基础排序增强。</div>' +
        '</div>';
    }
    opts = opts || {};
    var latestBacktest = summary.latest_backtest || null;
    var title = opts.title || 'Qlib 模型摘要';
    var note = opts.note || '评分和验证里与预测相关的部分，优先直接复用完整版 Qlib 的真实训练产物，而不是再造一套并行预测体系。';
    var metricGrid = '<div class="scorecard-stats-grid">' +
      renderScorecardMiniCard('模型', esc(summary.model_id), esc(fmtDate(summary.predict_date))) +
      renderScorecardMiniCard('预测覆盖', fmt(summary.prediction_count || 0), '训练股票 ' + fmt(summary.stock_count || 0)) +
      renderScorecardMiniCard('IC', scoreNum(summary.ic_mean), 'RankIC ' + scoreNum(summary.rank_ic_mean)) +
      renderScorecardMiniCard('Top50测试', fmtGain(summary.test_top50_avg_return != null ? summary.test_top50_avg_return * 100 : null), renderQlibFeatureStackLabel(summary.train_params)) +
      renderScorecardMiniCard('因子数', fmt(summary.factor_count || 0), esc((summary.factor_groups || []).length + ' 个因子组')) +
      renderScorecardMiniCard('完成时间', esc(fmtDate(summary.finished_at)), esc(summary.status || '-')) +
      '</div>';
    var ruleGrid = '<div class="score-rule-grid" style="margin-top:12px">' +
      '<div class="score-rule-card">' +
      '<div class="score-rule-title">训练窗口</div>' +
      '<div class="score-rule-list">' +
      '<div class="score-rule-item"><b>Train</b>：' + esc(summary.train_start || '-') + ' ~ ' + esc(summary.train_end || '-') + '</div>' +
      '<div class="score-rule-item"><b>Valid</b>：' + esc(summary.valid_start || '-') + ' ~ ' + esc(summary.valid_end || '-') + '</div>' +
      '<div class="score-rule-item"><b>Test</b>：' + esc(summary.test_start || '-') + ' ~ ' + esc(summary.test_end || '-') + '</div>' +
      '</div>' +
      '</div>' +
      '<div class="score-rule-card">' +
      '<div class="score-rule-title">因子组重要性</div>' +
      renderQlibFactorGroupSummary(summary.factor_group_top || summary.factor_groups || []) +
      '</div>' +
      '</div>';
    var factorCard = '<div class="score-rule-card" style="margin-top:12px">' +
      '<div class="score-rule-title">Top 因子</div>' +
      renderQlibTopFactors(summary.top_factors || []) +
      '</div>';
    var backtestCard = latestBacktest
      ? '<div class="score-rule-card" style="margin-top:12px">' +
      '<div class="score-rule-title">最近回测</div>' +
      '<div class="score-rule-list">' +
      '<div class="score-rule-item"><b>策略</b>：' + esc(latestBacktest.strategy || '-') + '</div>' +
      '<div class="score-rule-item"><b>Sharpe</b>：' + scoreNum(latestBacktest.sharpe_ratio) + ' · <b>Calmar</b>：' + scoreNum(latestBacktest.calmar_ratio) + '</div>' +
      '<div class="score-rule-item"><b>年化</b>：' + fmtGain(latestBacktest.annual_return != null ? latestBacktest.annual_return * 100 : null) + ' · <b>回撤</b>：' + fmtGain(latestBacktest.max_drawdown != null ? -latestBacktest.max_drawdown * 100 : null) + ' · <b>换手</b>：' + scoreNum(latestBacktest.turnover) + '</div>' +
      '</div>' +
      '</div>'
      : '<div class="score-rule-card" style="margin-top:12px">' +
      '<div class="score-rule-title">最近回测</div>' +
      '<div class="scorecard-note">当前库里还没有写入独立回测结果，验证页先直接使用 Qlib 模型指标、预测覆盖和快照后验。</div>' +
      '</div>';
    return '<div class="score-rule-card" style="margin-bottom:12px">' +
      '<div class="score-rule-title">' + esc(title) + '</div>' +
      '<div class="scorecard-note">' + esc(note) + '</div>' +
      '</div>' +
      metricGrid + ruleGrid + factorCard + backtestCard;
  }

  function renderStockScorecardStats(stats) {
    if (!stats) return '';
    var summary = stats.summary || {};
    var currentPools = Array.isArray(stats.current_pools) ? stats.current_pools : [];
    var archetypes = Array.isArray(stats.archetypes) ? stats.archetypes : [];
    var qlibSummary = stats.qlib_summary || {};
    var replay = stats.snapshot_replay || {};
    var coverage = replay.coverage || {};
    var baseline = replay.baseline || {};
    var byPool = Array.isArray(replay.by_pool) ? replay.by_pool : [];
    var snapshotPendingText = snapshotMaturityPendingText(
      {
        scored_snapshot_dates: summary.snapshot_scored_dates,
        scored_rows: summary.snapshot_scored_rows
      },
      {
        matured_10d_count: baseline.matured_10d_count,
        matured_30d_count: baseline.matured_30d_count,
        matured_60d_count: baseline.matured_60d_count
      }
    );

    var summaryCards = '<div class="scorecard-stats-grid">' +
      renderScorecardMiniCard('当前覆盖', fmt(summary.stock_count || 0), '已进入四层评分的股票') +
      renderScorecardMiniCard('Setup候选', fmt(summary.setup_count || 0), '当前带 Setup 标签的股票') +
      renderScorecardMiniCard('封顶样本', fmt(summary.capped_count || 0), '触发阶段/质量封顶') +
      renderScorecardMiniCard('A池股票', fmt(summary.a_pool_count || 0), '当前重点优先池') +
      renderScorecardMiniCard('快照样本', fmt(summary.snapshot_scored_rows || 0), '已写入四层主分的快照行') +
      renderScorecardMiniCard('快照日', fmt(summary.snapshot_scored_dates || 0), esc((summary.first_scored_snapshot_date || '-') + ' ~ ' + (summary.last_scored_snapshot_date || '-'))) +
      '</div>';

    var poolTable = currentPools.length
      ? '<div class="score-rule-card">' +
      '<div class="score-rule-title">当前池子分布</div>' +
      '<table class="score-pool-table"><thead><tr><th>池子</th><th>股票数</th><th>Setup</th><th>封顶</th><th>均综合</th><th>均质量</th><th>均阶段</th><th>近20日反馈</th></tr></thead><tbody>' +
      currentPools.map(function (item) {
        return '<tr>' +
          '<td>' + priorityPoolTag(item.priority_pool) + '</td>' +
          '<td>' + fmt(item.total) + '</td>' +
          '<td>' + fmt(item.setup_count) + '</td>' +
          '<td>' + fmt(item.capped_count) + '</td>' +
          '<td>' + scoreNum(item.avg_composite_score) + '</td>' +
          '<td>' + scoreNum(item.avg_quality_score) + '</td>' +
          '<td>' + scoreNum(item.avg_stage_score) + '</td>' +
          '<td>' + fmtGain(item.avg_price_20d_pct) + '</td>' +
          '</tr>';
      }).join('') +
      '</tbody></table>' +
      '</div>'
      : '';

    var archetypeTable = archetypes.length
      ? '<div class="score-rule-card">' +
      '<div class="score-rule-title">股票类型分布</div>' +
      '<table class="score-pool-table"><thead><tr><th>类型</th><th>股票数</th><th>A池</th><th>均质量</th><th>均阶段</th><th>均综合</th></tr></thead><tbody>' +
      archetypes.map(function (item) {
        return '<tr>' +
          '<td>' + esc(item.stock_archetype || '待分类') + '</td>' +
          '<td>' + fmt(item.total) + '</td>' +
          '<td>' + fmt(item.a_pool_count) + '</td>' +
          '<td>' + scoreNum(item.avg_quality_score) + '</td>' +
          '<td>' + scoreNum(item.avg_stage_score) + '</td>' +
          '<td>' + scoreNum(item.avg_composite_score) + '</td>' +
          '</tr>';
      }).join('') +
      '</tbody></table>' +
      '</div>'
      : '';

    var replayTable = byPool.length
      ? '<div class="score-rule-card">' +
      '<div class="score-rule-title">快照回放摘要</div>' +
      '<div class="scorecard-note">全量快照基线：10日 ' + fmtGain(baseline.avg_gain_10d) + ' · 30日 ' + fmtGain(baseline.avg_gain_30d) + ' · 60日 ' + fmtGain(baseline.avg_gain_60d) + '</div>' +
      (snapshotPendingText ? '<div class="scorecard-note">' + esc(snapshotPendingText) + '</div>' : '') +
      '<table class="score-pool-table"><thead><tr><th>池子</th><th>快照日</th><th>10日</th><th>30日</th><th>60日</th></tr></thead><tbody>' +
      byPool.map(function (item) {
        return '<tr>' +
          '<td>' + priorityPoolTag(item.priority_pool) + '</td>' +
          '<td>' + fmt(item.snapshot_days) + '</td>' +
          '<td><div>' + fmtGain(item.avg_gain_10d) + '</div><div class="muted" style="font-size:10px">样本 ' + fmt(item.matured_10d_count || 0) + ' · 胜率 ' + pct(item.win_rate_10d) + '</div></td>' +
          '<td><div>' + fmtGain(item.avg_gain_30d) + '</div><div class="muted" style="font-size:10px">样本 ' + fmt(item.matured_30d_count || 0) + ' · 胜率 ' + pct(item.win_rate_30d) + '</div></td>' +
          '<td><div>' + fmtGain(item.avg_gain_60d) + '</div><div class="muted" style="font-size:10px">样本 ' + fmt(item.matured_60d_count || 0) + ' · 胜率 ' + pct(item.win_rate_60d) + '</div></td>' +
          '</tr>';
      }).join('') +
      '</tbody></table>' +
      '</div>'
      : '';

    return '<div class="score-rule-card" style="margin-bottom:12px">' +
      '<div class="score-rule-title">当前样本与回放摘要</div>' +
      '<div class="scorecard-note">评分卡现在不只解释公式，也直接展示当前分池结构、股票类型分布和快照后验摘要，帮助判断这套口径在当前样本上的真实形态。</div>' +
      '</div>' +
      summaryCards +
      '<div style="margin-top:12px">' + renderQlibSummaryBlock(qlibSummary, { title: 'Qlib 真实模型摘要' }) + '</div>' +
      '<div class="score-rule-grid" style="margin-top:12px">' + poolTable + archetypeTable + '</div>' +
      replayTable;
  }

  function renderScorecardMiniCard(label, value, sub) {
    return '<div class="scorecard-mini-card">' +
      '<div class="scorecard-mini-label">' + esc(label || '-') + '</div>' +
      '<div class="scorecard-mini-value">' + value + '</div>' +
      '<div class="scorecard-mini-sub">' + esc(sub || '-') + '</div>' +
      '</div>';
  }

  async function loadInstScorecard() {
    var rs = await Promise.all([
      api('/api/inst/scoring/framework/institution'),
      api('/api/inst/scoring/config/institution'),
      api('/api/inst/scoring/framework/followability'),
      api('/api/inst/scoring/config/followability')
    ]);
    var instFw = rs[0]?.ok ? (rs[0].data || {}) : {};
    var instStats = rs[0]?.ok ? (rs[0].stats || {}) : {};
    var instCfg = rs[1]?.ok ? rs[1] : {};
    var followFw = rs[2]?.ok ? (rs[2].data || {}) : {};
    var followCfg = rs[3]?.ok ? rs[3] : {};

    el('instScorecardFramework').innerHTML =
      '<div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:12px;padding:14px 16px;margin-bottom:14px;font-size:12px;color:#475569;line-height:1.7">' +
      '<div style="font-size:14px;font-weight:700;color:#0f172a;margin-bottom:6px">机构评分双框架</div>' +
      '机构页当前同时维护“机构实力评分”和“可跟性评分”。前者回答机构信号本身好不好，后者回答普通跟随者是否容易复现。' +
      '</div>' +
      '<div class="score-framework-grid">' + (instFw.layers || []).map(renderStockFrameworkLayer).join('') + '</div>' +
      renderInstFrameworkRules(instFw) +
      '<div class="score-framework-grid" style="margin-top:14px">' + (followFw.layers || []).map(renderStockFrameworkLayer).join('') + '</div>' +
      renderInstFrameworkRules(followFw);

    el('instScorecardStats').innerHTML = renderInstScorecardStats(instStats);

    el('instScorecardParams').innerHTML =
      '<div class="score-rule-grid">' +
      renderScoreParamCard('instInstitutionParams', instFw, instCfg.config || {}, instCfg.defaults || {}) +
      renderScoreParamCard('instFollowabilityParams', followFw, followCfg.config || {}, followCfg.defaults || {}) +
      '</div>';
  }

  async function loadStockScorecard() {
    var fw = await api('/api/inst/scoring/framework/stock');
    if (!fw?.ok) return;
    var framework = fw.data || {};
    el('stockScoreFramework').innerHTML =
      '<div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:12px;padding:14px 16px;margin-bottom:14px;font-size:12px;color:#475569;line-height:1.7">' +
      '<div style="font-size:14px;font-weight:700;color:#0f172a;margin-bottom:6px">' + esc(framework.title || '四层研究评分框架') + '</div>' +
      esc(framework.summary || '-') +
      '</div>' +
      '<div class="score-framework-grid">' + (framework.layers || []).map(renderStockFrameworkLayer).join('') + '</div>';
    el('stockScoreRules').innerHTML = renderStockFrameworkRules(framework);
    el('stockScoreStats').innerHTML = renderStockScorecardStats(fw.stats || null);
  }

  function gatherScoreParams(containerIds) {
    var params = {};
    var ids = Array.isArray(containerIds) ? containerIds : [containerIds];
    ids.forEach(function (cid) {
      var c = el(cid);
      if (c) c.querySelectorAll('.score-input').forEach(function (inp) {
        params[inp.dataset.key] = parseFloat(inp.value) || 0;
      });
    });
    return params;
  }

  async function calcInstScore() {
    var instParams = gatherScoreParams('instInstitutionParams');
    var followParams = gatherScoreParams('instFollowabilityParams');
    await api('/api/inst/scoring/config/institution', { method: 'POST', body: JSON.stringify({ config: instParams }) });
    await api('/api/inst/scoring/config/followability', { method: 'POST', body: JSON.stringify({ config: followParams }) });
    var r = await api('/api/inst/scoring/calculate/institution', { method: 'POST' });
    if (r?.ok) { modalAlert(r.message || '计算完成'); loadResearch(); }
    else modalAlert('计算失败');
  }

  async function resetInstScore() {
    if (!await modalConfirm('恢复默认权重？')) return;
    await api('/api/inst/scoring/config/institution', { method: 'DELETE' });
    await api('/api/inst/scoring/config/followability', { method: 'DELETE' });
    loadInstScorecard();
  }

  // ============================================================
  // Init
  // ============================================================
  async function init() {
    renderIdleStepGrid();
    await checkHealth(); // ensure it awaits first to initialize module toggles
    showView('dashboard');
    el('btnUpdateAll').addEventListener('click', startUpdate);
    el('btnRefreshStatus')?.addEventListener('click', refreshWorkbenchStatus);
    el('btnStop').addEventListener('click', async () => {
      el('btnStop').disabled = true;
      el('btnStop').textContent = '停止中...';
      await api('/api/inst/update/stop', { method: 'POST' });
      addLog('已请求停止');
    });
    el('btnClearLog')?.addEventListener('click', () => el('updateLog').innerHTML = '');
    el('btnCopyLog')?.addEventListener('click', copyLogs);
    el('btnToggleSystemOps')?.addEventListener('click', () => togglePanel('btnToggleSystemOps', 'systemOpsPanel', '系统操作', '收起系统操作'));
    el('btnReset')?.addEventListener('click', resetDerivedData);
    el('btnSearchInst')?.addEventListener('click', searchInst);
    el('btnImportChecked')?.addEventListener('click', importChecked);
    el('mgmtSearch')?.addEventListener('keydown', function (e) { if (e.key === 'Enter') searchInst(); });
    el('btnBatchAlias')?.addEventListener('click', batchAlias);
    el('btnBatchType')?.addEventListener('click', batchType);
    el('btnBatchMerge')?.addEventListener('click', batchMerge);
    el('btnBatchBlack')?.addEventListener('click', batchBlack);
    el('btnBatchDelete')?.addEventListener('click', batchDelete);
    el('stockSearch')?.addEventListener('input', handleStockSearchInput);
    el('instSearch')?.addEventListener('input', filterInstList);
    el('btnLifeboat')?.addEventListener('click', runLifeboat);
    el('btnQlibTrain')?.addEventListener('click', startQlibTrain);
    el('btnQlibResetParams')?.addEventListener('click', resetQlibParams);
    el('btnEtfSync')?.addEventListener('click', async function () {
      var btn = this;
      btn.disabled = true; btn.textContent = '同步中...';
      el('etfMsg').textContent = '正在启动 ETF 同步任务...';
      var r = await api('/api/etf/sync', { method: 'POST' });
      if (!r || r.status !== 'ok') {
        btn.disabled = false; btn.textContent = '同步 ETF 数据';
        el('etfMsg').textContent = '启动失败: ' + (r?.message || '未知错误');
        return;
      }
      // 启动后台轮询，实时显示进度与日志
      pollEtfStatus(btn);
    });

    function pollEtfStatus(btn) {
      var msgEl = el('etfMsg');
      var box = el('etfLogBox');
      var poll = setInterval(async function () {
        var s = await api('/api/etf/status');
        var d = s && s.data;
        if (!d) return;
        var pct = (d.total > 0) ? Math.round((d.current || 0) / d.total * 100) : 0;
        var progressTxt = d.total > 0 ? (d.current + ' / ' + d.total + '  (' + pct + '%)') : '';
        var stageLabel = ({
          'idle': '等待', 'starting': '启动中', 'fetch_list': '拉取 ETF 列表',
          'write_universe': '写入资产池', 'sync_kline': '同步 K 线',
          'done': '完成', 'error': '失败'
        })[d.stage] || d.stage;
        if (msgEl) {
          msgEl.innerHTML = '<div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap">' +
            '<span style="font-weight:600">' + esc(stageLabel) + '</span>' +
            (progressTxt ? '<span class="muted">' + esc(progressTxt) + '</span>' : '') +
            '<span>' + esc(d.message || '') + '</span>' +
            '</div>' +
            (d.total > 0 ? '<div style="margin-top:6px;height:6px;background:#e2e8f0;border-radius:3px;overflow:hidden">' +
              '<div style="height:100%;width:' + pct + '%;background:' + (d.stage === 'error' ? '#ef4444' : '#3b82f6') + ';transition:width .3s"></div></div>' : '');
        }
        if (box && d.logs && d.logs.length) {
          box.innerHTML = d.logs.slice(-30).map(function (line) {
            var color = line.level === 'error' ? '#ef4444' : (line.level === 'warning' ? '#f59e0b' : '#475569');
            return '<div style="color:' + color + '">[' + (line.ts || '').slice(11, 19) + '] ' + esc(line.message) + '</div>';
          }).join('');
          box.scrollTop = box.scrollHeight;
        }
        if (!d.running) {
          clearInterval(poll);
          if (btn) { btn.disabled = false; btn.textContent = '同步 ETF 数据'; }
          if (d.stage === 'done') {
            loadEtf();
          }
        }
      }, 1500);
    }
    setInterval(checkHealth, 30000);
  }

  // ============================================================
  // ETF 研究板块
  // ============================================================
  var _etfCategoryFilter = 'all'; // current category filter for ETF list
  var _etfStrategyFilter = 'all'; // current strategy filter for ETF list

  // 通用 ETF helper 函数
  function etfNum(v, digits) {
    if (v == null || Number.isNaN(Number(v))) return '-';
    return Number(v).toFixed(digits == null ? 1 : digits);
  }
  function etfPctCell(v, invert) {
    if (v == null || Number.isNaN(Number(v))) return '<span class="muted">-</span>';
    var n = Number(v);
    var positive = invert ? n <= 0 : n >= 0;
    var cls = positive ? 'gain-pos' : 'gain-neg';
    var sign = n > 0 ? '+' : '';
    return '<span class="' + cls + '">' + sign + n.toFixed(2) + '%</span>';
  }
  function etfOverviewTone(state) {
    if (state === 'panic') return { bg: '#eff6ff', fg: '#2563eb', label: '恐慌待托底' };
    if (state === 'cooling') return { bg: '#fff7ed', fg: '#c2410c', label: '降温观察期' };
    if (state === 'heated') return { bg: '#fef2f2', fg: '#dc2626', label: '兑现降温期' };
    return { bg: '#f0fdf4', fg: '#166534', label: '趋势恢复期' };
  }
  function etfStrategyTone(kind) {
    if (kind === '趋势持有') return { bg: '#dcfce7', fg: '#166534' };
    if (kind === '网格候选') return { bg: '#dbeafe', fg: '#1d4ed8' };
    if (kind === '防守停泊') return { bg: '#f8fafc', fg: '#475569' };
    if (kind === '暂不参与') return { bg: '#fee2e2', fg: '#b91c1c' };
    return { bg: '#fef3c7', fg: '#b45309' };
  }
  function etfSetupTone(state) {
    if (state === '收敛待发') return { bg: '#dcfce7', fg: '#166534' };
    if (state === '趋势跟随') return { bg: '#dbeafe', fg: '#1d4ed8' };
    if (state === '低波防守') return { bg: '#f8fafc', fg: '#475569' };
    if (state === '结构松散') return { bg: '#fee2e2', fg: '#b91c1c' };
    return { bg: '#fef3c7', fg: '#b45309' };
  }
  function etfWatchTags(list, tone) {
    if (!list || !list.length) return '<span class="muted">-</span>';
    return list.map(function (item) {
      var meta = tone || { bg: '#eff6ff', fg: '#1d4ed8' };
      var extra = [];
      if (item.rotation_score != null) extra.push('轮动 ' + etfNum(item.rotation_score, 1));
      if (item.setup_state) extra.push(item.setup_state);
      if (item.strategy_type) extra.push(item.strategy_type);
      if (item.grid_step_pct != null) extra.push('步长 ' + etfNum(item.grid_step_pct, 1) + '%');
      return '<span style="display:inline-flex;align-items:center;gap:6px;padding:4px 10px;border-radius:999px;background:' + meta.bg + ';color:' + meta.fg + ';font-size:11px;font-weight:600;margin:4px 6px 0 0">' +
        esc((item.name || item.code || '-') + (extra.length ? ' · ' + extra.join(' · ') : '')) +
        '</span>';
    }).join('');
  }
  function etfCatColor(cat) {
    var map = {
      '宽基': '#2563eb', '跨境': '#7c3aed', '商品': '#b45309', '债券': '#64748b', '货币': '#94a3b8',
      '医疗健康': '#059669', '半导体': '#0891b2', '新能源': '#16a34a', '消费': '#d97706',
      '金融': '#dc2626', '军工': '#475569', '地产建筑': '#78716c', '周期资源': '#a16207',
      '数字科技': '#6366f1', '交通物流': '#0d9488', '电力公用': '#4f46e5', '汽车': '#ea580c',
      '高端制造': '#0284c7', '红利策略': '#be185d'
    };
    return map[cat] || '#0f766e';
  }

  // 主入口：加载 ETF 数据并分别渲染各子标签
  async function loadEtf() {
    var r = await api('/api/etf/list');
    if (!r?.data?.length) {
      _etfDataCache = null;
      var overviewBox = el('etfOverviewContainer');
      if (overviewBox) overviewBox.innerHTML = '';
      var c = el('etfTableContainer');
      if (c) c.innerHTML = '<div class="muted" style="padding:20px;text-align:center">暂无 ETF 数据，请点击上方同步按钮。</div>';
      return;
    }
    _etfDataCache = r;
    // 渲染当前活跃子标签
    loadEtfOverview();
    if (_currentEtfTab === 'list') loadEtfList();
  }

  function loadEtfOverview() {
    var r = _etfDataCache;
    if (!r?.data?.length) return;
    var overviewBox = el('etfOverviewContainer');
    if (!overviewBox) return;
    var ov = r.overview || {};
    var tone = etfOverviewTone(ov.market_state);
    var leadersHtml = etfWatchTags(ov.rotation_leaders, { bg: '#dcfce7', fg: '#166534' });
    var laggardsHtml = etfWatchTags(ov.rotation_laggards, { bg: '#fee2e2', fg: '#b91c1c' });

    // 策略计数卡 — 可点击跳转到 ETF 列表并过滤
    function stratBtn(label, count, stratType, color) {
      return '<div class="stat-card" style="cursor:pointer" data-strategy-filter="' + esc(stratType) + '" title="点击查看">' +
        '<div class="stat-value" style="color:' + color + '">' + esc(fmt(count || 0)) + '</div>' +
        '<div class="stat-label">' + esc(label) + '</div></div>';
    }

    overviewBox.innerHTML =
      '<div class="panel" style="margin-bottom:0">' +
      '<div class="panel-head" style="align-items:flex-start;gap:14px;flex-wrap:wrap">' +
      '<div style="min-width:280px;flex:1">' +
      '<div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:8px">' +
      '<span style="font-weight:700;font-size:15px">ETF 整体判断</span>' +
      '<span style="padding:4px 10px;border-radius:999px;background:' + tone.bg + ';color:' + tone.fg + ';font-size:12px;font-weight:700">' + esc(ov.regime_label || tone.label) + '</span>' +
      '<span class="muted">温度 ' + esc(etfNum(ov.temperature_score, 1)) + '</span>' +
      '</div>' +
      '<div style="font-size:13px;line-height:1.7;color:#334155">' + esc(ov.regime_reason || '暂无整体判断。') + '</div>' +
      '<div style="margin-top:8px;font-size:12px;color:#0f172a"><strong>当前动作：</strong>' + esc(ov.action_hint || '-') + '</div>' +
      '<div style="margin-top:6px;font-size:12px;color:#64748b"><strong>低频情景：</strong>' + esc(ov.macro_scenario || '-') + '。' + esc(ov.macro_note || '') + '</div>' +
      '<div style="margin-top:10px;font-size:12px;color:#0f172a"><strong>轮动规则：</strong>' + esc(ov.rotation_rule || '-') + '</div>' +
      '<div style="margin-top:10px;font-size:12px;color:#0f172a"><strong>关注名单：</strong>' + leadersHtml + '</div>' +
      '<div style="margin-top:6px;font-size:12px;color:#0f172a"><strong>回避名单：</strong>' + laggardsHtml + '</div>' +
      '</div>' +
      '<div style="min-width:240px;flex:1">' +
      '<div class="stats-row" style="grid-template-columns:repeat(5,minmax(0,1fr));margin-bottom:10px">' +
      '<div class="stat-card"><div class="stat-value">' + esc(etfNum(ov.positive_20d_ratio, 0)) + '%</div><div class="stat-label">宽基上涨占比</div></div>' +
      '<div class="stat-card"><div class="stat-value">' + esc(etfNum(ov.avg_momentum_20d, 1)) + '%</div><div class="stat-label">平均20日动量</div></div>' +
      '<div class="stat-card"><div class="stat-value">' + esc(etfNum(ov.avg_momentum_60d, 1)) + '%</div><div class="stat-label">平均60日动量</div></div>' +
      '<div class="stat-card"><div class="stat-value">' + esc(etfNum(ov.avg_volatility_20d, 1)) + '%</div><div class="stat-label">平均20日波动</div></div>' +
      '<div class="stat-card"><div class="stat-value">' + esc(etfNum(ov.avg_drawdown_60d, 1)) + '%</div><div class="stat-label">平均60日回撤</div></div>' +
      '</div>' +
      '<div class="stats-row" style="grid-template-columns:repeat(4,minmax(0,1fr));margin-bottom:0">' +
      stratBtn('趋势持有', ov.strategy_counts?.trend, '趋势持有', '#166534') +
      stratBtn('网格候选', ov.strategy_counts?.grid, '网格候选', '#1d4ed8') +
      stratBtn('防守停泊', ov.strategy_counts?.defensive, '防守停泊', '#b45309') +
      stratBtn('暂不参与', ov.strategy_counts?.avoid, '暂不参与', '#991b1b') +
      '</div>' +
      '</div>' +
      '</div>' +
      '</div>' +
      '<div id="overviewMiningSection" style="margin-top:14px"></div>' +
      '<div id="overviewRotationSection" style="margin-top:14px"></div>' +
      '<div id="overviewDeepPanel"></div>';

    // 策略计数卡点击 → 跳转到 ETF 列表并按策略过滤
    overviewBox.querySelectorAll('[data-strategy-filter]').forEach(function (card) {
      card.addEventListener('click', function () {
        _etfStrategyFilter = card.dataset.strategyFilter;
        _etfCategoryFilter = 'all';
        showEtfTab('list');
      });
    });

    // 加载挖掘建议（异步插入）
    _loadOverviewMining();
  }

  // 整体判断页 — 挖掘建议 + 轮动预测
  async function _loadOverviewMining() {
    var miningBox = el('overviewMiningSection');
    var rotationBox = el('overviewRotationSection');
    if (!miningBox) return;
    miningBox.innerHTML = '<div class="muted" style="padding:10px">加载挖掘建议...</div>';
    if (rotationBox) rotationBox.innerHTML = '';

    var r = await apiCached('/api/etf/mining?grid_topn=5&trend_topn=5&rotation_topn=5', SHORT_CACHE_TTL_MS);
    if (r?.status !== 'ok' || !r?.data) {
      miningBox.innerHTML = '<div class="muted">挖掘建议加载失败</div>';
      return;
    }
    var d = r.data || {};

    // --- 网格候选 + 趋势持有 ---
    function miningRow(title, sub, palette, extra) {
      return '<div style="padding:8px 10px;border-radius:10px;background:' + palette.bg + ';color:' + palette.fg + ';margin-bottom:6px;' + (extra?.cursor ? 'cursor:pointer' : '') + '"' + (extra?.attr || '') + '>' +
        '<div style="font-weight:700;font-size:12px">' + esc(title) + '</div>' +
        (sub ? '<div style="font-size:11px;line-height:1.6;margin-top:3px">' + sub + '</div>' : '') +
        '</div>';
    }

    var gridCards = (d.grid_candidates || []).map(function (item) {
      return miningRow(
        (item.name || item.code) + ' · 步长 ' + scoreNum(item.best_step_pct) + '%',
        esc(signedPct(item.backtest_return_pct) + ' · ' + fmt(item.backtest_trade_count || 0) + '次 · DD ' + pct(item.backtest_max_drawdown_pct)) +
        ' <span style="opacity:0.6;font-size:10px">▶ 深度分析</span>',
        { bg: '#dbeafe', fg: '#1d4ed8' },
        { cursor: true, attr: ' data-etf-analyze="' + esc(item.code) + '"' }
      );
    }).join('');

    var trendCards = (d.trend_candidates || []).map(function (item) {
      return miningRow(
        (item.name || item.code) + ' · ' + (item.action || '观察'),
        esc('4w ' + signedPct(item.relative_strength_4w) + ' / 12w ' + signedPct(item.relative_strength_12w)) +
        ' <span style="opacity:0.6;font-size:10px">▶ 深度分析</span>',
        { bg: '#dcfce7', fg: '#166534' },
        { cursor: true, attr: ' data-etf-analyze="' + esc(item.code) + '"' }
      );
    }).join('');

    miningBox.innerHTML =
      '<div style="display:flex;gap:16px;flex-wrap:wrap">' +
      '<div style="flex:1;min-width:260px">' +
      '<div style="font-weight:700;font-size:13px;margin-bottom:8px">🔷 网格候选 Top 5</div>' +
      (gridCards || '<div class="muted" style="font-size:12px">暂无</div>') +
      '</div>' +
      '<div style="flex:1;min-width:260px">' +
      '<div style="font-weight:700;font-size:13px;margin-bottom:8px">🟢 趋势持有 Top 5</div>' +
      (trendCards || '<div class="muted" style="font-size:12px">暂无</div>') +
      '</div>' +
      '</div>';

    // 绑定深度分析点击
    miningBox.querySelectorAll('[data-etf-analyze]').forEach(function (card) {
      card.addEventListener('click', function () {
        loadEtfDeepAnalysis(card.dataset.etfAnalyze, 'overviewDeepPanel');
      });
    });

    // --- 轮动预测 ---
    if (!rotationBox) return;
    var list = d.next_rotation_watchlist || [];
    if (!list.length) {
      rotationBox.innerHTML = '<div class="panel" style="padding:14px"><div style="font-weight:700;font-size:13px;margin-bottom:8px">行业轮动预测</div><div class="muted" style="font-size:12px">暂无可用 Qlib 预轮动行业</div></div>';
      return;
    }
    var top5 = list.slice(0, 5);
    var maxScore = Math.max.apply(null, top5.map(function (x) { return x.next_rotation_score || 0; }));
    if (maxScore <= 0) maxScore = 100;
    var barH = 32, gap = 6, padL = 110, padR = 50, svgW = 560;
    var svgH = top5.length * (barH + gap) + gap;
    var barColors = ['#16a34a', '#22c55e', '#4ade80', '#86efac', '#bbf7d0'];

    var svgBars = top5.map(function (item, i) {
      var score = item.next_rotation_score || 0;
      var barW = Math.max(4, (score / maxScore) * (svgW - padL - padR));
      var y = gap + i * (barH + gap);
      var bucket = item.rotation_bucket || '观察';
      return '<g>' +
        '<text x="' + (padL - 6) + '" y="' + (y + barH / 2 + 4) + '" text-anchor="end" font-size="11" font-weight="600" fill="#0f172a">' + esc(item.sector_name || '-') + '</text>' +
        '<rect x="' + padL + '" y="' + y + '" width="' + barW + '" height="' + barH + '" rx="5" fill="' + (barColors[i] || '#86efac') + '"/>' +
        '<text x="' + (padL + barW + 5) + '" y="' + (y + barH / 2 + 4) + '" font-size="10" font-weight="700" fill="#334155">' + etfNum(score, 1) + '</text>' +
        '<text x="' + (padL + 6) + '" y="' + (y + barH / 2 + 4) + '" font-size="9" fill="#fff" font-weight="600">' +
        'Qlib ' + etfNum(item.avg_qlib_percentile, 0) + ' · 高置信 ' + fmt(item.high_conviction_count || 0) + ' · ' + esc(bucket) +
        '</text>' +
        '</g>';
    }).join('');

    var modelNote = d.qlib_model_id
      ? '<span class="muted" style="font-size:10px;margin-left:12px">模型: ' + esc(d.qlib_model_id) + '</span>'
      : '';

    rotationBox.innerHTML =
      '<div class="panel" style="padding:14px">' +
      '<div style="font-weight:700;font-size:13px;margin-bottom:10px">行业轮动预测 Top 5' + modelNote + '</div>' +
      '<svg viewBox="0 0 ' + svgW + ' ' + svgH + '" style="width:100%;max-width:560px;height:auto">' + svgBars + '</svg>' +
      '</div>';
  }

  function loadEtfList() {
    var r = _etfDataCache;
    if (!r?.data?.length) return;
    var c = el('etfTableContainer');
    var filterBox = el('etfCategoryFilter');
    if (!c) return;

    // 提取所有分类并构建胶囊标签
    var categories = [];
    var catSet = {};
    r.data.forEach(function (e) {
      var cat = e.category || '其他';
      if (!catSet[cat]) { catSet[cat] = 0; }
      catSet[cat]++;
    });
    categories = Object.keys(catSet).sort(function (a, b) {
      var order = ['宽基', '医疗健康', '半导体', '新能源', '消费', '金融', '军工', '数字科技', '高端制造', '汽车', '电力公用', '地产建筑', '周期资源', '交通物流', '红利策略', '行业·其他', '跨境', '商品', '债券', '货币'];
      var ia = order.indexOf(a), ib = order.indexOf(b);
      return (ia < 0 ? 99 : ia) - (ib < 0 ? 99 : ib);
    });

    if (filterBox) {
      // 分类过滤行
      var filterHtml = '<div class="type-filter">';
      filterHtml += '<span class="type-tag' + (_etfCategoryFilter === 'all' ? ' active' : '') + '" data-etfcat="all">全部 (' + r.data.length + ')</span>';
      categories.forEach(function (cat) {
        var color = etfCatColor(cat);
        filterHtml += '<span class="type-tag' + (_etfCategoryFilter === cat ? ' active' : '') + '" data-etfcat="' + esc(cat) + '" style="--tc:' + color + '">' + esc(cat) + ' (' + catSet[cat] + ')</span>';
      });
      filterHtml += '</div>';
      // 策略过滤行
      var stratTypes = ['趋势持有', '网格候选', '防守停泊', '暂不参与'];
      filterHtml += '<div class="type-filter" style="margin-top:4px">';
      filterHtml += '<span class="type-tag' + (_etfStrategyFilter === 'all' ? ' active' : '') + '" data-etfstrat="all" style="font-size:11px">策略:全部</span>';
      stratTypes.forEach(function (s) {
        var st = etfStrategyTone(s);
        filterHtml += '<span class="type-tag' + (_etfStrategyFilter === s ? ' active' : '') + '" data-etfstrat="' + esc(s) + '" style="font-size:11px;--tc:' + st.fg + '">' + esc(s) + '</span>';
      });
      filterHtml += '</div>';
      filterBox.innerHTML = filterHtml;
      filterBox.querySelectorAll('[data-etfcat]').forEach(function (tag) {
        tag.addEventListener('click', function () {
          _etfCategoryFilter = tag.dataset.etfcat;
          loadEtfList();
        });
      });
      filterBox.querySelectorAll('[data-etfstrat]').forEach(function (tag) {
        tag.addEventListener('click', function () {
          _etfStrategyFilter = tag.dataset.etfstrat;
          loadEtfList();
        });
      });
    }

    // 过滤数据
    var filtered = r.data;
    if (_etfCategoryFilter !== 'all') filtered = filtered.filter(function (e) { return e.category === _etfCategoryFilter; });
    if (_etfStrategyFilter !== 'all') filtered = filtered.filter(function (e) { return e.strategy_type === _etfStrategyFilter; });

    // 代码 → xueqiu链接
    function xueqiuLink(code, name) {
      var prefix = code.startsWith('1') ? 'SZ' : 'SH';
      return '<a href="https://xueqiu.com/S/' + prefix + esc(code) + '" target="_blank" rel="noopener" style="color:var(--primary);font-size:11px" title="雪球">' + esc(code) + '</a>';
    }

    var head = '<table class="data-table"><thead><tr><th>名称</th><th>代码</th><th>分类</th><th>4周相强</th><th>12周相强</th><th>轮动分</th><th>日线结构</th><th>策略类型</th><th>参考步长</th><th>趋势</th></tr></thead><tbody>';
    var body = filtered.map(function (e) {
      var catColor = etfCatColor(e.category);
      var trendColor = e.trend_status === '多头' ? '#ef4444' : (e.trend_status === '空头' ? '#10b981' : '#64748b');
      var strategyTone = etfStrategyTone(e.strategy_type);
      var setupTone = etfSetupTone(e.setup_state);
      var rotationText = e.rotation_score != null ? etfNum(e.rotation_score, 1) + (e.rotation_bucket === 'leader' ? ' · 前排' : e.rotation_bucket === 'blacklist' ? ' · 回避' : '') : '—';
      return '<tr style="cursor:pointer" data-etf-code="' + esc(e.code) + '">' +
        '<td style="font-weight:600">' + esc(e.name) + '</td>' +
        '<td>' + xueqiuLink(e.code, e.name) + '</td>' +
        '<td><span style="padding:2px 8px;border-radius:999px;background:' + catColor + '14;color:' + catColor + ';font-size:11px;font-weight:600">' + esc(e.category) + '</span></td>' +
        '<td>' + etfPctCell(e.relative_strength_4w, false) + '</td>' +
        '<td>' + etfPctCell(e.relative_strength_12w, false) + '</td>' +
        '<td>' + esc(rotationText) + '</td>' +
        '<td><span style="padding:2px 8px;border-radius:999px;background:' + setupTone.bg + ';color:' + setupTone.fg + ';font-size:11px;font-weight:600">' + esc(e.setup_state || '-') + '</span></td>' +
        '<td><span style="padding:2px 8px;border-radius:999px;background:' + strategyTone.bg + ';color:' + strategyTone.fg + ';font-size:11px;font-weight:600" title="' + esc(e.strategy_reason || '') + '">' + esc(e.strategy_type || '-') + '</span></td>' +
        '<td>' + (e.grid_step_pct != null ? esc(etfNum(e.grid_step_pct, 1) + '%') : '<span class="muted">-</span>') + '</td>' +
        '<td style="color:' + trendColor + '">' + esc(e.trend_status) + '</td>' +
        '</tr>';
    }).join('');
    c.innerHTML = head + body + '</tbody></table>';
    // 点击行 → 在该行下方插入深度分析面板
    c.querySelectorAll('tr[data-etf-code]').forEach(function (row) {
      row.addEventListener('click', function () {
        var code = row.dataset.etfCode;
        // 移除已有的分析行
        var prev = c.querySelector('.etf-analysis-row');
        if (prev) prev.remove();
        // 在点击行后插入新的分析行
        var analysisRow = document.createElement('tr');
        analysisRow.className = 'etf-analysis-row';
        var td = document.createElement('td');
        td.colSpan = 10;
        td.id = 'etfListAnalysisPanel';
        td.style.padding = '0';
        td.style.background = 'var(--bg-subtle)';
        analysisRow.appendChild(td);
        row.parentNode.insertBefore(analysisRow, row.nextSibling);
        loadEtfDeepAnalysis(code, 'etfListAnalysisPanel');
      });
    });
  }

  // ============================================================
  // ETF 深度量化分析面板
  // ============================================================
  async function loadEtfDeepAnalysis(code, panelId) {
    panelId = panelId || 'etfDeepAnalysisPanel';
    var panel = el(panelId);
    if (!panel) return;
    panel.innerHTML = '<div class="panel" style="margin-top:14px"><div class="muted" style="padding:20px;text-align:center">加载 ' + esc(code) + ' 深度分析中...</div></div>';
    panel.scrollIntoView({ behavior: 'smooth', block: 'start' });

    var r = await api('/api/etf/analysis/' + encodeURIComponent(code));
    if (r?.status !== 'ok' || !r?.data) {
      panel.innerHTML = '<div class="panel" style="margin-top:14px"><div class="muted" style="padding:20px;text-align:center">分析失败: ' + esc(r?.message || r?.detail || '未知错误') + '</div></div>';
      return;
    }
    var d = r.data;
    var info = d.info || {};
    var verdict = d.verdict || {};
    var best = d.best_step || {};
    var bh = d.buy_hold || {};

    // --- 1. 头部信息卡 ---
    var ratingColors = { '强烈推荐': { bg: '#dcfce7', fg: '#166534' }, '推荐': { bg: '#dbeafe', fg: '#1d4ed8' }, '中性': { bg: '#fef3c7', fg: '#b45309' }, '谨慎': { bg: '#fee2e2', fg: '#991b1b' } };
    var rc = ratingColors[verdict.rating] || ratingColors['中性'];
    var recStrategy = d.recommended_strategy || '';
    var recStratBg = recStrategy === '网格交易' ? '#fef3c7' : '#dbeafe';
    var recStratFg = recStrategy === '网格交易' ? '#b45309' : '#1d4ed8';
    var headerHtml =
      '<div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:14px">' +
      '<div style="font-weight:700;font-size:16px">' + esc(info.name || code) + ' <span class="muted" style="font-size:12px;font-weight:400">' + esc(code) + '</span></div>' +
      '<span style="padding:3px 12px;border-radius:var(--radius-pill);background:' + rc.bg + ';color:' + rc.fg + ';font-size:12px;font-weight:700">' + esc(verdict.rating || '分析中') + '</span>' +
      (recStrategy ? '<span style="padding:3px 10px;border-radius:var(--radius-pill);background:' + recStratBg + ';color:' + recStratFg + ';font-size:11px;font-weight:700">推荐: ' + esc(recStrategy) + '</span>' : '') +
      (info.strategy_type ? '<span style="padding:2px 8px;border-radius:var(--radius-pill);background:var(--primary-light);color:var(--primary);font-size:11px;font-weight:600">' + esc(info.strategy_type) + '</span>' : '') +
      (info.setup_state ? '<span style="padding:2px 8px;border-radius:var(--radius-pill);background:var(--line-light);color:var(--text-2);font-size:11px;font-weight:600">' + esc(info.setup_state) + '</span>' : '') +
      '</div>';

    // --- 2. 核心指标对比卡 ---
    function metricCard(label, gridVal, bhVal, unit, better) {
      var gv = gridVal != null ? (typeof gridVal === 'number' ? gridVal.toFixed(2) : gridVal) : '-';
      var bv = bhVal != null ? (typeof bhVal === 'number' ? bhVal.toFixed(2) : bhVal) : '-';
      var gColor = 'var(--text)';
      var bColor = 'var(--text)';
      if (better === 'higher' && gridVal != null && bhVal != null) {
        gColor = gridVal >= bhVal ? 'var(--success)' : 'var(--muted)';
        bColor = bhVal >= gridVal ? 'var(--success)' : 'var(--muted)';
      } else if (better === 'lower' && gridVal != null && bhVal != null) {
        gColor = gridVal <= bhVal ? 'var(--success)' : 'var(--danger)';
        bColor = bhVal <= gridVal ? 'var(--success)' : 'var(--danger)';
      }
      return '<div style="text-align:center;min-width:100px">' +
        '<div class="muted" style="font-size:10px;margin-bottom:4px">' + esc(label) + '</div>' +
        '<div style="font-size:14px;font-weight:700;color:' + gColor + '">' + gv + (unit || '') + '</div>' +
        '<div style="font-size:11px;color:' + bColor + '">' + bv + (unit || '') + '</div>' +
        '</div>';
    }

    var comparisonHtml =
      '<div style="margin-bottom:14px">' +
      '<div style="display:flex;align-items:center;gap:12px;margin-bottom:8px">' +
      '<span style="font-weight:600;font-size:13px">核心指标对比</span>' +
      '<span style="padding:2px 8px;border-radius:var(--radius-pill);background:#dbeafe;color:#1d4ed8;font-size:10px;font-weight:600">网格 ' + (best.step_pct || '-') + '%</span>' +
      '<span style="padding:2px 8px;border-radius:var(--radius-pill);background:var(--line-light);color:var(--text-2);font-size:10px;font-weight:600">买入持有</span>' +
      '</div>' +
      '<div style="display:flex;gap:4px;flex-wrap:wrap;padding:12px;background:var(--bg-subtle);border-radius:var(--radius);border:1px solid var(--line)">' +
      metricCard('总收益', best.return_pct, bh.return_pct, '%', 'higher') +
      metricCard('年化收益', best.annual_return_pct, bh.annual_return_pct, '%', 'higher') +
      metricCard('最大回撤', best.max_drawdown_pct, bh.max_drawdown_pct, '%', 'lower') +
      metricCard('Sharpe', best.sharpe, bh.sharpe, '', 'higher') +
      metricCard('Calmar', best.calmar, bh.calmar, '', 'higher') +
      metricCard('胜率', best.win_rate, null, '%', null) +
      metricCard('交易次数', best.trade_count, null, '', null) +
      metricCard('回测天数', best.days, bh.days, '天', null) +
      '</div></div>';

    // --- 3. 全步长对比表 ---
    var stepsHtml = '';
    if (d.all_steps && d.all_steps.length) {
      var bestStep = best.step_pct;
      stepsHtml = '<div style="margin-bottom:14px">' +
        '<div style="font-weight:600;font-size:13px;margin-bottom:8px">全步长回测对比</div>' +
        '<div style="overflow-x:auto"><table class="data-table" style="font-size:11px"><thead><tr>' +
        '<th>步长</th><th>总收益</th><th>年化</th><th>最大回撤</th><th>Sharpe</th><th>Calmar</th><th>胜率</th><th>买卖次数</th>' +
        '</tr></thead><tbody>';
      d.all_steps.forEach(function (s) {
        var isBest = s.step_pct === bestStep;
        var rowStyle = isBest ? 'background:var(--primary-light);font-weight:600' : '';
        stepsHtml += '<tr style="' + rowStyle + '">' +
          '<td>' + s.step_pct + '%' + (isBest ? ' ★' : '') + '</td>' +
          '<td style="color:' + (s.return_pct >= 0 ? 'var(--danger)' : 'var(--success)') + '">' + signedPct(s.return_pct) + '</td>' +
          '<td>' + (s.annual_return_pct != null ? s.annual_return_pct.toFixed(1) + '%' : '-') + '</td>' +
          '<td>' + (s.max_drawdown_pct != null ? s.max_drawdown_pct.toFixed(2) + '%' : '-') + '</td>' +
          '<td>' + (s.sharpe != null ? s.sharpe.toFixed(2) : '-') + '</td>' +
          '<td>' + (s.calmar != null ? s.calmar.toFixed(1) : '-') + '</td>' +
          '<td>' + (s.win_rate != null ? s.win_rate.toFixed(0) + '%' : '-') + '</td>' +
          '<td>' + (s.buy_count || 0) + '买 / ' + (s.sell_count || 0) + '卖</td>' +
          '</tr>';
      });
      stepsHtml += '</tbody></table></div></div>';
    }

    // --- 4. 净值曲线 SVG ---
    var curveHtml = '';
    if (best.curve && best.curve.length > 2 && bh.curve && bh.curve.length > 2) {
      curveHtml = '<div style="margin-bottom:14px">' +
        '<div style="font-weight:600;font-size:13px;margin-bottom:8px">净值走势对比</div>' +
        _buildNavCurveSvg(best.curve, bh.curve, best.step_pct) +
        '</div>';
    }

    // --- 5. 多周期稳定性 ---
    var periodHtml = '';
    if (d.multi_period && d.multi_period.length) {
      periodHtml = '<div style="margin-bottom:14px">' +
        '<div style="font-weight:600;font-size:13px;margin-bottom:8px">多周期稳定性检验</div>' +
        '<div style="overflow-x:auto"><table class="data-table" style="font-size:11px"><thead><tr>' +
        '<th>窗口</th><th>天数</th><th>网格收益</th><th>持有收益</th><th>超额</th><th>网格DD</th><th>持有DD</th><th>最优步长</th>' +
        '</tr></thead><tbody>';
      d.multi_period.forEach(function (p) {
        var gb = p.best;
        var pbh = p.buy_hold;
        var gridR = gb ? gb.return_pct : null;
        var bhR = pbh ? pbh.return_pct : null;
        var excess = (gridR != null && bhR != null) ? (gridR - bhR).toFixed(2) : '-';
        var excessColor = excess !== '-' ? (parseFloat(excess) >= 0 ? 'var(--danger)' : 'var(--success)') : 'var(--muted)';
        periodHtml += '<tr>' +
          '<td style="font-weight:600">' + esc(p.window) + '</td>' +
          '<td>' + p.days + '</td>' +
          '<td style="color:' + ((gridR || 0) >= 0 ? 'var(--danger)' : 'var(--success)') + '">' + (gridR != null ? signedPct(gridR) : '-') + '</td>' +
          '<td style="color:' + ((bhR || 0) >= 0 ? 'var(--danger)' : 'var(--success)') + '">' + (bhR != null ? signedPct(bhR) : '-') + '</td>' +
          '<td style="color:' + excessColor + ';font-weight:600">' + (excess !== '-' ? (parseFloat(excess) >= 0 ? '+' : '') + excess + '%' : '-') + '</td>' +
          '<td>' + (gb ? (gb.max_drawdown_pct != null ? gb.max_drawdown_pct.toFixed(2) + '%' : '-') : '-') + '</td>' +
          '<td>' + (pbh ? (pbh.max_drawdown_pct != null ? pbh.max_drawdown_pct.toFixed(2) + '%' : '-') : '-') + '</td>' +
          '<td>' + (gb ? gb.step_pct + '%' : '-') + '</td>' +
          '</tr>';
      });
      periodHtml += '</tbody></table></div></div>';
    }

    // --- 6. 量化结论 ---
    var verdictHtml = '';
    if (verdict.lines && verdict.lines.length) {
      verdictHtml = '<div style="padding:12px 14px;background:' + rc.bg + ';border-radius:var(--radius);border:1px solid ' + rc.fg + '22">' +
        '<div style="font-weight:700;font-size:13px;margin-bottom:6px;color:' + rc.fg + '">量化基金经理结论 · ' + esc(verdict.rating) + '</div>' +
        verdict.lines.map(function (line) {
          return '<div style="font-size:12px;line-height:1.7;color:' + rc.fg + '">· ' + esc(line) + '</div>';
        }).join('') +
        '</div>';
    }

    panel.innerHTML = '<div class="panel" style="margin-top:14px">' +
      '<div class="panel-head" style="justify-content:space-between">' +
      '<span style="font-weight:600">深度量化分析</span>' +
      '<button class="btn-text" onclick="document.getElementById(\'' + panelId + '\').innerHTML=\'\'">关闭</button>' +
      '</div>' +
      headerHtml + comparisonHtml + curveHtml + stepsHtml + periodHtml + verdictHtml +
      '</div>';
    panel.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }

  // 净值曲线 SVG 生成（网格 vs 买入持有）
  function _buildNavCurveSvg(gridCurve, bhCurve, stepPct) {
    var W = 580, H = 200, padL = 50, padR = 20, padT = 20, padB = 30;
    var plotW = W - padL - padR, plotH = H - padT - padB;

    // 合并日期，找公共范围
    var allNavs = gridCurve.map(function (p) { return p.nav; }).concat(bhCurve.map(function (p) { return p.nav; }));
    var minNav = Math.min.apply(null, allNavs) * 0.98;
    var maxNav = Math.max.apply(null, allNavs) * 1.02;
    if (maxNav <= minNav) maxNav = minNav + 0.01;

    function toPath(curve, color) {
      if (!curve.length) return '';
      var pts = curve.map(function (p, i) {
        var x = padL + (i / (curve.length - 1)) * plotW;
        var y = padT + plotH - ((p.nav - minNav) / (maxNav - minNav)) * plotH;
        return (i === 0 ? 'M' : 'L') + x.toFixed(1) + ',' + y.toFixed(1);
      }).join(' ');
      return '<path d="' + pts + '" fill="none" stroke="' + color + '" stroke-width="2" stroke-linejoin="round"/>';
    }

    // Y 轴刻度
    var yTicks = 5;
    var yLines = '';
    for (var i = 0; i <= yTicks; i++) {
      var val = minNav + (maxNav - minNav) * (i / yTicks);
      var y = padT + plotH - (i / yTicks) * plotH;
      yLines += '<line x1="' + padL + '" y1="' + y.toFixed(1) + '" x2="' + (W - padR) + '" y2="' + y.toFixed(1) + '" stroke="var(--line)" stroke-width="0.5" stroke-dasharray="3,3"/>';
      yLines += '<text x="' + (padL - 4) + '" y="' + (y + 3).toFixed(1) + '" text-anchor="end" font-size="10" fill="var(--muted)">' + val.toFixed(2) + '</text>';
    }

    // X 轴日期标签（首尾 + 中间）
    var xLabels = '';
    [gridCurve[0], gridCurve[Math.floor(gridCurve.length / 2)], gridCurve[gridCurve.length - 1]].forEach(function (p, i) {
      if (!p) return;
      var x = padL + ([0, 0.5, 1][i]) * plotW;
      xLabels += '<text x="' + x.toFixed(1) + '" y="' + (H - 4) + '" text-anchor="middle" font-size="10" fill="var(--muted)">' + (p.date || '').slice(0, 10) + '</text>';
    });

    // 图例
    var legend =
      '<text x="' + (padL + 8) + '" y="' + (padT + 14) + '" font-size="11" fill="#3b82f6" font-weight="600">— 网格 ' + stepPct + '%</text>' +
      '<text x="' + (padL + 120) + '" y="' + (padT + 14) + '" font-size="11" fill="#94a3b8" font-weight="600">— 买入持有</text>';

    return '<svg viewBox="0 0 ' + W + ' ' + H + '" style="width:100%;max-width:' + W + 'px;height:auto;background:var(--panel);border:1px solid var(--line);border-radius:var(--radius-sm)">' +
      yLines + xLabels +
      toPath(bhCurve, '#94a3b8') +
      toPath(gridCurve, '#3b82f6') +
      legend +
      '</svg>';
  }

  // ETF Qlib 页面（独立于股东挖掘Qlib）
  async function loadEtfQlib() {
    var box = el('etfQlibContainer');
    if (!box) return;
    box.innerHTML = '<div class="muted" style="padding:40px;text-align:center">加载 ETF Qlib 概览...</div>';

    var r = await api('/api/etf/qlib-summary');
    if (r?.status !== 'ok' || !r?.data) {
      box.innerHTML = '<div class="panel" style="padding:20px"><div class="muted">加载失败: ' + esc(r?.message || '未知错误') + '</div></div>';
      return;
    }
    var data = r.data;
    var model = data.model;
    var sectors = data.sectors || [];

    // 模型信息
    var modelHtml = '';
    if (!model) {
      modelHtml = '<div class="panel" style="padding:20px"><div class="muted">尚未训练 Qlib 模型。请先在股东挖掘 > Qlib 页面训练模型。</div></div>';
      box.innerHTML = modelHtml;
      return;
    }
    var stockWarn = model.stock_count < 200
      ? '<span style="color:var(--danger);font-weight:600"> ⚠ 覆盖不足，建议重新训练（不设 sample_stock_limit）</span>'
      : '';
    modelHtml =
      '<div class="panel" style="margin-bottom:14px">' +
      '<div class="panel-head"><span style="font-weight:600">Qlib 模型概况</span></div>' +
      '<div style="display:flex;gap:16px;flex-wrap:wrap;font-size:12px">' +
      '<div>模型: <strong>' + esc(model.model_id) + '</strong></div>' +
      '<div>覆盖股票: <strong>' + fmt(model.stock_count) + '</strong>' + stockWarn + '</div>' +
      '<div>IC: <strong>' + (model.ic_mean != null ? model.ic_mean.toFixed(4) : '-') + '</strong></div>' +
      '<div>训练窗口: ' + esc(model.train_start || '-') + ' ~ ' + esc(model.test_end || '-') + '</div>' +
      '<div>创建: ' + esc((model.created_at || '').slice(0, 16)) + '</div>' +
      '</div></div>';

    // 行业 Qlib 热力表
    var sectorHtml = '';
    if (sectors.length) {
      sectorHtml =
        '<div class="panel">' +
        '<div class="panel-head"><span style="font-weight:600">行业 Qlib 共识</span> <span class="muted" style="font-size:11px">按 Qlib 均分位降序</span></div>' +
        '<div style="overflow-x:auto"><table class="data-table" style="font-size:12px"><thead><tr>' +
        '<th>行业</th><th>Qlib 均分位</th><th>高置信(≥80)</th><th>低置信(≤20)</th><th>覆盖股票</th><th>轮动分</th><th>轮动状态</th><th>代表ETF</th>' +
        '</tr></thead><tbody>';
      sectors.forEach(function (s) {
        var avg = s.avg_percentile != null ? s.avg_percentile.toFixed(1) : '-';
        var avgColor = s.avg_percentile >= 60 ? 'var(--danger)' : s.avg_percentile <= 40 ? 'var(--success)' : 'var(--text)';
        var bucket = s.rotation_bucket || '-';
        var bucketColor = bucket === 'leader' ? '#166534' : bucket === 'blacklist' ? '#b91c1c' : '#6b7280';
        var etfLinks = (s.etfs || []).map(function (e) {
          var prefix = e.code.startsWith('1') ? 'SZ' : 'SH';
          return '<a href="https://xueqiu.com/S/' + prefix + esc(e.code) + '" target="_blank" rel="noopener" style="color:var(--primary);font-size:11px">' + esc(e.name) + '</a>';
        }).join(', ') || '<span class="muted">-</span>';
        sectorHtml += '<tr>' +
          '<td style="font-weight:600">' + esc(s.sector_name) + '</td>' +
          '<td style="color:' + avgColor + ';font-weight:700">' + avg + '</td>' +
          '<td style="color:var(--danger)">' + (s.high_count || 0) + '</td>' +
          '<td style="color:var(--success)">' + (s.low_count || 0) + '</td>' +
          '<td>' + (s.stock_count || 0) + '</td>' +
          '<td>' + (s.rotation_score != null ? s.rotation_score.toFixed(1) : '-') + '</td>' +
          '<td><span style="padding:2px 6px;border-radius:999px;background:' + bucketColor + '18;color:' + bucketColor + ';font-size:10px;font-weight:600">' + esc(bucket) + '</span></td>' +
          '<td>' + etfLinks + '</td>' +
          '</tr>';
      });
      sectorHtml += '</tbody></table></div></div>';
    } else {
      sectorHtml = '<div class="panel" style="padding:20px"><div class="muted">暂无行业级 Qlib 数据。模型覆盖股票不足或行业映射缺失。</div></div>';
    }

    box.innerHTML = modelHtml + sectorHtml;
  }

  // 轮动预测 Top 5 — SVG 条形图（被整体判断调用）
  async function loadEtfRotation() {
    var box = el('etfRotationContainer');
    if (!box) return;
    box.innerHTML = '<div class="muted" style="padding:20px;text-align:center">加载轮动预测中...</div>';
    var r = await apiCached('/api/etf/mining?rotation_topn=5', SHORT_CACHE_TTL_MS);
    if (r?.status !== 'ok' || !r?.data) {
      box.innerHTML = '<div class="muted" style="padding:20px;text-align:center">加载失败</div>';
      return;
    }
    var list = r.data.next_rotation_watchlist || [];
    if (!list.length) {
      box.innerHTML = '<div class="muted" style="padding:20px;text-align:center">暂无可用的 Qlib 预轮动行业名单。尚需训练 Qlib 模型或数据不足。</div>';
      return;
    }
    // 取 Top 5
    var top5 = list.slice(0, 5);
    var maxScore = Math.max.apply(null, top5.map(function (x) { return x.next_rotation_score || 0; }));
    if (maxScore <= 0) maxScore = 100;

    var barH = 36, gap = 8, padL = 120, padR = 60, svgW = 600;
    var svgH = top5.length * (barH + gap) + gap;
    var barColors = ['#16a34a', '#22c55e', '#4ade80', '#86efac', '#bbf7d0'];

    var svgBars = top5.map(function (item, i) {
      var score = item.next_rotation_score || 0;
      var barW = Math.max(4, (score / maxScore) * (svgW - padL - padR));
      var y = gap + i * (barH + gap);
      var label = item.sector_name || '-';
      var bucket = item.rotation_bucket || '观察';
      var bucketColor = bucket === 'leader' ? '#166534' : bucket === 'blacklist' ? '#b91c1c' : '#6b7280';
      return '<g>' +
        '<text x="' + (padL - 8) + '" y="' + (y + barH / 2 + 4) + '" text-anchor="end" font-size="12" font-weight="600" fill="#0f172a">' + esc(label) + '</text>' +
        '<rect x="' + padL + '" y="' + y + '" width="' + barW + '" height="' + barH + '" rx="6" fill="' + (barColors[i] || '#86efac') + '"/>' +
        '<text x="' + (padL + barW + 6) + '" y="' + (y + barH / 2 + 4) + '" font-size="11" font-weight="700" fill="#334155">' + etfNum(score, 1) + '</text>' +
        '<text x="' + (padL + 8) + '" y="' + (y + barH / 2 + 4) + '" font-size="10" fill="#fff" font-weight="600">' +
        'Qlib ' + etfNum(item.avg_qlib_percentile, 0) + ' · 高置信 ' + fmt(item.high_conviction_count || 0) + ' · ' + esc(bucket) +
        '</text>' +
        '</g>';
    }).join('');

    var modelNote = r.data.qlib_model_id
      ? '<div class="muted" style="font-size:11px;margin-top:8px">Qlib 模型：' + esc(r.data.qlib_model_id) + '</div>'
      : '<div class="muted" style="font-size:11px;margin-top:8px">尚无可用 Qlib 模型，当前为基于预测特征的启发式排名。</div>';

    box.innerHTML =
      '<svg viewBox="0 0 ' + svgW + ' ' + svgH + '" style="width:100%;max-width:600px;height:auto">' +
      svgBars +
      '</svg>' +
      '<div style="margin-top:12px">' +
      top5.map(function (item, i) {
        var bucket = item.rotation_bucket || '观察';
        var bucketColor = bucket === 'leader' ? '#166534' : bucket === 'blacklist' ? '#b91c1c' : '#6b7280';
        return '<div style="display:flex;align-items:center;gap:10px;padding:8px 12px;border-radius:8px;background:#f8fafc;margin-bottom:6px">' +
          '<span style="font-weight:700;font-size:14px;color:#0f172a;min-width:24px">#' + (i + 1) + '</span>' +
          '<span style="font-weight:600;font-size:13px;color:#0f172a;min-width:80px">' + esc(item.sector_name || '-') + '</span>' +
          '<span style="padding:2px 8px;border-radius:999px;font-size:10px;font-weight:600;background:' + bucketColor + '18;color:' + bucketColor + '">' + esc(bucket) + '</span>' +
          '<span class="muted" style="font-size:11px">预轮动 ' + etfNum(item.next_rotation_score, 1) + ' · Qlib均 ' + etfNum(item.avg_qlib_percentile, 0) + ' · 高置信 ' + fmt(item.high_conviction_count || 0) + '</span>' +
          '</div>';
      }).join('') +
      '</div>' +
      modelNote;
  }

  // ============================================================
  // Qlib AI Tab
  // ============================================================
  function setQlibForm(params) {
    var cfg = Object.assign({}, QLIB_DEFAULT_PARAMS, params || {});
    el('qlibTrainStart').value = cfg.train_start || QLIB_DEFAULT_PARAMS.train_start;
    el('qlibTrainEnd').value = cfg.train_end || QLIB_DEFAULT_PARAMS.train_end;
    el('qlibValidEnd').value = cfg.valid_end || QLIB_DEFAULT_PARAMS.valid_end;
    el('qlibTestEnd').value = cfg.test_end || QLIB_DEFAULT_PARAMS.test_end;
    el('qlibBoostRounds').value = cfg.num_boost_round != null ? cfg.num_boost_round : QLIB_DEFAULT_PARAMS.num_boost_round;
    el('qlibEarlyStop').value = cfg.early_stopping_rounds != null ? cfg.early_stopping_rounds : QLIB_DEFAULT_PARAMS.early_stopping_rounds;
    el('qlibLeaves').value = cfg.num_leaves != null ? cfg.num_leaves : QLIB_DEFAULT_PARAMS.num_leaves;
    el('qlibLearningRate').value = cfg.learning_rate != null ? cfg.learning_rate : QLIB_DEFAULT_PARAMS.learning_rate;
    el('qlibSubsample').value = cfg.subsample != null ? cfg.subsample : QLIB_DEFAULT_PARAMS.subsample;
    el('qlibColsample').value = cfg.colsample_bytree != null ? cfg.colsample_bytree : QLIB_DEFAULT_PARAMS.colsample_bytree;
    el('qlibUseAlpha158').checked = cfg.use_alpha158 !== false;
    el('qlibUseFinancial').checked = cfg.use_financial !== false;
    el('qlibUseInstitution').checked = cfg.use_institution !== false;
  }

  function getQlibTrainParams() {
    return {
      train_start: el('qlibTrainStart')?.value || QLIB_DEFAULT_PARAMS.train_start,
      train_end: el('qlibTrainEnd')?.value || QLIB_DEFAULT_PARAMS.train_end,
      valid_end: el('qlibValidEnd')?.value || QLIB_DEFAULT_PARAMS.valid_end,
      test_end: el('qlibTestEnd')?.value || QLIB_DEFAULT_PARAMS.test_end,
      num_boost_round: Number(el('qlibBoostRounds')?.value || QLIB_DEFAULT_PARAMS.num_boost_round),
      early_stopping_rounds: Number(el('qlibEarlyStop')?.value || QLIB_DEFAULT_PARAMS.early_stopping_rounds),
      num_leaves: Number(el('qlibLeaves')?.value || QLIB_DEFAULT_PARAMS.num_leaves),
      learning_rate: Number(el('qlibLearningRate')?.value || QLIB_DEFAULT_PARAMS.learning_rate),
      subsample: Number(el('qlibSubsample')?.value || QLIB_DEFAULT_PARAMS.subsample),
      colsample_bytree: Number(el('qlibColsample')?.value || QLIB_DEFAULT_PARAMS.colsample_bytree),
      use_alpha158: !!el('qlibUseAlpha158')?.checked,
      use_financial: !!el('qlibUseFinancial')?.checked,
      use_institution: !!el('qlibUseInstitution')?.checked
    };
  }

  function qlibWindowText(model) {
    if (!model) return '-';
    var start = fmtDate(model.train_start);
    var end = fmtDate(model.test_end || model.train_end);
    if (start === '-' || end === '-') return '-';
    return start + ' ~ ' + end;
  }

  function qlibParamSummaryText(params) {
    if (!params) return '当前使用默认训练参数。';
    var toggles = [];
    if (params.use_alpha158) toggles.push('Alpha158');
    if (params.use_financial) toggles.push('财务因子');
    if (params.use_institution) toggles.push('机构因子');
    return [
      '轮数 ' + params.num_boost_round,
      '早停 ' + params.early_stopping_rounds,
      '叶子 ' + params.num_leaves,
      '学习率 ' + params.learning_rate,
      toggles.length ? ('特征 ' + toggles.join(' + ')) : '特征 全关'
    ].join(' · ');
  }

  function resetQlibParams() {
    setQlibForm(QLIB_DEFAULT_PARAMS);
    el('qlibParamSummary').textContent = qlibParamSummaryText(QLIB_DEFAULT_PARAMS);
  }

  async function startQlibTrain() {
    var btn = el('btnQlibTrain');
    if (!btn) return;
    var params = getQlibTrainParams();
    btn.disabled = true;
    btn.textContent = '训练中...';
    el('qlibTrainMsg').textContent = '正在提交训练任务，Qlib 结果会同步到股票研究列表。';
    el('qlibParamSummary').textContent = qlibParamSummaryText(params);
    var r = await api('/api/qlib/train', { method: 'POST', body: JSON.stringify(params) });
    if (!r?.ok) {
      btn.disabled = false;
      btn.textContent = '重新计算评分';
      el('qlibTrainMsg').textContent = r?.message || '启动失败';
      return;
    }
    el('qlibTrainMsg').textContent = r.message || '训练任务已启动';
    var poll = setInterval(async function () {
      var s = await api('/api/qlib/status');
      if (s && !s.training) {
        clearInterval(poll);
        loadQlib();
      }
    }, 5000);
  }

  async function loadQlib() {
    var status = await api('/api/qlib/status');
    if (!status) return;
    el('qlibStatStatus').style.color = '';
    var model = status.model || null;
    var params = null;
    if (model && model.train_params_json) {
      try { params = JSON.parse(model.train_params_json); } catch (e) { params = null; }
    }
    setQlibForm(params || QLIB_DEFAULT_PARAMS);
    el('qlibParamSummary').textContent = qlibParamSummaryText(params || QLIB_DEFAULT_PARAMS);

    // 依赖检查
    if (!status.available) {
      el('qlibStatStatus').textContent = '不可用';
      el('qlibStatFinished').textContent = '-';
      el('qlibStatStocks').textContent = '-';
      el('qlibStatFactors').textContent = '-';
      el('qlibStatWindow').textContent = '-';
      el('qlibTrainMsg').innerHTML = '<span style="color:#ef4444">依赖未安装: ' + esc(status.dependency_error || '') + '</span><br>请运行: pip3 install lightgbm scikit-learn';
      el('btnQlibTrain').disabled = true;
      el('btnQlibTrain').textContent = '重新计算评分';
      el('qlibFactorChart').innerHTML = '<div class="muted">Qlib 依赖不可用</div>';
      return;
    }

    if (status.training) {
      el('qlibStatStatus').textContent = '训练中...';
      el('qlibStatFinished').textContent = fmtDate(model?.finished_at);
      el('qlibStatStocks').textContent = fmt(model?.stock_count || 0);
      el('qlibStatFactors').textContent = fmt(model?.factor_count || 0);
      el('qlibStatWindow').textContent = qlibWindowText(model);
      el('btnQlibTrain').disabled = true;
      el('btnQlibTrain').textContent = '训练中...';
      el('qlibTrainMsg').textContent = '模型训练中，完成后会自动刷新最新因子和股票研究列表。';
      setTimeout(loadQlib, 5000);
      return;
    }

    if (!model || model.status !== 'trained') {
      el('qlibStatStatus').textContent = model?.status === 'failed' ? '失败' : '未训练';
      el('qlibStatFinished').textContent = fmtDate(model?.finished_at);
      el('qlibStatStocks').textContent = fmt(model?.stock_count || 0);
      el('qlibStatFactors').textContent = fmt(model?.factor_count || 0);
      el('qlibStatWindow').textContent = qlibWindowText(model);
      el('qlibTrainMsg').textContent = model?.error || '调整参数后点击“重新计算评分”，训练完成后会回写到股票研究列表。';
      el('btnQlibTrain').disabled = false;
      el('btnQlibTrain').textContent = '重新计算评分';
      el('qlibFactorChart').innerHTML = '<div class="muted">暂无因子数据</div>';
      return;
    }

    // 已训练
    el('qlibStatStatus').textContent = '已训练';
    el('qlibStatStatus').style.color = '#10b981';
    el('qlibStatFinished').textContent = fmtDate(model.finished_at);
    el('qlibStatStocks').textContent = fmt(model.stock_count || 0);
    el('qlibStatFactors').textContent = fmt(model.factor_count || 0);
    el('qlibStatWindow').textContent = qlibWindowText(model);
    el('qlibTrainMsg').textContent = '模型ID: ' + (model.model_id || '-') + ' · 训练完成后已同步最新 Qlib 排名到股票研究列表。';
    el('btnQlibTrain').disabled = false;
    el('btnQlibTrain').textContent = '重新计算评分';

    var factors = await api('/api/qlib/factors');
    if (factors?.data) renderQlibFactorChart(factors.data);
    else el('qlibFactorChart').innerHTML = '<div class="muted">暂无因子数据</div>';
  }

  function renderQlibFactorChart(factors) {
    var top = factors.slice(0, 20);
    if (!top.length) { el('qlibFactorChart').innerHTML = '<div class="muted">无因子数据</div>'; return; }
    var maxImp = Math.max.apply(null, top.map(function (f) { return f.importance || 0 }));
    if (maxImp <= 0) maxImp = 1;
    var barH = 18, gap = 4, pad = 120, w = 400, h = top.length * (barH + gap) + 10;
    var bars = top.map(function (f, i) {
      var y = i * (barH + gap) + 5;
      var barW = Math.max(2, (f.importance || 0) / maxImp * (w - pad - 20));
      var color = f.factor_group === 'institution' ? '#10b981' : (f.factor_group === 'financial' ? '#f59e0b' : '#3b82f6');
      var label = (f.factor_name || '').replace('inst_', '机构_');
      return '<text x="' + (pad - 4) + '" y="' + (y + barH - 4) + '" text-anchor="end" font-size="10" fill="#475569">' + esc(label) + '</text>' +
        '<rect x="' + pad + '" y="' + y + '" width="' + barW + '" height="' + barH + '" rx="3" fill="' + color + '" opacity="0.8"/>' +
        '<text x="' + (pad + barW + 4) + '" y="' + (y + barH - 4) + '" font-size="9" fill="#94a3b8">' + Number(f.importance).toFixed(0) + '</text>';
    }).join('');
    el('qlibFactorChart').innerHTML =
      '<div style="margin-bottom:6px"><span style="display:inline-block;width:10px;height:10px;background:#3b82f6;border-radius:2px;margin-right:4px"></span><span style="font-size:11px;color:#64748b">Alpha158</span> <span style="display:inline-block;width:10px;height:10px;background:#f59e0b;border-radius:2px;margin-left:12px;margin-right:4px"></span><span style="font-size:11px;color:#64748b">财务因子</span> <span style="display:inline-block;width:10px;height:10px;background:#10b981;border-radius:2px;margin-left:12px;margin-right:4px"></span><span style="font-size:11px;color:#64748b">机构因子</span></div>' +
      '<svg viewBox="0 0 ' + w + ' ' + h + '" style="width:100%;height:' + h + 'px">' + bars + '</svg>';
  }

  // ============================================================
  // Lifeboat
  // ============================================================
  async function runLifeboat() {
    var btn = el('btnLifeboat');
    if (!btn) return;
    btn.disabled = true;
    btn.textContent = '运行中...';
    var r = await api('/api/inst/lifeboat/run', { method: 'POST' });
    if (!r?.ok) {
      showModal('救生艇', r?.message || '启动失败');
      btn.disabled = false;
      btn.textContent = '救生艇';
      return;
    }
    // 轮询等待完成
    var polls = 0;
    var timer = setInterval(async function () {
      polls++;
      var s = await api('/api/inst/lifeboat/status');
      if (s && !s.running) {
        clearInterval(timer);
        btn.disabled = false;
        btn.textContent = '救生艇';
        if (s.result?.ok) {
          showModal('救生艇', '报告已生成！<br><br><a href="/api/inst/lifeboat/report" target="_blank" style="color:#3b82f6;font-weight:600">点击查看报告</a><br><br><span style="font-size:12px;color:#94a3b8">也可双击 lifeboat/run.command 独立运行</span>');
        } else {
          showModal('救生艇', '运行失败：' + (s.result?.message || '未知错误'));
        }
      }
      if (polls > 120) { clearInterval(timer); btn.disabled = false; btn.textContent = '救生艇'; }
    }, 3000);
  }
  window.App = { saveModuleSettings, showView, setAlias, setType, toggleBlack, deleteInst, restoreInst, calcInstScore, resetInstScore, toggleInstDetail, toggleInstBreakdown, toggleStockDetail, switchInstDim, switchStockDim, runSingleStep, openSectorValidation, clearStockValidationFilter, _api: api };
  document.addEventListener('DOMContentLoaded', init);
})();
