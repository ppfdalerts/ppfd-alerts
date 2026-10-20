/* Larger type, fitted to the dashboard's original cell and line dimensions. */
(function () {
  'use strict';
  var undo = [], observer, frame = 0, enabled = true, busy = false, revision = 0;
  var canvas = document.createElement('canvas'), measure = canvas.getContext('2d');
  var sizes = new Map();
  var number = function (value) { return parseFloat(value) || 0; };
  var visible = function (node) { return !!(node.isConnected && node.getClientRects().length && node.getBoundingClientRect().height); };
  var report = {revision:0, tables:0, fittedCells:0, labels:0};

  function remember(node, properties) {
    var saved = properties.map(function (key) { return [key, node.style.getPropertyValue(key), node.style.getPropertyPriority(key)]; });
    undo.push(function () {
      saved.forEach(function (item) {
        if (item[1]) node.style.setProperty(item[0], item[1], item[2]);
        else node.style.removeProperty(item[0]);
      });
    });
  }
  function restore() {
    for (var i = undo.length - 1; i >= 0; i--) undo[i]();
    undo = [];
  }
  function watch() {
    if (enabled) observer.observe(document.body, {subtree:true, childList:true, characterData:true, attributes:true, attributeFilter:['class','style','hidden','open']});
  }
  function fontNodes(root, fixedLine) {
    return [root].concat(Array.from(root.querySelectorAll('*'))).map(function (node) {
      var style = getComputedStyle(node);
      remember(node, ['font-size','line-height']);
      var font = {node:node, size:number(style.fontSize), line:style.lineHeight, family:style.fontFamily, weight:style.fontWeight};
      if (fixedLine) {
        measure.font = style.fontStyle + ' ' + style.fontWeight + ' ' + style.fontSize + ' ' + style.fontFamily;
        var ink = measure.measureText('Mg' + node.textContent);
        font.fixedLine = number(style.lineHeight) || ink.fontBoundingBoxAscent + ink.fontBoundingBoxDescent || font.size * 1.2;
        font.inkHeight = ink.actualBoundingBoxAscent + ink.actualBoundingBoxDescent;
      }
      return font;
    });
  }
  function setScale(fonts, factor) {
    fonts.forEach(function (font) {
      font.node.style.fontSize = (font.size * factor) + 'px';
      font.node.style.lineHeight = font.fixedLine ? font.fixedLine + 'px' : font.line === 'normal' ? 'normal' : (number(font.line) * factor) + 'px';
    });
    fonts[0].node.style.setProperty('--ppfd-type-factor', factor);
  }
  function fitTable(table) {
    if (!visible(table) || !table.tBodies.length || !table.tBodies[0].rows.length) return;
    var dataRow = Array.from(table.tBodies[0].rows).find(function (row) { return row.cells.length > 1 && Array.from(row.cells).every(function (cell) { return cell.colSpan === 1; }); });
    if (!dataRow) return;
    var tableRect = table.getBoundingClientRect();
    var widths = Array.from(dataRow.cells).map(function (cell) { return cell.getBoundingClientRect().width; });
    var rows = Array.from(table.rows).map(function (row) { return {node:row, height:row.getBoundingClientRect().height}; });
    var personnelColumns = [];
    var cells = Array.from(table.querySelectorAll('th,td')).map(function (cell) {
      var rect = cell.getBoundingClientRect(), style = getComputedStyle(cell);
      return {node:cell, width:rect.width - number(style.paddingLeft) - number(style.paddingRight) - number(style.borderLeftWidth) - number(style.borderRightWidth),
        height:rect.height - number(style.paddingTop) - number(style.paddingBottom) - number(style.borderTopWidth) - number(style.borderBottomWidth),
        font:number(style.fontSize)};
    });
    /* Freeze only the measured table grid. Original spacing and responsive
       layout are restored and remeasured before every update or resize. */
    remember(table, ['width','table-layout']);
    table.style.width = tableRect.width + 'px';
    table.style.tableLayout = 'fixed';
    var columns = document.createElement('colgroup');
    widths.forEach(function (width) { var col = document.createElement('col'); col.style.width = width + 'px'; columns.appendChild(col); });
    table.prepend(columns);
    undo.push(function () { columns.remove(); });
    rows.forEach(function (row) { remember(row.node, ['height']); row.node.style.height = row.height + 'px'; });
    cells.forEach(function (cell) {
      if (cell.node.querySelector('.activity-chart') || cell.width < 1 || cell.height < 1) return;
      var inner = document.createElement('div'); inner.className = 'ppfd-fit-content';
      while (cell.node.firstChild) inner.appendChild(cell.node.firstChild);
      cell.node.appendChild(inner);
      cell.node.classList.add('ppfd-fit-cell');
      undo.push(function () {
        if (inner.parentNode === cell.node) { while (inner.firstChild) cell.node.insertBefore(inner.firstChild, inner); inner.remove(); }
        cell.node.classList.remove('ppfd-fit-cell');
      });
      var fixedLine = table.id === 'board-personnel' && cell.node.tagName === 'TD';
      var fonts = fontNodes(inner, fixedLine);
      var key = [cell.width,cell.height,cell.node.tagName,cell.node.className,document.body.className,inner.innerHTML,
        fonts.map(function (font) { return [font.size,font.line,font.family,font.weight,font.fixedLine || '']; })].join('|');
      var fits = function () { return inner.getBoundingClientRect().height <= cell.height + 0.1 && inner.scrollWidth <= cell.width + 1; };
      var low = 1, high = Math.max(1, Math.min(3.5, (cell.node.tagName === 'TH' ? 20 : 48) / cell.font));
      if (fixedLine) high = Math.max(1, Math.min(high, ...fonts.map(function (font) { return font.fixedLine / Math.max(1, font.inkHeight); })));
      if (sizes.has(key)) {
        low = sizes.get(key);
        report.cachedCells++;
      } else {
        setScale(fonts, 1);
        if (fits()) {
          for (var step = 0; step < 7; step++) {
            var mid = (low + high) / 2;
            setScale(fonts, mid);
            if (fits()) low = mid; else high = mid;
          }
        }
        /* Quarter-pixel sizes keep live updates visually stable. */
        low = Math.max(1, Math.floor(cell.font * low * 4) / (cell.font * 4));
        sizes.set(key, low);
      }
      setScale(fonts, low);
      if (fixedLine) personnelColumns.push({fonts:fonts, scale:low, index:cell.node.cellIndex});
      else if (low > 1.01) report.fittedCells++;
    });
    /* A long name may make one native row taller. Keep personnel figures
       consistent down each column instead of emphasizing that one row. */
    var common = {};
    personnelColumns.forEach(function (cell) { common[cell.index] = Math.min(common[cell.index] || Infinity, cell.scale); });
    personnelColumns.forEach(function (cell) {
      setScale(cell.fonts, common[cell.index]);
      if (common[cell.index] > 1.01) report.fittedCells++;
    });
    report.tables++;
  }
  function fitLabel(node) {
    if (!visible(node)) return;
    var rect = node.getBoundingClientRect(), style = getComputedStyle(node);
    var text = node.tagName === 'INPUT' ? (node.value || node.placeholder) : node.tagName === 'SELECT' ? node.options[node.selectedIndex].text : node.textContent.trim();
    if (!text) return;
    var base = number(style.fontSize), line = number(style.lineHeight) || base * 1.2;
    var width = rect.width - number(style.paddingLeft) - number(style.paddingRight) - number(style.borderLeftWidth) - number(style.borderRightWidth);
    var height = rect.height - number(style.paddingTop) - number(style.paddingBottom) - number(style.borderTopWidth) - number(style.borderBottomWidth);
    var headerSpace = 0, alignRight = false;
    if (node.matches('.panel .hdr>span') && rect.height < line * 1.6) {
      if (node.classList.contains('sub')) {
        headerSpace = Math.max(0, number(style.marginLeft)) / 2;
        alignRight = headerSpace > 0;
      } else if (node.nextElementSibling && node.nextElementSibling.classList.contains('sub')) {
        headerSpace = Math.max(0, node.nextElementSibling.getBoundingClientRect().left - rect.right - number(getComputedStyle(node.parentElement).gap)) / 2;
      }
      width += headerSpace;
    }
    if (node.tagName === 'SELECT') width -= 18;
    if (width < 1 || height < 1) return;
    measure.font = style.fontStyle + ' ' + style.fontWeight + ' ' + base + 'px ' + style.fontFamily;
    var ink = measure.measureText('Mg' + text), textWidth = measure.measureText(text).width;
    var inkHeight = ink.actualBoundingBoxAscent + ink.actualBoundingBoxDescent;
    var factor = Math.min(1.6, line / Math.max(1, inkHeight));
    if (style.whiteSpace.indexOf('nowrap') !== -1 || rect.height < line * 1.6 || /^(INPUT|SELECT|BUTTON|A)$/.test(node.tagName)) factor = Math.min(factor, width / Math.max(1, textWidth));
    var size = Math.max(base, Math.floor(base * factor * 4) / 4);
    remember(node, ['width','height','font-size','line-height','display','flex-shrink','white-space','text-align']);
    node.style.width = rect.width + 'px'; node.style.height = rect.height + 'px';
    if (style.display === 'inline') node.style.display = 'inline-block';
    node.style.flexShrink = '0'; node.style.lineHeight = line + 'px';
    if (headerSpace) { node.style.whiteSpace = 'nowrap'; if (alignRight) node.style.textAlign = 'right'; }
    node.style.fontSize = size + 'px';
    /* Preserve wrapped notes and names: a larger font must use the same lines. */
    while (size > base && ((!headerSpace && node.scrollWidth > node.clientWidth + 1) || node.scrollHeight > node.clientHeight + 1)) {
      size = Math.max(base, size - 0.25); node.style.fontSize = size + 'px';
    }
    if (size > base + 0.1) report.labels++;
  }
  function run() {
    frame = 0;
    if (!enabled || busy) return;
    var started = performance.now();
    busy = true; observer.disconnect(); restore();
    report = {revision:++revision, tables:0, fittedCells:0, cachedCells:0, labels:0};
    if (sizes.size > 12000) sizes.clear();
    try {
      document.querySelectorAll('.panel table,#detailPop table,.personnel-call-scroll table').forEach(fitTable);
      document.querySelectorAll('.activity-summary,.activity-scale span,.activity-legend,.panel .hdr>span:not(.dot),.toolbar-toggle,.mini-btn,.mini-select,.personnel-controls input,.personnel-controls select,.health-grid .k,.health-grid .v,.personnel-coverage-note,.footer,.roster-title,.roster-meta,.roster-shift,.roster-entry .time,.roster-entry .name,.roster-entry .meta,.roster-empty,.detail-title,.detail-meta,.personnel-calls-dialog h2,.personnel-calls-dialog p,.personnel-calls-dialog button,.personnel-calls-dialog input,.personnel-calls-dialog select,#personnelCallsCount').forEach(fitLabel);
    } catch (error) {
      restore(); report.error = String(error);
      console.warn('Keeping original leaderboard typography:', error);
    } finally {
      report.durationMs = Math.round(performance.now() - started);
      busy = false; watch();
      document.dispatchEvent(new CustomEvent('ppfd:typography', {detail:report}));
    }
  }
  function schedule() {
    if (!enabled || busy) return;
    cancelAnimationFrame(frame);
    frame = requestAnimationFrame(function () { frame = requestAnimationFrame(run); });
  }
  observer = new MutationObserver(schedule);
  window.ppfdTypography = {
    refresh:run,
    disable:function () { enabled = false; cancelAnimationFrame(frame); observer.disconnect(); restore(); },
    enable:function () { enabled = true; run(); },
    status:function () { return report; }
  };
  document.addEventListener('ppfd:data', schedule);
  document.addEventListener('input', schedule);
  document.addEventListener('change', schedule);
  window.addEventListener('resize', schedule);
  if (document.fonts && document.fonts.ready) document.fonts.ready.then(function () { sizes.clear(); schedule(); });
  watch(); schedule();
}());
