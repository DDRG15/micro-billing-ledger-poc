/* Inlined into docs/architecture.html by build_architecture_viewer.py.
   No network requests, no editor callbacks, no external runtime. */
const panels = [...document.querySelectorAll('[role="tabpanel"]')];
const tabs = [...document.querySelectorAll('[role="tab"]')];
const states = new Map();
const rootStyle = getComputedStyle(document.documentElement);
const markerColors = Object.fromEntries(['incoming', 'outgoing', 'both']
  .map(name => [name, rootStyle.getPropertyValue(`--${name}`).trim()]));

for (const panel of panels) {
  const canvas = panel.querySelector('.canvas');
  const svg = canvas.querySelector('svg');
  const nodes = [...svg.querySelectorAll('[data-node-id]')];
  const edges = [...svg.querySelectorAll('[data-source][data-target]')];
  const edgeLabels = [...svg.querySelectorAll('.edgeLabel[data-edge-ref]')];
  const viewBox = svg.viewBox.baseVal;
  const data = {pinned: null, hovered: null, focused: null};
  states.set(panel, data);
  svg.removeAttribute('width');
  svg.removeAttribute('height');

  // Each highlighted edge points at its own colored copy of its marker. A shared
  // marker is never recolored, so unrelated arrowheads keep their color.
  for (const edge of edges) {
    edge.originalMarkers = {};
    for (const attribute of ['marker-end', 'marker-start']) {
      const original = edge.getAttribute(attribute);
      edge.originalMarkers[attribute] = original;
      const id = original?.match(/#([^)]*)\)/)?.[1];
      const marker = id && svg.querySelector(`[id="${CSS.escape(id)}"]`);
      if (!marker) continue;
      for (const [direction, color] of Object.entries(markerColors)) {
        const cloneId = `${id}-${direction}`;
        if (svg.querySelector(`[id="${CSS.escape(cloneId)}"]`)) continue;
        const clone = marker.cloneNode(true);
        clone.id = cloneId;
        for (const shape of clone.querySelectorAll('path, polygon, circle')) {
          shape.style.setProperty('fill', color, 'important');
          shape.style.setProperty('stroke', color, 'important');
        }
        marker.parentNode.append(clone);
      }
    }
  }

  const nodeOf = id => nodes.find(node => node.dataset.nodeId === id);
  const titleOf = id => nodeOf(id)?.dataset.title ?? id;

  function paint() {
    const selected = data.hovered ?? data.focused ?? data.pinned;
    canvas.classList.toggle('has-selection', selected !== null);
    for (const node of nodes) {
      node.classList.toggle('is-selected', node.dataset.nodeId === selected);
      node.setAttribute('aria-pressed', String(node.dataset.nodeId === data.pinned));
    }
    const connections = [];
    for (const edge of edges) {
      const incoming = selected !== null && edge.dataset.target === selected;
      const outgoing = selected !== null && edge.dataset.source === selected;
      const direction = incoming && outgoing ? 'both' : incoming ? 'incoming' : outgoing ? 'outgoing' : null;
      for (const name of Object.keys(markerColors)) edge.classList.toggle(`is-${name}`, direction === name);
      for (const [attribute, original] of Object.entries(edge.originalMarkers)) {
        if (original === null) continue;
        edge.setAttribute(attribute, direction ? original.replace(/#([^)]*)\)/, `#$1-${direction})`) : original);
      }
      for (const label of edgeLabels.filter(l => l.dataset.edgeRef === edge.dataset.edgeRef)) {
        for (const name of Object.keys(markerColors)) label.classList.toggle(`is-${name}`, direction === name);
      }
      if (direction) connections.push({edge, direction});
    }
    panel.querySelector('.selection-title').textContent = selected === null
      ? 'No node selected' : nodeOf(selected)?.dataset.label ?? selected;
    const count = kind => connections.filter(c => c.direction === kind || c.direction === 'both').length;
    panel.querySelector('.status').textContent = selected === null
      ? 'Hover or focus a node to trace its immediate connections. Click or press Enter or Space to pin it.'
      : `${count('incoming')} incoming · ${count('outgoing')} outgoing · ${data.pinned === selected
        ? 'Pinned — click it again, press Escape or Clear selection to release'
        : 'Preview — click or press Enter to pin'}`;
    // Rebuild the list only when its content changes: a blur or pointerleave repaint
    // between mousedown and mouseup would otherwise swap the button and drop the click.
    const list = panel.querySelector('.connections');
    const key = `${selected}|${connections.map(c => `${c.edge.dataset.edgeRef}:${c.direction}`).join(',')}`;
    if (list.dataset.key === key) return;
    list.dataset.key = key;
    list.replaceChildren();
    for (const {edge, direction} of connections) {
      const other = direction === 'incoming' ? edge.dataset.source : edge.dataset.target;
      const button = document.createElement('button');
      button.type = 'button';
      button.dataset.direction = direction;
      button.textContent = `${direction === 'incoming' ? 'From' : direction === 'outgoing' ? 'To' : 'Self'}: ${titleOf(other)}`;
      if (edge.dataset.edgeLabel) {
        const note = document.createElement('small');
        note.textContent = ` (${edge.dataset.edgeLabel})`;
        button.append(note);
      }
      button.addEventListener('click', () => {
        data.hovered = null;
        data.pinned = other;
        nodeOf(other)?.focus({preventScroll: false});
        paint();
      });
      list.append(button);
    }
  }

  function clear() { data.pinned = null; data.hovered = null; data.focused = null; paint(); }
  data.clear = clear;

  for (const node of nodes) {
    const id = node.dataset.nodeId;
    node.setAttribute('tabindex', '0');
    node.setAttribute('role', 'button');
    node.setAttribute('aria-label', `${node.dataset.label}. Show its connections; Enter or Space pins it.`);
    node.addEventListener('pointerenter', () => { data.hovered = id; paint(); });
    node.addEventListener('pointerleave', () => { data.hovered = null; paint(); });
    // Only keyboard focus previews: a mouse click also focuses the node, and that must
    // not keep a released selection painted.
    node.addEventListener('focus', () => { data.focused = node.matches(':focus-visible') ? id : null; paint(); });
    node.addEventListener('blur', () => { data.focused = null; paint(); });
    const toggle = () => { data.pinned = data.pinned === id ? null : id; paint(); };
    node.addEventListener('click', event => { event.stopPropagation(); toggle(); });
    node.addEventListener('keydown', event => {
      if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); toggle(); }
    });
  }
  canvas.addEventListener('click', event => { if (!event.target.closest('[data-node-id]')) clear(); });
  panel.querySelector('[data-action=clear]').addEventListener('click', clear);

  // Zoom is a multiple of a base width. "width" (the default) fills the canvas width and
  // scrolls; "fit" shows the whole diagram; "actual" is the size Mermaid drew it at, where
  // labels are legible on a large graph.
  function baseWidth(mode) {
    const style = getComputedStyle(canvas);
    const width = canvas.clientWidth - parseFloat(style.paddingLeft) - parseFloat(style.paddingRight);
    const height = canvas.clientHeight - parseFloat(style.paddingTop) - parseFloat(style.paddingBottom);
    if (mode === 'actual') return viewBox.width;
    return Math.max(200, mode === 'fit' ? Math.min(width, height * viewBox.width / viewBox.height) : width);
  }
  data.mode = 'width';
  data.zoom = 1;
  function applyZoom() { svg.style.width = `${Math.round(baseWidth(data.mode) * data.zoom)}px`; }
  for (const button of panel.querySelectorAll('[data-zoom]')) {
    button.addEventListener('click', () => {
      const value = button.dataset.zoom;
      if (value === 'fit' || value === 'width' || value === 'actual') { data.mode = value; data.zoom = 1; }
      else data.zoom = Math.max(.4, Math.min(6, data.zoom + Number(value)));
      applyZoom();
    });
  }
  data.applyZoom = applyZoom;
  paint();
}

function selectTab(tab, focus) {
  for (const other of tabs) {
    const selected = other === tab;
    other.setAttribute('aria-selected', String(selected));
    other.tabIndex = selected ? 0 : -1;
  }
  for (const panel of panels) panel.hidden = panel.id !== tab.getAttribute('aria-controls');
  if (focus) tab.focus();
  const panel = document.getElementById(tab.getAttribute('aria-controls'));
  states.get(panel)?.applyZoom();
}
tabs.forEach((tab, index) => {
  tab.addEventListener('click', () => selectTab(tab, false));
  tab.addEventListener('keydown', event => {
    const step = {ArrowRight: 1, ArrowLeft: -1}[event.key];
    if (step) { event.preventDefault(); selectTab(tabs[(index + step + tabs.length) % tabs.length], true); }
    if (event.key === 'Home') { event.preventDefault(); selectTab(tabs[0], true); }
    if (event.key === 'End') { event.preventDefault(); selectTab(tabs[tabs.length - 1], true); }
  });
});
document.addEventListener('keydown', event => {
  if (event.key === 'Escape') for (const data of states.values()) data.clear();
});
window.addEventListener('resize', () => {
  for (const [panel, data] of states) if (!panel.hidden) data.applyZoom();
});
for (const [panel, data] of states) if (!panel.hidden) data.applyZoom();
