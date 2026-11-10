#!/usr/bin/env node
/*
 * check_architecture_viewer.cjs -- drive docs/architecture.html in headless Chrome and
 * assert the interaction contract a unit test cannot see: highlighted connectors and
 * arrowheads, pinning, temporary preview, keyboard, reset, tabs, zoom and Fit, and that
 * the page makes no external request.
 *
 * Needs puppeteer (mermaid-cli ships it) and Chrome or Chromium; both paths come from the
 * environment, never from this file:
 *   PUPPETEER=/path/to/node_modules/puppeteer CHROME=/usr/bin/google-chrome \
 *     node scripts/check_architecture_viewer.cjs docs/architecture.html [evidence-dir]
 * With an evidence directory the script saves screenshots and browser-check.txt there.
 * PROFILE_DIR, when set, is the parent of the throwaway browser profile (default: the
 * system temporary directory). Exit 0 when every check passes, 1 when one fails.
 */
'use strict';
const fs = require('fs');
const os = require('os');
const path = require('path');
const puppeteer = require(process.env.PUPPETEER || 'puppeteer');

const file = path.resolve(process.argv[2] || 'docs/architecture.html');
const evidence = process.argv[3] ? path.resolve(process.argv[3]) : null;
const results = [];
const ok = (name, cond, detail = '') => results.push(`${cond ? 'PASS' : 'FAIL'} ${name}${cond ? '' : ' :: ' + detail}`);
const frame = () => new Promise(resolve => setTimeout(resolve, 60));  // let layout and hit-testing catch up
const settle = () => new Promise(resolve => setTimeout(resolve, 350));  // CSS transitions are 120 ms

(async () => {
  const profile = fs.mkdtempSync(path.join(process.env.PROFILE_DIR || os.tmpdir(), 'viewer-check-'));
  const browser = await puppeteer.launch({executablePath: process.env.CHROME || undefined,
    headless: true, userDataDir: profile, args: ['--no-sandbox']});
  try {
    const page = await browser.newPage();
    await page.setViewport({width: 1600, height: 1000});
    const requests = [], errors = [];
    page.on('request', r => { if (!r.url().startsWith('file://') && !r.url().startsWith('data:')) requests.push(r.url()); });
    page.on('pageerror', e => errors.push(String(e)));
    page.on('console', m => { if (m.type() === 'error') errors.push(m.text()); });
    await page.goto('file://' + file, {waitUntil: 'load'});
    const shot = async name => { if (evidence) { fs.mkdirSync(evidence, {recursive: true}); await page.screenshot({path: path.join(evidence, name)}); } };

    // State of one panel: which connectors lit up, in which direction.
    const read = panel => page.evaluate(panel => {
      const p = document.querySelector(panel);
      const edges = [...p.querySelectorAll('.edge-connection')];
      const pick = c => edges.filter(e => e.classList.contains(c)).map(e => `${e.dataset.source}>${e.dataset.target}`).sort();
      return {title: p.querySelector('.selection-title').textContent, status: p.querySelector('.status').textContent,
        incoming: pick('is-incoming'), outgoing: pick('is-outgoing'), both: pick('is-both'),
        selected: p.querySelector('.canvas').classList.contains('has-selection'),
        listed: [...p.querySelectorAll('.connections button')].map(b => b.dataset.direction)};
    }, panel);
    const tabs = await page.$$eval('[role=tab]', t => t.map(x => ({id: x.id, text: x.textContent})));
    ok('two tabs with the required names', tabs.length === 2 && tabs[0].text === 'Implemented'
      && tabs[1].text === 'Proposals, order and dependencies', JSON.stringify(tabs));

    // The interaction colors are defined once, as CSS variables, with the specified defaults.
    const css = await page.evaluate(() => {
      const text = [...document.querySelectorAll('style')].map(s => s.textContent).join('\n');
      const root = getComputedStyle(document.documentElement);
      return {definitions: (text.match(/--incoming\s*:/g) || []).length + (text.match(/--outgoing\s*:/g) || []).length,
        incoming: root.getPropertyValue('--incoming').trim(), outgoing: root.getPropertyValue('--outgoing').trim()};
    });
    ok('--incoming #58a6ff and --outgoing #ffb454 are each defined once', css.definitions === 2
      && css.incoming === '#58a6ff' && css.outgoing === '#ffb454', JSON.stringify(css));

    // Every node on every tab: hover it and compare what lit up with its mapped edges.
    for (let index = 0; index < tabs.length; index++) {
      const panel = `#panel-${index}`;
      await page.click(`#tab-${index}`);
      const ids = await page.$$eval(`${panel} [data-node-id]`, n => n.map(x => x.dataset.nodeId));
      const total = await page.$$eval(`${panel} .edge-connection`, e => e.length);
      const labelled = await page.$$eval(`${panel} [data-node-id]`, n => n.every(x => x.getAttribute('aria-label') && x.getAttribute('role') === 'button' && x.tabIndex === 0));
      ok(`tab ${index + 1}: ${ids.length} nodes and ${total} connectors, every node focusable with an accessible label`, ids.length > 0 && total > 0 && labelled);
      const mismatches = [];
      for (const id of ids) {
        // Scrolling a node into view can leave the pointer where it already was, so no enter
        // event fires; move away and hover again until the preview shows the node we aimed at.
        let s;
        for (let attempt = 0; attempt < 3; attempt++) {
          await page.$eval(`${panel} [data-node-id="${id}"]`, n => n.scrollIntoView({block: 'center', inline: 'center'}));
          await page.mouse.move(2, 2);
          await frame();
          await page.hover(`${panel} [data-node-id="${id}"]`);
          await frame();
          s = await read(panel);
          const label = await page.$eval(`${panel} [data-node-id="${id}"]`, n => n.dataset.label);
          if (s.title === label) break;
        }
        const expected = await page.$$eval(`${panel} .edge-connection`, (e, id) => ({
          incoming: e.filter(x => x.dataset.target === id && x.dataset.source !== id).map(x => `${x.dataset.source}>${x.dataset.target}`).sort(),
          outgoing: e.filter(x => x.dataset.source === id && x.dataset.target !== id).map(x => `${x.dataset.source}>${x.dataset.target}`).sort(),
        }), id);
        if (JSON.stringify([s.incoming, s.outgoing]) !== JSON.stringify([expected.incoming, expected.outgoing])
            || s.listed.length !== expected.incoming.length + expected.outgoing.length) mismatches.push(id);
      }
      ok(`tab ${index + 1}: hovering each node lights exactly its own incoming and outgoing connectors, and lists them`, mismatches.length === 0, mismatches.join(','));
      await page.mouse.move(2, 2);
    }
    await page.click('#tab-0');
    const P = '#panel-0';
    const node = id => `${P} [data-node-id="${id}"]`;

    // Colors on a node that has both directions: stroke and its own arrowhead, per edge.
    const probe = await page.$$eval(`${P} .edge-connection`, edges => {
      const inc = new Map(), out = new Map();
      for (const e of edges) { inc.set(e.dataset.target, (inc.get(e.dataset.target) || 0) + 1); out.set(e.dataset.source, (out.get(e.dataset.source) || 0) + 1); }
      return [...inc.keys()].filter(k => out.has(k)).sort((a, b) => (inc.get(b) + out.get(b)) - (inc.get(a) + out.get(a)))[0];
    });
    const markerFills = () => page.evaluate(P => [...document.querySelector(P).querySelectorAll('marker')].filter(m => m.querySelector('path, circle, polygon'))
      .map(m => `${m.id}=${getComputedStyle(m.querySelector('path, circle, polygon')).fill}`).sort(), P);
    const fillsBefore = await markerFills();
    await page.hover(node(probe));
    await settle();
    const colors = await page.evaluate(P => {
      const p = document.querySelector(P);
      const css = getComputedStyle(document.documentElement);
      const rgb = v => { const d = document.createElement('i'); d.style.color = v; document.body.append(d); const c = getComputedStyle(d).color; d.remove(); return c; };
      const look = e => { if (!e) return null; const id = e.getAttribute('marker-end').match(/#([^)]*)\)/)[1];
        return {id, arrow: getComputedStyle(p.querySelector(`[id="${CSS.escape(id)}"] path`)).fill, stroke: getComputedStyle(e).stroke, opacity: Number(getComputedStyle(e).opacity), width: parseFloat(getComputedStyle(e).strokeWidth)}; };
      const others = [...p.querySelectorAll('.edge-connection:not(.is-incoming):not(.is-outgoing):not(.is-both)')].map(look);
      return {incomingColor: rgb(css.getPropertyValue('--incoming')), outgoingColor: rgb(css.getPropertyValue('--outgoing')),
        incoming: [...p.querySelectorAll('.edge-connection.is-incoming')].map(look),
        outgoing: [...p.querySelectorAll('.edge-connection.is-outgoing')].map(look), others};
    }, P);
    ok(`"${probe}": every incoming connector and its arrowhead is blue`, colors.incoming.length > 0
      && colors.incoming.every(c => c.id.endsWith('-incoming') && c.arrow === colors.incomingColor && c.stroke === colors.incomingColor && c.opacity === 1), JSON.stringify(colors.incoming));
    ok(`"${probe}": every outgoing connector and its arrowhead is amber`, colors.outgoing.length > 0
      && colors.outgoing.every(c => c.id.endsWith('-outgoing') && c.arrow === colors.outgoingColor && c.stroke === colors.outgoingColor && c.opacity === 1), JSON.stringify(colors.outgoing));
    ok('unrelated connectors fade and keep their own, uncolored arrowhead', colors.others.length > 0
      && colors.others.every(c => !/-(incoming|outgoing|both)$/.test(c.id) && c.opacity < 0.2 && c.stroke !== colors.incomingColor && c.stroke !== colors.outgoingColor), JSON.stringify(colors.others.slice(0, 2)));
    const fillsDuring = await markerFills();
    const shared = fillsBefore.filter(entry => !/-(incoming|outgoing|both)=/.test(entry));
    ok('shared markers are never recolored: the original markers keep their fill while highlighted',
      shared.length > 0 && shared.every(entry => fillsDuring.includes(entry)), JSON.stringify(shared.filter(e => !fillsDuring.includes(e))));
    await shot('hover.png');
    await page.mouse.move(2, 2);
    await settle();
    const restored = await page.$$eval(`${P} .edge-connection`, e => e.every(x => !/-(incoming|outgoing|both)\)/.test(x.getAttribute('marker-end'))));
    ok('leaving the node restores every original arrowhead', restored);

    // The details panel keeps its height, so the page does not move under the pointer.
    const heights = [];
    for (const target of ['PGDLQ', 'DOWN']) {
      await page.hover(node(target)); await frame();
      heights.push(await page.$eval(`${P} .details`, d => d.getBoundingClientRect().height));
    }
    await page.mouse.move(2, 2); await frame();
    heights.push(await page.$eval(`${P} .details`, d => d.getBoundingClientRect().height));
    ok('the details panel keeps one height with and without a selection', new Set(heights).size === 1, JSON.stringify(heights));

    // Pin, preview another node, leave, release, reset paths.
    const ids = await page.$$eval(`${P} [data-node-id]`, n => n.map(x => x.dataset.nodeId));
    const [a, b] = [ids[0], ids[1]];
    await page.click(node(a));
    let s = await read(P); ok('a click pins a node', s.status.includes('Pinned') && s.title.length > 0, s.status);
    await page.hover(node(b));
    s = await read(P); ok('hovering another node previews it while one is pinned', s.status.includes('Preview'), s.status);
    await page.mouse.move(2, 2);
    s = await read(P); ok('leaving restores the pinned node', s.status.includes('Pinned'), s.status);
    await shot('pinned.png');
    await page.click(node(a)); await page.mouse.move(2, 2);
    s = await read(P); ok('a second click releases the pin', !s.selected);
    for (const [name, reset] of [
      ['Escape', () => page.keyboard.press('Escape')],
      ['Clear selection', () => page.click(`${P} [data-action=clear]`)],
      ['a background click', async () => { const r = await page.$eval(`${P} .canvas`, c => { const b = c.getBoundingClientRect(); return [b.x + 3, b.y + 3]; }); await page.mouse.click(...r); }],
    ]) {
      await page.click(node(a)); await page.mouse.move(2, 2); await reset();
      s = await read(P); ok(`${name} clears a pinned selection`, !s.selected);
    }

    // Keyboard: Tab reaches a node (preview), Enter and Space pin, Escape clears; focus ring is visible.
    await page.focus(`${P} [data-action=clear]`); await page.keyboard.press('Tab');
    s = await read(P); ok('keyboard focus on a node previews it', s.selected && s.status.includes('Preview'), s.status);
    const ring = await page.evaluate(() => { const n = document.activeElement; const r = n.querySelector('.label-container'); const st = getComputedStyle(r.tagName === 'g' ? r.querySelector('path') : r);
      return {focusVisible: n.matches(':focus-visible'), width: parseFloat(st.strokeWidth), color: st.stroke}; });
    ok('the focused node shows a visible focus ring', ring.focusVisible && ring.width >= 3, JSON.stringify(ring));
    await page.keyboard.press('Enter'); s = await read(P); ok('Enter pins the focused node', s.status.includes('Pinned'), s.status);
    await page.keyboard.press('Enter'); s = await read(P); ok('Enter on a pinned node unpins it', !s.status.includes('Pinned'), s.status);
    await page.keyboard.press(' '); s = await read(P); ok('Space pins the focused node', s.status.includes('Pinned'), s.status);
    const pinnedTitle = s.title;
    await page.keyboard.press('Tab'); s = await read(P);
    ok('Tab to the next node previews it while the first stays pinned underneath', s.status.includes('Preview') && s.title !== pinnedTitle, s.status);
    await page.keyboard.press('Escape'); await page.evaluate(() => document.activeElement.blur());
    s = await read(P); ok('Escape clears a keyboard pin', !s.selected);

    // Navigating the connection list moves the pin to the neighbour.
    await page.click(node(probe)); await page.mouse.move(2, 2);
    const before = (await read(P)).title;
    await page.click(`${P} .connections button`);
    s = await read(P); ok('a connection button moves the pin to that neighbour', s.status.includes('Pinned') && s.title !== before, `${before} -> ${s.title}`);
    await page.click(`${P} [data-action=clear]`);

    // Zoom and Fit.
    const width = () => page.$eval(`${P} svg`, e => e.getBoundingClientRect().width);
    const w0 = await width(); const cw = await page.$eval(`${P} .canvas`, c => c.clientWidth);
    ok('opens at fit width', w0 > cw * 0.9 && w0 <= cw, `${w0} vs ${cw}`);
    await page.click(`${P} [data-zoom="0.25"]`); const w1 = await width();
    ok('Zoom in enlarges the diagram', w1 > w0 * 1.2, `${w0}->${w1}`);
    await page.click(`${P} [data-zoom="-0.25"]`); await page.click(`${P} [data-zoom="-0.25"]`); const w2 = await width();
    ok('Zoom out shrinks it', w2 < w0 * 0.8, `${w0}->${w2}`);
    await page.click(`${P} [data-zoom="fit"]`);
    const fits = await page.$eval(`${P} .canvas`, c => { const s = c.querySelector('svg').getBoundingClientRect(), r = c.getBoundingClientRect(); return s.width <= r.width && s.height <= r.height; });
    ok('Fit shows the whole diagram inside the canvas', fits);
    await shot('fit.png');
    await page.click(`${P} [data-zoom="actual"]`);
    const actual = await page.$eval(`${P} svg`, e => [e.getBoundingClientRect().width, e.viewBox.baseVal.width]);
    ok('100% shows the diagram at its drawn size', Math.abs(actual[0] - actual[1]) < 2, JSON.stringify(actual));
    await page.click(`${P} [data-zoom="width"]`); ok('Fit width restores the opening width', Math.abs(await width() - w0) < 2);
    await page.click(`${P} [data-zoom="0.25"]`);
    await page.hover(node(probe)); await settle();
    s = await read(P); ok('hover highlighting still works after zooming', s.incoming.length + s.outgoing.length > 0);
    await shot('zoomed.png');
    await page.mouse.move(2, 2);
    await page.click(`${P} [data-zoom="width"]`);

    // Second tab: click, arrow keys, Home/End, wrap, zoom and hover inside it.
    await page.click('#tab-1');
    const shown = await page.evaluate(() => [document.querySelector('#panel-0').hidden, document.querySelector('#panel-1').hidden,
      document.querySelector('#tab-1').getAttribute('aria-selected'), document.querySelector('#tab-0').getAttribute('aria-selected'),
      document.querySelector('#tab-0').tabIndex, document.querySelector('#tab-1').tabIndex]);
    ok('clicking a tab shows its panel and moves selection and tabindex', JSON.stringify(shown) === JSON.stringify([true, false, 'true', 'false', -1, 0]), JSON.stringify(shown));
    const w = await page.$eval('#panel-1 svg', e => e.getBoundingClientRect().width);
    ok('the second tab opens with a laid-out diagram', w > 200, String(w));
    await page.click('#panel-1 [data-zoom="0.25"]');
    ok('Zoom in works on the second tab', await page.$eval('#panel-1 svg', e => e.getBoundingClientRect().width) > w * 1.2);
    await page.click('#panel-1 [data-zoom="fit"]');
    const fit2 = await page.$eval('#panel-1 .canvas', c => { const s = c.querySelector('svg').getBoundingClientRect(), r = c.getBoundingClientRect(); return s.width <= r.width + 1 && s.height <= r.height + 1; });
    ok('Fit works on the second tab', fit2);
    const second = await page.$eval('#panel-1 [data-node-id]', n => n.dataset.nodeId);
    await page.hover(`#panel-1 [data-node-id="${second}"]`);
    s = await read('#panel-1'); ok('hover highlighting works on the second tab', s.incoming.length + s.outgoing.length > 0);
    await page.mouse.move(2, 2);
    await shot('tab2.png');
    await page.focus('#tab-0'); await page.keyboard.press('End');
    let state = await page.evaluate(() => [document.activeElement.id, document.querySelector('#panel-1').hidden]);
    ok('End selects the last tab', state[0] === 'tab-1' && !state[1], JSON.stringify(state));
    await page.keyboard.press('ArrowRight');
    state = await page.evaluate(() => [document.activeElement.id, document.querySelector('#panel-0').hidden]);
    ok('ArrowRight from the last tab wraps to the first', state[0] === 'tab-0' && !state[1], JSON.stringify(state));
    await page.keyboard.press('ArrowLeft');
    state = await page.evaluate(() => document.activeElement.id);
    ok('ArrowLeft from the first tab goes to the last', state === 'tab-1', state);
    await page.keyboard.press('Home');
    state = await page.evaluate(() => document.activeElement.id);
    ok('Home selects the first tab', state === 'tab-0', state);
    await page.keyboard.press('Tab');
    state = await page.evaluate(() => document.activeElement.closest('[role=tabpanel]')?.id ?? document.activeElement.tagName);
    ok('Tab from the selected tab enters its panel, not the hidden one', state === 'panel-0', state);

    // Selection is independent per tab, and Escape resets all.
    await page.click(node(a));
    await page.click('#tab-1'); await page.click(`#panel-1 [data-node-id="${second}"]`); await page.mouse.move(2, 2);
    await page.keyboard.press('Escape');
    const both = [(await read('#panel-0')).selected, (await read('#panel-1')).selected];
    ok('Escape clears the selection on every tab', both.every(x => x === false), JSON.stringify(both));

    await page.setViewport({width: 390, height: 800});
    ok('no horizontal page scroll at 390 px', await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth));
    await shot('phone.png');
    await page.setViewport({width: 1600, height: 1000});

    // The page is self-contained: no request left the file, and nothing references a remote resource.
    const remote = await page.evaluate(() => [...document.querySelectorAll('[src],[href]')]
      .map(e => e.getAttribute('src') || e.getAttribute('href')).filter(v => /^(https?:)?\/\//i.test(v)));
    ok('zero external requests while loading and using the page', requests.length === 0, JSON.stringify(requests));
    ok('no src or href points at a remote resource', remote.length === 0, JSON.stringify(remote));
    ok('no page error or console error', errors.length === 0, JSON.stringify(errors));
  } finally {
    await browser.close();
    fs.rmSync(profile, {recursive: true, force: true});
  }
  const summary = `${results.filter(r => r.startsWith('PASS')).length} passed, ${results.filter(r => r.startsWith('FAIL')).length} failed`;
  const text = results.join('\n') + '\n' + summary;
  console.log(text);
  if (evidence) { fs.mkdirSync(evidence, {recursive: true}); fs.writeFileSync(path.join(evidence, 'browser-check.txt'), text + '\n'); }
  process.exit(results.some(r => r.startsWith('FAIL')) ? 1 : 0);
})().catch(error => { console.error(error); process.exit(2); });
