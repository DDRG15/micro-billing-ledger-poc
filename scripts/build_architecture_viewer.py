"""
build_architecture_viewer.py -- render docs/architecture.md's Mermaid blocks into one
self-contained, interactive HTML file (docs/architecture.html).

Why generated
-------------
The Mermaid blocks are the maintained source: they diff in review and GitHub renders
them in place. A rendered SVG alone cannot answer "what does this node talk to?" on a
graph of fifty nodes, so the viewer adds that: hover or focus a node and its incoming
connectors turn blue, its outgoing connectors amber, everything else fades.

How edges are mapped
--------------------
Nodes and edges are parsed from the Mermaid source, not read back from the rendered
text. The source is restricted on purpose (one node or one edge per line, ids made of
letters and digits, no repeated source-target pair); anything outside that subset stops
the build. mermaid-cli names its SVG edges "L_<source>_<target>_<n>", but the numeric
suffix is not stable between edges of one pair, so the suffix is ignored: each SVG edge
is matched to a source edge by the source and target ids named in its SVG id, using the
ids parsed from the source. The rendered set must then equal the parsed set, node for
node and edge for edge, or the build fails.

How --check works
-----------------
Rendering needs mermaid-cli and Chrome, which CI does not carry. The page embeds a
SHA-256 of everything its content depends on (the Mermaid blocks, the viewer CSS and
JavaScript, and this script). --check recomputes it and also compares the node ids and
edge pairs found in the HTML with those parsed from the Markdown. It never renders.

Usage
-----
    python scripts/build_architecture_viewer.py [--mmdc PATH] [--browser PATH]
    python scripts/build_architecture_viewer.py --check

mermaid-cli comes from --mmdc or $MMDC. Chrome or Chromium comes from --browser, $CHROME,
or PATH. The same input produces a byte-identical output file.
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
HERE = Path(__file__).resolve().parent
DEFAULT_SOURCE = ROOT / "docs" / "architecture.md"
DEFAULT_OUTPUT = ROOT / "docs" / "architecture.html"
DIGEST_META = "diagram-source-sha256"

SVG_NS = "http://www.w3.org/2000/svg"
ET.register_namespace("", SVG_NS)
ET.register_namespace("xhtml", "http://www.w3.org/1999/xhtml")

BLOCK = re.compile(r"```mermaid\n(.*?)```", re.S)

# Fixed render settings. They are part of the digest, so changing one demands a rebuild.
RENDER_CONFIG = {
    "theme": "dark",
    "look": "classic",
    "securityLevel": "strict",
    "flowchart": {"htmlLabels": True, "curve": "basis", "nodeSpacing": 40, "rankSpacing": 70,
                  "wrappingWidth": 300},
}

IDENT = r"[A-Za-z][A-Za-z0-9]*"
RE_HEADER = re.compile(r"^flowchart\s+(LR|RL|TB|TD|BT)$")
RE_SUBGRAPH = re.compile(rf'^subgraph\s+({IDENT})\["(.*)"\]$')
RE_NODE = re.compile(rf'^({IDENT})(\[\(|\[)"(.*)"(\)\]|\])$')
RE_EDGE = re.compile(rf'^({IDENT})\s+(-->|-\.->|==>)\s*(?:\|"(.*?)"\|)?\s*({IDENT})$')
RE_CLASSDEF = re.compile(r"^classDef\s+(\w+)\s+(.+)$")
RE_CLASS = re.compile(r"^class\s+(\S+)\s+(\w+)$")
RE_LEGEND = re.compile(r"^%%\s*legend\s+(\w+):\s*(.+)$")


@dataclass(frozen=True)
class Edge:
    source: str
    target: str
    label: str
    arrow: str


@dataclass
class Diagram:
    title: str
    text: str
    nodes: dict[str, str]  # id -> label with newlines for <br/>
    edges: list[Edge]
    legend: list[tuple[str, str, str]]  # (description, fill, stroke) from "%% legend" lines


def parse_mermaid(title: str, text: str) -> Diagram:
    """Parse the restricted flowchart subset. Anything else is an error, never a guess."""
    nodes: dict[str, str] = {}
    groups: set[str] = set()
    edges: list[Edge] = []
    legend_lines: list[tuple[str, str]] = []
    classdefs: dict[str, tuple[str, str]] = {}
    assignments: list[tuple[str, str, str]] = []
    depth = 0
    seen_header = False
    for number, raw in enumerate(text.splitlines(), 1):
        line = raw.strip()
        if (match := RE_LEGEND.match(line)):
            legend_lines.append((match.group(1), match.group(2).strip()))
            continue
        if not line or line.startswith("%%"):
            continue
        where = f"{title!r} line {number}: {line!r}"
        if not seen_header:
            if not RE_HEADER.match(line):
                raise SystemExit(f"{where}: the block must start with 'flowchart LR|TB|...'")
            seen_header = True
            continue
        if (match := RE_SUBGRAPH.match(line)):
            if match.group(1) in groups or match.group(1) in nodes:
                raise SystemExit(f"{where}: duplicate id")
            groups.add(match.group(1))
            depth += 1
        elif line == "end":
            depth -= 1
            if depth < 0:
                raise SystemExit(f"{where}: 'end' without 'subgraph'")
        elif (match := RE_NODE.match(line)):
            if match.group(1) in nodes or match.group(1) in groups:
                raise SystemExit(f"{where}: duplicate id")
            nodes[match.group(1)] = re.sub(r"<br\s*/?>", "\n", match.group(3))
        elif (match := RE_EDGE.match(line)):
            edges.append(Edge(match.group(1), match.group(4), match.group(3) or "", match.group(2)))
        elif (match := RE_CLASSDEF.match(line)):
            props = dict(part.split(":", 1) for part in match.group(2).split(",") if ":" in part)
            classdefs[match.group(1)] = (props.get("fill", "transparent"), props.get("stroke", "currentColor"))
        elif (match := RE_CLASS.match(line)):
            assignments.append((where, match.group(1), match.group(2)))
        else:
            raise SystemExit(f"{where}: outside the supported subset (one node or one edge per "
                             "line; quoted labels; ids of letters and digits)")
    if depth != 0:
        raise SystemExit(f"{title!r}: unbalanced subgraph/end")
    if not nodes or not edges:
        raise SystemExit(f"{title!r}: a diagram without nodes or edges cannot be interactive")
    pairs: set[tuple[str, str]] = set()
    for edge in edges:
        for end in (edge.source, edge.target):
            if end not in nodes:
                raise SystemExit(f"{title!r}: edge {edge.source} -> {edge.target} names "
                                 f"{end!r}, which is not a declared node")
        if (edge.source, edge.target) in pairs:
            raise SystemExit(f"{title!r}: more than one edge {edge.source} -> {edge.target}; "
                             "merge them into one labelled edge")
        pairs.add((edge.source, edge.target))
    for where, ids, name in assignments:
        if name not in classdefs:
            raise SystemExit(f"{where}: class {name!r} has no classDef")
        for node_id in ids.split(","):
            if node_id not in nodes:
                raise SystemExit(f"{where}: {node_id!r} is not a declared node")
    legend = []
    for name, description in legend_lines:
        if name not in classdefs:
            raise SystemExit(f"{title!r}: legend entry {name!r} has no classDef")
        legend.append((description, *classdefs[name]))
    return Diagram(title, text, nodes, edges, legend)


def read_diagrams(source: Path) -> list[Diagram]:
    """One Diagram per Mermaid block; the tab title is the closest '## ' heading above it."""
    text = source.read_text(encoding="utf-8")
    found = []
    for match in BLOCK.finditer(text):
        headings = re.findall(r"^## (.+)$", text[:match.start()], re.M)
        title = re.sub(r"^\d+\.\s*", "", headings[-1]).strip() if headings else source.stem
        found.append(parse_mermaid(title, match.group(1)))
    if not found:
        raise SystemExit(f"no Mermaid block found in {source}")
    return found


def digest(diagrams: list[Diagram]) -> str:
    """Hash of everything the page content depends on."""
    parts = {
        "diagrams": [[d.title, d.text] for d in diagrams],
        "css": (HERE / "architecture_viewer.css").read_text(encoding="utf-8"),
        "js": (HERE / "architecture_viewer.js").read_text(encoding="utf-8"),
        "script": Path(__file__).read_text(encoding="utf-8"),
        "render": RENDER_CONFIG,
    }
    payload = json.dumps(parts, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def enrich_svg(diagram: Diagram, index: int, markup: str) -> str:
    """Tag nodes with data-node-id/title/label and edges with data-source/target/label."""
    root = ET.fromstring(markup)
    prefix = f"diagram-{index}"

    # Nodes: the SVG group id is "<prefix>-flowchart-<id>-<n>"; <id> is checked against
    # the ids parsed from the source, longest first, so no id is read from rendered text.
    node_groups: dict[str, ET.Element] = {}
    for group in root.iter(f"{{{SVG_NS}}}g"):
        if "node" not in group.get("class", "").split():
            continue
        group_id = group.get("id", "")
        owners = [n for n in diagram.nodes if re.fullmatch(rf"{re.escape(prefix)}-flowchart-{re.escape(n)}-\d+", group_id)]
        if len(owners) != 1:
            raise SystemExit(f"{diagram.title!r}: SVG node {group_id!r} matches {len(owners)} source nodes")
        node_id = owners[0]
        lines = diagram.nodes[node_id].split("\n")
        group.set("data-node-id", node_id)
        group.set("data-title", lines[0])
        group.set("data-label", "; ".join(lines))
        node_groups[node_id] = group
    if set(node_groups) != set(diagram.nodes):
        missing = sorted(set(diagram.nodes) - set(node_groups))
        raise SystemExit(f"{diagram.title!r}: nodes in the source but not in the SVG: {missing}")

    # Edges: match each SVG edge to a source edge by the ids in "L_<source>_<target>_<n>".
    by_pair = {(e.source, e.target): (i, e) for i, e in enumerate(diagram.edges)}
    mapped: set[int] = set()
    labels = {g.get("data-id"): parent for parent in root.iter(f"{{{SVG_NS}}}g")
              if "edgeLabel" in parent.get("class", "").split()
              for g in parent if g.get("data-id")}
    for path in root.iter(f"{{{SVG_NS}}}path"):
        if "flowchart-link" not in path.get("class", "").split():
            continue
        edge_id = path.get("id", "")
        hits = [(pair, value) for pair, value in by_pair.items()
                if re.fullmatch(rf"{re.escape(prefix)}-L_{re.escape(pair[0])}_{re.escape(pair[1])}_\d+", edge_id)]
        if len(hits) != 1:
            raise SystemExit(f"{diagram.title!r}: SVG edge {edge_id!r} matches {len(hits)} source edges")
        (source, target), (position, edge) = hits[0]
        if position in mapped:
            raise SystemExit(f"{diagram.title!r}: source edge {source} -> {target} rendered twice")
        mapped.add(position)
        path.set("data-source", source)
        path.set("data-target", target)
        path.set("data-edge-label", edge.label)
        path.set("data-edge-ref", str(position))
        path.set("class", path.get("class", "") + " edge-connection")
        holder = labels.get(path.get("data-id"))
        if holder is not None:
            holder.set("data-edge-ref", str(position))
    if mapped != set(range(len(diagram.edges))):
        missing = [f"{diagram.edges[i].source} -> {diagram.edges[i].target}"
                   for i in sorted(set(range(len(diagram.edges))) - mapped)]
        raise SystemExit(f"{diagram.title!r}: edges in the source but not in the SVG: {missing}")

    out = ET.tostring(root, encoding="unicode")
    # Inside foreignObject the HTML parser is in HTML mode: keep plain div/span tags
    # rather than prefixed elements it would not recognise.
    out = re.sub(r"<(/?)xhtml:", r"<\1", out)
    return out


def render(diagrams: list[Diagram], mmdc: str, browser: str, work_dir: Path | None) -> list[str]:
    if work_dir is not None:
        work_dir.mkdir(parents=True, exist_ok=True)
    rendered = []
    with tempfile.TemporaryDirectory(prefix="architecture-render-", dir=work_dir) as tmp:
        tmp_dir = Path(tmp)
        puppeteer = tmp_dir / "puppeteer.json"
        puppeteer.write_text(json.dumps({
            "executablePath": browser,
            "userDataDir": str(tmp_dir / "profile"),
            "args": ["--no-sandbox"],
        }))
        config = tmp_dir / "mermaid.json"
        config.write_text(json.dumps(RENDER_CONFIG))
        for index, diagram in enumerate(diagrams):
            src, dst = tmp_dir / f"{index}.mmd", tmp_dir / f"{index}.svg"
            src.write_text(diagram.text, encoding="utf-8")
            subprocess.run([mmdc, "-q", "-i", str(src), "-o", str(dst), "-p", str(puppeteer),
                            "-c", str(config), "-b", "transparent", "-I", f"diagram-{index}"],
                           check=True)
            rendered.append(enrich_svg(diagram, index, dst.read_text(encoding="utf-8")))
            print(f"{diagram.title}: {len(diagram.nodes)} nodes, {len(diagram.edges)} edges")
    return rendered


def node_legend(diagram: Diagram) -> str:
    """Node-colour legend, from the '%% legend <class>: <text>' lines of the Mermaid source."""
    if not diagram.legend:
        return ""
    chips = "".join(
        f'<span><i class="chip" style="background:{html.escape(fill)};border-color:{html.escape(stroke)}"></i>'
        f'{html.escape(text)}</span>' for text, fill, stroke in diagram.legend)
    return f'<div class="legend node-legend" aria-label="Node colours">{chips}</div>'


def page(diagrams: list[Diagram], svgs: list[str], source: Path, title: str, source_digest: str) -> str:
    css = (HERE / "architecture_viewer.css").read_text(encoding="utf-8")
    js = (HERE / "architecture_viewer.js").read_text(encoding="utf-8")
    tabs, panels = [], []
    for index, (diagram, svg) in enumerate(zip(diagrams, svgs)):
        selected = index == 0
        name = html.escape(diagram.title)
        tabs.append(
            f'<button type="button" id="tab-{index}" role="tab" aria-selected="{str(selected).lower()}" '
            f'aria-controls="panel-{index}" tabindex="{0 if selected else -1}">{name}</button>')
        panels.append(
            f'<section id="panel-{index}" role="tabpanel" aria-labelledby="tab-{index}"{"" if selected else " hidden"}>'
            '<div class="toolbar"><div class="controls">'
            '<button type="button" data-zoom="0.25" aria-label="Zoom in">Zoom in</button>'
            '<button type="button" data-zoom="-0.25" aria-label="Zoom out">Zoom out</button>'
            '<button type="button" data-zoom="fit" aria-label="Fit the whole diagram">Fit</button>'
            '<button type="button" data-zoom="width" aria-label="Fit to width">Fit width</button>'
            '<button type="button" data-zoom="actual" aria-label="Actual size">100%</button>'
            '<button type="button" data-action="clear">Clear selection</button>'
            f'</div><span class="panel-title">{name}: {len(diagram.nodes)} nodes, {len(diagram.edges)} connections</span></div>'
            f'{node_legend(diagram)}<div class="canvas">{svg}</div>'
            '<div class="details"><strong class="selection-title"></strong>'
            '<p class="status" role="status" aria-live="polite"></p><div class="connections"></div></div>'
            '</section>')
    return f'''<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="{DIGEST_META}" content="{source_digest}">
<title>{html.escape(title)}</title><style>{css}</style></head>
<body><header><h1>{html.escape(title)}</h1>
<p class="hint">Hover or focus a node to trace its connections. Click or press Enter or Space to pin it; hover another node to preview it. Escape, Clear selection or a click on the background resets. Labels are small in the overview: use 100% for the drawn size, or read the selected node in the panel under the diagram.</p>
<div class="toolbar"><nav class="controls" role="tablist" aria-label="Choose a diagram">{"".join(tabs)}</nav>
<div class="legend" aria-label="Legend"><span><i class="swatch incoming"></i>Incoming connection (blue): listed as From</span><span><i class="swatch outgoing"></i>Outgoing connection (amber): listed as To</span><span><i class="swatch both"></i>Both directions (self link)</span><span><i class="swatch dashed"></i>Dashed line: indirect, such as a policy, an export or a start-up gate</span></div></div></header>
<main>{"".join(panels)}<footer>Generated from {html.escape(source.name)} by scripts/build_architecture_viewer.py. Everything is embedded: no server, CDN or network request.</footer></main>
<script>{js}</script></body></html>
'''


def embedded_digest(output: Path) -> str | None:
    if not output.exists():
        return None
    match = re.search(rf'<meta name="{DIGEST_META}" content="([0-9a-f]{{64}})">',
                      output.read_text(encoding="utf-8"))
    return match.group(1) if match else None


def check(diagrams: list[Diagram], output: Path, source: Path) -> int:
    """Exit status 0 when the HTML was built from this source and still maps the same graph."""
    expected = digest(diagrams)
    found = embedded_digest(output)
    if found != expected:
        print(f"{output} is out of date with {source} (embedded {found or 'none'}, expected "
              f"{expected}). Rebuild it: run this script without --check (needs $MMDC).",
              file=sys.stderr)
        return 1
    text = output.read_text(encoding="utf-8")
    panels = re.split(r'<section id="panel-\d+"', text)[1:]
    if len(panels) != len(diagrams):
        print(f"{output} has {len(panels)} panels, the source has {len(diagrams)} diagrams.",
              file=sys.stderr)
        return 1
    for diagram, panel in zip(diagrams, panels):
        html_nodes = sorted(re.findall(r'data-node-id="([^"]+)"', panel))
        html_edges = sorted(re.findall(r'data-source="([^"]+)" data-target="([^"]+)"', panel))
        want_edges = sorted((e.source, e.target) for e in diagram.edges)
        if html_nodes != sorted(diagram.nodes) or html_edges != want_edges:
            print(f"{output}: the nodes or edges in the {diagram.title!r} panel differ from "
                  "the Mermaid source.", file=sys.stderr)
            return 1
    print(f"{output.name} matches its Mermaid source ({len(diagrams)} diagrams, "
          f"{sum(len(d.nodes) for d in diagrams)} nodes, {sum(len(d.edges) for d in diagrams)} edges).")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0],
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE,
                        help="Markdown with Mermaid blocks (default: docs/architecture.md)")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--title", default="Micro-Billing-Ledger architecture")
    parser.add_argument("--mmdc", default=os.environ.get("MMDC") or shutil.which("mmdc"),
                        help="mermaid-cli executable (default: $MMDC, then mmdc on PATH)")
    parser.add_argument("--browser", default=os.environ.get("CHROME") or shutil.which("google-chrome")
                        or shutil.which("chromium") or shutil.which("chromium-browser"),
                        help="Chrome or Chromium for mermaid-cli (default: $CHROME, then PATH)")
    parser.add_argument("--work-dir", type=Path, default=None,
                        help="parent for temporary renders and the browser profile "
                             "(default: the system temporary directory)")
    parser.add_argument("--check", action="store_true",
                        help="render nothing; exit 1 if the output was built from a different source")
    args = parser.parse_args(argv)
    diagrams = read_diagrams(args.source)

    if args.check:
        return check(diagrams, args.output, args.source)

    if not args.mmdc or not args.browser:
        parser.error("mermaid-cli and Chrome/Chromium are required to render; pass --mmdc and "
                     "--browser, or set $MMDC and $CHROME")
    svgs = render(diagrams, args.mmdc, args.browser, args.work_dir)
    try:
        shown = args.source.resolve().relative_to(ROOT)
    except ValueError:
        shown = args.source
    output = page(diagrams, svgs, Path(str(shown)), args.title, digest(diagrams))
    with open(args.output, "w", encoding="utf-8", newline="\n") as handle:
        handle.write(output)
    print(f"wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
