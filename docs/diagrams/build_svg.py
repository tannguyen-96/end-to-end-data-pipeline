"""Render one page of a .drawio file to an editable .drawio.svg.

The output is a real SVG (renders on GitHub) that also carries the diagram XML in
its `content` attribute, so draw.io reopens it editable. Source of truth stays the
.drawio file: run this again after editing it.

Usage:  python docs/diagrams/build_svg.py <file.drawio> <page-id-or-name> <out.drawio.svg>
"""
import html
import io
import math
import re
import sys
from xml.etree import ElementTree as ET

FONT = "-apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif"
FS = 12.0
LH = 15.0
CHAR = 0.56  # average glyph width in em


def esc(s):
    return html.escape(s, quote=False)


def strip_tags(s):
    return re.sub(r"<[^>]+>", "", s)


def style_dict(style):
    d = {}
    for part in (style or "").split(";"):
        if "=" in part:
            k, v = part.split("=", 1)
            d[k.strip()] = v.strip()
        elif part.strip():
            d[part.strip()] = "1"
    return d


def line_parts(line):
    """Split one label line into (bold, plain_text) runs, HTML entities decoded."""
    out = []
    for part in re.split(r"(<b>.*?</b>)", line or ""):
        if not part:
            continue
        bold = part.startswith("<b>")
        txt = html.unescape(strip_tags(part))
        if txt:
            out.append((bold, txt))
    return out


def text_width(line):
    return sum(len(t) * CHAR * (1.08 if b else 1.0) for b, t in line_parts(line)) * FS


def text_block(value, cx, cy, anchor="middle", wrap_left=None):
    """Emit one <text> per line; the browser does the glyph advance and centring."""
    lines = [l for l in re.split(r"<br\s*/?>", value or "") if l.strip() != ""] or [""]
    if anchor == "middle":
        top = cy - (len(lines) - 1) * LH / 2.0
    else:
        top = cy + FS * 0.9
    out = []
    for i, line in enumerate(lines):
        y = top + i * LH
        tspans = []
        for bold, txt in line_parts(line):
            weight = ' font-weight="bold"' if bold else ""
            tspans.append("<tspan%s>%s</tspan>" % (weight, esc(txt)))
        out.append(
            '<text x="%.1f" y="%.1f" text-anchor="%s" font-size="%g" fill="#1f2933">%s</text>'
            % (cx, y, "middle" if anchor == "middle" else "start", FS, "".join(tspans))
        )
    return "".join(out)


def rect_clip(rect, p0, p1):
    """Point where the segment p0 (outside) -> p1 (inside) crosses the rect border."""
    x, y, w, h = rect
    dx, dy = p1[0] - p0[0], p1[1] - p0[1]
    tx = ((x - p0[0]) / dx) if dx > 1e-9 else (((x + w - p0[0]) / dx) if dx < -1e-9 else float("-inf"))
    ty = ((y - p0[1]) / dy) if dy > 1e-9 else (((y + h - p0[1]) / dy) if dy < -1e-9 else float("-inf"))
    t = max(tx, ty)
    if t == float("-inf"):
        return p1
    return (p0[0] + dx * t, p0[1] + dy * t)


def polyline_midpoint(pts):
    segs = []
    total = 0.0
    for a, b in zip(pts, pts[1:]):
        d = math.hypot(b[0] - a[0], b[1] - a[1])
        segs.append((a, b, d))
        total += d
    if total == 0:
        return pts[0]
    half = total / 2.0
    for a, b, d in segs:
        if half <= d:
            r = half / d if d else 0
            return (a[0] + (b[0] - a[0]) * r, a[1] + (b[1] - a[1]) * r)
        half -= d
    return pts[-1]


def load_page(path, key):
    tree = ET.parse(path)
    root = tree.getroot()
    for d in root.findall("diagram"):
        if key in (d.get("id"), d.get("name")):
            return d, d.find("mxGraphModel")
    raise SystemExit("page not found: %s" % key)


def render(model):
    g = model.find("root")
    verts, edges = {}, []
    for el in g:
        if el.tag == "UserObject":
            cell, value = el.find("mxCell"), el.get("label", "")
        elif el.tag == "mxCell":
            cell, value = el, el.get("value", "")
        else:
            continue
        if cell is None:
            continue
        geo = cell.find("mxGeometry")
        if cell.get("vertex") == "1" and geo is not None:
            if "<img" in (value or ""):
                continue  # hotlinked icons do not survive GitHub's SVG sanitizer
            verts[cell.get("id")] = {
                "value": value or "",
                "style": style_dict(cell.get("style")),
                "rect": (
                    float(geo.get("x", 0)),
                    float(geo.get("y", 0)),
                    float(geo.get("width", 0)),
                    float(geo.get("height", 0)),
                ),
            }
        elif cell.get("edge") == "1":
            pts = []
            if geo is not None:
                arr = geo.find('Array[@as="points"]')
                if arr is not None:
                    for p in arr.findall("mxPoint"):
                        pts.append((float(p.get("x")), float(p.get("y"))))
            edges.append(
                {
                    "value": value or "",
                    "style": style_dict(cell.get("style")),
                    "src": cell.get("source"),
                    "tgt": cell.get("target"),
                    "points": pts,
                }
            )

    xs, ys, xe, ye = [], [], [], []
    for v in verts.values():
        x, y, w, h = v["rect"]
        xs.append(x)
        ys.append(y)
        xe.append(x + w)
        ye.append(y + h)
    for e in edges:
        for p in e["points"]:
            xs.append(p[0])
            ys.append(p[1])
            xe.append(p[0])
            ye.append(p[1])
    pad = 30
    ox, oy = pad - min(xs), pad - min(ys)
    W = int(max(xe) - min(xs) + pad * 2)
    H = int(max(ye) - min(ys) + pad * 2)

    def T(p):
        return (p[0] + ox, p[1] + oy)

    body = []
    body.append('<rect width="%d" height="%d" fill="#ffffff"/>' % (W, H))

    def is_backdrop(v):
        st = v["style"]
        return st.get("fillColor") == "none" and "text" not in st

    def colour_of(st, key, default):
        val = st.get(key)
        if not val or val == "none":
            return "none" if val == "none" else default
        return "#" + val.lstrip("#")

    def draw_vertex(v):
        st = v["style"]
        x, y, w, h = v["rect"]
        fill = colour_of(st, "fillColor", "#ffffff")
        stroke = colour_of(st, "strokeColor", "#666666")
        dash = ' stroke-dasharray="8 8"' if st.get("dashed") == "1" else ""
        cx, cy = T((x + w / 2.0, y + h / 2.0))
        if "rhombus" in st:
            pts = [(cx, cy - h / 2.0), (cx + w / 2.0, cy), (cx, cy + h / 2.0), (cx - w / 2.0, cy)]
            body.append(
                '<polygon points="%s" fill="%s" stroke="%s"%s/>'
                % (" ".join("%.1f,%.1f" % p for p in pts), fill, stroke, dash)
            )
        elif "text" in st and "rounded" not in st:
            body.append(text_block(v["value"], cx - w / 2.0 + 2, cy - h / 2.0, anchor="start"))
            return
        else:
            body.append(
                '<rect x="%.1f" y="%.1f" width="%g" height="%g" rx="8" fill="%s" stroke="%s"%s/>'
                % (cx - w / 2.0, cy - h / 2.0, w, h, fill, stroke, dash)
            )
        body.append(text_block(v["value"], cx, cy))

    # containers (no fill) go behind the connectors, everything else in front of them
    for v in verts.values():
        if is_backdrop(v):
            draw_vertex(v)

    # edges: shapes paint over the centre-anchored stubs
    for e in edges:
        s, t = verts.get(e["src"]), verts.get(e["tgt"])
        if not s or not t:
            continue
        sc = (s["rect"][0] + s["rect"][2] / 2.0, s["rect"][1] + s["rect"][3] / 2.0)
        tc = (t["rect"][0] + t["rect"][2] / 2.0, t["rect"][1] + t["rect"][3] / 2.0)
        pts = [sc] + list(e["points"]) + [tc]
        end = rect_clip(t["rect"], pts[-2], pts[-1])
        pts[-1] = end
        st = e["style"]
        colour = "#" + st["strokeColor"].lstrip("#") if "strokeColor" in st else "#666666"
        dash = ' stroke-dasharray="6 4"' if st.get("dashed") == "1" else ""
        marker = "arRed" if colour.lower() in ("#b85450",) else "ar"
        d = "M %.1f,%.1f " % T(pts[0]) + " ".join("L %.1f,%.1f" % T(p) for p in pts[1:])
        body.append(
            '<path d="%s" fill="none" stroke="%s" stroke-width="1.3"%s marker-end="url(#%s)"/>'
            % (d, colour, dash, marker)
        )
        if e["value"]:
            mx, my = polyline_midpoint(pts)
            mx, my = T((mx, my))
            label = html.unescape(strip_tags(e["value"]))
            lw = len(label) * CHAR * FS + 8
            body.append(
                '<rect x="%.1f" y="%.1f" width="%.1f" height="%.1f" fill="#ffffff" fill-opacity="0.92"/>'
                % (mx - lw / 2.0, my - LH / 2.0, lw, LH)
            )
            body.append(
                '<text x="%.1f" y="%.1f" text-anchor="middle" font-size="%g" fill="#4b5563">%s</text>'
                % (mx, my + FS * 0.35, FS - 1, esc(label))
            )

    for v in verts.values():
        if not is_backdrop(v):
            draw_vertex(v)

    model_xml = ET.tostring(model, encoding="unicode")
    mxfile = "<mxfile host=\"app.diagrams.net\">%s</mxfile>" % model_xml
    return (
        '<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" viewBox="0 0 %d %d" '
        'font-family="%s" content="%s">\n'
        "  <defs>\n"
        '    <marker id="ar" viewBox="0 0 10 10" refX="9.5" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#666666"/></marker>\n'
        '    <marker id="arRed" viewBox="0 0 10 10" refX="9.5" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#b85450"/></marker>\n'
        "  </defs>\n  %s\n</svg>\n"
    ) % (W, H, W, H, FONT, html.escape(mxfile, quote=True), "\n  ".join(body))


if __name__ == "__main__":
    src, key, out = sys.argv[1], sys.argv[2], sys.argv[3]
    diagram, model = load_page(src, key)
    svg = render(model)
    io.open(out, "w", encoding="utf-8", newline="\n").write(svg)
    print("wrote %s (%d bytes)" % (out, len(svg)))
