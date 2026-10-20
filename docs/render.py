"""Render docs/design.md to docs/design.pdf with headless Chrome.

    pip install markdown          # or: pip install -e "py[docs]"
    python docs/render.py         # CHROME=/path/to/chrome to override the browser

Edit design.md, then rerun this; the PDF is generated, never edited by hand.
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from pathlib import Path

import markdown

DOCS = Path(__file__).resolve().parent
SOURCE = DOCS / "design.md"
OUTPUT = DOCS / "design.pdf"
TITLE = "Distributed Vector Mean — Design"

CHROME_CANDIDATES = [
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Chromium.app/Contents/MacOS/Chromium",
    "google-chrome",
    "chromium",
]

CSS = """
@page {
  size: A4;
  margin: 22mm 20mm 20mm 20mm;
  @top-left { content: "%(title)s"; font: 7.5pt -apple-system, "Helvetica Neue", Arial, sans-serif; color: #888; }
  @bottom-right { content: "Page " counter(page) " of " counter(pages); font: 7.5pt -apple-system, "Helvetica Neue", Arial, sans-serif; color: #888; }
}
html { font: 10pt/1.55 -apple-system, "Helvetica Neue", Arial, sans-serif; color: #1f1f1f; }
h1, h2, h3 { font-family: Georgia, "Times New Roman", serif; font-weight: 400; color: #111; break-after: avoid; }
h1 { font-size: 24pt; margin: 0 0 8pt; letter-spacing: -0.01em; }
h2 { font-size: 15pt; margin: 20pt 0 6pt; }
h3 { font-size: 12pt; margin: 14pt 0 4pt; }
p, li { orphans: 3; widows: 3; }
.byline span { background: #f1efe9; border-radius: 4px; padding: 1pt 5pt; font-size: 8.5pt; }
code { font: 8.5pt "SF Mono", Menlo, Consolas, monospace; background: #f3f2ee; border-radius: 3px; padding: 0 2pt; }
pre { background: #f7f6f2; border: 1px solid #e6e3da; border-radius: 4px; padding: 7pt 9pt; break-inside: avoid; }
pre code { background: none; padding: 0; font-size: 7.8pt; line-height: 1.45; }
table { border-collapse: collapse; margin: 6pt 0 10pt; font-size: 8.8pt; break-inside: auto; }
tr { break-inside: avoid; }
th { background: #f1efe9; font-weight: 500; color: #555; text-align: left; }
th, td { border: 1px solid #e3e0d8; padding: 4pt 7pt; vertical-align: top; }
img { max-width: 100%%; display: block; margin: 8pt auto; }
"""


def find_chrome() -> str:
    candidates = [os.environ["CHROME"]] if os.environ.get("CHROME") else CHROME_CANDIDATES
    for c in candidates:
        if Path(c).exists() or subprocess.run(["which", c], capture_output=True).returncode == 0:
            return c
    sys.exit("Chrome or Chromium not found; set CHROME=/path/to/chrome")


def main() -> None:
    body = markdown.markdown(SOURCE.read_text(), extensions=["tables", "fenced_code"])
    html = (
        f'<!doctype html><html><head><meta charset="utf-8"><title>{TITLE}</title>'
        f'<base href="{DOCS.as_uri()}/"><style>{CSS % {"title": TITLE}}</style></head>'
        f"<body>{body}</body></html>"
    )
    with tempfile.TemporaryDirectory() as tmp:
        page = Path(tmp) / "design.html"
        page.write_text(html)
        subprocess.run(
            [find_chrome(), "--headless=new", "--disable-gpu", "--no-pdf-header-footer",
             f"--user-data-dir={tmp}/profile", f"--print-to-pdf={OUTPUT}", page.as_uri()],
            check=True, capture_output=True,
        )
    print(f"wrote {OUTPUT.relative_to(DOCS.parent)}")


if __name__ == "__main__":
    main()
