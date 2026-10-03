"""Test pages shared by unit and integration tests."""

import codecs
from pathlib import Path


def fixture_html(name: str) -> str:
    return (Path(__file__).parent / "fixtures" / name).read_text(encoding="utf-8")


# Pages whose encoding must be worked out from the body, or from a header
# charset that cannot decode text: (body, header charset, expected title).
CAFE = "<title>Café</title>"
ENCODING_PAGES: dict[str, tuple[bytes, str | None, str]] = {
    # The encoding is declared only in the markup, not in the header.
    "meta-charset": (('<meta charset="windows-1252">' + CAFE).encode("cp1252"), None, "Café"),
    "meta-http-equiv": (
        '<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-2"><title>Łódź</title>'.encode(
            "iso-8859-2"
        ),
        None,
        "Łódź",  # "£ódŸ" under windows-1252
    ),
    "meta-shift-jis": ('<meta charset="shift_jis"><title>カフェ</title>'.encode("shift_jis"), None, "カフェ"),
    # The header charset wins over the markup.
    "header-charset-wins": (('<meta charset="utf-8">' + CAFE).encode("cp1252"), "windows-1252", "Café"),
    # A byte order mark wins over everything else.
    "utf8-bom": (codecs.BOM_UTF8 + CAFE.encode(), "windows-1252", "Café"),
    "utf16le-bom": (codecs.BOM_UTF16_LE + CAFE.encode("utf-16-le"), None, "Café"),
    "utf16be-bom": (codecs.BOM_UTF16_BE + CAFE.encode("utf-16-be"), None, "Café"),
    # A <meta> found in ASCII bytes cannot really mean UTF-16/32 (HTML spec):
    # the page is read as UTF-8.
    "meta-utf16": (('<meta charset="utf-16">' + CAFE).encode(), None, "Café"),
    "meta-utf32": (('<meta charset="utf-32">' + CAFE).encode(), None, "Café"),
    # Charsets that name no codec, or a codec that cannot decode a page.
    # "undefined" raises UnicodeError, which the client otherwise maps to
    # InvalidURLError for the IDNA step: decoding must handle it first.
    "meta-unknown": (('<meta charset="no-such-charset">' + CAFE).encode(), None, "Café"),
    "meta-undefined": (('<meta charset="undefined">' + CAFE).encode(), None, "Café"),
    "meta-idna": (('<meta charset="idna">' + CAFE).encode(), None, "Café"),
    "meta-base64": (('<meta charset="base64">' + CAFE).encode(), None, "Café"),
    "header-undefined": (CAFE.encode(), "undefined", "Café"),
    "header-idna": (CAFE.encode(), "idna", "Café"),
    "header-base64": (CAFE.encode(), "base64", "Café"),
    "header-unknown": (CAFE.encode(), "no-such-charset", "Café"),
}


# A small site for crawl tests, served under /site/. Depths from /site/:
#   0  /site/
#   1  a.html, b.html, missing.html (404), files/manual.pdf (404), the same
#      site on another host ({other_host} becomes http://localhost:<port>)
#   2  moved (redirects to c.html), a/deeper.html
#   3  a/deepest.html, c.html
# Cycles, duplicate links and self-links check that no page is fetched twice.
# /site/exits.html is not linked from the others: it starts crawls whose
# links redirect to another host (to-other-host) or to c.html (moved).
# /site/names.html links to pages whose URLs need percent-encoding, one of
# them twice: as raw text and already encoded.
# Not linked either: /site/go redirects to private/secret, and
# /site/cookie-check redirects to itself once to set a cookie.
# /site/robots.html links to pages that ask crawlers, by rel="nofollow",
# <meta name="robots"> or X-Robots-Tag, not to follow links or keep pages.
# /site/long.html links to a page with a query of LONG_QUERY characters.
# /site/variant.html names itself as canonical, so with any query it is a
# variant; /site/points-home.html names the home page, a canonical URL
# with another path.
LONG_QUERY = 3000
SITE_PAGES: dict[str, str] = {
    "/site/": """
        <title>Home</title>
        <a href="a.html">A</a> <a href="b.html">B</a> <a href="a.html#part">A again</a>
        <a href="missing.html">Broken</a> <a href="files/manual.pdf">Manual</a>
        <a href="{other_host}/site/">Same site, other host</a> <a href="mailto:owner@site">Mail</a>
    """,
    "/site/a.html": """
        <title>A</title><a href="/site/">Home</a> <a href="moved">Moved</a> <a href="a/deeper.html">Deeper</a>
    """,
    "/site/b.html": '<title>B</title><a href="a.html">A</a> <a href="b.html">Self</a>',
    "/site/a/deeper.html": '<title>Deeper</title><a href="deepest.html">Deepest</a> <a href="../c.html">C</a>',
    "/site/a/deepest.html": '<title>Deepest</title><a href="/site/">Home</a>',
    "/site/c.html": "<title>C</title>",
    "/site/private/secret": "<title>Secret</title>",
    "/site/cookie-check": "<title>Checked</title>",
    "/site/exits.html": '<title>Exits</title><a href="to-other-host">Sign in</a> <a href="moved">Moved</a>',
    "/site/names.html": """
        <title>Names</title>
        <a href="café.html">Raw</a> <a href="caf%C3%A9.html">Encoded</a> <a href="a b.html">Space</a>
    """,
    "/site/café.html": "<title>Café</title>",
    "/site/a b.html": "<title>Space</title>",
    "/site/robots.html": """
        <title>Robots</title><a href="a.html" rel="sponsored nofollow">Ad</a> <a href="noindex.html">Noindex</a>
        <a href="nofollow.html">Nofollow</a> <a href="tagged.html">Tagged</a>
    """,
    "/site/noindex.html": '<meta name="robots" content="noindex"><title>Noindex</title><a href="b.html">B</a>',
    "/site/nofollow.html": '<meta name="robots" content="nofollow"><title>Nofollow</title><a href="c.html">C</a>',
    "/site/tagged.html": '<title>Tagged</title><a href="a/deeper.html">Deeper</a>',
    "/site/variant.html": '<link rel="canonical" href="variant.html"><title>Variant</title><a href="c.html">C</a>',
    "/site/points-home.html": '<link rel="canonical" href="/site/"><title>Points home</title>',
    "/site/long.html": f'<title>Long</title><a href="c.html?q={"x" * LONG_QUERY}">Long</a> <a href="b.html">B</a>',
}
# Response headers of some of SITE_PAGES.
SITE_HEADERS: dict[str, dict[str, str]] = {"/site/tagged.html": {"X-Robots-Tag": "none"}}
