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
