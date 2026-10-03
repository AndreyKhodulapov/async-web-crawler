"""Unit tests: relative links of the README and of docs/ lead to existing files and headings."""

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[2]
DOCUMENTS = [ROOT / "README.md", *sorted((ROOT / "docs").glob("*.md"))]

LINK = re.compile(r"\[[^\]]*\]\(([^)\s]+)\)")
CODE_BLOCK = re.compile(r"^```.*?^```", re.MULTILINE | re.DOTALL)
HEADING = re.compile(r"^#+\s+(.*)$", re.MULTILINE)


def prose(document: Path) -> str:
    """The text of a document without its code blocks: a `#` or a bracket there is not markup."""
    return CODE_BLOCK.sub("", document.read_text(encoding="utf-8"))


def anchors(document: Path) -> set[str]:
    """The anchors GitHub gives the headings of a document."""
    found: set[str] = set()
    for heading in HEADING.findall(prose(document)):
        anchor = re.sub(r"[^\w\- ]", "", heading.strip().lower()).replace(" ", "-")
        repeat = sum(1 for known in found if re.fullmatch(rf"{re.escape(anchor)}(-\d+)?", known))
        found.add(f"{anchor}-{repeat}" if repeat else anchor)
    return found


def links(document: Path) -> list[str]:
    return [target for target in LINK.findall(prose(document)) if not re.match(r"[a-z][a-z0-9+.-]*:", target)]


def test_anchors_are_made_the_way_github_makes_them(tmp_path):
    document = tmp_path / "page.md"
    document.write_text(
        "# The `crawl` command: options & output\n\n## Notes\n\n```\n# not a heading\n```\n\n## Notes\n"
    )

    assert anchors(document) == {"the-crawl-command-options--output", "notes", "notes-1"}


def test_documents_have_links_to_check():
    assert len(DOCUMENTS) > 5
    assert links(ROOT / "README.md")


@pytest.mark.parametrize("document", DOCUMENTS, ids=lambda path: str(path.relative_to(ROOT)))
def test_relative_links_lead_somewhere(document):
    broken = []
    for target in links(document):
        path, _, anchor = target.partition("#")
        file = document if not path else (document.parent / path).resolve()
        if not file.exists():
            broken.append(f"{target}: no such file")
        elif anchor and anchor not in anchors(file):
            broken.append(f"{target}: no such heading")

    assert not broken
