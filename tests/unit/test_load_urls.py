"""Unit tests for reading a list of start URLs from a file or stdin."""

import io
import sys
from pathlib import Path

import pytest

from crawler import ConfigError, load_urls

SPACES = "a URL cannot contain spaces or control characters (a space is written %20)"


def write_list(tmp_path, content: bytes | str, name: str = "urls.txt") -> str:
    path = tmp_path / name
    if isinstance(content, str):
        content = content.encode("utf-8")
    path.write_bytes(content)
    return str(path)


def problems_of(path) -> ConfigError:
    with pytest.raises(ConfigError) as error:
        load_urls(path)
    return error.value


def test_one_url_per_line(tmp_path):
    path = write_list(tmp_path, "https://one.example/\nhttps://two.example/page?q=1#top\n")

    assert load_urls(path) == ["https://one.example/", "https://two.example/page?q=1#top"]


def test_comments_blank_lines_and_spaces_are_skipped(tmp_path):
    path = write_list(
        tmp_path,
        "# start pages\n\n   \n  https://one.example/  \n\t# indented comment\nhttp://two.example/\t\n",
    )

    assert load_urls(path) == ["https://one.example/", "http://two.example/"]


def test_bom_and_crlf_are_accepted(tmp_path):
    path = write_list(tmp_path, b"\xef\xbb\xbfhttps://one.example/\r\nhttps://two.example/\r\n")

    assert load_urls(path) == ["https://one.example/", "https://two.example/"]


def test_repeated_urls_keep_the_place_of_the_first(tmp_path):
    path = write_list(tmp_path, "https://b.example/\nhttps://a.example/\nhttps://b.example/\n https://a.example/\n")

    assert load_urls(path) == ["https://b.example/", "https://a.example/"]


@pytest.mark.parametrize("content", ["", "\n\n", "# nothing yet\n"])
def test_list_without_urls_is_empty(content, tmp_path):
    assert load_urls(write_list(tmp_path, content)) == []


def test_every_invalid_line_is_reported_with_its_number(tmp_path):
    path = write_list(
        tmp_path,
        "# header\nhttps://ok.example/\nexample.com\n\nftp://files.example/\r\nhttps://ok.example/2\nnot a url\n",
    )

    error = problems_of(path)

    assert error.problems == [
        f'{path}:3: expected an http:// or https:// URL, got "example.com"',
        f'{path}:5: expected an http:// or https:// URL, got "ftp://files.example/"',
        f'{path}:7: expected an http:// or https:// URL, got "not a url"',
    ]
    assert str(error).startswith(f"Invalid configuration: {path}: 2 URLs are valid, 3 lines are not\n  - {path}:3: ")


def test_one_invalid_line_is_counted_in_the_singular(tmp_path):
    path = write_list(tmp_path, "https://ok.example/\nexample.com\n")

    assert str(problems_of(path)) == (
        f"Invalid configuration: {path}: 1 URL is valid, 1 line is not\n"
        f'  - {path}:2: expected an http:// or https:// URL, got "example.com"'
    )


@pytest.mark.parametrize(
    ("line", "shown"),
    [
        ("https://one.example/ # the home page", '"https://one.example/ # the home page"'),
        ("https://one.example/my page", '"https://one.example/my page"'),
        ("https://one.example/a\tb", '"https://one.example/a\\tb"'),
        ("https://one.example/a\u00a0b", '"https://one.example/a\\u00a0b"'),
        ("https://one.example/a\x00b", '"https://one.example/a\\u0000b"'),
    ],
    ids=["comment", "space", "tab", "no-break-space", "null"],
)
def test_spaces_and_control_characters_inside_a_url_are_reported(line, shown, tmp_path):
    path = write_list(tmp_path, f"https://ok.example/\n{line}\n")

    assert problems_of(path).problems == [
        f"{path}:2: a URL cannot contain spaces or control characters (a space is written %20), got {shown}"
    ]


def test_lines_may_end_in_a_lone_carriage_return(tmp_path):
    path = write_list(tmp_path, "https://one.example/\rhttps://two.example/\r\nexample.com\rhttps://three.example/")

    error = problems_of(path)

    # Numbered as an editor shows them, not glued into one line.
    assert error.problems == [f'{path}:3: expected an http:// or https:// URL, got "example.com"']
    assert str(error).startswith(f"Invalid configuration: {path}: 3 URLs are valid, 1 line is not")


def test_long_invalid_line_is_shortened_in_the_message(tmp_path):
    path = write_list(tmp_path, "x" * 5000)

    (problem,) = problems_of(path).problems

    assert problem == f'{path}:1: expected an http:// or https:// URL, got "{"x" * 100}..."'


def test_many_invalid_lines_are_all_kept_but_the_message_shows_twenty(tmp_path):
    path = write_list(tmp_path, "https://ok.example/\n" + "bad\n" * 30)

    error = problems_of(path)

    assert len(error.problems) == 30
    message = str(error)
    assert message.startswith(f"Invalid configuration: {path}: 1 URL is valid, 30 lines are not\n")
    assert f"{path}:21:" in message and f"{path}:22:" not in message
    assert message.endswith("\n  - ... and 10 more")


def test_missing_file_is_a_configuration_error(tmp_path):
    path = str(tmp_path / "missing.txt")

    error = problems_of(path)

    assert error.source == path
    (problem,) = error.problems
    assert problem.startswith("cannot read the file: [Errno 2]")


def test_directory_is_a_configuration_error(tmp_path):
    (problem,) = problems_of(tmp_path).problems

    assert problem.startswith("cannot read the file: ")


def test_file_that_is_not_utf8_is_a_configuration_error(tmp_path):
    path = write_list(tmp_path, "https://example.com/caf\xe9\n".encode("latin-1"))

    (problem,) = problems_of(path).problems

    assert problem.startswith("cannot read the file: 'utf-8' codec can't decode byte 0xe9")


def test_dash_reads_stdin(monkeypatch):
    stdin = io.TextIOWrapper(
        io.BytesIO(b"\xef\xbb\xbf# from a pipe\r\nhttps://one.example/\r\nhttps://one.example/\r\n")
    )
    monkeypatch.setattr(sys, "stdin", stdin)

    assert load_urls("-") == ["https://one.example/"]


def test_stdin_is_named_in_the_problems(monkeypatch):
    monkeypatch.setattr(sys, "stdin", io.TextIOWrapper(io.BytesIO(b"example.com\n")))

    error = problems_of("-")

    assert error.source == "<stdin>"
    assert error.problems == ['<stdin>:1: expected an http:// or https:// URL, got "example.com"']


def test_missing_stdin_is_a_configuration_error(monkeypatch):
    # As Python sets it when the program starts with stdin closed (`<&-`).
    monkeypatch.setattr(sys, "stdin", None)

    error = problems_of("-")

    assert (error.source, error.problems) == ("<stdin>", ["there is no standard input to read URLs from"])


def test_example_list_is_valid():
    path = Path(__file__).parents[2] / "examples" / "urls.txt"

    assert load_urls(path) == [
        "https://books.toscrape.com/index.html",
        "https://quotes.toscrape.com/",
        "https://web-scraping.dev/products",
    ]
