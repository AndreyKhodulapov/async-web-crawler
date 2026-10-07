"""Integration tests: the images of the Dockerfile are built, and the crawler in a container crawls a site of the host.

The container reaches the test site, served on every address of the host,
by the name host.docker.internal; it writes its files to a directory of
the test mounted at /app/out, as the user running the tests.
"""

import asyncio
import json
import os
import subprocess
import uuid
from pathlib import Path

import pytest
import yaml
from helpers import FAST_CONFIG

pytestmark = pytest.mark.docker

ROOT = Path(__file__).parents[2]
IMAGE = "async-web-crawler:test"
JS_IMAGE = "async-web-crawler:test-js"
HOST = "host.docker.internal"


def build(target: str, tag: str) -> str:
    """Build the image of the Dockerfile `target` as `tag`; skips the test if there is no Docker."""
    try:
        subprocess.run(["docker", "info"], capture_output=True, check=True, timeout=30)
    except (OSError, subprocess.SubprocessError):
        pytest.skip("Docker is not running")
    result = subprocess.run(
        ["docker", "build", "--target", target, "-t", tag, str(ROOT)],
        capture_output=True,
        text=True,
        timeout=1800,
        check=False,
    )
    assert result.returncode == 0, result.stderr[-3000:]
    return tag


@pytest.fixture(scope="session")
def image() -> str:
    return build("crawler", IMAGE)


@pytest.fixture(scope="session")
def js_image() -> str:
    return build("js", JS_IMAGE)


@pytest.fixture
def server_host() -> str:
    """The test site listens on every address of the host, so that a container reaches it."""
    return "0.0.0.0"


@pytest.fixture
def out(tmp_path) -> Path:
    """The directory mounted at /app/out."""
    return tmp_path


def docker_run(image: str, out: Path, *args: str, options: tuple[str, ...] = ()) -> list[str]:
    """The `docker run` of `image` with the arguments of the crawler `args`."""
    return [
        "docker", "run", "--rm",
        "--add-host", f"{HOST}:host-gateway",  # Docker Desktop knows the name; Linux needs it
        "--user", f"{os.getuid()}:{os.getgid()}",
        "--volume", f"{out}:/app/out",
        *options, image, *args,
    ]  # fmt: skip


def write_config(out: Path, **sections) -> str:
    """Write the configuration of the crawl to the mounted directory; return its path in the container."""
    (out / "config.yaml").write_text(yaml.safe_dump({**FAST_CONFIG, **sections}), encoding="utf-8")
    return "out/config.yaml"


def saved_urls(path: Path) -> set[str]:
    return {json.loads(line)["url"] for line in path.read_text(encoding="utf-8").splitlines()}


async def run(command: list[str]) -> tuple[int, str, str]:
    process = await asyncio.create_subprocess_exec(
        *command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    output, errors = await asyncio.wait_for(process.communicate(), timeout=300)
    return process.returncode, output.decode(), errors.decode()


def test_help_is_the_default_command(image, out):
    result = subprocess.run(docker_run(image, out), capture_output=True, text=True, timeout=120, check=False)

    assert result.returncode == 0, result.stderr
    assert result.stdout.startswith("usage: main.py")
    assert (
        "worker"
        in subprocess.run(
            docker_run(image, out, "worker", "--help"), capture_output=True, text=True, timeout=120, check=False
        ).stdout
    )


async def test_crawls_a_site_of_the_host_into_the_mounted_directory(image, out, url):
    config = write_config(
        out,
        urls=[url("/site/", host=HOST)],
        filters={"same_domain_only": True},
        storage={"outputs": ["out/pages.jsonl"]},
    )

    code, output, errors = await run(docker_run(image, out, "--config", config, "--no-progress"))

    assert code == 0, errors
    assert saved_urls(out / "pages.jsonl") == {
        url(path, host=HOST) for path in ("/site/", "/site/a.html", "/site/b.html")
    }
    assert "=== Crawl finished (" in output


async def test_docker_stop_saves_the_pages_fetched_and_exits_with_143(image, out, url):
    config = write_config(
        out,
        urls=[url("/ok", host=HOST), url("/delay/60", host=HOST)],
        crawler={**FAST_CONFIG["crawler"], "max_depth": 0},
        storage={"outputs": ["out/pages.jsonl"], "batch_size": 100},
        report={"stats_json": "out/stats.json"},
    )
    name = f"crawler-test-{uuid.uuid4().hex[:8]}"
    process = await asyncio.create_subprocess_exec(
        *docker_run(image, out, "--config", config, options=("--name", name)),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        async with asyncio.timeout(120):
            # Without a terminal every update of the progress is a line: /ok is fetched, /delay/60 is in flight.
            while b"1/100 pages" not in await process.stderr.readline():
                assert process.returncode is None
        stop = await asyncio.create_subprocess_exec("docker", "stop", name, stdout=asyncio.subprocess.DEVNULL)
        output, _ = await asyncio.wait_for(process.communicate(), timeout=60)
        await stop.wait()
    finally:
        if process.returncode is None:
            await run(["docker", "kill", name])

    assert process.returncode == 143
    # The page was in the buffer of the storage, not written yet, when the container was stopped.
    assert saved_urls(out / "pages.jsonl") == {url("/ok", host=HOST)}
    assert json.loads((out / "stats.json").read_text(encoding="utf-8"))["successful"] == 1
    assert "=== Crawl interrupted (" in output.decode()


@pytest.mark.browser
async def test_js_image_renders_pages(js_image, out, url):
    config = write_config(
        out, urls=[url("/js/links", host=HOST)], rendering={"mode": "always"}, storage={"outputs": ["out/pages.jsonl"]}
    )

    code, _, errors = await run(
        docker_run(js_image, out, "--config", config, "--no-progress", options=("--init", "--shm-size", "1g"))
    )

    assert code == 0, errors
    # The link is made by the script of the page: only a browser finds it.
    assert saved_urls(out / "pages.jsonl") == {url("/js/links", host=HOST), url("/js/target", host=HOST)}
