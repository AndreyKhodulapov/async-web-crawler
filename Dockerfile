# The crawler in a container.
#   docker build -t async-web-crawler .               # without a browser
#   docker build --target js -t async-web-crawler:js .  # with Chromium, for rendering.mode
#   docker run --rm async-web-crawler --urls https://books.toscrape.com/ --max-pages 20
# Files the crawl writes go to /app/out, e.g. --output out/pages.jsonl; mount
# a directory there to keep them. docker-compose.yml runs crawl jobs.

ARG PYTHON_VERSION=3.14

# The environment of the crawler: its dependencies, then the package.
FROM python:${PYTHON_VERSION}-slim AS build
ENV PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
RUN python -m venv /opt/venv
ENV PATH=/opt/venv/bin:$PATH
WORKDIR /build
# A layer of its own: the dependencies are installed again only when they change.
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-deps .

# Only the environment and the scripts of the command line; the package
# `crawler` is the one installed in the environment.
FROM python:${PYTHON_VERSION}-slim AS crawler
ENV PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    MPLCONFIGDIR=/tmp/matplotlib
RUN useradd --uid 10001 --create-home --shell /usr/sbin/nologin crawler
COPY --from=build /opt/venv /opt/venv
WORKDIR /app
COPY src/*.py src/demo_urls.yaml src/
RUN mkdir out && chown crawler:crawler out
VOLUME /app/out
USER crawler
ENTRYPOINT ["python", "src/main.py"]
CMD ["--help"]

# With Chromium and the libraries it needs, for pages rendered by JavaScript.
# Run it with --init and --shm-size=1g: Chromium leaves processes behind and
# needs more shared memory than the 64 MB a container has.
FROM crawler AS js
USER root
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
RUN playwright install --with-deps chromium && rm -rf /var/lib/apt/lists/*
USER crawler

# `docker build` without --target builds the image without a browser.
FROM crawler
