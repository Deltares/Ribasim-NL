"""View local Ribasim models in the model viewer, optionally only their differences from another model.

    pixi run viewer path/to/model.toml
    pixi run viewer path/to/model.toml --base main
    pixi run viewer path/to/model.toml --base path/to/other/model.toml

`--base` is another model, or a git revision to compare with the same model at that revision. `--rev` views
the model at a git revision instead of in the working tree. Models at a revision are read with DVC, from the
DVC cache or remote. The models are exported with `ribasim_nl.webmap` to a cache that is reused while the
model is unchanged, and served with the viewer from `docs/viewer-app` on localhost.
"""

import argparse
import hashlib
import json
import mimetypes
import os
import re
import subprocess
import sys
import tomllib
import webbrowser
from collections.abc import Iterator
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlsplit

from dvc.api import DVCFileSystem

import ribasim_nl.webmap
import ribasim_nl.webmap_diff
from ribasim_nl.settings import settings
from ribasim_nl.webmap import export_webmap, model_input_dir, model_results_dir, netcdf_tables, read_config
from ribasim_nl.webmap_diff import diff_models

REPO_DIR = Path(__file__).parents[3]
APP_DIR = REPO_DIR / "docs/viewer-app"
CACHE_DIR = REPO_DIR / ".cache/viewer"
WATERBOARDS = Path("Basisgegevens/RWS_waterschaps_grenzen/waterschap.gpkg")
CONTENT_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".js": "text/javascript",
    ".css": "text/css",
    ".json": "application/json",
    ".geojson": "application/geo+json",
    ".parquet": "application/vnd.apache.parquet",
    ".pmtiles": "application/vnd.pmtiles",
    ".png": "image/png",
    ".svg": "image/svg+xml",
}
INDEX_HTML = """<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>{title}</title>
    <link rel="stylesheet" href="app/viewer.css" />
    <script type="module" src="app/viewer.js"></script>
    <style>html, body {{ margin: 0; height: 100%; }}</style>
  </head>
  <body>
    <div id="ribasim-viewer" style="height: 100vh"></div>
  </body>
</html>
"""
MANIFEST = "manifest.json"
# The result files that `export_webmap` reads
RESULT_FILES = ("basin.nc", "flow.nc", "control.nc")
# The characters of git revisions like main, origin/main, HEAD~2 or v1.0, without a leading dash: never an option
REVISION = re.compile(r"[\w.@/~^{}:][\w.@/~^{}:-]*")
RANGE = re.compile(r"bytes=(\d*)-(\d*)")
COPY_CHUNK = 1 << 20


def model_sources(toml_path: Path) -> list[Path]:
    """The files an export of the model depends on, including the export code."""
    config, input_dir, database = read_config(toml_path)
    sources = [toml_path, database, *netcdf_tables(config, input_dir).values(), Path(ribasim_nl.webmap.__file__)]
    results_dir = model_results_dir(toml_path, config)
    if results_dir.is_dir():
        sources += sorted(results_dir.glob("*.nc"))
    return sources


def is_up_to_date(output: Path, sources: list[Path]) -> bool:
    """Whether an output exists and is newer than all its sources."""
    return output.is_file() and all(output.stat().st_mtime >= source.stat().st_mtime for source in sources)


def cache_key(toml_path: Path) -> str:
    """A readable and unique cache directory name for a model."""
    digest = hashlib.sha256(str(toml_path.resolve()).lower().encode()).hexdigest()[:8]
    return f"{toml_path.stem}-{digest}"


def export_cached(toml_path: Path, cache_dir: Path = CACHE_DIR) -> Path:
    """Export a model for the viewer, unless the cached export is up to date; return the export directory."""
    output_dir = cache_dir / cache_key(toml_path)
    if is_up_to_date(output_dir / MANIFEST, model_sources(toml_path)):
        print(f"Using the cached export of {toml_path} in {output_dir}")
        return output_dir
    print(f"Exporting {toml_path} to {output_dir}")
    waterboards = settings.ribasim_nl_data_dir / WATERBOARDS
    if not waterboards.is_file():
        print(f"Leaving out the water boards, {waterboards} not found")
    export_webmap(toml_path, waterboards if waterboards.is_file() else None, output_dir)
    return output_dir


def diff_cached(
    base_toml: Path,
    head_toml: Path,
    base_dir: Path,
    head_dir: Path,
    labels: tuple[str, str],
    cache_dir: Path = CACHE_DIR,
) -> Path:
    """Compare two exported models, unless the cached comparison is up to date; return its directory."""
    output_dir = cache_dir / f"diff-{base_dir.name}-{head_dir.name}"
    output = output_dir / "diff.json"
    sources = [base_dir / MANIFEST, head_dir / MANIFEST, Path(ribasim_nl.webmap_diff.__file__)]
    if is_up_to_date(output, sources):
        print(f"Using the cached comparison in {output_dir}")
        # The same commits can be named by other revisions
        diff = json.loads(output.read_text())
        if (diff["base"]["label"], diff["head"]["label"]) != labels:
            diff["base"]["label"], diff["head"]["label"] = labels
            output.write_text(json.dumps(diff, indent=1))
    else:
        print(f"Comparing {base_toml} with {head_toml}")
        diff_models(base_toml, head_toml, base_dir, head_dir, output, labels)
    return output_dir


def resolve_commit(revision: str, repo_dir: Path = REPO_DIR) -> str | None:
    """The commit hash of a git revision, or None if it is not one."""
    if not REVISION.fullmatch(revision):
        return None
    # The revision is validated above and passed after --end-of-options
    result = subprocess.run(  # NOSONAR
        ["git", "-C", str(repo_dir), "rev-parse", "--verify", "--quiet", "--end-of-options", f"{revision}^{{commit}}"],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.stdout.strip() if result.returncode == 0 else None


def checkout_model(toml_path: Path, commit: str, repo_dir: Path = REPO_DIR, cache_dir: Path = CACHE_DIR) -> Path:
    """Get the files of a model at a git commit that the viewer needs into the cache; return the TOML there.

    The files keep their path relative to the repository, so the relative paths in the TOML still work.
    Files tracked by DVC are read from the DVC cache, or from the DVC remote if they are not in the cache.
    """
    repo_dir = repo_dir.resolve()
    assert toml_path.resolve().is_relative_to(repo_dir), f"{toml_path} is not in the repository {repo_dir}"
    root = cache_dir / "revisions" / commit
    target = root / toml_path.resolve().relative_to(repo_dir)
    complete = target.with_name(f"{target.name}.complete")
    if complete.is_file():
        print(f"Using the cached files of {toml_path} at {commit} in {root}")
        return target
    print(f"Getting the files of {toml_path} at {commit} into {root}")
    fs = DVCFileSystem(str(repo_dir), rev=commit)

    def repo_path(path: Path) -> str:
        normalized = Path(os.path.normpath(path))
        assert normalized.is_relative_to(root), f"{path} is outside the repository"
        return normalized.relative_to(root).as_posix()

    def get(path: Path) -> None:
        source = repo_path(path)
        assert fs.isfile(source), f"{source} not found at {commit}"
        path = root / source
        path.parent.mkdir(parents=True, exist_ok=True)
        fs.get_file(source, str(path))

    get(target)
    config = tomllib.loads(target.read_text())
    input_dir = model_input_dir(target, config)
    for path in [input_dir / "database.gpkg", *netcdf_tables(config, input_dir).values()]:
        get(path)
    results_dir = model_results_dir(target, config)
    for name in RESULT_FILES:
        if fs.isfile(repo_path(results_dir / name)):
            get(results_dir / name)
    complete.touch()
    return target


def parse_range(header: str, size: int) -> tuple[int, int] | None:
    """The first and last byte of a single `Range: bytes=...` header, or None if it is not satisfiable."""
    match = RANGE.fullmatch(header.strip())
    if not match or match.groups() == ("", ""):
        return None
    first, last = match.groups()
    if first == "":
        # A suffix range: the last bytes of the file
        return max(size - int(last), 0), size - 1
    start, end = int(first), min(int(last), size - 1) if last else size - 1
    return (start, end) if start <= end else None


def make_handler(mounts: dict[str, Path], index_html: str) -> type[BaseHTTPRequestHandler]:
    """A request handler that serves the index page and files from mounted directories, with range requests."""
    assert all(directory.is_dir() for directory in mounts.values()), f"Missing directory in {mounts}"

    class Handler(BaseHTTPRequestHandler):
        def resolve(self) -> Path | None:
            parts = unquote(urlsplit(self.path).path).lstrip("/").split("/", 1)
            if len(parts) != 2 or parts[0] not in mounts:
                return None
            root = mounts[parts[0]].resolve()
            path = (root / parts[1]).resolve()
            return path if path.is_relative_to(root) and path.is_file() else None

        def send_common_headers(self, content_type: str, length: int) -> None:
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(length))
            self.send_header("Accept-Ranges", "bytes")
            # Exports change in place, and the viewer development server may load from here
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Expose-Headers", "Content-Length, Content-Range")
            self.end_headers()

        def respond(self, body: bool) -> None:
            if urlsplit(self.path).path == "/":
                content = index_html.encode()
                self.send_response(HTTPStatus.OK)
                self.send_common_headers(CONTENT_TYPES[".html"], len(content))
                if body:
                    self.wfile.write(content)
                return
            path = self.resolve()
            if path is None:
                self.send_error(HTTPStatus.NOT_FOUND)
                return
            content_type = CONTENT_TYPES.get(path.suffix) or mimetypes.guess_type(path)[0] or "application/octet-stream"
            size = path.stat().st_size
            start, end = 0, size - 1
            header = self.headers.get("Range")
            if header:
                byte_range = parse_range(header, size)
                if byte_range is None:
                    self.send_response(HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE)
                    self.send_header("Content-Range", f"bytes */{size}")
                    self.send_common_headers(content_type, 0)
                    return
                start, end = byte_range
                self.send_response(HTTPStatus.PARTIAL_CONTENT)
                self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
            else:
                self.send_response(HTTPStatus.OK)
            self.send_common_headers(content_type, end - start + 1)
            if body:
                for chunk in read_chunks(path, start, end + 1):
                    self.wfile.write(chunk)

        def do_GET(self) -> None:
            self.respond(body=True)

        def do_HEAD(self) -> None:
            self.respond(body=False)

        def do_OPTIONS(self) -> None:
            self.send_response(HTTPStatus.NO_CONTENT)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Headers", "Range")
            self.end_headers()

        def log_message(self, format: str, *args: object) -> None:
            # Only errors, the viewer makes many range requests
            if args and str(args[1]).startswith(("4", "5")):
                super().log_message(format, *args)

    return Handler


def read_chunks(path: Path, start: int, stop: int) -> Iterator[bytes]:
    """Read the bytes from start up to stop of a file in chunks."""
    with path.open("rb") as f:
        f.seek(start)
        remaining = stop - start
        while remaining > 0:
            chunk = f.read(min(COPY_CHUNK, remaining))
            if not chunk:
                break
            remaining -= len(chunk)
            yield chunk


def serve(mounts: dict[str, Path], query: str, title: str, port: int, open_browser: bool) -> None:
    """Serve the viewer and the mounted directories on localhost until interrupted."""
    assert (APP_DIR / "viewer.js").is_file(), f"Viewer not built, run `pixi run viewer-build` to create {APP_DIR}"
    server = ThreadingHTTPServer(
        ("127.0.0.1", port), make_handler({"app": APP_DIR, **mounts}, INDEX_HTML.format(title=title))
    )
    url = f"http://localhost:{server.server_address[1]}/?{query}"
    print(f"Serving the model viewer at {url}, press Ctrl+C to stop")
    if open_browser:
        webbrowser.open(url)
    try:
        # Plain HTTP is fine on the loopback interface only
        server.serve_forever()  # NOSONAR
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def model_at(toml_path: Path, revision: str | None) -> tuple[Path, str]:
    """The TOML of a model in the working tree, or at a git revision, and a label: the revision or the model name."""
    if revision is None:
        return toml_path, toml_path.stem
    commit = resolve_commit(revision)
    if commit is None:
        sys.exit(f"{revision} is not a git revision")
    return checkout_model(toml_path, commit), revision


def main(argv: list[str] | None = None) -> None:
    """Command line entry point, see the module docstring."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("toml", type=Path, help="Ribasim model TOML to view")
    parser.add_argument("--rev", help="Git revision to view the model at, instead of the working tree")
    parser.add_argument(
        "--base", help="Ribasim model TOML, or git revision of the same model, to compare with to show the differences"
    )
    parser.add_argument("--port", type=int, default=8000, help="Port to serve on, 0 for any free port")
    parser.add_argument("--no-browser", action="store_true", help="Do not open the viewer in a web browser")
    args = parser.parse_args(argv)

    head_toml, head_label = model_at(args.toml, args.rev)
    head_dir = export_cached(head_toml)
    if args.base is None:
        serve(
            {"model": head_dir},
            "data=model/",
            f"{head_label if args.rev is None else f'{args.toml.stem} @ {args.rev}'} - Ribasim model viewer",
            args.port,
            not args.no_browser,
        )
        return
    base_path = Path(args.base)
    base_toml, base_label = (base_path, base_path.stem) if base_path.is_file() else model_at(args.toml, args.base)
    if not base_path.is_file() and args.rev is None:
        head_label = "working tree"
    base_dir = export_cached(base_toml)
    assert base_dir != head_dir, "The model and --base are the same model"
    diff_dir = diff_cached(base_toml, head_toml, base_dir, head_dir, (base_label, head_label))
    serve(
        {"head": head_dir, "base": base_dir, "diff": diff_dir},
        "data=head/&base=base/&diff=diff/",
        f"{args.toml.stem}: {base_label} → {head_label} - Ribasim model viewer",
        args.port,
        not args.no_browser,
    )


if __name__ == "__main__":
    main()
