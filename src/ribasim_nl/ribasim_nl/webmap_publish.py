"""Publish the `export_webmap` output to MinIO for the model viewer on the documentation site.

    pixi run publish-webmap            # the model of the pull request of this branch, compared with its base
    pixi run publish-webmap --pr 123   # the model of the working tree as that pull request
    pixi run publish-webmap --main     # only the model of origin/main

Everything is uploaded below the anonymously readable `doc-image/webmap/` prefix:

- `files/<hash>.<ext>`: the exported files, named by their content hash, so files that did not change are
  uploaded once and shared by all models.
- `lhm_coupled/snapshots/<md5>/manifest.json`: the manifest of an export, with the paths of the files in
  `files/`. The snapshot is named by the DVC hash of the `export_webmap` output in `dvc.lock`, which is how the
  viewer finds the model of `main`: it reads `dvc.lock` from GitHub.
- `lhm_coupled/main.json`: the snapshot of the latest published `main`, for when the snapshot of the current
  `main` is not published.
- `lhm_coupled/diffs/<base md5>-<head md5>.json`: the differences between two snapshots, see
  `ribasim_nl.webmap_diff`.
- `lhm_coupled/prs/<number>.json`: the snapshots and differences of a pull request, which the viewer shows for
  `?pr=<number>`. A pull request is compared with `main`, or in a stack of pull requests with the pull request
  of its base branch, so it only shows its own changes. That pull request must be published first.

DVC does not cache the `export_webmap` output, so this is the only copy that is kept. The export of another commit,
to compare with, is downloaded from its published snapshot. If it is not published, the model is exported again
from its run, which DVC does keep.

This needs the following environment variables to be set in `.env` (Deltares only):
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
They need to have write permission. A pull request is found and commented on with the GitHub CLI `gh`.
"""

import argparse
import copy
import io
import json
import re
import subprocess
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import yaml
from dvc.repo import Repo
from minio import Minio
from minio.error import S3Error

from ribasim_nl.settings import settings
from ribasim_nl.webmap import export_webmap, file_hash
from ribasim_nl.webmap_local import CACHE_DIR, REPO_DIR, checkout_model, diff_cached, resolve_commit, waterboards

MINIO_SERVER = "s3.deltares.nl"
BUCKET_NAME = "ribasim-nl"
PREFIX = "doc-image/webmap/"
MODEL = "lhm_coupled"
STAGE = "export_webmap"
EXPORT_PATH = "data/Rijkswaterstaat/webmap/lhm_coupled"
RUN_TOML = "data/Rijkswaterstaat/runs/lhm_coupled/lhm_coupled.toml"
MAIN = "origin/main"
VIEWER_URL = "https://ribasim.nl/viewer"
GITHUB_REPO = "Deltares/Ribasim-NL"
# Identifies the comment with the viewer link on a pull request, so it is updated instead of added again
COMMENT_MARKER = "<!-- ribasim-nl-webmap-preview -->"
HASH_KEY = "x-amz-meta-content-hash"
CONTENT_TYPES = {
    ".parquet": "application/vnd.apache.parquet",
    ".pmtiles": "application/vnd.pmtiles",
    ".geojson": "application/geo+json",
    ".json": "application/json",
}
IMMUTABLE = "public, max-age=31536000, immutable"
COMMIT = re.compile(r"[0-9a-f]{40}")
# The name of a file in the `files/` store: its content hash and extension
STORED_FILE = re.compile(r"[0-9a-f]+\.[a-z]+")
BRANCH = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/-]*")


def export_hash(lock: dict) -> str:
    """The DVC hash of the `export_webmap` output in a parsed `dvc.lock`, without the `.dir` suffix."""
    outs = lock["stages"][STAGE]["outs"]
    (out,) = (out for out in outs if out["path"] == EXPORT_PATH)
    md5 = out["md5"].removesuffix(".dir")
    assert len(md5) == 32 and all(c in "0123456789abcdef" for c in md5), f"Invalid md5 {out['md5']}"
    return md5


def manifest_entries(manifest: dict) -> list[dict]:
    """All file entries of a manifest, see `ribasim_nl.webmap.export_webmap`."""
    entries = list(manifest["files"].values()) + manifest["tables"]
    if results := manifest.get("results"):
        entries += [results[name][key] for name in ("basin", "flow") for key in ("by_id", "by_time")]
        if "control" in results:
            entries.append(results["control"])
    return entries


def store_manifest(manifest: dict) -> tuple[dict, dict[str, str]]:
    """A copy of a manifest with the paths of its files in the shared `files/` store, and the original paths.

    Files are named by their content hash, so the second result maps each new path to the exported file.
    """
    stored = copy.deepcopy(manifest)
    sources: dict[str, str] = {}
    for entry in manifest_entries(stored):
        suffix = Path(entry["path"]).suffix
        assert suffix in CONTENT_TYPES, f"Unknown file type {entry['path']}"
        name = f"{entry['hash']}{suffix}"
        # Files with the same content are stored once
        sources.setdefault(name, entry["path"])
        entry["path"] = name
    return stored, sources


def diff_counts(diff: dict) -> dict[str, Counter]:
    """The number of differing nodes and links by status, and of features with differing results."""
    counts = {kind: Counter(feature["status"] for feature in diff[kind].values()) for kind in ("nodes", "links")}
    if diff.get("results"):
        counts["results"] = Counter({name: len(diff["results"][name]["differences"]) for name in ("basin", "flow")})
    return counts


def comment_body(number: int, diff: dict, base_commit: str, head_commit: str, base_pr: int | None = None) -> str:
    """The pull request comment with the viewer link and a summary of the differences with its base.

    The base is `main`, or pull request `base_pr` in a stack.
    """
    counts = diff_counts(diff)
    base = "`main`" if base_pr is None else f"#{base_pr}"
    lines = [
        COMMENT_MARKER,
        f"**[View the model of this pull request]({VIEWER_URL}?pr={number})**, with the differences from {base}.",
        "",
        "| | added | removed | changed |",
        "|:--|--:|--:|--:|",
    ]
    statuses = ("added", "removed", "changed")
    lines += [f"| {kind} | {' | '.join(str(counts[kind][s]) for s in statuses)} |" for kind in ("nodes", "links")]
    if "results" in counts:
        lines += [
            "",
            f"Results differ for {counts['results']['basin']} Basins and {counts['results']['flow']} flow links.",
        ]
    elif diff.get("results_note"):
        lines += ["", f"Results are not compared: {diff['results_note']}"]
    if diff["config"]:
        lines += ["", "Changed settings: " + ", ".join(f"`{setting['key']}`" for setting in diff["config"])]
    lines += ["", f"Compared `{head_commit[:7]}` with {base} at `{base_commit[:7]}`, published {now()}."]
    return "\n".join(lines)


def now() -> str:
    """The current time in UTC, to the minute."""
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")


def commit_hash(commit: str) -> str:
    """Assert a commit is a full commit hash, so it is never read as an option; return it."""
    assert COMMIT.fullmatch(commit), f"Invalid commit {commit!r}"
    return commit


def pr_number(number: int) -> int:
    """Assert a pull request number is valid; return it."""
    assert isinstance(number, int) and number > 0, f"Invalid pull request number {number!r}"
    return number


def lock_at(commit: str | None) -> dict:
    """The parsed `dvc.lock` of the working tree, or at a commit."""
    if commit is None:
        return yaml.safe_load((REPO_DIR / "dvc.lock").read_text())
    show = subprocess.run(
        ["git", "show", f"{commit_hash(commit)}:dvc.lock"], check=True, capture_output=True, text=True, cwd=REPO_DIR
    )
    return yaml.safe_load(show.stdout)


def snapshot_name(md5: str) -> str:
    """The name of the manifest of snapshot `md5`."""
    return f"{MODEL}/snapshots/{md5}/manifest.json"


class Store:
    """The objects below `PREFIX` in the MinIO bucket."""

    def __init__(self) -> None:
        if not settings.aws_access_key_id or not settings.aws_secret_access_key:
            raise OSError("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set in the environment or .env file.")
        self.client = Minio(
            MINIO_SERVER, access_key=settings.aws_access_key_id, secret_key=settings.aws_secret_access_key
        )

    def metadata(self, name: str) -> dict[str, str] | None:
        """The metadata of an object, or None if it does not exist."""
        try:
            stat = self.client.stat_object(BUCKET_NAME, PREFIX + name)
        except S3Error as e:
            if e.code == "NoSuchKey":
                return None
            raise
        return dict(stat.metadata or {})

    def is_published(self, md5: str) -> bool:
        """Whether snapshot `md5` is published."""
        return self.metadata(snapshot_name(md5)) is not None

    def download_snapshot(self, md5: str, target: Path) -> None:
        """Download a published snapshot into a directory, as an export with the files named as in `files/`."""
        response = self.client.get_object(BUCKET_NAME, PREFIX + snapshot_name(md5))
        try:
            manifest = json.loads(response.read())
        finally:
            response.close()
            response.release_conn()
        hashes = {entry["path"]: entry["hash"] for entry in manifest_entries(manifest)}
        for name, content_hash in hashes.items():
            assert STORED_FILE.fullmatch(name), f"Invalid file {name} in snapshot {md5}"
            path = target / name
            self.client.fget_object(BUCKET_NAME, PREFIX + f"files/{name}", str(path))
            assert file_hash(path) == content_hash, f"{path} does not match its hash in snapshot {md5}"
        # The manifest is written last, so it never points to files that are not downloaded yet
        (target / "manifest.json").write_text(json.dumps(manifest, indent=1))

    def put_file(self, name: str, path: Path, content_hash: str) -> None:
        """Upload a file named by its content hash, unless it is already there."""
        if (self.metadata(name) or {}).get(HASH_KEY) == content_hash:
            return
        print(f"Uploading {name} ({path.stat().st_size / 1e6:.1f} MB) from {path}")
        self.client.fput_object(
            BUCKET_NAME,
            PREFIX + name,
            str(path),
            content_type=CONTENT_TYPES[path.suffix],
            metadata={"Cache-Control": IMMUTABLE, HASH_KEY: content_hash},
        )

    def put_json(self, name: str, data: dict, cache_control: str) -> None:
        """Upload a JSON object."""
        body = json.dumps(data, indent=1).encode()
        self.client.put_object(
            BUCKET_NAME,
            PREFIX + name,
            io.BytesIO(body),
            len(body),
            content_type=CONTENT_TYPES[".json"],
            metadata={"Cache-Control": cache_control},
        )

    def publish_snapshot(self, export_dir: Path, md5: str) -> str:
        """Upload the files and manifest of an export as snapshot `md5`, unless it exists; return its name."""
        name = snapshot_name(md5)
        if self.is_published(md5):
            print(f"Snapshot {name} is already published")
            return name
        manifest = json.loads((export_dir / "manifest.json").read_text())
        stored, sources = store_manifest(manifest)
        hashes = {entry["path"]: entry["hash"] for entry in manifest_entries(stored)}
        for stored_name, path in sources.items():
            source = export_dir / path
            assert source.is_file(), f"Missing exported file {source}"
            self.put_file(f"files/{stored_name}", source, hashes[stored_name])
        # The manifest is uploaded last, so it never points to files that are not uploaded yet
        self.put_json(name, stored, IMMUTABLE)
        print(f"Published snapshot {name}")
        return name


def checkout_export(store: Store, commit: str, md5: str, cache_dir: Path = CACHE_DIR) -> Path:
    """Get the `export_webmap` output at a commit, whose hash in `dvc.lock` is `md5`; return its directory.

    It is downloaded from its published snapshot. If that is not published, the model is exported again from
    its run in DVC, which need not be identical to the original export byte for byte.
    """
    target = cache_dir / "exports" / md5
    complete = target.with_name(f"{md5}.complete")
    if complete.is_file():
        print(f"Using the cached export of {EXPORT_PATH} at {commit} in {target}")
        return target
    if store.is_published(md5):
        print(f"Downloading snapshot {md5} of {EXPORT_PATH} at {commit} into {target}")
        target.mkdir(parents=True, exist_ok=True)
        store.download_snapshot(md5, target)
    else:
        print(f"Snapshot {md5} of {EXPORT_PATH} at {commit} is not published, exporting its run again")
        export_webmap(checkout_model(REPO_DIR / RUN_TOML, commit, cache_dir=cache_dir), waterboards(), target)
    complete.touch()
    return target


def publish_commit(store: Store, commit: str) -> str:
    """Publish the model of a commit as its snapshot, unless it is published; return the snapshot md5."""
    md5 = export_hash(lock_at(commit))
    if store.is_published(md5):
        print(f"Snapshot {md5} of {commit} is already published")
    else:
        store.publish_snapshot(checkout_export(store, commit, md5), md5)
    return md5


def branch_name(branch: str) -> str:
    """Assert a branch name is valid, so it is never read as an option; return it."""
    assert BRANCH.fullmatch(branch) and ".." not in branch, f"Invalid branch {branch!r}"
    return branch


def fetch_branch(branch: str) -> str:
    """Fetch a branch from GitHub and return its commit."""
    refspec = f"refs/heads/{branch_name(branch)}:refs/remotes/origin/{branch}"
    subprocess.run(
        ["git", "fetch", "--quiet", "origin", refspec], check=True, capture_output=True, text=True, cwd=REPO_DIR
    )
    commit = resolve_commit(f"origin/{branch}")
    assert commit is not None, f"origin/{branch} not found"
    return commit_hash(commit)


def fetch_main() -> str:
    """Fetch main from GitHub and return its commit."""
    commit = fetch_branch(MAIN.removeprefix("origin/"))
    assert commit == resolve_commit(MAIN)
    return commit


def gh_json(*args: str) -> object:
    """Run a GitHub CLI command with JSON output and return the parsed result."""
    result = subprocess.run(["gh", *args], check=True, capture_output=True, text=True, cwd=REPO_DIR)
    return json.loads(result.stdout)


def stack_base(number: int) -> tuple[str, int] | None:
    """The base branch of a pull request and the open pull request of that branch, or None if it is based on main.

    In a stack of pull requests, a pull request is based on the branch of the one below it.
    """
    pr = gh_json("pr", "view", str(pr_number(number)), "--repo", GITHUB_REPO, "--json", "baseRefName")
    assert isinstance(pr, dict)
    branch = branch_name(pr["baseRefName"])
    if branch == MAIN.removeprefix("origin/"):
        return None
    prs = gh_json("pr", "list", "--repo", GITHUB_REPO, "--head", branch, "--state", "open", "--json", "number")
    assert isinstance(prs, list)
    assert len(prs) == 1, f"Expected one open pull request of base branch {branch} of #{number}, found {prs}"
    return branch, pr_number(prs[0]["number"])


def publish_main(store: Store, commit: str) -> str:
    """Publish the model of a commit of main as its snapshot and as the latest main; return the snapshot md5."""
    md5 = publish_commit(store, commit)
    store.put_json(f"{MODEL}/main.json", {"snapshot": md5, "commit": commit, "published": now()}, "no-cache")
    return md5


def check_working_tree() -> str:
    """Assert the `export_webmap` output in the working tree is up to date; return its md5 in `dvc.lock`."""
    changed = Repo(str(REPO_DIR)).status(targets=[STAGE])
    assert not changed, f"{STAGE} is not up to date, reproduce it first: {changed}"
    md5 = export_hash(lock_at(None))
    if subprocess.run(
        ["git", "status", "--porcelain", "--", "dvc.lock"], check=True, capture_output=True, text=True, cwd=REPO_DIR
    ).stdout:
        print("dvc.lock has uncommitted changes, commit and push them, so main shows this model once merged")
    return md5


def find_pr() -> int:
    """The number of the pull request of the current branch."""
    result = subprocess.run(
        ["gh", "pr", "view", "--json", "number"], capture_output=True, text=True, cwd=REPO_DIR, check=False
    )
    if result.returncode != 0:
        sys.exit(f"No pull request found for this branch, open one first, or use --pr or --main.\n{result.stderr}")
    return pr_number(int(json.loads(result.stdout)["number"]))


def upsert_comment(number: int, body: str) -> None:
    """Update the viewer comment on a pull request, or add it if there is none."""
    comments_url = f"repos/{GITHUB_REPO}/issues/{pr_number(number)}/comments"
    pages = subprocess.run(
        ["gh", "api", "--paginate", "--slurp", comments_url], check=True, capture_output=True, text=True, cwd=REPO_DIR
    ).stdout
    existing = [comment for page in json.loads(pages) for comment in page if COMMENT_MARKER in comment["body"]]
    # The body is passed as JSON on stdin, rather than as an argument
    payload = json.dumps({"body": body})
    if existing:
        comment_url = f"repos/{GITHUB_REPO}/issues/comments/{int(existing[0]['id'])}"
        subprocess.run(
            ["gh", "api", "--method", "PATCH", comment_url, "--input", "-"],
            input=payload,
            check=True,
            capture_output=True,
            text=True,
            cwd=REPO_DIR,
        )
        print(f"Updated {existing[0]['html_url']}")
    else:
        subprocess.run(
            ["gh", "api", "--method", "POST", comments_url, "--input", "-"],
            input=payload,
            check=True,
            capture_output=True,
            text=True,
            cwd=REPO_DIR,
        )
        print(f"Commented on pull request #{number}")


def publish_pr(store: Store, number: int) -> None:
    """Publish the model of the working tree for a pull request, with its differences from its base.

    The base is main, or in a stack the pull request of the base branch.
    """
    head_md5 = check_working_tree()
    head_commit = commit_hash(
        subprocess.run(
            ["git", "rev-parse", "HEAD"], check=True, capture_output=True, text=True, cwd=REPO_DIR
        ).stdout.strip()
    )
    stack = stack_base(number)
    if stack is None:
        base_pr = None
        base_commit = fetch_main()
        base_md5 = publish_main(store, base_commit)
        base_label = "main"
    else:
        branch, base_pr = stack
        base_commit = fetch_branch(branch)
        base_md5 = publish_commit(store, base_commit)
        base_label = f"#{base_pr}"
    head_dir = REPO_DIR / EXPORT_PATH
    store.publish_snapshot(head_dir, head_md5)

    base_toml = checkout_model(REPO_DIR / RUN_TOML, base_commit)
    labels = (f"{base_label} @ {base_commit[:7]}", f"#{number} @ {head_commit[:7]}")
    base_dir = checkout_export(store, base_commit, base_md5)
    diff_dir = diff_cached(base_toml, REPO_DIR / RUN_TOML, base_dir, head_dir, labels)
    diff = json.loads((diff_dir / "diff.json").read_text())
    diff_name = f"{MODEL}/diffs/{base_md5}-{head_md5}.json"
    # The labels name the commit, so the differences are uploaded again with each publish
    store.put_json(diff_name, diff, "no-cache")

    pr = {
        "pr": number,
        "head": {"snapshot": head_md5, "commit": head_commit},
        "base": {"snapshot": base_md5, "commit": base_commit} | ({} if base_pr is None else {"pr": base_pr}),
        "diff": diff_name.removeprefix(f"{MODEL}/"),
        "published": now(),
    }
    store.put_json(f"{MODEL}/prs/{number}.json", pr, "no-cache")
    print(f"Published {VIEWER_URL}?pr={number}")
    upsert_comment(number, comment_body(number, diff, base_commit, head_commit, base_pr))


def main(argv: list[str] | None = None) -> None:
    """Command line entry point, see the module docstring."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--pr", type=int, help="pull request to publish the model of the working tree for")
    group.add_argument("--main", action="store_true", help="only publish the model of origin/main")
    args = parser.parse_args(argv)
    store = Store()
    if args.main:
        publish_main(store, fetch_main())
        print(f"Published {VIEWER_URL}")
    else:
        publish_pr(store, pr_number(args.pr) if args.pr is not None else find_pr())


if __name__ == "__main__":
    main()
