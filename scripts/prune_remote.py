"""Delete the data in the MinIO bucket that no kept commit uses: old DVC outputs and model viewer previews.

Every run pushes its outputs to the DVC remote and publishes its model to the viewer, so the bucket fills up.
This keeps the DVC data of:

- the last `--keep-main` runs of main: its last commits that changed `dvc.lock`;
- every version of the source data: the `.dvc` files of all commits of main, since these cannot be reproduced;
- all commits of open pull requests;
- the tips of GitHub branches that are not merged, whose tip is not the head of a merged pull request;
- the `dvc.lock` of git worktrees of this repository that changed in the last `--grace-days` days, such as a run
  that is pushed but not committed yet;
- everything uploaded in the last `--grace-days` days.

Of the model viewer previews below `doc-image/webmap/`, see `ribasim_nl.webmap_publish`, it keeps those of the kept
runs, of the latest published main, those published in the last `--grace-days` days, and the previews of open pull
requests and of the last `--keep-prs` pull requests, with the previews they are compared with. The records of pull
requests and differences that show a deleted preview are deleted with it, and so are the files that no kept preview
uses.

The outputs of an older commit of main can be reproduced from its source data, and a deleted preview can be published
again from its run with `pixi run publish-webmap --commit`. `dvc gc` does not fit: git worktrees share the cache of
the main checkout, which it would clean too, and it has no grace period.

Without `--delete` this only lists what it would delete, in `logs/prune-remote/`. With `--delete` it first checks that
DVC itself finds all data of the kept commits in what is kept.

Usage: pixi run prune-remote [--keep-main N] [--keep-prs N] [--grace-days DAYS] [--delete]
"""

import argparse
import csv
import json
import re
import subprocess
import sys
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import yaml
from minio import Minio
from minio.deleteobjects import DeleteObject
from ribasim_nl.settings import settings
from ribasim_nl.webmap_publish import (
    BUCKET_NAME,
    EXPORT_PATH,
    GITHUB_REPO,
    MINIO_SERVER,
    MODEL,
    STAGE,
    manifest_entries,
)
from ribasim_nl.webmap_publish import PREFIX as WEBMAP_PREFIX

ROOT = Path(__file__).resolve().parents[1]
MAIN = "main"
# The DVC remote `s3://ribasim-nl/dvc` of `.dvc/config`, which stores files by their md5 as `<2 chars>/<30 chars>`
DVC_PREFIX = "dvc/"
DVC_KEY = re.compile(r"dvc/files/md5/([0-9a-f]{2})/([0-9a-f]{30}(?:\.dir)?)")
MD5 = re.compile(r"[0-9a-f]{32}(?:\.dir)?")
_MODEL = re.escape(f"{WEBMAP_PREFIX}{MODEL}/")
SNAPSHOT_KEY = re.compile(rf"{_MODEL}snapshots/([0-9a-f]{{32}})/manifest\.json")
DIFF_KEY = re.compile(rf"{_MODEL}diffs/([0-9a-f]{{32}})-([0-9a-f]{{32}})\.json")
PR_KEY = re.compile(rf"{_MODEL}prs/([0-9]+)\.json")
MAIN_KEY = f"{WEBMAP_PREFIX}{MODEL}/main.json"
FILE_KEY = re.compile(rf"{re.escape(WEBMAP_PREFIX)}files/([0-9a-f]+\.[a-z]+)")
# Deleted in this order, so an interrupted run leaves no record that points to deleted objects, and no `.dir` whose
# files are deleted: DVC trusts the files of a `.dir` it finds on the remote, see `dvc_data.hashfile.status`.
DELETE_ORDER = ("pull request", "difference", "snapshot", "viewer file", "dvc directory", "dvc file")


@dataclass(frozen=True)
class Object:
    """An object in the bucket."""

    key: str
    size: int
    modified: datetime


@dataclass(frozen=True)
class Deletion:
    """An object to delete, and what kind of object it is, one of `DELETE_ORDER`."""

    kind: str
    obj: Object


def git(*args: str, cwd: Path = ROOT) -> str:
    """Run git and return its output."""
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout


def gh_json(*args: str) -> list[dict]:
    """Run a GitHub CLI command with JSON output and return the parsed list."""
    result = json.loads(subprocess.run(["gh", *args], cwd=ROOT, check=True, capture_output=True, text=True).stdout)
    assert isinstance(result, list), f"Expected a list from gh {args}"
    return result


def md5s(document: object) -> set[str]:
    """All values of `md5` keys in a parsed `dvc.lock` or `.dvc` file: the hashes of its outputs and dependencies."""
    found: set[str] = set()
    if isinstance(document, dict):
        for key, value in document.items():
            if key == "md5" and isinstance(value, str):
                assert MD5.fullmatch(value), f"Invalid md5 {value!r}"
                found.add(value)
            else:
                found |= md5s(value)
    elif isinstance(document, list):
        for item in document:
            found |= md5s(item)
    return found


def export_md5(lock: object) -> str | None:
    """The md5 of the `export_webmap` output in a parsed `dvc.lock`, the name of its snapshot, or None if it has none."""
    stages = lock.get("stages", {}) if isinstance(lock, dict) else {}
    outs = [out for out in stages.get(STAGE, {}).get("outs", []) if out.get("path") == EXPORT_PATH]
    assert len(outs) <= 1, f"{STAGE} has more than one output {EXPORT_PATH}"
    return outs[0]["md5"].removesuffix(".dir") if outs else None


def is_dvc_file(path: str) -> bool:
    """Whether a path in git is a `.dvc` file of source data."""
    return path.endswith(".dvc")


def is_lock(path: str) -> bool:
    """Whether a path in git is a `dvc.lock`."""
    return path.rsplit("/", 1)[-1] == "dvc.lock"


def dvc_blobs(commit: str, include: Callable[[str], bool]) -> dict[str, str]:
    """The blobs of the files of a commit that `include` selects, by path."""
    blobs = {}
    for entry in git("ls-tree", "-r", "-z", commit).split("\0"):
        if entry:
            meta, path = entry.split("\t", 1)
            _, kind, blob = meta.split()
            if kind == "blob" and include(path):
                blobs[path] = blob
    return blobs


def read_blobs(blobs: Iterable[str]) -> dict[str, str]:
    """The contents of git blobs, by blob id."""
    ids = sorted(set(blobs))
    if not ids:
        return {}
    out = subprocess.run(
        ["git", "cat-file", "--batch"], input="\n".join(ids).encode() + b"\n", cwd=ROOT, check=True, capture_output=True
    ).stdout
    contents = {}
    position = 0
    for blob in ids:
        end = out.index(b"\n", position)
        sha, kind, size = out[position:end].decode().split()
        assert sha == blob and kind == "blob", f"Unexpected git object {sha} {kind} for {blob}"
        start = end + 1
        contents[blob] = out[start : start + int(size)].decode()
        position = start + int(size) + 1
    return contents


@dataclass(frozen=True)
class Kept:
    """The commits whose data is kept."""

    main: str
    """The tip of main."""
    runs: list[str]
    """The last runs of main, newest first."""
    open_prs: dict[int, list[str]]
    """The commits of each open pull request that are not on main."""
    branch_tips: dict[str, str]
    """The tips of GitHub branches that are not merged."""
    main_commits: list[str]
    """All commits of main, whose source data is kept."""

    @property
    def full(self) -> set[str]:
        """The commits all of whose DVC data is kept."""
        pr_commits = {commit for commits in self.open_prs.values() for commit in commits}
        return {self.main, *self.runs} | pr_commits | set(self.branch_tips.values())


def fetch_kept(keep_main: int) -> Kept:
    """Fetch the kept commits from GitHub."""
    heads = {}
    for line in git("ls-remote", "--heads", "origin").splitlines():
        sha, ref = line.split("\t")
        heads[ref.removeprefix("refs/heads/")] = sha
    prs = gh_json(
        "pr", "list", "--repo", GITHUB_REPO, "--state", "all", "--limit", "10000", "--json", "number,state,headRefOid"
    )
    open_prs = {int(pr["number"]): pr["headRefOid"] for pr in prs if pr["state"] == "OPEN"}
    merged = {pr["headRefOid"] for pr in prs if pr["state"] == "MERGED"}
    git("fetch", "--quiet", "origin")
    if open_prs:
        git("fetch", "--quiet", "--no-write-fetch-head", "origin", *(f"refs/pull/{n}/head" for n in open_prs))
    main = heads[MAIN]
    runs = git("rev-list", "--first-parent", f"--max-count={keep_main}", main, "--", "dvc.lock").split()
    assert runs, f"No commit of {MAIN} changed dvc.lock"
    pr_commits = {n: git("rev-list", head, f"^{main}").split() for n, head in open_prs.items()}
    tips = {branch: sha for branch, sha in heads.items() if branch != MAIN and sha not in merged}
    return Kept(main, runs, pr_commits, tips, git("rev-list", main).split())


def recent_worktree_locks(cutoff: datetime) -> list[Path]:
    """The `dvc.lock` files of the git worktrees of this repository that changed since `cutoff`."""
    worktrees = [
        Path(line.removeprefix("worktree "))
        for line in git("worktree", "list", "--porcelain").splitlines()
        if line.startswith("worktree ")
    ]
    locks = [path / "dvc.lock" for path in worktrees]
    return [lock for lock in locks if lock.is_file() and datetime.fromtimestamp(lock.stat().st_mtime, UTC) >= cutoff]


def kept_hashes(kept: Kept) -> tuple[set[str], set[str]]:
    """The md5s that the kept commits refer to, and the md5s of the snapshots of their `export_webmap` output."""
    with ThreadPoolExecutor(16) as pool:
        full = list(pool.map(lambda commit: dvc_blobs(commit, lambda p: is_lock(p) or is_dvc_file(p)), kept.full))
        source = list(pool.map(lambda commit: dvc_blobs(commit, is_dvc_file), kept.main_commits))
    # Each version of a file is read once, though most commits share it
    paths = {blob: path for blobs in full + source for path, blob in blobs.items()}
    contents = read_blobs(paths)
    hashes: set[str] = set()
    exports: set[str] = set()
    for blob, path in paths.items():
        document = yaml.load(contents[blob], Loader=yaml.CSafeLoader)
        hashes |= md5s(document)
        if is_lock(path) and (md5 := export_md5(document)):
            exports.add(md5)
    return hashes, exports


def dvc_md5(key: str) -> str:
    """The md5 of an object of the DVC remote."""
    match = DVC_KEY.fullmatch(key)
    assert match, f"Unexpected object {key} in the DVC remote"
    return match[1] + match[2]


def with_dir_contents(hashes: set[str], read_dir: Callable[[str], Any], on_remote: set[str]) -> set[str]:
    """The md5s with those of the files of the `.dir` objects among them that are on the remote."""
    dirs = sorted(h for h in hashes if h.endswith(".dir") and h in on_remote)
    with ThreadPoolExecutor(16) as pool:
        listings = list(pool.map(read_dir, dirs))
    files = set()
    for listing in listings:
        for entry in listing:
            assert MD5.fullmatch(entry["md5"]) and not entry["md5"].endswith(".dir"), f"Invalid entry {entry}"
            files.add(entry["md5"])
    return hashes | files


def plan_dvc(objects: Iterable[Object], keep: set[str], cutoff: datetime) -> list[Deletion]:
    """The DVC objects to delete: those not in `keep`, uploaded before `cutoff`."""
    return [
        Deletion("dvc directory" if md5.endswith(".dir") else "dvc file", obj)
        for obj in objects
        if (md5 := dvc_md5(obj.key)) not in keep and obj.modified < cutoff
    ]


def plan_webmap(
    objects: list[Object],
    exports: set[str],
    open_prs: set[int],
    keep_prs: int,
    read_json: Callable[[str], Any],
    cutoff: datetime,
) -> list[Deletion]:
    """The objects of the model viewer to delete.

    Kept are the snapshots of `exports`, of the latest main, those published since `cutoff`, and those of the records
    of open pull requests and of the last `keep_prs` pull requests that have one.
    """
    snapshots: dict[str, Object] = {}
    diffs: dict[tuple[str, str], Object] = {}
    prs: dict[int, Object] = {}
    files: dict[str, Object] = {}
    for obj in objects:
        if match := SNAPSHOT_KEY.fullmatch(obj.key):
            snapshots[match[1]] = obj
        elif match := DIFF_KEY.fullmatch(obj.key):
            diffs[match[1], match[2]] = obj
        elif match := PR_KEY.fullmatch(obj.key):
            prs[int(match[1])] = obj
        elif match := FILE_KEY.fullmatch(obj.key):
            files[match[1]] = obj
        else:
            assert obj.key == MAIN_KEY, f"Unknown object {obj.key} of the model viewer"

    def pr_snapshots(number: int) -> set[str]:
        record = read_json(prs[number].key)
        assert record["pr"] == number, f"{prs[number].key} is of pull request {record['pr']}"
        return {record["head"]["snapshot"], record["base"]["snapshot"]}

    keep = set(exports) | {md5 for md5, obj in snapshots.items() if obj.modified >= cutoff}
    if any(obj.key == MAIN_KEY for obj in objects):
        keep.add(read_json(MAIN_KEY)["snapshot"])
    kept_prs = (open_prs & prs.keys()) | set(sorted(prs, reverse=True)[:keep_prs])
    keep |= {md5 for number in kept_prs for md5 in pr_snapshots(number)}
    deletions = [Deletion("snapshot", obj) for md5, obj in snapshots.items() if md5 not in keep]
    deletions += [Deletion("difference", obj) for pair, obj in diffs.items() if not set(pair) <= keep]
    deletions += [
        Deletion("pull request", obj)
        for number, obj in prs.items()
        if number not in kept_prs and not pr_snapshots(number) <= keep
    ]
    used = {
        entry["path"] for md5 in keep & snapshots.keys() for entry in manifest_entries(read_json(snapshots[md5].key))
    }
    deletions += [
        Deletion("viewer file", obj) for name, obj in files.items() if name not in used and obj.modified < cutoff
    ]
    return deletions


class Bucket:
    """The MinIO bucket."""

    def __init__(self) -> None:
        if not settings.aws_access_key_id or not settings.aws_secret_access_key:
            raise OSError("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set in the environment or .env file.")
        self.client = Minio(
            MINIO_SERVER, access_key=settings.aws_access_key_id, secret_key=settings.aws_secret_access_key
        )

    def objects(self, prefix: str) -> list[Object]:
        """The objects below a prefix."""
        objects = []
        for obj in self.client.list_objects(BUCKET_NAME, prefix=prefix, recursive=True):
            assert obj.object_name and obj.size is not None and obj.last_modified, f"Incomplete listing of {obj}"
            objects.append(Object(obj.object_name, obj.size, obj.last_modified))
        return objects

    def read_json(self, key: str) -> Any:
        """Read a JSON object."""
        response = self.client.get_object(BUCKET_NAME, key)
        try:
            return json.loads(response.read())
        finally:
            response.close()
            response.release_conn()

    def delete(self, keys: list[str]) -> None:
        """Delete objects in batches, stopping at the first error."""
        for start in range(0, len(keys), 1000):
            batch = keys[start : start + 1000]
            errors = list(self.client.remove_objects(BUCKET_NAME, [DeleteObject(key) for key in batch]))
            assert not errors, f"Failed to delete {[(e.name, e.code, e.message) for e in errors[:5]]}"
            print(f"Deleted {start + len(batch)} of {len(keys)} objects")


def check_with_dvc(commits: Iterable[str], keep: set[str], on_remote: set[str]) -> None:
    """Assert that the data that DVC itself finds for each commit, and for the workspace, is kept."""
    from dvc.repo import Repo

    repo = Repo(str(ROOT))
    commits = sorted(set(commits))
    for number, commit in enumerate(commits, 1):
        if number % 10 == 0:
            print(f"Checked {number} of {len(commits)} commits with DVC", flush=True)
        try:
            used = repo.used_objs(revs=[commit], num=1, force=True, jobs=8)
        except Exception as e:
            # Old commits can have a `dvc.yaml` that this DVC cannot read, their md5s are kept all the same
            print(f"DVC cannot read commit {commit[:10]}, not checked: {type(e).__name__}: {str(e)[:100]}")
            continue
        hashes = {hash_info.value for ids in used.values() for hash_info in ids}
        lost = (hashes & on_remote) - keep
        assert not lost, f"DVC uses {len(lost)} objects of {commit} or the workspace that would be deleted"
    print(f"DVC finds all data of {len(commits)} kept commits in what is kept")


def gigabytes(objects: Iterable[Object]) -> float:
    """The total size of objects in GB."""
    return sum(obj.size for obj in objects) / 1e9


def main() -> int:
    """Plan the deletions, and with `--delete` carry them out; return the exit code."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--keep-main", type=int, default=30, help="the number of last runs of main to keep")
    parser.add_argument("--keep-prs", type=int, default=30, help="the number of last pull request previews to keep")
    parser.add_argument("--grace-days", type=float, default=3.0, help="keep what was uploaded in these last days")
    parser.add_argument("--delete", action="store_true", help="delete, instead of only listing what to delete")
    args = parser.parse_args()
    assert args.keep_main >= 1, "Keep at least the last run of main"
    assert args.keep_prs >= 0, "The number of pull request previews to keep cannot be negative"
    assert args.grace_days >= 1, "Keep at least what was uploaded in the last day, it may not be committed yet"
    cutoff = datetime.now(UTC) - timedelta(days=args.grace_days)

    kept = fetch_kept(args.keep_main)
    oldest = git("show", "--no-patch", "--format=%cs", kept.runs[-1]).strip()
    print(
        f"Keeping the data of the last {len(kept.runs)} runs of main, back to {oldest}, of "
        f"{sum(map(len, kept.open_prs.values()))} commits of {len(kept.open_prs)} open pull requests, of "
        f"{len(kept.branch_tips)} unmerged branches, and the source data of {len(kept.main_commits)} commits of main"
    )
    hashes, exports = kept_hashes(kept)
    for lock in recent_worktree_locks(cutoff):
        print(f"Keeping the data of {lock}, which changed in the last {args.grace_days:g} days")
        document = yaml.load(lock.read_text(), Loader=yaml.CSafeLoader)
        hashes |= md5s(document)
        if md5 := export_md5(document):
            exports.add(md5)

    bucket = Bucket()
    dvc_objects = bucket.objects(DVC_PREFIX)
    on_remote = {dvc_md5(obj.key) for obj in dvc_objects}
    keep = with_dir_contents(
        hashes, lambda md5: bucket.read_json(f"{DVC_PREFIX}files/md5/{md5[:2]}/{md5[2:]}"), on_remote
    )
    deletions = plan_dvc(dvc_objects, keep, cutoff)
    webmap_objects = bucket.objects(WEBMAP_PREFIX)
    deletions += plan_webmap(webmap_objects, exports, set(kept.open_prs), args.keep_prs, bucket.read_json, cutoff)
    deletions.sort(key=lambda d: (DELETE_ORDER.index(d.kind), d.obj.key))

    for name, objects in (("DVC remote", dvc_objects), ("model viewer", webmap_objects)):
        print(f"{name}: {len(objects)} objects, {gigabytes(objects):.1f} GB")
    for kind in DELETE_ORDER:
        objects = [d.obj for d in deletions if d.kind == kind]
        print(f"  delete {len(objects):5d} {kind} objects, {gigabytes(objects):6.1f} GB")
    print(f"Deleting {len(deletions)} objects, {gigabytes(d.obj for d in deletions):.1f} GB")
    log = ROOT / "logs/prune-remote" / f"{datetime.now():%Y%m%d-%H%M%S}.csv"
    log.parent.mkdir(parents=True, exist_ok=True)
    with log.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["kind", "key", "size", "modified"])
        writer.writerows((d.kind, d.obj.key, d.obj.size, d.obj.modified.isoformat()) for d in deletions)
    print(f"Listed them in {log}")

    if args.delete:
        check_with_dvc(kept.full, keep, on_remote)
        bucket.delete([d.obj.key for d in deletions])
    else:
        print("Nothing is deleted, use --delete to delete them")
    return 0


if __name__ == "__main__":
    sys.exit(main())
