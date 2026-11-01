"""Tests for the catch-up runs of scripts/repro_main.py and their timer in scripts/install_repro_main.py"""

import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
from install_repro_main import service_unit, timer_unit
from repro_main import (
    BRANCH_PATTERN,
    LOCKED,
    MARKER,
    branch_name,
    changed_files,
    check_no_secrets,
    check_private,
    check_worktree,
    repro_command,
    superseded,
)


def git(repo: Path, *args: str) -> str:
    return subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True, text=True).stdout.strip()


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    """A repository with the locked files committed, and `origin/main` at that commit."""
    git(tmp_path, "init", "--quiet", "--initial-branch=main")
    git(tmp_path, "config", "user.email", "test@example.com")
    git(tmp_path, "config", "user.name", "Test")
    git(tmp_path, "config", "commit.gpgsign", "false")
    for path in LOCKED:
        (tmp_path / path).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / path).write_text("run 1\n")
    git(tmp_path, "add", ".")
    git(tmp_path, "commit", "--quiet", "--no-verify", "--message", "run 1")
    git(tmp_path, "update-ref", "refs/remotes/origin/main", "HEAD")
    return tmp_path


def commit(repo: Path, path: str, text: str) -> str:
    """Commit a file, move `origin/main` to it, and return the commit."""
    (repo / path).write_text(text)
    git(repo, "add", path)
    git(repo, "commit", "--quiet", "--no-verify", "--message", f"change {path}")
    git(repo, "update-ref", "refs/remotes/origin/main", "HEAD")
    return git(repo, "rev-parse", "HEAD")


def test_branch_name():
    name = branch_name(datetime(2026, 10, 28, 1, 13))
    assert name == "repro-main-20261028-0113"
    assert BRANCH_PATTERN.fullmatch(name)
    assert not BRANCH_PATTERN.fullmatch("repro-main")
    assert not BRANCH_PATTERN.fullmatch("main")


def test_repro_command():
    assert repro_command(full=False) == ["pixi", "run", "repro-local", "--incremental", "--push", "--no-publish"]
    assert repro_command(full=True) == ["pixi", "run", "repro-local", "--push", "--no-publish"]


def test_superseded(repo: Path):
    base = git(repo, "rev-parse", "HEAD")
    assert not superseded(repo, base)
    # Code changes on main do not supersede a run, a new run on main does
    commit(repo, "code.py", "x = 1\n")
    assert not superseded(repo, base)
    commit(repo, "dvc.lock", "run 2\n")
    assert superseded(repo, base)


def test_superseded_by_metrics(repo: Path):
    base = git(repo, "rev-parse", "HEAD")
    commit(repo, LOCKED[1], "run 2\n")
    assert superseded(repo, base)


def test_check_worktree(repo: Path):
    with pytest.raises(AssertionError, match="not the worktree"):
        check_worktree(repo)
    (repo / ".git" / MARKER).write_text("")
    assert check_worktree(repo) == git(repo, "rev-parse", "HEAD")

    (repo / "dvc.lock").write_text("changed\n")
    assert changed_files(repo) == ["dvc.lock"]
    with pytest.raises(AssertionError, match="has changes"):
        check_worktree(repo)
    git(repo, "checkout", "--", "dvc.lock")

    git(repo, "update-ref", "refs/remotes/origin/main", "HEAD~0")
    commit(repo, "code.py", "x = 1\n")
    git(repo, "update-ref", "refs/remotes/origin/main", "HEAD~1")
    with pytest.raises(AssertionError, match="is not origin/main"):
        check_worktree(repo)


def test_check_private(tmp_path: Path):
    path = tmp_path / ".env"
    check_private(path)  # a missing file is fine
    path.write_text("SECRET=x\n")
    path.chmod(0o644)
    with pytest.raises(AssertionError, match="accessible by others"):
        check_private(path)
    path.chmod(0o600)
    check_private(path)


def test_check_no_secrets(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "s3cr3t-value-1234")
    monkeypatch.delenv("GH_TOKEN", raising=False)
    check_no_secrets("The KGE of Lobith is 0.99")
    with pytest.raises(AssertionError, match="AWS_SECRET_ACCESS_KEY"):
        check_no_secrets("key s3cr3t-value-1234 leaked")


def test_service_unit(tmp_path: Path):
    git_dir = tmp_path / ".git/worktrees/repro-main"
    unit = service_unit(tmp_path, git_dir, "/usr/bin")
    lines = unit.splitlines()
    marker = lines.index(f"ExecStartPre=/usr/bin/test -f {git_dir / MARKER}")
    checkout = next(i for i, line in enumerate(lines) if "checkout --quiet --force" in line)
    # The worktree is only reset after checking that it is the one for this
    assert marker < checkout
    assert "NoNewPrivileges=yes" in lines
    assert f"WorkingDirectory={tmp_path}" in lines


def test_timer_unit():
    assert "OnCalendar=*-*-* 01:13:00" in timer_unit("01:13").splitlines()
