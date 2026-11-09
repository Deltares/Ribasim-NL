"""Tests of the figures of the documentation."""

import re
from pathlib import Path

import docs_figures
import pytest

DOCS = Path(__file__).parents[1] / "docs"


def test_pages_link_to_figures_that_are_made():
    made = {output for group in docs_figures.FIGURES.values() for output in group.outputs}
    pattern = re.compile(r"doc-image/figures/([\w-]+)\.png")
    linked = {
        name
        for page in DOCS.rglob("*.qmd")
        if "reference" not in page.parts
        for name in pattern.findall(page.read_text(encoding="utf-8"))
    }
    assert linked, "no page links to a figure"
    assert linked <= made, f"pages link to figures that are not made: {sorted(linked - made)}"
    assert made <= linked, f"figures that no page uses: {sorted(made - linked)}"


def test_groups_have_outputs_and_data():
    assert docs_figures.FIGURES
    for name, group in docs_figures.FIGURES.items():
        assert group.outputs, name
        assert group.pull, name


def test_upload(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    uploaded = []

    class FakeMinio:
        def __init__(self, server: str, access_key: str, secret_key: str) -> None:
            assert (access_key, secret_key) == ("key", "secret")

        def fput_object(self, bucket: str, name: str, path: str, content_type: str, metadata: dict) -> None:
            uploaded.append((bucket, name, path, content_type))

    monkeypatch.setattr(docs_figures, "Minio", FakeMinio)
    monkeypatch.setattr(docs_figures, "OUT", tmp_path)
    monkeypatch.setattr(docs_figures.settings, "aws_access_key_id", "key")
    monkeypatch.setattr(docs_figures.settings, "aws_secret_access_key", "secret")
    docs_figures.upload(["a"])
    assert uploaded == [("ribasim-nl", "doc-image/figures/a.png", str(tmp_path / "a.png"), "image/png")]


def test_upload_needs_credentials(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(docs_figures.settings, "aws_access_key_id", "")
    with pytest.raises(OSError, match="AWS_ACCESS_KEY_ID"):
        docs_figures.upload(["a"])
