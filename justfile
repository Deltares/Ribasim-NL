# Requires https://www.nushell.sh/, cross-platform shell
set shell := ["nu", "-c"]

default:
  just --list

install:
    uv sync
    uv tool install rust-just
    uv tool install prek
    uv tool install ruff
    uv tool install dvc[s3,webdav]
    uv run dvc pull
    uv run prek install -f

repro:
  uv run dvc repro
  uv run prek run

# run git hooks on all files
check:
  uv run prek run --all-files

# like check but skips the slower DVC checks
pycheck:
  uv run prek run --all-files --skip dvc-pre-commit --skip dvc-pre-push --skip dvc-post-checkout

[working-directory: 'docs']
docs:
  uv run quartodoc build
  uv run quarto preview

# upload system diagrams to s3.deltares.nl for use in documentation
upload-doc-images:
  uv run python scripts/upload_doc_images.py
