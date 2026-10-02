"""
Publish the `export_webmap` output to MinIO for the documentation model viewer.

Files are uploaded to the anonymously readable `doc-image/webmap/<model>/` prefix.
The viewer requests them as `<path>?v=<hash>`, so they are cached indefinitely, except
`manifest.json` which points to the current hashes and is revalidated on every load.

This needs the following environment variables to be set in `.env` (Deltares only):
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
They need to have write permission.
"""

import json

from minio import Minio
from minio.error import S3Error
from ribasim_nl.settings import settings

MINIO_SERVER = "s3.deltares.nl"
BUCKET_NAME = "ribasim-nl"
MODEL = "lhm_coupled"
HASH_KEY = "x-amz-meta-content-hash"
CONTENT_TYPES = {
    ".parquet": "application/vnd.apache.parquet",
    ".pmtiles": "application/vnd.pmtiles",
    ".geojson": "application/geo+json",
    ".json": "application/json",
}

if not settings.aws_access_key_id or not settings.aws_secret_access_key:
    raise OSError("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set in the environment or .env file.")

source_dir = settings.ribasim_nl_data_dir / "Rijkswaterstaat/webmap" / MODEL
prefix = f"doc-image/webmap/{MODEL}/"
manifest_path = source_dir / "manifest.json"
manifest = json.loads(manifest_path.read_text())
entries = list(manifest["files"].values()) + manifest["tables"]

client = Minio(MINIO_SERVER, access_key=settings.aws_access_key_id, secret_key=settings.aws_secret_access_key)


def remote_hash(name: str) -> str | None:
    """Return the content hash stored with an object, or None if it does not exist."""
    try:
        stat = client.stat_object(BUCKET_NAME, name)
    except S3Error as e:
        if e.code == "NoSuchKey":
            return None
        raise
    return (stat.metadata or {}).get(HASH_KEY)


for entry in entries:
    path = source_dir / entry["path"]
    assert path.is_file(), f"Missing exported file {path}, run `dvc repro export_webmap` first"
    name = prefix + entry["path"]
    if remote_hash(name) == entry["hash"]:
        print(f"Unchanged {name}")
        continue
    print(f"Uploading {name} ({entry['bytes'] / 1e6:.1f} MB)")
    client.fput_object(
        BUCKET_NAME,
        name,
        str(path),
        content_type=CONTENT_TYPES[path.suffix],
        metadata={"Cache-Control": "public, max-age=31536000, immutable", HASH_KEY: entry["hash"]},
    )

client.fput_object(
    BUCKET_NAME,
    prefix + "manifest.json",
    str(manifest_path),
    content_type=CONTENT_TYPES[".json"],
    metadata={"Cache-Control": "no-cache"},
)
print(f"Published https://{MINIO_SERVER}/{BUCKET_NAME}/{prefix}manifest.json")

published = {prefix + entry["path"] for entry in entries} | {prefix + "manifest.json"}
stale = [obj.object_name for obj in client.list_objects(BUCKET_NAME, prefix=prefix, recursive=True)]
stale = [name for name in stale if name not in published]
if stale:
    print("Objects no longer in the manifest, remove manually if unused:", *stale, sep="\n  ")
