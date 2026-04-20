"""
Get the system diagrams from the CloudStorage to s3.deltares.nl for use in the documentation.

This needs the following environment variables to be set in `.env` (Deltares only):
MINIO_ACCESS_KEY
MINIO_SECRET_KEY

And can be run with `just upload-doc-images`.
"""

from pathlib import Path

from minio import Minio
from minio.error import S3Error
from requests.exceptions import HTTPError
from ribasim_nl.settings import settings

from ribasim_nl import CloudStorage

if not settings.minio_access_key or not settings.minio_secret_key:
    raise OSError("MINIO_ACCESS_KEY and MINIO_SECRET_KEY must be set in the environment or .env file.")

MINIO_SERVER = "s3.deltares.nl"
BUCKET_NAME = "ribasim"


def upload_file(
    source: Path,
    destination: str,
    access_key: str,
    secret_key: str,
) -> None:
    """Upload a single file to the Ribasim MinIO bucket."""
    if not source.is_file():
        raise ValueError(f"The source file does not exist: {source}")

    client = Minio(MINIO_SERVER, access_key=access_key, secret_key=secret_key)
    try:
        client.fput_object(BUCKET_NAME, destination, str(source))
    except S3Error as e:
        print(f"Error occurred: {e}")


cloud = CloudStorage()


destination = "doc-image/ribasim-nl/watersystems/"

for authority in cloud.water_authorities:
    source = cloud.joinpath(authority, "verwerkt/sturing", f"{authority}.svg")
    try:
        cloud.synchronize([source])
    except HTTPError:
        print(f"Skipping {authority}, could not synchronize image.")
        continue
    upload_file(
        source,
        destination + source.name,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
    )
