"""
Rename and upload system diagram SVGs per water authority to CloudStorage.

For each authority, the single SVG in `verwerkt/sturing/` is renamed to
`{authority}.svg` (if needed) and then uploaded to CloudStorage.

After running this script, Deltares can run `upload_doc_images.py`
to push the SVGs from CloudStorage to s3.deltares.nl for use in the documentation.
"""

# %%
from pathlib import Path

from ribasim_nl import CloudStorage

cloud = CloudStorage()

SELECTION: list[str] = []


def get_sturing_dir(authority: str) -> Path:
    return cloud.joinpath(authority, "verwerkt", "sturing")


def rename_svg_to_authority(authority: str, sturing_dir: Path) -> Path:
    svg_paths = list(sturing_dir.glob("*.svg"))
    if len(svg_paths) != 1:
        raise ValueError(f"{authority}: expected exactly 1 svg in {sturing_dir}, found {len(svg_paths)}")

    svg_path = svg_paths[0]
    target_path = sturing_dir / f"{authority}.svg"

    if svg_path.name == target_path.name:
        return svg_path

    if target_path.exists() and svg_path.name.lower() != target_path.name.lower():
        raise FileExistsError(f"{authority}: target svg already exists: {target_path}")

    # On Windows a rename that only changes casing may be skipped or behave
    # inconsistently on case-insensitive filesystems, so use an intermediate name.
    if svg_path.name.lower() == target_path.name.lower():
        tmp_path = sturing_dir / f"{authority}__tmp__.svg"
        if tmp_path.exists():
            raise FileExistsError(f"{authority}: temporary svg already exists: {tmp_path}")
        svg_path.rename(tmp_path)
        tmp_path.rename(target_path)
    else:
        svg_path.rename(target_path)

    return target_path


for authority in cloud.water_authorities:
    sturing_dir = get_sturing_dir(authority)
    if not sturing_dir.exists():
        print(f"skipping {authority}: {sturing_dir} does not exist")
        continue

    svg_path = rename_svg_to_authority(authority, sturing_dir)
    print(f"uploading {svg_path}")
    cloud.create_dir(authority, "verwerkt", "sturing")
    cloud.upload_file(svg_path)
