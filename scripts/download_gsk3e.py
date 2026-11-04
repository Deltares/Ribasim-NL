"""Download the waterways of North Rhine-Westphalia from GSK3E into one GeoPackage, for routes through Germany.

GSK3E is the Gewässerstationierungskarte of LANUK. Its named waterways and its other waterways are written to the layer
`waterways`, as single-part 2D lines in the CRS of the models, to be tracked with `dvc add`. The columns come from these
two shapefiles, so GEWKZ3E, GEWHNAME and ABSNAME are only set for the named waterways, and GEWNAME for the other ones.
The named waterways are stationed from their mouth, so they are drawn in upstream direction.

Not a DVC stage: run it to update the dataset, and commit the updated `.dvc` file.

Usage: pixi run python scripts/download_gsk3e.py [--archive ZIP] [--output GPKG]
"""

import argparse
import logging
import urllib.request
from pathlib import Path

import geopandas as gpd
import pandas as pd
import shapely
from ribasim_nl.settings import settings

LOG = logging.getLogger(__name__)
URL = (
    "https://www.opengeodata.nrw.de/produkte/umwelt_klima/wasser/oberflaechengewaesser/gsk3e/gsk3e_EPSG25832_Shape.zip"
)
OUTPUT = settings.ribasim_nl_data_dir / "Basisgegevens/GSK3E/gsk3e.gpkg"
SHAPEFILES = {
    "gsk3e_gewkz_line_name": ["GEWKZ3E", "GEWHNAME", "ABSNAME"],
    "gsk3e_sonstige_line_basisdlm": ["GEWNAME"],
}


def read_waterways(archive: Path) -> gpd.GeoDataFrame:
    """Read the waterways of the archive of GSK3E as single-part 2D lines in EPSG:28992."""
    frames = [
        gpd.read_file(f"zip://{archive}!Eingangsdaten/shapefile/{shapefile}.shp", columns=columns)
        for shapefile, columns in SHAPEFILES.items()
    ]
    waterways = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), crs=frames[0].crs).to_crs(28992)
    waterways = waterways.explode(ignore_index=True)
    waterways = waterways[waterways.geom_type == "LineString"].reset_index(drop=True)
    assert not waterways.empty, f"No waterways in {archive}"
    return waterways.set_geometry(shapely.force_2d(waterways.geometry.to_numpy()), crs=waterways.crs)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--archive", type=Path, default=Path(".cache/gsk3e_EPSG25832_Shape.zip"), help="downloaded zip")
    parser.add_argument("--output", type=Path, default=OUTPUT, help="GeoPackage file to write")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    if not args.archive.exists():
        LOG.info(f"Downloading {URL}")
        args.archive.parent.mkdir(parents=True, exist_ok=True)
        urllib.request.urlretrieve(URL, args.archive)
    waterways = read_waterways(args.archive)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    waterways.to_file(args.output, layer="waterways")
    LOG.info(f"Wrote {len(waterways)} waterways to {args.output}")


if __name__ == "__main__":
    main()
