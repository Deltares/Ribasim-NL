"""Download the BGT water areas of the Netherlands into one GeoParquet file, see `ribasim_nl.bgt_water`.

The area of the water boards and Rijkswaterstaat is downloaded from PDOK in square tiles. Each tile is reduced to its
current objects and cached in `--tile-dir`, so an interrupted download resumes where it stopped. Then the tiles are
merged into `ribasim_nl.bgt_water.BGT_WATER`, to be tracked with `dvc add`.

Usage: pixi run python scripts/download_bgt_water.py [--tile-size METRES] [--tile-dir DIR] [--bbox XMIN YMIN XMAX YMAX]
"""

import argparse
import logging
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
from ribasim_nl.bgt_water import BGT_WATER, current_objects, download_tile, write_bgt_water
from ribasim_nl.settings import settings

LOG = logging.getLogger(__name__)
BOUNDARIES = settings.ribasim_nl_data_dir / "Basisgegevens/RWS_waterschaps_grenzen"


def tiles(area: shapely.Geometry, size: float) -> list[shapely.Polygon]:
    """Square tiles of `size` that intersect `area`, aligned to multiples of `size`."""
    xmin, ymin, xmax, ymax = (np.array(area.bounds) // size * size) + [0, 0, size, size]
    boxes = [
        shapely.box(x, y, x + size, y + size) for x in np.arange(xmin, xmax, size) for y in np.arange(ymin, ymax, size)
    ]
    shapely.prepare(area)
    return [box for box in boxes if area.intersects(box)]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--tile-size", type=float, default=20_000, help="tile size in metres")
    parser.add_argument(
        "--tile-dir", type=Path, default=Path(".cache/bgt_water_tiles"), help="cache of downloaded tiles"
    )
    parser.add_argument("--bbox", type=float, nargs=4, help="only this part of the area, in EPSG:28992, for testing")
    parser.add_argument("--output", type=Path, default=BGT_WATER, help="GeoParquet file to write")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    area = pd.concat([gpd.read_file(BOUNDARIES / name) for name in ("waterschap.gpkg", "Rijkswaterstaat.gpkg")])
    area = area.to_crs(28992).union_all()
    if args.bbox is not None:
        area = area.intersection(shapely.box(*args.bbox))
    area_tiles = tiles(area, args.tile_size)
    args.tile_dir.mkdir(parents=True, exist_ok=True)
    LOG.info(f"Downloading {len(area_tiles)} tiles of {args.tile_size / 1000:g} km")
    paths = []
    for i, tile in enumerate(area_tiles, start=1):
        xmin, ymin = (int(v) for v in tile.bounds[:2])
        path = args.tile_dir / f"bgt_water_{xmin}_{ymin}.parquet"
        if not path.exists():
            current_objects(download_tile(tile)).to_parquet(path)
        paths.append(path)
        LOG.info(f"Tile {i}/{len(area_tiles)}: {path.name}")

    gdf = current_objects_of_tiles(paths)
    write_bgt_water(gdf, args.output)
    LOG.info(f"Wrote {len(gdf)} objects to {args.output}; track it with `dvc add {args.output}`")


def current_objects_of_tiles(paths: list[Path]) -> gpd.GeoDataFrame:
    """Merge tiles of current objects, dropping the objects that lie in several tiles."""
    gdf = pd.concat([gpd.read_parquet(path) for path in paths], ignore_index=True)
    return gpd.GeoDataFrame(gdf.drop_duplicates("lokaalID").reset_index(drop=True), crs=28992)


if __name__ == "__main__":
    main()
