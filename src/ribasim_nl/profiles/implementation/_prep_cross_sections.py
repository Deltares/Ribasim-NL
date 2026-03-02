import logging

import geopandas as gpd
import pandas as pd
import shapely

from profiles.depth import make_depth_profiles
from ribasim_nl import CloudStorage

LOG = logging.getLogger(__name__)


def get_basins(water_authority: str, cloud: CloudStorage = CloudStorage()) -> gpd.GeoDataFrame:
    fn_basins = cloud.joinpath(water_authority, "modellen", f"{water_authority}_parameterized", "database.gpkg")
    return gpd.read_file(fn_basins, layer="Basin / area")


def points2lines_general(
    basins: gpd.GeoDataFrame, cloud: CloudStorage = CloudStorage(), *, buffer: float = 0
) -> gpd.GeoDataFrame:
    fn = cloud.joinpath("Basisgegevens", "profielen", "Profielen_NL.gpkg")
    LOG.info(f"Reading file: {fn}")
    points = gpd.read_file(fn, layer="profielpunt", bbox=tuple(basins.total_bounds))
    LOG.info(f"File read: {fn}")
    points = points[points.intersects(basins.union_all().buffer(buffer))]
    if len(points) == 0:
        LOG.critical("No datapoints within the basin collection: Empty GeoDataFrame")
        return points
    lines = make_depth_profiles(points)
    return lines


def points2lines_agv(cloud: CloudStorage = CloudStorage()) -> gpd.GeoDataFrame:
    fn = cloud.joinpath("Basisgegevens", "profielen", "AGV", "metingprofielpunt.gml")
    points = gpd.read_file(fn)
    lines = make_depth_profiles(points, col_profile_id="metingProfielLijnID")
    lines.rename(columns={"line_id": "profiellijnid"}, inplace=True)
    return lines


def points2lines_delfland(cloud: CloudStorage = CloudStorage()) -> gpd.GeoDataFrame:
    fn = cloud.joinpath("Basisgegevens", "profielen", "Delfland", "Profielen_Delfland.gpkg")
    points = gpd.read_file(fn, layer="Dwarsprofiel_point", columns=["OBJECTID", "BodemHo", "geometry"])
    lines = gpd.read_file(fn, layer="Dwarsprofiel_line", columns=["OBJECTID", "geometry"])
    out = pd.merge(points, lines, on="OBJECTID", suffixes=("_points", "_lines"))
    out.dropna(subset="BodemHo", inplace=True)
    out["geometry"] = out.apply(
        lambda r: shapely.LineString([(x, y, r["BodemHo"]) for x, y in r["geometry_lines"].geoms[0].coords]), axis=1
    )
    out = gpd.GeoDataFrame({"OBJECTID": out["OBJECTID"]}, geometry=out["geometry"], crs=lines.crs)
    out.rename(columns={"OBJECTID": "profiellijnid"}, inplace=True)
    return out


def points2lines_rivierenland(cloud: CloudStorage = CloudStorage()) -> gpd.GeoDataFrame:
    fn = cloud.joinpath("Basisgegevens", "profielen", "Rivierenland", "Profielen_Rivierenland.gpkg")
    lines = gpd.read_file(fn, layer="Profiellijn")
    lines["profiellijnid"] = lines.index.copy()
    return lines


def points2lines_scheldestromen(cloud: CloudStorage = CloudStorage(), *, buffer: float = 0) -> gpd.GeoDataFrame:
    fn = cloud.joinpath("Basisgegevens", "profielen", "Scheldestromen", "Profielen_Scheldestromen.gpkg")
    points = gpd.read_file(fn, layer="profielpunt", columns=["OBJECTID"])
    lines = gpd.read_file(fn, layer="profiellijn", columns=["PRO_ID"])
    out = points.sjoin(lines, how="left", predicate="dwithin", distance=buffer, rsuffix="line")
    out = make_depth_profiles(out.dropna(subset=["PRO_ID"]), col_profile_id="PRO_ID")
    out.rename(columns={"line_id": "profiellijnid"}, inplace=True)
    return out


def get_profiles(water_authority: str, cloud: CloudStorage = CloudStorage(), *, buffer: float = 0) -> gpd.GeoDataFrame:
    match water_authority:
        case "AmstelGooienVecht":
            return points2lines_agv(cloud=cloud)
        case "Delfland":
            return points2lines_delfland(cloud=cloud)
        case "Rivierenland":
            return points2lines_rivierenland(cloud=cloud)
        case "Scheldestromen":
            return points2lines_scheldestromen(cloud=cloud, buffer=buffer)
        case _:
            basins = get_basins(water_authority, cloud=cloud)
            return points2lines_general(basins, cloud=cloud, buffer=buffer)


def export_to_cloud(
    water_authority: str, cloud: CloudStorage = CloudStorage(), *, buffer: float = 0, overwrite: bool = False
) -> None:
    # sync cloud: Basisgegevens - profielen
    cloud.download_basisgegevens(["profielen"], overwrite=overwrite)

    # working directories
    folders = water_authority, "verwerkt", "profielen", "intermediate"
    src = cloud.joinpath(*folders)

    # ensure existence of working directories
    src.mkdir(parents=True, exist_ok=True)
    cloud.create_dir(*folders[:-1])
    cloud.create_dir(*folders)

    # get data
    lines = get_profiles(water_authority, cloud, buffer=buffer)
    print(lines.columns)

    # upload data
    fn = "lines_z.gpkg"
    lines.to_file(str(src / fn))
    cloud.upload_file(src / fn)
    print(f"File saved: {src / fn}")


if __name__ == "__main__":
    # only import `argparse` when necessary
    import argparse

    # initiate argument-parser
    parser = argparse.ArgumentParser(description="Preprocessing cross-section data per water authority.")

    # required argument: water authority
    parser.add_argument("water_authority", type=str, help="Mandatory argument: Water authority to preprocess")

    # optional arguments
    parser.add_argument(
        "--buffer", "-b", type=float, default=0.05, help="Optional argument: Buffer when coupling points to lines"
    )
    parser.add_argument("--log", "-l", type=str, default="WARNING", help="Optional argument: Log-level.")

    # parse arguments
    args = parser.parse_args()

    logging.basicConfig(level=args.log.upper())
    export_to_cloud(args.water_authority, buffer=args.buffer)
