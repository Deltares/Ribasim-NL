# %%
from pathlib import Path

import numpy as np
import numpy.typing as npt
import pandas as pd
import rasterio
import tqdm
from geopandas import GeoDataFrame
from rasterio.windows import from_bounds
from rasterstats import zonal_stats
from ribasim import Model, Node
from ribasim.nodes import basin, tabulated_rating_curve
from shapely.geometry import Point, Polygon

from ribasim_nl.cloud import CloudStorage

BANDEN = {
    "maaiveld": 1,
    "bodemhoogte_primair_winter": 2,
    "bodemhoogte_primair_zomer": 3,
    "bodemhoogte_secundair_winter": 4,
    "bodemhoogte_secundair_zomer": 5,
    "bodemhoogte_tertiair_winter": 6,
    "bodemhoogte_tertiair_zomer": 7,
    "ghg_2010-2019": 8,
    "glg_2010-2019": 9,
    "opp_primair": 10,
    "opp_secundair": 11,
    "opp_tertiair": 12,
}


def sample_raster(
    raster_file: Path,
    df: GeoDataFrame,
    band: int = 1,
    fill_value: float | None = None,
    all_touched: bool = False,
    stats: str = "mean",
    maaiveld_data: npt.ArrayLike | None = None,
):
    """Sample rasters over Polygons

    Args:
        raster_file (Path): Raster-file to sample
        df (GeoDataFrame): GeoDataFrame with polygons
        band (int, optional): Band in raster-file to sample from. Defaults to 1.
        fill_value (float | None, optional): Fill-value for nodata-cells. Defaults to None.
        all_touched (bool, optional): rasterize all_touched setting. Defaults to False.
        stats (str, optional): rasterstats stats setting. Defaults to "mean".
        maaiveld_data (npt.ArrayLike | None, optional): If numpy-array in same shape as raster, raster-data will be subtracted from it. Defaults to None.

    Returns
    -------
        list[dict]: Rasterstats output
    """
    with rasterio.open(raster_file) as raster_src:
        # read band
        data = raster_src.read(band)

        if maaiveld_data is not None:
            data = maaiveld_data - data

        # fill nodata
        if fill_value is not None:
            data = np.where(data == raster_src.nodata, fill_value, data)

        affine = raster_src.transform

        return zonal_stats(df, data, affine=affine, stats=stats, nodata=raster_src.nodata, all_touched=all_touched)


def add_basin_statistics(df: GeoDataFrame, lhm_raster_file: Path, ma_raster_file: Path) -> GeoDataFrame:
    """Add Vd Gaast basin-statistics to a Polygon basin GeoDataFrame

    Args:
        df (GeoDataFrame): GeoDataFrame with basins
        lhm_raster_file (Path): LHM raster-file with layers
        ma_raster_file (Path): Specific discharge (maatgevende afvoer) raster

    Returns
    -------
        GeoDataFrame: GeoDataFrame with basins ánd statistics
    """
    with rasterio.open(lhm_raster_file) as raster_src:
        # read band
        maaiveld_data = raster_src.read(BANDEN["maaiveld"])

    # sample rasters
    ghg = sample_raster(
        raster_file=lhm_raster_file,
        df=df,
        band=BANDEN["ghg_2010-2019"],
        all_touched=True,
        maaiveld_data=maaiveld_data,
    )
    df.loc[:, ["ghg"]] = pd.Series(dtype=float)
    df.loc[:, ["ghg"]] = [i["mean"] for i in ghg]

    glg = sample_raster(
        raster_file=lhm_raster_file,
        df=df,
        band=BANDEN["glg_2010-2019"],
        all_touched=True,
        maaiveld_data=maaiveld_data,
    )
    df.loc[:, ["glg"]] = pd.Series(dtype=float)
    df.loc[:, ["glg"]] = [i["mean"] for i in glg]

    ma = sample_raster(raster_file=ma_raster_file, df=df, all_touched=True, fill_value=37)  # 37mm/dag is
    df.loc[:, ["ma"]] = pd.Series(dtype=float)
    df.loc[:, ["ma"]] = [i["mean"] for i in ma]

    maaiveld = sample_raster(
        raster_file=lhm_raster_file, df=df, band=BANDEN["maaiveld"], stats="mean min max", all_touched=True
    )
    df.loc[:, ["maaiveld"]] = pd.Series(dtype=float)
    df.loc[:, ["maaiveld"]] = [i["mean"] if pd.isna(i["mean"]) else i["mean"] for i in maaiveld]
    df.loc[:, ["maaiveld_max"]] = [i["max"] if pd.isna(i["max"]) else i["max"] for i in maaiveld]
    df.loc[:, ["maaiveld_min"]] = [i["min"] if pd.isna(i["min"]) else i["min"] for i in maaiveld]

    return df


def get_rating_curve(row, min_level: float, maaiveld: None | float = None) -> tabulated_rating_curve.Static:
    """Generate a tabulated_rating_curve.Static object from basin_statistics

    Args:
        row (pd.Series): Row in a GeoDataFrame containing basin_statistics
        min_level (float): minimal level in rating curve (basin.profile.level.min() of upstream basin)
        maaiveld (None | float, optional): surface-level. If none, it should be in row.maaiveld. Defaults to None.

    Returns
    -------
        tabulated_rating_curve.Static: Static-table for tabulated_rating_curve node
    """
    flow_rate = np.round([0, row.ma * 0.2, row.ma * 0.33, row.ma / 2, row.ma * 2], decimals=2)
    depth = np.round([row.glg + 1, row.glg, row.ghg, row.ghg / 2, 0], decimals=2)

    # set GxG < 0 to 0
    depth[depth < 0] = 0

    # level relative to maaiveld
    if maaiveld is not None:
        level = maaiveld - depth
    else:
        level = row.maaiveld - depth

    # make sure level >= min_level
    level[level < min_level] = min_level

    # flow_rate in m3/s
    flow_rate = flow_rate / 1000 * row.geometry.area / 86400

    df = pd.DataFrame({"level": np.round(level, decimals=2), "flow_rate": np.round(flow_rate, decimals=5)})
    df.drop_duplicates("level", keep="first", inplace=True)
    df.drop_duplicates("flow_rate", keep="last", inplace=True)

    return tabulated_rating_curve.Static(level=df.level, flow_rate=df.flow_rate)


def get_basin_profile(
    basin_polygon: Polygon, polygon: Polygon, max_level: float, min_level: float, lhm_raster_file: Path
) -> basin.Profile:
    """Generate a basin.Static table for a Polygon using LHM rasters

    Args:
        basin_polygon (Polygon): Polygon defining the basin
        polygon (Polygon): Polygon defining all waters that are to be subtracted in primary waters
        max_level (float): minimal level in basin-profile
        min_level (float): maximal level in basin-profile
        lhm_raster_file (Path): path to lhm-rasters

    Returns
    -------
        basin.Profile: basin-profile for basin-node
    """
    with rasterio.open(lhm_raster_file) as src:
        level = np.array([], dtype=float)
        area = np.array([], dtype=float)

        # Get the window and its transform
        window = from_bounds(*basin_polygon.bounds, transform=src.transform)

        if (window.width) < 1 or (window.height < 1):
            window = from_bounds(*basin_polygon.centroid.buffer(125).bounds, transform=src.transform)
        window_transform = src.window_transform(window)

        # Primary water bottom-level
        window_data = src.read(3, window=window)

        # We don't want hoofdwater / doorgaand water to be in profile
        if (polygon is None) | (window_data.size == 0):
            mask = ~np.isnan(window_data)
        else:
            mask = rasterio.features.geometry_mask(
                [polygon], window_data.shape, window_transform, all_touched=True, invert=True
            )
            # Include nodata as False in mask
            mask[np.isnan(window_data)] = False

        # add levels
        level = np.concat([level, window_data[mask].ravel()])

        # add areas on same mask
        window_data = src.read(10, window=window)
        area = np.concat([area, window_data[mask].ravel()])

        # Secondary water
        window_data = src.read(5, window=window)
        mask = ~np.isnan(window_data)
        level = np.concat([level, window_data[mask].ravel()])

        window_data = src.read(11, window=window)
        area = np.concat([area, window_data[mask].ravel()])

        # Tertiary water water
        window_data = src.read(7, window=window)
        mask = ~np.isnan(window_data)
        level = np.concat([level, window_data[mask].ravel()])

        window_data = src.read(12, window=window)
        area = np.concat([area, window_data[mask].ravel()])

    # Make sure area is never larger than polygon-area
    area[area > basin_polygon.area] = basin_polygon.area

    # If area is empty, we add min_level at 5% of polygon-area
    if area.size == 0:
        level = np.append(level, min_level)
        area = np.append(area, basin_polygon.area * 0.05)

    # Add extra row with max_level at basin_polygon.area
    level = np.append(level, max_level)
    area = np.append(area, basin_polygon.area)

    # In pandas for magic
    df = pd.DataFrame({"level": np.round(level, decimals=2), "area": np.round(area)})
    df.loc[df["area"] == 0, "area"] = 1  # 0m2 area doesn't work
    df.sort_values(by="level", inplace=True)
    df = df.set_index("level").cumsum().reset_index()
    df.dropna(inplace=True)
    df.drop_duplicates("level", keep="last", inplace=True)

    # Return profile
    return basin.Profile(area=df.area, level=df.level)


class VdGaastBerging:
    def __init__(self, model: Model, cloud: CloudStorage, use_add_api: bool = True):
        self.model = model
        self.cloud = cloud
        self.use_add_api = use_add_api

        # check and add rasters paths
        lhm_raster_file = self.cloud.joinpath("Basisgegevens", "LHM", "4.3", "input", "LHM_data.tif")
        ma_raster_file = self.cloud.joinpath("Basisgegevens", "VanDerGaast_QH", "spafvoer1.tif")
        self.cloud.synchronize([lhm_raster_file, ma_raster_file])
        self.lhm_raster_file = lhm_raster_file
        self.ma_raster_file = ma_raster_file

    def add(self):
        model = self.model

        # get basin_area and add statistics
        print("compute basin statistics")
        basin_area_df = model.basin.area.df.dissolve("node_id").copy()
        basin_area_df = add_basin_statistics(
            df=basin_area_df, lhm_raster_file=self.lhm_raster_file, ma_raster_file=self.ma_raster_file
        )

        for row in tqdm.tqdm(
            model.basin.node.df.itertuples(), total=len(model.basin.node.df), desc="add storage nodes"
        ):
            # get basin_id and basin polygon
            basin_id = row.Index
            basin_row = basin_area_df.loc[basin_id]
            basin_polygon = basin_row.geometry

            # define storage basin node
            node = Node(
                meta_categorie="bergend",
                geometry=Point(row.geometry.x + 10, row.geometry.y),
            )

            # define storage basin data
            max_level = max_level = basin_area_df.at[basin_id, "maaiveld_max"]
            min_level = max_level = basin_area_df.at[basin_id, "maaiveld_min"]
            if min_level == max_level:
                min_level -= 0.1
            basin_profile = get_basin_profile(
                basin_polygon=basin_polygon,
                polygon=basin_polygon,
                max_level=max_level,
                min_level=min_level,
                lhm_raster_file=self.lhm_raster_file,
            )
            data = [
                basin_profile,
                basin.State(level=[basin_profile.df.level.min() + 0.1]),
                basin.Area(geometry=[basin_polygon]),
            ]

            # add storage basin
            basin_node = model.basin.add(node=node, tables=data)

            # add connector
            if any(pd.isna(getattr(basin_row, i)) for i in ["ghg", "glg", "ma"]):
                raise ValueError(f"No valid ghg, glg and/or ma for basin_id {basin_id}")
            else:
                # get tabulated rating curve data
                data = [get_rating_curve(row=basin_row, min_level=basin_profile.df.level.min())]

                # connect storage basin with basin with a tabulated rating curve
                model.add_and_connect_node(
                    basin_node.node_id,
                    to_basin_id=basin_id,
                    geometry=Point(row.geometry.x + 5, row.geometry.y),
                    node_type="TabulatedRatingCurve",
                    tables=data,
                    use_add_api=self.use_add_api,
                    meta_categorie="bergend",
                )
