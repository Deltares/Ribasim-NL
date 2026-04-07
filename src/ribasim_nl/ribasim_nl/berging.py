from pathlib import Path

import numpy as np
import numpy.typing as npt
import pandas as pd
import rasterio
import tqdm
from geopandas import GeoDataFrame
from rasterio.enums import Resampling
from rasterio.windows import from_bounds
from rasterstats import zonal_stats
from ribasim import Node
from ribasim.nodes import basin, tabulated_rating_curve
from shapely.geometry import Point, Polygon

from ribasim_nl.cloud import CloudStorage
from ribasim_nl.model import Model

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


def percentage_oppervlaktewater():
    """Single-use function to compute percentage surface water per LHM cell and write as GTIFF LHM_oppervlaktewater_percentage.tif"""
    cloud = CloudStorage()
    lhm_raster_file = cloud.joinpath("Basisgegevens/LHM/4.3/input/LHM_data.tif")
    out_file = lhm_raster_file.with_name("LHM_oppervlaktewater_percentage.tif")
    with rasterio.open(lhm_raster_file) as src:
        band11 = src.read(11).astype("float32")
        band12 = src.read(12).astype("float32")
        profile = src.profile.copy()

        # nodata ophalen
        nodata = src.nodata

        # celoppervlak uit resolutie
        cell_width = src.transform.a
        cell_height = abs(src.transform.e)
        cell_area = cell_width * cell_height

        # geldige pixels bepalen
        if nodata is not None:
            valid = (band11 != nodata) & (band12 != nodata)
        else:
            valid = np.ones(band11.shape, dtype=bool)

        # oppervlaktes optellen
        summed_area = np.full(band11.shape, np.nan, dtype="float32")
        summed_area[valid] = band11[valid] + band12[valid]

        # delen door celoppervlak
        result = np.full(band11.shape, np.nan, dtype="float32")
        result[valid] = summed_area[valid] / cell_area

        # output profiel aanpassen naar 1 band
        profile.update(dtype="float32", count=1, nodata=np.nan)

        with rasterio.open(out_file, "w", **profile) as dst:
            dst.write(result, 1)


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
    df.loc[:, ["maaiveld"]] = [i["mean"] for i in maaiveld]
    df.loc[:, ["maaiveld_max"]] = [i["max"] for i in maaiveld]
    df.loc[:, ["maaiveld_min"]] = [i["min"] for i in maaiveld]

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
    basin_polygon: Polygon, max_level: float, min_level: float, lhm_raster_file: Path, sample_res=25
) -> basin.Profile:
    """Generate a basin.Static table for a Polygon using LHM rasters

    Args:
        basin_polygon (Polygon): Polygon defining the basin
        max_level (float): minimal level in basin-profile
        min_level (float): maximal level in basin-profile
        lhm_raster_file (Path): path to lhm-rasters

    Returns
    -------
        basin.Profile: basin-profile for basin-node
    """
    with rasterio.open(lhm_raster_file) as src:
        window = from_bounds(*basin_polygon.bounds, transform=src.transform)

        if window.width < 1 or window.height < 1:
            window = from_bounds(*basin_polygon.centroid.buffer(125).bounds, transform=src.transform)

        # Maak window netjes integer op pixelgrenzen
        window = window.round_offsets().round_lengths()
        window_transform = src.window_transform(window)

        # original resolution
        xres, yres = abs(src.res[0]), abs(src.res[1])

        # shape to higher resolution so we follow the polygon a bit better
        out_height = max(1, round(window.height * yres / sample_res))
        out_width = max(1, round(window.width * xres / sample_res))

        # new transform
        new_transform = rasterio.Affine(sample_res, 0.0, window_transform.c, 0.0, -sample_res, window_transform.f)

        # resampled helper
        def read_resampled(band):
            return src.read(
                band, window=window, out_shape=(out_height, out_width), resampling=Resampling.nearest
            ).astype(float)

        # if no area 1% - 2% of basin-area
        def default_ah_df():
            df = pd.DataFrame(
                data={
                    "level": [min_level, max_level],
                    "area": [basin_polygon.area * 0.01, basin_polygon.area * 0.02],
                    "comment": ["default: 1% oppervlak", "default: 2% oppervlak"],
                }
            )
            df["level"] = df["level"].round(2)
            df["area"] = df["area"].round(1)
            return df

        mask = rasterio.features.geometry_mask(
            [basin_polygon], out_shape=(out_height, out_width), transform=new_transform, all_touched=True, invert=True
        )

        num_cells = np.sum(mask)
        if num_cells == 0:
            ah_df = default_ah_df()
        else:
            scale = basin_polygon.area / (num_cells * sample_res**2 * (abs(xres) / sample_res) ** 2)

            # 5) bandparen uitlezen
            band_pairs = [
                (BANDEN["bodemhoogte_tertiair_zomer"], BANDEN["opp_tertiair"]),
                (BANDEN["bodemhoogte_secundair_zomer"], BANDEN["opp_secundair"]),
            ]

            dfs = []

            for level_band, area_band in band_pairs:
                level = read_resampled(level_band)
                area = read_resampled(area_band)

                valid = mask & np.isfinite(level) & np.isfinite(area)

                if np.any(valid):
                    dfs.append(
                        pd.DataFrame(
                            {
                                "level": level[valid],
                                "area": area[valid] * scale,
                            }
                        )
                    )

            # make ah_df by summing identical levels, sorting and taking the cumulative sum of the area
            if len(dfs) > 1:
                df = pd.concat(dfs, ignore_index=True)
                # we round levels to 2 decimals so areas get summed
                df["level"] = df["level"].round(2)
                ah_df = df.groupby("level", as_index=False)["area"].sum().sort_values(by="level").reset_index(drop=True)
                ah_df["area"] = ah_df["area"].cumsum().round(1)
                ah_df["comment"] = pd.Series(dtype=str)

                # 0 m2 is not allowed we make it 1
                mask = ah_df.area <= 1
                ah_df.loc[mask, ["area"]] = 1
                ah_df.loc[mask, ["comment"]] = "oppervlak >= 1m2 gezet"

                if ah_df.empty:
                    ah_df = default_ah_df()
            else:
                ah_df = default_ah_df()
    # Return profile
    return basin.Profile(area=ah_df.area, level=ah_df.level, meta_comment=ah_df.comment)


class VdGaastBerging:
    def __init__(self, model: Model, cloud: CloudStorage, use_add_api: bool = True):
        self.model = model
        self.cloud = cloud
        self.use_add_api = use_add_api

        # check and add rasters paths
        lhm_raster_file = self.cloud.joinpath("Basisgegevens/LHM/4.3/input/LHM_data.tif")
        ma_raster_file = self.cloud.joinpath("Basisgegevens/VanDerGaast_QH/spafvoer1.tif")
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

            # define storage basin data
            max_level = basin_area_df.at[basin_id, "maaiveld_max"]
            min_level = basin_area_df.at[basin_id, "maaiveld_min"]
            if min_level == max_level:
                min_level -= 0.1
            basin_profile = get_basin_profile(
                basin_polygon=basin_polygon,
                max_level=max_level,
                min_level=min_level,
                lhm_raster_file=self.lhm_raster_file,
            )
            oppervlaktewater_percentage = round(basin_profile.df.area.max() / basin_polygon.area * 100, 1)
            data = [
                basin_profile,
                basin.State(level=[basin_profile.df.level.min() + 0.1]),
                basin.Area(geometry=[basin_polygon], meta_oppervlaktewater_percentage=[oppervlaktewater_percentage]),
            ]

            # add storage basin

            node = Node(
                meta_categorie="bergend",
                geometry=Point(row.geometry.x + 10, row.geometry.y),
            )
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
