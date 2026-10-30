# %%
from pathlib import Path
from typing import Any, cast

import numpy as np
import numpy.typing as npt
import pandas as pd
import rasterio
import shapely
import tqdm
from affine import Affine
from geopandas import GeoDataFrame
from rasterio.enums import Resampling
from rasterio.features import geometry_mask
from rasterio.io import MemoryFile
from rasterio.windows import Window, from_bounds
from rasterstats import zonal_stats
from ribasim import Node
from ribasim.nodes import basin, manning_resistance, tabulated_rating_curve
from shapely.geometry import Point, Polygon

from ribasim_nl.cloud import CloudStorage
from ribasim_nl.model import Model
from ribasim_nl.profiles import MIN_PROFILE_AREA
from ribasim_nl.profiles.implement import bergend_manning_static
from ribasim_nl.raster import with_rasterio_env
from ribasim_nl.settings import settings

LHM_RASTER_FILE = settings.ribasim_nl_data_dir / Path("Basisgegevens/LHM/4.3/input/LHM_data.tif")


def band_index(src: rasterio.DatasetReader, name: str) -> int:
    """The 1-based index of the band in `src` whose description is `name`, such as "opp_primair" in `LHM_RASTER_FILE`."""
    assert name in src.descriptions, f"No band {name!r} in {src.name}, only {src.descriptions}"
    return src.descriptions.index(name) + 1


def primary_surface_water_fraction() -> MemoryFile:
    """The fraction of each LHM cell that is primary surface water, as a raster in memory.

    It is derived from `LHM_RASTER_FILE` on each call, rather than cached next to it in a file that DVC does not track.
    """
    with rasterio.open(LHM_RASTER_FILE) as src:
        area = src.read(band_index(src, "opp_primair")).astype("float32")
        profile = src.profile.copy()
        nodata = src.nodata
        cell_area = src.transform.a * abs(src.transform.e)

    valid = area != nodata if nodata is not None else np.ones(area.shape, dtype=bool)
    fraction = np.full(area.shape, np.nan, dtype="float32")
    fraction[valid] = area[valid] / cell_area

    profile.update(driver="GTiff", dtype="float32", count=1, nodata=np.nan)
    memfile = MemoryFile()
    with memfile.open(**profile) as dst:
        dst.write(fraction, 1)
    return memfile


def sample_raster(
    raster_file: Path,
    df: GeoDataFrame,
    band: int | str = 1,
    fill_value: float | None = None,
    all_touched: bool = False,
    stats: str = "mean",
    maaiveld_data: npt.ArrayLike | None = None,
) -> list[dict[str, Any]]:
    """Sample rasters over Polygons

    Args:
        raster_file (Path): Raster-file to sample
        df (GeoDataFrame): GeoDataFrame with polygons
        band (int | str, optional): Band in raster-file to sample from, by 1-based index or description. Defaults to 1.
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
        data = raster_src.read(band if isinstance(band, int) else band_index(raster_src, band))

        if maaiveld_data is not None:
            data = maaiveld_data - data

        # fill nodata
        if fill_value is not None:
            data = np.where(data == raster_src.nodata, fill_value, data)

        affine = raster_src.transform

        return zonal_stats(df, data, affine=affine, stats=stats, nodata=raster_src.nodata, all_touched=all_touched)


def get_resampled_window(src, polygon, sample_res) -> tuple[tuple[int, int], Window, Affine]:
    window = from_bounds(*polygon.bounds, transform=src.transform)

    if window.width < 1 or window.height < 1:
        window = from_bounds(*polygon.centroid.buffer(125).bounds, transform=src.transform)

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
    return (out_height, out_width), window, new_transform


# resampled helper
def read_resampled(
    src: rasterio.DatasetReader,
    band: int,
    window: Window,
    out_shape: tuple[int, int],
) -> npt.NDArray[np.float64]:
    return src.read(band, window=window, out_shape=out_shape, resampling=Resampling.nearest).astype(float)


def basin_link_buffer_area(model: Model, node_id, buffer_distance=2.5):
    """
    Berekent het oppervlak van een buffer rond alle links van/naar een basin.

    Parameters
    ----------
    model : ribasim.Model
    node_id : int
        Basin node_id.
    buffer_distance : float
        Bufferafstand in CRS-eenheden, bijvoorbeeld meters bij EPSG:28992.

    Returns
    -------
    float
        Oppervlak van de unary-union buffer.
    """
    links = model.link.df

    mask = (links["from_node_id"] == node_id) | (links["to_node_id"] == node_id)

    basin_links = links.loc[mask]

    if basin_links.empty:
        return 0.0

    geom = basin_links.geometry.dropna()

    if geom.empty:
        return 0.0

    buffered_union = geom.buffer(buffer_distance).union_all()

    return buffered_union.area


@with_rasterio_env
def update_primary_basin_profiles(model: Model, sample_res: int = 25, depth: float = 2.0, buffer_distance=2.5) -> None:
    """Surface water area of primary basin based on primair_oppervlaktewater in LHM4.3

    The average of resampled surface water percentage raster is mulitplied by polygon.area

    Parameters
    ----------
    polygon : Polygon
        Polygon of the basin
    sample_res : int, optional
        Resolution in which LHM raster is read under the basin.polygon, by default 25
    buffer_distance: float, optional
        Buffer distance around connected links

    Returns
    -------
    float
        _description_
    """

    # ah_df to concat
    def ah_df(
        node_id: int,
        polygon: Polygon,
        target_level: float,
        area_fraction: float,
        buffer_distance: float = 2.5,
    ) -> pd.DataFrame:
        level = np.array([target_level - depth, target_level]).round(2)
        profile_node_ids = [node_id] * 2

        # If no fraction is available, assume 2% of the basin area.
        if pd.isna(area_fraction):
            sw_area = 0.02 * polygon.area
            comment = "default: 2% oppervlak"
        elif area_fraction < 0.001:
            sw_area = 0.001 * polygon.area
            comment = "oppervlak >= 0.1% gezet"
        else:
            sw_area = area_fraction * polygon.area
            comment = "%LHM * oppervlak"

        minimum_area = max(
            MIN_PROFILE_AREA,
            basin_link_buffer_area(model=model, node_id=node_id, buffer_distance=buffer_distance),
        )
        if sw_area < minimum_area:
            sw_area = minimum_area
            comment = f"oppervlak >= {round(minimum_area, 1)}m2 gezet"

        area = np.full(2, sw_area).round(1)
        df = basin.Profile(node_id=profile_node_ids, level=level, area=area, meta_comment=[comment] * 2).df
        assert df is not None
        return df

    basin_node_df = model.basin.node.df
    basin_area_df = cast(pd.DataFrame, model.basin.area.df)
    basin_profile_df = cast(pd.DataFrame, model.basin.profile.df)

    # read resampled raster average and multiply by polygon.area
    basin_ids = basin_node_df[basin_node_df.meta_categorie.isin(["hoofdwater", "doorgaand"])].index.values
    profiles: list[pd.DataFrame] = []
    with primary_surface_water_fraction() as memfile, memfile.open() as src:
        for basin_id in tqdm.tqdm(basin_ids, total=len(basin_ids), desc="profiles primary basins"):
            # read basin-area table
            polygon = cast(Polygon, cast(Any, basin_area_df.set_index("node_id").at[basin_id, "geometry"]))
            target_level = float(cast(Any, basin_area_df.set_index("node_id").at[basin_id, "meta_streefpeil"]))
            basin_fid = basin_area_df[basin_area_df.node_id == basin_id].index[0]

            # read raster sampled
            out_shape, window, transform = get_resampled_window(src=src, polygon=polygon, sample_res=sample_res)

            data = read_resampled(
                src,
                band=1,
                window=window,
                out_shape=out_shape,
            )

            if src.nodata is not None:
                data[data == src.nodata] = np.nan

            mask = geometry_mask(
                [polygon],
                transform=transform,
                invert=True,
                out_shape=data.shape,
            )

            values = data[mask]

            # get area_fraction and add oppervlaktewater_percentage to basin.area table
            area_fraction = np.nan if values.size == 0 or np.all(np.isnan(values)) else float(np.nanmean(values))

            basin_area_df.loc[basin_fid, "meta_oppervlaktewater_percentage"] = round(area_fraction * 100, 1)
            profile = ah_df(
                node_id=basin_id,
                polygon=polygon,
                target_level=target_level,
                area_fraction=area_fraction,
                buffer_distance=buffer_distance,
            )
            percentage = round(profile.area.max() / polygon.area * 100, 1)
            basin_area_df.loc[basin_fid, "meta_oppervlaktewater_percentage"] = percentage
            profiles += [profile]

    # replace old profiles
    df = pd.concat(profiles, ignore_index=True)
    basin_profile_df = basin_profile_df[~basin_profile_df.node_id.isin(basin_ids)]
    basin_profile_df = pd.concat([basin_profile_df, df], ignore_index=True)
    model.basin.profile.df = cast(Any, basin_profile_df)


@with_rasterio_env
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
        maaiveld_data = raster_src.read(band_index(raster_src, "maaiveld"))

    # sample rasters
    ghg = sample_raster(
        raster_file=lhm_raster_file,
        df=df,
        band="ghg_2010-2019",
        all_touched=True,
        maaiveld_data=maaiveld_data,
    )
    df.loc[:, ["ghg"]] = pd.Series(dtype=float)
    df.loc[:, ["ghg"]] = [i["mean"] for i in ghg]

    glg = sample_raster(
        raster_file=lhm_raster_file,
        df=df,
        band="glg_2010-2019",
        all_touched=True,
        maaiveld_data=maaiveld_data,
    )
    df.loc[:, ["glg"]] = pd.Series(dtype=float)
    df.loc[:, ["glg"]] = [i["mean"] for i in glg]

    ma = sample_raster(raster_file=ma_raster_file, df=df, all_touched=True, fill_value=37)  # 37mm/dag is
    df.loc[:, ["ma"]] = pd.Series(dtype=float)
    df.loc[:, ["ma"]] = [i["mean"] for i in ma]

    maaiveld = sample_raster(
        raster_file=lhm_raster_file, df=df, band="maaiveld", stats="mean min max", all_touched=True
    )
    df.loc[:, ["maaiveld"]] = pd.Series(dtype=float)
    df.loc[:, ["maaiveld"]] = [i["mean"] for i in maaiveld]
    df.loc[:, ["maaiveld_max"]] = [i["max"] for i in maaiveld]
    df.loc[:, ["maaiveld_min"]] = [i["min"] for i in maaiveld]

    return df


def get_rating_curve(row, min_level: float, maaiveld: float | None = None) -> tabulated_rating_curve.Static:
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
    level = maaiveld - depth if maaiveld is not None else row.maaiveld - depth

    # make sure level >= min_level
    level[level < min_level] = min_level

    # flow_rate in m3/s
    flow_rate = flow_rate / 1000 * row.geometry.area / 86400

    df = pd.DataFrame({"level": np.round(level, decimals=2), "flow_rate": np.round(flow_rate, decimals=5)})
    df.drop_duplicates("level", keep="first", inplace=True)
    df.drop_duplicates("flow_rate", keep="last", inplace=True)

    return tabulated_rating_curve.Static(level=df.level, flow_rate=df.flow_rate)


def get_manning_resistance(area: float) -> manning_resistance.Static:
    """Generate the ManningResistance static table between a storing and a flowing basin, see `bergend_manning_static`

    Args:
        area (float): area of the basin [m2]

    Returns
    -------
        manning_resistance.Static: Static-table for ManningResistance node
    """
    static = bergend_manning_static(pd.Series([area])).iloc[0]
    return manning_resistance.Static(
        length=[static["length"]],
        manning_n=[static["manning_n"]],
        profile_width=[static["profile_width"]],
        profile_slope=[static["profile_slope"]],
    )


def parse_meta_aanvoer(values: pd.Series) -> pd.Series:
    """Parse the `meta_aanvoer` column of the supply areas ('aanvoergebieden') to booleans

    The files store it as booleans, as 0 and 1, or as strings. A missing value means no supply.

    Args:
        values (pd.Series): Values of `meta_aanvoer`

    Returns
    -------
        pd.Series: True where the area is supplied
    """
    # 1 and 0 match True and False as keys, also as floats
    mapping = {True: True, False: False, "true": True, "false": False}

    def parse(value: Any) -> bool | None:
        if pd.isna(value):
            return False
        return mapping.get(value.lower() if isinstance(value, str) else value)

    parsed = values.map(parse)
    unknown = values[parsed.isna()]
    assert unknown.empty, f"Unknown meta_aanvoer values: {sorted(map(str, unknown.unique()))}"
    return parsed.astype(bool)


def supply_area_basin_ids(model: Model, supply_areas: GeoDataFrame, min_overlap: float = 0.5) -> set[int]:
    """Get the flowing (non-'bergend') basins that lie in the supplied areas ('aanvoergebieden')

    Only the supply areas with `meta_aanvoer` true are used, see `parse_meta_aanvoer`.
    Basins are selected by area rather than by node_id, since the node_ids in the supply areas can be outdated.

    Args:
        model (Model): Ribasim model
        supply_areas (GeoDataFrame): Polygons of the supply areas, with a `meta_aanvoer` column
        min_overlap (float, optional): Minimal fraction of the basin area within the supplied areas. Defaults to 0.5.

    Returns
    -------
        set[int]: node_ids of the basins in the supplied areas
    """
    assert 0 < min_overlap <= 1, f"min_overlap should be in (0, 1], not {min_overlap}"
    assert "meta_aanvoer" in supply_areas.columns, "Supply areas have no meta_aanvoer column"
    supply_areas = supply_areas[supply_areas.geometry.notna() & parse_meta_aanvoer(supply_areas["meta_aanvoer"])]
    if supply_areas.empty:
        return set()
    supply_geometries = supply_areas.geometry.make_valid().to_numpy()
    tree = shapely.STRtree(supply_geometries)

    basin_node_df = model.basin.node.df
    flowing_ids = basin_node_df.index[basin_node_df["meta_categorie"] != "bergend"]

    basin_area_df = model.basin.area.df
    assert basin_area_df is not None
    basin_area_df = basin_area_df[basin_area_df["node_id"].isin(flowing_ids) & basin_area_df.geometry.notna()]
    basin_area = basin_area_df.dissolve("node_id").geometry

    def overlap(geometry: Any) -> float:
        # Intersect with the union of only the supply areas that touch the basin, since the intersection with the
        # union of all of them can be wrong: Hunze en Aa's basin 1717 lies for 39% in them, but got 0% or 107%
        nearby = supply_geometries[tree.query(geometry, predicate="intersects")]
        if len(nearby) == 0:
            return 0.0
        return geometry.intersection(shapely.union_all(nearby)).area / geometry.area

    return {int(cast(int, node_id)) for node_id, geometry in basin_area.items() if overlap(geometry) > min_overlap}


def simplify_area_level_df(
    df: pd.DataFrame,
    level_col: str = "level",
    area_col: str = "area",
    max_points: int = 5,
) -> pd.DataFrame:
    """Simplify an area-level curve while preserving endpoints, area extrema, and the most important bend points.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame with level and area columns.
    level_col, area_col : str
        Column names for level and area.
    max_points : int
        Maximum number of points to retain.

    Returns
    -------
    pandas.DataFrame
        Simplified DataFrame with at most `max_points` rows.
    """
    if len(df) <= max_points:
        return df.copy()

    xy = df[[level_col, area_col]].to_numpy(float)

    keep = {
        0,
        len(df) - 1,
        int(np.argmin(xy[:, 1])),
        int(np.argmax(xy[:, 1])),
    }

    while len(keep) < max_points:
        best_i = None
        best_d = -1.0

        for i0, i1 in zip(sorted(keep)[:-1], sorted(keep)[1:], strict=False):
            if i1 <= i0 + 1:
                continue

            a, b = xy[i0], xy[i1]
            v = b - a
            w = xy[i0 + 1 : i1] - a

            denom = np.hypot(v[0], v[1])
            d = np.linalg.norm(w, axis=1) if denom == 0 else np.abs(v[0] * w[:, 1] - v[1] * w[:, 0]) / denom
            j = int(np.argmax(d))
            if d[j] > best_d:
                best_d = float(d[j])
                best_i = i0 + 1 + j

        if best_i is None:
            break

        keep.add(best_i)

    return df.iloc[sorted(keep)].copy()


def get_basin_profile(
    basin_polygon: Polygon, max_level: float, min_level: float, lhm_raster_file: Path, sample_res: int = 25
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
        # get resampled shape and transform
        out_shape, window, new_transform = get_resampled_window(src=src, polygon=basin_polygon, sample_res=sample_res)

        # if no area 1% - 2% of basin-area
        def default_ah_df() -> pd.DataFrame:
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

        mask = geometry_mask(
            [basin_polygon], out_shape=out_shape, transform=new_transform, all_touched=True, invert=True
        )

        num_cells = np.sum(mask)
        if num_cells == 0:
            ah_df = default_ah_df()
        else:
            scale = basin_polygon.area / (num_cells * sample_res**2 * (abs(src.res[0]) / sample_res) ** 2)

            # 5) bandparen uitlezen
            band_pairs = [
                (band_index(src, "bodemhoogte_tertiair_zomer"), band_index(src, "opp_tertiair")),
                (band_index(src, "bodemhoogte_secundair_zomer"), band_index(src, "opp_secundair")),
            ]

            dfs = []

            for level_band, area_band in band_pairs:
                level = read_resampled(
                    src,
                    band=level_band,
                    window=window,
                    out_shape=out_shape,
                )
                area = read_resampled(
                    src,
                    band=area_band,
                    window=window,
                    out_shape=out_shape,
                )

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
                ah_df = (
                    cast(pd.DataFrame, df.groupby("level", as_index=False)["area"].sum())
                    .sort_values(by="level")
                    .reset_index(drop=True)
                )
                ah_df["area"] = ah_df["area"].cumsum().round(1)
                ah_df["comment"] = pd.Series(dtype=str)

                mask = ah_df.area < MIN_PROFILE_AREA
                ah_df.loc[mask, ["area"]] = MIN_PROFILE_AREA
                ah_df.loc[mask, ["comment"]] = "oppervlak >= 10m2 gezet"
                ah_df = simplify_area_level_df(ah_df)

                if ah_df.empty:
                    ah_df = default_ah_df()
            else:
                ah_df = default_ah_df()
    # Return profile
    mask = ah_df.area < MIN_PROFILE_AREA
    ah_df.loc[mask, ["area"]] = MIN_PROFILE_AREA
    ah_df.loc[mask, ["comment"]] = "oppervlak >= 10m2 gezet"
    return basin.Profile(area=ah_df.area, level=ah_df.level, meta_comment=ah_df.comment)


class VdGaastBerging:
    def __init__(
        self,
        model: Model,
        cloud: CloudStorage,
        manning_basin_ids: set[int] | None = None,
    ) -> None:
        """Add a storing ('bergend') basin to every basin of the model

        A storing basin drains through a TabulatedRatingCurve, based on Van der Gaast. In the basins of
        `manning_basin_ids` it is connected with a ManningResistance instead, so it can also be supplied.

        Args:
            model (Model): Ribasim model
            cloud (CloudStorage): Cloud storage with the LHM and Van der Gaast rasters
            manning_basin_ids (set[int] | None, optional): Basins whose storing basin is connected with a
                ManningResistance, see `supply_area_basin_ids`. Defaults to None.
        """
        self.model = model
        self.cloud = cloud
        self.manning_basin_ids = set() if manning_basin_ids is None else manning_basin_ids

        lhm_raster_file = self.cloud.joinpath("Basisgegevens/LHM/4.3/input/LHM_data.tif")
        ma_raster_file = self.cloud.joinpath("Basisgegevens/VanDerGaast_QH/spafvoer1.tif")
        self.lhm_raster_file = lhm_raster_file
        self.ma_raster_file = ma_raster_file

    @with_rasterio_env
    def add(self) -> None:
        model = self.model
        unknown_ids = self.manning_basin_ids - set(model.filter_nodes("Basin").index)
        assert not unknown_ids, f"manning_basin_ids are not Basins in the model: {sorted(unknown_ids)}"

        # get basin_area and add statistics
        print("compute basin statistics")
        assert model.basin.area.df is not None
        basin_area_df = model.basin.area.df.dissolve("node_id").copy()

        basin_area_df = add_basin_statistics(
            df=basin_area_df, lhm_raster_file=self.lhm_raster_file, ma_raster_file=self.ma_raster_file
        )

        basin_node_df = model.filter_nodes("Basin")
        node_pairs = []
        for row in tqdm.tqdm(basin_node_df.itertuples(), total=len(basin_node_df), desc="add storage nodes"):
            # get basin_id and basin polygon
            basin_id = row.Index
            basin_row = basin_area_df.loc[basin_id]
            basin_polygon = basin_row.geometry

            if any(pd.isna(getattr(basin_row, i)) for i in ["ghg", "glg", "ma"]):
                raise ValueError(f"No valid ghg, glg and/or ma for basin_id {basin_id}")

            # define storage basin data
            max_level = cast(float, basin_area_df.at[basin_id, "maaiveld_max"])
            min_level = cast(float, basin_area_df.at[basin_id, "maaiveld_min"])

            if min_level == max_level:
                min_level -= 0.1
            basin_profile = get_basin_profile(
                basin_polygon=basin_polygon,
                max_level=max_level,
                min_level=min_level,
                lhm_raster_file=self.lhm_raster_file,
            )
            assert basin_profile.df is not None
            oppervlaktewater_percentage = round(basin_profile.df.area.max() / basin_polygon.area * 100, 1)
            ini_level = max(
                (basin_profile.df.level.min() + 0.01, basin_row["meta_streefpeil"])
            )  # 1cm above target-level/bottom-level
            data = [
                basin_profile,
                basin.State(level=[ini_level]),
                basin.Area(geometry=[basin_polygon], meta_oppervlaktewater_percentage=[oppervlaktewater_percentage]),
            ]

            # add storage basin

            node = Node(
                meta_categorie="bergend",
                geometry=Point(row.geometry.x + 10, row.geometry.y),
            )
            basin_node = model.basin.add(node=node, tables=data)

            # connect storage basin with basin with a ManningResistance or a TabulatedRatingCurve
            if basin_id in self.manning_basin_ids:
                node_type = "ManningResistance"
                data = [get_manning_resistance(area=basin_polygon.area)]
            else:
                node_type = "TabulatedRatingCurve"
                data = [get_rating_curve(row=basin_row, min_level=basin_profile.df.level.min())]

            # links are added at once after the loop, which is much faster
            connector_node = model.add_node(
                node_type=node_type,
                geometry=Point(row.geometry.x + 5, row.geometry.y),
                tables=data,
                meta_categorie="bergend",
            )
            node_pairs += [(basin_node, connector_node), (connector_node, model.get_node(basin_id))]

        model.add_links(node_pairs)
