# %%

import geopandas as gpd
import numpy as np
import numpy.typing as npt
import pandas as pd
import rasterio
from rasterio.windows import from_bounds
from rasterstats import zonal_stats
from ribasim import Node
from ribasim.nodes import basin, tabulated_rating_curve
from ribasim_nl import CloudStorage, Model
from ribasim_nl.geometry import basin_to_point
from shapely.geometry import LineString

cloud = CloudStorage()

ribasim_toml = cloud.joinpath("DeDommel", "modellen", "DeDommel_parameterized", "model.toml")
model = Model.read(ribasim_toml)

banden = {
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

basin_area_df = gpd.read_file(
    cloud.joinpath("DeDommel", "verwerkt", "basin_area.gpkg"), engine="pyogrio", fid_as_index=True
)
basin_area_df.set_index("node_id", inplace=True)

lhm_rasters = cloud.joinpath("Basisgegevens", "LHM", "4.3", "input", "LHM_data.tif")
ma_raster = cloud.joinpath("Basisgegevens", "VanDerGaast_QH", "spafvoer1.tif")


def sample_raster(
    raster_file,
    df,
    band=1,
    fill_value: float | None = None,
    all_touched=False,
    stats="mean",
    maaiveld_data: npt.ArrayLike | None = None,
):
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


def get_rating_curve(row, min_level, maaiveld: None | float = None):
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


def get_basin_profile(basin_polygon, polygon, max_level, min_level):
    with rasterio.open(lhm_rasters) as src:
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
    df.sort_values(by="level", inplace=True)
    df = df.set_index("level").cumsum().reset_index()
    df.dropna(inplace=True)
    df.drop_duplicates("level", keep="last", inplace=True)

    # Return profile
    return basin.Profile(area=df.area, level=df.level)


# %% add columns

with rasterio.open(lhm_rasters) as raster_src:
    # read band
    maaiveld_data = raster_src.read(banden["maaiveld"])

# sample rasters
ghg = sample_raster(
    raster_file=lhm_rasters,
    df=basin_area_df,
    band=banden["ghg_2010-2019"],
    all_touched=True,
    maaiveld_data=maaiveld_data,
)
basin_area_df.loc[:, ["ghg"]] = pd.Series(dtype=float)
basin_area_df.loc[:, ["ghg"]] = [i["mean"] for i in ghg]

glg = sample_raster(
    raster_file=lhm_rasters,
    df=basin_area_df,
    band=banden["glg_2010-2019"],
    all_touched=True,
    maaiveld_data=maaiveld_data,
)
basin_area_df.loc[:, ["glg"]] = pd.Series(dtype=float)
basin_area_df.loc[:, ["glg"]] = [i["mean"] for i in glg]

ma = sample_raster(raster_file=ma_raster, df=basin_area_df, all_touched=True, fill_value=37)  # 37mm/dag is
basin_area_df.loc[:, ["ma"]] = pd.Series(dtype=float)
basin_area_df.loc[:, ["ma"]] = [i["mean"] for i in ma]

maaiveld = sample_raster(
    raster_file=lhm_rasters, df=basin_area_df, band=banden["maaiveld"], stats="mean min max", all_touched=True
)
basin_area_df.loc[:, ["maaiveld"]] = pd.Series(dtype=float)
basin_area_df.loc[:, ["maaiveld"]] = [i["mean"] if pd.isna(i["mean"]) else i["mean"] for i in maaiveld]
basin_area_df.loc[:, ["maaiveld_max"]] = [i["max"] if pd.isna(i["max"]) else i["max"] for i in maaiveld]
basin_area_df.loc[:, ["maaiveld_min"]] = [i["min"] if pd.isna(i["min"]) else i["min"] for i in maaiveld]
# %%update model
edge_id = model.edge.df.index.max() + 1
for row in model.basin.node.df.itertuples():
    # row = next(row for row in model.basin.node.df.itertuples() if row.Index == 1013)
    node_id = row.Index

    if node_id in basin_area_df.index:
        # basin-polygon
        basin_row = basin_area_df.loc[node_id]
        basin_polygon = basin_area_df.at[node_id, "geometry"]

        # add basin-node
        basin_node_id = (
            model.next_node_id
        )  # FIXME: can be removed if issue is closed https://github.com/Deltares/Ribasim/issues/1805
        geometry = basin_to_point(basin_polygon=basin_polygon, tolerance=10)
        node = Node(
            node_id=basin_node_id,
            meta_categorie="bergend",
            geometry=geometry,
        )

        if node_id in model.basin.area.df.node_id.to_list():
            polygon = model.basin.area.df.set_index("node_id").at[node_id, "geometry"]
        else:
            polygon = None

        max_level = max_level = basin_area_df.at[node_id, "maaiveld_max"]
        min_level = max_level = basin_area_df.at[node_id, "maaiveld_min"]
        if min_level == max_level:
            min_level -= 0.1
        basin_profile = get_basin_profile(basin_polygon, polygon, max_level=max_level, min_level=min_level)
        data = [
            basin_profile,
            basin.State(level=[basin_profile.df.level.min() + 0.1]),
            basin.Area(geometry=[basin_polygon]),
        ]
        basin_node = model.basin.add(node=node, tables=data)

        # get line
        line = LineString([geometry, row.geometry])

        # add tabulated rating curve
        tbr_node_id = model.next_node_id
        geometry = line.interpolate(0.5, normalized=True)
        node = Node(
            node_id=tbr_node_id,
            meta_categorie="bergend",
            geometry=geometry,
        )
        if any(pd.isna(getattr(basin_row, i)) for i in ["ghg", "glg", "ma"]):
            raise ValueError(f"No valid ghg, glg and/or ma for basin_id {node_id}")
        else:
            data = [get_rating_curve(row=basin_row, min_level=basin_profile.df.level.min())]
        tbr_node = model.tabulated_rating_curve.add(node=node, tables=data)

        # add edges
        edge_id += 1  # FIXME: can be removed if issue is closed https://github.com/Deltares/Ribasim/issues/1804
        model.edge.add(basin_node, tbr_node, edge_id=edge_id, meta_categorie="bergend")
        edge_id += 1
        model.edge.add(tbr_node, model.basin[node_id], edge_id=edge_id, meta_categorie="bergend")

    else:
        print(f"Geen basin-vlak voor {node_id}")

# %%
ribasim_toml = cloud.joinpath("DeDommel", "modellen", "DeDommel_bergend", "model.toml")

model.write(ribasim_toml)
