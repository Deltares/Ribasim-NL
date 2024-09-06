# %%
import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterstats import zonal_stats
from ribasim import Node
from ribasim.nodes import basin, tabulated_rating_curve
from ribasim_nl import CloudStorage, Model
from ribasim_nl.geometry import basin_to_point
from shapely.geometry import LineString

cloud = CloudStorage()

ribasim_toml = cloud.joinpath("DeDommel", "modellen", "DeDommel_parameterized", "model.toml")
model = Model.read(ribasim_toml)


basin_area_df = gpd.read_file(
    cloud.joinpath("DeDommel", "verwerkt", "basin_area.gpkg"), engine="pyogrio", fid_as_index=True
)
basin_area_df.set_index("node_id", inplace=True)

maaiveld_raster = cloud.joinpath("Basisgegevens", "LHM", "4.3", "GxG", "Clipped_AHN_F250_IN_MNAP.tif")
ghg_raster = cloud.joinpath("Basisgegevens", "LHM", "4.3", "GxG", "GHG_LHM431_1991-2020_CORRECTED_L_tov_MV.tif")
glg_raster = cloud.joinpath("Basisgegevens", "LHM", "4.3", "GxG", "GLG_LHM431_1991-2020_CORRECTED_L_tov_MV.tif")
ma_raster = cloud.joinpath("Rijkswaterstaat", "verwerkt", "bergingsknopen", "spafvoer1.tif")


def sample_raster(raster_file, df, all_touched=False):
    with rasterio.open(raster_file) as raster_src:
        data = raster_src.read(1)
        affine = raster_src.transform

        return zonal_stats(df, data, affine=affine, stats="mean", nodata=raster_src.nodata, all_touched=all_touched)


def get_rating_curve(row, area, maaiveld: None | float = None):
    flow_rate = np.round([0, row.ma * 0.2, row.ma * 0.33, row.ma / 2, row.ma * 2], decimals=2)
    level = np.round([row.glg + 1, row.glg, row.ghg / 2, row.ghg, 0], decimals=2)

    # level relative to maaiveld
    if maaiveld is not None:
        level = maaiveld - level
    else:
        level = row.maaiveld - level

    # flow_rate in m3/s
    flow_rate = flow_rate / 1000 * area / 86400

    return tabulated_rating_curve.Static(level=level, flow_rate=flow_rate)


# %% add columns

# sample rasters
ghg = sample_raster(ghg_raster, basin_area_df, all_touched=True)
basin_area_df.loc[:, ["ghg"]] = pd.Series(dtype=float)
basin_area_df.loc[:, ["ghg"]] = [i["mean"] for i in ghg]

glg = sample_raster(glg_raster, basin_area_df, all_touched=True)
basin_area_df.loc[:, ["glg"]] = pd.Series(dtype=float)
basin_area_df.loc[:, ["glg"]] = [i["mean"] for i in glg]

ma = sample_raster(ma_raster, basin_area_df, all_touched=True)
basin_area_df.loc[:, ["ma"]] = pd.Series(dtype=float)
basin_area_df.loc[:, ["ma"]] = [i["mean"] for i in ma]

maaiveld = sample_raster(glg_raster, basin_area_df, all_touched=True)
basin_area_df.loc[:, ["maaiveld"]] = pd.Series(dtype=float)
basin_area_df.loc[:, ["maaiveld"]] = [i["mean"] if pd.isna(i["mean"]) else i["mean"] for i in maaiveld]

# %%

# update model
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
        data = [
            basin.Profile(level=[0, 1], area=[5, round(basin_polygon.area)]),
            basin.State(level=[1]),
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
            data = [tabulated_rating_curve.Static(level=[0, 1], flow_rate=[0, 1])]
        else:
            data = [get_rating_curve(row=basin_row, area=row.geometry.area)]
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
