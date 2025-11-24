# %%

import geopandas as gpd
import pandas as pd
from ribasim import Node
from ribasim.nodes import basin
from ribasim_nl.berging import add_basin_statistics, get_basin_profile, get_rating_curve
from ribasim_nl.geometry import basin_to_point
from shapely.geometry import LineString

from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()

ribasim_toml = cloud.joinpath("DeDommel/modellen/DeDommel_parameterized/model.toml")
model = Model.read(ribasim_toml)

basin_area_df = gpd.read_file(cloud.joinpath("DeDommel/verwerkt/basin_area.gpkg"), engine="pyogrio", fid_as_index=True)
basin_area_df.set_index("node_id", inplace=True)


lhm_raster_file = cloud.joinpath("Basisgegevens/LHM/4.3/input/LHM_data.tif")
ma_raster_file = cloud.joinpath("Basisgegevens/VanDerGaast_QH/spafvoer1.tif")


basin_area_df = add_basin_statistics(df=basin_area_df, lhm_raster_file=lhm_raster_file, ma_raster_file=ma_raster_file)


# %%update model
link_id = model.link.df.index.max() + 1
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
        basin_profile = get_basin_profile(
            basin_polygon=basin_polygon,
            polygon=polygon,
            max_level=max_level,
            min_level=min_level,
            lhm_raster_file=lhm_raster_file,
        )
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

        # add links
        link_id += 1  # FIXME: can be removed if issue is closed https://github.com/Deltares/Ribasim/issues/1804
        model.link.add(basin_node, tbr_node, link_id=link_id, meta_categorie="bergend")
        link_id += 1
        model.link.add(tbr_node, model.basin[node_id], link_id=link_id, meta_categorie="bergend")

    else:
        print(f"Geen basin-vlak voor {node_id}")

# %%
df = pd.DataFrame({"node_id": model.basin.node.df.index.to_list()})
df.index.name = "fid"
df.loc[:, "precipitation"] = 5.787037e-08
df.loc[:, "potential_evaporation"] = 1.157407e-08
df.loc[:, "drainage"] = 0
df.loc[:, "infiltration"] = 0
model.basin.static.df = df

# %%
ribasim_toml = cloud.joinpath("DeDommel/modellen/DeDommel_bergend/model.toml")

model.write(ribasim_toml)
