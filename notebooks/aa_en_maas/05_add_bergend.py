# %%
import geopandas as gpd
import pandas as pd
from ribasim import Node
from ribasim.nodes import basin
from shapely.geometry import LineString, Point

from ribasim_nl import CloudStorage, Model
from ribasim_nl.berging import add_basin_statistics, get_basin_profile, get_rating_curve

MODEL_EXEC: bool = False

# model settings
AUTHORITY: str = "AaenMaas"
SHORT_NAME: str = "aam"
MODEL_ID: str = "2025_7_0"

# connect with the GoodCloud
cloud = CloudStorage()

# collect relevant data from the GoodCloud
ribasim_model_dir = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_parameterized_model")
ribasim_toml = ribasim_model_dir / f"{SHORT_NAME}.toml"

model = Model.read(ribasim_toml)


# get basin_area and add statistics
basin_area_df = model.basin.area.df.set_index("node_id").copy()
lhm_raster_file = cloud.joinpath("Basisgegevens", "LHM", "4.3", "input", "LHM_data.tif")
ma_raster_file = cloud.joinpath("Basisgegevens", "VanDerGaast_QH", "spafvoer1.tif")

print("compute basin statistics")
basin_area_df = add_basin_statistics(df=basin_area_df, lhm_raster_file=lhm_raster_file, ma_raster_file=ma_raster_file)

link_data = []
print("add basins")
for row in model.basin.node.df.itertuples():
    # get basin_id and basin polygon
    basin_id = row.Index
    basin_row = basin_area_df.loc[basin_id]
    basin_polygon = basin_row.geometry

    # define basin_node
    node = Node(
        meta_categorie="bergend",
        geometry=Point(row.geometry.x + 10, row.geometry.y),
    )

    max_level = max_level = basin_area_df.at[basin_id, "maaiveld_max"]
    min_level = max_level = basin_area_df.at[basin_id, "maaiveld_min"]
    if min_level == max_level:
        min_level -= 0.1
    basin_profile = get_basin_profile(
        basin_polygon=basin_polygon,
        polygon=basin_polygon,
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

    node = Node(
        meta_categorie="bergend",
        geometry=Point(row.geometry.x + 5, row.geometry.y),
    )
    if any(pd.isna(getattr(basin_row, i)) for i in ["ghg", "glg", "ma"]):
        raise ValueError(f"No valid ghg, glg and/or ma for basin_id {basin_id}")
    else:
        data = [get_rating_curve(row=basin_row, min_level=basin_profile.df.level.min())]
    connector_node = model.tabulated_rating_curve.add(node=node, tables=data)

    # add edges
    link_data += [
        {
            "name": "",
            "from_node_id": basin_node.node_id,
            "to_node_id": connector_node.node_id,
            "geometry": LineString([basin_node.geometry, connector_node.geometry]),
        },
        {
            "name": "",
            "from_node_id": connector_node.node_id,
            "to_node_id": model.basin[basin_id].node_id,
            "geometry": LineString([connector_node.geometry, model.basin[basin_id].geometry]),
        },
    ]
    start = model.link.df.index.max() + 1
    stop = start + len(link_data)
    index = pd.RangeIndex(start=start, stop=stop, step=1)
    df = gpd.GeoDataFrame(link_data, index=index, crs=28992)
    # model.link.add(basin_node, connector_node, meta_categorie="bergend")
    # model.link.add(connector_node, model.basin[basin_id], meta_categorie="bergend")
    model.link.df = pd.concat([model.link.df, df])

ribasim_model_dir = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_bergend_model")
ribasim_toml = ribasim_model_dir / f"{SHORT_NAME}.toml"
model.write(ribasim_toml)
