# %%
import geopandas as gpd
import pandas as pd
from shapely.geometry import MultiPolygon

from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()


# %% load model
ribasim_toml = cloud.joinpath("DeDommel", "modellen", "DeDommel_fix_edges", "model.toml")
model = Model.read(ribasim_toml)


area_gdf = gpd.read_file(cloud.joinpath("DeDommel", "verwerkt", "watervlakken", "LWW_2023_A_water_vlak_V.shp"))


named_area_gdf = area_gdf.dissolve("NAAM", dropna=True).reset_index()
unnamed_area_poly = area_gdf[area_gdf.NAAM.isna()].buffer(0.1).union_all().buffer(-0.1)
unnamed_area_gdf = gpd.GeoDataFrame(geometry=list(unnamed_area_poly.geoms), crs=area_gdf.crs)

dissolved_area_gdf = pd.concat([unnamed_area_gdf, named_area_gdf])
dissolved_area_gdf.to_file(cloud.joinpath("DeDommel", "verwerkt", "water_area.gpkg"))

# %%
basin_area_gpkg = cloud.joinpath("DeDommel", "verwerkt", "basin_area.gpkg")
basin_area_df = model.basin.area.df
basin_area_df.to_file(basin_area_gpkg)
basin_area_df.set_index("node_id", inplace=True)

basin_df = model.basin.node.df

# %%
data = []
ignore_basins = [1278, 1228, 1877, 1030]
row = next(i for i in basin_df.itertuples() if i.Index == 1230)
for row in basin_df.itertuples():
    if row.Index not in ignore_basins:
        edges_mask = (model.edge.df.from_node_id == row.Index) | (model.edge.df.to_node_id == row.Index)
        edges_geom = model.edge.df[edges_mask].union_all()

        selected_areas = dissolved_area_gdf[dissolved_area_gdf.intersects(edges_geom)]

        basin_geom = gpd.clip(selected_areas, basin_area_df.at[row.Index, "geometry"]).union_all()
        if isinstance(basin_geom, MultiPolygon):
            basin_geom = MultiPolygon(i for i in basin_geom.geoms if i.area > 100)

        data += [{"node_id": row.Index, "geometry": basin_geom}]

        # set name to basin if empty
        name = ""
        area_df = selected_areas[selected_areas.contains(row.geometry)]
        if not area_df.empty:
            name = area_df.iloc[0].NAAM
        model.basin.node.df.loc[row.Index, ["name"]] = name
        # assign name to edges if defined
        model.edge.df.loc[edges_mask, ["name"]] = name

# Manuals
for node_id, name in [(1228, "Beatrixkanaal"), (1278, "Eindhovens kanaal")]:
    data += [{"node_id": node_id, "geometry": dissolved_area_gdf[dissolved_area_gdf.NAAM == name].iloc[0].geometry}]
    edges_mask = (model.edge.df.from_node_id == node_id) | (model.edge.df.to_node_id == node_id)
    model.edge.df.loc[edges_mask, "name"] = name

node_id, name = (1030, "Reusel")
data += [
    {
        "node_id": node_id,
        "geometry": dissolved_area_gdf[dissolved_area_gdf.NAAM == name]
        .clip(basin_area_df.loc[[1015, 1030]].union_all())
        .union_all(),
    }
]
edges_mask = (model.edge.df.from_node_id == node_id) | (model.edge.df.to_node_id == node_id)
model.edge.df.loc[edges_mask, "name"] = name

area_df = gpd.GeoDataFrame(data, crs=model.basin.node.df.crs)
area_df = area_df[~area_df.is_empty]
area_df.index.name = "fid"
mask = area_df.geometry.type == "Polygon"
area_df.loc[mask, "geometry"] = area_df.geometry[mask].apply(lambda x: MultiPolygon([x]))
model.basin.area.df = area_df
# %%
ribasim_toml = cloud.joinpath("DeDommel", "modellen", "DeDommel_fix_areas", "model.toml")
model.write(ribasim_toml)
