# %%
import geopandas as gpd
import pandas as pd
from ribasim import Node
from ribasim.nodes import basin
from ribasim_nl import CloudStorage, Model
from ribasim_nl.berging import add_basin_statistics, get_basin_profile, get_rating_curve
from ribasim_nl.geodataframe import split_basins
from ribasim_nl.geometry import basin_to_point
from shapely.geometry import LineString, MultiPolygon

cloud = CloudStorage()

# %% RWS-HWS
model_path = cloud.joinpath("Rijkswaterstaat", "modellen", "hws")
toml_file = model_path / "hws.toml"
rws_model = Model.read(toml_file)

# %% DeDommel
model_path = cloud.joinpath("DeDommel", "modellen", "DeDommel")
toml_file = model_path / "model.toml"
model = Model.read(toml_file)
basin_polygon = model.basin.area.df[model.basin.area.df.node_id != 1228].union_all()

drainage_area = gpd.read_file(
    cloud.joinpath("DeDommel", "verwerkt", "4_ribasim", "areas.gpkg"), layer="drainage_areas"
).union_all()


rws_selected_areas_df = rws_model.basin.area.df[rws_model.basin.area.df.intersects(drainage_area.buffer(-10))]
rws_selected_areas = rws_selected_areas_df.union_all()

poly = (
    rws_model.basin.area.df[rws_model.basin.area.df.intersects(drainage_area.buffer(-10))]
    .buffer(0.1)
    .union_all()
    .buffer(3000)
)
poly = poly.difference(basin_polygon).intersection(drainage_area)

berging_basins_df = gpd.GeoDataFrame(geometry=gpd.GeoSeries(poly.geoms, crs=28992))

berging_basins_df = berging_basins_df[berging_basins_df.geom_type == "Polygon"]
berging_basins_df = berging_basins_df[berging_basins_df.intersects(rws_selected_areas)]
berging_basins_df = berging_basins_df[berging_basins_df.area > 50]

cut_lines_df = gpd.read_file(cloud.joinpath("Rijkswaterstaat", "verwerkt", "couple_user_data.gpkg"), layer="cut_lines")

berging_basins_df = split_basins(berging_basins_df, cut_lines_df)
berging_basins_df = berging_basins_df[berging_basins_df.intersects(rws_selected_areas)]


rws_selected_basins_df = rws_model.basin.node.df[rws_model.basin.node.df.index.isin(rws_selected_areas_df.node_id)]

berging_basins_df.loc[:, "node_id"] = berging_basins_df.geometry.apply(
    lambda x: rws_selected_basins_df.distance(x).idxmin()
)

berging_basins_df.to_file(cloud.joinpath("Rijkswaterstaat", "verwerkt", "bergende_basins_rws.gpkg"))


# %%
rws_model.update_meta_properties({"meta_categorie": "hoofdwater"})


# %% toevoegen bergende gebieden
basin_area_df = gpd.read_file(cloud.joinpath("Rijkswaterstaat", "verwerkt", "bergende_basins_rws.gpkg"))
basin_area_df.set_index("node_id", inplace=True)
lhm_raster_file = cloud.joinpath("Basisgegevens", "LHM", "4.3", "input", "LHM_data.tif")
ma_raster_file = cloud.joinpath("Basisgegevens", "VanDerGaast_QH", "spafvoer1.tif")
basin_area_df = add_basin_statistics(df=basin_area_df, lhm_raster_file=lhm_raster_file, ma_raster_file=ma_raster_file)

# %%
edge_id = rws_model.edge.df.index.max() + 1
for row in rws_model.basin.node.df[rws_model.basin.node.df.index.isin(basin_area_df.index)].itertuples():
    # row = next(row for row in model.basin.node.df.itertuples() if row.Index == 1013)
    node_id = row.Index

    if node_id in basin_area_df.index:
        # basin-polygon
        basin_row = basin_area_df.loc[node_id]
        basin_polygon = basin_area_df.at[node_id, "geometry"]

        # add basin-node
        basin_node_id = (
            rws_model.next_node_id
        )  # FIXME: can be removed if issue is closed https://github.com/Deltares/Ribasim/issues/1805
        geometry = basin_to_point(basin_polygon=basin_polygon, tolerance=10)
        node = Node(
            node_id=basin_node_id,
            meta_categorie="bergend",
            geometry=geometry,
        )

        if node_id in rws_model.basin.area.df.node_id.to_list():
            polygon = rws_model.basin.area.df.set_index("node_id").at[node_id, "geometry"]
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
            basin.Area(geometry=[MultiPolygon([basin_polygon])]),
        ]
        basin_node = rws_model.basin.add(node=node, tables=data)

        # get line
        line = LineString([geometry, row.geometry])

        # add tabulated rating curve
        tbr_node_id = rws_model.next_node_id
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
        tbr_node = rws_model.tabulated_rating_curve.add(node=node, tables=data)

        # add edges
        edge_id += 1  # FIXME: can be removed if issue is closed https://github.com/Deltares/Ribasim/issues/1804
        rws_model.edge.add(basin_node, tbr_node, edge_id=edge_id, meta_categorie="bergend")
        edge_id += 1
        rws_model.edge.add(tbr_node, rws_model.basin[node_id], edge_id=edge_id, meta_categorie="bergend")

    else:
        print(f"Geen basin-vlak voor {node_id}")

# %%
model_path = cloud.joinpath("Rijkswaterstaat", "modellen", "hws_bergend")
toml_file = model_path / "hws.toml"

rws_model.write(toml_file)
