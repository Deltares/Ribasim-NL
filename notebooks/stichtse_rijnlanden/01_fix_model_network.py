# %%
import geopandas as gpd
import numpy as np
import pandas as pd
from ribasim import Node
from ribasim.nodes import basin, level_boundary, manning_resistance, outlet, tabulated_rating_curve
from shapely.geometry import LineString, MultiPolygon, Point

from ribasim_nl import CloudStorage, Model, NetworkValidator
from ribasim_nl.geometry import split_basin_multi_polygon

cloud = CloudStorage()

authority = "StichtseRijnlanden"
short_name = "hdsr"

ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_2024_6_3", f"{short_name}.toml")
database_gpkg = ribasim_toml.with_name("database.gpkg")
split_lijnen_df = gpd.read_file(
    cloud.joinpath("StichtseRijnlanden", "verwerkt", "modelfouten_met_verbeter_acties_BD_311024.gpkg"),
    layer="area_split_lijnen",
    fid_as_index=True,
)

extra_area_df = gpd.read_file(
    cloud.joinpath("StichtseRijnlanden", "verwerkt", "modelfouten_met_verbeter_acties_BD_311024.gpkg"),
    layer="nieuwe_areas_voor_opvulling",
    fid_as_index=True,
)

hydroobject_gdf = gpd.read_file(
    cloud.joinpath(authority, "verwerkt", "4_ribasim", "hydamo.gpkg"), layer="hydroobject", fid_as_index=True
)

afsluitmiddel_gdf = gpd.read_file(
    cloud.joinpath(authority, "verwerkt", "4_ribasim", "hydamo.gpkg"), layer="afsluitmiddel", fid_as_index=True
)


# %% read model
model = Model.read(ribasim_toml)
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_fix_model_network", f"{short_name}.toml")
network_validator = NetworkValidator(model)


# %% some stuff we'll need again
manning_data = manning_resistance.Static(length=[100], manning_n=[0.04], profile_width=[10], profile_slope=[1])
level_data = level_boundary.Static(level=[0])

basin_data = [
    basin.Profile(level=[0.0, 1.0], area=[0.01, 1000.0]),
    basin.Static(
        drainage=[0.0],
        potential_evaporation=[0.001 / 86400],
        infiltration=[0.0],
        precipitation=[0.005 / 86400],
    ),
    basin.State(level=[0]),
]
outlet_data = outlet.Static(flow_rate=[100])

tabulated_rating_curve_data = tabulated_rating_curve.Static(level=[0.0, 5], flow_rate=[0, 0.1])

# HIER KOMEN ISSUES

# %% https://github.com/Deltares/Ribasim-NL/issues/153#issuecomment-2455143137

# verwijderen duplicated edges
model.edge.df.drop_duplicates(inplace=True)


# %%
model.remove_edges(edge_ids=[2677])
for node_id in [52, 53, 2056, 2053, 2042]:
    model.remove_node(node_id, remove_edges=True)

model.edge.add(model.tabulated_rating_curve[924], model.level_boundary[51])
model.edge.add(model.tabulated_rating_curve[937], model.level_boundary[51])
model.edge.add(model.level_boundary[38], model.outlet[85])


# %% see: https://github.com/Deltares/Ribasim-NL/issues/153#issuecomment-2444112017
# toevoegen ontbrekende basins

basin_edges_df = network_validator.edge_incorrect_connectivity()
basin_nodes_df = network_validator.node_invalid_connectivity()

for row in basin_nodes_df.itertuples():
    # maak basin-node
    basin_node = model.basin.add(Node(geometry=row.geometry), tables=basin_data)

    # update edge_table
    model.edge.df.loc[basin_edges_df[basin_edges_df.from_node_id == row.node_id].index, ["from_node_id"]] = (
        basin_node.node_id
    )
    model.edge.df.loc[basin_edges_df[basin_edges_df.to_node_id == row.node_id].index, ["to_node_id"]] = (
        basin_node.node_id
    )

# %% see: https://github.com/Deltares/Ribasim-NL/issues/153#issuecomment-2456843484

# basins verwijderen

# basins samenvoegen
for basin_id, to_basin_id in [
    [1990, 1445],
    [1395, 1394],
    [1459, 1394],
    [1759, 1575],
    [1798, 1836],
    [1827, 1819],
    [1936, 2028],
    [1937, 2028],
    [2029, 1383],
    [2040, 1640],
    [2043, 1812],
    [2048, 1605],
    [2076, 2088],
    [2077, 1461],
    [2080, 2089],
    [2081, 1863],
    [2082, 1423],
    [2083, 1675],
    [1678, 1671],
    [1934, 1563],
    [2074, 1469],
]:
    model.merge_basins(basin_id=basin_id, to_basin_id=to_basin_id)

model.merge_basins(basin_id=1662, to_basin_id=1745, are_connected=False)
model.update_node(
    node_id=1336,
    node_type="TabulatedRatingCurve",
    data=[tabulated_rating_curve_data],
    node_properties={"name": "ST1999", "meta_object_type": "stuw"},
)

# verwijderen knopen
for node_id in [2078, 733]:
    model.remove_node(node_id, remove_edges=True)

# toevoegen edge
model.reverse_edge(edge_id=876)
model.edge.add(model.pump[565], model.level_boundary[56])

# updaten naar level_boundary
model.update_node(1965, "LevelBoundary", data=[level_data])


# %% https://github.com/Deltares/Ribasim-NL/issues/153#issuecomment-2456913415

# opheffen ST3122
geometry = Point(117995, 443062.5)
model.move_node(node_id=889, geometry=geometry)
model.tabulated_rating_curve.node.df.loc[889, "name"] = "ST1985"

for edge_id in [359, 360, 361]:
    model.redirect_edge(edge_id=edge_id, to_node_id=1572)

model.basin.area.df.at[1, "geometry"].buffer(0.1).buffer(-0.1)

result = split_basin_multi_polygon(
    MultiPolygon([model.basin.area.df.at[1, "geometry"].buffer(0.1).buffer(-0.1)]),
    line=split_lijnen_df.at[28, "geometry"],
)
model.basin.area.df.loc[1, "geometry"] = result[1]
model.basin.area.df.loc[480, "geometry"] = model.basin.area.df.at[480, "geometry"].union(result[0])


# %% https://github.com/Deltares/Ribasim-NL/issues/153#issuecomment-2457283397

# oplossen Loperiksingel/Nassaukade bij rand HHSK
geometry, _ = split_basin_multi_polygon(model.basin.area.df.at[135, "geometry"], line=split_lijnen_df.at[1, "geometry"])
model.basin.area.df.loc[135, "geometry"] = geometry
boundary_node = model.level_boundary.add(Node(geometry=model.level_boundary[75].geometry), tables=[level_data])

outlet_geom, basin_geom = hydroobject_gdf.at[10466, "geometry"].boundary.geoms
model.move_node(75, geometry=basin_geom)
model.update_node(node_id=75, node_type="Basin", data=basin_data)

outlet_node = model.outlet.add(Node(geometry=outlet_geom), tables=[outlet_data])
model.edge.add(model.basin[75], outlet_node)
model.edge.add(outlet_node, boundary_node)


# %% https://github.com/Deltares/Ribasim-NL/issues/153#issuecomment-2457334403

# Toevoegen inlaat en basin bij Sportpark Het Hofland Schoonhoven
hydro_object = hydroobject_gdf.at[8099, "geometry"]
afsluitmiddel = afsluitmiddel_gdf.loc[91]
outlet_node = model.outlet.add(
    Node(
        geometry=hydro_object.interpolate(hydro_object.project(afsluitmiddel.geometry)),
        name=afsluitmiddel.code,
        meta_object_type="afsluitmiddel",
    ),
    tables=[outlet_data],
)

basin_node = model.basin.add(Node(geometry=hydroobject_gdf.at[7950, "geometry"].boundary.geoms[0]), tables=basin_data)
model.redirect_edge(edge_id=2721, from_node_id=basin_node.node_id)
model.edge.add(model.level_boundary[74], outlet_node)
model.edge.add(outlet_node, basin_node)

# %% https://github.com/Deltares/Ribasim-NL/issues/153#issuecomment-2457385390

# Basin / Area Rijnsweerd mergen met Utrecht Centrum
geometry = model.basin.area.df.at[501, "geometry"]
model.basin.area.df.loc[194, "geometry"] = model.basin.area.df.at[194, "geometry"].union(geometry)
model.basin.area.df = model.basin.area.df[~model.basin.area.df.index.isin([501, 541])]

# %% https://github.com/Deltares/Ribasim-NL/issues/153#issuecomment-2457447764

# Split Basin / Area met area_split_lijnen


# group split_lines by basin_fid
# basin_fid, split_lijnen_select_df = list(split_lijnen_df[split_lijnen_df.opmerkingen.isna()].groupby("basin_area_fid"))[
#     0
# ]
new_basin_area_df = gpd.GeoSeries([], crs=model.crs)
for basin_fid, split_lijnen_select_df in split_lijnen_df[split_lijnen_df.opmerkingen.isna()].groupby("basin_area_fid"):
    # iterate trough split_lines per basin creating a series with geometries
    series = gpd.GeoSeries([model.basin.area.df.at[basin_fid, "geometry"]], crs=model.crs)

    for row in split_lijnen_select_df.itertuples():
        if not len(series[series.intersects(row.geometry)]) == 1:
            raise ValueError(f"line with fid {row.Index} intersects basin with fid {basin_fid} more than once")

        # select polygon
        fid = series[series.intersects(row.geometry)].index[0]
        try:
            result = split_basin_multi_polygon(series.loc[fid], line=row.geometry)
        except ValueError:
            result = split_basin_multi_polygon(series.loc[fid], line=LineString(row.geometry.boundary.geoms))

        series.loc[fid] = result[0]
        series = pd.concat([series, gpd.GeoSeries([result[1]], crs=series.crs)], ignore_index=True)

    # update existing basin / area if basin / node is within
    basin_row = model.basin.area.df.loc[basin_fid]
    drop_fid = True
    if basin_row.node_id in model.basin.node.df.index.to_numpy():  # check is valid node
        if series.contains(
            model.basin[basin_row.node_id].geometry
        ).any():  # check if any of series is contained by node-geometry
            model.basin.area.df.loc[basin_fid, "geometry"] = series[
                series.contains(model.basin[basin_row.node_id].geometry)
            ].iloc[0]
            series = series[~series.contains(model.basin[basin_row.node_id].geometry)]
            drop_fid = False

    if drop_fid:  # if we haven't updated existing record, we drop it
        model.basin.area.df = model.basin.area.df[model.basin.area.df.index != basin_fid]

    new_basin_area_df = pd.concat([new_basin_area_df, series], ignore_index=True)


# concat new_basin_area_df with existing model.basin.area.df
new_basin_area_df = gpd.GeoDataFrame(geometry=new_basin_area_df)
new_basin_area_df["node_id"] = None

new_basin_area_df.index += model.basin.area.df.index.max() + 1
new_basin_area_df.index.name = "fid"

model.basin.area.df = pd.concat([model.basin.area.df, new_basin_area_df])

# %% https://github.com/Deltares/Ribasim-NL/issues/153#issuecomment-2457597461

# Basins toevoegen met nieuwe_areas_voor_opvulling

new_basin_area_df = extra_area_df[extra_area_df.samenvoegen_met_basin_area_fid.isna()].reset_index()[["geometry"]]
new_basin_area_df["node_id"] = None
new_basin_area_df.index += model.basin.area.df.index.max() + 1
new_basin_area_df.index.name = "fid"
model.basin.area.df = pd.concat([model.basin.area.df, new_basin_area_df])

for basin_id, df in extra_area_df[extra_area_df.samenvoegen_met_basin_area_fid.notna()].groupby(
    "samenvoegen_met_basin_area_fid"
):
    model.basin.area.df.loc[int(basin_id), "geometry"] = model.basin.area.df.at[int(basin_id), "geometry"].union(
        df.union_all()
    )

# assign node_ids
model.fix_unassigned_basin_area()
model.fix_unassigned_basin_area(method="closest")
model.fix_unassigned_basin_area()

# %% https://github.com/Deltares/Ribasim-NL/issues/153#issuecomment-2457447764

# Split Basin / Area met area_split_lijnen

# EINDE ISSUES


# %%
# corrigeren knoop-topologie

# # ManningResistance bovenstrooms LevelBoundary naar Outlet
# for row in network_validator.edge_incorrect_type_connectivity().itertuples():
#     model.update_node(row.from_node_id, "Outlet", data=[outlet_data])

# # Inlaten van ManningResistance naar Outlet
# for row in network_validator.edge_incorrect_type_connectivity(
#     from_node_type="LevelBoundary", to_node_type="ManningResistance"
# ).itertuples():
#     model.update_node(row.to_node_id, "Outlet", data=[outlet_data])


## UPDATEN STATIC TABLES

# %%
# basin-profielen/state updaten
df = pd.DataFrame(
    {
        "node_id": np.repeat(model.basin.node.df.index.to_numpy(), 2),
        "level": [0.0, 1.0] * len(model.basin.node.df),
        "area": [0.01, 1000.0] * len(model.basin.node.df),
    }
)
df.index.name = "fid"
model.basin.profile.df = df

df = model.basin.profile.df.groupby("node_id")[["level"]].max().reset_index()
df.index.name = "fid"
model.basin.state.df = df

# %%
# tabulated_rating_curves updaten
df = pd.DataFrame(
    {
        "node_id": np.repeat(model.tabulated_rating_curve.node.df.index.to_numpy(), 2),
        "level": [0.0, 5] * len(model.tabulated_rating_curve.node.df),
        "flow_rate": [0, 0.1] * len(model.tabulated_rating_curve.node.df),
    }
)
df.index.name = "fid"
model.tabulated_rating_curve.static.df = df


# %%

# level_boundaries updaten
df = pd.DataFrame(
    {
        "node_id": model.level_boundary.node.df.index.to_list(),
        "level": [0.0] * len(model.level_boundary.node.df),
    }
)
df.index.name = "fid"
model.level_boundary.static.df = df

# %%
# manning_resistance updaten
length = len(model.manning_resistance.node.df)
df = pd.DataFrame(
    {
        "node_id": model.manning_resistance.node.df.index.to_list(),
        "length": [100.0] * length,
        "manning_n": [100.0] * length,
        "profile_width": [100.0] * length,
        "profile_slope": [100.0] * length,
    }
)
df.index.name = "fid"
model.manning_resistance.static.df = df

# %%
# flow boundaries updaten
length = len(model.flow_boundary.node.df)
df = pd.DataFrame(
    {
        "node_id": model.flow_boundary.node.df.index.to_list(),
        "flow_rate": [0.0] * length,
    }
)
df.index.name = "fid"
model.flow_boundary.static.df = df


#  %% write model
model.use_validation = False
model.write(ribasim_toml)

# %%
