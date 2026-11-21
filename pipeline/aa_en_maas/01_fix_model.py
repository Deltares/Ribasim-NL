# %%
from typing import cast

import geopandas as gpd
import numpy as np
import pandas as pd
from pandera.typing import DataFrame
from ribasim import Node
from ribasim.nodes import basin, level_boundary, outlet
from ribasim.schemas import (
    BasinProfileSchema,
    BasinStateSchema,
    FlowBoundaryStaticSchema,
    LevelBoundaryStaticSchema,
    ManningResistanceStaticSchema,
    TabulatedRatingCurveStaticSchema,
)
from ribasim_nl.gkw import get_data_from_gkw
from ribasim_nl.model import default_tables
from ribasim_nl.reset_static_tables import reset_static_tables
from ribasim_nl.sanitize_node_table import sanitize_node_table
from shapely.geometry import LineString, MultiPolygon, Point

from ribasim_nl import CloudStorage, Model, NetworkValidator

cloud = CloudStorage()

authority = "AaenMaas"
name = "aam"


# %% Check if model exist, otherwise download
ribasim_dir = cloud.joinpath(authority, f"modellen/{authority}_2024_6_3")
ribasim_toml = ribasim_dir / "model.toml"
hydamo_gpkg = cloud.joinpath(authority, "verwerkt/4_ribasim/hydamo.gpkg")
areas_gpkg = cloud.joinpath(authority, "verwerkt/3_input/areas.gpkg")
model_edits_gpkg = cloud.joinpath(authority, "verwerkt/model_edits.gpkg")

# %%
model = Model.read(ribasim_toml)
# nodes and links are referred to by their key, see docs/dev/node-keys.qmd
n = model.node_id
lk = model.link_id
network_validator = NetworkValidator(model)
hydroobject_gdf = gpd.read_file(hydamo_gpkg, layer="hydroobject", fid_as_index=True)

# %% some stuff we'll need again
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
outlet_data = outlet.Static(flow_rate=[5.0])


# HIER KOMEN ISSUES

# %% https://github.com/Deltares/Ribasim-NL/issues/149#issuecomment-2421959240

# Verwijderen link met 0m lengte
model.remove_node(n("bnd:34"), remove_links=True)
model.update_node(n("up:261OKK"), "LevelBoundary", data=[level_data], node_properties={"name": ""})


# %% see: https://github.com/Deltares/Ribasim-NL/issues/149#issuecomment-2421991252

# Corrigeren netwerk Den Bosch

# Binnenstad
basin_node = model.basin.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[2788, "geometry"]).boundary.geoms[0]), tables=basin_data
)

model.reverse_link(link_id=lk("bnd:46", "116KRU"))
model.redirect_link(link_id=lk("116KRU", "bnd:46"), to_node_id=basin_node.node_id)
model.redirect_link(link_id=lk("bnd:46", "116BIN"), from_node_id=basin_node.node_id)
model.redirect_link(link_id=lk("bnd:46", "116PKS"), from_node_id=basin_node.node_id)
outlet_node = model.outlet.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[4999, "geometry"]).interpolate(0.5, normalized=True)),
    tables=[outlet_data],
)
model.link.add(model.level_boundary[n("bnd:46")], outlet_node)
model.link.add(outlet_node, basin_node)

# Dommel
dommel_basin_node = model.basin.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[9055, "geometry"]).interpolate(0.5, normalized=True)),
    tables=basin_data,
)
model.redirect_link(link_id=lk("bnd:49", "1160460___5"), from_node_id=dommel_basin_node.node_id)

outlet_node = model.outlet.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[9055, "geometry"]).interpolate(0.1, normalized=True)),
    tables=[outlet_data],
)

model.link.add(model.level_boundary[n("bnd:49")], outlet_node)
model.link.add(outlet_node, dommel_basin_node)


# %% see: https://github.com/Deltares/Ribasim-NL/issues/149#issuecomment-2422078500

# Corrigeren netwerk bij sluis Empel
for node_id in [n("AKW787@2"), n("AKW787@3"), n("down:AKW787@3"), n("up:AKW787@2")]:
    model.remove_node(node_id, remove_links=True)


# %% see: https://github.com/Deltares/Ribasim-NL/issues/149#issuecomment-2431933060
# Omkeren linkrichting
for link_id in [
    lk("205DA", "up:204BA"),
    lk("254B", "up:254BAA"),
    lk("104FCW", "up:1080970"),
    lk("234SSW", "up:2340718"),
    lk("217KC", "up:2340691"),
    lk("108A20", "up:1060206"),
    lk("292ZLB", "up:2920087"),
]:
    model.reverse_link(link_id=link_id)


# %% see: https://github.com/Deltares/Ribasim-NL/issues/149#issuecomment-2422164355

# Corrigeren netwerk bij Spuisluis Crèvecoeur
model.remove_node(n("116CRV"), remove_links=True)
model.remove_node(n("bnd:4"), remove_links=True)

model.redirect_link(link_id=lk("1160520___2", "bnd:5"), to_node_id=n("up:Henriëttesluis"))

outlet_node = model.outlet.add(
    Node(
        geometry=cast(LineString, hydroobject_gdf.at[4825, "geometry"]).boundary.geoms[0],
        name="AKW839",
        meta_key="AKW839",
    ),
    tables=[outlet_data],
)

model.link.add(outlet_node, model.level_boundary[n("bnd:5")])
model.link.add(model.basin[n("up:1160520___2")], outlet_node)

# %% see: https://github.com/Deltares/Ribasim-NL/issues/149#issuecomment-2422373708

# Corrigeren Afleidingskanaal bij Holthees

# nabij grens
basin_node = model.basin.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[8868, "geometry"]).boundary.geoms[0]), tables=basin_data
)
outlet_node = model.outlet.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[4680, "geometry"]).interpolate(0.1, normalized=True)),
    tables=[outlet_data],
)

model.redirect_link(link_id=lk("bnd:56", "1130018"), from_node_id=n("bnd:56"), to_node_id=outlet_node.node_id)
model.redirect_link(link_id=lk("bnd:56", "113GIS"), from_node_id=outlet_node.node_id, to_node_id=basin_node.node_id)
model.redirect_link(link_id=lk("113GIS", "bnd:56"), from_node_id=n("1130018"), to_node_id=basin_node.node_id)
model.redirect_link(link_id=lk("1130018", "bnd:56"), from_node_id=basin_node.node_id, to_node_id=n("113GIS"))

# nabij afleidingskanaal
basin_node = model.basin.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[7466, "geometry"]).boundary.geoms[0]), tables=basin_data
)
outlet_node = model.outlet.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[8456, "geometry"]).interpolate(0.1, normalized=True)),
    tables=[outlet_data],
)
model.redirect_link(link_id=lk("bnd:54", "113SBW"), to_node_id=outlet_node.node_id)
model.redirect_link(link_id=lk("bnd:54", "113GIS"), from_node_id=outlet_node.node_id, to_node_id=basin_node.node_id)
model.link.add(model.tabulated_rating_curve[n("113GIS")], basin_node)
model.link.add(basin_node, model.tabulated_rating_curve[n("113SBW")])

# nabij Maas
basin_node = model.basin.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[2385, "geometry"]).boundary.geoms[0]), tables=basin_data
)
outlet_node = model.outlet.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[2539, "geometry"]).boundary.geoms[0]), tables=[outlet_data]
)

model.redirect_link(link_id=lk("113SBW", "bnd:26"), to_node_id=basin_node.node_id)
model.link.add(basin_node, outlet_node)
model.link.add(outlet_node, model.level_boundary[n("bnd:26")])


# %% see: https://github.com/Deltares/Ribasim-NL/issues/149#issuecomment-2422452167

# Toevoegen basin bij Oude Zuid-Willemsvaart
outlet_node = model.outlet.add(
    Node(
        geometry=cast(LineString, hydroobject_gdf.at[3174, "geometry"]).boundary.geoms[0],
        name="20301",
        meta_key="20301",
    ),
    tables=[outlet_data],
)
basin_node = model.basin.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[6499, "geometry"]).boundary.geoms[0]), tables=basin_data
)
model.redirect_link(link_id=lk("bnd:64", "2030230___3"), to_node_id=outlet_node.node_id)
model.link.add(outlet_node, basin_node)
model.redirect_link(link_id=lk("2030230___3", "bnd:66"), to_node_id=basin_node.node_id)

outlet_node = model.outlet.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[646, "geometry"]).interpolate(0.9, normalized=True)),
    tables=[outlet_data],
)
model.link.add(basin_node, outlet_node)
model.link.add(outlet_node, model.level_boundary[n("bnd:66")])

# EINDE ISSUES
##fixes:
for node_id in [
    n("201BSD"),
    n("201OBB"),
    n("201BGK"),
    n("2050236"),
    n("2110878"),
    n("2870063"),
    n("2050238"),
    n("2050229"),
    n("2050237"),
    n("261CTF"),
    n("2340277"),
    n("2343"),
    n("2344"),
    n("211BMD"),
    n("211OKS"),
    n("2010992"),
]:
    model.remove_node(node_id, remove_links=True)

# %%
# corrigeren knoop-topologie

# ManningResistance bovenstrooms LevelBoundary naar Outlet
for row in network_validator.link_incorrect_type_connectivity().itertuples():
    model.update_node(row.from_node_id, "Outlet", data=[outlet_data])

# Inlaten van ManningResistance naar Outlet
for row in network_validator.link_incorrect_type_connectivity(
    from_node_type="LevelBoundary", to_node_type="ManningResistance"
).itertuples():
    model.update_node(row.to_node_id, "Outlet", data=[outlet_data])


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
model.basin.profile.df = DataFrame[BasinProfileSchema](df)

assert model.basin.profile.df is not None
df = model.basin.profile.df.groupby("node_id")[["level"]].max().reset_index()
df.index.name = "fid"
model.basin.state.df = DataFrame[BasinStateSchema](df)

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
model.tabulated_rating_curve.static.df = DataFrame[TabulatedRatingCurveStaticSchema](df)


# %% see: https://github.com/Deltares/Ribasim-NL/issues/149#issuecomment-2431933060
assert model.tabulated_rating_curve.static.df is not None
model.tabulated_rating_curve.static.df.loc[
    model.tabulated_rating_curve.static.df.node_id.isin([n("254B"), n("275JK"), n("108A20"), n("292ZLB")]), "flow_rate"
] = 0.0


# %%

# level_boundaries updaten
df = pd.DataFrame(
    {
        "node_id": model.level_boundary.node.df.index.to_list(),
        "level": [0.0] * len(model.level_boundary.node.df),
    }
)
df.index.name = "fid"
model.level_boundary.static.df = DataFrame[LevelBoundaryStaticSchema](df)

# %%
# manning_resistance updaten
length = len(model.manning_resistance.node.df)
df = pd.DataFrame(
    {
        "node_id": model.manning_resistance.node.df.index.to_list(),
        "length": [100.0] * length,
        "manning_n": [0.03] * length,
        "profile_width": [100.0] * length,
        "profile_slope": [100.0] * length,
    }
)
df.index.name = "fid"
model.manning_resistance.static.df = DataFrame[ManningResistanceStaticSchema](df)

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
model.flow_boundary.static.df = DataFrame[FlowBoundaryStaticSchema](df)

# %% read
basin_node_edits_gdf = gpd.read_file(model_edits_gpkg, fid_as_index=True, layer="unassigned_basin_node")
basin_area_edits_gdf = gpd.read_file(model_edits_gpkg, fid_as_index=True, layer="unassigned_basin_area")
internal_basin_edits_gdf = gpd.read_file(model_edits_gpkg, fid_as_index=True, layer="internal_basins")
remove_nodes_df = gpd.read_file(model_edits_gpkg, fid_as_index=True, layer="remove_node")
areas_gdf = gpd.read_file(areas_gpkg, layer="areas")

# %% the areas assigned to a basin by hand, by geometry and by area code
for row in basin_area_edits_gdf[basin_area_edits_gdf["to_node_key"].notna()].itertuples():
    model.add_basin_area(cast(MultiPolygon, row.geometry), node_id=n(cast(str, row.to_node_key)))
for row in basin_node_edits_gdf[basin_node_edits_gdf["to_area_code"].notna()].itertuples():
    area = areas_gdf.loc[areas_gdf["code"] == row.to_area_code].union_all()
    model.add_basin_area(area, node_id=n(cast(str, row.node_key)))

# %% merge_basins
selection = basin_node_edits_gdf[basin_node_edits_gdf["to_node_key"].notna()]
for row in selection.fillna({"connected": True}).itertuples():
    model.merge_basins(
        node_id=n(cast(str, row.node_key)), to_node_id=n(cast(str, row.to_node_key)), are_connected=row.connected
    )

mask = internal_basin_edits_gdf["to_node_key"].notna() & internal_basin_edits_gdf["add_object"].isna()
for row in internal_basin_edits_gdf[mask].fillna({"connected": True}).itertuples():
    model.merge_basins(
        node_id=n(cast(str, row.node_key)), to_node_id=n(cast(str, row.to_node_key)), are_connected=row.connected
    )

# %% add and connect nodes
for row in internal_basin_edits_gdf[internal_basin_edits_gdf.add_object.notna()].itertuples():
    from_basin_id = n(cast(str, row.node_key))
    to_basin_id = n(cast(str, row.to_node_key))
    if row.add_object == "stuw":
        node_type = "TabulatedRatingCurve"
    model.add_and_connect_node(
        from_basin_id, to_basin_id, geometry=row.geometry, node_type=node_type, name=row.add_object_name
    )

# %% reverse direction at node
for row in internal_basin_edits_gdf[internal_basin_edits_gdf["reverse_direction"]].itertuples():
    model.reverse_direction_at_node(node_id=n(cast(str, row.node_key)))

# %% change node_type
for row in basin_node_edits_gdf[basin_node_edits_gdf["change_to_node_type"].notna()].itertuples():
    if row.change_to_node_type:
        model.update_node(n(cast(str, row.node_key)), row.change_to_node_type, data=[level_boundary.Static(level=[0])])


# %% remove_nodes
for row in remove_nodes_df.itertuples():
    model.remove_node(node_id=n(cast(str, row.node_key)), remove_links=cast(bool, row.remove_links))


# %% merge nodes

# Manning worden outlets tpv stuw/duiker
outlet_ids = [
    n("2030334"),
    n("2170097"),
    n("2170697"),
    n("2340748"),
    n("2340718"),
    n("2340879"),
    n("2751488"),
    n("2751015___1"),
    n("1020332"),
    n("1040515"),
    n("1047240___3"),
    n("1040271"),
    n("2340753"),
    n("2170131"),
    n("2340780"),
    n("2170576"),
    n("2030319"),
    n("2340750"),
    n("ow:1100670"),
    n("1170165"),
    n("1180220"),
    n("1140075"),
    n("1100074"),
    n("2611073"),
    n("2110006"),
    n("2400055"),
    n("1081900"),
    n("1070307"),
    n("2011600"),
    n("2030323"),
    n("2530128"),
    n("2530018"),
    n("2530328"),
    n("2530363"),
    n("2510106"),
    n("2510432"),
    n("2510112"),
    n("2870024"),
    n("2480021"),
    n("2110137"),
    n("2110162"),
    n("2110005"),
    n("2010005"),
    n("2010630"),
    n("2530364"),
    n("2790002"),
    n("2110597"),
    n("2110664"),
    n("2113600"),
    n("2110595"),
    n("ow:2113865"),
    n("2050175"),
    n("2320026"),
    n("2320026"),
    n("2320026"),
    n("2320026"),
    n("211N"),
    n("2270021"),
    n("2270201"),
    n("2040882"),
    n("2042435"),
    n("2110514"),
    n("2110562"),
    n("2040779"),
    n("2040742"),
    n("2040849"),
    n("2052"),
    n("2040231"),
    n("2270181"),
    n("2171570___3"),
    n("2030270"),
    n("1170274"),
    n("1170275"),
    n("1142560"),
    n("1080875"),
    n("1070039"),
    n("1142610"),
    n("1140521"),
    n("1140087"),
    n("1140199"),
    n("1140208"),
    n("1143650___0"),
    n("1140146"),
    n("1140125"),
    n("ow:1144290"),
    n("1080080"),
    n("1100046"),
    n("1140341"),
    n("2011270___3"),
    n("2010048"),
]

for node_id in dict.fromkeys(outlet_ids):
    model.update_node(node_id=node_id, node_type="Outlet")

# ligt benedenstrooms een stuw
model.update_node(node_id=n("ow:2042930"), node_type="Outlet")  # wordt outlet, was manning
assert model.outlet.static.df is not None
model.outlet.static.df.loc[model.outlet.static.df.node_id == n("ow:2042930"), "min_upstream_level"] = 4.48499

model.redirect_link(link_id=lk("up:204B", "204BN"), from_node_id=n("up:ow:2042930"), to_node_id=n("204BN"))
model.reverse_link(link_id=lk("2110005", "up:211MI"))
model.reverse_link(link_id=lk("up:2110005", "2110005"))


merge_pairs = [
    (n("up:2010675___4"), n("up:2010720___0")),
    (n("up:201H"), n("up:2010720___0")),
    (n("up:2010930"), n("up:2011600")),
    (n("up:2110009"), n("up:2110631")),
    (n("up:227J"), n("up:2110631")),
    (n("up:2112930___1"), n("up:2113070")),
    (n("up:2344560___1"), n("up:ow:2342270")),
    (n("up:2170249"), n("up:217EB")),
    (n("up:203I"), n("up:2030196")),
    (n("up:2030223"), n("up:203IA")),
    (n("up:217EG"), n("up:2170674")),
    (n("up:2340691"), n("up:234BKS")),
    (n("up:201C"), n("up:2011065___1")),
    (n("up:2011190"), n("up:201B")),
    (n("up:204KLW"), n("up:204A")),
    (n("up:2040267"), n("up:2040231")),
    (n("up:2270181"), n("up:2270870___0")),
    (n("up:2341260"), n("up:234HKL")),
    (n("up:234HKL"), n("up:234BUY")),
    (n("up:2341770"), n("up:2341983")),
    (n("up:2341983"), n("up:2344550")),
    (n("up:234D"), n("up:2342114")),
    (n("up:2342114"), n("up:2344550")),
    (n("up:2340748"), n("up:2343090")),
    (n("up:1171260___1"), n("up:1170274")),
    (n("up:1170275"), n("up:1171790")),
    (n("up:1140104"), n("up:114AQS")),
    (n("up:201BEU"), n("up:ow:2011330")),
    (n("up:2113865___2"), n("up:ow:2011330")),
]

for basin_id, to_basin_id in merge_pairs:
    model.merge_basins(
        node_id=basin_id,
        to_node_id=to_basin_id,
        are_connected=True,
    )


# %% Connect basins:
actions = [
    "add_basin",
    "update_node",
    "connect_basins",
]

available_layers = gpd.list_layers(model_edits_gpkg).name.to_list()
actions = [a for a in actions if a in available_layers]

model.apply_edits(model_edits_gpkg, actions, sort_by_order=True, drop_na=True)


# %% corrigeren knoop-topologie
outlet_data = outlet.Static(flow_rate=[5.0])
# ManningResistance bovenstrooms LevelBoundary naar Outlet
for row in network_validator.link_incorrect_type_connectivity().itertuples():
    model.update_node(row.from_node_id, "Outlet", data=[outlet_data])

# Inlaten van ManningResistance naar Outlet
for row in network_validator.link_incorrect_type_connectivity(
    from_node_type="LevelBoundary", to_node_type="ManningResistance"
).itertuples():
    model.update_node(row.to_node_id, "Outlet", data=[outlet_data])

# %% sanitize node-table
# TabulatedRatingCurve to Outlet
for row in model.node.df[model.node.df.node_type == "TabulatedRatingCurve"].itertuples():
    node_id = row.Index
    model.update_node(node_id=node_id, node_type="Outlet")

# basins and outlets we've added do not have category, we fill with hoofdwater
model.node.df.loc[model.basin.node.df.index[model.basin.node.df["meta_categorie"].isna()], "meta_categorie"] = (
    "hoofdwater"
)
model.node.df.loc[model.outlet.node.df.index[model.outlet.node.df["meta_categorie"].isna()], "meta_categorie"] = (
    "hoofdwater"
)


# somehow Sluis Engelen (beheerregister AAM) has been named Henriettesluis
model.node.df.loc[
    (model.node.df["node_type"] == "Outlet") & (model.node.df.name == "Henriëttesluis"),
    "name",
] = "AKW855"

# name-column contains the code we want to keep, meta_name the name we want to have
df = get_data_from_gkw(authority=authority, layers=["gemaal", "stuw", "sluis"])
df.set_index("code", inplace=True)
names = df["naam"]


# set meta_gestuwd in basins
model.node.df.loc[model.node.df["node_type"] == "Basin", "meta_gestuwd"] = False
model.node.df.loc[model.node.df["node_type"] == "Outlet", "meta_gestuwd"] = False
model.node.df.loc[model.node.df["node_type"] == "Pump", "meta_gestuwd"] = True

# set stuwen als gestuwd
model.node.df.loc[
    (model.node.df["node_type"] == "Outlet") & (model.node.df["meta_object_type"] == "stuw"),
    "meta_gestuwd",
] = True

# toevoegen 261HTE, zo goed mogelijk
df = gpd.read_file(hydamo_gpkg, layer="stuw")
geometry = df.set_index("code").at["261HTE", "geometry"]

model.connect_basins(
    from_basin_id=n("up:261BR"), to_basin_id=n("up:261COB"), node_type="Outlet", geometry=geometry, name="261HTE"
)
# Fixes kleine basins
# model traag door te kleine basins
model.merge_basins(node_id=n("up:251T"), to_node_id=n("up:251CO"), are_connected=True)  # Klein oppervlakte basin
model.merge_basins(node_id=n("up:287D"), to_node_id=n("up:ow:2870630"), are_connected=True)
model.merge_basins(node_id=n("up:ow:2870185"), to_node_id=n("up:ow:2870630"), are_connected=True)
model.merge_basins(node_id=n("up:275VSF"), to_node_id=n("up:275E"), are_connected=True)
model.merge_basins(node_id=n("up:114P16"), to_node_id=n("up:1140521"), are_connected=True)
model.merge_basins(node_id=n("up:107A3"), to_node_id=n("up:1140521"), are_connected=True)
model.merge_basins(node_id=n("up:107HA"), to_node_id=n("up:107P14"), are_connected=True)
model.merge_basins(node_id=n("up:2011440"), to_node_id=n("up:1200150___11"), are_connected=True)
model.merge_basins(node_id=n("up:1200150___11"), to_node_id=n("up:1160500___3"), are_connected=True)
model.merge_basins(node_id=n("up:103BIB"), to_node_id=n("up:ow:1048150"), are_connected=True)
model.merge_basins(node_id=n("up:261CZ"), to_node_id=n("up:261CW"), are_connected=True)
model.merge_basins(node_id=n("up:261COB"), to_node_id=n("up:2611073"), are_connected=True)
model.merge_basins(node_id=n("up:2060019"), to_node_id=n("up:206EA"), are_connected=True)
model.merge_basins(node_id=n("up:Henriëttesluis"), to_node_id=n("up:1160520___2"), are_connected=True)
model.merge_basins(node_id=n("up:1160520___2"), to_node_id=n("up:1160500___3"), are_connected=True)
model.merge_basins(node_id=n("up:ow:1048300"), to_node_id=n("up:ow:1048250"), are_connected=True)
model.merge_basins(node_id=n("up:1048250___0"), to_node_id=n("up:ow:1048250"), are_connected=True)
model.merge_basins(node_id=n("up:ow:1048120"), to_node_id=n("up:ow:1048110"), are_connected=True)
model.merge_basins(node_id=n("up:104FTW"), to_node_id=n("up:1040966"), are_connected=True)
model.merge_basins(node_id=n("up:201N"), to_node_id=n("up:201DDK"), are_connected=True)

# Drongelens kanaal
model.merge_basins(node_id=n("up:116HDW"), to_node_id=n("up:1160240___1"), are_connected=True)
model.merge_basins(node_id=n("up:1160240___1"), to_node_id=n("up:VRKW118"), are_connected=True)
model.merge_basins(node_id=n("up:VRKW118"), to_node_id=n("up:117BSL"), are_connected=True)

# set bovenstroomse basins als gestuwd
node_df = model.node.df[model.node.df["meta_gestuwd"] & model.node.df["node_type"].isin(["Outlet", "Pump"])]

upstream_node_ids = [model.upstream_node_id(i) for i in node_df.index]
basin_node_ids = model.basin.node.df.index.intersection(upstream_node_ids)
model.node.df.loc[basin_node_ids, "meta_gestuwd"] = True

# set álle benedenstroomse outlets van gestuwde basins als gestuwd (dus ook duikers en andere objecten)
downstream_node_ids = pd.Index(pd.Series([model.downstream_node_id(i) for i in basin_node_ids]).explode())
model.node.df.loc[model.outlet.node.df.index.intersection(downstream_node_ids), "meta_gestuwd"] = True

sanitize_node_table(
    model,
    meta_columns=["meta_code_waterbeheerder", "meta_categorie", "meta_gestuwd"],
    copy_map=[
        {"node_types": ["Outlet", "Pump"], "columns": {"name": "meta_code_waterbeheerder"}},
        {"node_types": ["LevelBoundary", "FlowBoundary"], "columns": {"meta_name": "name"}},
        {"node_types": ["Basin", "ManningResistance"], "columns": {"name": ""}},
    ],
    names=names,
)


# %% set flow-boundaries to level-boundaries (plus outlet)
model.replace_flow_boundaries_by_level_boundaries()

# basin Deurnsche Peel
model.move_node(node_id=n("up:3200005"), geometry=Point(192066.2, 377928.7))
model.move_node(node_id=n("3200005"), geometry=Point(189926.3, 382222.4))
model.redirect_link(link_id=lk("320SML", "bnd:35"), from_node_id=n("320SML"), to_node_id=n("up:3200005"))
model.redirect_link(link_id=lk("ow:3200030", "bnd:35"), from_node_id=n("ow:3200030"), to_node_id=n("up:3200005"))
model.reverse_direction_at_node(node_id=n("loc:192486,378114"))


# Eindhovens kanaal
model.move_node(node_id=n("2030225___3"), geometry=Point(170038.16, 384348.73))
model.redirect_link(link_id=lk("bnd:43", "234EE"), from_node_id=n("up:ow:2030225"), to_node_id=n("234EE"))

# Fix Drongelens kanaal
df = gpd.read_file(model_edits_gpkg, layer="add_basin_area", fid_as_index=True)
assert model.basin.area.df is not None
model.basin.area.df.loc[model.basin.area.df.node_id == n("up:1171790"), "geometry"] = df.loc[
    [154], "geometry"
].to_numpy()
# add new basin
basin_node = model.basin.add(
    Node(geometry=cast(Point, model.outlet.node.df.loc[n("ow:1160120")].geometry), meta_categorie="doorgaand"),
    tables=default_tables.basin,
)
model.add_basin_area(cast(MultiPolygon, df.at[155, "geometry"]), node_id=basin_node.node_id)

# fix topology
model.move_node(node_id=n("ow:1160120"), geometry=Point(145057.6, 409049.8))
model.redirect_link(link_id=lk("bnd:48", "ow:1160120"), from_node_id=basin_node.node_id, to_node_id=n("ow:1160120"))
outlet_node = model.outlet.add(Node(geometry=Point(148449.6, 410398.1)), tables=default_tables.outlet)
model.link.add(outlet_node, basin_node)
model.link.add(model.level_boundary[n("bnd:48")], outlet_node)
# nog een basin-merge i.v.m. manning
model.merge_basins(node_id=dommel_basin_node.node_id, to_node_id=n("up:1160500___3"))

# %% give the holes that the edits left in the basin areas to a basin
model.fill_basin_area_holes(areas_gpkg)

# %%
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_fix_model", f"{name}.toml")
model = reset_static_tables(model)
model.use_validation = True
model.validate_link_source_destination()
model.write(ribasim_toml)
model.report_basin_area()
model.report_internal_basins()

# %% Test run model
# result = model.run()
