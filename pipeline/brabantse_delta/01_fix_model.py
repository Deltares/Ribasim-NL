# %%
from typing import cast

import geopandas as gpd
import pandas as pd
from ribasim import Node
from ribasim.nodes import basin, level_boundary, outlet
from ribasim_nl.geometry import drop_z
from ribasim_nl.gkw import get_data_from_gkw
from ribasim_nl.reset_static_tables import reset_static_tables
from ribasim_nl.sanitize_node_table import sanitize_node_table
from shapely.geometry import LineString, Point

from ribasim_nl import CloudStorage, Model, NetworkValidator

cloud = CloudStorage()

authority = "BrabantseDelta"
short_name = "wbd"
run_model = False
ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_2024_6_3")
ribasim_toml = ribasim_dir / "model.toml"
hydamo_gpkg = cloud.joinpath(authority, "verwerkt/4_ribasim/hydamo.gpkg")
areas_gpkg = cloud.joinpath(authority, "verwerkt/3_input/areas.gpkg")
model_edits_gpkg = cloud.joinpath(authority, "verwerkt/model_edits.gpkg")

# %% read model and hydroobject
model = Model.read(ribasim_toml)
# nodes and links are referred to by their key, see docs/dev/node-keys.qmd
n = model.node_id
lk = model.link_id
network_validator = NetworkValidator(model)

# the hydroobjects by their code, without the hand additions that have none
hydroobject_gdf = gpd.read_file(hydamo_gpkg, layer="hydroobject").dropna(subset="code").set_index("code")


def start(code: str) -> Point:
    """The start of the hydroobject with this code."""
    return drop_z(cast(LineString, hydroobject_gdf.at[code, "geometry"]).boundary.geoms[0])


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
# %% https://github.com/Deltares/Ribasim-NL/issues/152#issue-2535747701
# Omkeren links
link_ids = [
    lk("KST01068", "bnd:28"),
    lk("KST01119", "bnd:28"),
    lk("KDU10783", "bnd:28"),
    lk("KDU30624", "bnd:27"),
    lk("KDU14603", "up:KDU14602"),
    lk("KDU14589", "bnd:30"),
    lk("KSL00215", "bnd:40"),
    lk("KDU28116", "bnd:49"),
    lk("KDU01724", "bnd:39"),
    lk("KST02654", "bnd:8"),
]

for link_id in link_ids:
    model.reverse_link(link_id=link_id)

# %% Connector nodes whose hydroobject is drawn against the flow in the HyDAMO data of 2026, unlike in that of 2023
for key in ["KDU11679", "KDU42101", "KDU42112", "KDU45089", "KDU60018", "KST00839", "KST00917", "ow:OVK04871"]:
    node_id = n(key)
    links = model.link.df
    for link_id in links.index[(links.from_node_id == node_id) | (links.to_node_id == node_id)]:
        model.reverse_link(link_id=link_id)

# %% https://github.com/Deltares/Ribasim-NL/issues/152#issuecomment-2428677846
# Toevoegen Donge
basin_node = model.basin.add(
    Node(geometry=start("OVK06048-2__2"), meta_key="up:uitlaat-donge"),
    tables=basin_data,
)
outlet_node = model.outlet.add(
    # the start of a hand addition without code
    Node(geometry=Point(117683.425, 413972.059), meta_key="uitlaat-donge"),
    tables=[outlet_data],
)
model.redirect_link(link_id=lk("KGM00043", "bnd:31"), from_node_id=basin_node.node_id, to_node_id=n("KGM00043"))
model.link.add(basin_node, outlet_node)
model.link.add(outlet_node, model.level_boundary[n("bnd:31")])

outlet_node = model.outlet.add(
    Node(geometry=start("OVK06048-1"), meta_key="inlaat-donge"),
    tables=[outlet_data],
)
model.redirect_link(link_id=lk("bnd:47", "KGM00043"), to_node_id=outlet_node.node_id)
model.redirect_link(
    link_id=lk("bnd:47", "GEM_KGM00069"), from_node_id=outlet_node.node_id, to_node_id=basin_node.node_id
)

# EINDE ISSUES
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


# %%
model.apply_edits(model_edits_gpkg, gpd.list_layers(model_edits_gpkg).name)
# %%
# Toeveoegen Inlaten vanuit Volkerak door omkeren links:
# Omkeren links
link_ids = [
    lk("ow:OVK12037", "bnd:18"),
    lk("up:KDU32642", "ow:OVK12037"),
    lk("ow:OVK12030", "bnd:36"),
    lk("up:ow:OVK12030", "ow:OVK12030"),
]
for link_id in link_ids:
    model.reverse_link(link_id=link_id)

# %% corrigeren knoop-topologie
# ManningResistance bovenstrooms LevelBoundary naar Outlet
for row in network_validator.link_incorrect_type_connectivity().itertuples():
    model.update_node(row.from_node_id, "Outlet")

# Inlaten van ManningResistance naar Outlet
for row in network_validator.link_incorrect_type_connectivity(
    from_node_type="LevelBoundary", to_node_type="ManningResistance"
).itertuples():
    model.update_node(row.to_node_id, "Outlet")

# %%
# fixes model
# merge basins
model.merge_basins(node_id=n("up:ow:OVK03889"), to_node_id=n("up:ow:OVK03891"), are_connected=True)
model.merge_basins(node_id=n("up:OVK01904"), to_node_id=n("up:KST00209"), are_connected=True)
model.merge_basins(node_id=n("up:ow:OVK03174-0"), to_node_id=n("up:KST02037"), are_connected=True)
model.merge_basins(node_id=n("up:ow:OVK11365"), to_node_id=n("up:OVK11367-0"), are_connected=True)
model.merge_basins(node_id=n("up:ow:OVK00551"), to_node_id=n("up:KST00955"), are_connected=True)
model.merge_basins(node_id=n("up:ow:OVK02133"), to_node_id=n("up:ow:OVK02843"), are_connected=True)
model.merge_basins(node_id=n("up:ow:OVK05065"), to_node_id=n("up:KDU05199"), are_connected=True)
model.merge_basins(node_id=n("up:ow:OVK11364"), to_node_id=n("up:KST02884"), are_connected=True)
model.merge_basins(node_id=n("up:KDU00321"), to_node_id=n("up:GEM_KGM00431"), are_connected=True)
model.merge_basins(node_id=n("up:ow:OVK11370"), to_node_id=n("up:ow:OVK11374"), are_connected=True)
model.merge_basins(node_id=n("up:ow:OVK11374"), to_node_id=n("up:ow:OVK11373-0"), are_connected=True)
# into the Basin above weir KST00814, which holds part of OVK11373 in the HyDAMO data of 2026. The Basin below that
# weir, up:KST02907, is therefore no longer merged with it
model.merge_basins(node_id=n("up:ow:OVK11373-0"), to_node_id=n("up:KST00814"), are_connected=True)
model.merge_basins(node_id=n("up:ow:OVK11371"), to_node_id=n("up:ow:OVK11363"), are_connected=True)
model.merge_basins(node_id=n("up:OVK11367-0"), to_node_id=n("up:ow:OVK11363"), are_connected=True)
model.merge_basins(node_id=n("up:ow:OVK01991"), to_node_id=n("up:KST00209"), are_connected=True)
model.merge_basins(node_id=n("up:ow:OVK12562"), to_node_id=n("up:OVK04510"), are_connected=True)
model.merge_basins(node_id=n("up:OVK04510"), to_node_id=n("up:KST00115"), are_connected=True)
model.merge_basins(node_id=n("up:ow:OVK03167"), to_node_id=n("up:KST02037"), are_connected=True)
model.merge_basins(node_id=n("up:KDU03599"), to_node_id=n("up:KDU20154"), are_connected=True)
model.merge_basins(node_id=n("up:ow:OVK05066"), to_node_id=n("up:ow:OVK05068"), are_connected=True)
model.merge_basins(node_id=n("up:KDU15142"), to_node_id=n("up:ow:OVK02908"), are_connected=True)
model.merge_basins(node_id=n("up:ow:OVK02919"), to_node_id=n("up:ow:OVK02914"), are_connected=True)
model.merge_basins(node_id=n("up:ow:OVK02914"), to_node_id=n("up:ow:OVK02911"), are_connected=True)
model.merge_basins(node_id=n("up:ow:OVK02911"), to_node_id=n("up:ow:OVK02906"), are_connected=True)
model.merge_basins(node_id=n("up:ow:OVK02906"), to_node_id=n("up:ow:OVK02936"), are_connected=True)
model.merge_basins(node_id=n("up:KSY00032"), to_node_id=n("up:OVK20632"), are_connected=True)
model.merge_basins(node_id=n("up:KDU19785"), to_node_id=n("up:ow:OVK12030"), are_connected=True)
model.merge_basins(node_id=n("up:KDU44734"), to_node_id=n("up:ow:OVK12128"), are_connected=True)
model.merge_basins(node_id=n("up:ow:OVK12128"), to_node_id=n("up:OVK06052"), are_connected=True)
model.merge_basins(node_id=n("up:KST01776"), to_node_id=n("up:KDU04391"), are_connected=True)
model.merge_basins(node_id=n("up:ow:OVK02843"), to_node_id=n("up:OVK02836"), are_connected=True)
model.merge_basins(node_id=n("up:ow:OVK02936"), to_node_id=n("up:ow:OVK02908"), are_connected=True)
model.merge_basins(node_id=n("up:OVK02836"), to_node_id=n("up:OVK02837"), are_connected=True)
model.merge_basins(node_id=n("up:OVK02837"), to_node_id=n("up:KST00207"), are_connected=True)
model.merge_basins(node_id=n("up:KST01493"), to_node_id=n("up:ow:OVK03186"), are_connected=True)
model.merge_basins(node_id=n("up:KDU24269"), to_node_id=n("up:KST00209"), are_connected=True)
model.merge_basins(node_id=n("up:GEM_KGM00053"), to_node_id=n("up:KST00792"), are_connected=True)
model.merge_basins(node_id=n("up:KGM00052"), to_node_id=n("up:KST00406"), are_connected=True)
model.merge_basins(node_id=n("up:KST02131"), to_node_id=n("up:KDU08584"), are_connected=True)
model.merge_basins(node_id=n("up:KGM00421"), to_node_id=n("up:KDU02968"), are_connected=True)
model.merge_basins(node_id=n("up:KDU14607"), to_node_id=n("up:KST02296"), are_connected=True)
model.merge_basins(node_id=n("up:KST02296"), to_node_id=n("up:KDU03212"), are_connected=True)
model.merge_basins(node_id=n("up:KDU10669"), to_node_id=n("up:ow:OVK99994"), are_connected=True)
model.merge_basins(node_id=n("up:KDU06233"), to_node_id=n("up:KDU03371"), are_connected=True)
model.merge_basins(node_id=n("up:KDU32642"), to_node_id=n("up:ow:OVK99994"), are_connected=True)
model.merge_basins(node_id=n("up:ow:OVK12030"), to_node_id=n("up:OVK12124"), are_connected=True)
model.merge_basins(node_id=n("up:OVK12124"), to_node_id=n("up:OVK06052"), are_connected=True)
model.merge_basins(node_id=n("up:ow:OVK06051"), to_node_id=n("up:KDU19778"), are_connected=True)
model.merge_basins(node_id=n("up:KDU03370"), to_node_id=n("up:KDU19778"), are_connected=True)
model.merge_basins(node_id=n("up:OVK04869___17"), to_node_id=n("up:KDU04571"), are_connected=True)
model.merge_basins(node_id=n("up:KDU16939"), to_node_id=n("up:KDU16940"), are_connected=True)
model.merge_basins(node_id=n("up:KDU16940"), to_node_id=n("up:ow:OVK10756-2"), are_connected=True)
model.merge_basins(node_id=n("up:KDU22219"), to_node_id=n("up:OVK08441-0___1"), are_connected=True)
model.remove_node(n("OVK01598___1"), remove_links=True)
# %% Reset static tables

# Reset static tables
model = reset_static_tables(model)

# Sanitize node_table
for node_id in model.tabulated_rating_curve.node.df.index:
    model.update_node(node_id=node_id, node_type="Outlet")

# ManningResistance that are duikersifonhevel to outlet
for node_id in model.manning_resistance.node.df[
    model.manning_resistance.node.df["meta_object_type"] == "duikersifonhevel"
].index:
    model.update_node(node_id=node_id, node_type="Outlet")

# nodes we've added do not have category, we fill with hoofdwater
model.node.df.loc[model.node.df["meta_categorie"].isna(), "meta_categorie"] = "hoofdwater"

# name-column contains the code we want to keep, meta_name the name we want to have
df = get_data_from_gkw(layers=["sluis", "gemaal", "stuw"], authority=authority)
df.set_index("code", inplace=True)
names = df["naam"]

sanitize_node_table(
    model,
    meta_columns=["meta_code_waterbeheerder", "meta_categorie"],
    copy_map=[
        {"node_types": ["Outlet", "Pump"], "columns": {"name": "meta_code_waterbeheerder"}},
        {"node_types": ["Basin", "ManningResistance"], "columns": {"name": ""}},
        {"node_types": ["FlowBoundary", "LevelBoundary"], "columns": {"meta_name": "name"}},
    ],
    names=names,
)

# label flow-boundaries to buitenlandse-aanvoer
model.node.df.loc[model.flow_boundary.node.df.index, "meta_categorie"] = "buitenlandse aanvoer"


# %%

# init gestuwd voor basins, pumps en outlets
model.node.df.loc[model.node.df["node_type"] == "Basin", "meta_gestuwd"] = False
model.node.df.loc[model.node.df["node_type"] == "Outlet", "meta_gestuwd"] = False
model.node.df.loc[model.node.df["node_type"] == "Pump", "meta_gestuwd"] = True

#
node_ids = model.node.df[
    model.node.df["meta_code_waterbeheerder"].str.startswith("KST")
    | model.node.df["meta_code_waterbeheerder"].str.startswith("GEM_")
].index

upstream_node_ids = [model.upstream_node_id(i) for i in node_ids]

basin_node_ids = model.basin.node.df.index[model.basin.node.df.index.isin(upstream_node_ids)]
model.node.df.loc[model.node.df.index.isin(basin_node_ids), "meta_gestuwd"] = True

downstream_node_ids = pd.Series([model.downstream_node_id(i) for i in basin_node_ids]).explode().to_numpy()
model.node.df.loc[
    (model.node.df["node_type"] == "Outlet") & model.node.df.index.isin(downstream_node_ids),
    "meta_gestuwd",
] = True


# %% give the holes that the edits left in the basin areas to a basin
model.fill_basin_area_holes(areas_gpkg)

#  %% write model
model.use_validation = True
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_fix_model", f"{short_name}.toml")
model.write(ribasim_toml)
model.report_basin_area()
model.report_internal_basins()

# %%#
# Test run model
if run_model:
    result = model.run()
    assert result.exit_code == 0

# %%
