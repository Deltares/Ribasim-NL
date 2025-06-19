# %%


import geopandas as gpd
import pandas as pd

from peilbeheerst_model import ribasim_parametrization
from peilbeheerst_model.controle_output import Control
from ribasim_nl import CloudStorage, Model, check_basin_level
from ribasim_nl.from_to_nodes_and_levels import add_from_to_nodes_and_levels
from ribasim_nl.parametrization.basin_tables import update_basin_static

# execute model run
MODEL_EXEC: bool = True

# model settings
AUTHORITY: str = "Limburg"
SHORT_NAME: str = "limburg"
MODEL_ID: str = "2025_7_0"

# connect with the GoodCloud
cloud = CloudStorage()

# collect relevant data from the GoodCloud
ribasim_model_dir = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_parameterized_model")
ribasim_toml = ribasim_model_dir / f"{SHORT_NAME}.toml"
qlr_path = cloud.joinpath("Basisgegevens", "QGIS_lyr", "output_controle_vaw_aanvoer.qlr")
aanvoer_gpkg = cloud.joinpath("Landelijk", "waterverdeling", "aanvoer.gpkg")
aanvoer_gdf = gpd.read_file(aanvoer_gpkg, layer="aanvoergebieden")
aanvoer_gdf = aanvoer_gdf[aanvoer_gdf["waterbeheerder"] == "Limburg"]
model_edits_aanvoer_gpkg = cloud.joinpath(AUTHORITY, "verwerkt", "model_edits_aanvoer.gpkg")
cloud.synchronize(
    filepaths=[
        aanvoer_gpkg,
    ]
)

# read model
model = Model.read(ribasim_toml)
original_model = model.model_copy(deep=True)
update_basin_static(model=model, evaporation_mm_per_day=1)
add_from_to_nodes_and_levels(model)

# re-parameterize
ribasim_parametrization.set_aanvoer_flags(model, aanvoer_gdf, overruling_enabled=False)
ribasim_parametrization.determine_min_upstream_max_downstream_levels(model, AUTHORITY)
check_basin_level.add_check_basin_level(model=model)

# TODO: The addition of `ContinuousControl`-nodes is subsequently a minor modification:
"""To allow the addition of `ContinuousControl`-nodes, the branch 'continuous_control' must be merged first to access
the required function: `ribasim_parametrization.add_continuous_control(<model>)`. The expansion of adding the continuous
control requires a proper working schematisation of both 'afvoer'- and 'aanvoer'-situations, and so these should be
fixed and up-and-running beforehand.
"""
# ribasim_parametrization.add_continuous_control(model)

"""For the addition of `ContinuousControl`-nodes, it might be necessary to set `model.basin.static.df=None`, as the
`ContinuousControl`-nodes require `Time`-tables instead of `Static`-tables. If both are defined (for the same node,
Ribasim will raise an error and thus not execute.
"""
model.manning_resistance.static.df.loc[:, "manning_n"] = 0.04
mask = model.outlet.static.df["meta_aanvoer"] == 0
model.outlet.static.df.loc[mask, "max_downstream_level"] = pd.NA
model.outlet.static.df.flow_rate = original_model.outlet.static.df.flow_rate
model.pump.static.df.flow_rate = original_model.pump.static.df.flow_rate

# set upstream level boundaries at 999 meters
# boundary_node_ids = [i for i in model.level_boundary.node.df.index if not model.upstream_node_id(i) is not None]
# model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id.isin(boundary_node_ids), "level"] = 999
model.outlet.static.df.loc[
    model.outlet.static.df.node_id.isin(model.upstream_connection_node_ids(node_type="Outlet")), "flow_rate"
] = 10
model.outlet.static.df.loc[
    model.outlet.static.df.node_id.isin(model.upstream_connection_node_ids(node_type="Pump")), "flow_rate"
] = 10

model.merge_basins(basin_id=2461, to_basin_id=2070, are_connected=True)
model.merge_basins(basin_id=2070, to_basin_id=2360, are_connected=True)
model.merge_basins(basin_id=1885, to_basin_id=2360, are_connected=True)
model.merge_basins(basin_id=2205, to_basin_id=2144, are_connected=True)

actions = gpd.list_layers(model_edits_aanvoer_gpkg).name.to_list()
# for action in actions:
#     print(action)
#     # get method and args
#     method = getattr(model, action)
#     keywords = inspect.getfullargspec(method).args
#     df = gpd.read_file(model_edits_aanvoer_gpkg, layer=action, fid_as_index=True)
#     if "order" in df.columns:
#         df.sort_values("order", inplace=True)
#     for row in df.itertuples():
#         # filter kwargs by keywords
#         kwargs = {k: v for k, v in row._asdict().items() if k in keywords}
#         method(**kwargs)

# %% sturing uit alle niet-gestuwde outlets halen
node_ids = model.outlet.node.df[model.outlet.node.df["meta_gestuwd"] == "False"].index
mask = model.outlet.static.df["node_id"].isin(node_ids)
model.outlet.static.df.loc[mask, "min_upstream_level"] = pd.NA
model.outlet.static.df.loc[mask, "max_downstream_level"] = pd.NA

# write model
ribasim_toml = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model", f"{SHORT_NAME}.toml")

model.pump.static.df["meta_func_afvoer"] = 1
model.pump.static.df["meta_func_aanvoer"] = 0
model.write(ribasim_toml)

# run model
if MODEL_EXEC:
    model.run()
    controle_output = Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path)
    indicators = controle_output.run_all()
