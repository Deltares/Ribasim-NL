# %%
import geopandas as gpd
import pandas as pd

from peilbeheerst_model import ribasim_parametrization
from peilbeheerst_model.controle_output import Control
from ribasim_nl import CloudStorage, Model, check_basin_level
from ribasim_nl.from_to_nodes_and_levels import add_from_to_nodes_and_levels
from ribasim_nl.parametrization.basin_tables import update_basin_static

# execute model run
MODEL_EXEC: bool = False

# model settings
AUTHORITY: str = "DrentsOverijsselseDelta"
SHORT_NAME: str = "dod"

# connect with the GoodCloud
cloud = CloudStorage()

# collect relevant data from the GoodCloud
ribasim_model_dir = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_parameterized_model")
ribasim_toml = ribasim_model_dir / f"{SHORT_NAME}.toml"
qlr_path = cloud.joinpath("Basisgegevens", "QGIS_lyr", "output_controle_vaw_aanvoer.qlr")
aanvoer_path = cloud.joinpath(AUTHORITY, "aangeleverd", "Na_levering", "HyDAMO_WM_20230720.gpkg")
model_edits_aanvoer_gpkg = cloud.joinpath(AUTHORITY, "verwerkt", "model_edits_aanvoer.gpkg")

cloud.synchronize(
    filepaths=[
        aanvoer_path,
    ]
)

# %%
# read model
model = Model.read(ribasim_toml)
original_model = model.model_copy(deep=True)
update_basin_static(model=model, evaporation_mm_per_day=1)


# alle niet-gecontrolleerde basins krijgen een meta_streefpeil uit de final state van de parameterize_model.py
update_levels = model.basin_outstate.df.set_index("node_id")["level"]
basin_ids = model.basin.node.df[model.basin.node.df["meta_gestuwd"] == "False"].index
mask = model.basin.area.df["node_id"].isin(basin_ids)
model.basin.area.df.loc[mask, "meta_streefpeil"] = model.basin.area.df[mask]["node_id"].apply(
    lambda x: update_levels[x]
)
add_from_to_nodes_and_levels(model)

# filter aanvoergebieden
aanvoergebieden_df = gpd.read_file(aanvoer_path, layer="afvoergebiedaanvoergebied")
aanvoergebieden_df = aanvoergebieden_df[aanvoergebieden_df["soortafvoeraanvoergebied"] == "Aanvoergebied"]
aanvoergebieden_df = gpd.GeoDataFrame({"geometry": list(aanvoergebieden_df.union_all().geoms)}, crs=28992)
aanvoergebieden_df_dissolved = aanvoergebieden_df.dissolve()

# re-parameterize
ribasim_parametrization.set_aanvoer_flags(model, aanvoergebieden_df_dissolved, overruling_enabled=False)
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

# Hoofdinlaten krijgen 10m3/s

node_ids = model.outlet.static.df[model.outlet.static.df["meta_categorie"] == "Inlaat"]["node_id"].to_numpy()
model.outlet.static.df.loc[model.outlet.static.df["node_id"].isin(node_ids), "max_flow_rate"] = 0.1


model.outlet.static.df.loc[
    model.outlet.static.df.node_id.isin(model.upstream_connection_node_ids(node_type="Outlet")), "flow_rate"
] = 10
model.pump.static.df.loc[
    model.pump.static.df.node_id.isin(model.upstream_connection_node_ids(node_type="Pump")), "flow_rate"
] = 10
model.outlet.static.df.loc[
    model.outlet.static.df.node_id.isin(model.upstream_connection_node_ids(node_type="Outlet")), "max_flow_rate"
] = 10
model.pump.static.df.loc[
    model.pump.static.df.node_id.isin(model.upstream_connection_node_ids(node_type="Pump")), "max_flow_rate"
] = 10

# %% sturing uit alle niet-gestuwde outlets halen
node_ids = model.outlet.node.df[model.outlet.node.df["meta_gestuwd"] == "False"].index
non_control_mask = model.outlet.static.df["node_id"].isin(node_ids)
model.outlet.static.df.loc[non_control_mask, "min_upstream_level"] = pd.NA
model.outlet.static.df.loc[non_control_mask, "max_downstream_level"] = pd.NA


# write model
ribasim_toml = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model", f"{SHORT_NAME}.toml")
check_basin_level.add_check_basin_level(model=model)
model.pump.static.df["meta_func_afvoer"] = 1
model.pump.static.df["meta_func_aanvoer"] = 0
model.write(ribasim_toml)

# run model
if MODEL_EXEC:
    # TODO: Different ways of executing the model; choose the one that suits you best:
    ribasim_parametrization.tqdm_subprocess(["ribasim", ribasim_toml], print_other=False, suffix="init")
    # exit_code = model.run()

    # assert exit_code == 0

    """Note that currently, the Ribasim-model is unstable but it does execute, i.e., the model re-parametrisation is
    successful. This might be due to forcing the schematisation with precipitation while setting the 'sturing' of the
    outlets on 'aanvoer' instead of the more suitable 'afvoer'. This should no longer be a problem once the next step of
    adding `ContinuousControl`-nodes is implemented.
    """

    controle_output = Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path)
    indicators = controle_output.run_all()
