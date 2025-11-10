# %%

import inspect

import geopandas as gpd
import pandas as pd
from peilbeheerst_model.controle_output import Control
from ribasim_nl.from_to_nodes_and_levels import add_from_to_nodes_and_levels
from ribasim_nl.parametrization.basin_tables import update_basin_static

from peilbeheerst_model import ribasim_parametrization
from ribasim_nl import CloudStorage, Model, check_basin_level

# execute model run
MODEL_EXEC: bool = False

# model settings
AUTHORITY: str = "HunzeenAas"
SHORT_NAME: str = "hea"
MODEL_ID: str = "2025_7_0"

# connect with the GoodCloud
cloud = CloudStorage()

# collect relevant data from the GoodCloud
ribasim_model_dir = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_parameterized_model")
ribasim_toml = ribasim_model_dir / f"{SHORT_NAME}.toml"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_lyr/output_controle_vaw_aanvoer.qlr")
aanvoer_path = cloud.joinpath(AUTHORITY, "verwerkt/4_ribasim/areas.gpkg")

model_edits_aanvoer_gpkg = cloud.joinpath(AUTHORITY, "verwerkt/model_edits_aanvoer.gpkg")

cloud.synchronize(
    filepaths=[
        aanvoer_path,
    ]
)

# read model
model = Model.read(ribasim_toml)
original_model = model.model_copy(deep=True)
aanvoergebieden_df = gpd.read_file(aanvoer_path, layer="supply_areas")
aanvoergebieden_df_dissolved = aanvoergebieden_df.dissolve()
update_basin_static(model=model, evaporation_mm_per_day=1)

# alle niet-gecontrolleerde basins krijgen een meta_streefpeil uit de final state van de parameterize_model.py
update_levels = model.basin_outstate.df.set_index("node_id")["level"]
basin_ids = model.basin.node.df[model.basin.node.df["meta_gestuwd"] == "False"].index
mask = model.basin.area.df["node_id"].isin(basin_ids)
model.basin.area.df.loc[mask, "meta_streefpeil"] = model.basin.area.df[mask]["node_id"].apply(
    lambda x: update_levels[x]
)
add_from_to_nodes_and_levels(model)

# re-parameterize
ribasim_parametrization.set_aanvoer_flags(model, aanvoergebieden_df_dissolved, overruling_enabled=True)
ribasim_parametrization.determine_min_upstream_max_downstream_levels(
    model,
    AUTHORITY,
    aanvoer_upstream_offset=0.02,
    aanvoer_downstream_offset=0.0,
    afvoer_upstream_offset=0.02,
    afvoer_downstream_offset=0.0,
)
check_basin_level.add_check_basin_level(model=model)

model.manning_resistance.static.df.loc[:, "manning_n"] = 0.04
mask = model.outlet.static.df["meta_aanvoer"] == 0
model.outlet.static.df.loc[mask, "max_downstream_level"] = pd.NA
model.outlet.static.df.flow_rate = 100
model.pump.static.df.flow_rate = 100
model.outlet.static.df.max_flow_rate = original_model.outlet.static.df.max_flow_rate
model.pump.static.df.max_flow_rate = original_model.pump.static.df.max_flow_rate

# Area basin 1516 niet OK, te klein, model instabiel
model.explode_basin_area()
actions = gpd.list_layers(model_edits_aanvoer_gpkg).name.to_list()
for action in actions:
    print(action)
    # get method and args
    method = getattr(model, action)
    keywords = inspect.getfullargspec(method).args
    df = gpd.read_file(model_edits_aanvoer_gpkg, layer=action, fid_as_index=True)
    if "order" in df.columns:
        df.sort_values("order", inplace=True)
    for row in df.itertuples():
        # filter kwargs by keywords
        kwargs = {k: v for k, v in row._asdict().items() if k in keywords}
        method(**kwargs)


# %%
def set_values(df, node_ids, values: dict):
    """Hulpfunctie om meerdere kolommen tegelijk te updaten voor bepaalde node_ids."""
    mask = df["node_id"].isin(node_ids)
    for col, val in values.items():
        df.loc[mask, col] = val


# === 1. Bepaal upstream/downstream connection nodes ===
upstream_outlet_nodes = model.upstream_connection_node_ids(node_type="Outlet")
downstream_outlet_nodes = model.downstream_connection_node_ids(node_type="Outlet")
upstream_pump_nodes = model.upstream_connection_node_ids(node_type="Pump")
downstream_pump_nodes = model.downstream_connection_node_ids(node_type="Pump")

# === 2. Zet waardes voor upstream nodes ===
set_values(
    model.outlet.static.df,
    upstream_outlet_nodes,
    {
        "flow_rate": 10,
        "max_flow_rate": 10,
        "min_upstream_level": pd.NA,
    },
)
set_values(
    model.pump.static.df,
    upstream_pump_nodes,
    {
        "flow_rate": 10,
        "max_flow_rate": 10,
        "min_upstream_level": pd.NA,
    },
)

# === 3. Zet waardes voor downstream nodes ===
set_values(
    model.outlet.static.df,
    downstream_outlet_nodes,
    {
        "max_downstream_level": pd.NA,
    },
)
set_values(
    model.pump.static.df,
    downstream_pump_nodes,
    {
        "max_downstream_level": pd.NA,
    },
)

# === 2b. Verhoog mmin_upstream_level met offset voor downstream Outlets
mask = model.outlet.static.df["node_id"].isin(downstream_outlet_nodes)
model.outlet.static.df.loc[mask, "min_upstream_level"] = model.outlet.static.df.loc[mask, "min_upstream_level"] + 0.02
model.pump.static.df["max_downstream_level"] = model.pump.static.df["max_downstream_level"] - 0.02

# === 4. Zet max/min levels op NA voor niet-gestuwde, niet-verbonden outlets ===
# non_control_nodes = model.outlet.node.df.query("meta_gestuwd == 'False'").index
# excluded_nodes = set(upstream_outlet_nodes) | set(downstream_outlet_nodes)

# model.outlet.static.df.loc[
#    model.outlet.static.df["node_id"].isin(non_control_nodes) & ~model.outlet.static.df["node_id"].isin(excluded_nodes),
#    ["max_downstream_level", "min_upstream_level"],
# ] = pd.NA


# Dokwerd, sluis ten onrechte op 10m3/s gezet
model.pump.static.df.loc[model.pump.static.df.node_id == 20, "max_flow_rate"] = 20
model.pump.static.df.loc[model.pump.static.df.node_id == 20, "flow_rate"] = 20
model.pump.static.df.loc[model.pump.static.df.node_id == 152, "max_flow_rate"] = 0.1
model.pump.static.df.loc[model.pump.static.df.node_id == 152, "flow_rate"] = 0.1

# Zomerpeil KST-W-20240 en KST-W-10430(Borgerweg) was 6.55, verhoogd naar 6.8m, anders geen aanvoer mogelijk
model.outlet.static.df.loc[model.outlet.static.df.node_id == 227, "min_upstream_level"] = 6.78
model.outlet.static.df.loc[model.outlet.static.df.node_id == 227, "max_downstream_level"] = 6.78
model.outlet.static.df.loc[model.outlet.static.df.node_id == 492, "min_upstream_level"] = 6.82

# Bij boundaries downstream level nodig
model.outlet.static.df.loc[model.outlet.static.df.node_id == 2011, "max_downstream_level"] = 15.82
model.outlet.static.df.loc[model.outlet.static.df.node_id == 2014, "max_downstream_level"] = 14.92

# Oude Sluis en Nieuwsluis alleen lekverliezen, afvoer via gemaal Rozema
model.outlet.static.df.loc[model.outlet.static.df.node_id == 984, "flow_rate"] = 0.1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 986, "flow_rate"] = 0.1

# Beetserwijk/Ruiten: lek
model.outlet.static.df.loc[model.outlet.static.df.node_id == 231, "min_upstream_level"] = 6.8
model.outlet.static.df.loc[model.outlet.static.df.node_id == 548, "min_upstream_level"] = 6.8

# Dokwerd, sluis ten onrechte op 10m3/s gezet

# Alle inlaten en duikers op max cap 5m3/s
# node_ids = model.outlet.node.df[model.outlet.node.df.meta_code_waterbeheerder.str.startswith("KIN")].index.to_numpy()
# model.outlet.static.df.loc[model.outlet.static.df.node_id.isin(node_ids), "max_flow_rate"] = 5

# node_ids = model.outlet.node.df[model.outlet.node.df.meta_code_waterbeheerder.str.startswith("KDU")].index.to_numpy()
# model.outlet.static.df.loc[model.outlet.static.df.node_id.isin(node_ids), "max_flow_rate"] = 5

# write model
ribasim_toml = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model", f"{SHORT_NAME}.toml")
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
