# %%
import geopandas as gpd
from peilbeheerst_model.controle_output import Control
from ribasim_nl.control import (
    add_controllers_to_supply_area,
    add_controllers_to_uncontrolled_connector_nodes,
    mark_level_update_protected,
)
from ribasim_nl.junctions import junctionify
from ribasim_nl.parametrization.basin_tables import update_basin_static

from ribasim_nl import CloudStorage, Model


def set_flow_rate(static_df, max_flow_rate_by_node_id: dict[int, float], flow_rates: list[str] | None = None) -> None:
    max_flow_rate = static_df["node_id"].map(max_flow_rate_by_node_id)
    mask = max_flow_rate.notna()
    flow_rates = ["flow_rate", "max_flow_rate"] if flow_rates is None else flow_rates

    for flow_rate in flow_rates:
        static_df.loc[mask, flow_rate] = max_flow_rate[mask]


# %%
# Globale settings

MODEL_EXEC: bool = False  # execute model run
AUTHORITY: str = "RijnenIJssel"  # authority
SHORT_NAME: str = "wrij"  # short_name used in toml-file
CONTROL_NODE_TYPES = ["Outlet", "Pump"]
IS_SUPPLY_NODE_COLUMN: str = "meta_supply_node"
drain_nodes = [59]
supply_nodes = [654, 479, 437, 438, 677, 409, 1190, 390, 428]
flushing_nodes = {
    320: 3,
    337: 0.9,
    161: 0.1,
    349: 0.2,  # 0.1-0.3 m3/s volgens waterschap
}
flow_control_nodes = [306]

outlet_max_flow_rate_by_node_id = {
    120: 1.3,  # Lochem
    90: 0.6,  # Herkel
}
outlet_max_flow_rate_from_results = {
    65: 56,  # Stokkersbrug
    68: 29,  # Buurserbeek Gaathuizenweg 52
    169: 96,  # Het Klooster
    322: 29,  # ST80160035
    329: 26,  # Kuipersbrug
    466: 17,  # DR09180001
    471: 2,  # DR80740011
    472: 2,  # AF80740003
    493: 2,  # DR84620035
    516: 26,  # St96110095
    575: 26,  # ST80160105
    584: 29,  # ST80180034
    587: 54,  # Voorst
    591: 20,  # ST96230089
    1194: 27,  # ST96210074
}
outlet_max_flow_rate_coupled_by_node_id = {
    390: 2,  # gekoppeld max=0.96, huidige max=0.00, link=700040
    409: 11,  # gekoppeld max=6.45, huidige max=0.00, link=700489
    428: 3,  # gekoppeld max=1.48, huidige max=0.00, link=700214
    1190: 12,  # gekoppeld max=7.13, huidige max=0.00, link=701396
}
pump_max_flow_rate_by_node_id = {
    654: 1.4,  # Schipbeek
}
drain_nodes = [59]
supply_nodes = [654, 479, 437, 438, 677, 409, 1190, 390, 428]
flushing_nodes = {
    320: 3,
    337: 0.9,
    161: 0.1,
    349: 0.2,  # 0.1-0.3 m3/s volgens waterschap
}
flow_control_nodes = [306]

outlet_max_flow_rate_by_node_id = {
    120: 1.3,  # Lochem
    90: 0.6,  # Herkel
}
outlet_max_flow_rate_from_results = {
    65: 56,  # Stokkersbrug
    68: 29,  # Buurserbeek Gaathuizenweg 52
    169: 96,  # Het Klooster
    322: 29,  # ST80160035
    329: 26,  # Kuipersbrug
    466: 17,  # DR09180001
    471: 2,  # DR80740011
    472: 2,  # AF80740003
    493: 2,  # DR84620035
    516: 26,  # St96110095
    575: 26,  # ST80160105
    584: 29,  # ST80180034
    587: 54,  # Voorst
    591: 20,  # ST96230089
    1194: 27,  # ST96210074
    436: 0,  # Bredevoort richting Keizersbeek (doet niets in normale situaties)
}
outlet_max_flow_rate_coupled_by_node_id = {
    390: 2,  # gekoppeld max=0.96, huidige max=0.00, link=700040
    409: 11,  # gekoppeld max=6.45, huidige max=0.00, link=700489
    428: 3,  # gekoppeld max=1.48, huidige max=0.00, link=700214
    1190: 12,  # gekoppeld max=7.13, huidige max=0.00, link=701396
}
pump_max_flow_rate_by_node_id = {
    654: 1.4,  # Schipbeek
}

# Sluizen die geen rol hebben in de waterverdeling (aanvoer/afvoer), maar wel in het model zitten
EXCLUDE_NODES = {436}

# %%
# Definieren paden en syncen met cloud
cloud = CloudStorage()
ribasim_model_dir = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_parameterized_model")
ribasim_toml = ribasim_model_dir / f"{SHORT_NAME}.toml"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_aanvoer.qlr")
aanvoergebieden_gpkg = cloud.joinpath(rf"{AUTHORITY}/verwerkt/sturing/aanvoergebieden.gpkg")
cloud.synchronize(filepaths=[aanvoergebieden_gpkg, qlr_path])


# %%
# Read data
model = Model.read(ribasim_toml)

outlet_max_flow_rate_by_node_id = {
    654: 1.4,  # Schipbeek
    120: 1.3,  # Lochem
    90: 0.6,  # Herkel
    437: 0.05,  # Sifon Boven-Slinge
    438: 0.05,  # Sifon Boven-Slinge
    63: 48,  # Haarlo -> Bolksbeek (Capaciteit uit DM, 40% van totaal)
    581: 72,  # Haarlo -> Berkel (60% van totaal)
    117: 88,  # Aflaatwerk Lochem
    320: 22,  # Verdeelwerk Lochem Berkel
    337: 0.9,  # Warken Berkel (Zutphen)
    161: 0.1,  # Warken Brummeler Laak (Gemaal Helbergen)
    121: 110,  # Eefde aflaatwerk
    59: 8,  # Noodoverloop Schipbeek Twentekanaal (uit waterakkoord Twentekanalen)
    71: 8.87,  # Capaciteit volgens DM rapportage
    555: 5.3,  # Capaciteit volgens DM rapportage
    623: 10.3,  # Capaciteit volgens DM rapportage
    1192: 5.2,  # Capaciteit volgens DM rapportage
    560: 1.0,  # Capaciteit volgens DM rapportage
    677: 0.05,  # Verdeelwerk Koppelleiding
}
outlet_max_flow_rate_afvoer_by_node_id = {}
for max_flow_rates in (
    outlet_max_flow_rate_from_results,
    outlet_max_flow_rate_coupled_by_node_id,
):
    for node_id, max_flow_rate in max_flow_rates.items():
        outlet_max_flow_rate_afvoer_by_node_id[node_id] = max(
            outlet_max_flow_rate_afvoer_by_node_id.get(node_id, 0.0),
            max_flow_rate,
        )
outlet_max_flow_rate_afvoer_by_node_id.update(outlet_max_flow_rate_by_node_id)
outlet_max_flow_rate_aanvoer_by_node_id = dict.fromkeys(model.outlet.static.df.node_id.astype(int), 10.0)

# Handmatige inlaatcapaciteiten gelden ook in aanvoer; niet terugvallen op de default van 10 m3/s.
outlet_max_flow_rate_aanvoer_by_node_id.update(outlet_max_flow_rate_by_node_id)
max_flow_rate_aanvoer_by_node_id = {
    **outlet_max_flow_rate_aanvoer_by_node_id,
    **pump_max_flow_rate_by_node_id,
}

# capaciteit inlaten
set_flow_rate(static_df=model.outlet.static.df, max_flow_rate_by_node_id=outlet_max_flow_rate_by_node_id)

# markeer inlaten
model.node.df[IS_SUPPLY_NODE_COLUMN] = False
mask = model.node.df["name"].str.contains("inlaat", case=False, na=False)
model.node.df.loc[mask, IS_SUPPLY_NODE_COLUMN] = True


aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")


# %%
# Schipbeek

polygon = aanvoergebieden_df.loc[["Schipbeek"], "geometry"].union_all().buffer(1).buffer(-1)
# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = []

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    flow_control_nodes=flow_control_nodes,
    supply_nodes=supply_nodes,
    is_supply_node_column=IS_SUPPLY_NODE_COLUMN,
    control_node_types=CONTROL_NODE_TYPES,
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
)

# %%
# Herkel

polygon = aanvoergebieden_df.loc[["Herkel"], "geometry"].union_all().buffer(1).buffer(-1)
# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = []

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    is_supply_node_column=IS_SUPPLY_NODE_COLUMN,
    control_node_types=CONTROL_NODE_TYPES,
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
)

# %%
# Lochem

polygon = aanvoergebieden_df.loc[["Lochem"], "geometry"].union_all().buffer(1).buffer(-1)
# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = []

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    is_supply_node_column=IS_SUPPLY_NODE_COLUMN,
    control_node_types=CONTROL_NODE_TYPES,
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
)

# %%
# En de rest toevoegen


add_controllers_to_uncontrolled_connector_nodes(
    model=model,
    exclude_nodes=list(EXCLUDE_NODES),
    supply_nodes=supply_nodes,
    flushing_nodes=flushing_nodes,
    flow_control_nodes=flow_control_nodes,
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
)

# %%

# Lochem aflaatwerk limiteren tot 32 m3/s
mask = (model.outlet.static.df.node_id == 117) & (model.outlet.static.df.control_state == "afvoer")
model.outlet.static.df.loc[mask, "max_flow_rate"] = 32

# Verdeelwerk Lochem Berkel limiteren tot 3 m3/s in aanvoerstand.
mask = (model.outlet.static.df.node_id == 320) & (model.outlet.static.df.control_state == "aanvoer")
model.outlet.static.df.loc[mask, "max_flow_rate"] = 3.1

# Controller 1330: ook bij truth_state FF naar aanvoer.
mask = (model.discrete_control.logic.df.node_id == 1330) & (model.discrete_control.logic.df.truth_state == "FF")
model.discrete_control.logic.df.loc[mask, "control_state"] = "aanvoer"


# Verdeelwerk Hackfort limiteren tot 0.2 m3/s in aanvoerstand.
mask = (model.outlet.static.df.node_id == 349) & (model.outlet.static.df.control_state == "aanvoer")
model.outlet.static.df.loc[mask, "max_flow_rate"] = 0.2

# Noodoverloop Twentekanaal pas bij onvoldoende door sifon (node_id 306)
mask = model.outlet.static.df.node_id == 59
model.outlet.static.df.loc[mask, "min_upstream_level"] += 0.1
mark_level_update_protected(model.outlet.static.df, mask, model=model)

# doorlaten
min_flow_rates = {
    320: 3,
    161: 0.1,
    337: 0.9,
}

set_flow_rate(model.outlet.static.df, max_flow_rate_by_node_id=min_flow_rates, flow_rates=["min_flow_rate"])

# %% Junctionfy!
junctionify(model)

aanvoer_only_node_ids = set(supply_nodes) - set(drain_nodes) - set(flow_control_nodes)

# Aanvoer-cap: doorlaten/inlaten mogen in aanvoer niet de hoge afvoercapaciteit gebruiken.
aanvoer_outlet_mask = model.outlet.static.df.control_state == "aanvoer"
model.outlet.static.df.loc[aanvoer_outlet_mask, ["flow_rate", "max_flow_rate"]] = model.outlet.static.df.loc[
    aanvoer_outlet_mask, ["flow_rate", "max_flow_rate"]
].clip(upper=10.0)
zero_aanvoer_node_ids = {
    node_id for node_id, max_flow_rate in outlet_max_flow_rate_aanvoer_by_node_id.items() if max_flow_rate == 0
}
zero_aanvoer_mask = aanvoer_outlet_mask & model.outlet.static.df.node_id.isin(zero_aanvoer_node_ids)
model.outlet.static.df.loc[zero_aanvoer_mask, ["flow_rate", "max_flow_rate"]] = 0.0

for static_df, max_flow_rate_by_node_id in (
    (model.outlet.static.df, outlet_max_flow_rate_by_node_id),
    (model.pump.static.df, pump_max_flow_rate_by_node_id),
):
    max_flow_rate = static_df["node_id"].map(max_flow_rate_by_node_id)
    aanvoer_mask = (
        static_df["control_state"].eq("aanvoer")
        & static_df["node_id"].isin(aanvoer_only_node_ids)
        & max_flow_rate.notna()
    )
    static_df.loc[aanvoer_mask, "flow_rate"] = max_flow_rate[aanvoer_mask]
    static_df.loc[aanvoer_mask, "max_flow_rate"] = max_flow_rate[aanvoer_mask]

# Afvoer-cap: voorkom blokkades door te lage max_flow_rate in afvoer.
node_type_by_id = model.node.df["node_type"].to_dict()
flow_demand_controlled_node_ids = set(
    model.link.df.loc[
        model.link.df["from_node_id"].map(node_type_by_id).eq("FlowDemand"),
        "to_node_id",
    ]
    .dropna()
    .astype(int)
)
manual_max_flow_rate_node_ids = set(outlet_max_flow_rate_by_node_id)
manual_max_flow_rate_node_ids.update(pump_max_flow_rate_by_node_id)
protected_max_flow_rate_node_ids = set(EXCLUDE_NODES) | flow_demand_controlled_node_ids | manual_max_flow_rate_node_ids
for static_df in (model.outlet.static.df, model.pump.static.df):
    afvoer_mask = (
        static_df["control_state"].eq("afvoer")
        & static_df["flow_rate"].fillna(0).gt(0)
        & ~static_df["node_id"].isin(protected_max_flow_rate_node_ids)
    )
    static_df.loc[afvoer_mask, "max_flow_rate"] = (
        static_df.loc[afvoer_mask, "max_flow_rate"].fillna(0.5).clip(lower=0.5)
    )

for static_df in (model.outlet.static.df, model.pump.static.df):
    afvoer_mask = static_df["control_state"].eq("afvoer") & static_df["node_id"].isin(aanvoer_only_node_ids)
    static_df.loc[afvoer_mask, ["flow_rate", "max_flow_rate"]] = 0.0

# %%
# Model run

ribasim_toml_wet = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_wet", f"{SHORT_NAME}.toml")
ribasim_toml_dry = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_dry", f"{SHORT_NAME}.toml")
ribasim_toml = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model", f"{SHORT_NAME}.toml")

model.discrete_control.condition.df.loc[model.discrete_control.condition.df.time.isna(), ["time"]] = model.starttime

# hoofd run met verdamping
update_basin_static(model=model, evaporation_mm_per_day=0.1)
model.write(ribasim_toml_dry)

# run hoofdmodel
if MODEL_EXEC:
    model.run()
    Control(ribasim_toml=ribasim_toml_dry, qlr_path=qlr_path).run_all()
    model = Model.read(ribasim_toml_dry)

# prerun om het model te initialiseren met neerslag
update_basin_static(model=model, precipitation_mm_per_day=2)
model.write(ribasim_toml_wet)

# run prerun model
if MODEL_EXEC:
    model.run()
    Control(ribasim_toml=ribasim_toml_wet, qlr_path=qlr_path).run_all()
    model = Model.read(ribasim_toml_wet)

# hoofd run
update_basin_static(model=model, precipitation_mm_per_day=1.5)
model.write(ribasim_toml)
# run hoofdmodel
if MODEL_EXEC:
    model.run()
    Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path).run_all()
