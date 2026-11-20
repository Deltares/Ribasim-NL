# %%
import geopandas as gpd
from ribasim_nl.control import (
    add_controllers_to_supply_area,
    add_controllers_to_uncontrolled_connector_nodes,
    mark_level_update_protected,
    supply_area_polygon,
)
from ribasim_nl.junctions import junctionify
from ribasim_nl.run_model import write_steady_state_scenarios

from ribasim_nl import CloudStorage, Model

# %%
# Globale settings

MODEL_EXEC: bool = False  # execute model run
AUTHORITY: str = "DeDommel"  # authority
SHORT_NAME: str = "dommel"  # short_name used in toml-file
CONTROL_NODE_TYPES = ["Outlet", "Pump"]
IS_SUPPLY_NODE_COLUMN: str = "meta_supply_node"

# Sluizen die geen rol hebben in de waterverdeling (aanvoer/afvoer), maar wel in het model zitten
EXCLUDE_NODES = {}
EXCLUDE_SUPPLY_NODES = []

cloud = CloudStorage()
ribasim_model_dir = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_parameterized_model")
ribasim_toml = ribasim_model_dir / f"{SHORT_NAME}.toml"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_aanvoer.qlr")

aanvoergebieden_gpkg = cloud.joinpath(r"DeDommel/verwerkt/sturing/aanvoergebieden.gpkg")


# %%
# Read data
model = Model.read(ribasim_toml)
# nodes are referred to by their key, see docs/dev/node-keys.qmd
n = model.node_id
lk = model.link_id

# Handmatige inlaatcapaciteiten uit WATAK.
outlet_max_flow_rate_by_node_id = {
    n("DSH0023156"): 0.6,  # Olen
    n("DO28-st1"): 0.16,  # Sonse Heide
    n("inlaat-keersop"): 0.25,  # Bocholt naar Herentals, max aanvoer??, niet in WATAK
}
outlet_max_flow_rate_from_results = {
    n("GA1-st2"): 21,  # GA1-st2
    n("DO80-st3"): 3,  # DO80-st3
    n("DO80-st5"): 6,  # DO80-st5
    n("DO80-st2"): 3,  # DO80-st2
    n("DO80-st1"): 2,  # DO80-st1
    n("KS50-st5"): 18,  # KS50-st5
    n("DSH0021865"): 15,  # DSH0021865
    n("DSH0002859"): 15,  # DSH0002859
    n("RS304-KDU24"): 9,  # RS304-KDU24
}
outlet_max_flow_rate_coupled_by_node_id = {
    n("KD25-dr3"): 8,  # KD25-dr3; gekoppeld max=4.58, parameterized=3.23
    n("KD25-st3"): 9,  # KD25-st3; gekoppeld max=5.12, parameterized=3.14
    n("DSH0001997"): 8,  # gekoppeld max=4.06, huidige max=4.00, link=2700460
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
# Handmatige indeling control-, supply- en drain-nodes.
# Houd deze lange data-lijsten compact; formatters klappen ze anders uit naar een node-id per regel.

# fmt: off
flow_control_nodes = [n("DO78-st2"), n("DO79 stuw Damianen"), n("DO79 stuw Wolvensteeg"), n("DO80-st5"), n("DO78-st1"), n("onbekend"), n("KS50-st5")]

supply_nodes = [n("KS52-st2"), n("DSH0019985"), n("DO28-st1"), n("DSH0021558"), n("DSH0023156"), n("DSH0019826"), n("ow:DO23_1"), n("up:RS186-KSY1"), n("inlaat-keersop")]

drain_nodes = [n("DL62-st1"), n("DSH0018205"), n("DSH0017161"), n("DO84-gm1"), n("DSH0016456"), n("DSH0016725"), n("DSH0016469"), n("KS48-st1"), n("ow:DL1_43"), n("ow:KS50_0"), n("ow:KS52_6"), n("ow:KS40_16"), n("ow:DO28_3"), n("ow:DO23_0"), n("ow:DO53_0"), n("ow:DO86_0")]

flushing_nodes = {}

SUPPLY_AREA_IGNORE_LINKS = {
    "Son en Breugel": [lk("ow:DO28_3", "up:ow:DO1_55"), lk("ow:DO53_0", "up:ow:DO1_40"), lk("up:DO1-dr1", "DO1-dr1"),lk("up:DO1-dr1", "ow:DO1.3_0@1"), lk("up:DSH0000083", "ow:DO28_3"), lk("up:ow:DO1_55", "ow:DO1_55")],
    "Olen": [lk("ow:DO88_0", "up:onbekend"), lk("ow:DO81_0", "up:DO79 stuw Wolvensteeg"), lk("ow:DO83_0", "up:DO79 stuw Wolvensteeg"), lk("ow:DO91_0", "up:DO78-st1"), lk("ow:DO23_0", "up:ow:DO1_57"), lk("ow:DO1_61", "up:ow:DO1_57"), lk("up:DSH0017681", "ow:DO91_0"), lk("up:ow:DO23_0", "ow:DO23_0"), lk("up:ow:DO81_0", "ow:DO81_0")],
    "Beekloop": [lk("ow:KS50_0", "up:KS40_7___2"), lk("ow:KS1_17", "up:KS1-dr5"), lk("up:ow:KS42_0", "ow:KS42_0"), lk("up:KS40_7___2", "KS40_7___2"), lk("bnd:5", "up:KS40_7___2"), lk("loc:155032,366181", "up:KS40_7___2")],
}
# fmt: on


# %%
# Helpers


def add_supply_area_control(
    model: Model,
    aanvoergebieden_df: gpd.GeoDataFrame,
    area_name: str,
    ignore_intersecting_links: list[int],
    supply_nodes: list[int],
    drain_nodes: list[int],
    flow_control_nodes: list[int],
) -> None:
    add_controllers_to_supply_area(
        model=model,
        polygon=supply_area_polygon(aanvoergebieden_df, area_name),
        exclude_nodes=EXCLUDE_NODES,
        ignore_intersecting_links=ignore_intersecting_links,
        drain_nodes=drain_nodes,
        flushing_nodes={},
        supply_nodes=supply_nodes,
        flow_control_nodes=flow_control_nodes,
        control_node_types=CONTROL_NODE_TYPES,
        flow_rate_aanvoer=20.0,
        max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
        flow_rate_afvoer=100.0,
        max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
    )


# %%
# Definieren paden en syncen met cloud

assert model.outlet.static.df is not None
outlet_max_flow_rate_aanvoer_by_node_id = dict.fromkeys(model.outlet.static.df.node_id.astype(int), 10.0)

# Handmatige inlaatcapaciteiten gelden ook in aanvoer; niet terugvallen op de default van 10 m3/s.
outlet_max_flow_rate_aanvoer_by_node_id.update(outlet_max_flow_rate_by_node_id)

aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")

# %% Fixes
outlet_ids = [
    n("ow:DO254.1-AA_1"),
    n("ow:DO23_0"),
    n("ow:KS50_0"),
    n("ow:KS52_6"),
    n("ow:KS40_16"),
    n("ow:DO23_1"),
    n("ow:DO28_3"),
    n("ow:KD4-AA_0"),
    n("ow:DO53_0"),
    n("ow:DO86_0"),
]

for node_id in dict.fromkeys(outlet_ids):
    model.update_node(node_id=node_id, node_type="Outlet")

model.update_node(node_id=n("ow:DL1_43"), node_type="Outlet")

# Deze knoop is geen gestuurd kunstwerk maar open doorstroming.
model.update_node(node_id=n("DO1-dr1"), node_type="ManningResistance")

# %%
# Toevoegen aanvoergebieden

for area_name, ignore_intersecting_links in SUPPLY_AREA_IGNORE_LINKS.items():
    print(f"Toevoegen {area_name}")
    add_supply_area_control(
        model=model,
        aanvoergebieden_df=aanvoergebieden_df,
        area_name=area_name,
        ignore_intersecting_links=ignore_intersecting_links,
        drain_nodes=drain_nodes,
        supply_nodes=supply_nodes,
        flow_control_nodes=flow_control_nodes,
    )

# %% Toevoegen waar nog geen sturing is toegevoegd

add_controllers_to_uncontrolled_connector_nodes(
    model=model,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    exclude_nodes=list(EXCLUDE_NODES),
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
)

# Pomp-capaciteiten op basis van hoogste berekende dynamic debiet, afgerond naar boven.
pump_max_flow_rate_from_results = {
    n("GA53-gm1"): 7,  # 't Goor; gekoppeld max=6.24, huidige max=3.00, link=2700754
    n("DL1-7-gm2"): 60,  # Oude Gracht uit; oude static flow_rate=39.8
}
assert model.pump.static.df is not None
mask = (
    model.pump.static.df.node_id.isin(pump_max_flow_rate_from_results)
    & model.pump.static.df.flow_rate.notna()
    & (model.pump.static.df.flow_rate > 0)
)
model.pump.static.df.loc[mask, "max_flow_rate"] = model.pump.static.df.loc[mask, "node_id"].map(
    pump_max_flow_rate_from_results
)

# %% Junctionfy(!)
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

max_flow_rate = model.outlet.static.df["node_id"].map(outlet_max_flow_rate_by_node_id)
aanvoer_mask = (
    model.outlet.static.df["control_state"].eq("aanvoer")
    & model.outlet.static.df["node_id"].isin(aanvoer_only_node_ids)
    & max_flow_rate.notna()
)
model.outlet.static.df.loc[aanvoer_mask, "flow_rate"] = max_flow_rate[aanvoer_mask]
model.outlet.static.df.loc[aanvoer_mask, "max_flow_rate"] = max_flow_rate[aanvoer_mask]

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
manual_max_flow_rate_node_ids.add(379)
protected_max_flow_rate_node_ids = set(EXCLUDE_NODES) | flow_demand_controlled_node_ids | manual_max_flow_rate_node_ids
for static_df in (model.outlet.static.df, model.pump.static.df):
    assert static_df is not None
    afvoer_mask = (
        static_df["control_state"].eq("afvoer")
        & static_df["flow_rate"].fillna(0).gt(0)
        & ~static_df["node_id"].isin(protected_max_flow_rate_node_ids)
    )
    static_df.loc[afvoer_mask, "max_flow_rate"] = static_df.loc[afvoer_mask, "max_flow_rate"].fillna(10).clip(lower=10)

for static_df in (model.outlet.static.df, model.pump.static.df):
    afvoer_mask = static_df["control_state"].eq("afvoer") & static_df["node_id"].isin(aanvoer_only_node_ids)
    static_df.loc[afvoer_mask, ["flow_rate", "max_flow_rate"]] = 0.0

# Model run


assert model.discrete_control.condition.df is not None
model.discrete_control.condition.df.loc[model.discrete_control.condition.df.time.isna(), ["time"]] = model.starttime

# %% Verdeelwerk bij Eindhoven 10% afvgevoerd via Beatrixkanaal (max 20m3/s) en 90% via Dieze
model.outlet.static.df.loc[model.outlet.static.df.node_id == n("DL1.10-st1"), "max_flow_rate"] = 0
mask = model.outlet.static.df.node_id.isin([n("DL1-st5"), n("DL62-st1")])
model.outlet.static.df.loc[mask, "min_upstream_level"] = 16.2
mark_level_update_protected(model.outlet.static.df, mask, model=model)

# flow rates WATAK Olen en Sonse Heide
flow_updates = {
    n("DO1-st3"): 200,  # Vughterstuw
    n("DL62-st1"): 20,  # Blaatthem
    n("DL1-st5"): 200,  # Verdeelwerk
}

mask = model.outlet.static.df.node_id.isin(flow_updates.keys()) & (model.outlet.static.df["control_state"] == "afvoer")

model.outlet.static.df.loc[mask, "max_flow_rate"] = model.outlet.static.df.loc[mask, "node_id"].map(flow_updates)

# %%

# write the dry and wet steady state scenarios and the full control model
model = write_steady_state_scenarios(
    model,
    AUTHORITY,
    SHORT_NAME,
    dry_evaporation_mm_per_day=1,
    model_exec=MODEL_EXEC,
    qlr_path=qlr_path,
)

# %%
