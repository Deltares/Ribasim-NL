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

# %%
# Globale settings

MODEL_EXEC: bool = False  # execute model run
AUTHORITY: str = "Noorderzijlvest"  # authority
SHORT_NAME: str = "nzv"  # short_name used in toml-file
CONTROL_NODE_TYPES = ["Outlet", "Pump"]
IS_SUPPLY_NODE_COLUMN: str = "meta_supply_node"
SCHUTVERLIES_FLOW_RATE_BY_NODE_ID = {
    #    1756: 1.5,  # Oostersluis
}
INLAAT_FLOW_RATE_AANVOER_BY_NODE_ID = {
    1741: 26.0,  # Gaarkeuken
}
GAARKEUKEN_OUTLET_NODE_ID = 1741
GAARKEUKEN_MIN_UPSTREAM_LEVEL = -0.93
OUTLET_FLOW_RATE_AFVOER_OVERRIDE_BY_NODE_ID = {
    724: 400.0,
    728: 9999.0,
    732: 50.0,
    # 1748: 0.0,Checken!
    1753: 5.0,
    # 1754: 0.0,Checken!
    # 1755: 0.0,Checken!
}


# Sluizen die geen rol hebben in de waterverdeling (aanvoer/afvoer), maar wel in het model zitten
# 722: KSL001 Waterkering Westerwijtwerdermaar
# 716: INL499 (opgeheven)
# 725: Beijumersluis
# 1738: Westerhavensluis
# 1740: Jan B. Bronssluis
# 1744: Gaarkeukensluis
# 1745: Groevesluis Noord
# 1746: Roggenkampsluis
# 1756: Oostersluis
# 1752: Dokwerdersluis
EXCLUDE_NODES = {716, 722, 725, 1738, 1740, 1744, 1745, 1746, 1752, 1756}

# %%
# Definieren paden en syncen met cloud
cloud = CloudStorage()
ribasim_model_dir = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_parameterized_model")
ribasim_toml = ribasim_model_dir / f"{SHORT_NAME}.toml"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_aanvoer.qlr")
aanvoergebieden_gpkg = cloud.joinpath(r"Noorderzijlvest/verwerkt/sturing/aanvoergebieden.gpkg")
cloud.synchronize(filepaths=[aanvoergebieden_gpkg, qlr_path])


def configure_gaarkeuken_control(model: Model) -> None:
    outlet_mask = model.outlet.static.df["node_id"].eq(GAARKEUKEN_OUTLET_NODE_ID) & model.outlet.static.df[
        "control_state"
    ].eq("aanvoer")
    if outlet_mask.sum() != 1:
        raise ValueError(f"Expected one aanvoer row for Gaarkeuken outlet {GAARKEUKEN_OUTLET_NODE_ID}")
    model.outlet.static.df.loc[outlet_mask, "min_upstream_level"] = GAARKEUKEN_MIN_UPSTREAM_LEVEL
    mark_level_update_protected(model.outlet.static.df, outlet_mask)


# %%
# Read data
model = Model.read(ribasim_toml)
model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id == 16, "level"] = -1.0

outlet_flow_rate_afvoer_by_node_id = (
    model.outlet.static.df.dropna(subset=["flow_rate"]).set_index("node_id")["flow_rate"].astype(float).to_dict()
)
outlet_flow_rate_afvoer_by_node_id = {
    int(node_id): flow_rate for node_id, flow_rate in outlet_flow_rate_afvoer_by_node_id.items()
}
outlet_flow_rate_afvoer_by_node_id.update(OUTLET_FLOW_RATE_AFVOER_OVERRIDE_BY_NODE_ID)
outlet_max_flow_rate_aanvoer_by_node_id = {}
for outlet_static in model.outlet.static.df.itertuples():
    max_flow_rate = outlet_static.max_flow_rate
    if max_flow_rate != max_flow_rate:
        max_flow_rate = outlet_static.flow_rate
    outlet_max_flow_rate_aanvoer_by_node_id[int(outlet_static.node_id)] = (
        0.0 if max_flow_rate != max_flow_rate else float(max_flow_rate)
    )

# Oostersluis maken we een pomp voor schutverlies als uitlaat, nog testen
# model.update_node(node_id=1756)

aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")

# %%
# Identificeren aanvoerknopen en voorzien van afvoercapaciteit

# aanmaken node_df en specificeren supply_nodes
# knopen die beginnen met INL, i of eindigen op i, maar niet op fictief
for node_type in CONTROL_NODE_TYPES:
    # set is supply
    node_df = model.get_component(node_type).node.df
    model.node.df.loc[node_df.index, IS_SUPPLY_NODE_COLUMN] = (
        node_df["meta_code_waterbeheerder"].str.startswith("INL")
        | node_df["meta_code_waterbeheerder"].str.startswith("i")
        | node_df["meta_code_waterbeheerder"].str.endswith("i")
    ) & ~(node_df.node_type.isin(CONTROL_NODE_TYPES) & node_df["meta_code_waterbeheerder"].str.endswith("fictief"))

# %% [markdown]
# # Aanpak sturing per aanvoergebied
#
# ## verplichte invoer
# - `polygon`: Polygoon van het aanvoergebied
# ## optionele invoer:
# - `flusing_nodes`: uitlaten met doorspoeldebiet (dict met {node_id:doorspoeldebiet}). Deze worden nooit automatisch gedetecteerd (!)
# - `ignore_intersecting_links`: links die het aanvoergebied snijden, geen control-nodes bevatten en waarbij dat geaccepteerd moet worden
# - `supply_nodes`: inlaten die niet automatisch worden gedetecteerd
# - `drain_nodes`: uitlaten die niet automatisch worden gedetecteerd
#
# ## In 1x, wanneer alles goed gaat
# `node_functions_df` kun je gebruiken om e.e.a. te plotten
# ```
# node_functions_df = add_controllers_to_supply_area(
#     model=model,
#     polygon=polygon,
#     ignore_intersecting_links= ignore_intersecting_links,
#     drain_nodes=drain_nodes,
#     flushing_nodes=flushing_nodes,
#     supply_nodes=supply_nodes,
#     control_node_types=CONTROL_NODE_TYPES
# )
# ```
# ## Debuggen: in stappen
# 1. bepalen positie van alle knopen relatief tot aanvoergebied (inflow, outflow, internal)
# ```
# node_positions_df = get_control_nodes_position_from_supply_area(
#     model=model,
#     polygon=polygon,
#     control_node_types=CONTROL_NODE_TYPES,


# )
# ```
# 2. bepalen functie van alle knopen (drain, supply, flow_control and flushing)
# ```
# node_functions_df = add_control_functions_to_connector_nodes(
#     model=model,
#     node_positions=node_positions_df["position"],
#     supply_nodes=supply_nodes,
#     drain_nodes=drain_nodes,
#     flushing_nodes=flushing_nodes,
#     is_supply_node_column=IS_SUPPLY_NODE_COLUMN,
# )
# ```
#
# 3. toevoegen van controllers aan alle knopen
# ```
# add_controllers_to_connector_nodes(
#     model=model,
#     node_functions_df=node_functions_df,
# )
# ```


polygon = aanvoergebieden_df.loc[
    ["Marnerwaard", "Noordpolder", "Spijksterpompen", "Fivelingoboezem"], "geometry"
].union_all()

# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = []

# doorspoeling (op uitlaten)
# 41: Spijksterpompen
# 412: Grote Herculesstuw
flushing_nodes = {41: 0.4, 412: 0.2}

# handmatig opgegeven drain nodes (uitlaten) definieren
# 40: Noordpolder
# 412: Marnerwaard
# 442: Marnerwaard
# 837: Spijksterpompen
drain_nodes = [40, 442, 387, 837]

# handmatig opgegeven supply nodes (inlaten)
# 136: Nieuwstad
# 139: Spijk
# 943: Inlaat Spijksterpompen
# 125: Zwarte Pier
# 107: Bokumerklief
# 112: Kluten
# 116: KGM116
# 121: Slikken
# 184: Buiten

supply_nodes = [136, 139, 943, 125, 107, 112, 116, 121, 184]

# toevoegen sturing
add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    control_node_types=CONTROL_NODE_TYPES,
    flow_rate_aanvoer=10.0,
    max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_flow_rate_afvoer_by_node_id,
)

# %%
# Toevoegen Jonkersvaart

polygon = aanvoergebieden_df.at["Jonkersvaart", "geometry"]

# links die intersecten die we kunnen negeren
# 1810: sloot naast prallelweg (A7) die vrij afwatert op Oude Diep
ignore_intersecting_links: list[int] = [1810]

# doorspoeling (op uitlaten)
# 357: KST0159 Trambaanstuw
# 357: KST0159 Trambaanstuw
# 393: KST0135 Ackerenstuw
# 350: KST0053 Lage Hamrikstuw
# 401: KST0148 Mastuw
# 501: KST0379 Ooster Lietsstuw
# 383: KST0113 Lage Rietstuw
flushing_nodes = {357: 0.02, 393: 0.012, 350: 0.015, 401: 0.017, 501: 0.034, 383: 0.013}

# handmatig opgegeven drain nodes (uitlaten) definieren
# 551: KST0556
# 652: KST1072
drain_nodes = [551, 652]

# handmatig opgegeven supply nodes (inlaten)
# 37: Diepswal
# 38: Jonkersvaart
supply_nodes = [37, 38]

# toevoegen sturing
add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    control_node_types=CONTROL_NODE_TYPES,
    flow_rate_aanvoer=10.0,
    max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_flow_rate_afvoer_by_node_id,
)

# %%
# %%
# Toevoegen Peizerdiep

polygon = aanvoergebieden_df.at["Peizerdiep", "geometry"]

# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = []

# doorspoeling (op uitlaten)
# node_id: Naam
flushing_nodes = {}

# Peizerdiep: geen doorspoelvraag, alleen verdeling over doorlaten.
flow_control_nodes = [391, 419]
supply_flow_rate = {391: 0.043 * 0.75, 419: 0.043 * 0.25}
drain_flow_rate = {391: 100.0, 419: 0.0}

# handmatig opgegeven drain nodes (uitlaten) definieren
# node_id: Naam
drain_nodes = []

# handmatig opgegeven supply nodes (inlaten)
supply_nodes = []

# toevoegen sturing
add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    flow_control_nodes=flow_control_nodes,
    supply_nodes=supply_nodes,
    control_node_types=CONTROL_NODE_TYPES,
    flow_rate_aanvoer=supply_flow_rate,
    max_flow_rate_aanvoer=supply_flow_rate,
    flow_rate_afvoer=drain_flow_rate,
    max_flow_rate_afvoer=drain_flow_rate,
)

# %%
# EXCLUDE NODES op 0 m3/s zetten
mask = model.outlet.static.df.node_id.isin(EXCLUDE_NODES)
model.outlet.static.df.loc[mask, "flow_rate"] = 0.0
model.outlet.static.df.loc[mask, "min_flow_rate"] = 0.0
model.outlet.static.df.loc[mask, "max_flow_rate"] = 0.0


# %% add all remaining inlets/outlets


# 728: Lamerburen Schutsluis (bij complex De Waterwolf)
# 640: KST1027
# 641: KST1030
flow_control_nodes = [728, 640, 641]

# 39 Zuidhornder Zuidertocht
# 165: Harssensbosch
# 680: KST9970
# 1753: Gemaal Dorkwerd
supply_nodes = [39, 680, 165, 1741, 1753]

drain_nodes = []

add_controllers_to_uncontrolled_connector_nodes(
    model=model,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    drain_nodes=drain_nodes,
    exclude_nodes=list(EXCLUDE_NODES),
    flow_rate_aanvoer=10.0,
    max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_flow_rate_afvoer_by_node_id,
)

# %%
# Model run

ribasim_toml_wet = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_wet", f"{SHORT_NAME}.toml")
ribasim_toml_dry = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_dry", f"{SHORT_NAME}.toml")
ribasim_toml = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model", f"{SHORT_NAME}.toml")

model.discrete_control.condition.df.loc[model.discrete_control.condition.df.time.isna(), ["time"]] = model.starttime

# Schutverliezen als vaste ondergrens; na EXCLUDE_NODES/defaultcapaciteiten zodat dit niet wordt overschreven.
for node_id, flow_rate in SCHUTVERLIES_FLOW_RATE_BY_NODE_ID.items():
    for static_df in (model.outlet.static.df, model.pump.static.df):
        mask = static_df.node_id == node_id
        if not mask.any():
            continue
        columns = [column for column in ["flow_rate", "min_flow_rate", "max_flow_rate"] if column in static_df.columns]
        static_df.loc[mask, columns] = flow_rate

# NZV: max_flow_rate in aanvoer blijft exact leidend vanuit de parameterized xlsx/static.
aanvoer_outlet_mask = model.outlet.static.df.control_state == "aanvoer"
aanvoer_max_flow_rate = model.outlet.static.df.loc[aanvoer_outlet_mask, "node_id"].map(
    outlet_max_flow_rate_aanvoer_by_node_id
)
model.outlet.static.df.loc[aanvoer_outlet_mask, "max_flow_rate"] = aanvoer_max_flow_rate

for node_id, flow_rate in INLAAT_FLOW_RATE_AANVOER_BY_NODE_ID.items():
    mask = (model.outlet.static.df.node_id == node_id) & (model.outlet.static.df.control_state == "aanvoer")
    model.outlet.static.df.loc[mask, ["flow_rate", "max_flow_rate"]] = flow_rate

# Gaarkeuken apart overrulen: alleen de bronvoorwaarde verruimen.
configure_gaarkeuken_control(model)

# %% Junctionify(!)
junctionify(model)

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
