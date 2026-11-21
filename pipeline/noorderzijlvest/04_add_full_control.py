# %%

from typing import cast

import geopandas as gpd
from ribasim_nl.control import (
    add_controllers_to_supply_area,
    add_controllers_to_uncontrolled_connector_nodes,
    set_continuous_level_control,
)
from ribasim_nl.junctions import junctionify
from ribasim_nl.run_model import write_steady_state_scenarios
from shapely.geometry import MultiPolygon, Polygon

from ribasim_nl import CloudStorage, Model

# %%
# Globale settings

MODEL_EXEC: bool = False  # execute model run
AUTHORITY: str = "Noorderzijlvest"  # authority
SHORT_NAME: str = "nzv"  # short_name used in toml-file
CONTROL_NODE_TYPES = ["Outlet", "Pump"]
IS_SUPPLY_NODE_COLUMN: str = "meta_supply_node"

# %%
# Definieren paden en syncen met cloud
cloud = CloudStorage()
ribasim_model_dir = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_parameterized_model")
ribasim_toml = ribasim_model_dir / f"{SHORT_NAME}.toml"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_aanvoer.qlr")
aanvoergebieden_gpkg = cloud.joinpath(r"Noorderzijlvest/verwerkt/sturing/aanvoergebieden.gpkg")

# %%
# Read data
model = Model.read(ribasim_toml)
# nodes and links are referred to by their key, see docs/dev/node-keys.qmd
n = model.node_id
lk = model.link_id

SCHUTVERLIES_FLOW_RATE_BY_NODE_ID = {
    #    1756: 1.5,  # Oostersluis
}
# Gaarkeuken supplies on the level of the Van Starkenborghkanaal with the relation of the Landelijk Sobek Model (LSM3),
# and keeps its source limit on the Fryslân boezem, see https://github.com/Deltares/Ribasim-NL/issues/878
GAARKEUKEN_OUTLET_NODE_ID = n("loc:216814,585025")
GAARKEUKEN_LISTEN_NODE_ID = n("up:1460")  # Van Starkenborghkanaal
GAARKEUKEN_MIN_UPSTREAM_LEVEL = -0.93
GAARKEUKEN_CONTROL = [(-2.0, 24.0), (-1.0, 24.0), (-0.95, 10.0), (-0.94, 8.7), (-0.93, 0.0)]  # level [m+NAP], m3/s
# Gaarkeukensluis: schutverlies
GAARKEUKENSLUIS_NODE_ID = n("loc:216796,585120")
GAARKEUKENSLUIS_FLOW_RATE = 0.1
GAARKEUKENSLUIS_MAX_FLOW_RATE = 0.2
OUTLET_FLOW_RATE_AFVOER_OVERRIDE_BY_NODE_ID = {
    n("KSL011"): 400.0,
    n("KSL027"): 9999.0,
    n("KSL999"): 50.0,
    # 1748: 0.0,Checken!
    n("loc:229841,586106"): 5.0,
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
EXCLUDE_NODES = {
    n("INL499"),
    n("KSL001"),
    n("KSL014"),
    n("loc:233144,581396"),
    n("loc:239038,584076"),
    n("loc:216796,585120"),
    n("loc:252330,592176"),
    n("loc:256891,593879"),
    n("loc:229863,586135"),
    n("loc:235444,583009"),
}


def configure_gaarkeuken_control(model: Model) -> None:
    """Control Gaarkeuken on the Van Starkenborghkanaal, and give the Gaarkeukensluis its schutverlies."""
    levels, flow_rates = zip(*GAARKEUKEN_CONTROL, strict=True)
    set_continuous_level_control(
        model,
        GAARKEUKEN_OUTLET_NODE_ID,
        GAARKEUKEN_LISTEN_NODE_ID,
        level=levels,
        flow_rate=flow_rates,
        min_upstream_level=GAARKEUKEN_MIN_UPSTREAM_LEVEL,
        name="inlaat: Gaarkeuken",
    )
    assert model.outlet.static.df is not None
    sluis_mask = model.outlet.static.df["node_id"].eq(GAARKEUKENSLUIS_NODE_ID)
    if sluis_mask.sum() != 1:
        raise ValueError(f"Expected one static row for Gaarkeukensluis {GAARKEUKENSLUIS_NODE_ID}")
    model.outlet.static.df.loc[sluis_mask, ["flow_rate", "min_flow_rate", "max_flow_rate"]] = [
        GAARKEUKENSLUIS_FLOW_RATE,
        0.0,
        GAARKEUKENSLUIS_MAX_FLOW_RATE,
    ]


# %%
assert model.level_boundary.static.df is not None
model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id == n("bnd:16"), "level"] = -1.0

assert model.outlet.static.df is not None
outlet_flow_rate_afvoer_by_node_id = (
    model.outlet.static.df.dropna(subset=["flow_rate"]).set_index("node_id")["flow_rate"].astype(float).to_dict()
)
outlet_flow_rate_afvoer_by_node_id = {
    int(cast(int, node_id)): flow_rate for node_id, flow_rate in outlet_flow_rate_afvoer_by_node_id.items()
}
outlet_flow_rate_afvoer_by_node_id.update(OUTLET_FLOW_RATE_AFVOER_OVERRIDE_BY_NODE_ID)
outlet_max_flow_rate_aanvoer_by_node_id: dict[int, float] = {}
for outlet_static in model.outlet.static.df.itertuples():
    max_flow_rate = cast(float, outlet_static.max_flow_rate)
    if max_flow_rate != max_flow_rate:
        max_flow_rate = cast(float, outlet_static.flow_rate)
    outlet_max_flow_rate_aanvoer_by_node_id[int(cast(int, outlet_static.node_id))] = (
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
flushing_nodes = {n("KGM015"): 0.4, n("KST0169"): 0.2}

# handmatig opgegeven drain nodes (uitlaten) definieren
# 40: Noordpolder
# 412: Marnerwaard
# 442: Marnerwaard
# 837: Spijksterpompen
drain_nodes = [n("KGM014"), n("KST0243"), n("KST0120"), n("1331")]

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

supply_nodes = [
    n("KGM140"),
    n("KGM144"),
    n("2433"),
    n("KGM126"),
    n("KGM107"),
    n("KGM112"),
    n("KGM116"),
    n("KGM121"),
    n("KGM307"),
]

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

polygon = cast(Polygon | MultiPolygon, aanvoergebieden_df.at["Jonkersvaart", "geometry"])

# links die intersecten die we kunnen negeren
# 1810: sloot naast prallelweg (A7) die vrij afwatert op Oude Diep
ignore_intersecting_links: list[int] = [lk("up:175", "175")]

# doorspoeling (op uitlaten)
# 357: KST0159 Trambaanstuw
# 357: KST0159 Trambaanstuw
# 393: KST0135 Ackerenstuw
# 350: KST0053 Lage Hamrikstuw
# 401: KST0148 Mastuw
# 501: KST0379 Ooster Lietsstuw
# 383: KST0113 Lage Rietstuw
flushing_nodes = {
    n("KST0069"): 0.02,
    n("KST0135"): 0.012,
    n("KST0053"): 0.015,
    n("KST0148"): 0.017,
    n("KST0379"): 0.034,
    n("KST0113"): 0.013,
}

# handmatig opgegeven drain nodes (uitlaten) definieren
# 551: KST0556
# 652: KST1072
drain_nodes = [n("KST0566"), n("KST1072")]

# handmatig opgegeven supply nodes (inlaten)
# 37: Diepswal
# 38: Jonkersvaart
supply_nodes = [n("KGM010"), n("KGM011")]

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

polygon = cast(Polygon | MultiPolygon, aanvoergebieden_df.at["Peizerdiep", "geometry"])

# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = []

# doorspoeling (op uitlaten)
# node_id: Naam
flushing_nodes = {}

# Peizerdiep: geen doorspoelvraag, alleen verdeling over doorlaten.
flow_control_nodes = [n("KST0130"), n("KST0183")]
supply_flow_rate = {n("KST0130"): 0.043 * 0.75, n("KST0183"): 0.043 * 0.25}
drain_flow_rate = {n("KST0130"): 100.0, n("KST0183"): 0.0}

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
flow_control_nodes = [n("KSL027"), n("KST1027"), n("KST1030")]

# 39 Zuidhornder Zuidertocht
# 165: Harssensbosch
# 680: KST9970
# 1753: Gemaal Dorkwerd
supply_nodes = [n("KGM013"), n("KST9970"), n("KGM230"), n("loc:216814,585025"), n("loc:229841,586106")]

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


assert model.discrete_control.condition.df is not None
model.discrete_control.condition.df.loc[model.discrete_control.condition.df.time.isna(), ["time"]] = model.starttime

# Schutverliezen als vaste ondergrens; na EXCLUDE_NODES/defaultcapaciteiten zodat dit niet wordt overschreven.
for node_id, flow_rate in SCHUTVERLIES_FLOW_RATE_BY_NODE_ID.items():
    for static_df in (model.outlet.static.df, model.pump.static.df):
        assert static_df is not None
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

# Gaarkeuken apart overrulen met ContinuousControl.
configure_gaarkeuken_control(model)

# %% Junctionify(!)
junctionify(model)

# write the dry and wet steady state scenarios and the full control model
model = write_steady_state_scenarios(
    model,
    AUTHORITY,
    SHORT_NAME,
    dry_evaporation_mm_per_day=0.1,
    model_exec=MODEL_EXEC,
    qlr_path=qlr_path,
)
