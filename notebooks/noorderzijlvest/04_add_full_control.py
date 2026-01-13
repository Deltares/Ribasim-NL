# %%

import geopandas as gpd
import pandas as pd
from peilbeheerst_model.controle_output import Control
from ribasim.nodes import pid_control
from ribasim_nl.case_conversions import pascal_to_snake_case
from ribasim_nl.control import add_controllers_to_supply_area, add_controllers_to_uncontrolled_connector_nodes
from ribasim_nl.parametrization.basin_tables import update_basin_static

from ribasim_nl import CloudStorage, Model

# %%
# Globale settings

LEVEL_DIFFERENCE_THRESHOLD = 0.02  # sync model.solver.level_difference_threshold and control-settings
MODEL_EXEC: bool = True  # execute model run
AUTHORITY: str = "Noorderzijlvest"  # authority
SHORT_NAME: str = "nzv"  # short_name used in toml-file
CONTROL_NODE_TYPES = ["Outlet", "Pump"]
IS_SUPPLY_NODE_COLUMN: str = "meta_supply_node"

# Sluizen die geen rol hebben in de waterverdeling (aanvoer/afvoer), maar wel in het model zitten
# 722: KSL001 Waterkering Westerwijtwerdermaar
# 716: INL499 (opgeheven)
# 725: Beijumersluis
# 1738: Westerhavensluis
# 1740: Jan B. Bronssluis
# 1744: Gaarkeukensluis
# 1745: Groevesluis Noord
# 1746: Roggenkampsluis
# 1747: Sluis bj Munnekezijlsterried (inlaat?)
# 1756: Oostersluis
# 1752: Dokwerdersluis
EXCLUDE_NODES = {716, 722, 725, 1738, 1740, 1744, 1745, 1746, 1747, 1752, 1756}

# %%
# Definieren paden en syncen met cloud
cloud = CloudStorage()
ribasim_model_dir = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_parameterized_model")
ribasim_toml = ribasim_model_dir / f"{SHORT_NAME}.toml"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_aanvoer.qlr")
aanvoer_path = cloud.joinpath(
    AUTHORITY, "verwerkt/1_ontvangen_data//20250527/gebieden_met_wateraanvoermogelijkheid_noorderzijlvest.gpkg"
)
aanvoergebieden_gpkg = cloud.joinpath(r"Noorderzijlvest/verwerkt/aanvoergebieden.gpkg")
cloud.synchronize(filepaths=[aanvoer_path, qlr_path])


# %%
# Read data
model = Model.read(ribasim_toml)

aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")

# %%
# Identificeren aanvoerknopen en voorzien van afvoercapaciteit

# aanmaken node_df en specificeren supply_nodes
# knopen die beginnen met INL, i of eindigen op i, maar niet op fictief
for node_type in CONTROL_NODE_TYPES:
    # set is supply
    node_df = getattr(model, pascal_to_snake_case(node_type)).node.df
    node_df[IS_SUPPLY_NODE_COLUMN] = (
        node_df["meta_code_waterbeheerder"].str.startswith("INL")
        | node_df["meta_code_waterbeheerder"].str.startswith("i")
        | node_df["meta_code_waterbeheerder"].str.endswith("i")
    ) & ~(node_df.node_type.isin(CONTROL_NODE_TYPES) & node_df["meta_code_waterbeheerder"].str.endswith("fictief"))
    getattr(model, pascal_to_snake_case(node_type)).node.df = node_df

    # force nan or 0 to 20 m3/s
    node_ids = node_df[node_df[IS_SUPPLY_NODE_COLUMN]].index.values
    static_df = getattr(model, pascal_to_snake_case(node_type)).static.df
    mask = static_df.node_id.isin(node_ids) & (static_df.flow_rate == 0 | static_df.flow_rate.isna())
    static_df.loc[mask, "flow_rate"] = 20

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
#     level_difference_threshold=LEVEL_DIFFERENCE_THRESHOLD,
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
#     ignore_intersecting_links=ignore_intersecting_links,
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
#     level_difference_threshold=LEVEL_DIFFERENCE_THRESHOLD,
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
drain_nodes = [40, 442, 412, 837]

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
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    level_difference_threshold=LEVEL_DIFFERENCE_THRESHOLD,
    control_node_types=CONTROL_NODE_TYPES,
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
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    level_difference_threshold=LEVEL_DIFFERENCE_THRESHOLD,
    control_node_types=CONTROL_NODE_TYPES,
)

# %%
# Toevoegen Peizerdiep

polygon = aanvoergebieden_df.at["Peizerdiep", "geometry"]

# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = []

# doorspoeling (op uitlaten)
# node_id: Naam
flushing_nodes = {}

# handmatig opgegeven drain nodes (uitlaten) definieren
# node_id: Naam
drain_nodes = []

# handmatig opgegeven supply nodes (inlaten)
supply_nodes = []

# toevoegen sturing
node_functions_df = add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    level_difference_threshold=LEVEL_DIFFERENCE_THRESHOLD,
    control_node_types=CONTROL_NODE_TYPES,
)

# %%
# EXCLUDE NODES op 0 m3/s zetten
mask = model.outlet.static.df.node_id.isin(EXCLUDE_NODES)
model.outlet.static.df.loc[mask, "flow_rate"] = 0
model.outlet.static.df.loc[mask, "min_flow_rate"] = 0
model.outlet.static.df.loc[mask, "max_flow_rate"] = 0

# %%
# Gaarkeuken: PID control
years = [2017, 2020]
gaarkeuken_listen_node = 1186


def make_year_times(y):
    return [
        pd.Timestamp(y, 3, 31),  # net vóór actieve periode -> P=0
        pd.Timestamp(y, 4, 1),  # start actieve periode
        pd.Timestamp(y, 9, 30),  # einde actieve periode
        pd.Timestamp(y, 10, 1),  # na actieve periode -> P=0
    ]


pid_times_all = []
for y in sorted(years):
    pid_times_all.extend(make_year_times(y))

# Als model.starttime eerder is dan de eerste tijd in pid_times_all, voeg een start-marker toe
sim_start = pd.Timestamp(model.starttime)
if sim_start < min(pid_times_all):
    # alleen toevoegen als het nog niet exact bestaat
    if sim_start != pid_times_all[0]:
        pid_times_all.insert(0, sim_start)
pid_times_all = sorted(dict.fromkeys(pid_times_all))
pattern_proportional = [1e6, 1e6, 1e6, 1e6]
pattern_integral = [0.0, 0.0, 0.0, 0.0]
pattern_derivative = [0.0, 0.0, 0.0, 0.0]
pattern_target = [-0.93, -0.93, -0.93, -0.93]

proportional = []
integral = []
derivative = []
targets = []
listen_nodes = []

# helper: maak mapping van year -> its 4 times for matching
year_times_map = {y: make_year_times(y) for y in years}

for t in pid_times_all:
    assigned = False
    for y, times in year_times_map.items():
        if t in times:
            idx = times.index(t)  # 0..3
            proportional.append(pattern_proportional[idx])
            integral.append(pattern_integral[idx])
            derivative.append(pattern_derivative[idx])
            targets.append(pattern_target[idx])
            listen_nodes.append(gaarkeuken_listen_node)
            assigned = True
            break
    if not assigned:
        proportional.append(0.0)
        integral.append(0.0)
        derivative.append(0.0)
        targets.append(-0.93)
        listen_nodes.append(gaarkeuken_listen_node)

model.add_control_node(
    to_node_id=1741,
    data=[
        pid_control.Time(
            time=pid_times_all,
            listen_node_id=listen_nodes,
            target=targets,
            proportional=proportional,
            integral=integral,
            derivative=derivative,
        )
    ],
    ctrl_type="PidControl",
    node_offset=15,
)

# %% add all remaining inlets/outlets


# 29: De Waterwolf
# 640: KST1027
# 641: KST1030
flow_control_nodes = [29, 640, 641]

# 39 Zuidhornder Zuidertocht
# 165: Harssensbosch
# 680: KST9970
# 1753: Gemaal Dorkwerd
supply_nodes = [39, 680, 165, 1753]

add_controllers_to_uncontrolled_connector_nodes(
    model=model,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    exclude_nodes=list(EXCLUDE_NODES),
    us_threshold_offset=LEVEL_DIFFERENCE_THRESHOLD,
)

# %%
# Model run

ribasim_toml_wet = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_wet", f"{SHORT_NAME}.toml")
ribasim_toml_dry = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_dry", f"{SHORT_NAME}.toml")
ribasim_toml = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model", f"{SHORT_NAME}.toml")
model.solver.level_difference_threshold = LEVEL_DIFFERENCE_THRESHOLD

model.discrete_control.condition.df.loc[model.discrete_control.condition.df.time.isna(), ["time"]] = model.starttime
model.basin.area.df["meta_aanvoer"] = True
model.outlet.static.df["meta_aanvoer"] = 1
model.pump.static.df["meta_func_aanvoer"] = 1
model.pump.static.df["meta_func_afvoer"] = 1
model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id == 16, "level"] = -1

# hoofd run met verdamping
update_basin_static(model=model, evaporation_mm_per_day=0.1)
model.write(ribasim_toml_dry)

# run hoofdmodel
if MODEL_EXEC:
    result = model.run()
    controle_output = Control(ribasim_toml=ribasim_toml_dry, qlr_path=qlr_path)
    indicators = controle_output.run_all()
    model = Model.read(ribasim_toml_dry)

# prerun om het model te initialiseren met neerslag
update_basin_static(model=model, precipitation_mm_per_day=2)
model.write(ribasim_toml_wet)

# run prerun model
if MODEL_EXEC:
    prerun_result = model.run()
    controle_output = Control(ribasim_toml=ribasim_toml_wet, qlr_path=qlr_path)
    indicators = controle_output.run_all()
    model = Model.read(ribasim_toml_wet)

# hoofd run
update_basin_static(model=model, precipitation_mm_per_day=1.5)
model.write(ribasim_toml)
# run hoofdmodel
if MODEL_EXEC:
    result = model.run()
    controle_output = Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path)
    indicators = controle_output.run_all()
