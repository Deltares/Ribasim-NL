# %%

import geopandas as gpd
import pandas as pd
from ribasim import Node
from ribasim.nodes import discrete_control, pump
from shapely.geometry import Point

from peilbeheerst_model import ribasim_parametrization
from peilbeheerst_model.controle_output import Control
from ribasim_nl import CloudStorage, Model, check_basin_level
from ribasim_nl.from_to_nodes_and_levels import add_from_to_nodes_and_levels
from ribasim_nl.parametrization.basin_tables import update_basin_static

# execute model run
MODEL_EXEC: bool = True

# model settings
AUTHORITY: str = "StichtseRijnlanden"
SHORT_NAME: str = "hdsr"
MODEL_ID: str = "2025_7_0"

# connect with the GoodCloud
cloud = CloudStorage()

# collect relevant data from the GoodCloud
ribasim_model_dir = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_parameterized_model")
ribasim_toml = ribasim_model_dir / f"{SHORT_NAME}.toml"
qlr_path = cloud.joinpath("Basisgegevens", "QGIS_lyr", "output_controle_vaw_aanvoer.qlr")
aanvoer_path = cloud.joinpath(AUTHORITY, "verwerkt", "4_ribasim", "peilgebieden_bewerkt.gpkg")
model_edits_extra_gpkg = cloud.joinpath(AUTHORITY, "verwerkt", "model_edits_aanvoer.gpkg")

pump_hoofdwater_gpkg = cloud.joinpath(AUTHORITY, "verwerkt", "pomp_bij_hoofdwater.gpkg")


cloud.synchronize(
    filepaths=[
        aanvoer_path,
    ]
)

# %%
# read model
model = Model.read(ribasim_toml)
aanvoergebieden_df = gpd.read_file(aanvoer_path)
aanvoergebieden_df_dissolved = aanvoergebieden_df.dissolve()
original_model = model.model_copy(deep=True)

# %%
update_basin_static(model=model, precipitation_mm_per_day=1)

# %%
# alle niet-gecontrolleerde basins krijgen een meta_streefpeil uit de final state van de parameterize_model.py
update_levels = model.basin_outstate.df.set_index("node_id")["level"]
basin_ids = model.basin.node.df[model.basin.node.df["meta_gestuwd"] == "False"].index
mask = model.basin.area.df["node_id"].isin(basin_ids)
model.basin.area.df.loc[mask, "meta_streefpeil"] = model.basin.area.df[mask]["node_id"].apply(
    lambda x: update_levels[x]
)
# model basin area
model.basin.area.df["meta_streefpeil"] = model.basin.area.df["meta_streefpeil"] + 0.02
model.outlet.static.df["min_upstream_level"] = model.outlet.static.df["min_upstream_level"] + 0.02
model.pump.static.df["min_upstream_level"] = model.pump.static.df["min_upstream_level"] + 0.02


# %%
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
model.outlet.static.df.flow_rate = 50
model.pump.static.df.flow_rate = 50
# model.outlet.static.df.max_flow_rate = original_model.outlet.static.df.max_flow_rate
model.outlet.static.df.max_flow_rate = 50
model.pump.static.df.max_flow_rate = 50
# model.pump.static.df.max_flow_rate = original_model.pump.static.df.max_flow_rate
model.basin.area.df["meta_streefpeil"] = model.basin.area.df["meta_streefpeil"] - 0.02

# %% Alle inlaten op max debiet gezet.
node_ids = model.outlet.node.df[model.outlet.node.df.meta_code_waterbeheerder.str.startswith("I")].index.to_numpy()
model.outlet.static.df.loc[model.outlet.static.df.node_id.isin(node_ids), "max_flow_rate"] = 0.1
# %%

model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id == 45, "level"] = -1
model.pump.static.df.loc[model.pump.static.df.node_id == 541, "max_downstream_level"] = -1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1162, "max_flow_rate"] = 0.2
model.outlet.static.df.loc[model.outlet.static.df.node_id == 514, "max_flow_rate"] = 0.1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1082, "max_flow_rate"] = 0.1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1081, "max_flow_rate"] = 0.1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 368, "max_flow_rate"] = 0.1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 947, "max_flow_rate"] = 0.1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 911, "max_flow_rate"] = 0.1


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

# === 2b. Verhoog min_upstream_level met offset voor downstream Outlets
mask = model.outlet.static.df["node_id"].isin(downstream_outlet_nodes)
model.outlet.static.df.loc[mask, "min_upstream_level"] = model.outlet.static.df.loc[mask, "min_upstream_level"] + 0.02
mask = model.pump.static.df["node_id"].isin(downstream_pump_nodes)
model.pump.static.df.loc[mask, "min_upstream_level"] = model.pump.static.df.loc[mask, "min_upstream_level"] + 0.02

# model.pump.static.df["min_upstream_level"] = model.pump.static.df["min_upstream_level"] + 0.02
model.level_boundary.static.df["level"] = model.level_boundary.static.df["level"] + 0.02
# model.pump.static.df["max_downstream_level"]+ = 0.02

# pompen aan hoofdwater geen downstream level
pump_hfw_gpkg = gpd.read_file(pump_hoofdwater_gpkg)
pump_nodes = pump_hfw_gpkg.node_id
mask = model.pump.static.df["node_id"].isin(pump_nodes)
model.pump.static.df.loc[mask, "max_downstream_level"] = pd.NA
model.pump.static.df.loc[mask, "min_upstream_level"] -= 0.0

# === 4. Zet max/min levels op NA voor niet-gestuwde, niet-verbonden outlets ===
non_control_nodes = model.outlet.node.df.query("meta_gestuwd == 'False'").index
excluded_nodes = set(upstream_outlet_nodes) | set(downstream_outlet_nodes)

# %%
model.discrete_control.add(
    Node(2113, Point(114000, 446400)),
    [
        discrete_control.Variable(
            compound_variable_id=1,
            listen_node_id=2090,
            variable=["level"],
        ),
        discrete_control.Condition(
            compound_variable_id=1,
            condition_id=[1, 2],
            # min, max
            threshold_high=[-2.21, -1.9],
        ),
        discrete_control.Logic(
            truth_state=["FF", "TF", "TT"],
            control_state=["in", "none", "out"],
        ),
    ],
)

model.pump.add(
    Node(2114, Point(113950, 446370)),
    [pump.Static(control_state=["none", "in", "out"], flow_rate=[0.0, 1e-3, 20])],
)
model.link.add(model.basin[2090], model.pump[2114])
model.link.add(model.pump[2114], model.basin[1920])
model.link.add(model.discrete_control[2113], model.pump[2114])


# %%
model.pump.static.df.flow_rate = 20


# %% Pompen halen downstream_level niet in Ribasim.
# Daarom moet outlets min_upstream_level hier iets omlaag. Behalve outlets die in zelfde basins liggen als pumps anders krijg je rondpompen
# 1. Haal alle pomp-node IDs op
pump_ids = model.pump.node.df.index

# 2. Downstream basin nodes van pompen (één stap)
downstream_basin_nodes_pump = pd.Series([model.downstream_node_id(i) for i in pump_ids]).explode().dropna().unique()

# 3. Upstream basin nodes van pompen (voor de filter later)
upstream_basin_nodes_pump = pd.Series([model.upstream_node_id(i) for i in pump_ids]).explode().dropna().unique()

# 4. Eén extra stap downstream vanaf de basin nodes van de pomp
step2_nodes = pd.Series([model.downstream_node_id(i) for i in downstream_basin_nodes_pump]).explode().dropna().unique()

# 5. Alleen de nodes die daadwerkelijk outlets zijn
outlet_ids = model.outlet.node.df.index
outlet_nodes_downstream = [nid for nid in step2_nodes if nid in outlet_ids]

# 6. Bepaal downstream basin node per outlet
outlet_to_downstream_basin = {outlet: model.downstream_node_id(outlet) for outlet in outlet_nodes_downstream}

# 7. Filter: verwijder outlets waarvan downstream basin node in upstream_basin_nodes_pump zit
upstream_basin_set = set(upstream_basin_nodes_pump)

filtered_outlet_nodes = [
    outlet for outlet, basin in outlet_to_downstream_basin.items() if basin not in upstream_basin_set
]
model.outlet.static.df.loc[model.outlet.static.df.node_id.isin(filtered_outlet_nodes), "min_upstream_level"] -= 0.002


# Outlets aan hoofdwaterlopen mogen niet omlaag! Nog script maken
model.outlet.static.df.loc[model.outlet.static.df.node_id == 448, "min_upstream_level"] += 0.001
model.outlet.static.df.loc[model.outlet.static.df.node_id == 509, "min_upstream_level"] += 0.001
model.outlet.static.df.loc[model.outlet.static.df.node_id == 288, "min_upstream_level"] += 0.001
model.outlet.static.df.loc[model.outlet.static.df.node_id == 178, "min_upstream_level"] = -0.48
model.outlet.static.df.loc[model.outlet.static.df.node_id == 356, "min_upstream_level"] = -0.48
model.outlet.static.df.loc[model.outlet.static.df.node_id == 224, "min_upstream_level"] = -0.48
model.outlet.static.df.loc[model.outlet.static.df.node_id == 214, "min_upstream_level"] += 0.001
model.outlet.static.df.loc[model.outlet.static.df.node_id == 154, "min_upstream_level"] += 0.001
model.outlet.static.df.loc[model.outlet.static.df.node_id == 410, "min_upstream_level"] += 0.001
model.outlet.static.df.loc[model.outlet.static.df.node_id == 479, "min_upstream_level"] += 0.001
model.outlet.static.df.loc[model.outlet.static.df.node_id == 489, "min_upstream_level"] += 0.001
model.outlet.static.df.loc[model.outlet.static.df.node_id == 488, "min_upstream_level"] += 0.001
model.outlet.static.df.loc[model.outlet.static.df.node_id == 492, "min_upstream_level"] += 0.001
model.outlet.static.df.loc[model.outlet.static.df.node_id == 493, "min_upstream_level"] += 0.001
model.outlet.static.df.loc[model.outlet.static.df.node_id == 194, "min_upstream_level"] += 0.001
model.outlet.static.df.loc[model.outlet.static.df.node_id == 370, "min_upstream_level"] += 0.001
model.outlet.static.df.loc[model.outlet.static.df.node_id == 372, "min_upstream_level"] += 0.001
model.outlet.static.df.loc[model.outlet.static.df.node_id == 751, "min_upstream_level"] += 0.001
model.outlet.static.df.loc[model.outlet.static.df.node_id == 753, "min_upstream_level"] += 0.001
model.outlet.static.df.loc[model.outlet.static.df.node_id == 742, "min_upstream_level"] += 0.001
model.outlet.static.df.loc[model.outlet.static.df.node_id == 79, "min_upstream_level"] += 0.001

# %% Add inlaat
model.pump.static.df.loc[model.pump.static.df.node_id == 1138, "min_upstream_level"] = 0.55
model.outlet.static.df.loc[model.outlet.static.df.node_id == 2106, "min_upstream_level"] = 0.55
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1172, "min_upstream_level"] = 0.55
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1171, "min_upstream_level"] = 0.55
# Pompen min_upstream
model.pump.static.df.loc[model.pump.static.df.node_id == 534, "min_upstream_level"] = -1.75
model.pump.static.df.loc[model.pump.static.df.node_id == 640, "min_upstream_level"] = -1.56

model.pump.static.df.loc[model.pump.static.df.node_id == 561, "min_upstream_level"] = 1.32
model.pump.static.df.loc[model.pump.static.df.node_id == 653, "min_upstream_level"] = 0.55
model.pump.static.df.loc[model.pump.static.df.node_id == 532, "min_upstream_level"] = 0.55
model.pump.static.df.loc[model.pump.static.df.node_id == 633, "min_upstream_level"] = 0.55
model.pump.static.df.loc[model.pump.static.df.node_id == 603, "min_upstream_level"] = 0.55
model.pump.static.df.loc[model.pump.static.df.node_id == 605, "min_upstream_level"] = 0.55
model.pump.static.df.loc[model.pump.static.df.node_id == 532, "max_downstream_level"] = 1.55
model.pump.static.df.loc[model.pump.static.df.node_id == 557, "max_downstream_level"] = 0.54
model.pump.static.df.loc[model.pump.static.df.node_id == 531, "min_upstream_level"] = 0
model.pump.static.df.loc[model.pump.static.df.node_id == 545, "min_upstream_level"] = 0
model.pump.static.df.loc[model.pump.static.df.node_id == 563, "min_upstream_level"] = -0.85
model.pump.static.df.loc[model.pump.static.df.node_id == 563, "max_downstream_level"] = 0.52
model.pump.static.df.loc[model.pump.static.df.node_id == 608, "max_downstream_level"] = 2.71


# Afvoerpompen naar hoofsysteem hebben geen downstream level
model.pump.static.df.loc[model.pump.static.df.node_id == 562, "min_upstream_level"] = 1.32

# Afvoer pomp
model.pump.static.df.loc[model.pump.static.df.node_id == 572, "max_downstream_level"] = pd.NA
model.pump.static.df.loc[model.pump.static.df.node_id == 578, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 529, "min_upstream_level"] = 0.46
model.pump.static.df.loc[model.pump.static.df.node_id == 1373, "min_upstream_level"] = 0.46
model.outlet.static.df.loc[model.outlet.static.df.node_id == 105, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 155, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 196, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 181, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 196, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 291, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 213, "max_downstream_level"] = -0.68
model.outlet.static.df.loc[model.outlet.static.df.node_id == 326, "max_downstream_level"] = -2.20
model.outlet.static.df.loc[model.outlet.static.df.node_id == 325, "max_downstream_level"] = -2.20
model.outlet.static.df.loc[model.outlet.static.df.node_id == 324, "max_downstream_level"] = -2.20
model.outlet.static.df.loc[model.outlet.static.df.node_id == 323, "max_downstream_level"] = -2.20
model.outlet.static.df.loc[model.outlet.static.df.node_id == 322, "max_downstream_level"] = -2.20
model.outlet.static.df.loc[model.outlet.static.df.node_id == 321, "max_downstream_level"] = -2.20
model.outlet.static.df.loc[model.outlet.static.df.node_id == 320, "max_downstream_level"] = -2.20
model.outlet.static.df.loc[model.outlet.static.df.node_id == 316, "max_downstream_level"] = -2.20
model.outlet.static.df.loc[model.outlet.static.df.node_id == 317, "min_upstream_level"] = -2.2
model.outlet.static.df.loc[model.outlet.static.df.node_id == 318, "max_downstream_level"] = -2.20
model.outlet.static.df.loc[model.outlet.static.df.node_id == 319, "max_downstream_level"] = -2.20
model.outlet.static.df.loc[model.outlet.static.df.node_id == 440, "max_downstream_level"] = -2.20
model.outlet.static.df.loc[model.outlet.static.df.node_id == 441, "max_downstream_level"] = -2.20
model.outlet.static.df.loc[model.outlet.static.df.node_id == 197, "max_downstream_level"] = -2.20
model.outlet.static.df.loc[model.outlet.static.df.node_id == 346, "max_downstream_level"] = -2.20
model.outlet.static.df.loc[model.outlet.static.df.node_id == 384, "max_downstream_level"] = -2.20
model.outlet.static.df.loc[model.outlet.static.df.node_id == 385, "max_downstream_level"] = -2.20
model.outlet.static.df.loc[model.outlet.static.df.node_id == 386, "max_downstream_level"] = -2.20
model.outlet.static.df.loc[model.outlet.static.df.node_id == 411, "max_downstream_level"] = -2.20
model.outlet.static.df.loc[model.outlet.static.df.node_id == 821, "max_downstream_level"] = -2.20
model.outlet.static.df.loc[model.outlet.static.df.node_id == 806, "max_downstream_level"] = -2.2
model.outlet.static.df.loc[model.outlet.static.df.node_id == 889, "max_downstream_level"] = -2.20
model.outlet.static.df.loc[model.outlet.static.df.node_id == 762, "max_downstream_level"] = -2.20
model.outlet.static.df.loc[model.outlet.static.df.node_id == 921, "max_downstream_level"] = -2.20
model.outlet.static.df.loc[model.outlet.static.df.node_id == 922, "max_downstream_level"] = -2.20
model.outlet.static.df.loc[model.outlet.static.df.node_id == 955, "max_downstream_level"] = -2.20
model.outlet.static.df.loc[model.outlet.static.df.node_id == 962, "max_downstream_level"] = -2.2
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1052, "max_downstream_level"] = -2.20
model.outlet.static.df.loc[model.outlet.static.df.node_id == 340, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 339, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 341, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 342, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 343, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 344, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 405, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 405, "max_downstream_level"] = 0.50
model.outlet.static.df.loc[model.outlet.static.df.node_id == 746, "min_upstream_level"] = 0.5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 409, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 422, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 423, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 424, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 809, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 249, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 265, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 266, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 333, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 557, "max_downstream_level"] = 1.55
model.outlet.static.df.loc[model.outlet.static.df.node_id == 959, "max_downstream_level"] = -1.08
model.outlet.static.df.loc[model.outlet.static.df.node_id == 408, "max_downstream_level"] = -1.08
model.outlet.static.df.loc[model.outlet.static.df.node_id == 315, "max_downstream_level"] = -1.08
model.outlet.static.df.loc[model.outlet.static.df.node_id == 983, "max_downstream_level"] = -1.08
model.outlet.static.df.loc[model.outlet.static.df.node_id == 164, "max_downstream_level"] = -1.08
model.outlet.static.df.loc[model.outlet.static.df.node_id == 210, "max_downstream_level"] = -1.08
model.outlet.static.df.loc[model.outlet.static.df.node_id == 930, "min_upstream_level"] = -1.08

# De Aanvoerder
model.pump.static.df.loc[model.pump.static.df.node_id == 542, "max_downstream_level"] = 0.01
model.outlet.static.df.loc[model.outlet.static.df.node_id == 836, "min_upstream_level"] = 0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 107, "min_upstream_level"] = 0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 258, "min_upstream_level"] = 0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 743, "min_upstream_level"] = 0.01
model.outlet.static.df.loc[model.outlet.static.df.node_id == 2111, "max_downstream_level"] = -2.18
model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id == 2108, "level"] = -2
model.basin.area.df.loc[model.basin.area.df.node_id == 1698, "meta_streefpeil"] = 0
model.basin.area.df.loc[model.basin.area.df.node_id == 1474, "meta_streefpeil"] = -0.48
model.basin.area.df.loc[model.basin.area.df.node_id == 1492, "meta_streefpeil"] = 0.52
model.basin.area.df.loc[model.basin.area.df.node_id == 1396, "meta_streefpeil"] = 0.52
model.basin.area.df.loc[model.basin.area.df.node_id == 1562, "meta_streefpeil"] = 0.52
model.basin.area.df.loc[model.basin.area.df.node_id == 1387, "meta_streefpeil"] = -2.22
model.basin.area.df.loc[model.basin.area.df.node_id == 1516, "meta_streefpeil"] = -2.22
model.basin.area.df.loc[model.basin.area.df.node_id == 1376, "meta_streefpeil"] = -2.22
model.basin.area.df.loc[model.basin.area.df.node_id == 1380, "meta_streefpeil"] = -2.22
model.basin.area.df.loc[model.basin.area.df.node_id == 1572, "meta_streefpeil"] = -2.22

# Veel waterlopen krijgen geen downstream_level. Gevaar op rondpompen!
model.outlet.static.df.loc[model.outlet.static.df.node_id == 509, "max_downstream_level"] = -1.83
model.outlet.static.df.loc[model.outlet.static.df.node_id == 837, "max_downstream_level"] = -2.56
model.outlet.static.df.loc[model.outlet.static.df.node_id == 808, "max_downstream_level"] = -2.56
model.outlet.static.df.loc[model.outlet.static.df.node_id == 900, "max_downstream_level"] = -2.56
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1013, "max_downstream_level"] = -2.56
model.outlet.static.df.loc[model.outlet.static.df.node_id == 103, "max_downstream_level"] = -2.56
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1178, "min_upstream_level"] = -2.4
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1178, "max_downstream_level"] = -2.56
model.pump.static.df.loc[model.pump.static.df.node_id == 543, "min_upstream_level"] = -2.58
model.outlet.static.df.loc[model.outlet.static.df.node_id == 982, "max_downstream_level"] = -2.56
model.outlet.static.df.loc[model.outlet.static.df.node_id == 444, "max_downstream_level"] = -2.56
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1025, "max_downstream_level"] = -1.88

# Lekken daardoor rondpompen
model.outlet.static.df.loc[model.outlet.static.df.node_id == 210, "max_downstream_level"] = -1.75
model.outlet.static.df.loc[model.outlet.static.df.node_id == 281, "max_downstream_level"] = -1.75
model.outlet.static.df.loc[model.outlet.static.df.node_id == 282, "max_downstream_level"] = -1.75
model.outlet.static.df.loc[model.outlet.static.df.node_id == 283, "max_downstream_level"] = -1.75
model.outlet.static.df.loc[model.outlet.static.df.node_id == 429, "max_downstream_level"] = -1.75
model.outlet.static.df.loc[model.outlet.static.df.node_id == 430, "max_downstream_level"] = -1.75
model.outlet.static.df.loc[model.outlet.static.df.node_id == 431, "max_downstream_level"] = -1.75
model.outlet.static.df.loc[model.outlet.static.df.node_id == 432, "max_downstream_level"] = -1.75
model.outlet.static.df.loc[model.outlet.static.df.node_id == 435, "max_downstream_level"] = -1.75
model.outlet.static.df.loc[model.outlet.static.df.node_id == 451, "max_downstream_level"] = -1.75
model.outlet.static.df.loc[model.outlet.static.df.node_id == 260, "max_downstream_level"] = -1.75
model.outlet.static.df.loc[model.outlet.static.df.node_id == 286, "max_downstream_level"] = -1.75
model.outlet.static.df.loc[model.outlet.static.df.node_id == 287, "max_downstream_level"] = -1.75
model.outlet.static.df.loc[model.outlet.static.df.node_id == 453, "max_downstream_level"] = -1.75
model.outlet.static.df.loc[model.outlet.static.df.node_id == 452, "max_downstream_level"] = -1.75
model.outlet.static.df.loc[model.outlet.static.df.node_id == 436, "max_downstream_level"] = -1.75
model.outlet.static.df.loc[model.outlet.static.df.node_id == 437, "max_downstream_level"] = -1.75
model.outlet.static.df.loc[model.outlet.static.df.node_id == 438, "max_downstream_level"] = -1.75
model.outlet.static.df.loc[model.outlet.static.df.node_id == 439, "max_downstream_level"] = -1.75
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1088, "max_downstream_level"] = -1.75
model.outlet.static.df.loc[model.outlet.static.df.node_id == 261, "max_downstream_level"] = -1.75
model.outlet.static.df.loc[model.outlet.static.df.node_id == 920, "max_downstream_level"] = -1.75
model.outlet.static.df.loc[model.outlet.static.df.node_id == 779, "max_downstream_level"] = -1.75
model.outlet.static.df.loc[model.outlet.static.df.node_id == 818, "max_downstream_level"] = -1.75
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1067, "max_downstream_level"] = -1.53
model.outlet.static.df.loc[model.outlet.static.df.node_id == 247, "max_downstream_level"] = -1.53
model.outlet.static.df.loc[model.outlet.static.df.node_id == 246, "max_downstream_level"] = -1.53
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1156, "max_downstream_level"] = -1.53
model.outlet.static.df.loc[model.outlet.static.df.node_id == 420, "min_upstream_level"] = -1.55
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1005, "min_upstream_level"] = -1.55
model.outlet.static.df.loc[model.outlet.static.df.node_id == 487, "max_downstream_level"] = -1.53
model.outlet.static.df.loc[model.outlet.static.df.node_id == 678, "min_upstream_level"] = -1.55
model.outlet.static.df.loc[model.outlet.static.df.node_id == 678, "max_downstream_level"] = -1.75
model.outlet.static.df.loc[model.outlet.static.df.node_id == 887, "max_downstream_level"] = -2.2
model.outlet.static.df.loc[model.outlet.static.df.node_id == 821, "max_downstream_level"] = -2.2
model.outlet.static.df.loc[model.outlet.static.df.node_id == 821, "max_flow_rate"] = 0.1
# Keulevaart
model.pump.static.df.loc[model.pump.static.df.node_id == 623, "min_upstream_level"] = -2.20
model.remove_node(node_id=623, remove_edges=True)
# %%
model.outlet.static.df.loc[model.outlet.static.df.node_id == 835, "max_downstream_level"] = 0.57
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1052, "max_downstream_level"] = 0.57
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1052, "max_downstream_level"] = 0.57
model.outlet.static.df.loc[model.outlet.static.df.node_id == 838, "max_downstream_level"] = 0.57
model.outlet.static.df.loc[model.outlet.static.df.node_id == 949, "max_downstream_level"] = 0.57
model.outlet.static.df.loc[model.outlet.static.df.node_id == 749, "max_downstream_level"] = 0.57
model.outlet.static.df.loc[model.outlet.static.df.node_id == 919, "max_downstream_level"] = 0.57
model.outlet.static.df.loc[model.outlet.static.df.node_id == 193, "max_downstream_level"] = 0.57
model.outlet.static.df.loc[model.outlet.static.df.node_id == 864, "max_downstream_level"] = 0.57
model.outlet.static.df.loc[model.outlet.static.df.node_id == 151, "max_downstream_level"] = 0.57
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1145, "max_downstream_level"] = 0.57
model.outlet.static.df.loc[model.outlet.static.df.node_id == 929, "max_downstream_level"] = 0.57
model.outlet.static.df.loc[model.outlet.static.df.node_id == 866, "max_downstream_level"] = 0.57
model.pump.static.df.loc[model.pump.static.df.node_id == 561, "max_downstream_level"] = 1.27
model.pump.static.df.loc[model.pump.static.df.node_id == 792, "max_downstream_level"] = 0.57
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1194, "min_upstream_level"] = -1.22
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1279, "min_upstream_level"] = pd.NA


model.update_node(node_id=730, node_type="Outlet")
model.outlet.static.df.loc[model.outlet.static.df.node_id == 730, "min_upstream_level"] = -1

# %%
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
