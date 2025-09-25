# %%

import geopandas as gpd
import pandas as pd
from ribasim import Node
from ribasim.nodes import discrete_control, outlet
from shapely.geometry import Point

from peilbeheerst_model import ribasim_parametrization
from peilbeheerst_model.controle_output import Control
from ribasim_nl import CloudStorage, Model, check_basin_level
from ribasim_nl.from_to_nodes_and_levels import add_from_to_nodes_and_levels
from ribasim_nl.parametrization.basin_tables import update_basin_static

# execute model run
MODEL_EXEC: bool = True

# model settings
AUTHORITY: str = "Noorderzijlvest"
SHORT_NAME: str = "nzv"
MODEL_ID: str = "2025_7_0"

# connect with the GoodCloud
cloud = CloudStorage()

# collect relevant data from the GoodCloud
ribasim_model_dir = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_parameterized_model")
ribasim_toml = ribasim_model_dir / f"{SHORT_NAME}.toml"
qlr_path = cloud.joinpath("Basisgegevens", "QGIS_lyr", "output_controle_vaw_aanvoer.qlr")
aanvoer_path = cloud.joinpath(
    AUTHORITY,
    "verwerkt",
    "1_ontvangen_data",
    "",
    "20250527",
    "gebieden_met_wateraanvoermogelijkheid_noorderzijlvest.gpkg",
)

cloud.synchronize(
    filepaths=[
        aanvoer_path,
    ]
)

# read model
model = Model.read(ribasim_toml)
original_model = model.model_copy(deep=True)
update_basin_static(model=model, precipitation_mm_per_day=1)

# %%
add_from_to_nodes_and_levels(model)

aanvoergebieden_df = gpd.read_file(aanvoer_path)
aanvoergebieden_df_dissolved = aanvoergebieden_df.dissolve()

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


# %% bovenstroomse outlets op 10m3/s zetten en boundary afvoer pumps/outlets
# geen downstreamm level en aanvoer  pumps/outlets geen upstream level
def set_values_where(df, updates, node_ids=None, key_col="node_id", mask=None):
    if mask is None:
        mask = df[key_col].isin(node_ids)
    sub = df.loc[mask]
    for col, val in updates.items():
        df.loc[mask, col] = val(sub) if callable(val) else val
    return int(mask.sum())


# === 1. Bepaal upstream/downstream connection nodes ===
upstream_outlet_nodes = model.upstream_connection_node_ids(node_type="Outlet")
downstream_outlet_nodes = model.downstream_connection_node_ids(node_type="Outlet")
upstream_pump_nodes = model.upstream_connection_node_ids(node_type="Pump")
downstream_pump_nodes = model.downstream_connection_node_ids(node_type="Pump")

# === 1a. Upstream outlets met aanvoer: max_downstream = min_upstream + 0.02 en min_upstream = NA
out_static = model.outlet.static.df
pump_static = model.pump.static.df
mask_upstream_aanvoer = out_static["node_id"].isin(upstream_outlet_nodes)

node_ids = model.outlet.node.df[model.outlet.node.df["meta_categorie"] == "hoofdwater"].index
mask = model.outlet.static.df["node_id"].isin(node_ids)
model.outlet.static.df.loc[mask, "max_downstream_level"] = pd.NA

# %%
# === 1a. Upstream outlets (bij boundaries) met aanvoer, alleen aanvoer mogelijk ===
set_values_where(
    out_static,
    mask=mask_upstream_aanvoer,
    updates={
        "max_downstream_level": lambda d: d["min_upstream_level"],  # + 0.02,
        "min_upstream_level": pd.NA,
    },
)

updates_plan = [
    # Upstream boundary: Outlets en Pumps
    # (out_static, upstream_outlet_nodes, {"flow_rate": 100, "max_flow_rate": 100}),
    (pump_static, upstream_pump_nodes, {"min_upstream_level": pd.NA}),  # "flow_rate": 100, "max_flow_rate": 100,
    # Downstream boundary: Outlets en Pumps
    (out_static, downstream_outlet_nodes, {"max_downstream_level": pd.NA}),
    (pump_static, downstream_pump_nodes, {"max_downstream_level": pd.NA}),
]

for df, nodes, updates in updates_plan:
    set_values_where(df, node_ids=nodes, updates=updates)

# Alle pumps corrigeren met offset
set_values_where(
    pump_static,
    mask=pump_static.index.notna(),  # alle rijen
    updates={"max_downstream_level": lambda d: d["max_downstream_level"]},
)

# set upstream level boundaries
boundary_node_ids = [i for i in model.level_boundary.node.df.index if model.upstream_node_id(i) is None]
mask = model.level_boundary.static.df.node_id.isin(boundary_node_ids)
model.level_boundary.static.df.loc[mask, "level"] += 0.2

# set downstream level boundaries
boundary_node_ids = [i for i in model.level_boundary.node.df.index if model.upstream_node_id(i) is not None]
mask = model.level_boundary.static.df.node_id.isin(boundary_node_ids)
model.level_boundary.static.df.loc[mask, "level"] -= 1
# %%
# Inlaten
node_ids = model.outlet.static.df[model.outlet.static.df["meta_name"].str.startswith("INL", na=False)][
    "node_id"
].to_numpy()
model.outlet.static.df.loc[model.outlet.static.df["node_id"].isin(node_ids), "max_downstream_level"] += 0.02

# %%
# Afvoer altijd mogelijk door max_downstream op NA te zetten
mask = ~model.pump.static.df["meta_code"].str.contains("iKGM|_i", case=False, na=False)
node_ids = model.pump.static.df.loc[mask, "node_id"].to_numpy()
model.pump.static.df.loc[model.pump.static.df["node_id"].isin(node_ids), "max_downstream_level"] = pd.NA

model.pump.static.df.loc[
    model.pump.static.df.node_id == 40, "min_upstream_level"
] = -0.36  # Check! Bij min_upstream_level =-0.35m NP geen afvoer mogelijk.

# Oostersluis alleen schutverliezen op nul
model.pump.static.df.loc[model.pump.static.df.node_id == 142, "flow_rate"] = 0

# Min_upstream_level Oudendijk pump omlaag anders voert die alles af en gaat niks via Abelstok, klopt streefpeil hier wel?
model.pump.static.df.loc[model.pump.static.df.node_id == 118, "min_upstream_level"] = -1.09
model.outlet.static.df.loc[model.outlet.static.df.node_id == 487, "max_downstream_level"] = -1.09

# Den Deelen max_downstream_level op NA zetten
model.pump.static.df.loc[model.pump.static.df.node_id == 35, "min_upstream_level"] = pd.NA

# Drie Delfzijlen via gemaal
model.outlet.static.df.loc[model.outlet.static.df.node_id == 0, "flow_rate"] = 0
# inlaten naast pomp Rondpompen voorkomen, gelijk zetten aan min_upstream_level pomp
model.outlet.static.df.loc[model.outlet.static.df.node_id == 703, "max_downstream_level"] = -1.28
model.outlet.static.df.loc[model.outlet.static.df.node_id == 702, "max_downstream_level"] = -1.28
model.outlet.static.df.loc[model.outlet.static.df.node_id == 721, "min_upstream_level"] = -0.93
# Leek 2 inlaten naast pomp: rondpompen voorkomen, gelijk zetten aan min_upstream_level pomp
model.outlet.static.df.loc[model.outlet.static.df.node_id == 389, "max_downstream_level"] = -0.95
model.outlet.static.df.loc[model.outlet.static.df.node_id == 564, "max_downstream_level"] = -0.95
# Leek 2 inlaten naast pomp: rondpompen voorkomen
model.outlet.static.df.loc[model.outlet.static.df.node_id == 565, "max_downstream_level"] = 0.7
model.pump.static.df.loc[model.pump.static.df.node_id == 37, "min_upstream_level"] = 0.7

# Pomp en inlaat naast elkaar: rondpompen voorkomen
model.outlet.static.df.loc[model.outlet.static.df.node_id == 680, "max_downstream_level"] = -1.22

model.outlet.static.df.loc[model.outlet.static.df.node_id == 724, "flow_rate"] = 200
# Outlets Lauwersmeer aanpassen
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1748, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1754, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1755, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1747, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1748, "min_upstream_level"] = -0.95
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1754, "min_upstream_level"] = -0.95
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1755, "min_upstream_level"] = -0.95
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1747, "min_upstream_level"] = -0.95

model.outlet.static.df.loc[model.outlet.static.df.node_id == 1746, "max_flow_rate"] = 0.0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1756, "max_flow_rate"] = 0.0  # Check! Sluis
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1747, "max_flow_rate"] = 0.0  # Check! Sluis

# Streefpeilen basin gelijk daardoor geen aanvoer
model.outlet.static.df.loc[model.outlet.static.df.node_id == 653, "max_downstream_level"] = 9.14
model.outlet.static.df.loc[model.outlet.static.df.node_id == 460, "min_upstream_level"] = 9.12
model.outlet.static.df.loc[model.outlet.static.df.node_id == 460, "max_downstream_level"] = 9.10
model.outlet.static.df.loc[model.outlet.static.df.node_id == 421, "min_upstream_level"] = 9.08
model.outlet.static.df.loc[model.outlet.static.df.node_id == 439, "min_upstream_level"] = 7.14

# %%
# Rondpompen voorkomen bij 2 aanvoer en afvoer gemaal direct naast elkaar, min_upstream en max_downstream gelijk maken
# === INLINE: max_downstream_level(iKGM/iKST-pumps) = min_upstream_level(KGM/KST peer: pump óf outlet) ===

pump_static_df = model.pump.static.df
outlet_static_df = model.outlet.static.df

# Kolomnamen bepalen
code_col_pump = "meta_code_waterbeheerder" if "meta_code_waterbeheerder" in pump_static_df.columns else "meta_code"
code_col_outlet = "meta_code_waterbeheerder" if "meta_code_waterbeheerder" in outlet_static_df.columns else "meta_code"

min_us_col_pump = "min_upstream_level" if "min_upstream_level" in pump_static_df.columns else "min_upstream_water_level"
min_us_col_outlet = (
    "min_upstream_level" if "min_upstream_level" in outlet_static_df.columns else "min_upstream_water_level"
)

# --- 1) Bron (peer) verzamelen: NIET-i codes die beginnen met KGM of KST, met geldige min_upstream_level ---
peer_from_pumps = pump_static_df[[code_col_pump, min_us_col_pump]].copy()
peer_from_outlet = outlet_static_df[[code_col_outlet, min_us_col_outlet]].copy()

peer_from_pumps["code"] = peer_from_pumps[code_col_pump].astype(str)
peer_from_outlet["code"] = peer_from_outlet[code_col_outlet].astype(str)

peer_from_pumps = peer_from_pumps[
    peer_from_pumps["code"].str.startswith(("KGM", "KST"), na=False) & peer_from_pumps[min_us_col_pump].notna()
]
peer_from_outlet = peer_from_outlet[
    peer_from_outlet["code"].str.startswith(("KGM", "KST"), na=False) & peer_from_outlet[min_us_col_outlet].notna()
]

# Harmoniseer kolomnaam voor min_upstream in één samengevoegde tabel
if not peer_from_pumps.empty:
    peer_from_pumps = peer_from_pumps.rename(columns={min_us_col_pump: "min_upstream_peer"})
else:
    peer_from_pumps = pd.DataFrame(columns=["code", "min_upstream_peer"])

if not peer_from_outlet.empty:
    peer_from_outlet = peer_from_outlet.rename(columns={min_us_col_outlet: "min_upstream_peer"})
else:
    peer_from_outlet = pd.DataFrame(columns=["code", "min_upstream_peer"])

peer_sources_df = pd.concat(
    [peer_from_pumps[["code", "min_upstream_peer"]], peer_from_outlet[["code", "min_upstream_peer"]]], ignore_index=True
)

# Uniek per code (jij gaf aan: geen dubbele peers; 'first' is dan prima)
peer_sources_df = peer_sources_df.drop_duplicates(subset=["code"])

# Mapping: 'KGMxxx' / 'KSTyyy' -> min_upstream_peer
code_to_min_upstream_peer = dict(
    zip(peer_sources_df["code"].to_numpy(), peer_sources_df["min_upstream_peer"].astype(float).to_numpy())
)

# Niets te doen zonder peers
if code_to_min_upstream_peer:
    # --- 2) Doel: i-pompen (iKGM… / iKST…) uit PUMP ---
    i_pumps_df = pump_static_df[[code_col_pump, "node_id"]].copy()
    i_pumps_df["icode"] = i_pumps_df[code_col_pump].astype(str)
    i_pumps_df = i_pumps_df[i_pumps_df["icode"].str.startswith(("iKGM", "iKST"), na=False)]

    if not i_pumps_df.empty:
        # Basiscode = zonder 'i' voorloop: iKGM123 -> KGM123
        i_pumps_df["base_code"] = i_pumps_df["icode"].str[1:]

        # Nieuwe max_downstream_level ophalen van peer-map
        i_pumps_df["new_max_downstream_level"] = i_pumps_df["base_code"].map(code_to_min_upstream_peer)
        i_pumps_df = i_pumps_df[i_pumps_df["new_max_downstream_level"].notna()]

        if not i_pumps_df.empty:
            node_to_new_maxds = dict(
                zip(i_pumps_df["node_id"].to_numpy(), i_pumps_df["new_max_downstream_level"].to_numpy())
            )
            apply_mask = pump_static_df["node_id"].isin(node_to_new_maxds.keys())
            pump_static_df.loc[apply_mask, "max_downstream_level"] = pump_static_df.loc[apply_mask, "node_id"].map(
                node_to_new_maxds
            )
# %%

# === Instellingen ===
selected_outlet_ids = [714, 708]  # <-- vul hier jouw outlet node_ids in
LISTEN_NODE_ID = 1188  # luistersensor
DELTA_LOW = 0.07  # 5 cm
basin = model.basin.area.df
out_static = model.outlet.static.df
out_pump = model.pump.static.df

# === 1) Bepaal TH_HIGH vanaf min_upstream_level van LISTEN_NODE_ID ===
th_high = None

# probeer als outlet

row = basin.loc[basin["node_id"] == LISTEN_NODE_ID]
if not row.empty and "meta_streefpeil" in row.columns:
    val = row["meta_streefpeil"].iloc[0]
    if pd.notna(val):
        th_high = float(val) - 0.02
if th_high is None:
    raise ValueError(f"Kon 'min_upstream_level' voor listen_node_id {LISTEN_NODE_ID} niet vinden.")

th_low = th_high - DELTA_LOW

# === 2) Voeg per geselecteerde outlet de states + DC toe ===
for outlet_id in selected_outlet_ids:
    # lees h = huidige max_downstream_level van de outlet
    h_row = out_static.loc[out_static["node_id"] == outlet_id, "max_downstream_level"]
    if h_row.empty or pd.isna(h_row.iloc[0]):
        print(f"[skip] outlet {outlet_id}: geen geldige max_downstream_level")
        continue
    h = float(h_row.iloc[0])

    # 2a) outlet krijgt 2 states met max_downstream_level [h, h+0.02]
    model.update_node(
        node_id=outlet_id,
        node_type="Outlet",
        data=[
            outlet.Static(
                control_state=["closed", "open"],
                flow_rate=[0.0, 1.0],  # pas aan indien nodig
                max_downstream_level=[h + 0.02, h + 0.04],
            )
        ],
    )

    # 2b) DC die naar LISTEN_NODE_ID luistert met drempels TH_LOW/TH_HIGH
    geom = model.outlet[outlet_id].geometry
    x0, y0 = geom.x, geom.y

    dc_node_id = int(900000 + int(outlet_id))  # uniek id voor DC-node

    dc = model.discrete_control.add(
        Node(dc_node_id, Point(x0 + 10, y0)),
        [
            discrete_control.Variable(
                compound_variable_id=1,
                listen_node_id=LISTEN_NODE_ID,
                variable=["level"],
            ),
            discrete_control.Condition(
                compound_variable_id=1,
                condition_id=[1],
                threshold_high=[th_high],  # True als > TH_HIGH
                threshold_low=[th_low],  # False als ≤ TH_LOW
            ),
            discrete_control.Logic(
                truth_state=["T", "F"],
                control_state=["closed", "open"],
            ),
        ],
    )

    model.link.add(dc, model.outlet[outlet_id])

    print(
        f"[ok] outlet {outlet_id}: states [h={h:.3f}, h+0.02={h + 0.02:.3f}] + DC(th_low={th_low:.3f}, th_high={th_high:.3f})"
    )

# Discrete control toevoegen aan alle upstream outlets met aanvoer
upstream_outlet_ids = out_static.loc[mask_upstream_aanvoer, "node_id"].to_numpy()

for outlet_id in upstream_outlet_ids:
    # haal h = huidige max_downstream_level van deze outlet
    h_vals = out_static.loc[out_static["node_id"] == outlet_id, "max_downstream_level"].to_numpy()
    if len(h_vals) != 1 or pd.isna(h_vals[0]):
        # geen geldige h → overslaan
        continue
    h = float(h_vals[0])

    # 1) outlet: 2 states met variabele max_downstream_level [h, h+0.02]
    #    (flow_rate kun je hier aanpassen indien gewenst)
    model.update_node(
        node_id=outlet_id,
        node_type="Outlet",
        data=[
            outlet.Static(
                control_state=["closed", "open"],
                flow_rate=[0.0, 20.0],
                max_downstream_level=[h, h + 0.1],
            )
        ],
    )

    # 2) discrete control: luisteren naar listen_node_id=1493 met vaste band 7.62/7.68
    geom = model.outlet[outlet_id].geometry
    x0, y0 = geom.x, geom.y

    # uniek DC-node-id (voorkom botsing met bestaande ids)
    dc_node_id = int(900000 + int(outlet_id))

    dc = model.discrete_control.add(
        Node(dc_node_id, Point(x0 + 10, y0)),
        [
            # Luister naar peil Polder 6 (vast id = 1493)
            discrete_control.Variable(
                compound_variable_id=1,
                listen_node_id=1493,
                variable=["level"],
            ),
            # Hysterese-band vast: 7.62 / 7.68
            discrete_control.Condition(
                compound_variable_id=1,
                condition_id=[1],
                threshold_high=[7.68],  # True als > 7.68
            ),
            # True  -> closed
            # False -> open
            discrete_control.Logic(
                truth_state=["T", "F"],
                control_state=["closed", "open"],
            ),
        ],
    )

    # 3) koppel DC aan de outlet
    model.link.add(dc, model.outlet[outlet_id])

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

# %%
