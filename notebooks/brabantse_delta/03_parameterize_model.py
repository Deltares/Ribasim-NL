# %%
import time

import pandas as pd
from peilbeheerst_model.controle_output import Control
from ribasim_nl.check_basin_level import add_check_basin_level

from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()
authority = "BrabantseDelta"
short_name = "wbd"

run_model = True

parameters_dir = cloud.joinpath(authority, "verwerkt/parameters")
static_data_xlsx = parameters_dir / "static_data.xlsx"
profiles_gpkg = parameters_dir / "profiles.gpkg"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_afvoer.qlr")

ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_prepare_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"

# # you need the excel, but the model should be local-only by running 01_fix_model.py
cloud.synchronize(filepaths=[static_data_xlsx])

# %%

# read
model = Model.read(ribasim_toml)
start_time = time.time()

# %%
# parameterize
# fix basin streefpeilen
model.basin.area.df.loc[model.basin.area.df.node_id == 1354, "meta_streefpeil"] = (
    4.1  # n.a.v. min_upstream_level van outlet 342
)
model.parameterize(static_data_xlsx=static_data_xlsx, precipitation_mm_per_day=5, profiles_gpkg=profiles_gpkg)
print("Elapsed Time:", time.time() - start_time, "seconds")
model.manning_resistance.static.df.loc[:, "manning_n"] = 0.005


# %% Geen sturing op duikers in niet gestuwde gebieden
node_ids = model.outlet.node.df[model.outlet.node.df["meta_gestuwd"] == "False"].index
mask = model.outlet.static.df["node_id"].isin(node_ids)
model.outlet.static.df.loc[mask, "min_upstream_level"] = pd.NA
model.outlet.static.df.loc[mask, "max_downstream_level"] = pd.NA

# %%
# Flow rate en levels pumps verbeteren
model.pump.static.df.loc[model.pump.static.df.node_id == 535, "flow_rate"] = 0.05
model.pump.static.df.loc[model.pump.static.df.node_id == 623, "flow_rate"] = 5.0  # Let op: boven max cap van 2m3/s!
model.pump.static.df.loc[model.pump.static.df.node_id == 829, "flow_rate"] = 0.1  # inlaat
model.pump.static.df.loc[model.pump.static.df.node_id == 829, "max_downstream_level"] = 6.0
model.pump.static.df.loc[model.pump.static.df.node_id == 977, "flow_rate"] = 0.1  # inlaat
model.pump.static.df.loc[model.pump.static.df.node_id == 984, "flow_rate"] = 0.1  # Gemaal keersluis Leursche haven
model.pump.static.df.loc[model.pump.static.df.node_id == 446, "flow_rate"] = 2.0  # Let op: boven max cap van 0.06m3/s!
model.pump.static.df.loc[model.pump.static.df.node_id == 214, "max_downstream_level"] = 1.4
model.pump.static.df.loc[model.pump.static.df.node_id == 214, "min_upstream_level"] = 0.55
model.pump.static.df.loc[model.pump.static.df.node_id == 901, "flow_rate"] = 1.0  # max cap verhoogd! Check!
model.pump.static.df.loc[model.pump.static.df.node_id == 453, "flow_rate"] = 1.0  # max cap verhoogd! Check!
model.pump.static.df.loc[model.pump.static.df.node_id == 376, "flow_rate"] = 1.0  # max cap verhoogd! Check!
model.pump.static.df.loc[model.pump.static.df.node_id == 449, "flow_rate"] = 1.0  # max cap verhoogd! Check!
model.pump.static.df.loc[model.pump.static.df.node_id == 703, "flow_rate"] = 1.0  # max cap verhoogd! Check!
model.pump.static.df.loc[model.pump.static.df.node_id == 747, "flow_rate"] = 1.0  # max cap verhoogd! Check!

# Upstream levels kloppen niet
model.outlet.static.df.loc[model.outlet.static.df.node_id == 845, "min_upstream_level"] = 6.1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 146, "min_upstream_level"] = 6.1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 536, "min_upstream_level"] = -1.6
model.outlet.static.df.loc[model.outlet.static.df.node_id == 217, "min_upstream_level"] = 1.2  # Turfvaart meetpunt
model.outlet.static.df.loc[model.outlet.static.df.node_id == 342, "min_upstream_level"] = 4.1
model.pump.static.df.loc[model.pump.static.df.node_id == 972, "min_upstream_level"] = 0.55  # Roode Vaart Afvoergemaal

# Voor outlets flow_updates
flow_updates = {
    123: 1,
    150: 5,  # Let op: boven cap van 0.07 m³/s! Check!
    218: 0.5,
    233: 5,  # Let op, Het Laag verhoogd naar 5m3/s! Check!
    240: 0,  # Geen Aanvoer Marksluis
    241: 0.1,
    255: 0.1,
    271: 0.1,
    368: 0.1,
    369: 0.1,
    375: 0.1,
    376: 0.5,  # Max cap verhoogd van 0.05m3/s! Check!
    383: 0.1,
    384: 0.1,
    385: 0.1,
    386: 0.1,
    387: 0.1,
    391: 0.1,
    396: 0.1,
    399: 0.1,
    400: 0.1,
    401: 0.1,
    403: 0.1,
    405: 0.1,
    407: 0.1,
    408: 3,
    410: 0.1,
    411: 0.1,
    414: 0.1,
    415: 0.1,
    416: 0.1,
    417: 0.1,
    418: 0.1,
    422: 0.1,
    427: 0.1,
    439: 0.1,
    441: 0.1,
    461: 1,  # Aanvoer
    497: 0.1,
    499: 0.1,
    503: 0.1,
    537: 0.1,
    544: 0.1,
    556: 0.1,
    566: 0.1,
    576: 1,  # Aanvoer
    577: 1,  # Aanvoer
    580: 1,  # Aanvoer
    581: 1,  # Aanvoer
    585: 1,  # Aanvoer
    589: 0.55,
    593: 1,  # Aanvoer
    614: 0.1,
    615: 0.1,
    655: 0.1,
    656: 0.1,
    676: 0.1,
    732: 0.5,
    737: 1,
    738: 1,
    745: 1,  # Let op, max cap Groenvenseweg was 0.067m3/s, nu 1 m3/s! Check!
    799: 0.1,
    955: 1,  # Aanvoer
    935: 0.1,
    983: 0.1,
    987: 0.1,
    971: 0,  # Geen Aanvoer Marksluis
    991: 1,  # Aanvoer
    393: 0.1,
    539: 0.1,
    2323: 1,  # Aanvoer
}

for node_id, flow_rate in flow_updates.items():
    model.outlet.static.df.loc[model.outlet.static.df.node_id == node_id, "flow_rate"] = flow_rate

# Upstream levels kloppen niet
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1055, "min_upstream_level"] = 0.1  # Benedensas Volkerak
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1056, "min_upstream_level"] = 0.1  # Benedensas Volkerak
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1048, "min_upstream_level"] = 0.1  # Volkerak
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1049, "min_upstream_level"] = 0.1  # Volkerak
model.outlet.static.df.loc[model.outlet.static.df.node_id == 503, "min_upstream_level"] = 0.1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 667, "min_upstream_level"] = 0.1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 668, "min_upstream_level"] = 0.1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 408, "min_upstream_level"] = 0.55  # Haven van Zevenbergen
model.outlet.static.df.loc[model.outlet.static.df.node_id == 589, "min_upstream_level"] = 0.55  # Roode Vaart Sluis
model.outlet.static.df.loc[model.outlet.static.df.node_id == 884, "min_upstream_level"] = 0.55  # Roode Vaart duiker
model.outlet.static.df.loc[
    model.outlet.static.df.node_id == 732, "min_upstream_level"
] = -0.5  # Jan Steenlaan (LOPstuw), aangepast anders stroomt model leeg via Hooislobben
model.outlet.static.df.loc[
    model.outlet.static.df.node_id == 250, "min_upstream_level"
] = -0.5  # Rijksweg/Kraanschotsedijk
# %%

# Write model
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_parameterized_model", f"{short_name}.toml")
add_check_basin_level(model=model)
model.basin.area.df.loc[:, "meta_area"] = model.basin.area.df.area
model.write(ribasim_toml)

# %%

# run model
if run_model:
    result = model.run()
    assert result.exit_code == 0

    # # %%
    controle_output = Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path)
    indicators = controle_output.run_afvoer()
# %%
