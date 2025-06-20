# %%
import time

from peilbeheerst_model.controle_output import Control
from ribasim_nl import CloudStorage, Model
from ribasim_nl.check_basin_level import add_check_basin_level

cloud = CloudStorage()
authority = "BrabantseDelta"
short_name = "wbd"

run_model = False

parameters_dir = static_data_xlsx = cloud.joinpath(authority, "verwerkt", "parameters")
static_data_xlsx = parameters_dir / "static_data.xlsx"
profiles_gpkg = parameters_dir / "profiles.gpkg"
qlr_path = cloud.joinpath("Basisgegevens\\QGIS_lyr\\output_controle_vaw_afvoer.qlr")

ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_prepare_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"

# # you need the excel, but the model should be local-only by running 01_fix_model.py
cloud.synchronize(filepaths=[static_data_xlsx, profiles_gpkg], check_on_remote=False)
cloud.synchronize(filepaths=[ribasim_dir], check_on_remote=False)

# %%

# read
model = Model.read(ribasim_toml)
start_time = time.time()

# %%
# fixes model
# merge basins
model.merge_basins(node_id=2101, to_node_id=2058, are_connected=True)
model.merge_basins(node_id=1885, to_node_id=2114, are_connected=True)
model.merge_basins(node_id=2239, to_node_id=1617, are_connected=True)
model.merge_basins(node_id=1850, to_node_id=2022, are_connected=True)
model.merge_basins(node_id=2271, to_node_id=1766, are_connected=True)
model.merge_basins(node_id=2048, to_node_id=1412, are_connected=True)
model.merge_basins(node_id=2300, to_node_id=2198, are_connected=True)
model.merge_basins(node_id=2001, to_node_id=2273, are_connected=True)
model.merge_basins(node_id=1441, to_node_id=1799, are_connected=True)
model.merge_basins(node_id=2282, to_node_id=2120, are_connected=True)
model.merge_basins(node_id=2120, to_node_id=1928, are_connected=True)
model.merge_basins(node_id=1928, to_node_id=1581, are_connected=True)
model.merge_basins(node_id=1946, to_node_id=1581, are_connected=True)
model.merge_basins(node_id=2218, to_node_id=2311, are_connected=True)
model.merge_basins(node_id=2022, to_node_id=2311, are_connected=True)
model.merge_basins(node_id=1452, to_node_id=2114, are_connected=True)
model.merge_basins(node_id=2306, to_node_id=2281, are_connected=True)
model.merge_basins(node_id=2281, to_node_id=2176, are_connected=True)
model.merge_basins(node_id=2143, to_node_id=1617, are_connected=True)
model.merge_basins(node_id=2286, to_node_id=2295, are_connected=True)
model.merge_basins(node_id=2294, to_node_id=2275, are_connected=True)
model.merge_basins(node_id=1475, to_node_id=1689, are_connected=True)
model.merge_basins(node_id=1574, to_node_id=2276, are_connected=True)
model.merge_basins(node_id=2276, to_node_id=1854, are_connected=True)
model.merge_basins(node_id=1854, to_node_id=2293, are_connected=True)
model.merge_basins(node_id=2293, to_node_id=1858, are_connected=True)
model.merge_basins(node_id=1939, to_node_id=1445, are_connected=True)
model.merge_basins(node_id=1403, to_node_id=1611, are_connected=True)
model.merge_basins(node_id=1957, to_node_id=1856, are_connected=True)
model.merge_basins(node_id=1856, to_node_id=2289, are_connected=True)
model.merge_basins(node_id=1573, to_node_id=1720, are_connected=True)
model.merge_basins(node_id=1412, to_node_id=1563, are_connected=True)
model.merge_basins(node_id=1858, to_node_id=1689, are_connected=True)
model.merge_basins(node_id=1563, to_node_id=2280, are_connected=True)
model.merge_basins(node_id=2280, to_node_id=1802, are_connected=True)
model.merge_basins(node_id=1740, to_node_id=2047, are_connected=True)
model.merge_basins(node_id=2253, to_node_id=2114, are_connected=True)
model.merge_basins(node_id=1708, to_node_id=2129, are_connected=True)
model.merge_basins(node_id=1878, to_node_id=2214, are_connected=True)
model.merge_basins(node_id=2250, to_node_id=1428, are_connected=True)
model.merge_basins(node_id=1812, to_node_id=1870, are_connected=True)
model.merge_basins(node_id=1622, to_node_id=1780, are_connected=True)
model.merge_basins(node_id=1780, to_node_id=1694, are_connected=True)
model.merge_basins(node_id=1616, to_node_id=1909, are_connected=True)
model.merge_basins(node_id=2302, to_node_id=1808, are_connected=True)
model.merge_basins(node_id=1568, to_node_id=1909, are_connected=True)
model.merge_basins(node_id=1611, to_node_id=1543, are_connected=True)
model.merge_basins(node_id=1543, to_node_id=2289, are_connected=True)
model.merge_basins(node_id=2208, to_node_id=2242, are_connected=True)
model.merge_basins(node_id=2028, to_node_id=2242, are_connected=True)
model.merge_basins(node_id=2201, to_node_id=1820, are_connected=True)
model.merge_basins(node_id=2002, to_node_id=2215, are_connected=True)
model.merge_basins(node_id=2215, to_node_id=2248, are_connected=True)
model.merge_basins(node_id=2247, to_node_id=1998, are_connected=True)
model.remove_node(998, remove_edges=True)
model.basin.area.df.loc[model.basin.area.df.node_id == 342, "meta_streefpeil"] = 4.1
model.basin.area.df.loc[model.basin.area.df.node_id == 1989, "meta_streefpeil"] = 0.1
model.basin.area.df.loc[model.basin.area.df.node_id == 1909, "meta_streefpeil"] = 0.1
model.basin.area.df.loc[model.basin.area.df.node_id == 1634, "meta_streefpeil"] = 1.2  # Basin Turfvaart meetpunt
model.basin.area.df.loc[model.basin.area.df.node_id == 1584, "meta_streefpeil"] = 0.15
model.basin.area.df.loc[model.basin.area.df.node_id == 1987, "meta_streefpeil"] = -0.5
# %%
# parameterize
model.parameterize(static_data_xlsx=static_data_xlsx, precipitation_mm_per_day=5, profiles_gpkg=profiles_gpkg)
print("Elapsed Time:", time.time() - start_time, "seconds")


# %%
model.manning_resistance.static.df.loc[:, "manning_n"] = 0.001

# %%
# Flow rate en levels pumps verbeteren
model.pump.static.df.loc[model.pump.static.df.node_id == 535, "max_flow_rate"] = 0.05
model.pump.static.df.loc[model.pump.static.df.node_id == 517, "max_flow_rate"] = (
    5  # Let op: boven max cap van 2.83m3/s!
)
model.pump.static.df.loc[model.pump.static.df.node_id == 829, "max_flow_rate"] = 0.1  # inlaat
model.pump.static.df.loc[model.pump.static.df.node_id == 829, "max_downstream_level"] = 6
model.pump.static.df.loc[model.pump.static.df.node_id == 977, "max_flow_rate"] = 0.1  # inlaat
model.pump.static.df.loc[model.pump.static.df.node_id == 984, "max_flow_rate"] = 0.1  # Gemaal keersluis Leursche haven
model.pump.static.df.loc[model.pump.static.df.node_id == 446, "max_flow_rate"] = (
    2  # Let op: boven max cap van 0.06m3/s!
)
model.pump.static.df.loc[model.pump.static.df.node_id == 214, "max_downstream_level"] = 1.4
model.pump.static.df.loc[model.pump.static.df.node_id == 214, "min_upstream_level"] = 0.55
model.pump.static.df.loc[model.pump.static.df.node_id == 901, "max_flow_rate"] = 1  # max cap verhoogd! Check!
model.pump.static.df.loc[model.pump.static.df.node_id == 453, "max_flow_rate"] = 1  # max cap verhoogd! Check!
model.pump.static.df.loc[model.pump.static.df.node_id == 376, "max_flow_rate"] = 1  # max cap verhoogd! Check!
model.pump.static.df.loc[model.pump.static.df.node_id == 449, "max_flow_rate"] = 1  # max cap verhoogd! Check!
model.pump.static.df.loc[model.pump.static.df.node_id == 703, "max_flow_rate"] = 1  # max cap verhoogd! Check!
model.pump.static.df.loc[model.pump.static.df.node_id == 747, "max_flow_rate"] = 1  # max cap verhoogd! Check!

# Upstream levels kloppen niet
model.outlet.static.df.loc[model.outlet.static.df.node_id == 845, "min_upstream_level"] = 6.1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 146, "min_upstream_level"] = 6.1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 536, "min_upstream_level"] = -1.6
model.outlet.static.df.loc[model.outlet.static.df.node_id == 217, "min_upstream_level"] = 1.2  # Turfvaart meetpunt
model.outlet.static.df.loc[model.outlet.static.df.node_id == 342, "min_upstream_level"] = 4.1
model.pump.static.df.loc[model.pump.static.df.node_id == 972, "min_upstream_level"] = 0.55  # Roode Vaart Afvoergemaal
model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id == 36, "level"] = -3


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
    971: 0,  # Geeb Aanvoer Marksluis
    991: 1,  # Aanvoer
    393: 0.1,
    539: 0.1,
    2323: 1,  # Aanvoer
}

for node_id, flow_rate in flow_updates.items():
    model.outlet.static.df.loc[model.outlet.static.df.node_id == node_id, "max_flow_rate"] = flow_rate

# %% Geen sturing op duikers in niet gestuwde gebieden
node_ids = model.outlet.node.df[model.outlet.node.df["meta_gestuwd"] == "False"].index
mask = model.outlet.static.df["node_id"].isin(node_ids)
model.outlet.static.df.loc[mask, "min_upstream_level"] = pd.NA
model.outlet.static.df.loc[mask, "max_downstream_level"] = pd.NA

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
    exit_code = model.run()
    assert exit_code == 0

    # # %%
    controle_output = Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path)
    indicators = controle_output.run_afvoer()
# %%
