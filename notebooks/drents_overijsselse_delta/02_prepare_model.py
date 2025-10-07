# %%
import inspect

import geopandas as gpd
import pandas as pd
from peilbeheerst_model.assign_authorities import AssignAuthorities
from ribasim_nl.gkw import get_data_from_gkw
from ribasim_nl.link_geometries import fix_link_geometries
from ribasim_nl.link_profiles import add_link_profile_ids
from ribasim_nl.parametrization.damo_profiles import DAMOProfiles
from ribasim_nl.parametrization.static_data_xlsx import StaticData

from ribasim_nl import CloudStorage, Model, Network

cloud = CloudStorage()
authority = "DrentsOverijsselseDelta"
short_name = "dod"

# %% files
ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_fix_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"

parameters_dir = static_data_xlsx = cloud.joinpath(authority, "verwerkt", "parameters")
static_data_xlsx = parameters_dir / "static_data_template.xlsx"
profiles_gpkg = parameters_dir / "profiles.gpkg"
link_geometries_gpkg = parameters_dir / "link_geometries.gpkg"

peilgebieden_path = cloud.joinpath(authority, "verwerkt/1_ontvangen_data/extra data/Peilgebieden/Peilgebieden.shp")
hydamo_wm_gpkg = cloud.joinpath(authority, "verwerkt/1_ontvangen_data/HyDAMO_WM_20230720.gpkg")
meppelerdiep_gpkg = cloud.joinpath(authority, "verwerkt/2_voorbewerking/meppelerdiep.gpkg")
top10NL_gpkg = cloud.joinpath("Basisgegevens", "Top10NL", "top10nl_Compleet.gpkg")
model_edits_aanvoer_gpkg = cloud.joinpath(authority, "verwerkt", "model_edits_aanvoer.gpkg")

cloud.synchronize(filepaths=[peilgebieden_path, top10NL_gpkg])

# %% init things
model = Model.read(ribasim_toml)
ribasim_toml = ribasim_dir.with_name(f"{authority}_prepare_model") / ribasim_toml.name
lines_gdf = pd.concat(
    [gpd.read_file(hydamo_wm_gpkg, layer="hydroobject"), gpd.read_file(meppelerdiep_gpkg)], ignore_index=True
)
network = Network(lines_gdf=lines_gdf, tolerance=0.2)
damo_profiles = DAMOProfiles(
    model=model,
    network=network,
    profile_line_df=gpd.read_file(hydamo_wm_gpkg, layer="profiellijn"),
    profile_point_df=gpd.read_file(hydamo_wm_gpkg, layer="profielpunt"),
    water_area_df=gpd.read_file(top10NL_gpkg, layer="top10nl_waterdeel_vlak"),
)

if not profiles_gpkg.exists():
    profiles_df = damo_profiles.process_profiles()
    profiles_df.to_file(profiles_gpkg)
else:
    profiles_df = gpd.read_file(profiles_gpkg)

profiles_df.set_index("profiel_id", inplace=True)
static_data = StaticData(model=model, xlsx_path=static_data_xlsx)


# %%

# fix link geometries
if link_geometries_gpkg.exists():
    link_geometries_df = gpd.read_file(link_geometries_gpkg).set_index("link_id")
    model.edge.df.loc[link_geometries_df.index, "geometry"] = link_geometries_df["geometry"]
    model.edge.df.loc[link_geometries_df.index, "meta_profielid_waterbeheerder"] = link_geometries_df[
        "meta_profielid_waterbeheerder"
    ]
else:
    add_link_profile_ids(model, profiles=damo_profiles)
    fix_link_geometries(model, network, max_straight_line_ratio=5)
    model.edge.df.reset_index().to_file(link_geometries_gpkg)

# %%
# %% Quick fix basins

actions = [
    "remove_basin_area",
    #    "remove_node",
    #    "remove_edge",
    "add_basin",
    "add_basin_area",
    # "update_basin_area",
    # "merge_basins",
    # "reverse_edge",
    # "move_node",
    "connect_basins",
    #   "update_node",
    "redirect_edge",
]
actions = [i for i in actions if i in gpd.list_layers(model_edits_aanvoer_gpkg).name.to_list()]
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


model.outlet.static.df.loc[model.outlet.static.df.node_id == 1389, "flow_rate"] = 0.1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 2609, "flow_rate"] = 0.1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 2609, "min_upstream_level"] = 9.35
model.outlet.static.df.loc[model.outlet.static.df.node_id == 463, "min_upstream_level"] = 9.35
model.outlet.static.df.loc[model.outlet.static.df.node_id == 463, "flow_rate"] = 0.5
# %%

if "meta_code_waterbeheerder" not in model.basin.area.df.columns:
    model.basin.area.df["meta_code_waterbeheerder"] = pd.Series(dtype=str)

if "meta_streefpeil" in model.basin.area.df.columns:
    model.basin.area.df.drop(columns="meta_streefpeil", inplace=True)
model.basin.area.df["meta_streefpeil"] = pd.Series(dtype=float)


# %% Bepaal min_upstream_level Outlet`

static_data.reset_data_frame(node_type="Outlet")
node_ids = static_data.outlet.node_id
levels = []
for node_id in node_ids:
    node = model.outlet[node_id]
    peilgebieden_df = gpd.read_file(peilgebieden_path)
    tolerance = 50  # afstand voor zoeken bovenstrooms
    node_id = node.node_id
    node_geometry = node.geometry

    line_to_node = model.link.df.set_index("to_node_id").at[node_id, "geometry"]
    distance_to_interpolate = line_to_node.length - tolerance
    if distance_to_interpolate < 0:
        distance_to_interpolate = 0

    containing_point = line_to_node.interpolate(distance_to_interpolate)
    peilgebieden_select_df = peilgebieden_df[peilgebieden_df.contains(containing_point)]
    if not peilgebieden_select_df.empty:
        peilgebied = peilgebieden_select_df.iloc[0]
        if peilgebied["GPGZMRPL"] < 30:
            level = peilgebied["GPGZMRPL"]
        else:
            level = None
    levels += [level]

min_upstream_level = pd.Series(levels, index=node_ids, name="min_upstream_level")
min_upstream_level.index.name = "node_id"

static_data.add_series(node_type="Outlet", series=min_upstream_level)


# %% Bepaal min_upstream_level pumps

static_data.reset_data_frame(node_type="Pump")
node_ids = static_data.pump.node_id
levels = []

for node_id in node_ids:
    node = model.pump[node_id]
    peilgebieden_df = gpd.read_file(peilgebieden_path)
    tolerance = 50
    node_id = node.node_id
    node_geometry = node.geometry

    line_to_node = model.link.df.set_index("to_node_id").at[node_id, "geometry"]
    distance_to_interpolate = line_to_node.length - tolerance
    if distance_to_interpolate < 0:
        distance_to_interpolate = 0

    containing_point = line_to_node.interpolate(distance_to_interpolate)
    peilgebieden_select_df = peilgebieden_df[peilgebieden_df.contains(containing_point)]

    if not peilgebieden_select_df.empty:
        peilgebied = peilgebieden_select_df.iloc[0]
        if peilgebied["GPGZMRPL"] < 30:  # 🔹 Drempelwaarde voor max peil
            level = peilgebied["GPGZMRPL"]
        else:
            level = None
    else:
        level = None

    levels.append(level)

min_upstream_level = pd.Series(levels, index=node_ids, name="min_upstream_level")
min_upstream_level.index.name = "node_id"

static_data.add_series(node_type="Pump", series=min_upstream_level)

# %% Bepaal de basin streefpeilen door minimale upstream level outlet en gemalen. Streefpeil stuw en gemalen krijgt voorrang

static_data.reset_data_frame(node_type="Basin")
node_ids = static_data.basin[static_data.basin.streefpeil.isna()].node_id.to_numpy()
ds_node_ids = []

for node_id in node_ids:
    try:
        ds = model.downstream_node_id(int(node_id))  # Zorg dat type klopt
        if isinstance(ds, pd.Series):
            ds_node_ids.append(ds.to_list())
        else:
            ds_node_ids.append([ds])
    except KeyError:
        ds_node_ids.append([])

ds_node_ids = pd.Series(ds_node_ids, index=node_ids).explode()
ds_node_ids = ds_node_ids[ds_node_ids.isin(static_data.outlet.node_id) | ds_node_ids.isin(static_data.pump.node_id)]
combined = pd.concat([static_data.outlet, static_data.pump])
combined = combined.reset_index(drop=True)

combined["source"] = combined.apply(
    lambda row: "pump" if row["node_id"] in static_data.pump.node_id.values else "outlet", axis=1
)

valid_ids = ds_node_ids[ds_node_ids.isin(combined["node_id"])]
streefpeil = combined.set_index("node_id").loc[valid_ids.to_numpy(), ["min_upstream_level", "code", "source"]]
streefpeil["basin_node_id"] = valid_ids.loc[valid_ids.isin(streefpeil.index)].index
streefpeil["node_id"] = streefpeil.index  # outlet/pump node_id
streefpeil = streefpeil.set_index("basin_node_id")
streefpeil.dropna(inplace=True)

# Sorteer per groep zodat de kleinste 'min_upstream_level' bovenaan staat
streefpeil = streefpeil.sort_index().sort_values(by="min_upstream_level", inplace=False)
non_kdu_values = streefpeil[~streefpeil["code"].str.startswith("KDU") | (streefpeil["source"] == "pump")]
non_kdu_first = non_kdu_values.groupby(level=0).first()
all_first = streefpeil.groupby(level=0).first()

# update duiker min_upstream_level wanneer een stuw/pomp in dat basin ligt
streefpeil_update = streefpeil.loc[streefpeil.index.isin(non_kdu_first.index)].copy()
streefpeil_update["min_upstream_level"] = streefpeil_update.index.map(non_kdu_first["min_upstream_level"])
min_upstream_level_outlets = streefpeil_update.set_index("node_id")["min_upstream_level"]
min_upstream_level_outlets.index.name = "node_id"

static_data.add_series(node_type="Outlet", series=min_upstream_level_outlets, fill_na=False)

# update basin levels
streefpeil = non_kdu_first.combine_first(all_first)
streefpeil = streefpeil["min_upstream_level"]
streefpeil.index.name = "node_id"
streefpeil.name = "streefpeil"
static_data.add_series(node_type="Basin", series=streefpeil, fill_na=True)

# %% Bepaal min_upstream_level at Manning locations`en vul de nodata basins met deze streefpeilen
node_ids = model.manning_resistance.static.df["node_id"]
min_upstream_level = []

levels = []
for node_id in node_ids:
    node = model.manning_resistance[node_id]
    peilgebieden_df = gpd.read_file(peilgebieden_path)
    tolerance = 50
    node_id = node.node_id
    node_geometry = node.geometry

    line_to_node = model.link.df.set_index("to_node_id").at[node_id, "geometry"]
    distance_to_interpolate = line_to_node.length - tolerance
    if distance_to_interpolate < 0:
        distance_to_interpolate = 0

    containing_point = line_to_node.interpolate(distance_to_interpolate)
    peilgebieden_select_df = peilgebieden_df[peilgebieden_df.contains(containing_point)]
    if not peilgebieden_select_df.empty:
        peilgebied = peilgebieden_select_df.iloc[0]
        if peilgebied["GPGZMRPL"] < 30:
            level = peilgebied["GPGZMRPL"]
        else:
            level = None
    levels += [level]

min_upstream_level = pd.Series(levels, index=node_ids, name="min_upstream_level")
min_upstream_level.index.name = "node_id"

missing_basins = static_data.basin[static_data.basin.streefpeil.isna()]
basin_node_ids = missing_basins.node_id.to_numpy()

# Get downstream node(s) for each basin
ds_node_ids = []
ds_index = []
for i in basin_node_ids:
    try:
        ds_ids = model.downstream_node_id(i)
        ds_ids = ds_ids.to_list() if isinstance(ds_ids, pd.Series) else [ds_ids]
        ds_node_ids.extend(ds_ids)
        ds_index.extend([i] * len(ds_ids))
    except KeyError:
        print(f"No downstream node found for basin node {i}")

ds_node_ids = pd.Series(ds_node_ids, index=ds_index, name="ds_node_id")

# Keep only those that exist in min_upstream_level
valid_ds = ds_node_ids[ds_node_ids.isin(min_upstream_level.index)]
streefpeil = min_upstream_level.loc[valid_ds.values].rename(index=dict(zip(valid_ds.values, valid_ds.index)))
streefpeil = streefpeil.groupby(streefpeil.index).min()
streefpeil.index.name = "node_id"
streefpeil.name = "streefpeil"

static_data.add_series(node_type="Basin", series=streefpeil, fill_na=True)


# %%
# DAMO-profielen bepalen voor outlets wanneer min_upstream_level nodata
node_ids = static_data.outlet[static_data.outlet.min_upstream_level.isna()].node_id.to_numpy()
profile_ids = model.edge.df.set_index("to_node_id").loc[node_ids]["meta_profielid_waterbeheerder"]
levels = ((profiles_df.loc[profile_ids]["bottom_level"] + profiles_df.loc[profile_ids]["invert_level"]) / 2).to_numpy()
min_upstream_level = pd.Series(levels, index=node_ids, name="min_upstream_level")
min_upstream_level.index.name = "node_id"
static_data.add_series(node_type="Outlet", series=min_upstream_level, fill_na=True)


# %%
## PUMP.flow_rate
gkw_gemaal_df = get_data_from_gkw(authority=authority, layers=["gemaal"])
flow_rate = gkw_gemaal_df.set_index("code")["maximalecapaciteit"] / 60  # m3/minuut to m3/s
flow_rate.name = "flow_rate"
static_data.add_series(node_type="Pump", series=flow_rate)

# %%

model.basin.area.df.loc[model.basin.area.df.node_id == 2190, "meta_streefpeil"] = -0.6
model.basin.area.df.loc[model.basin.area.df.node_id == 1868, "meta_streefpeil"] = 3.5
model.basin.area.df.loc[model.basin.area.df.node_id == 1612, "meta_streefpeil"] = model.basin.area.df.set_index(
    "node_id"
).at[1769, "meta_streefpeil"]

model.basin.area.df.loc[model.basin.area.df.node_id == 2190, "meta_streefpeil"] = -0.19


# %%

# get all nodata streefpeilen with their profile_ids and levels
node_ids = static_data.basin[static_data.basin.streefpeil.isna()].node_id.to_numpy()
profile_ids = [damo_profiles.get_profile_id(node_id) for node_id in node_ids]
levels = (
    profiles_df.loc[profile_ids]["bottom_level"]
    + (profiles_df.loc[profile_ids]["invert_level"] - profiles_df.loc[profile_ids]["bottom_level"]) / 2
).to_numpy()

# # update static_data
profielid = pd.Series(profile_ids, index=pd.Index(node_ids, name="node_id"), name="profielid")
static_data.add_series(node_type="Basin", series=profielid, fill_na=True)
streefpeil = pd.Series(levels, index=pd.Index(node_ids, name="node_id"), name="streefpeil")
static_data.add_series(node_type="Basin", series=streefpeil, fill_na=True)

# # update model basin-data
model.basin.area.df.set_index("node_id", inplace=True)
streefpeil = static_data.basin.set_index("node_id")["streefpeil"]
model.basin.area.df.loc[streefpeil.index, "meta_streefpeil"] = streefpeil
profiellijnid = static_data.basin.set_index("node_id")["profielid"]
model.basin.area.df.loc[streefpeil.index, "meta_profiellijnid"] = profiellijnid
model.basin.area.df["meta_streefpeil"] = pd.to_numeric(model.basin.area.df["meta_streefpeil"], errors="coerce")

model.basin.area.df.reset_index(drop=False, inplace=True)
model.basin.area.df.index += 1
model.basin.area.df.index.name = "fid"


# # koppelen
ws_grenzen_path = cloud.joinpath("Basisgegevens", "RWS_waterschaps_grenzen", "waterschap.gpkg")
RWS_grenzen_path = cloud.joinpath("Basisgegevens", "RWS_waterschaps_grenzen", "Rijkswaterstaat.gpkg")
assign = AssignAuthorities(
    ribasim_model=model,
    waterschap=authority,
    ws_grenzen_path=ws_grenzen_path,
    RWS_grenzen_path=RWS_grenzen_path,
)
model = assign.assign_authorities()

# %%
# defaults
static_data.write()

# write model
model.write(ribasim_toml)


# %%
# OUTLET

# OUTLET.min_upstream_level
# # from basin streefpeil
# static_data.reset_data_frame(node_type="Outlet")
# min_upstream_level = upstream_target_levels(model=model, node_ids=static_data.outlet.node_id)
# min_upstream_level = min_upstream_level[min_upstream_level.notna()]
# min_upstream_level.name = "min_upstream_level"
# static_data.add_series(node_type="Outlet", series=min_upstream_level)


# # %%

# # PUMP

# # PUMP.min_upstream_level
# # from basin streefpeil
# static_data.reset_data_frame(node_type="Pump")
# min_upstream_level = upstream_target_levels(model=model, node_ids=static_data.pump.node_id)
# min_upstream_level = min_upstream_level[min_upstream_level.notna()]
# min_upstream_level.name = "min_upstream_level"
# static_data.add_series(node_type="Pump", series=min_upstream_level)


# # %%

# # # BASIN
# static_data.reset_data_frame(node_type="Basin")

# # %%

# # write
# static_data.write()

# model.write(ribasim_toml)

# %%
