# %%

import geopandas as gpd
import pandas as pd

from ribasim_nl import CloudStorage, Model, Network
from ribasim_nl.gkw import get_data_from_gkw
from ribasim_nl.link_geometries import fix_link_geometries
from ribasim_nl.link_profiles import add_link_profile_ids
from ribasim_nl.parametrization.damo_profiles import DAMOProfiles
from ribasim_nl.parametrization.static_data_xlsx import StaticData

cloud = CloudStorage()
authority = "ValleienVeluwe"
short_name = "venv"

ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_fix_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"

model = Model.read(ribasim_toml)
ribasim_toml = ribasim_dir.with_name(f"{authority}_prepare_model") / ribasim_toml.name

# %%
# check files
peilgebieden_path = cloud.joinpath(cloud.joinpath(authority, "verwerkt/1_ontvangen_data/20250428/Peilvakken.shp"))
top10NL_gpkg = cloud.joinpath("Basisgegevens", "Top10NL", "top10nl_Compleet.gpkg")
venv_hydamo_gpkg = cloud.joinpath(authority, "verwerkt", "2_voorbewerking", "hydamo.gpkg")

parameters_dir = static_data_xlsx = cloud.joinpath(authority, "verwerkt", "parameters")
static_data_xlsx = parameters_dir / "static_data_template.xlsx"
profiles_gpkg = parameters_dir / "profiles.gpkg"
link_geometries_gpkg = parameters_dir / "link_geometries.gpkg"

cloud.synchronize(filepaths=[peilgebieden_path, top10NL_gpkg])

bbox = None
# init classes
static_data = StaticData(model=model, xlsx_path=static_data_xlsx)
# %% Edges

network = Network(lines_gdf=gpd.read_file(venv_hydamo_gpkg, layer="hydroobject", bbox=bbox), tolerance=0.2)
damo_profiles = DAMOProfiles(
    model=model,
    network=network,
    profile_line_df=gpd.read_file(venv_hydamo_gpkg, layer="profiellijn", bbox=bbox),
    profile_point_df=gpd.read_file(venv_hydamo_gpkg, layer="profielpunt", bbox=bbox),
    water_area_df=gpd.read_file(top10NL_gpkg, layer="top10nl_waterdeel_vlak", bbox=bbox),
    profile_line_id_col="code",
)
if not profiles_gpkg.exists():
    damo_profiles.process_profiles().to_file(profiles_gpkg)


# %%
# fix link geometries
if link_geometries_gpkg.exists():
    link_geometries_df = gpd.read_file(link_geometries_gpkg).set_index("link_id")
    model.edge.df.loc[link_geometries_df.index, "geometry"] = link_geometries_df["geometry"]
    if "meta_profielid_waterbeheerder" in link_geometries_df.columns:
        model.edge.df.loc[link_geometries_df.index, "meta_profielid_waterbeheerder"] = link_geometries_df[
            "meta_profielid_waterbeheerder"
        ]
    profiles_df = gpd.read_file(profiles_gpkg)
else:
    profiles_df = damo_profiles.process_profiles()
    profiles_df.to_file(profiles_gpkg)
    fix_link_geometries(model, network)
    add_link_profile_ids(model, profiles=damo_profiles, id_col="code")
    model.edge.df.reset_index().to_file(link_geometries_gpkg)
profiles_df.set_index("profiel_id", inplace=True)

# %% Bepaal min_upstream_level Outlet`

static_data.reset_data_frame(node_type="Outlet")
node_ids = static_data.outlet.node_id
levels = []
for node_id in node_ids:
    node = model.outlet[node_id]
    peilgebieden_df = gpd.read_file(peilgebieden_path)
    tolerance = 10  # afstand voor zoeken bovenstrooms
    node_id = node.node_id
    node_geometry = node.geometry

    # haal bovenstroomse en bendenstroomse links op
    line_to_node = model.link.df.set_index("to_node_id").at[node_id, "geometry"]
    # bepaal een punt 10 meter bovenstrooms node
    containing_point = line_to_node.interpolate(line_to_node.length - tolerance)
    # filter peilgebieden met intersect bovenstroomse link
    peilgebieden_select_df = peilgebieden_df[peilgebieden_df.contains(containing_point)]
    # Als er meerdere peilgebieden zijn, kies de juiste
    if not peilgebieden_select_df.empty:
        peilgebied = peilgebieden_select_df.iloc[0]
        if peilgebied["WS_MAX_PEI"] < 30:
            level = peilgebied["WS_MAX_PEI"]
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
    tolerance = 10
    node_id = node.node_id
    node_geometry = node.geometry

    # Haal de bovenstroomse en benedenstroomse links op
    line_to_node = model.link.df.set_index("to_node_id").at[node_id, "geometry"]

    # Bepaal een punt 10 meter bovenstrooms de node
    containing_point = line_to_node.interpolate(line_to_node.length - tolerance)

    # Filter peilgebieden die dit punt bevatten
    peilgebieden_select_df = peilgebieden_df[peilgebieden_df.contains(containing_point)]

    # Als er meerdere peilgebieden zijn, kies de juiste
    if not peilgebieden_select_df.empty:
        peilgebied = peilgebieden_select_df.iloc[0]
        if peilgebied["WS_MAX_PEI"] < 30:  # 🔹 Drempelwaarde voor max peil
            level = peilgebied["WS_MAX_PEI"]
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
    tolerance = 10
    node_id = node.node_id
    node_geometry = node.geometry
    line_to_node = model.link.df.set_index("to_node_id").at[node_id, "geometry"]
    containing_point = line_to_node.interpolate(line_to_node.length - tolerance)
    peilgebieden_select_df = peilgebieden_df[peilgebieden_df.contains(containing_point)]
    if not peilgebieden_select_df.empty:
        peilgebied = peilgebieden_select_df.iloc[0]
        if peilgebied["WS_MAX_PEI"] < 30:
            level = peilgebied["WS_MAX_PEI"]
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
# DAMO-profielen bepalen voor pumps wanneer min_upstream_level nodata
node_ids = static_data.pump[static_data.pump.min_upstream_level.isna()].node_id.to_numpy()
profile_ids = model.edge.df.set_index("to_node_id").loc[node_ids]["meta_profielid_waterbeheerder"]
levels = ((profiles_df.loc[profile_ids]["bottom_level"] + profiles_df.loc[profile_ids]["invert_level"]) / 2).to_numpy()
min_upstream_level = pd.Series(levels, index=node_ids, name="min_upstream_level")
min_upstream_level.index.name = "node_id"
static_data.add_series(node_type="Pump", series=min_upstream_level, fill_na=True)

## PUMP.flow_rate
gkw_gemaal_df = get_data_from_gkw(authority=authority, layers=["gemaal"])
flow_rate = gkw_gemaal_df.set_index("code")["maximalecapaciteit"] / 60  # m3/minuut to m3/s
flow_rate.name = "flow_rate"
static_data.add_series(node_type="Pump", series=flow_rate)


# %% correct some streefpeilen that are wrong
static_data.basin.loc[static_data.basin.node_id == 1134, "streefpeil"] = -0.1
static_data.basin.loc[static_data.basin.node_id == 1035, "streefpeil"] = -0.1
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


# %%
# defaults
static_data.write()

# write model
model.write(ribasim_toml)

# %%
