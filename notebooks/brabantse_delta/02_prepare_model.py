# %%
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
authority = "BrabantseDelta"
short_name = "wbd"

# %% files
ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_fix_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"

parameters_dir = static_data_xlsx = cloud.joinpath(authority, "verwerkt", "parameters")
static_data_xlsx = parameters_dir / "static_data_template.xlsx"
profiles_gpkg = parameters_dir / "profiles.gpkg"
link_geometries_gpkg = parameters_dir / "link_geometries.gpkg"

hydamo_gpkg = cloud.joinpath(authority, "verwerkt/4_ribasim/hydamo.gpkg")
damo_profiles_gpkg = cloud.joinpath(authority, "verwerkt/profielen.gpkg")
peilgebieden_path = cloud.joinpath(authority, "verwerkt/4_ribasim/hydamo.gpkg")
top10NL_gpkg = cloud.joinpath("Basisgegevens", "Top10NL", "top10nl_Compleet.gpkg")
network_gpkg = cloud.joinpath(authority, "verwerkt", "network.gpkg")
sturing_xlsx = cloud.joinpath(
    authority, "verwerkt/1_ontvangen_data/sturing_gemalen_stuwen_22-5-2022/sturingGemalenStuwen_v2.xlsx"
)

cloud.synchronize(filepaths=[peilgebieden_path, top10NL_gpkg])

# %% init things
model = Model.read(ribasim_toml)
ribasim_toml = ribasim_dir.with_name(f"{authority}_prepare_model") / ribasim_toml.name
static_data = StaticData(model=model, xlsx_path=static_data_xlsx)


# %%
# prepare DAMO profiles and network
lines_gdf = gpd.read_file(hydamo_gpkg, layer="hydroobject")

if not network_gpkg.exists():
    network = Network(lines_gdf=lines_gdf, tolerance=0.2)
    network.to_file(network_gpkg)
else:
    network = Network.from_network_gpkg(network_gpkg)

damo_profiles = DAMOProfiles(
    model=model,
    network=network,
    profile_line_df=gpd.read_file(damo_profiles_gpkg, layer="profiellijn"),
    profile_point_df=gpd.read_file(damo_profiles_gpkg, layer="profielpunt"),
    water_area_df=gpd.read_file(top10NL_gpkg, layer="top10nl_waterdeel_vlak"),
    profile_line_id_col="code",
)
if not profiles_gpkg.exists():
    profiles_df = damo_profiles.process_profiles()
    profiles_df.to_file(profiles_gpkg)
else:
    profiles_df = gpd.read_file(profiles_gpkg)

# %%

# fix link geometries
if link_geometries_gpkg.exists():
    link_geometries_df = gpd.read_file(link_geometries_gpkg).set_index("link_id")
    model.link.df.loc[link_geometries_df.index, "geometry"] = link_geometries_df["geometry"]
    if "meta_profielid_waterbeheerder" in link_geometries_df.columns:
        model.link.df.loc[link_geometries_df.index, "meta_profielid_waterbeheerder"] = link_geometries_df[
            "meta_profielid_waterbeheerder"
        ]
    profiles_df = gpd.read_file(profiles_gpkg).set_index("profiel_id")
else:
    profiles_df = damo_profiles.process_profiles()
    profiles_df.to_file(profiles_gpkg)
    profiles_df.set_index("profiel_id", inplace=True)
    add_link_profile_ids(model, profiles=damo_profiles, id_col="code")
    fix_link_geometries(model, network)
    model.link.df.reset_index().to_file(link_geometries_gpkg)


# %%
if "meta_code_waterbeheerder" not in model.basin.area.columns():
    model.basin.area.df["meta_code_waterbeheerder"] = pd.Series(dtype=str)

if "meta_streefpeil" in model.basin.area.columns():
    model.basin.area.df.drop(columns="meta_streefpeil", inplace=True)
model.basin.area.df["meta_streefpeil"] = pd.Series(dtype=float)

# %%
static_data.reset_data_frame(node_type="Pump")
static_data.reset_data_frame(node_type="Outlet")

# --- Corrigeer 'GEM'-prefix in Pump-codekolom ---
pump_df = static_data.pump.copy()

if "code" in pump_df.columns:
    pump_df["code"] = pump_df["code"].str.replace("^GEM_", "", regex=True)
    static_data.pump = pump_df


# %% Bewerk peilenkaart
peilgebieden_path_editted = peilgebieden_path.parent.joinpath("peilgebieden_bewerkt.gpkg")
peilgebieden_df = gpd.read_file(peilgebieden_path, layer="peilgebiedpraktijk")

# fill with zomerpeil
peilgebieden_df["streefpeil"] = peilgebieden_df["WS_ZOMERPEIL"]

# if nodata, fill with vastpeil
mask = peilgebieden_df["streefpeil"].isna()
peilgebieden_df.loc[mask, "streefpeil"] = peilgebieden_df[mask]["WS_VAST_PEIL"]

# if nodata and onderpeil is nodata, fill with bovenpeil
mask = peilgebieden_df["streefpeil"].isna()
peilgebieden_df.loc[mask, "streefpeil"] = peilgebieden_df[mask]["WS_MAXIMUM"]
peilgebieden_df[peilgebieden_df["streefpeil"].notna()].to_file(peilgebieden_path_editted)

# %%# OUTLET en Pump frome excel sturingsbestand
# --- Load Peilregister data ---
peilregister_df = pd.read_excel(sturing_xlsx, sheet_name="Samengevoegd2")
peilregister_df.drop_duplicates("code_verbeterd", inplace=True)
peilregister_df.set_index("code_verbeterd", inplace=True)
peilregister_df.index.name = "code"

# Extract min_upstream_level, excluding invalid placeholder
min_upstream_level = peilregister_df["Zomerpeil "]
max_cap = pd.to_numeric(peilregister_df["Tabel2.Max totale capaciteit [m3/s]"], errors="coerce")
min_upstream_level = min_upstream_level[min_upstream_level != -9.99]
min_upstream_level.name = "min_upstream_level"
max_cap.name = "flow_rate"

# --- Add Peilregister values to static_data ---
static_data.add_series(node_type="Outlet", series=min_upstream_level, fill_na=True)
static_data.add_series(node_type="Outlet", series=max_cap, fill_na=True)
static_data.add_series(node_type="Pump", series=min_upstream_level, fill_na=True)
static_data.add_series(node_type="Pump", series=max_cap, fill_na=True)


# %% Add peilen van peilenkaart to pumps en outlets
def add_min_upstream_level_from_peilenkaart(node_type):
    if node_type == "Outlet":
        node_df = static_data.outlet[static_data.outlet.min_upstream_level.isna()]
    else:
        node_df = getattr(static_data, node_type.lower())

    node_ids = node_df.node_id.to_numpy()
    levels = []

    for node_id in node_ids:
        node = getattr(model, node_type.lower())[node_id]
        tolerance = 30  # afstand voor zoeken bovenstrooms

        line_to_node = model.link.df.set_index("to_node_id").at[node.node_id, "geometry"]
        distance_to_interpolate = line_to_node.length - tolerance
        if distance_to_interpolate < 0:
            distance_to_interpolate = 0

        containing_point = line_to_node.interpolate(distance_to_interpolate)
        peilgebieden_select_df = peilgebieden_df[peilgebieden_df.contains(containing_point)]

        if not peilgebieden_select_df.empty:
            peilgebied = peilgebieden_select_df.iloc[0]
            if peilgebied["streefpeil"] != 0 and peilgebied["streefpeil"] < 30:
                level = peilgebied["streefpeil"]
            else:
                level = None
        else:
            level = None

        levels.append(level)

    series = pd.Series(levels, index=node_ids, name="min_upstream_level")
    series.index.name = "node_id"
    static_data.add_series(node_type=node_type, series=series, fill_na=True)


# Aanroepen voor beide types
add_min_upstream_level_from_peilenkaart("Outlet")
add_min_upstream_level_from_peilenkaart("Pump")

# %% Bepaal de basin streefpeilen door minimale upstream level van stuwen en gemalen. Duikers in basin worden gelijk gezet aan basinpeil
static_data.reset_data_frame(node_type="Basin")
node_ids = static_data.basin[static_data.basin.streefpeil.isna()].node_id.to_numpy()
ds_node_ids = []

for node_id in node_ids:
    try:
        ds = model.downstream_node_id(int(node_id))
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
# alle pumps en outlets zonder duikers
non_kdu_values = streefpeil[~streefpeil["code"].str.startswith("KDU") | (streefpeil["source"] == "pump")]
non_kdu_first = non_kdu_values.groupby(level=0).first()
all_first = streefpeil.groupby(level=0).first()

# Reset min_upstream level voor duikers op basis van streefpeilen
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
static_data.add_series(node_type="Basin", series=streefpeil, fill_na=False)

# Update stuwen met nodata op basis van basin streefpeil
missing_min_level_outlets = static_data.outlet[static_data.outlet.min_upstream_level.isna()]
downstream_basin_ids = model.link.df.set_index("to_node_id").loc[missing_min_level_outlets.node_id].from_node_id
downstream_basin_levels = static_data.basin.set_index("node_id").reindex(downstream_basin_ids)["streefpeil"]
downstream_basin_levels.index = missing_min_level_outlets.node_id
downstream_basin_levels.name = "min_upstream_level"
static_data.add_series(node_type="Outlet", series=downstream_basin_levels.dropna(), fill_na=False)

# Update pumps met nodata op basis van basin streefpeil
missing_min_level_pumps = static_data.pump[static_data.pump.min_upstream_level.isna()]
downstream_basin_ids = model.link.df.set_index("to_node_id").loc[missing_min_level_pumps.node_id].from_node_id
downstream_basin_levels = static_data.basin.set_index("node_id").reindex(downstream_basin_ids)["streefpeil"]
downstream_basin_levels.index = missing_min_level_pumps.node_id
downstream_basin_levels.name = "min_upstream_level"
static_data.add_series(node_type="Pump", series=downstream_basin_levels.dropna(), fill_na=False)


# %%
# Outlet
# from DAMO profiles
node_ids = static_data.outlet[static_data.outlet.min_upstream_level.isna()].node_id.to_numpy()
profile_ids = [
    model.link.df[model.link.df.to_node_id == node_id].iloc[0]["meta_profielid_waterbeheerder"] for node_id in node_ids
]
levels = (
    profiles_df.loc[profile_ids]["bottom_level"]
    + (profiles_df.loc[profile_ids]["invert_level"] - profiles_df.loc[profile_ids]["bottom_level"]) / 3
).to_numpy()

min_upstream_level = pd.Series(levels, index=node_ids, name="min_upstream_level")
min_upstream_level.index.name = "node_id"
static_data.add_series(node_type="Outlet", series=min_upstream_level, fill_na=True)


# %%
# PUMP
# from DAMO profiles
node_ids = static_data.pump[static_data.pump.min_upstream_level.isna()].node_id.to_numpy()
profile_ids = [
    model.link.df[model.link.df.to_node_id == node_id].iloc[0]["meta_profielid_waterbeheerder"] for node_id in node_ids
]
levels = (
    profiles_df.loc[profile_ids]["bottom_level"]
    + (profiles_df.loc[profile_ids]["invert_level"] - profiles_df.loc[profile_ids]["bottom_level"]) / 3
).to_numpy()

min_upstream_level = pd.Series(levels, index=node_ids, name="min_upstream_level")
min_upstream_level.index.name = "node_id"
static_data.add_series(node_type="Pump", series=min_upstream_level, fill_na=True)


# %%

# Update de basins op basis van hun downstream outlet (outlets én pumps)
node_ids = static_data.basin[static_data.basin.streefpeil.isna()].node_id.to_numpy()
ds_node_ids = (model.downstream_node_id(i) for i in node_ids)
ds_node_ids = [i.to_list() if isinstance(i, pd.Series) else [i] for i in ds_node_ids]
ds_node_ids = pd.Series(ds_node_ids, index=node_ids).explode()

ds_levels = pd.concat([static_data.basin, static_data.outlet, static_data.pump], ignore_index=True).set_index(
    "node_id"
)["min_upstream_level"]
ds_levels.dropna(inplace=True)
ds_levels = ds_levels[ds_levels.index.isin(ds_node_ids)]
ds_node_ids = ds_node_ids[ds_node_ids.isin(ds_levels.index)]

levels = ds_node_ids.apply(lambda x: ds_levels[x])
streefpeil = levels.groupby(levels.index).min()
streefpeil.name = "streefpeil"
streefpeil.index.name = "node_id"
static_data.add_series(node_type="Basin", series=streefpeil, fill_na=True)

# %%
## PUMP.flow_rate
gkw_gemaal_df = get_data_from_gkw(authority=authority, layers=["gemaal"])
flow_rate = gkw_gemaal_df.set_index("code")["maximalecapaciteit"] / 60  # m3/minuut to m3/s
flow_rate.name = "flow_rate"
static_data.add_series(node_type="Pump", series=flow_rate)

# %%
# get all nodata streefpeilen with their profile_ids and levels
node_ids = static_data.basin[static_data.basin.streefpeil.isna()].node_id.to_numpy()
profile_ids = [damo_profiles.get_profile_id(node_id) for node_id in node_ids]
levels = (
    profiles_df.loc[profile_ids]["bottom_level"]
    + (profiles_df.loc[profile_ids]["invert_level"] - profiles_df.loc[profile_ids]["bottom_level"]) / 3
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
    custom_nodes={2030: "Buitenland", 2073: "Buitenland", 2093: "Buitenland"},
)
model = assign.assign_authorities()

# %%

# write
static_data.write()

model.write(ribasim_toml)

# %%
