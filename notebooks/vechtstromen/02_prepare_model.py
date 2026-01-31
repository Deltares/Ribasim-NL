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
authority = "Vechtstromen"
short_name = "vechtstromen"

# %% files
ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_fix_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"

parameters_dir = cloud.joinpath(authority, "verwerkt/parameters")
parameters_dir.mkdir(parents=True, exist_ok=True)
static_data_xlsx = parameters_dir / "static_data_template.xlsx"
profiles_gpkg = parameters_dir / "profiles.gpkg"
link_geometries_gpkg = parameters_dir / "link_geometries.gpkg"

hydamo_gpkg = cloud.joinpath(authority, "verwerkt/4_ribasim/hydamo.gpkg")
profielpunt_shp = cloud.joinpath(authority, "verwerkt/1_ontvangen_data/nalevering_20240920/Meting_profielpunt_wvs.shp")
profiellijn_shp = cloud.joinpath(
    authority, "verwerkt/1_ontvangen_data/nalevering_20240920/meting_profiellijn_soort_profiel_wsv.shp"
)
peilgebieden_path = cloud.joinpath(
    authority, "verwerkt/1_ontvangen_data/aanvulling feb 24/Peilgebied_(met_peilregister_peilen).shp"
)
peilgebieden_RD = cloud.joinpath(
    authority, "verwerkt/1_ontvangen_data/downloads/peilgebieden_voormalig_velt_en_vecht.gpkg"
)
top10NL_gpkg = cloud.joinpath("Basisgegevens/Top10NL/top10nl_Compleet.gpkg")
peilregister_xlsx = cloud.joinpath(authority, "verwerkt/1_ontvangen_data/nalevering_20240920/Peilregister.xlsx")
feedback_xlsx = cloud.joinpath(
    authority, "verwerkt/1_ontvangen_data/Feedbackform_20250428/20250428_Feedback Formulier.xlsx"
)
waterinlaten = cloud.joinpath(authority, "verwerkt/1_ontvangen_data/aanvulling feb 24/Waterinlaten.shp")

cloud.synchronize(
    filepaths=[
        top10NL_gpkg,
        profielpunt_shp,
        profiellijn_shp,
        peilgebieden_path,
        peilregister_xlsx,
        feedback_xlsx,
        waterinlaten,
    ]
)

# %% init things
model = Model.read(ribasim_toml)
ribasim_toml = ribasim_dir.with_name(f"{authority}_prepare_model") / ribasim_toml.name
static_data = StaticData(model=model, xlsx_path=static_data_xlsx)

# %%
lines_gdf = gpd.read_file(hydamo_gpkg, layer="hydroobject")
network = Network(lines_gdf=gpd.read_file(hydamo_gpkg, layer="hydroobject"), tolerance=0.2)

# %%
profile_line_df = gpd.read_file(profiellijn_shp).to_crs(28992)
profile_line_df = profile_line_df[profile_line_df["SOORTPROFI"] == 7]
profile_point_df = gpd.read_file(profielpunt_shp).to_crs(28992)
profile_point_df.rename(columns={"METINGPROF": "profiellijnid", "CODEVOLGNU": "codevolgnummer"}, inplace=True)

profile_point_df = profile_point_df[(profile_point_df.geometry.z != 0) | (profile_point_df.geometry.z > 50)]
damo_profiles = DAMOProfiles(
    model=model,
    network=network,
    profile_line_df=profile_line_df,
    profile_point_df=profile_point_df,
    water_area_df=gpd.read_file(top10NL_gpkg, layer="top10nl_waterdeel_vlak"),
    profile_line_id_col="GLOBALID",
)


# %%
if not profiles_gpkg.exists():
    profiles_df = damo_profiles.process_profiles()
    profiles_df = profiles_df[profiles_df.bottom_level != 0]
    profiles_df = profiles_df[profiles_df.invert_level < 50]
    profiles_df.to_file(profiles_gpkg)
else:
    profiles_df = gpd.read_file(profiles_gpkg)


# %%

# fix link geometries
if link_geometries_gpkg.exists():
    link_geometries_df = gpd.read_file(link_geometries_gpkg).set_index("link_id")
    model.link.df.loc[link_geometries_df.index, "geometry"] = link_geometries_df["geometry"]
    model.link.df.loc[link_geometries_df.index, "meta_profielid_waterbeheerder"] = link_geometries_df[
        "meta_profielid_waterbeheerder"
    ]

else:
    fix_link_geometries(model, network, max_straight_line_ratio=3)
    add_link_profile_ids(model, profiles=profiles_df, id_col="profiel_id")
    model.link.df.reset_index().to_file(link_geometries_gpkg)
profiles_df.set_index("profiel_id", inplace=True)
# %%

# add link profiles

# %%
if "meta_code_waterbeheerder" not in model.basin.area.columns():
    model.basin.area.df["meta_code_waterbeheerder"] = pd.Series(dtype=str)

if "meta_streefpeil" in model.basin.area.columns():
    model.basin.area.df.drop(columns="meta_streefpeil", inplace=True)
model.basin.area.df["meta_streefpeil"] = pd.Series(dtype=float)

# %%
# OUTLET
static_data.reset_data_frame(node_type="Outlet")
# --- Load Peilregister data ---
peilregister_df = pd.read_excel(peilregister_xlsx, sheet_name="Blad2")
peilregister_df.drop_duplicates("KXXIDENT", inplace=True)
peilregister_df.set_index("KXXIDENT", inplace=True)
peilregister_df.index.name = "code"

# Extract min_upstream_level, excluding invalid placeholder
min_upstream_level_pr = peilregister_df["GGOR_ZPNAP"]
min_upstream_level_pr = min_upstream_level_pr[min_upstream_level_pr != -9.99]
min_upstream_level_pr.name = "min_upstream_level"

# --- Add Peilregister values to static_data ---
static_data.add_series(node_type="Outlet", series=min_upstream_level_pr, fill_na=True)

# From Peilenkaart
node_ids = static_data.outlet[static_data.outlet.min_upstream_level.isna()].node_id.to_numpy()
levels = []

peilgebieden_df = gpd.read_file(peilgebieden_path)
peilgebieden_df["PEILREG_ZP_FIRST"] = peilgebieden_df["PEILREG_ZP"].str.split(";").str[0].astype(float)

levels = []

for node_id in node_ids:
    node = model.outlet[node_id]
    tolerance = 5  # afstand voor zoeken bovenstrooms
    node_geometry = node.geometry

    line_to_node = model.link.df.set_index("to_node_id").at[node.node_id, "geometry"]
    distance_to_interpolate = line_to_node.length - tolerance
    if distance_to_interpolate < 0:
        distance_to_interpolate = 0

    containing_point = line_to_node.interpolate(distance_to_interpolate)
    peilgebieden_select_df = peilgebieden_df[peilgebieden_df.contains(containing_point)]

    if not peilgebieden_select_df.empty:
        peilgebied = peilgebieden_select_df.iloc[0]
        if peilgebied["PEILREG_ZP_FIRST"] != 0 and peilgebied["PEILREG_ZP_FIRST"] < 30:
            level = peilgebied["PEILREG_ZP_FIRST"]
        else:
            level = None
    else:
        level = None

    levels.append(level)


min_upstream_level = pd.Series(levels, index=node_ids, name="min_upstream_level")
min_upstream_level.index.name = "node_id"
static_data.add_series(node_type="Outlet", series=min_upstream_level, fill_na=True)

# From Peilenkaart voormalig RD
levels = []
peilgebieden_rd_df = gpd.read_file(peilgebieden_RD)
for node_id in node_ids:
    node = model.outlet[node_id]
    tolerance = 5  # afstand voor zoeken bovenstrooms
    node_id = node.node_id
    node_geometry = node.geometry
    line_to_node = model.link.df.set_index("to_node_id").at[node_id, "geometry"]
    distance_to_interpolate = line_to_node.length - tolerance
    if distance_to_interpolate < 0:
        distance_to_interpolate = 0

    containing_point = line_to_node.interpolate(distance_to_interpolate)
    peilgebieden_select_df = peilgebieden_rd_df[peilgebieden_rd_df.contains(containing_point)]
    if not peilgebieden_select_df.empty:
        peilgebied = peilgebieden_select_df.iloc[0]
        if peilgebied["GPGZMRPL"] != 0 and peilgebied["GPGZMRPL"] < 30:
            level = peilgebied["GPGZMRPL"]
        else:
            level = None
    else:
        level = None
    levels += [level]

min_upstream_level = pd.Series(levels, index=node_ids, name="min_upstream_level")
min_upstream_level.index.name = "node_id"
static_data.add_series(node_type="Outlet", series=min_upstream_level, fill_na=True)

# --- Load Feedback data ---
feedback_df = pd.read_excel(feedback_xlsx, sheet_name="Streefpeilen")
feedback_df.drop_duplicates("Basin node_id", inplace=True)
feedback_df.set_index("Basin node_id", inplace=True)
feedback_df.index.name = "node_id"

# Fill Streefpeil ZP with Maatg. kruinhoogte (m NAP) where missing
min_upstream_level_fb = feedback_df["Streefpeil ZP"]
fallback_values = feedback_df["Maatg. kruinhoogte (m NAP)"] + 0.25
min_upstream_level_fb = min_upstream_level_fb.fillna(fallback_values)
min_upstream_level_fb.name = "min_upstream_level"

# --- Add Feedback values to static_data (only where still missing) ---
static_data.add_series(node_type="Outlet", series=min_upstream_level_fb, fill_na=True)

# %%custum, zomerpeil Overijsselsche Vecht bij waterlevl boundary kloppen niet, uit DOD gehaald
static_data.outlet.loc[static_data.outlet.node_id == 1289, "min_upstream_level"] = 1.25
static_data.outlet.loc[static_data.outlet.node_id == 1290, "min_upstream_level"] = 1.25
# %% Bepaal min_upstream_level pumps from peilenkaart
static_data.reset_data_frame(node_type="Pump")
node_ids = static_data.pump.node_id
levels = []
for node_id in node_ids:
    node = model.pump[node_id]
    tolerance = 5  # afstand voor zoeken bovenstrooms
    node_geometry = node.geometry

    line_to_node = model.link.df.set_index("to_node_id").at[node.node_id, "geometry"]
    distance_to_interpolate = line_to_node.length - tolerance
    if distance_to_interpolate < 0:
        distance_to_interpolate = 0

    containing_point = line_to_node.interpolate(distance_to_interpolate)
    peilgebieden_select_df = peilgebieden_df[peilgebieden_df.contains(containing_point)]

    if not peilgebieden_select_df.empty:
        peilgebied = peilgebieden_select_df.iloc[0]
        if peilgebied["PEILREG_ZP_FIRST"] != 0 and peilgebied["PEILREG_ZP_FIRST"] < 30:
            level = peilgebied["PEILREG_ZP_FIRST"]
        else:
            level = None
    else:
        level = None

    levels.append(level)

min_upstream_level = pd.Series(levels, index=node_ids, name="min_upstream_level")
min_upstream_level.index.name = "node_id"
static_data.add_series(node_type="Pump", series=min_upstream_level)


# %% Bepaal de basin streefpeilen door minimale upstream level van stuwen en gemalen. DUikers in basin worden gelijk gezet aan basinpeil

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
static_data.add_series(node_type="Basin", series=streefpeil, fill_na=True)

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


# %% Laatste fallback optie DAMO profielen gebruiken
# DAMO-profielen bepalen voor outlets wanneer min_upstream_level nodata
# Fallback DAMO profielen voor outlets
node_ids_outlets = static_data.outlet[static_data.outlet.min_upstream_level.isna()].node_id.to_numpy()
profile_ids_outlets = model.link.df.set_index("to_node_id").loc[node_ids_outlets]["meta_profielid_waterbeheerder"]
levels_outlets = (profiles_df.loc[profile_ids_outlets]["invert_level"] - 1).to_numpy()
min_upstream_level_outlets = pd.Series(levels_outlets, index=node_ids_outlets, name="min_upstream_level")
min_upstream_level_outlets.index.name = "node_id"
static_data.add_series(node_type="Outlet", series=min_upstream_level_outlets, fill_na=False)

# DAMO-profielen bepalen voor pumps wanneer min_upstream_level nodata
node_ids_pumps = static_data.pump[static_data.pump.min_upstream_level.isna()].node_id.to_numpy()
profile_ids_pumps = model.link.df.set_index("to_node_id").loc[node_ids_pumps]["meta_profielid_waterbeheerder"]
# Veel bottom_levels kloppen niet, dus we nemen de invert_level -1
levels_pumps = (profiles_df.loc[profile_ids_pumps]["invert_level"] - 1).to_numpy()
min_upstream_level_pumps = pd.Series(levels_pumps, index=node_ids_pumps, name="min_upstream_level")
min_upstream_level_pumps.index.name = "node_id"
static_data.add_series(node_type="Pump", series=min_upstream_level_pumps, fill_na=False)

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
static_data.add_series(node_type="Basin", series=streefpeil, fill_na=False)

# %%
## PUMP.flow_rate
gkw_gemaal_df = get_data_from_gkw(authority=authority, layers=["gemaal"])
flow_rate = gkw_gemaal_df.set_index("code")["maximalecapaciteit"] / 60  # m3/minuut to m3/s
flow_rate.name = "flow_rate"
static_data.add_series(node_type="Pump", series=flow_rate)

# %%
# update all nodata streefpeilen with their profile_ids and levels
node_ids = static_data.basin[static_data.basin.streefpeil.isna()].node_id.to_numpy()
profile_ids = [damo_profiles.get_profile_id(node_id) for node_id in node_ids]
levels = (profiles_df.loc[profile_ids]["invert_level"] - 1).to_numpy()

# # update static_data
profielid = pd.Series(profile_ids, index=pd.Index(node_ids, name="node_id"), name="profielid")
static_data.add_series(node_type="Basin", series=profielid, fill_na=False)
streefpeil = pd.Series(levels, index=pd.Index(node_ids, name="node_id"), name="streefpeil")
static_data.add_series(node_type="Basin", series=streefpeil, fill_na=False)

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

# %% some customs

# %% Set waterinlaten
# --- Load Shapefile ---
waterinlaten_gdf = gpd.read_file(waterinlaten)

# --- Load Waterinlaten data ---
waterinlaten_gdf.drop_duplicates("IDENT", inplace=True)
waterinlaten_gdf.set_index("IDENT", inplace=True)
waterinlaten_gdf.index.name = "code"

# --- Filter data for Outlet ---
type_outlet = waterinlaten_gdf["IWS_WATERB"].copy()
type_outlet.name = "categorie"
type_outlet = type_outlet.replace({"Inlaat (extern)": "Inlaat", "Inlaat (intern)": "Inlaat"})
valid_values = type_outlet[~type_outlet.str.contains("Aflaat", na=False)]
static_data.add_series(node_type="Outlet", series=valid_values, fill_na=False)

# %% aanvoer of aanvoergemaal
# --- Load Feedback data ---
feedback_df = pd.read_excel(feedback_xlsx, sheet_name="Functie gemalen")
feedback_df.drop_duplicates("Pump node_id", inplace=True)
feedback_df.set_index("Pump node_id", inplace=True)
feedback_df.index.name = "code"

# Fill Streefpeil ZP with Maatg. kruinhoogte (m NAP) where missing
type_gemaal = feedback_df["Aanvoer / afvoer?"]
type_gemaal.name = "categorie"
type_gemaal = type_gemaal.apply(lambda x: x.capitalize() if isinstance(x, str) else x)
valid_values = type_gemaal.dropna()
static_data.add_series(node_type="Pump", series=valid_values, fill_na=False)
# %% some customs
model.remove_node(2297)

# # koppelen
ws_grenzen_path = cloud.joinpath("Basisgegevens/RWS_waterschaps_grenzen/waterschap.gpkg")
RWS_grenzen_path = cloud.joinpath("Basisgegevens/RWS_waterschaps_grenzen/Rijkswaterstaat.gpkg")
assign = AssignAuthorities(
    ribasim_model=model,
    waterschap=authority,
    ws_grenzen_path=ws_grenzen_path,
    RWS_grenzen_path=RWS_grenzen_path,
    custom_nodes={21: "Buitenland", 34: "Buitenland", 2250: "Buitenland", 2265: "Buitenland", 2268: "Buitenland"},
)
model = assign.assign_authorities()

# %%
# write
static_data.write()

model.write(ribasim_toml)

# %%
