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

# %%
bbox = None
# init classes
network = Network(lines_gdf=gpd.read_file(venv_hydamo_gpkg, layer="hydroobject", bbox=bbox))
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
static_data = StaticData(model=model, xlsx_path=static_data_xlsx)

# %%
# Edges
network = Network(lines_gdf=gpd.read_file(venv_hydamo_gpkg, layer="hydroobject"))
damo_profiles = DAMOProfiles(
    model=model,
    network=network,
    profile_line_df=gpd.read_file(venv_hydamo_gpkg, layer="profiellijn"),
    profile_point_df=gpd.read_file(venv_hydamo_gpkg, layer="profielpunt"),
    water_area_df=gpd.read_file(top10NL_gpkg, layer="top10nl_waterdeel_vlak"),
    profile_line_id_col="code",
)

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
    node = model.pump[node_id]  # 🔹 Nu voor PUMP i.p.v. OUTLET
    peilgebieden_df = gpd.read_file(peilgebieden_path)
    tolerance = 10  # 🔹 Afstand voor zoeken bovenstrooms
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

    levels.append(level)  # 🔹 Correcte append-methode

# Maak een pandas Series van de levels
min_upstream_level = pd.Series(levels, index=node_ids, name="min_upstream_level")
min_upstream_level.index.name = "node_id"

# 🔹 Voeg de data toe aan "Pump"
static_data.add_series(node_type="Pump", series=min_upstream_level)
# %%
# Alternatieve methode voor bepalen peil basins: Reset data voor "Basin"
static_data.reset_data_frame(node_type="Basin")

# Haal de node_ids op waarvoor het streefpeil nog niet bekend is
node_ids = static_data.basin[static_data.basin.streefpeil.isna()].node_id.to_numpy()

# Zoek de downstream nodes (outlets) van deze Basin-nodes
ds_node_ids = (model.downstream_node_id(i) for i in node_ids)
ds_node_ids = [i.to_list() if isinstance(i, pd.Series) else [i] for i in ds_node_ids]
ds_node_ids = pd.Series(ds_node_ids, index=node_ids).explode()

# Filter alleen de nodes die in de outlet zitten
ds_node_ids = ds_node_ids[ds_node_ids.isin(static_data.outlet.node_id)]

# Haal de min_upstream_level en code op
streefpeil = static_data.outlet.set_index("node_id").loc[ds_node_ids.to_numpy(), ["min_upstream_level", "code"]]

# Zet de index van streefpeil gelijk aan de node_ids van de basins
streefpeil.index = ds_node_ids.index

# Drop eventuele NaN waarden
streefpeil.dropna(inplace=True)

# Sorteer per groep zodat de kleinste 'min_upstream_level' bovenaan komt
streefpeil = streefpeil.sort_values(by=["basin_node_id", "min_upstream_level"])

# Groepeer per basin_node_id
for basin_node_id, group in streefpeil.groupby("basin_node_id"):
    # Zoek de rijen voor KDU en KST in de groep
    kdu_row = group[group["code"] == "KDU"]
    kst_row = group[group["code"] == "KST"]

    # Als zowel KDU als KST in dezelfde basin_node_id groep aanwezig zijn
    if not kdu_row.empty and not kst_row.empty:
        kdu_value = kdu_row["min_upstream_level"].values[0]
        kst_value = kst_row["min_upstream_level"].values[0]

        # Als KDU lager is dan KST, vervang de KDU waarde met de KST waarde
        if kdu_value < kst_value:
            streefpeil.loc[streefpeil["basin_node_id"] == basin_node_id, "min_upstream_level"] = kst_value

# Print de gecorrigeerde streefpeil tabel
print(streefpeil)

# Voeg de streefpeilen toe aan "Basin" (alleen voor de basins waar streefpeil nog niet is ingevuld)
static_data.add_series(node_type="Basin", series=streefpeil["min_upstream_level"], fill_na=True)


# BASIN
static_data.reset_data_frame(node_type="Basin")

# fill streefpeil from ds min_upstream_level
node_ids = static_data.basin[static_data.basin.streefpeil.isna()].node_id.to_numpy()
node_ids_outlet = static_data.outlet.node_id.to_numpy()

ds_node_ids = (model.downstream_node_id(i) for i in node_ids)
ds_node_ids = [i.to_list() if isinstance(i, pd.Series) else [i] for i in ds_node_ids]
ds_node_ids = pd.Series(ds_node_ids, index=node_ids).explode()

ds_node_ids = ds_node_ids[ds_node_ids.isin(static_data.outlet.node_id)]
streefpeil = static_data.outlet.set_index("node_id").loc[ds_node_ids.to_numpy(), ["min_upstream_level", "code"]]

streefpeil.index = ds_node_ids.index

streefpeil.dropna(inplace=True)

# Sorteer per groep zodat de kleinste 'min_upstream_level' bovenaan staat
streefpeil = streefpeil.sort_index().sort_values(by="min_upstream_level", inplace=False)


# Stap 1: Filter de rijen die niet beginnen met 'KDU'
non_kdu_values = streefpeil[~streefpeil["code"].str.startswith("KDU")]

# Stap 2: Groepeer de niet-KDU waarden en haal de eerste waarde per groep
non_kdu_first = non_kdu_values.groupby(level=0).first()

# Stap 3: Groepeer alle waarden (ook KDU) en haal de eerste waarde per groep
all_first = streefpeil.groupby(level=0).first()

# Stap 4: Combineer de niet-KDU en KDU waarden: kies de niet-KDU waarde als die er is, anders de KDU waarde
streefpeil = non_kdu_first.combine_first(all_first)

# Zet de juiste index- en kolomnamen
# treefpeil.rename(columns={"min_upstream_level": "streefpeil"}, inplace=True)
streefpeil = streefpeil["min_upstream_level"]  # Pak alleen de kolom, geen DF
# Selecteer alleen de kolom 'min_upstream_level' en hernoem naar 'streefpeil'
# streefpeil = streefpeil["streefpeil"].rename("streefpeil")
streefpeil.index.name = "node_id"
streefpeil.name = "streefpeil"


streefpeil = streefpeil.astype("float64")  # Zorg dat het type correct is
streefpeil.index = streefpeil.index.astype("int64")  # Zorg dat de index int is

# Voeg de data toe als een Series
static_data.add_series(node_type="Basin", series=streefpeil, fill_na=True)

# %%
# Reset Basin data
static_data.reset_data_frame(node_type="Basin")

# Haal de node_ids van basins waarvoor het streefpeil onbekend is
node_ids = static_data.basin[static_data.basin.streefpeil.isna()].node_id.to_numpy()

# Zoek de downstream nodes (kunnen zowel outlets als pumps zijn)
ds_node_ids = (model.downstream_node_id(i) for i in node_ids)
ds_node_ids = [i.to_list() if isinstance(i, pd.Series) else [i] for i in ds_node_ids]
ds_node_ids = pd.Series(ds_node_ids, index=node_ids).explode()

# Filter alleen de downstream nodes die in outlets zitten
valid_outlet_nodes = ds_node_ids[ds_node_ids.isin(static_data.outlet.node_id)]
valid_pump_nodes = ds_node_ids[ds_node_ids.isin(static_data.pump.node_id)]

# Streefpeil ophalen voor outlets en de correcte index behouden
outlet_streefpeil = static_data.outlet.set_index("node_id").loc[valid_outlet_nodes.to_numpy(), ["min_upstream_level"]]
outlet_streefpeil.index = valid_outlet_nodes.index  # Herstel de basin-node_id als index

# Streefpeil ophalen voor pumps en de correcte index behouden
pump_streefpeil = static_data.pump.set_index("node_id").loc[valid_pump_nodes.to_numpy(), ["min_upstream_level"]]
pump_streefpeil.index = valid_pump_nodes.index  # Herstel de basin-node_id als index

# Merge beide datasets en neem de kleinste waarde per basin-node_id
streefpeil = pd.concat([outlet_streefpeil, pump_streefpeil]).dropna()
streefpeil = streefpeil.groupby(level=0).min()

# Correcte namen instellen
streefpeil.index.name = "node_id"
streefpeil.columns = ["streefpeil"]  # Zorg dat de kolomnaam klopt

# Voeg de data toe aan static_data
static_data.add_series(node_type="Basin", series=streefpeil["streefpeil"], fill_na=True)


# %%
import pandas as pd

# Reset data voor "Basin"
static_data.reset_data_frame(node_type="Basin")

# Haal node_ids op waarvoor streefpeil nog niet bekend is
node_ids = static_data.basin[static_data.basin.streefpeil.isna()].node_id.to_numpy()

# Zoek de downstream nodes (outlets) van deze Basin-nodes
ds_node_ids = (model.downstream_node_id(i) for i in node_ids)
ds_node_ids = [i.to_list() if isinstance(i, pd.Series) else [i] for i in ds_node_ids]
ds_node_ids = pd.Series(ds_node_ids, index=node_ids).explode()

# ✅ Dit is de juiste mapping van Basin → Outlet
basin_to_outlet_map = ds_node_ids[ds_node_ids.isin(static_data.outlet.node_id)]

# Haal min_upstream_level en code op
streefpeil = static_data.outlet.set_index("node_id").loc[basin_to_outlet_map.to_numpy(), ["min_upstream_level", "code"]]

# ✅ Gebruik outlet-node_id’s als index, niet de Basin-node_id’s!
streefpeil.index = basin_to_outlet_map.values
streefpeil.dropna(inplace=True)

# Sorteer per groep zodat de kleinste 'min_upstream_level' bovenaan staat
streefpeil = streefpeil.sort_values(by="min_upstream_level")

# Stap 1: Filter non-KDU waardes
non_kdu_values = streefpeil[~streefpeil["code"].str.startswith("KDU")]

# Stap 2: Neem de eerste non-KDU waarde per groep
non_kdu_first = non_kdu_values.groupby(level=0).first()

# Stap 3: Haal eerste waarde per groep (inclusief KDU)
all_first = streefpeil.groupby(level=0).first()

# Stap 4: Combineer: gebruik non-KDU als mogelijk, anders de eerste KDU
streefpeil = non_kdu_first.combine_first(all_first)

# ✅ **Gebruik outlet-node_id’s om KDU-outlets te vinden!**
kdu_nodes = streefpeil[streefpeil["code"].str.startswith("KDU")].copy()

# ✅ Gebruik de outlet-node_id’s als index, niet de Basin-node_id’s!
kdu_nodes.index = basin_to_outlet_map.loc[basin_to_outlet_map.isin(kdu_nodes.index)].values

# ✅ Overschrijf de min_upstream_level voor KDU-outlet nodes
kdu_nodes["min_upstream_level"] = streefpeil.loc[kdu_nodes.index, "min_upstream_level"]

# Pak alleen de min_upstream_level en hernoem naar 'streefpeil'
streefpeil = streefpeil["min_upstream_level"].rename("streefpeil")
streefpeil.index.name = "node_id"

# ✅ Voeg de juiste streefpeilen toe aan "Basin"
static_data.add_series(node_type="Basin", series=streefpeil, fill_na=True)

# ✅ KDU-outlet node_id’s correct overschrijven
min_upstream_level = kdu_nodes["min_upstream_level"]
min_upstream_level.index.name = "node_id"

static_data.add_series(node_type="Outlet", series=min_upstream_level, fill_na=True)


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
