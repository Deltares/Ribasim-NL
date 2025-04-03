# %%
import geopandas as gpd
import pandas as pd

from ribasim_nl import CloudStorage, Model, Network
from ribasim_nl.gkw import get_data_from_gkw
from ribasim_nl.link_geometries import fix_link_geometries
from ribasim_nl.parametrization.static_data_xlsx import StaticData

cloud = CloudStorage()
authority = "StichtseRijnlanden"
short_name = "hdsr"

# %% files
ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_fix_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"

parameters_dir = static_data_xlsx = cloud.joinpath(authority, "verwerkt", "parameters")
static_data_xlsx = parameters_dir / "static_data_template.xlsx"
profiles_gpkg = parameters_dir / "profiles.gpkg"
link_geometries_gpkg = parameters_dir / "link_geometries.gpkg"
hydroobject_gpkg = cloud.joinpath(authority, "verwerkt/4_ribasim/hydroobject.gpkg")
peilgebieden_path = cloud.joinpath(authority, "verwerkt/4_ribasim/peilgebieden.gpkg")
top10NL_gpkg = cloud.joinpath("Basisgegevens", "Top10NL", "top10nl_Compleet.gpkg")

cloud.synchronize(filepaths=[peilgebieden_path, top10NL_gpkg])

# %% init things
model = Model.read(ribasim_toml)
ribasim_toml = ribasim_dir.with_name(f"{authority}_prepare_model") / ribasim_toml.name
network = Network(lines_gdf=gpd.read_file(hydroobject_gpkg, layer="hydroobject"))
# damo_profiles = DAMOProfiles(
#     model=model,
#     network=network,
#     profile_line_df=gpd.read_file(hydamo_wm_gpkg, layer="profiellijn"),
#     profile_point_df=gpd.read_file(hydamo_wm_gpkg, layer="profielpunt"),
#     water_area_df=gpd.read_file(top10NL_gpkg, layer="top10nl_waterdeel_vlak"),
# )

static_data = StaticData(model=model, xlsx_path=static_data_xlsx)


# %%

# fix link geometries
if link_geometries_gpkg.exists():
    link_geometries_df = gpd.read_file(link_geometries_gpkg).set_index("edge_id")
    model.edge.df.loc[link_geometries_df.index, "geometry"] = link_geometries_df["geometry"]
    # model.edge.df.loc[link_geometries_df.index, "meta_profielid_waterbeheerder"] = link_geometries_df[
    #     "meta_profielid_waterbeheerder"
    # ]
else:
    fix_link_geometries(model, network)
    # add_link_profile_ids(model, profiles=damo_profiles)
    model.edge.df.reset_index().to_file(link_geometries_gpkg)

# %%

# # add streefpeilen
peilgebieden_path_editted = peilgebieden_path.with_name(f"{peilgebieden_path.stem}_bewerkt.gpkg")
if not peilgebieden_path_editted.exists():
    df = gpd.read_file(peilgebieden_path)

    # LBW_A_069 has 999 bovenpeil
    mask = df["WS_BP"] == 999
    df.loc[mask, "WS_BP"] = pd.NA

    # fill with zomerpeil
    df["streefpeil"] = df["WS_ZP"]

    # if nodata, fill with vastpeil
    mask = df["streefpeil"].isna()
    df.loc[mask, "streefpeil"] = df[mask]["WS_VP"]

    # if nodata and onderpeil is nodata, fill with bovenpeil
    mask = df["streefpeil"].isna()
    df.loc[mask, "streefpeil"] = df[mask]["WS_BP"]

    # if nodata and onderpeil is nodata, fill with onderpeil
    mask = df["streefpeil"].isna()
    df.loc[mask, "streefpeil"] = df[mask]["WS_OP"]

    for basin_id, to_basin_ids in ((2016, [1963, 2015]), (1562, [1563, 1432])):
        df.loc[df["node_id"].isin(to_basin_ids), "streefpeil"] = df.set_index("node_id").at[basin_id, "streefpeil"]

    # write
    df.to_file(peilgebieden_path_editted)


# add_streefpeil(
#     model=model,
#     peilgebieden_path=peilgebieden_path_editted,
#     layername=None,
#     target_level="streefpeil",
#     code="WS_PGID",
# )

# migrate peilen
# for basin_id, to_basin_ids in ((2016, [1963, 2015]), (1562, [1563, 1432])):
#     model.basin.area.df.loc[model.basin.area.df.node_id.isin(to_basin_ids), "meta_streefpeil"] = (
#         model.basin.area.df.set_index("node_id").at[basin_id, "meta_streefpeil"]
#     )

#     model.basin.area.df.loc[model.basin.area.df.node_id.isin(to_basin_ids), "meta_code_waterbeheerder"] = (
#          model.basin.area.df.set_index("node_id").at[basin_id, "meta_code_waterbeheerder"]
#     )


# %% Bepaal min_upstream_level Outlet`
static_data.reset_data_frame(node_type="Outlet")
node_ids = static_data.outlet.node_id
levels = []
for node_id in node_ids:
    node = model.outlet[node_id]
    peilgebieden_df = gpd.read_file(peilgebieden_path_editted)
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

        # Check in volgorde van prioriteit
        for col in ["WS_ZP", "WS_VP", "WS_BP", "WS_OP"]:
            if peilgebied[col] < 30:
                level = peilgebied[col]
                break  # Stop bij de eerste geldige waarde

    levels.append(level)

min_upstream_level = pd.Series(levels, index=node_ids, name="min_upstream_level")
min_upstream_level.index.name = "node_id"

static_data.add_series(node_type="Outlet", series=min_upstream_level)


# %% Bepaal min_upstream_level pumps
static_data.reset_data_frame(node_type="Pump")
node_ids = static_data.pump.node_id
levels = []

for node_id in node_ids:
    node = model.pump[node_id]  # 🔹 Nu voor PUMP i.p.v. OUTLET
    peilgebieden_df = gpd.read_file(peilgebieden_path_editted)
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

        # Check in volgorde van prioriteit
        for col in ["WS_ZP", "WS_VP", "WS_BP", "WS_OP"]:
            if peilgebied[col] < 30:
                level = peilgebied[col]
                break  # Stop bij de eerste geldige waarde

    levels.append(level)

# Maak een pandas Series van de levels
min_upstream_level = pd.Series(levels, index=node_ids, name="min_upstream_level")
min_upstream_level.index.name = "node_id"

# 🔹 Voeg de data toe aan "Pump"
static_data.add_series(node_type="Pump", series=min_upstream_level)


# %% Bepaal de basin streefpeilen
static_data.reset_data_frame(node_type="Basin")

# fill streefpeil from ds min_upstream_level
node_ids = static_data.basin[static_data.basin.streefpeil.isna()].node_id.to_numpy()

ds_node_ids = (model.downstream_node_id(i) for i in node_ids)
ds_node_ids = [i.to_list() if isinstance(i, pd.Series) else [i] for i in ds_node_ids]
ds_node_ids = pd.Series(ds_node_ids, index=node_ids).explode()
ds_node_ids = ds_node_ids[ds_node_ids.isin(static_data.outlet.node_id)]
streefpeil = static_data.outlet.set_index("node_id").loc[ds_node_ids.to_numpy(), ["min_upstream_level", "code"]]

streefpeil.index = ds_node_ids.index

streefpeil.dropna(inplace=True)

# Sorteer per groep zodat de kleinste 'min_upstream_level' bovenaan staat
streefpeil = streefpeil.sort_index().sort_values(by="min_upstream_level", inplace=False)
non_kdu_values = streefpeil[streefpeil["code"].str.startswith("S")]
non_kdu_first = non_kdu_values.groupby(level=0).first()
all_first = streefpeil.groupby(level=0).first()

# Stap 4: Combineer de niet-KDU en KDU waarden: kies de niet-KDU waarde als die er is, anders de KDU waarde
streefpeil = non_kdu_first.combine_first(all_first)
streefpeil = streefpeil["min_upstream_level"]
streefpeil.index.name = "node_id"
streefpeil.name = "streefpeil"

# Voeg de data toe als een Series
static_data.add_series(node_type="Basin", series=streefpeil, fill_na=True)


# %% Corrigeer de outlet duikers wanneer er een stuw in peilgebied voorkomt

# Haal node_ids op waarvoor streefpeil bekend is
node_ids = static_data.basin[static_data.basin.streefpeil.notna()].node_id.to_numpy()

# Zoek de downstream nodes (outlets) van deze Basin-nodes
ds_node_ids = (model.downstream_node_id(i) for i in node_ids)
ds_node_ids = [i.to_list() if isinstance(i, pd.Series) else [i] for i in ds_node_ids]
ds_node_ids = pd.Series(ds_node_ids, index=node_ids).explode()

basin_to_outlet_map = ds_node_ids[ds_node_ids.isin(static_data.outlet.node_id)]

# Haal min_upstream_level en code op
streefpeil = static_data.outlet.set_index("node_id").loc[basin_to_outlet_map.to_numpy(), ["min_upstream_level", "code"]]
streefpeil.index = basin_to_outlet_map.values
streefpeil.dropna(inplace=True)

# Sorteer per groep zodat de kleinste 'min_upstream_level' bovenaan staat
streefpeil = streefpeil.sort_values(by="min_upstream_level")
non_kdu_values = streefpeil[streefpeil["code"].str.startswith("S")]
non_kdu_first = non_kdu_values.groupby(level=0).first()
all_first = streefpeil.groupby(level=0).first()
streefpeil = non_kdu_first.combine_first(all_first)

kdu_nodes = streefpeil[~streefpeil["code"].str.startswith("S")].copy()
kdu_nodes.index = basin_to_outlet_map.loc[basin_to_outlet_map.isin(kdu_nodes.index)].values
kdu_nodes["min_upstream_level"] = streefpeil.loc[kdu_nodes.index, "min_upstream_level"]
min_upstream_level = kdu_nodes["min_upstream_level"]
min_upstream_level.index.name = "node_id"

static_data.add_series(node_type="Outlet", series=min_upstream_level, fill_na=False)

# %% HDSR heeft geen DAMO profielen
# DAMO-profielen bepalen voor outlets wanneer min_upstream_level nodata
# node_ids = static_data.outlet[static_data.outlet.min_upstream_level.isna()].node_id.to_numpy()
# profile_ids = model.edge.df.set_index("to_node_id").loc[node_ids]["meta_profielid_waterbeheerder"]
# levels = ((profiles_df.loc[profile_ids]["bottom_level"] + profiles_df.loc[profile_ids]["invert_level"]) / 2).to_numpy()
# min_upstream_level = pd.Series(levels, index=node_ids, name="min_upstream_level")
# min_upstream_level.index.name = "node_id"
# static_data.add_series(node_type="Outlet", series=min_upstream_level, fill_na=True)

# %% HDSR heeft geen DAMO profielen
# DAMO-profielen bepalen voor pumps wanneer min_upstream_level nodata
# node_ids = static_data.pump[static_data.pump.min_upstream_level.isna()].node_id.to_numpy()
# profile_ids = model.edge.df.set_index("to_node_id").loc[node_ids]["meta_profielid_waterbeheerder"]
# levels = ((profiles_df.loc[profile_ids]["bottom_level"] + profiles_df.loc[profile_ids]["invert_level"]) / 2).to_numpy()
# min_upstream_level = pd.Series(levels, index=node_ids, name="min_upstream_level")
# min_upstream_level.index.name = "node_id"
# static_data.add_series(node_type="Pump", series=min_upstream_level, fill_na=True)

# %%
## PUMP.flow_rate
gkw_gemaal_df = get_data_from_gkw(authority=authority, layers=["gemaal"])
flow_rate = gkw_gemaal_df.set_index("code")["maximalecapaciteit"] / 60  # m3/minuut to m3/s
flow_rate.name = "flow_rate"
static_data.add_series(node_type="Pump", series=flow_rate)


# %%

model.basin.area.df.set_index("node_id", inplace=True)
streefpeil = static_data.basin.set_index("node_id")["streefpeil"]
model.basin.area.df.loc[streefpeil.index, "meta_streefpeil"] = streefpeil
model.basin.area.df["meta_streefpeil"] = pd.to_numeric(model.basin.area.df["meta_streefpeil"], errors="coerce")

model.basin.area.df.reset_index(drop=False, inplace=True)
model.basin.area.df.index += 1
model.basin.area.df.index.name = "fid"

# %%
static_data.write()
model.write(ribasim_toml)

# %%
