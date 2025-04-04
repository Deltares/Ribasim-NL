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
peilgebieden_gpkg = cloud.joinpath(authority, "verwerkt/4_ribasim/peilgebieden.gpkg")
peilgebieden_vig_gpkg = cloud.joinpath(authority, "verwerkt/4_ribasim/peilgebieden_vigerend.gpkg")
top10NL_gpkg = cloud.joinpath("Basisgegevens", "Top10NL", "top10nl_Compleet.gpkg")

cloud.synchronize(filepaths=[peilgebieden_gpkg, top10NL_gpkg])

# %% init things
model = Model.read(ribasim_toml)
ribasim_toml = ribasim_dir.with_name(f"{authority}_prepare_model") / ribasim_toml.name
network = Network(lines_gdf=gpd.read_file(hydroobject_gpkg, layer="hydroobject"))
static_data = StaticData(model=model, xlsx_path=static_data_xlsx)

# %%

# fix link geometries
if link_geometries_gpkg.exists():
    link_geometries_df = gpd.read_file(link_geometries_gpkg).set_index("link_id")
    model.edge.df.loc[link_geometries_df.index, "geometry"] = link_geometries_df["geometry"]
    if "meta_profielid_waterbeheerder" in link_geometries_df.columns:
        model.edge.df.loc[link_geometries_df.index, "meta_profielid_waterbeheerder"] = link_geometries_df[
            "meta_profielid_waterbeheerder"
        ]
else:
    fix_link_geometries(model, network)
    model.edge.df.reset_index().to_file(link_geometries_gpkg)


# %%

# add streefpeilen
peilgebieden_gpkg_editted = peilgebieden_gpkg.with_name(f"{peilgebieden_gpkg.stem}_bewerkt.gpkg")

if not peilgebieden_gpkg_editted.exists():
    df = gpd.read_file(peilgebieden_gpkg)
    df_extra = gpd.read_file(peilgebieden_vig_gpkg)

    # Kolomnamen gelijk maken
    df_extra = df_extra.rename(
        columns={
            "ZOMERPEIL": "WS_ZP",
            "WINTERPEIL": "WS_WP",
            "ONDERPEIL": "WS_OP",
            "BOVENPEIL": "WS_BP",
            "VASTPEIL": "WS_VP",
        }
    )

    if df.crs != df_extra.crs:
        df_extra = df_extra.to_crs(df.crs)

    # Overlay: Opvullen ontbrekende peilen
    df_extra_only = gpd.overlay(df_extra, df, how="difference", keep_geom_type=True)

    # Kolommen gelijk maken
    for col in set(df.columns) - set(df_extra_only.columns):
        df_extra_only[col] = pd.NA
    for col in set(df_extra_only.columns) - set(df.columns):
        df[col] = pd.NA

    df_extra_only = df_extra_only[df.columns]
    df_combined = pd.concat([df, df_extra_only], ignore_index=True)

    # Gebruik df_combined voor verdere bewerkingen
    mask = df_combined["WS_BP"] == 999
    df_combined.loc[mask, "WS_BP"] = pd.NA

    df_combined["streefpeil"] = df_combined["WS_ZP"]
    mask = df_combined["streefpeil"].isna()
    df_combined.loc[mask, "streefpeil"] = df_combined.loc[mask, "WS_VP"]
    mask = df_combined["streefpeil"].isna()
    df_combined.loc[mask, "streefpeil"] = df_combined.loc[mask, "WS_BP"]
    mask = df_combined["streefpeil"].isna()
    df_combined.loc[mask, "streefpeil"] = df_combined.loc[mask, "WS_OP"]

    # Wegschrijven
    df_combined.to_file(peilgebieden_gpkg_editted, driver="GPKG")

# Inlezen
peilgebieden_df = gpd.read_file(peilgebieden_gpkg_editted)


# %% Bepaal min_upstream_level Outlet`
static_data.reset_data_frame(node_type="Outlet")
node_ids = static_data.outlet.node_id
levels = []
for node_id in node_ids:
    node = model.outlet[node_id]
    tolerance = 10  # afstand voor zoeken bovenstrooms
    node_id = node.node_id
    node_geometry = node.geometry

    line_to_node = model.link.df.set_index("to_node_id").at[node_id, "geometry"]
    containing_point = line_to_node.interpolate(line_to_node.length - tolerance)
    peilgebieden_select_df = peilgebieden_df[peilgebieden_df.contains(containing_point)]
    if not peilgebieden_select_df.empty:
        peilgebied = peilgebieden_select_df.iloc[0]

        # Check in volgorde van prioriteit
        for col in ["WS_ZP", "WS_VP", "WS_BP", "WS_OP"]:
            if peilgebied[col] < 30:
                level = peilgebied[col]
                break
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
    tolerance = 10  # 🔹 Afstand voor zoeken bovenstrooms
    node_id = node.node_id
    node_geometry = node.geometry

    line_to_node = model.link.df.set_index("to_node_id").at[node_id, "geometry"]
    containing_point = line_to_node.interpolate(line_to_node.length - tolerance)
    peilgebieden_select_df = peilgebieden_df[peilgebieden_df.contains(containing_point)]
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

static_data.add_series(node_type="Pump", series=min_upstream_level)


# %% Bepaal de basin streefpeilen door minimale upstream level outlet en pumps. Streefpeil stuw krijgt voorrang

static_data.reset_data_frame(node_type="Basin")

# Vul streefpeil van downstream min_upstream_level (outlets en pumps)
node_ids = static_data.basin[static_data.basin.streefpeil.isna()].node_id.to_numpy()

# Haal downstream node ids (outlets en pumps)
ds_node_ids = (model.downstream_node_id(i) for i in node_ids)
ds_node_ids = [i.to_list() if isinstance(i, pd.Series) else [i] for i in ds_node_ids]
ds_node_ids = pd.Series(ds_node_ids, index=node_ids).explode()
ds_node_ids = ds_node_ids[ds_node_ids.isin(static_data.outlet.node_id) | ds_node_ids.isin(static_data.pump.node_id)]
combined = pd.concat([static_data.outlet, static_data.pump])

# Voeg een 'source' kolom toe om te labelen of het een outlet of pump is
combined["source"] = combined.apply(
    lambda row: "pump" if row["node_id"] in static_data.pump.node_id.values else "outlet", axis=1
)

# Haal de streefpeilen voor de geselecteerde downstream node_ids (outlets en pumps)
streefpeil = combined.set_index("node_id").loc[ds_node_ids.to_numpy(), ["min_upstream_level", "code", "source"]]

# Zet de index gelijk aan de downstream node ids
streefpeil.index = ds_node_ids.index

# Verwijder NaN waarden
streefpeil.dropna(inplace=True)


# Sorteer per groep zodat de kleinste 'min_upstream_level' bovenaan staat
streefpeil = streefpeil.sort_index().sort_values(by="min_upstream_level", inplace=False)
non_kdu_values = streefpeil[streefpeil["code"].str.startswith("ST") | (streefpeil["source"] == "pump")]
non_kdu_first = non_kdu_values.groupby(level=0).first()
all_first = streefpeil.groupby(level=0).first()

# Stap 4: Combineer de niet-KDU en KDU waarden: kies de niet-KDU waarde als die er is, anders de KDU waarde
streefpeil = non_kdu_first.combine_first(all_first)
streefpeil = streefpeil["min_upstream_level"]
streefpeil.index.name = "node_id"
streefpeil.name = "streefpeil"

# Voeg de data toe als een Series
static_data.add_series(node_type="Basin", series=streefpeil, fill_na=True)


# %% Bepaal min_upstream_level at Manning locations`en vul de nodata basins met deze streefpeilen

node_ids = model.manning_resistance.static.df["node_id"]
levels = []
for node_id in node_ids:
    node = model.manning_resistance[node_id]
    tolerance = 10
    node_geometry = node.geometry
    line_to_node = model.link.df.set_index("to_node_id").at[node_id, "geometry"]
    containing_point = line_to_node.interpolate(line_to_node.length - tolerance)
    peilgebieden_select_df = peilgebieden_df[peilgebieden_df.contains(containing_point)]
    if not peilgebieden_select_df.empty:
        peilgebied = peilgebieden_select_df.iloc[0]
        level = next(
            (
                peilgebied[col]
                for col in ["WS_ZP", "WS_VP", "WS_BP", "WS_OP"]
                if col in peilgebied and peilgebied[col] < 30
            ),
            None,
        )
    else:
        level = None

    levels.append(level)

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

# %% Fill missing streefpeilen by basin location
# %% Fill missing streefpeilen by basin location
still_missing_basins = static_data.basin[static_data.basin.streefpeil.isna()]
basin_node_ids = still_missing_basins.node_id.to_numpy()

# Load peilgebieden once
fallback_levels = []

for node_id in basin_node_ids:
    node = model.basin[node_id]
    print(node)
    node_geometry = node.geometry
    print(node_geometry)

    # Zoek het peilgebied dat de basin bevat
    containing = peilgebieden_df[peilgebieden_df.contains(node_geometry)]

    if not containing.empty:
        peilgebied = containing.iloc[0]
        level = next(
            (
                peilgebied[col]
                for col in ["WS_ZP", "WS_VP", "WS_BP", "WS_OP"]
                if col in peilgebied and peilgebied[col] < 30
            ),
            None,
        )
        if level is not None:
            fallback_levels.append((node_id, level))

# Voeg de fallback levels toe aan static_data als een Series
if fallback_levels:
    fallback_series = pd.Series(dict(fallback_levels), name="streefpeil")
    fallback_series.index.name = "node_id"
    static_data.add_series(node_type="Basin", series=fallback_series, fill_na=True)

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
