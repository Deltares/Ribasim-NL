# %%

import geopandas as gpd
import pandas as pd
from peilbeheerst_model.assign_authorities import AssignAuthorities
from ribasim_nl.gkw import get_data_from_gkw
from ribasim_nl.link_geometries import fix_link_geometries
from ribasim_nl.parametrization.static_data_xlsx import StaticData

from ribasim_nl import CloudStorage, Model, Network

cloud = CloudStorage()
authority = "StichtseRijnlanden"
short_name = "hdsr"

# %% files
ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_fix_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"

parameters_dir = cloud.joinpath(authority, "verwerkt/parameters")
parameters_dir.mkdir(parents=True, exist_ok=True)
static_data_xlsx = parameters_dir / "static_data_template.xlsx"
profiles_gpkg = parameters_dir / "profiles.gpkg"
link_geometries_gpkg = parameters_dir / "link_geometries.gpkg"
hydroobject_gpkg = cloud.joinpath(authority, "verwerkt/4_ribasim/hydroobject.gpkg")
peilgebieden_gpkg = cloud.joinpath(authority, "verwerkt/4_ribasim/peilgebieden.gpkg")
peilgebieden_vig_gpkg = cloud.joinpath(authority, "verwerkt/4_ribasim/peilgebieden_vigerend.gpkg")
top10NL_gpkg = cloud.joinpath("Basisgegevens/Top10NL/top10nl_Compleet.gpkg")

cloud.synchronize(filepaths=[peilgebieden_gpkg, peilgebieden_vig_gpkg, top10NL_gpkg, hydroobject_gpkg])

# %% init things
model = Model.read(ribasim_toml)
ribasim_toml = ribasim_dir.with_name(f"{authority}_prepare_model") / ribasim_toml.name
network = Network(lines_gdf=gpd.read_file(hydroobject_gpkg, layer="hydroobject"), tolerance=0.2)
static_data = StaticData(model=model, xlsx_path=static_data_xlsx)

# %%

# fix link geometries
if link_geometries_gpkg.exists():
    link_geometries_df = gpd.read_file(link_geometries_gpkg).set_index("link_id")
    model.link.df.loc[link_geometries_df.index, "geometry"] = link_geometries_df["geometry"]
    if "meta_profielid_waterbeheerder" in link_geometries_df.columns:
        model.link.df.loc[link_geometries_df.index, "meta_profielid_waterbeheerder"] = link_geometries_df[
            "meta_profielid_waterbeheerder"
        ]
else:
    fix_link_geometries(model, network, max_straight_line_ratio=5)
    model.link.df.reset_index().to_file(link_geometries_gpkg)


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
    tolerance = 30  # afstand voor zoeken bovenstrooms
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
    node = model.pump[node_id]
    tolerance = 30
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

        # Check in volgorde van prioriteit
        for col in ["WS_ZP", "WS_VP", "WS_BP", "WS_OP"]:
            if peilgebied[col] < 30:
                level = peilgebied[col]
                break  # Stop bij de eerste geldige waarde

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
        print(f"Geen downstream node gevonden voor basin met node_id {node_id}")
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
non_kdu_values = streefpeil[streefpeil["code"].str.startswith("ST") | (streefpeil["source"] == "pump")]
non_kdu_first = non_kdu_values.groupby(level=0).first()
all_first = streefpeil.groupby(level=0).first()

# update duiker min_upstream_level wanneer een stuw/pomp in dat basin ligt
streefpeil_update = streefpeil.loc[streefpeil.index.isin(non_kdu_first.index)].copy()
streefpeil_update["min_upstream_level"] = streefpeil_update.index.map(non_kdu_first["min_upstream_level"])
min_upstream_level_outlets = streefpeil_update.set_index("node_id")["min_upstream_level"]
min_upstream_level_outlets.index.name = "node_id"

static_data.add_series(node_type="Outlet", series=min_upstream_level_outlets, fill_na=False)

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
    tolerance = 30
    node_geometry = node.geometry
    line_to_node = model.link.df.set_index("to_node_id").at[node_id, "geometry"]
    distance_to_interpolate = line_to_node.length - tolerance
    if distance_to_interpolate < 0:
        distance_to_interpolate = 0

    containing_point = line_to_node.interpolate(distance_to_interpolate)
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


# %%
## PUMP.flow_rate on code
gkw_gemaal_df = get_data_from_gkw(authority=authority, layers=["gemaal"])
flow_rate = gkw_gemaal_df.set_index("code")["maximalecapaciteit"] / 60  # m3/minuut to m3/s
flow_rate.name = "flow_rate"
static_data.add_series(node_type="Pump", series=flow_rate)

# add date on node_id
flow_rate = pd.Series(
    {
        602: 0.5,
        578: 0.7,
        554: 0.75,
        596: 0.9,
        584: 1,
        587: 1,
        588: 1.25,
        616: 1.75,
        608: 1.8,
        619: 1.95,
        600: 2.5,
        601: 3.3,
        617: 4,
        623: 5.34,
        557: 5.5,
        542: 7,
        541: 11.2,
        536: 12,
        567: 14,
    },
    name="flow_rate",
)
flow_rate.index.name = "node_id"
static_data.add_series(node_type="Pump", series=flow_rate)

# %%
flow_rate = pd.Series(
    {
        752: 56,
        755: 25,
        252: 18,
        757: 16,
        756: 12,
        468: 12,
        481: 10,
        753: 7,
        754: 7,
        141: 6.2,
        742: 6.2,
        761: 6,
        100: 3,
        758: 0,
    },
    name="flow_rate",
)
flow_rate.index.name = "node_id"
static_data.add_series(node_type="Outlet", series=flow_rate)
static_data.outlet.loc[static_data.outlet.flow_rate.isna(), "flow_rate"] = 20
# %%

model.basin.area.df.set_index("node_id", inplace=True)
streefpeil = static_data.basin.set_index("node_id")["streefpeil"]
model.basin.area.df.loc[streefpeil.index, "meta_streefpeil"] = streefpeil
model.basin.area.df["meta_streefpeil"] = pd.to_numeric(model.basin.area.df["meta_streefpeil"], errors="coerce")
model.basin.area.df.reset_index(drop=False, inplace=True)
model.basin.area.df.index += 1
model.basin.area.df.index.name = "fid"


# # koppelen
ws_grenzen_path = cloud.joinpath("Basisgegevens/RWS_waterschaps_grenzen/waterschap.gpkg")
RWS_grenzen_path = cloud.joinpath("Basisgegevens/RWS_waterschaps_grenzen/Rijkswaterstaat.gpkg")
assign = AssignAuthorities(
    ribasim_model=model,
    waterschap=authority,
    ws_grenzen_path=ws_grenzen_path,
    RWS_grenzen_path=RWS_grenzen_path,
)
model = assign.assign_authorities()

# %%
static_data.write()
model.write(ribasim_toml)
