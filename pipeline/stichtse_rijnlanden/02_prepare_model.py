# %%

from typing import cast

import geopandas as gpd
import pandas as pd
import shapely
from ribasim_nl.assign_authorities import AssignAuthorities
from ribasim_nl.gkw import get_data_from_gkw
from ribasim_nl.link_geometries import fix_link_geometries
from ribasim_nl.network_snapping import lines_in, route_basin_links
from ribasim_nl.parametrization.static_data_xlsx import StaticData
from shapely.geometry import LineString, Point

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

# %% init things
model = Model.read(ribasim_toml)
# nodes and links are referred to by their key, see docs/dev/node-keys.qmd
n = model.node_id
ribasim_toml = ribasim_dir.with_name(f"{authority}_prepare_model") / ribasim_toml.name
network = Network(lines_gdf=gpd.read_file(hydroobject_gpkg, layer="hydroobject"), tolerance=0.2)
static_data = StaticData(model=model, xlsx_path=static_data_xlsx)

# %%

# fix link geometries
fix_link_geometries(model, network)

# the inlets from the Grecht into polder Oud-Kamerik follow the ditches of the polder, rather than the Grecht, which is
# the shortest way along the hydro-objects, see #1078
OUD_KAMERIK = n("up:G0019")
basin_areas = model.basin.area.df
assert basin_areas is not None
oud_kamerik_area = basin_areas.loc[basin_areas["node_id"] == OUD_KAMERIK].union_all().buffer(10.0)
hydroobject_lines = shapely.force_2d(network.lines_gdf.geometry.explode().to_numpy())
_, oud_kamerik_lines = lines_in(hydroobject_lines, shapely.STRtree(hydroobject_lines), oud_kamerik_area)
grechtkade_inlets = {
    model.link_id(key, "up:G0019"): cast(Point, model.node.df.at[n(key), "geometry"]) for key in ("I1692", "I6013")
}
_, routes = route_basin_links(
    cast(Point, model.node.df.at[OUD_KAMERIK, "geometry"]),
    grechtkade_inlets,
    list(oud_kamerik_lines),
    [False] * len(oud_kamerik_lines),
)
assert routes.keys() == grechtkade_inlets.keys(), f"Not routed: {grechtkade_inlets.keys() - routes.keys()}"
for link_id, coords in routes.items():
    model.link.df.loc[[link_id], "geometry"] = gpd.GeoSeries([LineString(coords[::-1])], index=[link_id], crs=model.crs)
model.link.df.reset_index().to_file(link_geometries_gpkg)


# %%
# add streefpeilen
peilgebieden_gpkg_editted = peilgebieden_gpkg.with_name(f"{peilgebieden_gpkg.stem}_bewerkt.gpkg")

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
    assert df.crs is not None
    df_extra = df_extra.to_crs(df.crs)

# Overlay: Opvullen ontbrekende peilen
df_extra_only = gpd.overlay(df_extra, df, how="difference", keep_geom_type=True)

# Kolommen gelijk maken
for col in sorted(set(df.columns) - set(df_extra_only.columns)):
    df_extra_only[col] = pd.NA
for col in sorted(set(df_extra_only.columns) - set(df.columns)):
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
peilgebieden_gpkg_editted.unlink(missing_ok=True)  # a new file, so its bytes only depend on its content
df_combined.to_file(peilgebieden_gpkg_editted, driver="GPKG")

# Inlezen
peilgebieden_df = gpd.read_file(peilgebieden_gpkg_editted)


# %% Bepaal min_upstream_level Outlet`
static_data.reset_data_frame(node_type="Outlet")
assert static_data.outlet is not None
node_ids = static_data.outlet.node_id
levels = []
for node_id in node_ids:
    node = model.outlet[node_id]
    tolerance = 30  # afstand voor zoeken bovenstrooms
    node_id = node.node_id
    node_geometry = node.geometry

    line_to_node = cast(LineString, model.link.df.set_index("to_node_id").at[node_id, "geometry"])
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
assert static_data.pump is not None
node_ids = static_data.pump.node_id
levels = []

for node_id in node_ids:
    node = model.pump[node_id]
    tolerance = 30
    node_id = node.node_id
    node_geometry = node.geometry

    line_to_node = cast(LineString, model.link.df.set_index("to_node_id").at[node_id, "geometry"])
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
assert static_data.basin is not None
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

pump_node_ids = static_data.pump.node_id.values
combined["source"] = combined.apply(lambda row: "pump" if row["node_id"] in pump_node_ids else "outlet", axis=1)

valid_ids = ds_node_ids[ds_node_ids.isin(combined["node_id"])]
streefpeil = combined.set_index("node_id").loc[valid_ids.to_list(), ["min_upstream_level", "code", "source"]]
streefpeil["basin_node_id"] = valid_ids.loc[valid_ids.isin(streefpeil.index)].index
streefpeil["node_id"] = streefpeil.index  # outlet/pump node_id
streefpeil = streefpeil.set_index("basin_node_id")
streefpeil = streefpeil.dropna()


# Sorteer per groep zodat de kleinste 'min_upstream_level' bovenaan staat
streefpeil = streefpeil.sort_index().sort_values(by="min_upstream_level")
non_kdu_values = streefpeil[streefpeil["code"].str.startswith("ST") | (streefpeil["source"] == "pump")]
non_kdu_first = non_kdu_values.groupby(level=0).first()
all_first = streefpeil.groupby(level=0).first()

# update duiker min_upstream_level wanneer een stuw/pomp in dat basin ligt
streefpeil_update = streefpeil.loc[streefpeil.index.isin(non_kdu_first.index)].copy()
streefpeil_update["min_upstream_level"] = streefpeil_update.index.map(non_kdu_first["min_upstream_level"])  # ty: ignore[invalid-argument-type]
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

assert model.manning_resistance.static.df is not None
node_ids = model.manning_resistance.static.df["node_id"]
levels = []
for node_id in node_ids:
    node = model.manning_resistance[node_id]
    tolerance = 30
    node_geometry = node.geometry
    line_to_node = cast(LineString, model.link.df.set_index("to_node_id").at[node_id, "geometry"])
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
streefpeil = min_upstream_level.loc[valid_ds.to_list()].rename(
    index=dict(zip(valid_ds.values, valid_ds.index, strict=True))
)
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
        n("G1002"): 0.5,
        n("G4056"): 0.7,
        n("G0007"): 0.75,
        n("G3004"): 0.9,
        n("G0090"): 1,
        n("G4026"): 1,
        n("G8002"): 1.25,
        n("G4065"): 1.75,
        n("G4251"): 1.8,
        n("G4041"): 1.95,
        n("G4046"): 2.5,
        n("G3003"): 3.3,
        n("G0058"): 4,
        n("G3025"): 5.34,
        n("G4066"): 5.5,
        n("G4031"): 7,
        n("G5002"): 11.2,
        n("G6007"): 12,
        n("G8000"): 14,
    },
    name="flow_rate",
)
flow_rate.index.name = "node_id"
static_data.add_series(node_type="Pump", series=flow_rate)

# %%
flow_rate = pd.Series(
    {
        n("SL0015"): 56,
        n("SL0005"): 25,
        n("I6119"): 10,  # inlaat Vreeswijk (i.o. Epke)
        n("SL0009"): 16,  # weertsluis (op basis van waterakkoord)
        n("SL0011"): 12,
        n("SY0070"): 12,
        n("AF0093"): 10,
        n("SL0006"): 7,
        n("SL0030"): 7,
        n("I6098"): 6.2,
        n("SL0003"): 6.2,
        n("ST3003"): 6,
        n("I1523"): 3,
        n("SL0016"): 0,
        n("I6073"): 1.25,  # Westriool (waterakkoord)
        n("ST2028"): 15,  # Werkhoven, de rest moet langs Caspargouw. I.o. Epke
    },
    name="flow_rate",
)
flow_rate.index.name = "node_id"
static_data.add_series(node_type="Outlet", series=flow_rate)
static_data.outlet.loc[static_data.outlet.flow_rate.isna(), "flow_rate"] = 20.0
# %%

static_data.update_model_basin_levels(profile_ids=False)


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
