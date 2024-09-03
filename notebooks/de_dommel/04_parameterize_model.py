# %%
import geopandas as gpd
import pandas as pd
from ribasim.nodes import manning_resistance, pump
from ribasim_nl import CloudStorage, Model
from ribasim_nl.structure_node import get_outlet, get_tabulated_rating_curve
from shapely.geometry import MultiLineString

PROFIEL_ID_COLUMN = "PROFIELLIJNID"
PROFIEL_LINE_ID_COLUMN = "profiel_id"
PROFIEL_HOOGTE_COLUMN = "HOOGTE"
PROFIEL_BREEDTE_COLUMN = "breedte"
STUW_TARGET_LEVEL_COLUMN = "WS_STREEFPEILLAAG"
STUW_CREST_LEVEL_COLUMN = "LAAGSTEDOORSTROOMHOOGTE"
STUW_CODE_COLUMN = "WS_DOMMELID"
STUW_WIDTH_COLUMN = "KRUINBREEDTE"
STUW_NAME_COLUMN = "NAAM"

KDU_INVERT_LEVEL_US_COLUMN = "WS_BODEMHOOGTEBOV"
KDU_INVERT_LEVEL_DS_COLUMN = "WS_BODEMHOOGTEBEN"
KDU_WIDTH_COLUMN = "BREEDTEOPENING"
KDU_HEIGHT_COLUMN = "HOOGTEOPENING"
KDU_SHAPE_COLUMN = "VORMKOKER2"
KDU_SHAPE_MAP = {
    "rond": "round",
    "rechthoekig": "rectangle",
    "eivorming": "ellipse",
    "heulprofiel": "ellipse",
    "muilprofiel": "ellipse",
    "ellipsvormig": "ellipse",
}

KGM_CAPACITY_COLUMN = "MAXIMALECAPACITEIT"
KGM_NAME_COLUMN = "NAAM"
KGM_CODE_COLUMN = "WS_DOMMELID"


cloud = CloudStorage()


# %% Voorbereiden profielen uit HyDAMO
ribasim_toml = cloud.joinpath("DeDommel", "modellen", "DeDommel_fix_areas", "model.toml")
model = Model.read(ribasim_toml)
model.tabulated_rating_curve.static.df = None
model.manning_resistance.static.df = None
model.outlet.static.df = None


profile_gpkg = cloud.joinpath("DeDommel", "verwerkt", "profile.gpkg")
hydamo_gpkg = cloud.joinpath("DeDommel", "verwerkt", "4_ribasim", "hydamo.gpkg")
stuw_df = gpd.read_file(hydamo_gpkg, layer="stuw", engine="pyogrio")
stuw_df.loc[stuw_df.CODE.isna(), ["CODE"]] = stuw_df[stuw_df.CODE.isna()].NAAM
stuw_df.loc[stuw_df.CODE.isna(), ["CODE"]] = stuw_df[stuw_df.CODE.isna()].WS_DOMMELID
stuw_df.set_index("CODE", inplace=True)

kdu_df = gpd.read_file(hydamo_gpkg, layer="duikersifonhevel", engine="pyogrio").set_index("CODE")

kgm_df = gpd.read_file(hydamo_gpkg, layer="gemaal", engine="pyogrio").set_index("CODE")

basin_area_df = gpd.read_file(cloud.joinpath("DeDommel", "verwerkt", "basin_area.gpkg"), engine="pyogrio").set_index(
    "node_id"
)

if not profile_gpkg.exists():
    profielpunt_gdf = gpd.read_file(
        hydamo_gpkg,
        layer="profielpunt",
        engine="pyogrio",
        fid_as_index=True,
    )

    profiellijn_gdf = gpd.read_file(
        hydamo_gpkg,
        layer="profiellijn",
        engine="pyogrio",
        fid_as_index=True,
    ).set_index("GLOBALID")

    hydroobject_gdf = gpd.read_file(
        hydamo_gpkg,
        layer="hydroobject",
        engine="pyogrio",
        fid_as_index=True,
    )

    area_df = gpd.read_file(
        cloud.joinpath("DeDommel", "verwerkt", "watervlakken", "LWW_2023_A_water_vlak_V.shp"),
        engine="pyogrio",
        fid_as_index=True,
    )

    data = []
    for profiel_id, df in profielpunt_gdf.groupby(PROFIEL_ID_COLUMN):
        if not df.empty:
            if profiel_id in profiellijn_gdf.index:
                lowest_point = df.at[df[PROFIEL_HOOGTE_COLUMN].idxmin(), "geometry"]
                containing_area_df = area_df[area_df.contains(lowest_point)]
                if (not containing_area_df.empty) | (len(containing_area_df) > 1):
                    area_poly = containing_area_df.iloc[0].geometry
                    print(profiel_id)
                    profiel_geom = profiellijn_gdf.at[profiel_id, "geometry"]
                    breedte = profiellijn_gdf.at[profiel_id, PROFIEL_BREEDTE_COLUMN]
                    profiel_geom = profiel_geom.intersection(area_poly)
                    if isinstance(profiel_geom, MultiLineString):
                        geoms = [i for i in profiel_geom.geoms if hydroobject_gdf.intersects(i).any()]
                    else:
                        geoms = [profiel_geom]

                    bodemhoogte = df[PROFIEL_HOOGTE_COLUMN].min()
                    insteekhoogte = df[PROFIEL_HOOGTE_COLUMN].max()
                    waterlijnhoogte = df[df.within(area_poly)][PROFIEL_HOOGTE_COLUMN].max()

                    data += [
                        {
                            "profiel_id": profiel_id,
                            "bodemhoogte": bodemhoogte,
                            "insteekhoogte": insteekhoogte,
                            "waterlijnhoogte": waterlijnhoogte,
                            "breedte": breedte,
                            "geometry": geom,
                        }
                        for geom in geoms
                    ]

    profile_df = gpd.GeoDataFrame(data, crs=profielpunt_gdf.crs)
    profile_df = profile_df[~profile_df.is_empty]
    profile_df.drop_duplicates("profiel_id", inplace=True)
    profile_df.to_file(profile_gpkg, engine="pyogrio")
else:
    profile_df = gpd.read_file(profile_gpkg, engine="pyogrio", fid_as_index=True)
    profile_df.drop_duplicates("profiel_id", inplace=True)


# %% Basin / Profile
# of all profiles within basin/area we take the one with the lowest level
def get_area_and_profile(node_id):
    area_geometry = None

    # try to get a sensible area_geometry from basin-area
    if node_id in model.basin.area.df.node_id.to_list():
        area_geometry = model.basin.area[node_id].set_index("node_id").at[node_id, "geometry"]
        if area_geometry.area > 1000:
            selected_profiles_df = profile_df[profile_df.intersects(area_geometry)]
        else:
            area_geometry = None

    # if we didn't get an area (of sufficient size) we get it from profiles and edges
    if area_geometry is None:
        edges_select_df = model.edge.df[(model.edge.df.from_node_id == node_id) | (model.edge.df.to_node_id == node_id)]
        selected_profiles_df = profile_df[profile_df.intersects(edges_select_df.union_all())]
        if selected_profiles_df.empty:
            width = 2
        else:
            width = selected_profiles_df.length.mean()
        area_geometry = edges_select_df.buffer(width / 2).union_all()

    # select profile
    if not selected_profiles_df.empty:  # we select the profile with the lowest level
        profile = selected_profiles_df.loc[selected_profiles_df["bodemhoogte"].idxmin()]

    else:  # we select closest profile
        print(f"basin without intersecting profile {row.Index}")
        profile = profile_df.loc[profile_df.distance(row.geometry).idxmin()]

    return area_geometry, profile


# %% update basin / profile
basin_profile_df = model.basin.profile.df[model.basin.profile.df.node_id == -999]
for row in model.basin.node.df.itertuples():
    area_geometry, profile = get_area_and_profile(row.Index)

    level = [profile.bodemhoogte, profile.insteekhoogte]
    area = [1, round(max(area_geometry.area, 999))]

    # remove profile from basin
    model.basin.profile.df = model.basin.profile.df[model.basin.profile.df.node_id != row.Index]

    # add profile to basin
    basin_profile_df = pd.concat(
        [
            basin_profile_df,
            pd.DataFrame({"node_id": [row.Index] * len(level), "level": level, "area": area}),
        ]
    )

    model.basin.node.df.loc[row.Index, ["meta_profile_id"]] = profile[PROFIEL_LINE_ID_COLUMN]


basin_profile_df.reset_index(inplace=True, drop=True)
basin_profile_df.index.name = "fid"
model.basin.profile.df = basin_profile_df

# set basin state
state_df = model.basin.profile.df.groupby("node_id").max()["level"].reset_index()
state_df.index.name = "fid"
model.basin.state.df = state_df

# %%
# Stuwen als tabulated_rating_cuves
for row in model.node_table().df[model.node_table().df.meta_object_type == "stuw"].itertuples():
    node_id = row.Index

    # get weir
    if row.name in stuw_df.index:
        kst = stuw_df.loc[row.name]
    elif stuw_df.distance(row.geometry).min() < 1:
        kst = stuw_df.loc[stuw_df.distance(row.geometry).idxmin()]
    else:
        raise ValueError(f"Geen stuw gevonden voor node_id {node_id}")

    if isinstance(kst, gpd.GeoDataFrame):
        kst = kst.iloc[0]
    name = kst[STUW_NAME_COLUMN]
    code = kst[STUW_CODE_COLUMN]
    if pd.isna(name):
        name = code

    # get upstream, if at flowboundary downstream profile
    basin_node_id = model.upstream_node_id(node_id)
    if not model.node_table().df.at[basin_node_id, "node_type"] == "Basin":
        basin_node_id = model.downstream_node_id(node_id)

    profile = profile_df.set_index("profiel_id").loc[model.node_table().df.at[basin_node_id, "meta_profile_id"]]

    # get level

    # from target-level
    crest_level = kst[STUW_TARGET_LEVEL_COLUMN]

    # if NA crest-level
    if pd.isna(crest_level):
        crest_level = kst[STUW_CREST_LEVEL_COLUMN]

    # if NA upstream min basin-level + 10cm
    if pd.isna(crest_level):
        crest_level = profile.waterlijnhoogte
        if pd.isna(crest_level):
            crest_level = profile[["bodemhoogte", "waterlijnhoogte"]].mean()

    # if crest_level < upstream bottom-level we lower it to upstream bottom-level
    if crest_level < profile.bodemhoogte:
        crest_level = profile.bodemhoogte + 0.1

    # get width
    crest_width = kst[STUW_WIDTH_COLUMN]

    # if NA or implausible we overwrite with 0.5 of profile width
    if pd.isna(crest_width) | (crest_width > profile.geometry.length):  # cannot be > profile width
        crest_width = 0.5 * profile.geometry.length  # assumption is 1/2 profile_width

    # get data
    data = [get_tabulated_rating_curve(crest_level=crest_level, width=crest_width)]

    model.update_node(
        node_id,
        node_type="TabulatedRatingCurve",
        data=data,
        node_properties={"name": name, "meta_code_waterbeheerder": code},
    )

# %% Duikers als tabulated_rating_cuves
for row in model.node_table().df[model.node_table().df.meta_object_type == "duikersifonhevel"].itertuples():
    node_id = row.Index

    # get culvert
    if row.name in kdu_df.index:
        kdu = kdu_df.loc[row.name]
    elif kdu_df.distance(row.geometry).min() < 1:
        kdu = kdu_df.loc[kdu_df.distance(row.geometry).idxmin()]
    else:
        raise ValueError(f"Geen stuw gevonden voor node_id {node_id}")

    # get upstream, if at flowboundary downstream profile
    basin_node_id = model.upstream_node_id(node_id)
    if not model.node_table().df.at[basin_node_id, "node_type"] == "Basin":
        basin_node_id = model.downstream_node_id(node_id)

    profile = profile_df.set_index("profiel_id").loc[model.node_table().df.at[basin_node_id, "meta_profile_id"]]

    # get level

    # from invert-levels
    crest_level = kdu[[KDU_INVERT_LEVEL_US_COLUMN, KDU_INVERT_LEVEL_DS_COLUMN]].dropna().max()

    # if NA upstream min basin-level + 10cm
    if pd.isna(crest_level):
        crest_level = profile.waterlijnhoogte
        if pd.isna(crest_level):
            crest_level = profile[["bodemhoogte", "waterlijnhoogte"]].mean()

    # if crest_level < upstream bottom-level we lower it to upstream bottom-level
    if crest_level < profile.bodemhoogte:
        crest_level = profile.bodemhoogte + 0.1

    # get width
    width = kdu[KDU_WIDTH_COLUMN]

    # if NA or implausible we overwrite with 0.5 of profile width
    if pd.isna(width) | (width > profile.geometry.length):  # cannot be > profile width
        width = profile.geometry.length / 3  # assumption is 1/3 profile_width

    # get height
    height = kdu[KDU_HEIGHT_COLUMN]

    if pd.isna(height):
        height = width

    # get shape
    shape = kdu[KDU_SHAPE_COLUMN]

    if pd.isna(shape):
        shape = "rectangle"
    else:
        shape = KDU_SHAPE_MAP[shape]

    # update model
    data = get_tabulated_rating_curve(
        crest_level=crest_level,
        width=width,
        height=height,
        shape=shape,
        levels=[0, 0.1, 0.5],
    )

    model.update_node(
        node_id,
        node_type="TabulatedRatingCurve",
        data=[data],
        node_properties={"meta_code_waterbeheerder": row.name},
    )

# %% gemalen als pump
for row in model.node_table().df[model.node_table().df.meta_object_type == "gemaal"].itertuples():
    node_id = row.Index

    # get upstream profile
    basin_node_id = model.upstream_node_id(node_id)
    profile = profile_df.set_index("profiel_id").loc[model.node_table().df.at[basin_node_id, "meta_profile_id"]]
    min_upstream_level = profile.waterlijnhoogte

    kgm = kgm_df.loc[row.name]

    # set name and code column
    name = kgm[KGM_NAME_COLUMN]
    code = kgm[KGM_CODE_COLUMN]

    if pd.isna(name):
        name = code

    # get flow_rate
    flow_rate = kgm[KGM_CAPACITY_COLUMN]

    if pd.isna(flow_rate):
        flow_rate = round(
            basin_area_df.at[model.upstream_node_id(node_id), "geometry"].area * 0.015 / 86400, 2
        )  # 15mm/day of upstream areay

    data = pump.Static(flow_rate=[flow_rate], meta_min_upstream_level=min_upstream_level)

    model.update_node(
        node_id,
        node_type="Pump",
        data=[data],
        node_properties={"name": name, "meta_code_waterbeheerder": code},
    )

# %% update open water
for row in model.node_table().df[model.node_table().df.node_type == "ManningResistance"].itertuples():
    node_id = row.Index

    # get depth
    basin_node_id = model.upstream_node_id(node_id)
    profile = profile_df.set_index("profiel_id").loc[model.node_table().df.at[basin_node_id, "meta_profile_id"]]
    depth = profile.insteekhoogte - profile.bodemhoogte

    # compute profile_width from slope
    profile_slope = 0.5
    profile_width = profile.geometry.length - ((depth / profile_slope) * 2)

    # if width < 1/3 * profile.geometry.length (width at invert), we compute profile_slope from profile_width
    if profile_width < profile.geometry.length / 3:
        profile_width = profile.geometry.length / 3
        profile_slope = depth / profile_width

    # get length
    length = round(
        model.edge.df[(model.edge.df.from_node_id == node_id) | (model.edge.df.to_node_id == node_id)].length.sum()
    )

    # # update node
    data = manning_resistance.Static(
        length=[round(length)],
        profile_width=[round(profile_width, 2)],
        profile_slope=[round(profile_slope, 2)],
        manning_n=[0.04],
    )
    model.update_node(node_id, node_type="ManningResistance", data=[data])


# %% update outlets
for row in model.node_table().df[model.node_table().df.node_type == "Outlet"].itertuples():
    node_id = row.Index

    # get upstream, if at flowboundary downstream profile
    basin_node_id = model.upstream_node_id(node_id)
    if not model.node_table().df.at[basin_node_id, "node_type"] == "Basin":
        basin_node_id = model.downstream_node_id(node_id)

    profile = profile_df.set_index("profiel_id").loc[model.node_table().df.at[basin_node_id, "meta_profile_id"]]

    height = round(profile.insteekhoogte - profile.bodemhoogte, 2)
    width = round(profile.geometry.length, 2)

    try:
        crest_level = round(model.upstream_profile(node_id).level.min() + 0.1, 2)
    except ValueError:
        crest_level = round(model.downstream_profile(node_id).level.min() + 0.1, 2)

    data = get_outlet(crest_level=crest_level, width=width, height=height, max_velocity=0.5)

    model.update_node(node_id, node_type="Outlet", data=[data])

# %% clean boundaries
model.flow_boundary.static.df = model.flow_boundary.static.df[
    model.flow_boundary.static.df.node_id.isin(
        model.node_table().df[model.node_table().df.node_type == "FlowBoundary"].index
    )
]
model.flow_boundary.static.df.loc[:, "flow_rate"] = 0
# %% write model
ribasim_toml = cloud.joinpath("DeDommel", "modellen", "DeDommel_parameterized", "model.toml")
model.write(ribasim_toml)

# %%
