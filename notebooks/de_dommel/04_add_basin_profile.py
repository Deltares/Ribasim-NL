# %%
import geopandas as gpd
import pandas as pd
from ribasim.nodes import tabulated_rating_curve
from ribasim_nl import CloudStorage, Model
from shapely.geometry import MultiLineString

PROFIEL_ID_COLUMN = "PROFIELLIJNID"
PROFIEL_LINE_ID_COLUMN = "profiel_id"
PROFIEL_HOOGTE_COLUMN = "HOOGTE"
PROFIEL_BREEDTE_COLUMN = "breedte"

cloud = CloudStorage()


# %%

ribasim_toml = cloud.joinpath("DeDommel", "modellen", "DeDommel", "model.toml")
model = Model.read(ribasim_toml)
profile_gpkg = cloud.joinpath("DeDommel", "verwerkt", "profile.gpkg")

if not profile_gpkg.exists():
    profielpunt_gdf = gpd.read_file(
        cloud.joinpath("DeDommel", "verwerkt", "2_voorbewerking", "hydamo.gpkg"),
        layer="profielpunt",
        engine="pyogrio",
        fid_as_index=True,
    )

    profiellijn_gdf = gpd.read_file(
        cloud.joinpath("DeDommel", "verwerkt", "2_voorbewerking", "hydamo.gpkg"),
        layer="profiellijn",
        engine="pyogrio",
        fid_as_index=True,
    ).set_index("GLOBALID")

    hydroobject_gdf = gpd.read_file(
        cloud.joinpath("DeDommel", "verwerkt", "2_voorbewerking", "hydamo.gpkg"),
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
    # profiel_id, df = list(profielpunt_gdf.groupby(PROFIEL_ID_COLUMN))[2]
    for profiel_id, df in profielpunt_gdf.groupby(PROFIEL_ID_COLUMN):
        # df = df[df.within(area_poly)]
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
                    data += [
                        {
                            "profiel_id": profiel_id,
                            "bodemhoogte": bodemhoogte,
                            "insteekhoogte": insteekhoogte,
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


# %%
# of all profiles within basin/area we take the one with the lowest level
def get_area_and_profile(node_id):
    area_geometry = None

    # try to get a sensible area_geometry from basin-area
    if node_id in model.basin.area.df.node_id.to_list():
        area_geometry = model.basin.area.df.set_index("node_id").loc[node_id, "geometry"]
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
        print(f"basin without intersecting profile {row.node_id}")
        profile = profile_df.loc[profile_df.distance(row.geometry).idxmin()]

    return area_geometry, profile


# %% update basin / profile
for row in model.basin.node.df.itertuples():
    area_geometry, profile = get_area_and_profile(row.node_id)

    level = [profile.bodemhoogte, profile.insteekhoogte]
    area = [0, max(area_geometry.area, 999)]

    # remove profile from basin
    model.basin.profile.df = model.basin.profile.df[model.basin.profile.df.node_id != row.node_id]

    # add profile to basin
    model.basin.profile.df = pd.concat(
        [
            model.basin.profile.df,
            pd.DataFrame({"node_id": [row.node_id] * len(level), "level": level, "area": area}),
        ]
    )

    model.basin.node.df.loc[row.Index, ["meta_profile_id"]] = profile[PROFIEL_LINE_ID_COLUMN]

# %% update manning / static
for row in model.manning_resistance.static.df.itertuples():
    # get profile_width
    from_basin_id = model.edge.df.set_index("to_node_id").at[row.node_id, "from_node_id"]
    profile_id = model.basin.node.df.set_index("node_id").at[from_basin_id, "meta_profile_id"]
    profile = profile_df.set_index(PROFIEL_LINE_ID_COLUMN).loc[profile_id]
    depth = profile.insteekhoogte - profile.bodemhoogte

    # compute profile_width from slope
    profile_slope = 0.5
    profile_width = profile.geometry.length - ((depth / profile_slope) * 2)

    # if width < 1/3 * profile.geometry.length (width at invert), we compute profile_slope from profile_width
    if profile_width < profile.geometry.length / 3:
        profile_width = profile.geometry.length / 3
        profile_slope = depth / profile_width

    # get length
    to_basin_id = model.edge.df.set_index("from_node_id").at[row.node_id, "to_node_id"]
    df = model.edge.df.set_index(["from_node_id", "to_node_id"])
    length = (
        df.at[(from_basin_id, row.node_id), "geometry"].length + df.at[(row.node_id, to_basin_id), "geometry"].length
    )

    # update manning-static
    model.manning_resistance.static.df.loc[row.Index, ["length"]] = round(length)
    model.manning_resistance.static.df.loc[row.Index, ["profile_width"]] = round(profile_width, 2)
    model.manning_resistance.static.df.loc[row.Index, ["profile_slope"]] = round(profile_slope, 2)

    # add profile to meta_data
    model.manning_resistance.node.df.loc[row.Index, ["meta_profile_id"]] = profile_id


# %% update tabulated rating curves
def weir_flow(level, crest_level, crest_width, loss_coefficient=0.63):
    """Compute free weir flow from level, crest_level and crest_width"""
    if level < crest_level:
        return 0
    else:
        u = loss_coefficient * ((2 / 3) * 9.81 * (level - crest_level)) ** (1 / 2)
        a = crest_width * ((2 / 3) * (level - crest_level))
        return round(u * a, 2)


stuwen_gdf = gpd.read_file(cloud.joinpath("DeDommel", "verwerkt", "4_ribasim", "hydamo.gpkg"), layer="stuw").set_index(
    "CODE"
)

stuwen_lww_gdf = gpd.read_file(cloud.joinpath("DeDommel", "downloads", "LWW_2023_Stuw_V.shp")).set_index("Lokaal_ID")

for row in model.tabulated_rating_curve.node.df.itertuples():
    # get basin_profile for comparison
    from_basin_id = model.edge.df.set_index("to_node_id").at[row.node_id, "from_node_id"]
    if from_basin_id in model.basin.node.df.node_id.to_list():
        profile_id = model.basin.node.df.set_index("node_id").at[from_basin_id, "meta_profile_id"]
        profile = profile_df.set_index(PROFIEL_LINE_ID_COLUMN).loc[profile_id]

        if row.name in stuwen_gdf.index:
            stuw = stuwen_gdf.loc[row.name]
            if isinstance(stuw, gpd.GeoDataFrame):
                stuw.loc[:, "distance"] = stuw.distance(row.geometry)
                stuw = stuw.sort_values("distance").iloc[0]
        else:
            stuw = None

        # we determine crest_width
        if stuw is not None:
            crest_width = stuw.KRUINBREEDTE
        if pd.isna(crest_width) | (crest_width > profile.geometry.length):  # cannot be > profile width
            crest_width = 0.5 * profile.geometry.length  # assumption is 1/2 profile_width

        # we determine crest_level
        crest_level: float | None = None
        try:
            if stuw is not None:
                if stuw.WS_DOMMELID in stuwen_lww_gdf.index:  #
                    crest_level_str = stuwen_lww_gdf.at[stuw.WS_DOMMELID, "min_doorst"]

                elif stuwen_lww_gdf.distance(row.geometry) < 1:
                    crest_level_str = stuwen_lww_gdf.at[stuwen_lww_gdf.distance(row.geometry).idxmin(), "min_doorst"]

                if crest_level_str is not None:
                    crest_level = pd.to_numeric(crest_level_str.replace(",", "."))
        except ValueError:
            crest_level = None

        if pd.isna(crest_level):
            if stuw is not None:
                crest_level = stuw.HOOGTECONSTRUCTIE

        if pd.isna(crest_level):  # presume crest_level will be within bottom and insteeklevel
            crest_level = (profile.bodemhoogte + profile.insteekhoogte) / 2
        elif crest_level < profile.bodemhoogte:
            crest_level = profile.bodemhoogte + 0.1

        level = [round(crest_level, 2) + i for i in [0, 0.1, 0.25, 0.5, 2]]
        flow_rate = [weir_flow(i, round(crest_level, 2), crest_width) for i in level]

        # update resistance-static
        if stuw is not None:
            node_properties = {"name": stuw.NAAM, "meta_code_waterbeheerder": stuw.WS_DOMMELID}
        else:
            node_properties = {"name": row.name}
        model.update_node(
            node_id=row.node_id,
            node_type=row.node_type,
            data=[tabulated_rating_curve.Static(level=level, flow_rate=flow_rate)],
            node_properties=node_properties,
        )
    else:
        print(f"{row.node_id} probably shouldn't be a TabulatedRatingCurve but Outlet")

# %% write model
model.write(ribasim_toml)
