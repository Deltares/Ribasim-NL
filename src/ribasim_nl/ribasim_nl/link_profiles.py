# %%
import geopandas as gpd
from shapely.geometry import Point
from tqdm import tqdm

from ribasim_nl.model import Model
from ribasim_nl.parametrization.damo_profiles import DAMOProfiles


def project_profile_on_link(geometry, link_geometry):
    point = geometry.intersection(link_geometry)
    if isinstance(point, Point):
        return link_geometry.project(point)
    else:
        return 0.0


def link_profile_id(link_id: int, model: Model, profiles: DAMOProfiles | gpd.GeoDataFrame, id_col="globalid") -> str:
    if isinstance(profiles, DAMOProfiles):
        profiles = profiles.profile_line_df

    link_geometry = model.link.df.at[link_id, "geometry"]

    # get intersecting profiles
    profile_select_df = profiles[profiles.intersects(link_geometry)]

    # update profile by furthest downstream the link_geometry if we have intersections
    if not profile_select_df.empty:
        profile = profile_select_df.loc[
            profile_select_df.geometry.apply(project_profile_on_link, args=(link_geometry,)).idxmax()
        ]
    else:
        # take closest profile from to_node as default
        to_node_geometry = link_geometry.boundary.geoms[1]
        profile = profiles.loc[profiles.distance(to_node_geometry).idxmin()]

        # try to find a better one on the links in the same basin
        node_ids = model.link.df.loc[link_id][["from_node_id", "to_node_id"]].to_numpy()
        node_id = next((i for i in node_ids if model.node_table().df.at[i, "node_type"] == "Basin"), None)
        if node_id is not None:
            geometry = model.link.df[
                (model.link.df.from_node_id == node_id) | (model.link.df.to_node_id == node_id)
            ].union_all()
            profile_select_df = profiles[profiles.intersects(geometry)]

            if not profile_select_df.empty:
                profile = profile_select_df.loc[profile_select_df.distance(to_node_geometry).idxmin()]

    return getattr(profile, id_col)


def add_link_profile_ids(model: Model, profiles: DAMOProfiles, id_col="globalid"):
    for link_id in tqdm(model.link.df.index, desc="link_profile_ids"):
        profile_id = link_profile_id(link_id, model, profiles, id_col)
        model.link.df.loc[link_id, "meta_profielid_waterbeheerder"] = profile_id
