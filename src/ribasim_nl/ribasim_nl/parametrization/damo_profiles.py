import geopandas as gpd
import pandas as pd
from pydantic import BaseModel, ConfigDict
from tqdm import tqdm

from ribasim_nl.model import Model
from ribasim_nl.network import Network


class DAMOProfiles(BaseModel):
    model: Model
    profile_line_df: gpd.GeoDataFrame
    profile_point_df: gpd.GeoDataFrame
    water_area_df: gpd.GeoDataFrame | None = None
    network: Network | None = None
    profile_id_col: str = "meta_profielid_waterbeheerder"
    profile_line_id_col: str = "globalid"

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def model_post_init(self, __context):
        if self.network is None:
            self.network = Network(lines_gdf=self.model.edge.df)

        # drop duplicated profile-lines
        self.profile_line_df.drop_duplicates(self.profile_line_id_col, inplace=True)

        # drop NA geometries
        self.profile_line_df = self.profile_line_df[self.profile_line_df.geometry.notna()]
        self.profile_point_df = self.profile_point_df[self.profile_point_df.geometry.notna()]

        # in principle globalid in line should be in profiellijnid of point. In case they don't match we clean in 2 directions
        self.profile_line_df = self.profile_line_df[
            self.profile_line_df[self.profile_line_id_col].isin(self.profile_point_df.profiellijnid.to_numpy())
        ]

        # explode multipoints
        if "MultiPoint" in self.profile_point_df.geometry.type.unique():
            self.profile_point_df = self.profile_point_df.explode()

        self.profile_point_df = self.profile_point_df[
            self.profile_point_df.profiellijnid.isin(self.profile_line_df[self.profile_line_id_col].to_numpy())
        ]

        # drop nan profile.geometries
        self.profile_line_df = self.profile_line_df[~self.profile_line_df.geometry.isna()]

        # clip points by water area's
        if self.water_area_df is not None:
            sjoined = self.profile_point_df.sjoin(self.water_area_df, how="left", predicate="within")
            self.profile_point_df["within_water"] = ~sjoined.index_right.isna()

    def get_profile_level(self, profile_id, statistic="max"):
        profile_points = self.profile_point_df.set_index("profiellijnid").loc[profile_id]

        if isinstance(profile_points, pd.Series):
            z_values = [profile_points.geometry.z]
        else:
            profile_points_in_water = (
                profile_points[profile_points.within_water] if "within_water" in profile_points else pd.DataFrame()
            )
            z_values = (
                profile_points_in_water.geometry.apply(lambda g: g.z)
                if not profile_points_in_water.empty
                else profile_points.geometry.apply(lambda g: g.z)
            ).tolist()

        if len(z_values) == 1:
            return z_values[0]

        return getattr(pd.Series(z_values), statistic)()

    def get_profile_id(self, node_id, statistic="max"):
        node_type = self.model.get_node_type(node_id)
        if node_type == "Basin":
            profile_ids = self.model.edge.df[
                (self.model.edge.df.from_node_id == node_id) | (self.model.edge.df.to_node_id == node_id)
            ][self.profile_id_col].to_numpy()
            levels = [self.get_profile_level(profile_id, statistic) for profile_id in profile_ids]
            return pd.Series(levels, index=profile_ids).idxmin()  # use pandas to get the profileid with min level
        else:
            return self.model.edge.df[self.model.edge.df.to_node_id == node_id].iloc[0][self.profile_id_col]

    def get_node_level(self, node_id, statistic="max"):
        node_type = self.model.get_node_type(node_id)
        if node_type == "Basin":
            profile_ids = self.model.edge.df[
                (self.model.edge.df.from_node_id == node_id) | (self.model.edge.df.to_node_id == node_id)
            ][self.profile_id_col].to_numpy()
            return min(self.get_profile_level(profile_id, statistic) for profile_id in profile_ids)
        else:
            profile_id = self.model.edge.df[self.model.edge.df.to_node_id == node_id].iloc[0][self.profile_id_col]
            return self.get_profile_level(profile_id, statistic)

    def process_profiles(
        self,
        elevation_col: str | None = None,
        default_profile_slope: float = 0.5,
        min_profile_width: float = 1,
        min_profile_depth: float = 0.5,
    ):
        data = []
        for profiel_id, df in tqdm(self.profile_point_df.groupby("profiellijnid"), desc="process_profiles"):
            if elevation_col is None:
                df.loc[:, "elevation"] = df.geometry.z
            else:
                df.loc[:, "elevation"] = df[elevation_col]

            geometry = self.profile_line_df.set_index(self.profile_line_id_col).at[profiel_id, "geometry"]

            # compute stuff from points
            bottom_level = df["elevation"].min()
            invert_level = df["elevation"].max()
            water_level = df[df.within_water]["elevation"].max()
            depth = max(water_level - bottom_level, min_profile_depth)

            # fix codevolgnummer if it is messed-up
            if df.codevolgnummer.duplicated().any():
                df["distance_on_line"] = [geometry.project(i) for i in df.geometry]
                df.sort_values("distance_on_line", inplace=True)
                df.loc[:, "codevolgnummer"] = [i + 1 for i in range(len(df))]

            # estimate width at water_level from codevolgnummer
            df.set_index("codevolgnummer", inplace=True)

            if df.within_water.any():
                width_at_water_level = df.at[df[df.within_water].index.min(), "geometry"].distance(
                    df.at[df[df.within_water].index.max(), "geometry"]
                )
            else:
                width_at_water_level = geometry.length / 3
                depth = max(invert_level - bottom_level, min_profile_depth)

            # estimate profile_width from width at water_level, depth and slope
            profile_width = width_at_water_level - ((depth / default_profile_slope) * 2)

            # we assume profile_width is more than 1/3 of width at water_level. Correct values accordingly
            if profile_width < width_at_water_level / 3:
                profile_width = max(width_at_water_level / 3, min_profile_width)
                profile_slope = depth / profile_width
            else:
                profile_slope = default_profile_slope

            data += [
                {
                    "profiel_id": profiel_id,
                    "bottom_level": round(bottom_level, 2),
                    "water_level": round(water_level, 2),
                    "invert_level": round(invert_level, 2),
                    "profile_slope": round(profile_slope, 2),
                    "profile_width": round(profile_width, 2),
                    "geometry": geometry,
                }
            ]

        return gpd.GeoDataFrame(data, crs=self.profile_line_df.crs)
