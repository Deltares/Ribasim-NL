import geopandas as gpd
import pandas as pd
from pydantic import BaseModel, ConfigDict
from shapely.geometry import box

from ribasim_nl import Model, Network


class DAMOProfiles(BaseModel):
    model: Model
    profile_line_df: gpd.GeoDataFrame
    profile_point_df: gpd.GeoDataFrame
    water_area_df: gpd.GeoDataFrame | None = None
    network: Network | None = None
    profile_id_col: str = "meta_profielid_waterbeheerder"

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def model_post_init(self, __context):
        if self.network is None:
            self.network = Network(lines_gdf=self.model.edge.df)

        # in principle globalid in line should be in profiellijnid of point. In case they don't match we clean in 2 directions
        self.profile_line_df = self.profile_line_df[
            self.profile_line_df.globalid.isin(self.profile_point_df.profiellijnid.to_numpy())
        ]

        self.profile_point_df = self.profile_point_df[
            self.profile_point_df.profiellijnid.isin(self.profile_line_df.globalid.to_numpy())
        ]

        # clip points by water area's
        if self.water_area_df is not None:
            self.water_area_df = self.water_area_df[
                self.water_area_df.intersects(box(*self.profile_line_df.total_bounds))
            ]

    def filter_by_water_area(self, profile_points, profile_line):
        apply_filter = False
        if self.water_area_df is not None:
            water_area_poly = self.water_area_df[self.water_area_df.intersects(profile_line.geometry)].union_all()
            if profile_points.within(water_area_poly).any():
                apply_filter = True

        if apply_filter:
            return profile_points[profile_points.within(water_area_poly)]
        else:
            return profile_points

    def get_profile_level(self, profile_id, statistic="max"):
        profile_line = self.profile_line_df.set_index("globalid").loc[profile_id]
        profile_points = self.profile_point_df.set_index("profiellijnid").loc[profile_id]
        profile_points = self.filter_by_water_area(profile_points, profile_line)

        return getattr(profile_points.geometry.z, statistic)()

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
