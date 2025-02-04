import geopandas as gpd
from pydantic import BaseModel, ConfigDict

from ribasim_nl import Model, Network


class DAMOProfiles(BaseModel):
    model: Model
    profile_line_df: gpd.GeoDataFrame
    profile_point_df: gpd.GeoDataFrame
    water_area_df: gpd.GeoDataFrame | None = None
    network: Network | None = None

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def __post_init__(self):
        if self.network is None:
            self.network = Network(lines_gdf=self.model.edge.df)

    def filter_by_water_area(self, profile_points, profile_line):
        apply_filter = False
        if self.water_area_df is not None:
            water_area_poly = self.water_area_df[self.water_area_df.intersects(profile_line.geometry)].union_all()
            if profile_points.within(water_area_poly).any():
                apply_filter = True

        if apply_filter:
            return profile_points[profile_points.within(water_area_poly)].geometry.z.max()
        else:
            return profile_points.geometry.z.max()

    def upstream_levels(self, node_ids):
        return [self.upstream_level(node_id) for node_id in node_ids]

    def upstream_level(self, node_id):
        result = None
        to_point = self.model.node_table().df.at[node_id, "geometry"]
        from_node_id = self.model.upstream_node_id(node_id)
        from_point = self.model.node_table().df.at[from_node_id, "geometry"]
        # %get us_edge
        distance = self.network.nodes.distance(from_point)
        if distance.min() < 0.1:
            node_from = distance.idxmin()
        else:
            node_from = self.network.add_node(from_point, max_distance=10, align_distance=1)

        # get or add node_to
        distance = self.network.nodes.distance(to_point)
        if distance.min() < 0.1:
            node_to = distance.idxmin()
        else:
            node_to = self.network.add_node(to_point, max_distance=10, align_distance=1)

        if (node_from is not None) and (node_to is not None):
            # get line geometry
            geometry = self.network.get_line(node_from, node_to)
            # get us_profile
            profile_select_df = self.profile_line_df[self.profile_line_df.intersects(geometry)]

            if not profile_select_df.empty:
                profile_line = self.profile_line_df.loc[
                    profile_select_df.geometry.apply(lambda x: geometry.project(x.intersection(geometry))).idxmax()
                ]

                profile_points = self.profile_point_df[self.profile_point_df.profiellijnid == profile_line.globalid]

                result = self.filter_by_water_area(profile_points, profile_line)
        if result is None:
            profile_line = self.profile_line_df.loc[self.profile_line_df.distance(to_point).idxmin()]
            profile_points = self.profile_point_df[self.profile_point_df.profiellijnid == profile_line.globalid]

            result = self.filter_by_water_area(profile_points, profile_line)

        return result
