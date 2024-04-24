# %%
import geopandas as gpd
import ribasim
from ribasim_nl import CloudStorage
from shapely.geometry import LineString, MultiLineString

cloud = CloudStorage()

MAX_PROFILE_LENGTH = 50000

ribasim_model_dir = cloud.joinpath("Rijkswaterstaat", "modellen", "hws_prefix")
ribasim_toml = ribasim_model_dir / "hws.toml"
model = ribasim.Model.read(ribasim_toml)


# %%
def get_profile(line, point, profile_length):
    distance = line.project(point, normalized=True)
    offsets = [
        line.parallel_offset(profile_length / 2, side=i) for i in ["left", "right"]
    ]
    points = [i.interpolate(distance, normalized=True) for i in offsets]
    return LineString(points)


def clip_profile(line, point, poly):
    line = line.intersection(poly)
    if isinstance(line, MultiLineString):
        distances = [i.distance(point) for i in line.geoms]
        line = line.geoms[distances.index(min(distances))]
    return line


manning_lines = []
manning_nodes_df = model.node_table().df[
    model.node_table().df["node_type"] == "ManningResistance"
]


# row = next((row for idx, row in enumerate(manning_nodes_df.itertuples()) if idx == 0))

for row in manning_nodes_df.itertuples():
    # get edge_to (ending at manning node) and edge_from (starting at manning node)
    edge_to = model.edge.df[model.edge.df["to_node_id"] == row.node_id].iloc[0]
    edge_from = model.edge.df[model.edge.df["from_node_id"] == row.node_id].iloc[0]

    # length = sum of both lengths
    length = edge_to.geometry.length + edge_from.geometry.length

    # union both basins to polygon for clipping
    basin_area_df = model.basin.area.df.copy()
    basin_area_df.set_index("node_id", inplace=True)
    if edge_from.to_node_type == "LevelBoundary":
        clip_poly = basin_area_df.at[edge_to.from_node_id, "geometry"]
    else:
        clip_poly = (
            basin_area_df.loc[[edge_to.from_node_id, edge_from.to_node_id]]
            .buffer(0.1)
            .unary_union.buffer(-0.1)
        )

    axis_line = LineString([edge_to.geometry.coords[-2], edge_from.geometry.coords[1]])
    profile_line = get_profile(axis_line, row.geometry, MAX_PROFILE_LENGTH)
    profile_line = clip_profile(profile_line, row.geometry, clip_poly)

    profile_width = profile_line.length

    model.manning_resistance.static.df.loc[
        model.manning_resistance.static.df.node_id == row.node_id,
        ["length", "profile_width"],
    ] = length, profile_width

    manning_lines += [{"node_id": row.node_id, "geometry": profile_line}]

# %% wegschrijven model
model.write(ribasim_toml)
manning_lines_gdf = gpd.GeoDataFrame(manning_lines, crs=28992)

manning_lines_gdf.to_file(
    ribasim_model_dir / "manning_profile.gpkg",
    engine="pyogrio",
)

# %%
