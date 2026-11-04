"""Route the links from the outlets near Groesbeek to the Ooijpolder along the waterways through Germany.

The links are made in `01_feedback.py`. They run far outside the basin area of the Ooijpolder, so the snapping of the
forcing step cannot route them. See https://github.com/Deltares/Ribasim-NL/issues/995.

This uses the network of that snapping, and moves the Basin onto it likewise, so that snapping then keeps these routes.
"""

import geopandas as gpd
import shapely
from ribasim_nl.network_snapping import snap_model
from ribasim_nl.workflow import WaterBoardPaths
from shapely.geometry import box

from ribasim_nl import CloudStorage, Model

OOIJPOLDER = 161
GROESBEEK_OUTLETS = (749, 755, 1019)
# the waterways between the outlets and the Ooijpolder
AREA = box(192500, 420500, 199500, 428500)

cloud = CloudStorage()
paths = WaterBoardPaths(cloud, "Rivierenland", "2024_12_0")
toml = paths.model_dir("profiles") / "ribasim.toml"
model = Model.read(toml)

links = model.link.df
link_ids = links.index[links["from_node_id"].isin(GROESBEEK_OUTLETS) & (links["to_node_id"] == OOIJPOLDER)].tolist()
assert len(link_ids) == len(GROESBEEK_OUTLETS), f"Expected a link from each outlet to Basin {OOIJPOLDER}: {link_ids}"

waterways = gpd.read_file(cloud.joinpath("Basisgegevens/GSK3E/gsk3e.gpkg"), layer="waterways", mask=AREA)
model = snap_model(
    model,
    paths.profielen_dir,
    crossings_path=paths.crossings,
    water_path=paths.bgt_water,
    top10nl_path=paths.top10nl,
    link_ids=link_ids,
    extra_waterways={OOIJPOLDER: waterways},
)
num_points = shapely.get_num_points(model.link.df.loc[link_ids, "geometry"].to_numpy())
assert (num_points > 3).all(), f"Not all links {link_ids} are routed: {num_points} points"
model.write(toml)
