"""Parameterisation of water board: Hollandse Delta."""

import geopandas as gpd
import numpy as np
import ribasim_nl.ribasim_parametrization as ribasim_param
from ribasim import Node
from ribasim.nodes import level_boundary, pump, tabulated_rating_curve
from ribasim_nl.geometry import polygon_parts, union_polygons
from ribasim_nl.workflow import UNKNOWN_STREEFPEIL, WaterBoardPaths, process_feedback_form, write_feedback_model
from shapely import MultiPolygon, Point

from ribasim_nl import CloudStorage

AANVOER_CONDITIONS: bool = True
MIXED_CONDITIONS: bool = True

if MIXED_CONDITIONS and not AANVOER_CONDITIONS:
    AANVOER_CONDITIONS = True

# model settings
waterschap = "HollandseDelta"
base_model_versie = "2024_12_3"

# connect with the GoodCloud
cloud = CloudStorage()

paths = WaterBoardPaths(cloud, waterschap, base_model_versie)

default_level = 1.24 if AANVOER_CONDITIONS else -0.42

# process the feedback form and load the model
ribasim_model = process_feedback_form(paths)
# nodes and links are referred to by their key, see docs/dev/node-keys.qmd
n = ribasim_model.node_id

# The feedback form removes two Basins, the KRW water body De Viersprong that HKV split off from Polder de Biesbosch
# as peilgebieden of their own, and their areas with them. Their areas go back to the polder, see #873.
base_gpkg = paths.base_model_toml.parent / "input/database.gpkg"
base_area = gpd.read_file(base_gpkg, layer="Basin / area")
base_key = gpd.read_file(base_gpkg, layer="Node", fid_as_index=True, ignore_geometry=True)["meta_key"]
viersprong = base_area.loc[base_area["node_id"].map(base_key).isin(["NL40_10_3", "NL40_11_3"]), "geometry"]
assert len(viersprong) == 2
assert ribasim_model.basin.area.df is not None
polder = ribasim_model.basin.area.df.index[ribasim_model.basin.area.df["node_id"] == n("P-D21.006")]
assert len(polder) == 1
polder_area = union_polygons(np.array([ribasim_model.basin.area.df.geometry[polder[0]], *viersprong]))
ribasim_model.basin.area.df.loc[polder, "geometry"] = gpd.GeoSeries(
    [MultiPolygon(polygon_parts(polder_area))], index=polder, crs=ribasim_model.crs
)

# model specific tweaks
# merge small basins into larger basins for numerical stability
ribasim_model.merge_basins(node_id=n("P-Y11.015"), to_node_id=n("P-Y11.001"))
ribasim_model.merge_basins(node_id=n("NL40_49_3"), to_node_id=n("P-G33.008"))
ribasim_model.merge_basins(node_id=n("NL40_51_3"), to_node_id=n("P-G32.007"))
ribasim_model.merge_basins(node_id=n("NL40_50_3"), to_node_id=n("P-G32.007"))
# basins too small: aggregate basins
ribasim_model.merge_basins(node_id=n("NL40_54_3"), to_node_id=n("P-G12.008"))
ribasim_model.merge_basins(node_id=n("P-Y04.005"), to_node_id=n("P-Y04.004"))
ribasim_model.merge_basins(node_id=n("P-V09.005"), to_node_id=n("P-V09.007"))
ribasim_model.merge_basins(node_id=n("P-Y12.050"), to_node_id=n("P-Y12.065"))
ribasim_model.merge_basins(node_id=n("P-G12.020"), to_node_id=n("P-G12.019"))
ribasim_model.merge_basins(node_id=n("P-D22.004"), to_node_id=n("P-D22.001"))
ribasim_model.merge_basins(node_id=n("P-H10.013"), to_node_id=n("P-H10.010"))
ribasim_model.merge_basins(node_id=n("P-H13.005"), to_node_id=n("P-H13.001"))
ribasim_model.merge_basins(node_id=n("P-H03.032S"), to_node_id=n("P-H03.030S"))
ribasim_model.merge_basins(node_id=n("P-V05.019"), to_node_id=n("P-V05.012"))
ribasim_model.merge_basins(node_id=n("P-H14.028"), to_node_id=n("P-H14.026"))
ribasim_model.merge_basins(node_id=n("P-Y12.042"), to_node_id=n("P-Y12.035"))
ribasim_model.merge_basins(node_id=n("P-Y11.023"), to_node_id=n("P-Y11.013"))
ribasim_model.merge_basins(node_id=n("P-V16.001"), to_node_id=n("P-V21.003"))
ribasim_model.merge_basins(node_id=n("P-Y12.036"), to_node_id=n("P-Y12.034"))
ribasim_model.merge_basins(node_id=n("P-V01.045"), to_node_id=n("P-V01.046"), are_connected=False)
ribasim_model.merge_basins(node_id=n("P-V01.097"), to_node_id=n("P-V01.046"))
ribasim_model.merge_basins(node_id=n("P-D25.005"), to_node_id=n("P-D25.001"))
ribasim_model.merge_basins(node_id=n("P-V01.122"), to_node_id=n("P-V01.043"))
ribasim_model.merge_basins(node_id=n("P-Y12.053"), to_node_id=n("P-Y12.065"))
ribasim_model.merge_basins(node_id=n("P-Y02.008"), to_node_id=n("P-Y02.003"))
ribasim_model.merge_basins(node_id=n("P-Y12.048"), to_node_id=n("P-Y12.065"))
ribasim_model.merge_basins(node_id=n("P-Y12.047"), to_node_id=n("P-Y12.065"))
ribasim_model.merge_basins(node_id=n("P-V01.104"), to_node_id=n("P-V01.029"))
ribasim_model.merge_basins(node_id=n("P-Y12.052"), to_node_id=n("P-Y12.065"))
ribasim_model.merge_basins(node_id=n("P-V03.021"), to_node_id=n("P-V03.009"))
ribasim_model.merge_basins(node_id=n("P-H03.007"), to_node_id=n("P-H03.003"))
ribasim_model.merge_basins(node_id=n("P-V09.007"), to_node_id=n("P-V09.004"))
ribasim_model.merge_basins(node_id=n("P-Y11.010"), to_node_id=n("P-Y11.001"))
ribasim_model.merge_basins(node_id=n("P-H14.025"), to_node_id=n("P-H14.024"))
ribasim_model.merge_basins(node_id=n("P-H10.019"), to_node_id=n("P-H10.018"))
ribasim_model.merge_basins(node_id=n("P-H03.033"), to_node_id=n("P-H03.001"))
ribasim_model.merge_basins(node_id=n("P-H03.035"), to_node_id=n("P-H03.034"))
ribasim_model.merge_basins(node_id=n("P-H03.034"), to_node_id=n("P-H03.001"))
ribasim_model.merge_basins(node_id=n("P-Y11.043"), to_node_id=n("P-Y14.004"))
ribasim_model.merge_basins(node_id=n("P-Y04.006"), to_node_id=n("P-Y04.004"))
ribasim_model.merge_basins(node_id=n("P-Y11.018"), to_node_id=n("P-Y11.017"))
ribasim_model.merge_basins(node_id=n("NL40_47_3"), to_node_id=n("P-G44.005"))
ribasim_model.merge_basins(node_id=n("P-H10.021"), to_node_id=n("P-H10.001"))
ribasim_model.merge_basins(node_id=n("P-Y12.022"), to_node_id=n("P-Y12.002"))
ribasim_model.merge_basins(node_id=n("P-V01.099"), to_node_id=n("P-V01.037"))
ribasim_model.merge_basins(node_id=n("P-Y12.041"), to_node_id=n("P-Y12.028"))
ribasim_model.merge_basins(node_id=n("P-V03.030"), to_node_id=n("P-V03.029"))
ribasim_model.merge_basins(node_id=n("P-Y11.009"), to_node_id=n("P-Y11.020"))
ribasim_model.merge_basins(node_id=n("P-Y11.022"), to_node_id=n("P-Y11.021"))
ribasim_model.merge_basins(node_id=n("P-Y12.034"), to_node_id=n("P-Y12.039"))
ribasim_model.merge_basins(node_id=n("P-Y12.039"), to_node_id=n("P-Y12.033"))
ribasim_model.merge_basins(node_id=n("P-Y12.040"), to_node_id=n("P-Y12.033"))
ribasim_model.merge_basins(node_id=n("P-H10.023"), to_node_id=n("P-H10.001"))
ribasim_model.merge_basins(node_id=n("P-H17.021"), to_node_id=n("P-H17.009"))
ribasim_model.merge_basins(node_id=n("P-H10.015"), to_node_id=n("P-H10.005"))
ribasim_model.merge_basins(node_id=n("P-H10.014"), to_node_id=n("P-H10.010"))
ribasim_model.merge_basins(node_id=n("P-H17.038S"), to_node_id=n("P-H17.023S"))
ribasim_model.merge_basins(node_id=n("P-H03.005"), to_node_id=n("P-H03.002"))
ribasim_model.merge_basins(node_id=n("P-Y12.049"), to_node_id=n("P-Y12.051"))
ribasim_model.merge_basins(node_id=n("P-H17.013"), to_node_id=n("P-H17.012"))
ribasim_model.merge_basins(node_id=n("P-H10.020"), to_node_id=n("P-H10.001"))
ribasim_model.merge_basins(node_id=n("P-H11.004"), to_node_id=n("P-H11.003"))
ribasim_model.merge_basins(node_id=n("P-V03.020"), to_node_id=n("P-V03.019"))
ribasim_model.merge_basins(node_id=n("P-Y12.032"), to_node_id=n("P-Y12.020"))
ribasim_model.merge_basins(node_id=n("P-Y11.011"), to_node_id=n("P-Y11.021"))
ribasim_model.merge_basins(node_id=n("P-R01.007"), to_node_id=n("P-R01.004"))
ribasim_model.merge_basins(node_id=n("P-H15.003"), to_node_id=n("P-H15.004"))
ribasim_model.merge_basins(node_id=n("P-Y12.055"), to_node_id=n("P-Y12.007"))
ribasim_model.merge_basins(node_id=n("P-Y12.010"), to_node_id=n("P-Y12.001"))
ribasim_model.merge_basins(node_id=n("P-D22.002"), to_node_id=n("P-D22.001"))
ribasim_model.merge_basins(node_id=n("P-V03.025"), to_node_id=n("P-V21.001"))
ribasim_model.merge_basins(node_id=n("P-V01.098"), to_node_id=n("P-V01.027"))
ribasim_model.merge_basins(node_id=n("P-V03.017"), to_node_id=n("P-V03.002"))
ribasim_model.merge_basins(node_id=n("P-H15.005"), to_node_id=n("P-H15.002"))
ribasim_model.merge_basins(node_id=n("nan_606"), to_node_id=n("P-Y07.018"))
ribasim_model.merge_basins(node_id=n("P-H12.002"), to_node_id=n("P-H12.001"))
ribasim_model.merge_basins(node_id=n("P-Y12.045"), to_node_id=n("P-Y12.044"))
ribasim_model.merge_basins(node_id=n("P-V11.003"), to_node_id=n("P-V11.002"))
ribasim_model.merge_basins(node_id=n("P-V01.105"), to_node_id=n("P-V01.089"))
ribasim_model.merge_basins(node_id=n("P-V05.003"), to_node_id=n("P-V21.001"))
ribasim_model.merge_basins(node_id=n("P-V01.076"), to_node_id=n("P-V01.022"))
ribasim_model.merge_basins(node_id=n("P-Y07.028"), to_node_id=n("P-Y07.026"))
ribasim_model.merge_basins(node_id=n("P-H03.011S"), to_node_id=n("P-H03.012S"))
ribasim_model.merge_basins(node_id=n("P-Y11.013"), to_node_id=n("P-Y11.001"))
ribasim_model.merge_basins(node_id=n("P-Y12.058"), to_node_id=n("P-Y12.030"))
ribasim_model.merge_basins(node_id=n("P-V06.007"), to_node_id=n("P-V06.008"))
ribasim_model.merge_basins(node_id=n("NL40_12_3"), to_node_id=n("P-H18.002"))
ribasim_model.merge_basins(node_id=n("P-V11.004"), to_node_id=n("P-V11.002"))
ribasim_model.merge_basins(node_id=n("P-H18.003"), to_node_id=n("P-H18.002"))
ribasim_model.merge_basins(node_id=n("P-H14.023"), to_node_id=n("P-H14.002"))
ribasim_model.merge_basins(node_id=n("P-Y04.004"), to_node_id=n("P-Y04.001"))
ribasim_model.merge_basins(node_id=n("P-H17.029S"), to_node_id=n("P-H17.030S"))
ribasim_model.merge_basins(node_id=n("P-V03.023"), to_node_id=n("P-V03.012"))
ribasim_model.merge_basins(node_id=n("P-H17.019"), to_node_id=n("P-H17.003"))
ribasim_model.merge_basins(node_id=n("P-Y07.006"), to_node_id=n("P-Y07.018"))
ribasim_model.merge_basins(node_id=n("P-V05.010"), to_node_id=n("P-V05.008"))
ribasim_model.merge_basins(node_id=n("P-H10.018"), to_node_id=n("P-H10.017"))
ribasim_model.merge_basins(node_id=n("P-V03.031"), to_node_id=n("P-V03.001"))
ribasim_model.merge_basins(node_id=n("P-H03.023"), to_node_id=n("P-H03.002"))
ribasim_model.merge_basins(node_id=n("P-H03.022"), to_node_id=n("P-H03.002"))
ribasim_model.merge_basins(node_id=n("P-H07.002"), to_node_id=n("P-H07.003"))
ribasim_model.merge_basins(node_id=n("P-V01.074"), to_node_id=n("P-V01.066"))
ribasim_model.merge_basins(node_id=n("P-V01.115"), to_node_id=n("P-V01.050"))
ribasim_model.merge_basins(node_id=n("NL40_23_3"), to_node_id=n("P-V05.008"))
ribasim_model.merge_basins(node_id=n("P-H10.011"), to_node_id=n("P-H10.016"))
ribasim_model.merge_basins(node_id=n("NL40_21_3"), to_node_id=n("P-V05.008"))
ribasim_model.merge_basins(node_id=n("P-Y12.057"), to_node_id=n("P-Y12.001"))
ribasim_model.merge_basins(node_id=n("P-V01.059"), to_node_id=n("P-V01.057"))
ribasim_model.merge_basins(node_id=n("P-H17.010"), to_node_id=n("P-H17.006"))
ribasim_model.merge_basins(node_id=n("P-H13.004"), to_node_id=n("P-H13.002"))
ribasim_model.merge_basins(node_id=n("P-V01.109"), to_node_id=n("P-V01.047"))
ribasim_model.merge_basins(node_id=n("P-V01.119"), to_node_id=n("P-V01.047"))
ribasim_model.merge_basins(node_id=n("P-Y07.027"), to_node_id=n("P-Y07.026"))
ribasim_model.merge_basins(node_id=n("P-Y14.009"), to_node_id=n("P-Y14.008"))
ribasim_model.merge_basins(node_id=n("P-H03.004"), to_node_id=n("P-H03.019S"))
ribasim_model.merge_basins(node_id=n("P-G22.010"), to_node_id=n("P-G46.002"))
ribasim_model.merge_basins(node_id=n("P-Y03.007"), to_node_id=n("P-Y03.004"))
ribasim_model.merge_basins(node_id=n("P-Y03.011"), to_node_id=n("P-Y03.004"))
ribasim_model.merge_basins(node_id=n("P-H03.028S"), to_node_id=n("P-H03.026S"))
ribasim_model.merge_basins(node_id=n("P-H02.003"), to_node_id=n("P-H02.002"))
ribasim_model.merge_basins(node_id=n("P-G12.033"), to_node_id=n("P-G12.008"))
ribasim_model.merge_basins(node_id=n("NL40_09_3"), to_node_id=n("P-H17.009"))
ribasim_model.merge_basins(node_id=n("P-H17.041"), to_node_id=n("P-H17.009"))
ribasim_model.merge_basins(node_id=n("P-H03.020S"), to_node_id=n("P-H03.027S"))
ribasim_model.merge_basins(node_id=n("P-Y12.054"), to_node_id=n("P-Y12.065"))
ribasim_model.merge_basins(node_id=n("P-Y12.065"), to_node_id=n("P-Y12.001"))
ribasim_model.merge_basins(node_id=n("NL40_14_3"), to_node_id=n("P-D27.006"))
ribasim_model.merge_basins(node_id=n("P-H17.027S"), to_node_id=n("P-H17.024S"))
ribasim_model.merge_basins(node_id=n("P-V01.118"), to_node_id=n("P-V01.038"))
ribasim_model.merge_basins(node_id=n("P-V01.049"), to_node_id=n("P-V01.050"))
ribasim_model.merge_basins(node_id=n("P-G11.012"), to_node_id=n("P-G11.005"))
ribasim_model.merge_basins(node_id=n("P-V01.100"), to_node_id=n("P-V01.033"))
ribasim_model.merge_basins(node_id=n("P-V01.102"), to_node_id=n("P-V01.027"))
ribasim_model.merge_basins(node_id=n("P-H11.005"), to_node_id=n("P-H11.007"))
ribasim_model.merge_basins(node_id=n("P-Y03.009"), to_node_id=n("P-Y03.008"))
ribasim_model.merge_basins(node_id=n("P-Y03.008"), to_node_id=n("P-Y03.001"))
ribasim_model.merge_basins(node_id=n("P-H17.017"), to_node_id=n("P-H17.032S"))
ribasim_model.merge_basins(node_id=n("P-V01.071"), to_node_id=n("P-V01.022"))
ribasim_model.merge_basins(node_id=n("P-V01.086"), to_node_id=n("P-V01.087"))
ribasim_model.merge_basins(node_id=n("P-V01.070"), to_node_id=n("P-V01.066"))
ribasim_model.merge_basins(node_id=n("P-R01.006"), to_node_id=n("P-R01.004"))
ribasim_model.merge_basins(node_id=n("P-V06.015"), to_node_id=n("P-V06.010"))
ribasim_model.merge_basins(node_id=n("P-Y07.004"), to_node_id=n("P-Y06.001"))
ribasim_model.merge_basins(node_id=n("P-D25.003"), to_node_id=n("P-D25.002"))
ribasim_model.merge_basins(node_id=n("P-Y06.002"), to_node_id=n("P-Y06.001"))
ribasim_model.merge_basins(node_id=n("P-V01.111"), to_node_id=n("P-V01.012"), are_connected=False)
ribasim_model.merge_basins(node_id=n("P-H17.015"), to_node_id=n("P-H17.009"))
ribasim_model.merge_basins(node_id=n("P-Y12.021"), to_node_id=n("P-Y11.001"))
ribasim_model.merge_basins(node_id=n("P-Y12.051"), to_node_id=n("P-Y12.001"))
ribasim_model.merge_basins(node_id=n("P-Y07.008"), to_node_id=n("P-Y07.018"))
ribasim_model.merge_basins(node_id=n("P-Y11.017"), to_node_id=n("P-Y11.004"))
ribasim_model.merge_basins(node_id=n("P-Y11.016"), to_node_id=n("P-Y11.003"))
ribasim_model.merge_basins(node_id=n("P-H17.016"), to_node_id=n("P-H17.008"))
ribasim_model.merge_basins(node_id=n("P-H10.022"), to_node_id=n("P-H10.001"))
ribasim_model.merge_basins(node_id=n("P-D24.004"), to_node_id=n("P-D24.002"))
ribasim_model.merge_basins(node_id=n("P-H03.018S"), to_node_id=n("P-H03.019S"))
ribasim_model.merge_basins(node_id=n("P-V01.089"), to_node_id=n("P-V01.087"))
ribasim_model.merge_basins(node_id=n("P-H02.013"), to_node_id=n("P-H02.004"))
ribasim_model.merge_basins(node_id=n("P-H11.003"), to_node_id=n("P-H11.002"))
ribasim_model.merge_basins(node_id=n("P-H03.006"), to_node_id=n("P-H03.002"))
ribasim_model.merge_basins(node_id=n("P-V01.040"), to_node_id=n("P-V01.037"))
ribasim_model.merge_basins(node_id=n("P-V01.121"), to_node_id=n("P-V01.001"))
ribasim_model.merge_basins(node_id=n("P-V14.001"), to_node_id=n("P-V15.001"))
ribasim_model.merge_basins(node_id=n("P-H17.026S"), to_node_id=n("P-H17.031S"))
ribasim_model.merge_basins(node_id=n("P-V01.031"), to_node_id=n("P-V01.030"))
ribasim_model.merge_basins(node_id=n("P-V11.001"), to_node_id=n("P-V11.002"))
ribasim_model.merge_basins(node_id=n("P-G13.005"), to_node_id=n("P-G13.006"))
ribasim_model.merge_basins(node_id=n("P-Y07.035"), to_node_id=n("P-Y07.033"))
ribasim_model.merge_basins(node_id=n("P-V01.082"), to_node_id=n("P-V01.084"))
ribasim_model.merge_basins(node_id=n("P-D22.006"), to_node_id=n("P-D27.005"))
ribasim_model.merge_basins(node_id=n("P-V06.028"), to_node_id=n("P-V06.001"))
ribasim_model.merge_basins(node_id=n("P-V03.027"), to_node_id=n("P-V03.011"))
ribasim_model.merge_basins(node_id=n("P-H17.020"), to_node_id=n("P-H17.009"))
# merge overlapping basins
ribasim_model.merge_basins(node_id=n("Voorne Oost-Noord_496"), to_node_id=n("P-V03.034"))
ribasim_model.merge_basins(node_id=n("Deltaziekenhuis_601"), to_node_id=n("P-Y05.001"))
ribasim_model.merge_basins(node_id=n("P-V03.010"), to_node_id=n("P-V19.001"))  # small basin area

ribasim_model.merge_basins(node_id=n("P-R01.002"), to_node_id=n("P-R01.005"))  # numerical limiting outlet #1350
ribasim_model.merge_basins(node_id=n("P-R01.008"), to_node_id=n("P-R01.005"))  # numerical limiting outlet #1453
ribasim_model.merge_basins(node_id=n("P-R01.003"), to_node_id=n("P-R01.005"))  # numerical limiting outlet #2366
ribasim_model.merge_basins(node_id=n("P-V06.017"), to_node_id=n("P-V06.025"))  # numerical limiting MR #6685
ribasim_model.merge_basins(node_id=n("P-V06.021"), to_node_id=n("P-V06.016"))  # numerical limiting MR #6588
ribasim_model.merge_basins(node_id=n("P-V01.030"), to_node_id=n("P-V01.034"))  # numerical limiting outlet #1628

ribasim_model.merge_basins(node_id=n("P-H03.017S"), to_node_id=n("P-H03.019S"))  # wrong target level

# circular pumping of water between basins
ribasim_model.merge_basins(node_id=n("P-V09.004"), to_node_id=n("P-V09.006"))
ribasim_model.merge_basins(node_id=n("P-V09.003"), to_node_id=n("P-V09.006"))
ribasim_model.merge_basins(node_id=n("P-V09.002"), to_node_id=n("P-V09.006"))

ribasim_model.merge_basins(node_id=n("P-V01.051"), to_node_id=n("P-V01.095"))  # limiting node for coupling

# convergence issues
ribasim_model.merge_basins(node_id=n("P-V03.019"), to_node_id=n("P-V03.009"))
ribasim_model.merge_basins(node_id=n("P-H15.004"), to_node_id=n("P-H15.002"))
ribasim_model.merge_basins(node_id=n("P-H15.006S"), to_node_id=n("P-H15.002"))
ribasim_model.merge_basins(node_id=n("P-H17.039S"), to_node_id=n("P-H17.001"))
ribasim_model.merge_basins(node_id=n("P-G12.006"), to_node_id=n("P-G12.005"))
ribasim_model.merge_basins(node_id=n("P-G12.035"), to_node_id=n("P-G12.005"))
ribasim_model.merge_basins(node_id=n("P-Y11.008"), to_node_id=n("P-Y11.014"))
ribasim_model.merge_basins(node_id=n("P-Y08.004"), to_node_id=n("P-Y08.001"))
ribasim_model.merge_basins(node_id=n("P-Y11.012"), to_node_id=n("P-Y14.004"))
ribasim_model.merge_basins(node_id=n("P-Y14.005"), to_node_id=n("P-Y14.004"))
ribasim_model.merge_basins(node_id=n("P-G12.034"), to_node_id=n("P-G12.008"))
ribasim_model.merge_basins(node_id=n("P-H11.002"), to_node_id=n("P-H11.007"))
ribasim_model.merge_basins(node_id=n("P-Y14.003"), to_node_id=n("P-Y14.006"))
ribasim_model.merge_basins(node_id=n("P-G12.031"), to_node_id=n("P-G12.025"))
ribasim_model.merge_basins(node_id=n("P-G12.030"), to_node_id=n("P-G12.025"))
ribasim_model.merge_basins(node_id=n("P-R01.004"), to_node_id=n("P-R01.005"))
ribasim_model.merge_basins(node_id=n("P-D20.005"), to_node_id=n("P-D20.006"))
ribasim_model.merge_basins(node_id=n("P-Y07.034"), to_node_id=n("P-Y07.033"))
ribasim_model.merge_basins(node_id=n("P-Y12.023"), to_node_id=n("P-Y12.024"))
ribasim_model.merge_basins(node_id=n("P-Y11.002"), to_node_id=n("P-Y14.002"))
ribasim_model.merge_basins(node_id=n("P-V01.085"), to_node_id=n("P-V01.083"))
ribasim_model.merge_basins(node_id=n("P-V01.087"), to_node_id=n("P-V01.083"))
ribasim_model.merge_basins(node_id=n("P-V21.002"), to_node_id=n("P-V21.003"))
ribasim_model.merge_basins(node_id=n("P-Y01.007"), to_node_id=n("P-Y01.004"))
ribasim_model.merge_basins(node_id=n("P-V06.027"), to_node_id=n("P-V06.026"))
ribasim_model.merge_basins(node_id=n("P-Y12.003"), to_node_id=n("P-Y12.005"))
ribasim_model.merge_basins(node_id=n("P-Y12.006"), to_node_id=n("P-Y12.005"))
ribasim_model.merge_basins(node_id=n("P-Y12.064"), to_node_id=n("P-Y12.001"))
ribasim_model.merge_basins(node_id=n("P-H17.031S"), to_node_id=n("P-H17.025S"))
ribasim_model.merge_basins(node_id=n("P-D26.001"), to_node_id=n("P-D25.004"))
ribasim_model.merge_basins(node_id=n("P-Y11.039"), to_node_id=n("P-Y11.030"))
ribasim_model.merge_basins(node_id=n("P-V05.009"), to_node_id=n("P-V05.008"))
ribasim_model.merge_basins(node_id=n("NL40_48_3"), to_node_id=n("P-G43.006"))
ribasim_model.merge_basins(node_id=n("P-H17.036S"), to_node_id=n("P-H17.028S"))
ribasim_model.merge_basins(node_id=n("P-V02.007"), to_node_id=n("P-V02.005"))
ribasim_model.merge_basins(node_id=n("P-V01.002"), to_node_id=n("P-V01.006"))
ribasim_model.merge_basins(node_id=n("P-H17.014"), to_node_id=n("P-H17.022"))
ribasim_model.merge_basins(node_id=n("P-V01.041"), to_node_id=n("P-V01.022"))
ribasim_model.merge_basins(node_id=n("P-V03.007"), to_node_id=n("P-V03.008"))
ribasim_model.merge_basins(node_id=n("P-H10.012"), to_node_id=n("P-H10.010"))
ribasim_model.merge_basins(node_id=n("P-P01.001"), to_node_id=n("P-P01.002"))
ribasim_model.merge_basins(node_id=n("P-Y09.024"), to_node_id=n("P-Y09.025"))
ribasim_model.merge_basins(node_id=n("P-V01.080"), to_node_id=n("P-V01.083"))
ribasim_model.merge_basins(node_id=n("P-V02.008"), to_node_id=n("P-V02.005"))
ribasim_model.merge_basins(node_id=n("P-Y12.011"), to_node_id=n("P-Y12.001"))

# check basin area
ribasim_param.validate_basin_area(ribasim_model)

# check streefpeilen at manning nodes
ribasim_param.validate_manning_basins(ribasim_model)

# change unknown streefpeilen to a default streefpeil
assert ribasim_model.basin.area.df is not None
ribasim_model.basin.area.df.loc[
    ribasim_model.basin.area.df["meta_streefpeil"] == "Onbekend streefpeil", "meta_streefpeil"
] = str(UNKNOWN_STREEFPEIL)
ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df["meta_streefpeil"] == -9.999, "meta_streefpeil"] = str(
    UNKNOWN_STREEFPEIL
)

# change high initial states to 0
assert ribasim_model.basin.state.df is not None
ribasim_model.basin.state.df.loc[ribasim_model.basin.state.df["level"] == 9.999, "level"] = 0.0
ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df["meta_streefpeil"] == 9.999, "meta_streefpeil"] = str(
    UNKNOWN_STREEFPEIL
)

# an error occured in the feedback form, which prevented a LevelBoundary and pump being schematized. Add it here.
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(55992, 424882)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(55982, 424908)), [pump.Static(flow_rate=[0.1])])
assert ribasim_model.pump.static.df is not None
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == pump_node.node_id, "meta_func_afvoer"] = 0
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == pump_node.node_id, "meta_func_aanvoer"] = 1
ribasim_model.link.add(level_boundary_node, pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[n("P-G12.019")])

# both an afvoer as well as aanvoer gemaal. Aanvoer gemaal already in model, add afvoer
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(81148, 418947)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(81179, 419027)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[n("NL40_02_3")], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# 7th biggest gemaal of Hollandse Delta is missing: add it
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(81551, 425469)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(81526, 425553)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[n("P-V06.012")], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# add gemaal and LB near Brienenoordbrug
pump_node = ribasim_model.pump.add(Node(geometry=Point(96675, 434551)), [pump.Static(flow_rate=[20])])
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(96690, 434593)), [level_boundary.Static(level=[default_level])]
)
ribasim_model.link.add(ribasim_model.basin[n("P-Y10.014")], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# add gemaal and LB near the south of Rotterdam
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(93596, 434790)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(
    Node(geometry=Point(93610.26, 434788.89)),
    [pump.Static(flow_rate=[20])],
)
ribasim_model.link.add(ribasim_model.basin[n("P-Y09.017")], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# add gemaal and LB at a football field
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(102168, 421137)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(102158, 421104)), [pump.Static(flow_rate=[0.1])])
ribasim_param.change_pump_func(ribasim_model, pump_node.node_id, "afvoer", 0)
ribasim_param.change_pump_func(ribasim_model, pump_node.node_id, "aanvoer", 1)
ribasim_model.link.add(level_boundary_node, pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[n("P-H17.025S")])

# add gemaal and LB
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(91963, 414083)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(91929, 414495)), [pump.Static(flow_rate=[5])])
ribasim_model.link.add(ribasim_model.basin[n("NL40_05_3")], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# add afvoergemaal
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(78275, 423949)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(78251, 423919)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[n("P-H01.001")], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# add a TRC and LB to a small harbour
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(103964, 429864)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(103927, 429888)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[n("P-Y12.002")])

# add gemaal and LB at the Voornse Meer
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(64560, 439130)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(64555, 439130)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[n("P-V22.001")], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# changes after samenwerkdag of February
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(110570.5, 421238.1)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(110535.2, 421208.5)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[n("P-D21.007")])

level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(87175.8, 415515.9)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(87187.6, 415538.6)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[n("P-H05.002")])

level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(89336.4, 415678.2)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(89318.7, 415662.7)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[n("P-H06.001")])

level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(89352.6, 415703.5)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(89348.1, 415718)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[n("P-H04.011S")])

level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(77197.2, 424267.6)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(77197.2, 424241.6)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[n("P-V17.001")])

level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(93464.1, 428403.3)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(93464.1, 428423.3)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[n("P-Y07.018")])

# addition of LLB-TRC combination
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(73429, 420133)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(73429, 420123)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[n("P-G32.014")])

# TEMP add LB
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(88565, 429038)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(88566.5, 429040)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[n("P-Y07.015")])

# reversing Pump and Outlet (not possible in FF)
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(98844, 425287)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(98846, 425288)), [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(98845, 425289)), [pump.Static(flow_rate=[20])])
ribasim_model.link.add(level_boundary_node, pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[n("P-Y13.008")])
ribasim_model.link.add(ribasim_model.basin[n("P-Y13.008")], tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, level_boundary_node)

# Pump/Outlet without LB
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(93607, 434789)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(93610, 434789)), [pump.Static(flow_rate=[20])])
ribasim_model.link.add(level_boundary_node, pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[n("P-Y09.017")])

# complex situation: Inlaat van Palandt
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(74124, 419410)), [level_boundary.Static(level=[default_level])]
)
# > Inlaat van Palandt I
pump_node = ribasim_model.pump.add(Node(geometry=Point(74047, 419343)), [pump.Static(flow_rate=[20])])
ribasim_model.link.add(level_boundary_node, pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[n("P-G47.001")])
# > Inlaat van Palandt II
pump_node = ribasim_model.pump.add(Node(geometry=Point(74047, 419343)), [pump.Static(flow_rate=[20])])
ribasim_model.link.add(level_boundary_node, pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[n("P-G32.007")])

# missing Outlets (adjacent to Pumps)
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(106575, 418823)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(106615, 418823)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[n("P-D21.006")])

level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(105338, 418523)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(105288, 418523)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[n("P-D20.006")])

# Changed direction of two pumps: See updated feedback form.
ribasim_param.change_pump_func(ribasim_model, n("08022GM"), "aanvoer", 1)
ribasim_param.change_pump_func(ribasim_model, n("08022GM"), "afvoer", 1)
ribasim_param.change_pump_func(ribasim_model, n("11901ST"), "aanvoer", 0)
ribasim_param.change_pump_func(ribasim_model, n("11901ST"), "afvoer", 1)
ribasim_param.change_pump_func(ribasim_model, n("05766GM"), "aanvoer", 0)
ribasim_param.change_pump_func(ribasim_model, n("05766GM"), "afvoer", 1)
ribasim_param.change_pump_func(ribasim_model, n("05001GM"), "aanvoer", 0)
ribasim_param.change_pump_func(ribasim_model, n("10165GM"), "aanvoer", 0)
ribasim_param.change_pump_func(ribasim_model, n("00450GM"), "aanvoer", 0)
ribasim_param.change_pump_func(ribasim_model, n("03463GM"), "aanvoer", 0)
ribasim_param.change_pump_func(ribasim_model, n("03463GM"), "afvoer", 1)
ribasim_param.change_pump_func(ribasim_model, n("03464GM|P-D21.006>"), "aanvoer", 0)
ribasim_param.change_pump_func(ribasim_model, n("09224GM"), "aanvoer", 0)
ribasim_param.change_pump_func(ribasim_model, n("09304GM"), "aanvoer", 0)
ribasim_param.change_pump_func(ribasim_model, n("00533GM"), "aanvoer", 0)
ribasim_param.change_pump_func(ribasim_model, n("09252GM"), "aanvoer", 0)
ribasim_param.change_pump_func(ribasim_model, n("08508GM"), "afvoer", 1)
ribasim_param.change_pump_func(ribasim_model, n("11019GM|P-H18.001>"), "afvoer", 0)

# change pumps from aanvoer to afvoer
aanvoer_to_afvoer_pump = [n("30162GM"), n("03295GM"), n("03296GM"), n("12806GM"), n("07611GM"), n("05505GM")]
for pumpid in aanvoer_to_afvoer_pump:
    ribasim_param.change_pump_func(ribasim_model, pumpid, "aanvoer", 0)
    ribasim_param.change_pump_func(ribasim_model, pumpid, "afvoer", 1)

# basins are connected by both a `Pump`- and `Manning`-node: removed `Pump`-nodes due to same 'streefpeil'-values
ribasim_model.remove_node(n("30174GM"), True)
ribasim_model.remove_node(n("00047GM"), True)
ribasim_model.remove_node(n("30205GM"), True)
ribasim_model.remove_node(n("03306GM"), True)

ribasim_model.remove_node(n("loc:103744,429819"), True)

# (re) set 'meta_node_id'
for node_type in ["LevelBoundary", "TabulatedRatingCurve", "Pump"]:
    mask = ribasim_model.node.df["node_type"] == node_type
    ribasim_model.node.df.loc[mask, "meta_node_id"] = ribasim_model.node.df.loc[mask].index

# change unknown streefpeilen to a default streefpeil
ribasim_model.basin.area.df.loc[
    ribasim_model.basin.area.df["meta_streefpeil"] == "Onbekend streefpeil", "meta_streefpeil"
] = str(UNKNOWN_STREEFPEIL)
ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df["meta_streefpeil"] == -9.999, "meta_streefpeil"] = str(
    UNKNOWN_STREEFPEIL
)

# convert all boundary nodes to LevelBoundaries
ribasim_param.Terminals_to_LevelBoundaries(ribasim_model=ribasim_model, default_level=default_level)  # clean
ribasim_param.FlowBoundaries_to_LevelBoundaries(ribasim_model=ribasim_model, default_level=default_level)

# add outlet
ribasim_param.add_outlets(ribasim_model, delta_crest_level=0.10)

ribasim_param.clean_tables(ribasim_model, waterschap)
write_feedback_model(ribasim_model, paths)
