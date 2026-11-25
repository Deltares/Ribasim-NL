"""Parameterisation of water board: Fryslan."""

import geopandas as gpd
import ribasim_nl.ribasim_parametrization as ribasim_param
from ribasim import Node
from ribasim.nodes import level_boundary, pump, tabulated_rating_curve
from ribasim_nl.split_basins import NodeMetaCache, SplitBasins
from ribasim_nl.workflow import UNKNOWN_STREEFPEIL, WaterBoardPaths, process_feedback_form, write_feedback_model
from shapely import Point

from ribasim_nl import CloudStorage

AANVOER_CONDITIONS: bool = True
MIXED_CONDITIONS: bool = True

if MIXED_CONDITIONS and not AANVOER_CONDITIONS:
    AANVOER_CONDITIONS = True

# model settings
waterschap = "WetterskipFryslan"
base_model_versie = "2025_5_1"

# connect with the GoodCloud
cloud = CloudStorage()

paths = WaterBoardPaths(cloud, waterschap, base_model_versie)
gaarkeuken_path = cloud.joinpath(waterschap, "aangeleverd/Na_levering/gaarkeuken.gpkg")

splitted_basin_6_path = cloud.joinpath(waterschap, "verwerkt/Splitting_basins/Opgeknipte_basin_6.gpkg")
splitted_basin_16_path = cloud.joinpath(waterschap, "verwerkt/Splitting_basins/Opgeknipte_basin_16.gpkg")
splitted_basin_21_path = cloud.joinpath(waterschap, "verwerkt/Splitting_basins/Opgeknipte_basin_21.gpkg")


default_level = 10 if AANVOER_CONDITIONS else -2.3456  # default LevelBoundary level

# process the feedback form and load the model
ribasim_model = process_feedback_form(paths)
# nodes and links are referred to by their key, see docs/dev/node-keys.qmd
n = ribasim_model.node_id

# Uitlaat toevoegen at Ter Schelling
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(155665, 601591)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(155658, 601976)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(ribasim_model.basin[n("PBH0090604")], tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, level_boundary_node)

# Uitlaat toevoegen at Vlieland
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(128115, 586097)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(127144, 585944)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(ribasim_model.basin[n("PBH0094029")], tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, level_boundary_node)

# Uitlaat toevoegen at Vlieland
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(129831, 587677)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(129804, 587869)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(ribasim_model.basin[n("PBH0092578")], tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, level_boundary_node)

# remove node which causes error in feedback form
ribasim_model.remove_node(n("KST0009114"), True)

# numerical stability
ribasim_model.merge_basins(node_id=n("PBH0235569"), to_node_id=n("PBH0255856"))  # 569 m2
ribasim_model.merge_basins(node_id=n("PBH0239516"), to_node_id=n("PBH0091254"))  # 1737 m2
ribasim_model.merge_basins(node_id=n("PBH0093024"), to_node_id=n("PBH0090670"))  # 1933 m2
ribasim_model.merge_basins(node_id=n("PBH0094725"), to_node_id=n("PBH0094701"))  # 1939 m2
ribasim_model.merge_basins(node_id=n("PBH0089103"), to_node_id=n("PBH0239177"))  # 2044 m2
ribasim_model.merge_basins(node_id=n("PBH0089319"), to_node_id=n("PBH0094865"))
ribasim_model.merge_basins(node_id=n("PBH0094865"), to_node_id=n("PBH0091397"))  # 2617 m2
ribasim_model.merge_basins(node_id=n("PBH0185387"), to_node_id=n("PBH0248142"))  # 2637 m2
ribasim_model.merge_basins(node_id=n("PBH0239400"), to_node_id=n("PBH0094062"))  # 2880 m2
ribasim_model.merge_basins(node_id=n("PBH0093427"), to_node_id=n("PBH0095101"))  # 2883 m2
ribasim_model.merge_basins(node_id=n("PBH0090894"), to_node_id=n("PBH0094085"))  # 3012 m2
ribasim_model.merge_basins(node_id=n("PBH0248198"), to_node_id=n("PBH0239177"))  # 3878 m2
ribasim_model.merge_basins(node_id=n("PBH0120629"), to_node_id=n("PBH0094244"))  # 4407 m2
ribasim_model.merge_basins(node_id=n("PBH0092964"), to_node_id=n("PBH0239177"))  # 5274 m2
ribasim_model.merge_basins(node_id=n("PBH0091187"), to_node_id=n("PBH0248142"))  # 5863 m2
ribasim_model.merge_basins(node_id=n("PBH0088540"), to_node_id=n("PBH0095361"))  # 7283 m2
ribasim_model.merge_basins(node_id=n("PBH0196055"), to_node_id=n("PBH0248142"))  # 7709 m2
ribasim_model.merge_basins(node_id=n("PBH0201155"), to_node_id=n("PBH0093666"))  # 7920 m2
ribasim_model.merge_basins(node_id=n("PBH0256689"), to_node_id=n("PBH0095361"))  # 8514 m2
ribasim_model.merge_basins(node_id=n("PBH0095014"), to_node_id=n("PBH0239177"))  # 10273 m2
ribasim_model.merge_basins(node_id=n("PBH0201293"), to_node_id=n("PBH0093666"))  # 10299 m2
ribasim_model.merge_basins(node_id=n("PBH0094615"), to_node_id=n("PBH0089843"))  # 11880 m2
ribasim_model.merge_basins(node_id=n("PBH0255380"), to_node_id=n("PBH0088302"))  # 10299 m2
ribasim_model.merge_basins(node_id=n("PBH0255999"), to_node_id=n("PBH0090939"))  # 13571 m2
ribasim_model.merge_basins(node_id=n("PBH0190695"), to_node_id=n("PBH0190697"))  # 13853 m2
ribasim_model.merge_basins(node_id=n("PBH0096291"), to_node_id=n("PBH0113669"))  # 14022 m2
ribasim_model.merge_basins(node_id=n("PBH0088292"), to_node_id=n("PBH0093696"))  # 14121 m2
ribasim_model.merge_basins(node_id=n("PBH0096577"), to_node_id=n("PBH0095476"))  # 14121 m2
ribasim_model.merge_basins(node_id=n("PBH0190533"), to_node_id=n("PBH0089236"))  # 15085 m2

ribasim_model.merge_basins(node_id=n("PBH0094981"), to_node_id=n("PBH0088577"))  # small area + MR
ribasim_model.merge_basins(node_id=n("PBH0088577"), to_node_id=n("PBH0091015"))  # small area + MR
ribasim_model.merge_basins(node_id=n("PBH0094480"), to_node_id=n("PBH0091015"))  # small area + MR

ribasim_model.merge_basins(node_id=n("PBH0096302"), to_node_id=n("PBH0095116"))  # 15138 m2
ribasim_model.merge_basins(node_id=n("PBH0099389"), to_node_id=n("PBH0239177"))  # 15486 m2
ribasim_model.merge_basins(node_id=n("PBH0258920"), to_node_id=n("PBH0089476"))  # 16396 m2
ribasim_model.merge_basins(node_id=n("PBH0255990"), to_node_id=n("PBH0094602"), are_connected=False)  # 17464 m2
# basins below 20.000 m2 have been merged, whenever hydrologically possible

# upon request
ribasim_model.merge_basins(node_id=n("PBH0230501"), to_node_id=n("PBH0093163"))  # limiting node for coupling
ribasim_model.merge_basins(node_id=n("PBH0093307"), to_node_id=n("PBH0089191"))  # limiting node for coupling
ribasim_model.merge_basins(node_id=n("PBH0094039"), to_node_id=n("PBH0093538"))  # limiting node for coupling
ribasim_model.merge_basins(node_id=n("PBH0230350"), to_node_id=n("PBH0089302"))  # limiting node for coupling

# water supply
ribasim_model.merge_basins(node_id=n("PBH0100018"), to_node_id=n("PBH0248142"))  # boezem stukje
ribasim_model.merge_basins(node_id=n("PBH0095998"), to_node_id=n("PBH0090259"))  # boezem stuk
ribasim_model.merge_basins(node_id=n("PBH0190292"), to_node_id=n("PBH0190302"))  # boezem stukje
ribasim_model.merge_basins(node_id=n("PBH0092284"), to_node_id=n("PBH0088589"), are_connected=False)  # boezem stukje
ribasim_model.merge_basins(node_id=n("PBH0190713"), to_node_id=n("PBH0088589"))  # boezem stukje
ribasim_model.merge_basins(node_id=n("PBH0000032"), to_node_id=n("PBH0089015"))  # boezem stukje
ribasim_model.merge_basins(node_id=n("PBH0096584"), to_node_id=n("PBH0088793"))  # basin bij boezem
ribasim_model.merge_basins(node_id=n("PBH0096257"), to_node_id=n("PBH0089015"))  # basin bij boezem
ribasim_model.merge_basins(node_id=n("PBH0090084"), to_node_id=n("PBH0090824"))  # basin omringt door boezem
ribasim_model.merge_basins(node_id=n("PBH0092816"), to_node_id=n("PBH0248142"), are_connected=False)  # basin bij boezem
ribasim_model.merge_basins(node_id=n("PBH0093566"), to_node_id=n("PBH0248142"))  # basin bij boezem
ribasim_model.merge_basins(node_id=n("PBH0191105"), to_node_id=n("PBH0194354"))  # klein gebied

ribasim_model.merge_basins(
    node_id=n("PBH0090176"), to_node_id=n("PBH0090939")
)  # veel Manning knopen bij boezem, ook zelfde streefpeil
ribasim_model.merge_basins(
    node_id=n("PBH0093090"), to_node_id=n("PBH0090939")
)  # veel Manning knopen bij boezem, ook zelfde streefpeil
ribasim_model.merge_basins(
    node_id=n("PBH0099267"), to_node_id=n("PBH0260186")
)  # veel Manning knopen bij boezem, ook zelfde streefpeil
ribasim_model.merge_basins(
    node_id=n("PBH0099303"), to_node_id=n("PBH0260186")
)  # veel Manning knopen bij boezem, ook zelfde streefpeil
ribasim_model.merge_basins(
    node_id=n("PBH0099310"), to_node_id=n("PBH0260186")
)  # veel Manning knopen bij boezem, ook zelfde streefpeil

ribasim_model.merge_basins(
    node_id=n("PBH0095211"), to_node_id=n("PBH0090259")
)  # klein gebied in de buurt van de boezem
ribasim_model.merge_basins(node_id=n("PBH0090366"), to_node_id=n("PBH0256361"))  # klein gebied aan de rand van model
ribasim_model.merge_basins(
    node_id=n("PBH0093180"), to_node_id=n("PBH0094985"), are_connected=False
)  # klein gebied midden in groter gebied
ribasim_model.merge_basins(node_id=n("PBH0243466"), to_node_id=n("PBH0093442"))  # klein gebied naast groter gebied
ribasim_model.merge_basins(node_id=n("PBH0090538"), to_node_id=n("PBH0088414"))  # klein gebied
ribasim_model.merge_basins(node_id=n("PBH0256377"), to_node_id=n("PBH0088632"))  # klein gebied naast boezem
ribasim_model.merge_basins(node_id=n("PBH0091532"), to_node_id=n("PBH0088632"))  # klein gebied naast boezem
ribasim_model.merge_basins(node_id=n("PBH0000033"), to_node_id=n("PBH0089476"))  # klein gebied naast boezem
ribasim_model.merge_basins(node_id=n("PBH0100282"), to_node_id=n("PBH0239177"))  # klein gebied naast boezem
ribasim_model.merge_basins(node_id=n("PBH0089433"), to_node_id=n("PBH0093786"))  # klein gebied in groter gebied
ribasim_model.merge_basins(
    node_id=n("PBH0099734"), to_node_id=n("PBH0260186"), are_connected=False
)  # klein gebied in groter gebied
ribasim_model.merge_basins(node_id=n("PBH0099343"), to_node_id=n("PBH0260186"))  # klein gebied in groter gebied

ribasim_model.merge_basins(
    node_id=n("PBH0089022"), to_node_id=n("PBH0093479"), are_connected=False
)  # havenachtig gebied
ribasim_model.merge_basins(node_id=n("PBH0092662"), to_node_id=n("PBH0093479"))  # havenachtig gebied
ribasim_model.merge_basins(node_id=n("PBH0099341"), to_node_id=n("PBH0260186"))  # havenachtig gebied
ribasim_model.merge_basins(node_id=n("PBH0111882"), to_node_id=n("PBH0260186"))  # havenachtig gebied

ribasim_model.merge_basins(node_id=n("PBH0099232"), to_node_id=n("PBH0260186"))  # boezemland met veel MR
ribasim_model.merge_basins(node_id=n("PBH0099247"), to_node_id=n("PBH0260186"))  # boezemland met veel MR
ribasim_model.merge_basins(node_id=n("PBH0099252"), to_node_id=n("PBH0260186"))  # boezemland met veel MR
ribasim_model.merge_basins(node_id=n("PBH0093659"), to_node_id=n("PBH0248142"), are_connected=False)  # boezem

ribasim_model.merge_basins(
    node_id=n("PBH0091001"), to_node_id=n("PBH0094602")
)  # ontbreekt gemaal / foutieve streefpeil
ribasim_model.merge_basins(
    node_id=n("PBH0100683"), to_node_id=n("PBH0109596"), are_connected=False
)  # boezem via duiker verbonden aan andere boezem

ribasim_model.merge_basins(node_id=n("PBH0094509"), to_node_id=n("PBH0095498"))  # vrijafstromend
ribasim_model.merge_basins(node_id=n("PBH0091817"), to_node_id=n("PBH0095498"))  # vrijafstromend
ribasim_model.merge_basins(node_id=n("PBH0093200"), to_node_id=n("PBH0095498"))  # vrijafstromend
ribasim_model.merge_basins(node_id=n("PBH0091039"), to_node_id=n("PBH0095498"))  # vrijafstromend
ribasim_model.merge_basins(node_id=n("PBH0091281"), to_node_id=n("PBH0092896"))  # vrijafstromend

ribasim_model.merge_basins(node_id=n("PBH0092361"), to_node_id=n("PBH0089478"))  # vrijafstromend
ribasim_model.merge_basins(node_id=n("PBH0091816"), to_node_id=n("PBH0089478"))  # vrijafstromend
ribasim_model.merge_basins(node_id=n("PBH0096327"), to_node_id=n("PBH0089478"))  # vrijafstromend
ribasim_model.merge_basins(node_id=n("PBH0089872"), to_node_id=n("PBH0089478"))  # vrijafstromend
ribasim_model.merge_basins(node_id=n("PBH0092895"), to_node_id=n("PBH0089478"))  # vrijafstromend

ribasim_model.merge_basins(node_id=n("PBH0226711"), to_node_id=n("PBH0118888"))  # vrijafstromend

ribasim_model.merge_basins(node_id=n("PBH0090241"), to_node_id=n("PBH0088801"))  # vrijafstromend
ribasim_model.merge_basins(node_id=n("PBH0094364"), to_node_id=n("PBH0088632"))  # vrijafstromend
ribasim_model.merge_basins(node_id=n("PBH0089061"), to_node_id=n("PBH0088632"))  # vrijafstromend

# small basin area
ribasim_model.merge_basins(node_id=n("PBH0000006"), to_node_id=n("PBH0089302"))
ribasim_model.merge_basins(node_id=n("PBH0095914"), to_node_id=n("PBH0260186"))
ribasim_model.merge_basins(node_id=n("PBH0095146"), to_node_id=n("PBH0185781"))
ribasim_model.merge_basins(node_id=n("PBH0095759"), to_node_id=n("PBH0088803"))
ribasim_model.merge_basins(node_id=n("PBH0094214"), to_node_id=n("PBH0093322"))
ribasim_model.merge_basins(node_id=n("PBH0094851"), to_node_id=n("PBH0089452"))
ribasim_model.merge_basins(node_id=n("PBH0183694"), to_node_id=n("PBH0096567"))
ribasim_model.merge_basins(node_id=n("PBH0096411"), to_node_id=n("PBH0088393"))
ribasim_model.merge_basins(node_id=n("PBH0094989"), to_node_id=n("PBH0091535"))
ribasim_model.merge_basins(node_id=n("PBH0090623"), to_node_id=n("PBH0088911"))
ribasim_model.merge_basins(node_id=n("PBH0092500"), to_node_id=n("PBH0096177"))
ribasim_model.merge_basins(node_id=n("PBH0191107"), to_node_id=n("PBH0090125"))
ribasim_model.merge_basins(node_id=n("PBH0094062"), to_node_id=n("PBH0094602"))
ribasim_model.merge_basins(node_id=n("PBH0092989"), to_node_id=n("PBH0088768"))
ribasim_model.merge_basins(node_id=n("PBH0094997"), to_node_id=n("PBH0248142"))
ribasim_model.merge_basins(node_id=n("PBH0260567"), to_node_id=n("PBH0246949"))
ribasim_model.merge_basins(node_id=n("PBH0091672"), to_node_id=n("PBH0091537"))
ribasim_model.merge_basins(node_id=n("PBH0190697"), to_node_id=n("PBH0088589"))
ribasim_model.merge_basins(node_id=n("PBH0119784"), to_node_id=n("PBH0248142"))
ribasim_model.merge_basins(node_id=n("PBH0095240"), to_node_id=n("PBH0093811"))
ribasim_model.merge_basins(node_id=n("PBH0092487"), to_node_id=n("PBH0093811"))
ribasim_model.merge_basins(node_id=n("PBH0089601"), to_node_id=n("PBH0093811"))
ribasim_model.merge_basins(node_id=n("PBH0093012"), to_node_id=n("PBH0248142"))
ribasim_model.merge_basins(node_id=n("PBH0088546"), to_node_id=n("PBH0094352"))
ribasim_model.merge_basins(node_id=n("PBH0120818"), to_node_id=n("PBH0239177"))
ribasim_model.merge_basins(node_id=n("PBH0089840"), to_node_id=n("PBH0088911"))
ribasim_model.merge_basins(node_id=n("PBH0093001"), to_node_id=n("PBH0089041"))
ribasim_model.merge_basins(node_id=n("PBH0183868"), to_node_id=n("PBH0191112"))
ribasim_model.merge_basins(node_id=n("PBH0184326"), to_node_id=n("PBH0248142"))
ribasim_model.merge_basins(node_id=n("PBH0094592"), to_node_id=n("PBH0092308"))
ribasim_model.merge_basins(node_id=n("PBH0236172"), to_node_id=n("PBH0089974"))
ribasim_model.merge_basins(node_id=n("PBH0093669"), to_node_id=n("PBH0089283"))

# fix water supply
ribasim_model.merge_basins(node_id=n("PBH0094197"), to_node_id=n("PBH0092051"))
ribasim_model.merge_basins(node_id=n("PBH0096021"), to_node_id=n("PBH0092051"))
ribasim_model.merge_basins(node_id=n("PBH0088784"), to_node_id=n("PBH0092051"))
ribasim_model.merge_basins(node_id=n("PBH0088665"), to_node_id=n("PBH0092051"))
ribasim_model.merge_basins(node_id=n("PBH0090120"), to_node_id=n("PBH0092051"))

# water supply
ribasim_model.merge_basins(node_id=n("PBH0261697"), to_node_id=n("PBH0088690"))  # klein gebied
ribasim_model.merge_basins(node_id=n("PBH0244987"), to_node_id=n("PBH0262405"))  # klein gebied
ribasim_model.merge_basins(node_id=n("PBH0091530"), to_node_id=n("PBH0094985"))  # klein gebied
ribasim_model.merge_basins(node_id=n("PBH0096635"), to_node_id=n("PBH0089738"))  # klein gebied
ribasim_model.merge_basins(node_id=n("PBH0092468"), to_node_id=n("PBH0093685"))  # klein gebied
ribasim_model.merge_basins(node_id=n("PBH0093022"), to_node_id=n("PBH0094978"), are_connected=False)  # klein gebied

ribasim_model.merge_basins(node_id=n("PBH0091792"), to_node_id=n("PBH0261853"))  # samenvoegen voor wateraanvoer
ribasim_model.merge_basins(node_id=n("PBH0261853"), to_node_id=n("PBH0088800"))  # samenvoegen voor wateraanvoer
ribasim_model.merge_basins(node_id=n("PBH0091019"), to_node_id=n("PBH0248142"))  # samenvoegen voor wateraanvoer

ribasim_model.merge_basins(node_id=n("PBH0093660"), to_node_id=n("PBH0248142"))  # part of the boezem
ribasim_model.merge_basins(node_id=n("PBH0252919"), to_node_id=n("PBH0090401"))  # part of the boezem
ribasim_model.merge_basins(node_id=n("PBH0090207"), to_node_id=n("PBH0089269"))  #

# add gaarkeuken to basin 184
# Add Gaarkeuken to basin 184
assert ribasim_model.basin.area.df is not None
gaarkeuken_area = ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df["node_id"] == n("PBH0094352")]
gaarkeuken = gpd.read_file(gaarkeuken_path)
merged_geom = gaarkeuken_area.geometry.iloc[0].union(gaarkeuken.geometry.union_all())
ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df["node_id"] == n("PBH0094352"), "geometry"] = merged_geom

inlaat_pump = []  # pumps
inlaat_structures = []  # weirs / outlets
to_drain_node_ids = []

# add gemaal in middle of beheergebied. Dont use FF as it is an aanvoergemaal
pump_node = ribasim_model.pump.add(Node(geometry=Point(207500, 587902)), [pump.Static(flow_rate=[1.8])])
ribasim_model.link.add(ribasim_model.basin[n("PBH0094701")], pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[n("PBH0094978")])
inlaat_pump.append(pump_node.node_id)

# add gemaal where basins were added twice
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(206469, 592738)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(206449, 592819)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[n("PBH0092850@2")], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# Inlaat toevoegen
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(206516, 592761)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(206508, 592803)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[n("PBH0092850@2")])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

# Inlaat toevoegen
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(155919, 563047)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(156038, 563082)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[n("PBH0111783")])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

# Ropta-pumps as 'aanvoer'
inlaat_pump += [n("KGM0003006"), n("KGM0000153")]
# Inlaat toevoegen at validation location
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(174976, 539741)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(174961, 539814)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[n("PBH0099100")])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

# add gemaal at validation location
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(153292, 543364)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(153456, 543428)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[n("PBH0239177")], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# add gemaal at Mieden Tjonger, Oost
pump_node = ribasim_model.pump.add(
    Node(geometry=Point(209035, 555696), name="Midden Tjonger Oost, De"), [pump.Static(flow_rate=[0.2])]
)
ribasim_model.link.add(ribasim_model.basin[n("PBH0092494")], pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[n("PBH0089999")])

# Inlaat toevoegen at validation location
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(153296, 543488)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(153406, 543502)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[n("PBH0239177")])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

# Uitlaat toevoegen at validation location
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(206572, 592599)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(206421, 592530)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(ribasim_model.basin[n("PBH0239874")], tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, level_boundary_node)
to_drain_node_ids.append(tabulated_rating_curve_node.node_id)

# Uitlaat toevoegen at validation location
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(206437, 592726)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(206360, 592679)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(ribasim_model.basin[n("PBH0239874")], tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, level_boundary_node)
to_drain_node_ids.append(tabulated_rating_curve_node.node_id)

# move node for improved representation
ribasim_model.move_node(geometry=Point(220029, 561890), node_id=n("KST0050006"))
ribasim_model.move_node(geometry=Point(216800, 585120), node_id=n("ow:OAF-F1020450"))
ribasim_model.move_node(geometry=Point(216818, 585123), node_id=n("bnd:ow:OAF-F1020450"))

# embed inlaat information
assert ribasim_model.pump.static.df is not None
for node_id in inlaat_pump:
    ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == node_id, "meta_func_aanvoer"] = 1
    ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == node_id, "meta_func_afvoer"] = 0

ribasim_param.change_pump_func(ribasim_model, n("KGM0000716|PBH0090746>PBH0260186"), "afvoer", 1)
ribasim_param.change_pump_func(ribasim_model, n("KGM0000716|PBH0090746>PBH0260186"), "aanvoer", 0)

# check basin area
ribasim_param.validate_basin_area(ribasim_model)

# check streefpeilen at manning nodes
ribasim_param.validate_manning_basins(ribasim_model)

# model specific tweaks
# change unknown streefpeilen to a default streefpeil
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

# (re)set 'meta_node_id'-values
for node_type in ["LevelBoundary", "TabulatedRatingCurve", "Pump"]:
    mask = ribasim_model.node.df["node_type"] == node_type
    ribasim_model.node.df.loc[mask, "meta_node_id"] = ribasim_model.node.df.loc[mask].index

# convert all boundary nodes to LevelBoundaries
ribasim_param.Terminals_to_LevelBoundaries(ribasim_model=ribasim_model, default_level=default_level)  # clean
ribasim_param.FlowBoundaries_to_LevelBoundaries(ribasim_model=ribasim_model, default_level=default_level)

# add outlet
ribasim_param.add_outlets(ribasim_model, delta_crest_level=0.10)

assert ribasim_model.outlet.static.df is not None
for node in inlaat_structures:
    ribasim_model.outlet.static.df.loc[ribasim_model.outlet.static.df["node_id"] == node, "meta_func_aanvoer"] = 1

# add some more outlets (created due to FB, hence not in FF)
ribasim_model.outlet.static.df.loc[
    ribasim_model.outlet.static.df["node_id"] == n("ow:KDU0000579"), "meta_func_aanvoer"
] = 1

ribasim_param.clean_tables(ribasim_model, waterschap)

# loop through all splitted basins
node_cache = NodeMetaCache(ribasim_model)
for splitted_basin_path, basin_id in zip(
    [splitted_basin_6_path, splitted_basin_16_path, splitted_basin_21_path],
    [n("PBH0239177"), n("PBH0248142"), n("PBH0260186")],
    strict=True,
):
    # split basins to improve model convergence
    splitter = SplitBasins(
        model=ribasim_model, splitted_basin_path=splitted_basin_path, basin_node_id_to_split=basin_id
    )
    ribasim_model = splitter.run()

node_cache.set_meta_category(ribasim_model)
write_feedback_model(ribasim_model, paths)
del node_cache
