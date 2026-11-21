"""Parameterisation of water board: Hollands Noorderkwartier."""

import ribasim_nl.ribasim_parametrization as ribasim_param
from ribasim import Node
from ribasim.nodes import level_boundary, manning_resistance, pump
from ribasim_nl.control import set_flow_rate, set_node_functions
from ribasim_nl.design_capacities import set_design_capacities
from ribasim_nl.workflow import (
    WaterBoardPaths,
    add_controllers,
    assign_pump_and_basin_names,
    categorize_nodes,
    complete_parametrization,
    keep_model_supply_basins,
    node_function_table,
    set_boundary_conditions,
    set_manning_resistance_outside_bergend,
    write_and_run_forcing_model,
)
from shapely.geometry import Point

from ribasim_nl import CloudStorage, Model

# model settings
waterschap = "HollandsNoorderkwartier"
base_model_versie = "2024_12_0"

# connect with the GoodCloud
cloud = CloudStorage()

paths = WaterBoardPaths(cloud, waterschap, base_model_versie)
aanvoer_path = cloud.joinpath(
    waterschap, "aangeleverd/Na_levering/20240618_peilgebieden_en_polders/Polders_export_2024-06-18.shp"
)

# recreate the feedback form for set_aanvoer_flags
# TODO, see if we can move set_aanvoer_flags to the feedback stage so we don't need this object
processor = paths.feedback_processor(work_dir=paths.model_dir("profiles"))

ribasim_model = Model.read(paths.model_dir("profiles") / "ribasim.toml")
# nodes and links are referred to by their key, see docs/dev/node-keys.qmd
n = ribasim_model.node_id

# add gemaal Kadoelen which is removed due to the merging
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(122405, 491275)), [level_boundary.Static(level=[0.42])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(122475, 491317)), [pump.Static(flow_rate=[11.67])])
ribasim_model.link.add(ribasim_model.basin[n("peilgebied_5170-2")], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)
ribasim_model.node.df.loc[level_boundary_node.node_id, "meta_node_id"] = level_boundary_node.node_id
ribasim_model.node.df.loc[pump_node.node_id, "meta_node_id"] = pump_node.node_id

# a manning node misses at splitted boezem due to awkward basin shape
manning_node = ribasim_model.manning_resistance.add(
    Node(geometry=Point(110789, 518114)),
    [manning_resistance.Static(length=[10.0], manning_n=[0.01], profile_width=[10.0], profile_slope=[3.0])],
)
ribasim_model.link.add(ribasim_model.basin[n("peilgebied_1000-01/27")], manning_node)
ribasim_model.link.add(manning_node, ribasim_model.basin[n("peilgebied_1000-01/15")])
ribasim_model.node.df.loc[manning_node.node_id, "meta_node_id"] = manning_node.node_id

# reset pump capacity for each pump
assert ribasim_model.pump.static.df is not None
ribasim_model.pump.static.df["flow_rate"] = 10 / 60  # 10m3/min

ribasim_model = set_boundary_conditions(ribasim_model, paths, level_boundary_levels=(-0.42, -0.4))

# prepare 'aanvoergebieden'
# fmt: off
basin_aanvoer_off = (
    n("peilgebied_8040 E"), n("peilgebied_8071-44"), n("peilgebied_8020 V"), n("peilgebied_8010 K"), n("peilgebied_8071-28"),  # Texel
    n("peilgebied_2852-08"), n("peilgebied_2956-16"),  # Wieringermeer
    n("peilgebied_04070-11"), n("peilgebied_04080-07"), n("peilgebied_04902-44"), n("peilgebied_04230-19"), n("peilgebied_04541-15"),  # duinen
)
# fmt: on

# add control, based on the meta_categorie
keep_model_supply_basins(processor, ribasim_model)
basin_ids = set(ribasim_model.basin.node.df.index)
basin_aanvoer_off = tuple(node_id for node_id in basin_aanvoer_off if node_id in basin_ids)

ribasim_param.set_aanvoer_flags(
    ribasim_model, str(aanvoer_path), processor, aanvoer_enabled=True, basin_aanvoer_off=basin_aanvoer_off
)
categorize_nodes(ribasim_model)

# remove defying-gravity-outlets
ribasim_model = ribasim_param.remove_non_free_flowing_outlets(ribasim_model, printing=True)

from_to_node_function_table = node_function_table(ribasim_model)

to_drain = (
    n("ow:OAF-A-11576_184579"),
    n("KGM-Q-29156"),
    n("KGM-Q-25357"),
    n("loc:114698,551327"),
)
to_flow_control = (
    n("KGM-Q-25388"),
    n("KST-A-1795"),
    n("ow:OAF-E-48040_228"),
    n("KGM-N-25"),
    n("KGM-Q-25380|peilgebied_2803-03>peilgebied_2804-09"),
    n("KST-P-140144"),
    n("KST-C-4326"),
    n("KST-M-5192"),
    n("KST-JB-5"),
    n("KST-JH-148"),
    n("KGM-P-67"),
    n("KST-C-4307"),
    n("KGM-JL-2149"),
    n("KST-A-1462"),
    n("ow:OAF-W-3034_122948"),
    n("KST-JH-2026"),
    n("ow:OAF-QJ-51157_87572"),
    n("KGM-Q-25398|peilgebied_2803-03>peilgebied_2804-09"),
    n("KST-JF-150"),
    n("ow:OAF-Q-89676_48639"),
    n("KST-Q-19453"),
    n("KGM-Q-31664"),
    n("KST-J-88"),
    n("ow:OAF-O-8092_156869"),
    n("ow:OAF-Q-130992_83616"),
    n("KST-E-255"),
    n("KST-Q-19580"),
    n("ow:OAF-M-10639_113878"),
    n("KGM-A-394"),
    n("KGM-Q-25348"),
    n("ow:OAF-JF-2511_163131"),
    n("KGM-Q-20395"),
)
to_supply = (
    n("KST-JH-5090"),
    n("KST-Q-31238"),
    n("KST-JH-2009"),
    n("KST-Q-28969"),
    n("KST-Q-28673"),
    n("KST-J-1502"),
    n("ow:OAF-Q-117083_37000"),
    n("KST-JH-5100"),
    n("ow:OAF-M-3980_5451"),
    n("KGM-B-1680|peilgebied_1000-01>peilgebied_2774 A"),
    n("KGM-JL-17"),
    n("KGM-Q-29138"),
    n("KST-Q-28690"),
    n("KGM-Q-24889"),
    n("KGM-Q-25197|peilgebied_1000-01>peilgebied_2060-13"),
    n("ow:OAF-JF-280_9072"),
    n("KST-I-002056"),
    n("KGM-A-7756|peilgebied_04852-01>peilgebied_04851-70"),
    n("ow:OAF-Q-130981_14992"),
    n("KST-A-1112"),
    n("KGM-Q-29251"),
    n("KGM-J-2355|peilgebied_1000-01>peilgebied_2020-5"),
    n("KST-Q-34433"),
    n("KST-AA-20"),
    n("KST-Q-28638"),
    n("ow:OAF-QJ-11381_44774"),
    n("KGM-B-1241"),
    n("KGM-Q-25185"),
    n("ow:OAF-Q-130977_50115"),
    n("ow:OAF-A-14142_162219"),
    n("KST-JF-107"),
    n("KGM-Q-29115|peilgebied_1000-01>peilgebied_2780-02"),
    n("KST-Q-28989"),
    n("KGM-Q-25196"),
    n("ow:OAF-J-2131_67373"),
    n("KST-JF-96"),
    n("KST-JF-102"),
    n("KGM-Q-25197|peilgebied_1000-01>peilgebied_2020-5"),
    n("KGM-Q-25340"),
    n("KGM-Q-25183"),
    n("KGM-Q-24975"),
    n("KGM-Q-29241"),
    n("KGM-B-1680|peilgebied_2774 A>peilgebied_1000-01"),
    n("KGM-Q-29162|peilgebied_1000-01>peilgebied_2010-07"),
    n("KST-JL-2265"),  # rondpompen
    n("KGM-Q-24934"),
    n("KGM-Q-25128"),
    n("KGM-A-7758|peilgebied_04852-01>peilgebied_04851-70"),
    n("KGM-Q-25184"),
    n("ow:OAF-A-10491_169622"),
    n("KGM-Q-25146"),
)
from_to_node_function_table = set_node_functions(
    from_to_node_function_table, to_supply=to_supply, to_flow_control=to_flow_control, to_drain=to_drain
)

add_controllers(ribasim_model, from_to_node_function_table, remove_duplicates=True)

assign_pump_and_basin_names(ribasim_model, paths, param_name="Noorderkwartier.gpkg")

# presumably wrong conversion of flow capacity in the data
assert ribasim_model.pump.static.df is not None
set_flow_rate(ribasim_model.pump.static.df, [n("KGM-W-37")], 0.4)
set_flow_rate(
    ribasim_model.pump.static.df, [n("KGM-A-3929|peilgebied_1000-01>peilgebied_2780-02"), n("KGM-A-7727")], 1.17
)

# according data flow_rate of 0
zero_flow_pumps = [
    n("KGM-Q-24974|peilgebied_2752 A>peilgebied_1000-01"),
    n("KGM-Q-24974|peilgebied_2752 A>peilgebied_2775 A"),
]
set_flow_rate(ribasim_model.pump.static.df, zero_flow_pumps, 25.0)

increase_flow_rate_pumps = [
    n("KGM-Q-30557"),
    n("KGM-Q-20428"),
    n("KGM-Q-20427"),
    n("KGM-Q-20388"),
    n("KGM-Q-29335"),
    n("KGM-Q-29334"),
    n("KGM-Q-20391"),
    n("KGM-Q-20382"),
    n("KGM-Q-29109"),
    n("KGM-Q-24973"),
    n("KGM-JL-2025"),
    n("KGM-A-7727"),
    n("KGM-A-3929|peilgebied_1000-01>peilgebied_2780-02"),
    n("KGM-A-3929|peilgebied_2780-02>peilgebied_1000-01"),
    n("KGM-Q-25117"),
    n("KGM-Q-25116"),
]
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df["node_id"].isin(increase_flow_rate_pumps), ["flow_rate", "max_flow_rate"]
] *= 60

# Manning resistance
# there is a MR without geometry and without links for some reason
ribasim_model.node.df = ribasim_model.node.df.dropna(subset="geometry")

# lower the difference in waterlevel for each manning node, except those of storing basins
set_manning_resistance_outside_bergend(ribasim_model, length=10.0, manning_n=0.01)

ribasim_model = complete_parametrization(
    ribasim_model,
    paths,
    rws_buffer=3000,  # due to harbour
    custom_nodes={
        n("bnd:ow:OAF-M-6869_168824"): "AmstelGooienVecht",
        n("bnd:KGM-Q-29167"): "Noordzee",
        n("bnd:KGM-OH-548"): "Noordzee",
        n("bnd:KGM-OH-547"): "Noordzee",
        n("bnd:ow:OAF-A-6016_43725"): "Noordzee",
        n("bnd:KGM-Q-29168"): "Noordzee",
        n("bnd:KGM-A-371"): "Noordzee",
        n("bnd:KGM-OH-549"): "Noordzee",
        n("bnd:KGM-Q-29157"): "Noordzee",
        n("bnd:KGM-JF-44"): "Noordzee",
        n("bnd:KGM-M-7054"): "Noordzee",
        n("bnd:KGM-Q-29235"): "Noordzee",
        n("bnd:KGM-Q-29234"): "Noordzee",
        n("bnd:KGM-OH-546"): "Noordzee",
        n("bnd:ow:OAF-Q-59560_184416"): "Noordzee",
        n("loc:114728,551405"): "Noordzee",
        n("loc:131275,548374"): "Noordzee",
        n("loc:131506,548321"): "Noordzee",
        n("loc:131273,548374"): "Noordzee",
        n("loc:131271,548375"): "Noordzee",
        n("loc:131268,548375"): "Noordzee",
    },
)

# set the unknown capacities of Pumps and Outlets from the area they drain or supply
set_design_capacities(ribasim_model, from_to_node_function_table)
write_and_run_forcing_model(ribasim_model, paths)
