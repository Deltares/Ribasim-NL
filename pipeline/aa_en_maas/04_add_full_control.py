# %%

import math
from collections.abc import Mapping
from datetime import datetime
from typing import cast

import geopandas as gpd
from ribasim.nodes import flow_demand, outlet
from ribasim_nl.control import (
    _offset_new_node,
    _target_level,
    add_controllers_to_supply_area,
    add_controllers_to_supply_nodes,
    add_controllers_to_uncontrolled_connector_nodes,
    get_node_table_with_from_to_node_ids,
    mark_level_update_protected,
)
from ribasim_nl.junctions import junctionify
from ribasim_nl.run_model import write_steady_state_scenarios
from shapely.geometry import MultiPolygon

from ribasim_nl import CloudStorage, Model

# %%
# Globale settings

MODEL_EXEC: bool = False  # execute model run
AUTHORITY: str = "AaenMaas"
SHORT_NAME: str = "aam"
CONTROL_NODE_TYPES = ["Outlet", "Pump"]
IS_SUPPLY_NODE_COLUMN: str = "meta_supply_node"

# %%
# Definieren paden en syncen met cloud
cloud = CloudStorage()
ribasim_model_dir = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_parameterized_model")
ribasim_toml = ribasim_model_dir / f"{SHORT_NAME}.toml"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_aanvoer.qlr")
aanvoergebieden_gpkg = cloud.joinpath(r"AaenMaas/verwerkt/sturing/aanvoergebieden.gpkg")
aanvoerpunten_path = cloud.joinpath("AaenMaas/verwerkt/1_ontvangen_data/wateraanvoer-10-4-2026/Inlaten_verdelen.gdb")
aanvoerpunten_layer = "Wateraanvoersysteem"


# %%
# Read data
model = Model.read(ribasim_toml)
# nodes and links are referred to by their key, see docs/dev/node-keys.qmd
n = model.node_id
lk = model.link_id

# Sluizen die geen rol hebben in de waterverdeling (aanvoer/afvoer), maar wel in het model zitten
# 745: Sluis Engelen, alles via Crevecoeur
EXCLUDE_NODES = {
    n("211LH"),
    n("2110005"),
    n("Henriëttesluis"),
    n("211MP"),
    n("211MI"),
    n("211SPV"),
    n("211LK"),
    n("2110162"),
    n("203A"),
    n("119IZW"),
    n("268A"),
    n("201JVL"),
}
outlet_max_flow_rate_by_node_id = {
    n("257IA"): 12.0,  # Schabbert
}
outlet_max_flow_rate_parameterized_zero_by_node_id = {
    n("217BKK"): 5,  # xlsx max_flow_rate nul; voorkom afvoerblokkade
    n("2341029"): 5,  # xlsx max_flow_rate nul; voorkom afvoerblokkade
    n("1130018"): 5,  # xlsx max_flow_rate nul; voorkom afvoerblokkade
    n("2030225___3"): 50,  # xlsx max_flow_rate nul; voorkom afvoerblokkade
    n("loc:148512,410583"): 300.0,  # xlsx max_flow_rate nul; voorkom afvoerblokkade
}
outlet_max_flow_rate_from_results = {
    n("114P17"): 9,  # 114P17
    n("203KDZ"): 2,  # 203KDZ
    n("294EC"): 2,  # 294EC
    n("201JSL"): 9,  # 201JSL
    n("253M"): 60,  # 253M
    n("248KA"): 18,  # 248KA
    n("201BGS"): 2,  # 201BGS
    n("2530328"): 8,  # 2530328
    n("2510112"): 62,  # 2510112
    n("3200005"): 10,  # 3200005
    n("2110137"): 21,  # 2110137
    n("201JAA@1"): 6,  # 201JAA
    n("2480021"): 21,  # 2480021
    n("203K"): 5,  # 203K
    n("248K"): 36,  # 248K
    n("253R"): 15,  # 253R
    n("253LD"): 36,  # 253LD
    n("253J"): 41,  # 253J
    n("107P12"): 35,  # 107P12
    n("107P13"): 29,  # 107P13
    n("107P14"): 30,  # 107P14
    n("2530128"): 15,  # 2530128
    n("1047240___3"): 8,  # 1047240___3
    n("loc:192486,378114"): 27,  # Zijtak Helenavaart Limburg
    n("loc:190367,375169"): 5,  # Helenavaart Limburg
}
outlet_max_flow_rate_coupled_by_node_id = {
    n("104CU"): 8,  # 104CU; gekoppeld max=4.21, parameterized=3.76
    n("116HDW"): 15,  # 116HDW; gekoppeld max=9.90, huidige max=6.00, link=3802084
    n("2940006"): 12,  # parameterized nul; gekoppeld max=7.65
    n("113SBW"): 5,  # 113SBW; gekoppeld max=2.59, parameterized=1.21
    n("2041790___1"): 3,  # parameterized nul; gekoppeld max=1.23
    n("ow:1160120"): 14,  # gekoppeld max=8.74, huidige max=2.00, link=3800983
    n("1160440___4"): 23,  # parameterized nul; gekoppeld max=14.83
    n("ow:3200030"): 3,  # gekoppeld max=1.27, parameterized=0.39
    n("loc:148450,410398"): 2,  # parameterized nul; gekoppeld max=0.67
    n("202DBR"): 9,  # gekoppeld max=5.66, huidige max=0.664, link=3800017
    n("104FTW"): 3,  # gekoppeld max=1.17, huidige max=0.00, link=3800208
}
outlet_max_flow_rate_afvoer_by_node_id = {}
for max_flow_rates in (
    outlet_max_flow_rate_from_results,
    outlet_max_flow_rate_coupled_by_node_id,
    outlet_max_flow_rate_parameterized_zero_by_node_id,
):
    for node_id, max_flow_rate in max_flow_rates.items():
        outlet_max_flow_rate_afvoer_by_node_id[node_id] = max(
            outlet_max_flow_rate_afvoer_by_node_id.get(node_id, 0.0),
            max_flow_rate,
        )
outlet_max_flow_rate_afvoer_by_node_id.update(outlet_max_flow_rate_by_node_id)
pump_max_flow_rate_by_node_id = {
    n("211VEL"): 5.0,  # Gemaal Veluwe
}

flushing_nodes: dict[int, float] = {}
drain_nodes = [
    n("234WHK"),
    n("234MIE"),
    n("254B"),
    n("234CA"),
    n("234CK"),
    n("261OKB"),
    n("234CN"),
    n("2030225___3"),
    n("2110562"),
    n("2040667"),
    n("2041790___1"),
    n("205BLW"),
    n("204BK"),
    n("204BI"),
    n("227VEN"),
    n("205LFT"),
    n("204B"),
    n("211HA"),
    n("211LA"),
    n("227BA"),
    n("204TRS"),
    n("204AA"),
    n("205C"),
    n("204BA"),
    n("211HE"),
    n("2110514"),
    n("204D"),
    n("2270021"),
    n("227B"),
    n("ow:2113865"),
    n("205BEU"),
    n("206PLD"),
    n("250A"),
    n("251A"),
    n("253AAW"),
    n("256A"),
    n("253A"),
    n("246A"),
    n("245A"),
    n("205MOL"),
    n("240A"),
    n("217A"),
    n("2010005"),
    n("2320026"),
    n("224KDW"),
    n("203VWW"),
    n("2170333"),
    n("2170184"),
    n("ow:2171320"),
    n("203J"),
    n("206EA"),
    n("2171570___3"),
    n("261BE"),
    n("261JK"),
    n("261CQA"),
    n("248A"),
    n("253B"),
    n("253LEA"),
    n("287DB"),
    n("2751"),
    n("2920028"),
    n("2940205"),
    n("2940153"),
    n("201PVB"),
    n("201L"),
    n("201JAA@1"),
    n("2740007"),
    n("234NVW"),
    n("2170131"),
    n("2170097"),
    n("2340750"),
    n("114SAM"),
    n("107GTB"),
    n("107BOE"),
    n("107VHS"),
    n("1070307"),
    n("1140146"),
    n("108KKS"),
    n("108BRO"),
    n("108HES"),
    n("108HQS"),
    n("108TDE"),
    n("1140199"),
    n("1140208"),
    n("114DWS"),
    n("1143650___0"),
    n("117NSK"),
    n("117MW"),
    n("117MU"),
    n("1170274"),
    n("1170286"),
    n("AKW895"),
    n("116HDW"),
    n("116BIN"),
    n("116PKS"),
    n("118DL"),
    n("118DG"),
    n("loc:148512,410583"),
    n("118PVW"),
    n("118AYS"),
    n("104HPN"),
    n("104FXW"),
    n("104CU"),
    n("104CY"),
    n("104STA"),
    n("103KTS"),
    n("104MSB"),
    n("104ESU"),
    n("104AF"),
    n("104CT"),
    n("104ESW"),
    n("104AA"),
    n("104STU"),
    n("104N"),
    n("104F"),
    n("108STW"),
    n("104ASZ"),
    n("1040515"),
    n("102DDP"),
    n("102CEV"),
    n("1020332"),
    n("104FKW"),
    n("102BFS"),
    n("104SRO"),
    n("103HOE"),
    n("104FRW"),
    n("104UDL"),
    n("1020252"),
    n("201O"),
    n("104FTX"),
    n("201R"),
    n("238A"),
    n("201Q"),
    n("1140125"),
    n("AKW762DK4"),
    n("1070039"),
    n("2110664"),
    n("2270181"),
    n("201PP"),
    n("234A"),
    n("201N"),
    n("201T"),
    n("261JAA"),
    n("211NA"),
    n("ow:1160120"),
]
supply_nodes: list[int] = [n("loc:149521,410704"), n("116KRU"), n("201JSL")]
flow_control_nodes = [
    n("114P17"),
    n("loc:148450,410398"),
    n("ow:3200030"),
    n("2340879"),
    n("234F"),
    n("234BUY"),
    n("234C"),
    n("234D"),
    n("234SLA"),
    n("204BK"),
    n("205DA"),
    #  n("275P"),
    n("211J"),
    n("211L"),
    n("2040231"),
    n("2040779"),
    n("3200005"),
    n("2270021"),
    n("211G"),
    n("211EA"),
    n("2040882"),
    n("2052"),
    n("211D"),
    n("211M"),
    n("211I"),
    n("211E"),
    n("211BVP"),
    n("211B"),
    n("211A"),
    n("227B"),
    n("201VST"),
    n("201D"),
    n("204BF"),
    n("227DPT"),
    n("2113600"),
    n("201BEU"),
    n("201G"),
    n("201B"),
    n("201C"),
    n("201F"),
    n("201H"),
    n("ow:2113865"),
    n("2010630"),
    n("211VEL"),
    n("217FA"),
    n("203IA"),
    n("217AC"),
    n("2030270"),
    n("2030319"),
    n("2170576"),
    n("217I"),
    n("217H"),
    n("217J"),
    n("261VGD"),
    n("2611073"),
    n("253U"),
    n("251P"),
    n("248KA"),
    n("2510112"),
    n("2110137"),
    n("2480021"),
    n("248K"),
    n("248E"),
    n("211MLA"),
    n("287A"),
    n("294E"),
    n("294C"),
    n("201N"),
    n("234CC"),
    n("2170131"),
    n("108HOO"),
    n("108HH"),
    n("1100074"),
    n("107A3"),
    n("1080080"),
    n("1140087"),
    n("107P12"),
    n("107P07"),
    n("107P11"),
    n("108HUL"),
    n("108PAP"),
    n("107P10"),
    n("107P09"),
    n("107P08"),
    n("107P01"),
    n("110GG"),
    n("114AQS"),
    n("108GZ"),
    n("1142610"),
    n("ow:1144290"),
    n("ow:1100670"),
    n("1142560"),
    n("117MW"),
    n("1180220"),
    n("1160440___4"),
    n("204GDD"),
    n("201E"),
    n("107P14"),
    n("107P13"),
]


def add_discharge_supply_nodes(
    discharge_supply_nodes: Mapping[int, Mapping[str, float]],
    us_target_level_offset_supply: float = -0.04,
    new_nodes_offset: float = 10,
    demand_node_angle: int = 45,
    demand_name_prefix: str = "inlaat",
):
    # get tables and nodes
    node_table_df = get_node_table_with_from_to_node_ids(model, node_ids=list(discharge_supply_nodes.keys()))
    node_types = model.node.df["node_type"]

    # demand parameters
    summer_season_start: tuple[int, int] = (4, 1)
    winter_season_start: tuple[int, int] = (10, 1)
    time = [
        datetime(2020, *summer_season_start),
        datetime(2020, *winter_season_start),
        datetime(2021, *summer_season_start),
    ]

    for node_id, demand in discharge_supply_nodes.items():
        print(node_id)
        demand_flow_rate_summer = demand["summer"]
        demand_flow_rate_winter = demand["winter"]

        print(f"Adding supply_node {node_id}: summer={demand_flow_rate_summer} | winter={demand_flow_rate_winter}")
        demand_flow_str = (
            str(demand_flow_rate_summer)
            if demand_flow_rate_summer == demand_flow_rate_winter
            else f"{demand_flow_rate_summer}/{demand_flow_rate_winter}"
        )

        # update static table
        us_target_level = _target_level(
            model=model,
            node_id=cast(int, node_table_df.at[node_id, "from_node_id"]),
            target_level_column="meta_streefpeil",
            node_types=node_types,
            allow_missing=False,
        )
        assert us_target_level is not None

        model.update_node(
            node_id,
            "Outlet",
            [
                outlet.Static(
                    min_upstream_level=[us_target_level + us_target_level_offset_supply],
                    flow_rate=[0],
                    min_flow_rate=[float("nan")],
                    max_flow_rate=[float("nan")],
                )
            ],
        )

        # demand parameters
        demand_node_name = f"{demand_name_prefix} {demand_flow_str} [m3/s]"
        demand_tables = [
            flow_demand.Time(
                time=time,
                demand=[
                    float(demand_flow_rate_summer),
                    float(demand_flow_rate_winter),
                    float(demand_flow_rate_summer),
                ],
                demand_priority=[1, 1, 1],
            )
        ]
        cyclic = True
        node = model.get_node(node_id=node_id)
        demand_node = model.flow_demand.add(
            _offset_new_node(
                node=node,
                offset=new_nodes_offset,
                angle=demand_node_angle,
                name=demand_node_name,
                cyclic_time=cyclic,
            ),
            tables=demand_tables,
        )
        model.link.add(demand_node, node)


# %%
assert model.outlet.static.df is not None
outlet_max_flow_rate_aanvoer_by_node_id = dict.fromkeys(model.outlet.static.df.node_id.astype(int), 10.0)

# Handmatige inlaatcapaciteiten gelden ook in aanvoer; niet terugvallen op de default van 10 m3/s.
outlet_max_flow_rate_aanvoer_by_node_id.update(outlet_max_flow_rate_by_node_id)

aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")

assert model.pump.static.df is not None
model.pump.static.df.loc[model.pump.static.df.node_id.isin(list(EXCLUDE_NODES)), "flow_rate"] = 0
model.outlet.static.df.loc[model.outlet.static.df.node_id.isin(list(EXCLUDE_NODES)), "flow_rate"] = 0

# fixes
# Schabbert
model.outlet.static.df.loc[model.outlet.static.df.node_id == n("257IA"), "flow_rate"] = 12

# Was manning, wordt outlet, duiker
model.update_node(node_id=n("2740007"), node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=n("ow:2171320"), node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=n("2170333"), node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=n("2170184"), node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=n("2870102"), node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=n("2750399"), node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=n("2750346"), node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=n("2751"), node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=n("1070125"), node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=n("1070573"), node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=n("1140047"), node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=n("1140037"), node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=n("1140047"), node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=n("1070417"), node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=n("1100074"), node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=n("2800165___6"), node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=n("2800041"), node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=n("2810019"), node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=n("2790078"), node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=n("2800108"), node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=n("2940205"), node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=n("2940153"), node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=n("2940006"), node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=n("2920087"), node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=n("2920028"), node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=n("2830012"), node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=n("2110915"), node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=n("2040667"), node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=n("2041790___1"), node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=n("1020095"), node_type="Outlet")  # wordt outlet, was Manning
model.update_node(node_id=n("1020252"), node_type="Outlet")  # wordt outlet, was Manning

model.remove_node(n("201LGS"), remove_links=True)
model.remove_node(n("2011600"), remove_links=True)
model.remove_node(n("261OKK"), remove_links=True)
model.remove_node(n("loc:190367,375169"), remove_links=True)  # Parallel De Halte
model.remove_node(n("bnd:37"), remove_links=True)  # Boundary Parallel De Halte
# Mierlo wordt aanvoer als afvoer gemaal
model.update_node(node_id=n("234MIE"), node_type="Pump")  # wordt outlet, was outlet
model.update_node(node_id=n("234FL"), node_type="Pump")  # wordt outlet, was outlet


# Teveel inlaten naar Oefeltse Raam
model.remove_node(n("1140529"), remove_links=True)


# %%
# Toevoegen alle aanvoer-knopen met flow_demand
meta_codes = {
    # algemeen
    n("294EC"): "294EC",
    n("292F"): "WSL_Meij",
    n("275JD"): "275JD",
    n("287GB"): "287GB",
    n("287DF"): "287DF",
    n("275KDN"): "275ZKS",
    n("261BR"): "261LGD",
    # Bakelse Aa
    n("loc:188904,384285"): "261CZA",
    n("2611054"): "261JS",
    n("261CQA"): "261CQA",
    n("261JN"): "261JN",
    # Peelsche Loop
    n("261JI"): "261N",
    n("2530001"): "253V",
    n("loc:186922,393204"): "251U",
    n("253LG"): "253LG",
    n("251LAA"): "251LAA",
    # Maaskant-Oost
    #   n("114P17"): "114P18",
    n("114FF"): "114FF",
    n("108TDE"): "108TDE",
    n("loc:197255,397851"): "WSL_VBMB",
    n("1140510"): "114LAC",
    n("Inlaatgemaal Waranda"): "114WRD",
    # Maaskant-Midden
    n("104MGT"): "104MGT",
    n("103BIB"): "103BIB",
    # Mierlo
    n("234FL"): "234FL_1",
    n("217M"): "217L",
    # Whkanaal
    n("234CJ"): "234CJ",
    # Z_W West
    n("203KDZ"): "203KDZ",
    # Helmond
    n("201JVL"): "201JVL",
    # Peelrijt Dommel
    n("loc:174792,378640"): "280X",
    # Hurkske Aa
    n("257IA"): "257IA",
    n("254BAA"): "254BAA",
    n("235A"): "235VGZ",
}

for node_id, code in meta_codes.items():
    model.node.df.at[node_id, "meta_code_waterbeheerder"] = code

summer_col = "ZOMER"
winter_col = "WINTER"
code_col = "CODE_1"
discharge_supply_df = gpd.read_file(aanvoerpunten_path, layer=aanvoerpunten_layer).rename(
    columns={summer_col: "summer", winter_col: "winter", code_col: "code"}
)[["code", "summer", "winter", "geometry"]]
discharge_supply_df.index += 1

# drop Na
discharge_supply_df = discharge_supply_df[discharge_supply_df[["summer", "winter"]].notna().all(axis=1)]

# drop %
discharge_supply_df = discharge_supply_df[
    ~(
        discharge_supply_df["summer"].astype(str).str.endswith("%")
        | discharge_supply_df["winter"].astype(str).str.endswith("%")
    )
]

# convert to numeric
discharge_supply_df["summer"] = (
    discharge_supply_df["summer"].astype(str).str.replace(",", ".", regex=False).astype(float)
)
discharge_supply_df["winter"] = (
    discharge_supply_df["winter"].astype(str).str.replace(",", ".", regex=False).astype(float)
)

# make code (and node type) table
node_table_df = model.node.df
code_df = node_table_df[node_table_df["meta_code_waterbeheerder"].notna()][
    ["meta_code_waterbeheerder", "node_type"]
].rename(columns={"meta_code_waterbeheerder": "code"})
code_df = code_df.reset_index(drop=False).set_index("code")

discharge_supply_df = discharge_supply_df[discharge_supply_df["code"].isin(code_df.index.to_list())]
# Only the codes that are used need to be unique
used_codes = code_df.loc[code_df.index.isin(discharge_supply_df["code"]), "node_id"]
discharge_supply_df["node_id"] = discharge_supply_df["code"].map(used_codes)
discharge_supply_nodes = {
    int(cast(int, row.node_id)): {"summer": cast(float, row.summer), "winter": cast(float, row.winter)}
    for row in discharge_supply_df.itertuples()
}

# checken in laatste file Aa en Maas; dit kúnnen geen inlaten zijn
# discharge_supply_nodes.pop(n("275P"), None)
discharge_supply_nodes.pop(n("287A"), None)
discharge_supply_nodes.pop(n("227DPT"), None)
discharge_supply_nodes.pop(n("211VEL"), None)

# FlowDemand-inlaten krijgen hun debiet uit FlowDemand, niet uit afvoer-capaciteit overrides.
for node_id in discharge_supply_nodes:
    outlet_max_flow_rate_afvoer_by_node_id.pop(node_id, None)

discharge_supply_df.to_file(cloud.joinpath("AaenMaas/verwerkt/sturing/aanvoerpunten.gpkg"))


# add level supply nodes, no flow-demand-node, but discrete control on downstream basin level
level_supply_nodes = [
    n("AKW787@1"),
    n("117ENG"),
    n("201VSB"),
    n("104MGT"),
    n("257IA"),
    n("203NWH"),
    n("104DA"),
    n("211UWE"),
    n("205LFT"),
    #  n("114P17"),
    n("211RBW"),
    n("203KDZ"),
    n("102CLS"),
    n("253BCA"),
    n("117NS"),
    n("294EC"),
    n("211DRF"),
    n("205GVN"),
    n("205WRS"),
    n("101ITW"),
    n("275JK"),
    n("234SSW"),
    n("201BGS"),
    n("227BBA"),
    n("103BIB"),
    n("238BSW"),
    n("2340748"),
    n("2400055"),
    n("2110597"),
    n("2170697"),
    n("1040271"),
    n("2030323"),
    n("1020095"),
    n("2110006"),
    n("1170165"),
    n("204KLW"),
    n("116VMW"),
    n("108AA"),
    n("114FF"),
    n("261JJ"),
    n("104FTW"),
    n("loc:198625,397452"),
    n("loc:188889,379433"),
]

double_defined = [i for i in level_supply_nodes if i in discharge_supply_nodes]
if double_defined:
    raise ValueError(f"these nodes are labelled as q-supply and level-supply {double_defined}")

supply_nodes_df = get_node_table_with_from_to_node_ids(model=model, node_ids=level_supply_nodes)

all_nodes = list(discharge_supply_nodes.keys()) + level_supply_nodes

model.node.df[IS_SUPPLY_NODE_COLUMN] = model.node.df.index.isin(all_nodes)


# Gemaal Veluwe
model.update_node(node_id=n("211VEL"), node_type="Pump")  # wordt outlet, was outlet
model.pump.static.df.loc[model.pump.static.df.node_id == n("211VEL"), "flow_rate"] = 5

# Gemaal Kameren
model.update_node(node_id=n("211GOW"), node_type="Pump")  # wordt outlet, was outlet


# Grote Wetering is een gemaal
model.update_node(node_id=n("204GRW"), node_type="Pump")  # wordt pump was outlet
model.pump.static.df.loc[model.pump.static.df.node_id == n("204GRW"), "flow_rate"] = 1


# %%
# Toevoegen Mierlo

polygon = aanvoergebieden_df.loc[["Mierlo"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [
    lk("2342655", "up:2343"),
    lk("2340643", "up:2342570"),
    lk("2341056", "up:234SLO"),
    lk("2340606", "up:ow:2342340"),
    lk("2340650", "up:2342655"),
    lk("2340556", "up:2344550"),
    lk("2340572", "up:2344550"),
    lk("2341105", "up:234BUY"),
    lk("2343870", "up:234BUY"),
    lk("up:2030230___3", "2030230___3"),
    lk("up:2340589", "2340589"),
    lk("up:2340441", "2340441"),
    lk("up:2343", "2345"),
    lk("up:2341070", "2341070"),
]

# toevoegen sturing
add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    control_node_types=CONTROL_NODE_TYPES,
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
    add_supply_nodes=False,
)
# %%
# Toevoegen Leijgraaf

polygon = aanvoergebieden_df.loc[["Leijgraaf"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [
    lk("2052", "up:2050229"),
    lk("2040546", "up:204E"),
    lk("2270181", "up:2011065___1"),
    lk("2040398", "up:204BK"),
    lk("2320026", "up:232HAM"),
    lk("2110656", "up:211BMD"),
    lk("2110915", "up:2110631"),
    lk("2110664", "up:2040903"),
    lk("ow:2113865", "up:ow:2011330"),
    lk("up:2052", "2052"),
    lk("up:2040474", "2040474"),
    lk("up:2110692", "2110692"),
    lk("up:2040524", "2040524"),
    lk("up:2110597", "2110597"),
    lk("up:2040546", "2040849"),
    lk("up:2110909", "2110909"),
    lk("up:2110514", "2110514"),
    lk("up:2040398", "2040398"),
    lk("up:2343", "2345"),
    lk("up:ow:2113865", "ow:2113865"),
    lk("2040667", "up:204D"),
]

# toevoegen sturing
add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    control_node_types=CONTROL_NODE_TYPES,
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
    add_supply_nodes=False,
)
# %%
# Toevoegen Hurkske/Aa

polygon = aanvoergebieden_df.loc[["Hurkske/Aa"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [
    lk("2050175", "up:ow:2011330"),
    lk("2011270___3", "up:ow:2011330"),
    lk("2300025", "up:2010992"),
    lk("2016", "up:2010630"),
    lk("ow:2010580", "up:2010630"),
    lk("2010048", "up:201EPD"),
    lk("2270181", "up:2011065___1"),
    lk("2230024", "up:201B"),
    lk("2180009", "up:201B"),
    lk("2320026", "up:232HAM"),
    lk("ow:2113865", "up:ow:2011330"),
    lk("up:2010048", "2010048"),
    lk("up:2130024", "2130024"),
    lk("up:2230024", "2230024"),
    lk("up:2180009", "2180009"),
    lk("up:ow:2113865", "ow:2113865"),
]

# toevoegen sturing
add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    control_node_types=CONTROL_NODE_TYPES,
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
    add_supply_nodes=False,
)

# %%
# Toevoegen Z-W West (Zuid-Willemsvaart West)

polygon = aanvoergebieden_df.loc[["Z-W West"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [
    lk("2050175", "up:ow:2011330"),
    lk("2011270___3", "up:ow:2011330"),
    lk("2170131", "up:217J"),
    lk("ow:2171320", "up:2170674"),
    lk("2345", "up:ow:2343550"),
    lk("2170228", "up:217FA"),
    lk("2170184", "up:217FA"),
    lk("2170097", "up:217EJ"),
    lk("2170697", "up:2170131"),
    lk("up:2170228", "2170228"),
    lk("up:2170097", "2170097"),
    lk("up:2343", "2345"),
]

# toevoegen sturing
add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    control_node_types=CONTROL_NODE_TYPES,
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
    add_supply_nodes=False,
)


# %%
# Toevoegen Bakelse Aa

polygon = aanvoergebieden_df.loc[["Bakelse Aa"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [
    lk("2610884", "up:ow:2614050"),
    lk("2613715", "up:ow:2614050"),
    lk("2610781", "up:261BF"),
    lk("2751015___1", "up:275F"),
    lk("2751488", "up:275F"),
    lk("2750346", "up:2751488"),
    lk("up:2610884", "2610884"),
    lk("up:2750399", "2750399"),
]

# toevoegen sturing
add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    control_node_types=CONTROL_NODE_TYPES,
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
    add_supply_nodes=False,
)
# %%
# Toevoegen Peelsche Loop

polygon = aanvoergebieden_df.loc[["Peelsche Loop"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [lk("2110099", "up:2110005"), lk("up:2110099", "2110099")]

# toevoegen sturing
add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    control_node_types=CONTROL_NODE_TYPES,
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
    add_supply_nodes=False,
)
# %%
# Toevoegen Neerkant

polygon = aanvoergebieden_df.loc[["Neerkant"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [
    lk("2751", "up:275JB"),
    lk("2790002", "up:279H"),
    lk("2920028", "up:292C"),
    lk("up:2790002", "2790002"),
    lk("up:2870108", "2870108"),
]

# toevoegen sturing
add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    control_node_types=CONTROL_NODE_TYPES,
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
    add_supply_nodes=False,
)


# %%
# Toevoegen Someren

polygon = aanvoergebieden_df.loc[["Someren"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [
    lk("2940212", "up:294E"),
    lk("2940153", "up:294E"),
    lk("2940189", "up:294C"),
    lk("2940205", "up:294C"),
]

# toevoegen sturing
add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    control_node_types=CONTROL_NODE_TYPES,
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
    add_supply_nodes=False,
)
# %%
# Toevoegen Helmond

polygon = aanvoergebieden_df.loc[["Helmond"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [
    lk("2740007", "up:201M"),
    lk("2341105", "up:234BUY"),
    lk("2343870", "up:234BUY"),
    lk("up:2030230___3", "2030230___3"),
]

# toevoegen sturing
add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    control_node_types=CONTROL_NODE_TYPES,
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
    add_supply_nodes=False,
)


# %%
# Toevoegen Whkanaal (Wilhelminakanaal)

polygon = aanvoergebieden_df.loc[["Whkanaal"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [
    lk("2340718", "up:234BKS"),
    lk("2342655", "up:2343"),
    lk("2340746", "up:2343090"),
    lk("2340750", "up:2343090"),
    lk("up:2340739", "2340739"),
    lk("up:2340728", "2340728"),
    lk("up:2341070", "2341070"),
    lk("2170131", "up:217J"),
    lk("2170097", "up:217EJ"),
    lk("2170697", "up:2170131"),
    lk("up:2170097", "2170097"),
]

# toevoegen sturing
add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    control_node_types=CONTROL_NODE_TYPES,
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
    add_supply_nodes=False,
)


# %%
# Toevoegen Maaskant-Oost

polygon = aanvoergebieden_df.loc[["Maaskant-Oost"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [
    lk("1070370", "up:107P03"),
    lk("1080529", "up:108GAR"),
    lk("1070413", "up:108KAM"),
    lk("1102", "up:108FB"),
    lk("1070417", "up:107EBS"),
    lk("201NLR", "up:205BEU"),
    lk("1070039", "up:107P14"),
    lk("1100047", "up:1101330___1"),
    lk("1100122", "up:1101330___1"),
    lk("1140125", "up:114AQS"),
    lk("1140532", "up:114AQS"),
    lk("1140047", "up:1140521"),
    lk("1070231", "up:107P08"),
    lk("1070271", "up:107P08"),
    lk("1090085", "up:1090038"),
    lk("1142560", "up:1142610"),
    lk("up:1100122", "1100122"),
    lk("up:1100122", "1102"),
    lk("up:1140037", "1140037"),
    lk("up:1070231", "1070231"),
    lk("up:1140532", "1140532"),
    lk("up:1070039", "1070039"),
    lk("up:1140146", "1140146"),
    lk("up:1080529", "1080529"),
    lk("up:1080371", "1080371"),
    lk("up:1145950___1", "1145950___1"),
    lk("up:1101410___1", "1101410___1"),
]

# toevoegen sturing
add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    control_node_types=CONTROL_NODE_TYPES,
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
    add_supply_nodes=False,
)

# %%
# Toevoegen Maaskant-West

polygon = aanvoergebieden_df.loc[["Maaskant-West"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = []

# toevoegen sturing
add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    control_node_types=CONTROL_NODE_TYPES,
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
    add_supply_nodes=False,
)


# %%
# Toevoegen Maaskant-Midden

polygon = aanvoergebieden_df.loc[["Maaskant-Midden"], "geometry"].union_all()

# kleine buffer om scheurtjes te dichten; kies schaal passend bij je CRS!
polygon = polygon.buffer(0).buffer(0)

if isinstance(polygon, MultiPolygon):
    polygon = max(polygon.geoms, key=lambda g: g.area)


# links die intersecten die we kunnen negeren
# link_id: beschrijving
ignore_intersecting_links: list[int] = [
    lk("1087610___2", "up:1080970"),
    lk("2052", "up:2050229"),
    lk("2010048", "up:201EPD"),
    lk("1087515___5", "up:1087610___2"),
    lk("1012", "up:1010401"),
    lk("1030273", "up:102CLS"),
    lk("1040669", "up:1041980"),
    lk("ow:1041060", "up:1041980"),
    lk("1041470___1", "up:1041980"),
    lk("ow:1048400@3", "up:1010401"),
    lk("ow:1048300", "up:ow:1048400@2"),
    lk("up:1040582", "1040582"),
    lk("up:1040690", "1040690"),
    lk("up:1040669", "1040669"),
    lk("up:1040395", "1040395"),
]

# toevoegen sturing
add_controllers_to_supply_area(
    model=model,
    polygon=polygon,
    exclude_nodes=EXCLUDE_NODES,
    ignore_intersecting_links=ignore_intersecting_links,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    control_node_types=CONTROL_NODE_TYPES,
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
    add_supply_nodes=False,
)


# %%
# EXCLUDE NODES op 0 m3/s zetten

mask = model.outlet.static.df.node_id.isin(EXCLUDE_NODES)
model.outlet.static.df.loc[mask, "flow_rate"] = 0
model.outlet.static.df.loc[mask, "min_flow_rate"] = 0
model.outlet.static.df.loc[mask, "max_flow_rate"] = 0

# %% Toevoegen sturing op inlaten

# add discharge supply nodes -> no control, but flow-demand-node
add_discharge_supply_nodes(discharge_supply_nodes=discharge_supply_nodes)

# add level supply nodes -> discrete control, no flow-demand
add_controllers_to_supply_nodes(
    model=model,
    us_target_level_offset_supply=-0.04,
    supply_nodes_df=supply_nodes_df,
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
)

# %% add all remaining inlets/outlets
add_controllers_to_uncontrolled_connector_nodes(
    model=model,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    drain_nodes=drain_nodes,
    flushing_nodes=flushing_nodes,
    exclude_nodes=list(EXCLUDE_NODES),
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=outlet_max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
)

# %%

# Crevecoeur
mask = (model.outlet.static.df.node_id == n("AKW839")) & (model.outlet.static.df.control_state == "afvoer")
model.outlet.static.df.loc[mask, "flow_rate"] = 300
model.outlet.static.df.loc[mask, "max_flow_rate"] = 300

# 220: #253ZOM Deze stuw zit niet in model en lastig in te bouwen met huidige basins. Daarom Outlet 220 (253BG) en flow op 0 gezet
model.outlet.static.df.loc[model.outlet.static.df.node_id == n("253BG"), "flow_rate"] = 0

# Procentuele Verdeling Peelsche Loop uitlaten naar aanvoergebied Leygraaf opgelegd door flow_rate
model.outlet.static.df.loc[model.outlet.static.df.node_id == n("2110162"), "flow_rate"] = 50
model.outlet.static.df.loc[model.outlet.static.df.node_id == n("211MP"), "flow_rate"] = 25
model.outlet.static.df.loc[model.outlet.static.df.node_id == n("211MI"), "flow_rate"] = 20
model.outlet.static.df.loc[model.outlet.static.df.node_id == n("2110005"), "flow_rate"] = 5
model.outlet.static.df.loc[model.outlet.static.df.node_id == n("211LH"), "flow_rate"] = 0
model.outlet.static.df.loc[model.outlet.static.df.node_id == n("211SPV"), "flow_rate"] = 16.67
model.outlet.static.df.loc[model.outlet.static.df.node_id == n("211LK"), "flow_rate"] = 5

model.outlet.static.df.loc[model.outlet.static.df.node_id == n("2110162"), "max_flow_rate"] = 50
model.outlet.static.df.loc[model.outlet.static.df.node_id == n("211MP"), "max_flow_rate"] = 25
model.outlet.static.df.loc[model.outlet.static.df.node_id == n("211MI"), "max_flow_rate"] = 20
model.outlet.static.df.loc[model.outlet.static.df.node_id == n("2110005"), "max_flow_rate"] = 5
model.outlet.static.df.loc[model.outlet.static.df.node_id == n("211LH"), "max_flow_rate"] = 0
model.outlet.static.df.loc[model.outlet.static.df.node_id == n("211SPV"), "max_flow_rate"] = 16.67
model.outlet.static.df.loc[model.outlet.static.df.node_id == n("211LK"), "max_flow_rate"] = 5

reverse_link_ids = [lk("235A", "up:2011600"), lk("up:2320035", "235A")]


for link_id in reverse_link_ids:
    model.reverse_link(link_id=link_id)


# Inlaat Sambeek
model.update_node(node_id=n("Inlaatgemaal Waranda"), node_type="Pump")  # wordt pump was outlet
model.pump.static.df.loc[model.pump.static.df.node_id == n("Inlaatgemaal Waranda"), "flow_rate"] = 0
mask = model.pump.static.df.node_id == n("Inlaatgemaal Waranda")
model.pump.static.df.loc[mask, "min_upstream_level"] = 10.36


model.update_node(node_id=n("114HKS"), node_type="Pump")  # wordt pump, was outlet
model.pump.static.df.loc[model.pump.static.df.node_id == n("114HKS"), "flow_rate"] = 0

# Gemaal Mierlo
model.update_node(node_id=n("234FL"), node_type="Pump")  # wordt pump, was outlet
model.pump.static.df.loc[model.pump.static.df.node_id == n("234FL"), "flow_rate"] = 0

# Inlaten aan Drongelens kanaal krijgen pd.NA bij min_upstream_level
mask = model.outlet.static.df.node_id.isin([n("117BSL"), n("VRKW118")])
model.outlet.static.df.loc[mask, "min_upstream_level"] = -math.inf
mark_level_update_protected(model.outlet.static.df, mask, model=model)

boundary_ids = [
    n("bnd:9"),
    n("bnd:13"),
    n("bnd:39"),
    n("bnd:38"),
    n("bnd:53"),
    n("up:2611054"),
    n("up:261OKK"),
    n("loc:195714,406068"),
    n("bnd:33"),
    n("bnd:32"),
    n("bnd:31"),
    n("bnd:59"),
    n("bnd:54"),
    n("bnd:44"),
    n("bnd:42"),
    n("bnd:64"),
    n("bnd:63"),
]
assert model.level_boundary.static.df is not None
mask = model.level_boundary.static.df["node_id"].isin(boundary_ids)
model.level_boundary.static.df.loc[mask, "level"] = model.level_boundary.static.df.loc[mask, "level"] + 0.04


boundary_levels = {
    n("bnd:38"): 31,
}

for node_id, level in boundary_levels.items():
    model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id == node_id, "level"] = level


# 201JSL is een RWS-inlaat naar Aa en Maas, geen afvoer richting Aa en Maas.
mask = (model.outlet.static.df.node_id == n("201JSL")) & (model.outlet.static.df.control_state == "aanvoer")
model.outlet.static.df.loc[mask, "flow_rate"] = 20
model.outlet.static.df.loc[mask, "max_flow_rate"] = 20
mask = (model.outlet.static.df.node_id == n("201JSL")) & (model.outlet.static.df.control_state == "afvoer")
model.outlet.static.df.loc[mask, "flow_rate"] = 0
model.outlet.static.df.loc[mask, "max_flow_rate"] = 0

# Pomp-capaciteiten op basis van hoogste berekende dynamic debiet, afgerond naar boven.
pump_max_flow_rate_from_results = {
    n("205BEU"): 2,  # naam onbekend; oude static flow_rate=0.333
    n("108FOW"): 1,  # Fortunaweg; oude static flow_rate=0.05
}
mask = (
    model.pump.static.df.node_id.isin(pump_max_flow_rate_from_results)
    & model.pump.static.df.flow_rate.notna()
    & (model.pump.static.df.flow_rate > 0)
)
model.pump.static.df.loc[mask, "max_flow_rate"] = model.pump.static.df.loc[mask, "node_id"].map(
    pump_max_flow_rate_from_results
)

# %% Junctionfy(!)
junctionify(model)

aanvoer_only_node_ids = set(supply_nodes) - set(drain_nodes) - set(flow_control_nodes)

# Aanvoer-cap: doorlaten/inlaten mogen in aanvoer niet de hoge afvoercapaciteit gebruiken.
aanvoer_outlet_mask = model.outlet.static.df.control_state == "aanvoer"
model.outlet.static.df.loc[aanvoer_outlet_mask, ["flow_rate", "max_flow_rate"]] = model.outlet.static.df.loc[
    aanvoer_outlet_mask, ["flow_rate", "max_flow_rate"]
].clip(upper=10.0)
zero_aanvoer_node_ids = {
    node_id for node_id, max_flow_rate in outlet_max_flow_rate_aanvoer_by_node_id.items() if max_flow_rate == 0
}
zero_aanvoer_mask = aanvoer_outlet_mask & model.outlet.static.df.node_id.isin(zero_aanvoer_node_ids)
model.outlet.static.df.loc[zero_aanvoer_mask, ["flow_rate", "max_flow_rate"]] = 0.0

for static_df, max_flow_rate_by_node_id in (
    (model.outlet.static.df, outlet_max_flow_rate_by_node_id),
    (model.pump.static.df, pump_max_flow_rate_by_node_id),
):
    assert static_df is not None
    max_flow_rate = static_df["node_id"].map(max_flow_rate_by_node_id)
    aanvoer_mask = (
        static_df["control_state"].eq("aanvoer")
        & static_df["node_id"].isin(aanvoer_only_node_ids)
        & max_flow_rate.notna()
    )
    static_df.loc[aanvoer_mask, "flow_rate"] = max_flow_rate[aanvoer_mask]
    static_df.loc[aanvoer_mask, "max_flow_rate"] = max_flow_rate[aanvoer_mask]

# Afvoer-cap: voorkom blokkades door te lage max_flow_rate in afvoer.
node_type_by_id = model.node.df["node_type"].to_dict()
flow_demand_controlled_node_ids = set(
    model.link.df.loc[
        model.link.df["from_node_id"].map(node_type_by_id).eq("FlowDemand"),
        "to_node_id",
    ]
    .dropna()
    .astype(int)
)
manual_max_flow_rate_node_ids = set(outlet_max_flow_rate_by_node_id)
manual_max_flow_rate_node_ids.update(pump_max_flow_rate_by_node_id)
manual_max_flow_rate_node_ids.add(n("201JSL"))
protected_max_flow_rate_node_ids = set(EXCLUDE_NODES) | flow_demand_controlled_node_ids | manual_max_flow_rate_node_ids
for static_df in (model.outlet.static.df, model.pump.static.df):
    afvoer_mask = (
        static_df["control_state"].eq("afvoer")
        & static_df["flow_rate"].fillna(0).gt(0)
        & ~static_df["node_id"].isin(protected_max_flow_rate_node_ids)
    )
    static_df.loc[afvoer_mask, "max_flow_rate"] = static_df.loc[afvoer_mask, "max_flow_rate"].fillna(10).clip(lower=10)

for static_df in (model.outlet.static.df, model.pump.static.df):
    afvoer_mask = static_df["control_state"].eq("afvoer") & static_df["node_id"].isin(aanvoer_only_node_ids)
    static_df.loc[afvoer_mask, ["flow_rate", "max_flow_rate"]] = 0.0

# Model run


assert model.discrete_control.condition.df is not None
model.discrete_control.condition.df.loc[model.discrete_control.condition.df.time.isna(), ["time"]] = model.starttime

# %%

# write the dry and wet steady state scenarios and the full control model
model = write_steady_state_scenarios(
    model,
    AUTHORITY,
    SHORT_NAME,
    dry_evaporation_mm_per_day=0.1,
    dry_period=(datetime(2020, 5, 1), datetime(2020, 9, 1)),
    wet_period=(datetime(2020, 1, 1), datetime(2020, 4, 1)),
    model_exec=MODEL_EXEC,
    qlr_path=qlr_path,
)

# %%
