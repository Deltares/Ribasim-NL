# %%
from pathlib import Path
from typing import cast

import geopandas as gpd
from ribasim import Node
from ribasim.nodes import level_boundary
from ribasim_nl.control import (
    add_controllers_to_supply_area,
    add_controllers_to_uncontrolled_connector_nodes,
    mark_level_update_protected,
    print_node_list_diff,
)
from ribasim_nl.junctions import junctionify
from ribasim_nl.keys import KEY_COLUMN
from ribasim_nl.run_model import write_steady_state_scenarios

from ribasim_nl import CloudStorage, Model

# execute model run
MODEL_EXEC: bool = False

# model settings
AUTHORITY: str = "Vechtstromen"
SHORT_NAME: str = "vechtstromen"
CONTROL_NODE_TYPES = ["Outlet", "Pump"]
IS_SUPPLY_NODE_COLUMN: str = "meta_supply_node"


# %%
# Helpers


def add_supply_area_control(
    model: Model,
    aanvoergebieden_df: gpd.GeoDataFrame,
    area_name: str,
    ignore_intersecting_links: list[int],
    supply_nodes: list[int],
    drain_nodes: list[int],
    flow_control_nodes: list[int],
) -> None:
    polygon = aanvoergebieden_df.loc[[area_name], "geometry"].union_all()

    # toevoegen sturing
    add_controllers_to_supply_area(
        model=model,
        polygon=polygon,
        exclude_nodes=EXCLUDE_NODES,
        ignore_intersecting_links=ignore_intersecting_links,
        drain_nodes=drain_nodes,
        supply_nodes=supply_nodes,
        flow_control_nodes=flow_control_nodes,
        flushing_nodes={},
        control_node_types=CONTROL_NODE_TYPES,
        add_supply_nodes=True,
        flow_rate_aanvoer=20.0,
        max_flow_rate_aanvoer=max_flow_rate_aanvoer_by_node_id,
        flow_rate_afvoer=100.0,
        max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
    )


def set_max_flow_rate(static_df, max_flow_rate_by_node_id: dict[int, float]) -> None:
    if static_df is None:
        return

    max_flow_rate = static_df["node_id"].map(max_flow_rate_by_node_id)
    mask = max_flow_rate.notna()

    static_df.loc[mask, "max_flow_rate"] = max_flow_rate[mask]
    static_df.loc[mask, "flow_rate"] = max_flow_rate[mask]


def node_list(*groups: list[int]) -> list[int]:
    return [node_id for group in groups for node_id in group]


def reverse_existing_links(model: Model, link_ids: list[int]) -> None:
    for link_id in link_ids:
        if link_id in model.link.df.index:
            model.reverse_link(link_id=link_id)


def validate_file_gdb(gdb_path: Path) -> None:
    if not gdb_path.is_dir():
        raise FileNotFoundError(
            f"{gdb_path} is geen lokale FileGDB-directory. Synchroniseer dit pad opnieuw met overwrite=True."
        )


def lower_outlet_max_downstream_level(model: Model, node_id: int, offset: float = 0.01) -> None:
    downstream_basin_ids = model.link.df.loc[
        (model.link.df.from_node_id == node_id) & model.link.df.to_node_id.isin(model.basin.node.df.index),
        "to_node_id",
    ].unique()
    if len(downstream_basin_ids) != 1:
        raise ValueError(f"Inlaat {node_id}: verwacht 1 benedenstrooms basin, gevonden {downstream_basin_ids}")

    downstream_basin_id = int(downstream_basin_ids[0])
    assert model.basin.area.df is not None
    downstream_target_level = model.basin.area.df.loc[
        model.basin.area.df.node_id == downstream_basin_id,
        "meta_streefpeil",
    ].dropna()
    if downstream_target_level.empty:
        raise ValueError(f"Inlaat {node_id}: geen streefpeil gevonden voor benedenstrooms basin {downstream_basin_id}")

    assert model.outlet.static.df is not None
    mask = model.outlet.static.df.node_id == node_id
    if "control_state" in model.outlet.static.df.columns:
        aanvoer_mask = mask & (model.outlet.static.df.control_state == "aanvoer")
        if aanvoer_mask.any():
            mask = aanvoer_mask

    model.outlet.static.df.loc[mask, "max_downstream_level"] = float(downstream_target_level.iloc[0]) - offset
    mark_level_update_protected(model.outlet.static.df, mask, model=model)


def duplicate_level_boundary_for_link(model: Model, source_node_id: int, link_id: int) -> int | None:
    if link_id not in model.link.df.index:
        return None

    current_from_node_id = int(cast(int, model.link.df.at[link_id, "from_node_id"]))
    current_to_node_id = int(cast(int, model.link.df.at[link_id, "to_node_id"]))
    if current_from_node_id != source_node_id:
        if current_from_node_id in model.level_boundary.node.df.index:
            return current_from_node_id
        if current_to_node_id in model.level_boundary.node.df.index:
            return current_to_node_id
        raise ValueError(f"Link {link_id} is niet verbonden met level boundary {source_node_id}.")

    if source_node_id not in model.level_boundary.node.df.index:
        raise ValueError(f"Node {source_node_id} is geen level boundary in dit model.")

    source_node = model.level_boundary[source_node_id]
    source_name = model.node.df.at[source_node_id, "name"] if "name" in model.node.df.columns else ""
    if not isinstance(source_name, str):
        source_name = ""
    new_name = f"{source_name} extra boundary".strip()
    # the copy gets the key of its source with a number, see `ribasim_nl.keys`
    boundary_node = model.level_boundary.add(
        Node(
            geometry=source_node.geometry, name=new_name, meta_key=f"{model.node.df.at[source_node_id, KEY_COLUMN]}@2"
        ),
        tables=[level_boundary.Static(level=[0.0])],
    )

    new_node_id = boundary_node.node_id

    not_copied = {"node_type", "name", "geometry", KEY_COLUMN}
    node_columns = [column for column in model.node.df.columns if column not in not_copied]
    model.node.df.loc[new_node_id, node_columns] = model.node.df.loc[source_node_id, node_columns]

    assert model.level_boundary.static.df is not None
    source_static_mask = model.level_boundary.static.df.node_id == source_node_id
    new_static_mask = model.level_boundary.static.df.node_id == new_node_id
    static_columns = [column for column in model.level_boundary.static.df.columns if column != "node_id"]
    source_static = model.level_boundary.static.df.loc[source_static_mask, static_columns].iloc[0].dropna()
    model.level_boundary.static.df.loc[new_static_mask, source_static.index] = source_static

    model.redirect_link(link_id=link_id, from_node_id=new_node_id)

    return new_node_id


# %%
# Definieren paden en syncen met cloud

cloud = CloudStorage()

ribasim_model_dir = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_parameterized_model")
ribasim_toml = ribasim_model_dir / f"{SHORT_NAME}.toml"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_aanvoer.qlr")
aanvoergebieden_gpkg = cloud.joinpath(AUTHORITY, "verwerkt", "sturing", "aanvoergebieden.gpkg")
lhm_gemaal_gdb = cloud.joinpath(AUTHORITY, "verwerkt", "1_ontvangen_data", "LHM20230418.gdb")


# %%
# Read data

model = Model.read(ribasim_toml)
# nodes and links are referred to by their key, see docs/dev/node-keys.qmd
n = model.node_id
lk = model.link_id

# Sluizen die geen rol hebben in de waterverdeling (aanvoer/afvoer), maar wel in het model zitten
EXCLUDE_NODES = {n("SL00004"), n("SL00001"), n("DK21403")}

aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")

# Splits de bestaande boundary bij node 15 zodat link 2822 een eigen boundary node krijgt.
duplicate_level_boundary_for_link(model=model, source_node_id=n("bnd:15"), link_id=lk("bnd:15", "SL00010"))

reverse_existing_links(
    model,
    [
        lk("GM00232", "bnd:15"),
        lk("up:DK25518", "GM00232"),
        lk("bnd:15@2", "SL00010"),
        lk("SL00010", "up:DK25518"),
        lk("up:ST03541", "DK01810"),
        lk("DK01810", "up:DK04366"),
    ],
)
# De Haandrik: ManningResistance 49 voert meestal af van Basin 2222 naar Basin 1700, zie #846
reverse_existing_links(model, [lk("SL00002", "up:ow:WL02747-4"), lk("up:DK22015", "SL00002")])

# Gemaal Orveltersluis was opggedeeld
model.remove_node(node_id=n("GM00231"), remove_links=True)
model.remove_node(node_id=n("DK22326"), remove_links=True)

model.redirect_link(link_id=lk("GM00135", "up:DK20656"), to_node_id=n("up:DK20656"))


# %%
# Topologie fixes


# %%
# Max-capaciteiten inlaten
# Uit waterakkoord

outlet_max_flow_rate_by_node_id = {
    n("ST00633"): 1.75,  # punt 902 - Vroomshoop
    n("PT00526"): 0.63,  # punt 56 - Banisschuiven
    n("ST01777"): 0.8,  # punt 62 - Geerdijk (Hammerflier)
    n("DK22903"): 1.5,  # punt 68 - Radewijkerbeek
    n("ST00591"): 0.35,  # punt 87 - Twickelervaart Delden
    n("ST02055"): 0.5,  # punt 106 - Twickelervaart
    n("GM00020"): 0.1,  # punt 52 - Bolscherbeek
    n("loc:238366,496287"): 0.28,  # punt 60 - Vriezenveense Veenkanaal
    n("DK23361"): 0.16,  # punt 64 - Brucht
    n("DK23474"): 0.18,  # punt 65 - Zilverveen
    n("DK22934"): 0.03,  # punt 66 - Emtenbroek
    n("DK17947"): 0.02,  # punt 67 - Eggenweg/Hoogeweg
    n("DK14459"): 1.0,  # punt 77 - De Stouwe
    n("DK17872"): 0.3,  # punt 90 - Boerendijk of Lijntje (Dooze)
    n("DK25085"): 0.06,  # punt 109 - Roskam
    n("DK23050"): 0.06,  # punt 911 - Steenmaat
    n("DK20932"): 0.02,  # punt 912 - Koenderink
    n("SL00015"): 8,  # Sluis Aadorp
    n("ST02574"): 6.21,  # aflaat punt 1 - Usselerstroom
    n("ST00695"): 0.81,  # aflaat punt 2 - Schoolbeek
    n("ST02015"): 4.76,  # aflaat punt 7 - Nieuwe Oelerbeek
    n("ST00645"): 12.87,  # aflaat punt 9/15 - Hagmolenbeek / Oude Hagmolenbeek
    n("ST01950"): 0.22,  # aflaat punt 10 - waterleiding vd Exterkottenlanden
    n("ST02269"): 2.54,  # aflaat punt 16 - Wienerveldsleiding
    n("ST00168"): 0.06,  # aflaat punt 17 - Bentelerbeek
    n("ST00067"): 48.0,  # aflaat punt 19/30 - Bolscherbeek / Overstort Bolksbeek
    n("ST00558"): 2.61,  # aflaat punt 23 - Boven-Regge/Diepenh. Molenbeek
    n("ST04702"): 8.98,  # aflaat punt 21 - Poelsbeek (doorlaat)
    n("ST03089"): 7.84,  # aflaat punt 31 - Grote Waterleiding (doorlaat)
}
outlet_max_flow_rate_parameterized_zero_by_node_id = {
    n("ow:WL05791-16@1"): 100.0,  # xlsx max_flow_rate nul; voorkom afvoerblokkade
    n("loc:268243,491536"): 100.0,  # xlsx max_flow_rate nul; voorkom afvoerblokkade
}
outlet_max_flow_rate_from_results = {
    n("bnd:26"): 2,  # naam onbekend
    n("SL00010"): 2,  # SL00010
    n("ST04531"): 2,  # Inlaat Kleine Rietplas
    n("ST01050"): 11,  # Stuw Rompelman Linderbeek
    n("ST04518"): 2,  # Inlaat Nijboer
    n("ST03490"): 2,  # Inlaatschuif Witten Noord
    n("ST01448"): 11,  # Overlaat Assenhoek
    n("ST00266"): 2,  # Inlaatschuif Omvloed
    n("ST00332"): 2,  # Drempel bypass Vispassage Melodiestraat
    n("ST02636"): 5,  # Overlaat Groot Boerman
    n("ST03266"): 6,  # Stuw RW 34
    n("ST03471"): 11,  # Stuw Linderdijk
    n("ST02204"): 2,  # Overlaat Esbeek-Vinkebeek
    n("ST01031"): 2,  # Stuw Melodiestraat
    n("ST00167"): 2,  # Stuw Zuivering Enschede West
    n("ST03222"): 11,  # Stuw Het Bergje
    n("ST00579"): 2,  # Overlaat Broeierdweg
    n("ST00106"): 12,  # Stuw Pelmolen
    n("ST03459"): 8,  # Stuw Dalerveense straat
    n("ST03458"): 6,  # Stuw Galgaten
    n("ST04277"): 2,  # Inlaatschuif Havenstraat
    n("ST02259"): 30,  # Stuw Bremmer 1 bij Russendijk
    n("ST03983"): 5,  # Overlaat Energiefabriek Hengelo
    n("ST02546"): 3,  # Stuw Heemafstraat
    n("DK31294"): 2,  # DK31294
    n("DK00646"): 2,  # naam onbekend
    n("DK31245"): 2,  # naam onbekend
    n("DK06419"): 15,  # naam onbekend
    n("DK29364"): 2,  # naam onbekend
    n("DK22337"): 2,  # naam onbekend
    n("DK03955"): 2,  # naam onbekend
    n("DK16430"): 2,  # naam onbekend
    n("DK25521"): 2,  # naam onbekend
    n("DK08342"): 2,  # naam onbekend
    n("DK07569"): 2,  # naam onbekend
    n("DK21919"): 2,  # naam onbekend
    n("DK16490"): 2,  # naam onbekend
    n("DK12866"): 2,  # naam onbekend
    n("DK08116"): 3,  # naam onbekend
    n("DK00667"): 2,  # naam onbekend
    n("DK29246"): 2,  # naam onbekend
    n("DK28807"): 2,  # naam onbekend
    n("DK00370"): 2,  # naam onbekend
    n("DK01810"): 3,  # naam onbekend
    n("DK07807"): 2,  # naam onbekend
    n("DK07969"): 2,  # naam onbekend
    n("DK04478"): 3,  # naam onbekend
    n("DK01985"): 2,  # naam onbekend
    n("DK07894"): 2,  # naam onbekend
    n("DK05763"): 2,  # naam onbekend
    n("DK10665"): 2,  # naam onbekend
    n("SY00175"): 2,  # naam onbekend
    n("DK29761"): 2,  # naam onbekend
    n("DK23608"): 2,  # naam onbekend
    n("DK13718"): 2,  # naam onbekend
    n("DK06546"): 2,  # naam onbekend
    n("DK06882"): 2,  # naam onbekend
    n("DK13721"): 2,  # naam onbekend
    n("DK05584"): 2,  # naam onbekend
    n("DK09749"): 2,  # naam onbekend
    n("DK11840"): 2,  # naam onbekend
    n("DK12885"): 2,  # naam onbekend
    n("DK10564"): 2,  # naam onbekend
    n("DK03448"): 2,  # naam onbekend
    n("DK08881"): 2,  # naam onbekend
    n("DK20566"): 2,  # naam onbekend
    n("DK03987"): 2,  # naam onbekend
    n("DK21938"): 2,  # naam onbekend
    n("DK11298"): 2,  # naam onbekend
    n("DK11424"): 2,  # naam onbekend
    n("DK10000"): 2,  # naam onbekend
    n("DK31895"): 2,  # naam onbekend
    n("DK24281"): 2,  # naam onbekend
    n("DK01495"): 2,  # naam onbekend
    n("DK13662"): 2,  # naam onbekend
    n("DK01290"): 2,  # naam onbekend
    n("DK12545"): 2,  # naam onbekend
    n("DK06526"): 2,  # naam onbekend
    n("DK01305"): 2,  # naam onbekend
    n("DK08441"): 2,  # naam onbekend
    n("DK13021"): 2,  # naam onbekend
    n("DK07090"): 2,  # naam onbekend
    n("DK11781"): 2,  # naam onbekend
    n("DK26292"): 2,  # naam onbekend
    n("DK06236"): 2,  # naam onbekend
    n("DK28416"): 3,  # naam onbekend
    n("DK04268"): 2,  # naam onbekend
    n("DK29245"): 2,  # naam onbekend
    n("SY00177"): 5,  # naam onbekend
    n("DK07883"): 11,  # naam onbekend
    n("DK14235"): 2,  # naam onbekend
    n("DK07712"): 2,  # naam onbekend
    n("DK01629"): 29,  # naam onbekend
    n("DK06557"): 2,  # naam onbekend
    n("DK30499"): 2,  # naam onbekend
    n("DK25702"): 3,  # naam onbekend
    n("DK04356"): 2,  # naam onbekend
    n("DK14315"): 3,  # naam onbekend
    n("DK19079"): 5,  # naam onbekend
    n("DK22716"): 5,  # naam onbekend
    n("DK21233"): 3,  # DK21233
    n("DK25153"): 2,  # naam onbekend
    n("DK17873"): 6,  # naam onbekend
    n("DK18155"): 2,  # naam onbekend
    n("ow:WL01959-32"): 27,  # naam onbekend
    n("ow:WL01052-8"): 3,  # naam onbekend
    n("ow:WL00859-0"): 5,  # naam onbekend
    n("ow:WL05820-5"): 2,  # naam onbekend
    n("ow:WL01863-9"): 2,  # naam onbekend
    n("ow:WL01803-3"): 2,  # naam onbekend
    n("ow:WL01758-3"): 2,  # naam onbekend
    n("ow:WL00840-0"): 2,  # naam onbekend
    n("ow:WL00609-4-0-0"): 2,  # naam onbekend
    n("ow:WL00607-1-0-0"): 2,  # naam onbekend
    n("ow:WL06694-3"): 2,  # naam onbekend
    n("WL02311"): 2,  # naam onbekend
    n("WL05569___3"): 2,  # naam onbekend
    n("WL02126-1___4"): 2,  # naam onbekend
    n("ow:WL01052-0"): 3,  # naam onbekend
    n("WL02926-0___1"): 81,  # naam onbekend
    n("WL00510-1___16"): 2,  # naam onbekend
    n("WL00510-1___6"): 2,  # naam onbekend
    n("loc:238834,496508"): 3,  # Uitlaat Westerhaar
}
outlet_max_flow_rate_coupled_by_node_id = {
    n("ST03059"): 6,  # Debietbegrenzer Duitslandweg; gekoppeld max=3.36, parameterized=0.91
    n("ST01448"): 32,  # Overlaat Assenhoek; gekoppeld max=20.82, parameterized=3.27
    n("ST03138"): 30,  # gekoppeld max=19.71, huidige max=0.00, link=4400599
    n("ST03627"): 30,  # gekoppeld max=19.79, huidige max=4.05, link=4400650
    n("ST03471"): 20,  # Stuw Linderdijk; gekoppeld max=12.70, parameterized=3.58
    n("ST04446"): 12,  # gekoppeld max=7.28, huidige max=0.00, link=4400871
    n("ST03459"): 11,  # Stuw Dalerveense straat; gekoppeld max=6.21, parameterized=2.01
    n("ST03458"): 9,  # Stuw Galgaten; gekoppeld max=5.90, parameterized=1.51
    n("ST00245"): 3,  # Inlaat Daarleschebeek; gekoppeld max=1.50, parameterized=0.40
    n("DK20616"): 6,  # gekoppeld max=3.78, huidige max=0.00, link=4401061
    n("DK20558"): 2,  # parameterized nul; gekoppeld max=0.15
    n("DK07883"): 32,  # gekoppeld max=20.82, parameterized=0.06
    n("DK06557"): 5,  # gekoppeld max=2.12, parameterized=0.35
    n("DK20441"): 12,  # gekoppeld max=7.11, huidige max=0.00, link=4400826
    n("DK19591"): 11,  # gekoppeld max=6.98, huidige max=0.00, link=4401176
    n("DK25702"): 147,  # gekoppeld max=97.96, parameterized=0.23
    n("DK20196_BO"): 30,  # gekoppeld max=20.00, huidige max=0.00, link=8000941
    n("DK20968"): 3,  # gekoppeld max=1.81, parameterized=0.40
    n("loc:263698,528136"): 2,  # gekoppeld max=0.58, huidige max=0.00, link=8000976
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
assert model.outlet.static.df is not None
outlet_max_flow_rate_aanvoer_by_node_id = dict.fromkeys(model.outlet.static.df.node_id.astype(int), 10.0)

# Handmatige inlaatcapaciteiten gelden ook in aanvoer; niet terugvallen op de default van 10 m3/s.
outlet_max_flow_rate_aanvoer_by_node_id.update(outlet_max_flow_rate_by_node_id)

pump_max_flow_rate_by_node_id = {
    n("GM00021"): 0.42,  # punt 53 - Stokkumerflier
    n("GM00248"): 1.2,  # punt 904 - Stieltjeskanaalsluis
    n("GM00181"): 0.14,  # punt 54 - Herikerflier
    n("GM00146"): 0.2,  # punt 74 - Junne (vecht)
    n("GM00195"): 0.15,  # punt 104 - Oude Poelsebeek
    n("GM00207"): 0.0,  # punt 107 - Hedeman
    n("aanvoergemaal-westerhaar"): 0.88,  # punt 905 - Westerhaar
    n("DK29483"): 0.65,  # punt 103 - Oude Hagmolenbeek
    n("GM00033"): 10.0,  # aflaat punt 13 - Banisgemaal
    n("GM00048"): 0.33,  # aflaat punt 24 - Leidebeek
    n("GM00119"): 0.92,  # aflaat punt 35 - Afwatering vd Bierkamp
    n("GM00003"): 0.56,  # aflaat punt 22 - Stokkumerflier (doorlaat)
    n("GM00244"): 5.8,  # Ericasluis
    n("GM00232"): 1.2,  # Orveltersluis
    n("GM00245"): 1.2,  # Oranjesluis
}
pump_max_flow_rate_from_results = {
    n("GM00150"): 20,  # gekoppeld max=19.80, huidige max=0.00, link=4400277
}
max_flow_rate_aanvoer_by_node_id = {
    **outlet_max_flow_rate_aanvoer_by_node_id,
    **pump_max_flow_rate_by_node_id,
}


set_max_flow_rate(model.outlet.static.df, outlet_max_flow_rate_by_node_id)
set_max_flow_rate(model.pump.static.df, pump_max_flow_rate_by_node_id)
assert model.pump.static.df is not None
mask = (
    model.pump.static.df.node_id.isin(pump_max_flow_rate_from_results)
    & model.pump.static.df.flow_rate.notna()
    & (model.pump.static.df.flow_rate > 0)
)
model.pump.static.df.loc[mask, "max_flow_rate"] = model.pump.static.df.loc[mask, "node_id"].map(
    pump_max_flow_rate_from_results
)

# %%Duikers voor nu op 1m3/s
node_ids = model.outlet.node.df[model.outlet.node.df["meta_object_type"] == "duikersifonhevel"].index
mask = model.outlet.static.df["node_id"].isin(node_ids)


def print_supply_role_conflicts(
    label: str,
    source_supply_nodes: list[int],
    manual_drain_nodes: list[int],
    manual_flow_control_nodes: list[int],
) -> None:
    source_supply_node_set = set(source_supply_nodes)
    print(
        f"{label} overruled door handmatig: "
        f"flow_control={sorted(source_supply_node_set & set(manual_flow_control_nodes))}, "
        f"drain={sorted(source_supply_node_set & set(manual_drain_nodes))}"
    )


def combine_control_node_roles(
    kunstwerk_supply_nodes: list[int],
    kunstwerk_drain_nodes: list[int],
    manual_supply_nodes: list[int],
    manual_drain_nodes: list[int],
    manual_flow_control_nodes: list[int],
) -> tuple[list[int], list[int], list[int]]:
    flow_control_nodes = list(dict.fromkeys(manual_flow_control_nodes))
    supply_nodes = list(dict.fromkeys(node_id for node_id in manual_supply_nodes if node_id not in flow_control_nodes))
    drain_nodes = list(
        dict.fromkeys(
            node_id
            for node_id in manual_drain_nodes
            if node_id not in flow_control_nodes and node_id not in supply_nodes
        )
    )

    manual_nodes = set(flow_control_nodes + supply_nodes + drain_nodes)
    kunstwerk_supply_nodes = list(
        dict.fromkeys(node_id for node_id in kunstwerk_supply_nodes if node_id not in manual_nodes)
    )
    kunstwerk_drain_nodes = list(
        dict.fromkeys(
            node_id
            for node_id in kunstwerk_drain_nodes
            if node_id not in manual_nodes and node_id not in kunstwerk_supply_nodes
        )
    )

    supply_nodes += kunstwerk_supply_nodes
    drain_nodes += kunstwerk_drain_nodes

    return supply_nodes, drain_nodes, flow_control_nodes


def normalize_name_series(series):
    return series.astype("string").str.strip().str.replace(r"\s+", " ", regex=True).str.lower()


def get_lhm_gemaal_supply_nodes(
    model: Model,
    lhm_gemaal_gdb: Path,
    layer: str = "GEMAAL",
    name_column: str = "NAAM",
    name_prefix: str = "inlaat",
    node_types: list[str] = CONTROL_NODE_TYPES,
) -> list[int]:
    validate_file_gdb(lhm_gemaal_gdb)
    gemaal_df = gpd.read_file(lhm_gemaal_gdb, layer=layer)

    inlaat_mask = normalize_name_series(gemaal_df[name_column]).str.startswith(name_prefix.lower(), na=False)
    inlaat_names = set(normalize_name_series(gemaal_df.loc[inlaat_mask, name_column]).dropna())

    control_node_df = model.node.df[model.node.df["node_type"].isin(node_types)].copy()
    control_node_df["_match_name"] = normalize_name_series(control_node_df["name"])
    matched_node_df = control_node_df[control_node_df["_match_name"].isin(inlaat_names)]

    matched_node_ids = matched_node_df.index.astype(int).drop_duplicates().to_list()
    matched_names = set(matched_node_df["_match_name"].dropna())
    unmatched_names = sorted(inlaat_names - matched_names)

    print(f"LHM GEMAAL inlaten: {len(inlaat_names)} namen, {len(matched_node_ids)} Ribasim-nodes gevonden")
    if unmatched_names:
        print(f"LHM GEMAAL inlaten zonder match: {unmatched_names}")

    return matched_node_ids


model.update_node(node_id=n("WL03433-3___8"), node_type="Outlet")
model.update_node(node_id=n("ow:WL03437-9"), node_type="Outlet")
model.update_node(node_id=n("ow:WL04956-0"), node_type="Outlet")


# %%
#
# #Toevoegen aanvoergebieden
# Handmatige indeling control-, supply- en drain-nodes.

# fmt: off
flow_control_nodes = node_list(
    [n("SL00003"), n("ST02886"), n("ST00572"), n("ST04716"), n("ST01667"), n("ST04024"), n("ST02235"), n("ST01833"), n("ST03419")],
    [n("ST00591"), n("ST03266"), n("ST02355"), n("ST01745"), n("ST03588"), n("ST02972"), n("ST04267"), n("ST00218"), n("ST03261"), n("ST05005"), n("ST00220")],
    [n("ST02134"), n("ST00018"), n("ST01584"), n("ST04715"), n("ST00848"), n("ST01733"), n("ST03459"), n("ST03458")],
    [n("ST01855"), n("ST04025"), n("ST04517"), n("ST02860"), n("ST03269"), n("GM00154"), n("GM00258"), n("GM00192"), n("GM00192")],
    [n("DK31245"), n("SY00185"), n("DK25574"), n("DK05185"), n("DK25487"), n("DK07883"), n("SY00239"), n("DK24977")],
    [n("DK30499"), n("ow:WL07323"), n("DK18797"), n("GM00091"),n("ow:WL01909-9"),n("ow:WL02747-62"), n("ow:WL03325-2"), n("ow:WL03437-9"), n("ST03089")],
    [n("ST04687")],  # Stuw De Haandrik in de Vecht is geen inlaat en moet ook kunnen afvoeren (#675)
)

supply_nodes = node_list(
    [n("bnd:26"), n("SL00015"), n("DK30436_BO"), n("ST04531"), n("ST03657"), n("ST05103"), n("ST01777"), n("ST00287"), n("ST03127"), n("ST03390"), n("ST04266")],
    [n("ST02973"), n("ST02237"), n("ST04106"), n("ST03440"),n("ST04268"),n("ST04832"),n("ST03026"), n("ST03447"), n("ST03620")],
    [n("ST00610"), n("ST04516"), n("ST01942"), n("ST01549"), n("ST04265"), n("ST04446"), n("ST03211"), n("ST03544"), n("ST02055"), n("ST04838")],
    [n("ST00512"), n("ST00750"), n("ST00633"), n("ST00245"), n("GM00181"), n("GM00150"), n("GM00224"), n("GM00141"), n("GM00112"), n("GM00090"), n("GM00106"), n("GM00207")],
    [n("GM00245"), n("GM00178"), n("GM00195"), n("GM00101"), n("GM00137"), n("GM00146"), n("GM00248"), n("GM00021"), n("GM00020"), n("GM00110")],
    [n("GM00142"), n("GM00232"), n("GM00105"), n("DK31294"), n("DK31940"), n("DK05134"), n("DK28476"), n("DK22336"), n("DK19778"), n("DK22934")],
    [n("DK29483"), n("DK04254"), n("DK04254"), n("DK19428"), n("DK29373"), n("DK27337"), n("DK20616"), n("DK20616"), n("DK21040"), n("DK26410")],
    [n("DK23182"), n("DK21454"), n("DK09836"), n("DK06237"), n("DK25430"), n("DK02627"), n("DK08466"), n("DK27007")],
    [n("DK20584"), n("DK28737"), n("DK29136"), n("DK14459"), n("DK18391"), n("DK22903"), n("DK26377"), n("DK26997"), n("DK18813"), n("DK17872")],
    [n("DK16741"), n("DK20655"), n("DK17212"), n("DK20441"), n("DK24243"), n("DK23050"), n("DK20932"), n("DK17947"), n("DK28591"), n("DK28749"), n("DK28749")],
    [n("DK23474"), n("DK25568"), n("DK19591"), n("DK14315"), n("DK25620"), n("DK25620"), n("DK23361"), n("DK25085"), n("DK17636"), n("DK22967")],
    [n("DK21982"), n("DK21875"), n("DK22251"), n("DK20196_BO"), n("GM0001"), n("ow:WL03296-2"), n("ow:WL03182-7"), n("WL02311"), n("loc:263698,528136"), n("PT00526")],
    [n("loc:238984,496571"), n("aanvoergemaal-westerhaar"), n("loc:243225,495478"), n("loc:238366,496287"), n("loc:241322,486551")],
)

drain_nodes = node_list(
    [n("bnd:36"), n("SL00017"), n("SL00001"), n("SL00010"), n("SL00011"), n("SL00018"), n("SL00007"), n("SL00009"), n("SL00008"), n("ST03404")],
    [n("ST02970"), n("ST01050"), n("ST03725"), n("ST03806"), n("ST04951"), n("ST03059"), n("ST03347"), n("ST03258"), n("ST03456")],
    [n("ST03407"), n("ST03407"), n("ST01353"), n("ST03276"), n("ST03240"), n("ST00525"), n("ST04431"), n("ST02338"), n("ST03616"), n("ST03587")],
    [n("ST00132"), n("ST02898"), n("ST01456"), n("ST01587"), n("ST03834"), n("ST02905"), n("ST02160"), n("ST03862"), n("ST01185"), n("ST03584")],
    [n("ST04317"), n("ST03540"), n("ST01164"), n("ST03813"), n("ST03005"), n("ST03094"), n("ST03312"), n("ST03214"), n("ST01448"), n("ST00101")],
    [n("ST03006"), n("ST02496"), n("ST04908"), n("ST04684"), n("ST01228"), n("ST03485"), n("ST00203"), n("ST01763"), n("ST02431")],
    [n("ST03230"), n("ST00107"), n("ST03090"), n("ST00769"), n("ST00825"), n("ST00629"), n("ST03168"), n("ST01438"), n("ST01750"), n("ST02896")],
    [n("ST02625"), n("ST02057"), n("ST03431"), n("ST03381"), n("ST03381"), n("ST01589"), n("ST02918"), n("ST01689"), n("ST00849"), n("ST03377")],
    [n("ST03377"), n("ST00157"), n("ST02489"), n("ST04686"), n("ST03309"), n("ST03427"), n("ST04784"), n("ST03103"), n("ST00642"), n("ST02908")],
    [n("ST01903"), n("ST01487"), n("ST03592"), n("ST02532"), n("ST03249"), n("ST03713"), n("ST00911"), n("ST03634"), n("ST03383"), n("ST03408")],
    [n("ST00929"), n("ST01130"), n("ST03454"), n("ST03610"), n("ST01159"), n("ST04383"), n("ST00017"), n("ST01959"), n("ST04026"), n("ST00913")],
    [n("ST03222"), n("ST01333"), n("ST02626"), n("ST01629"), n("ST01756"), n("ST04918"), n("ST02089"), n("ST03815")],
    [n("ST02998"), n("ST00158"), n("ST00236"), n("ST03198"), n("ST00482"), n("ST02825"), n("ST03133"), n("ST03424")],
    [n("ST03983"), n("ST00647"), n("ST00307"), n("ST03111"), n("ST03265"), n("ST03077"), n("ST02546"), n("ST01922"), n("ST00104"), n("ST00110")],
    [n("ST00916"), n("ST01136"), n("ST00640"), n("GM00124"), n("GM00078"), n("GM00115"), n("GM00159"), n("GM00201"), n("GM00004"), n("GM00239")],
    [n("GM00170"), n("GM00007"), n("GM00200"), n("GM00211"), n("GM00064"), n("GM00017"), n("GM00223"), n("GM00214"), n("GM00199"), n("GM00010")],
    [n("GM00132"), n("GM00122"), n("GM00218"), n("GM00222"), n("GM00003"), n("GM00135"), n("GM00027"), n("GM00093")],
    [n("GM00125"), n("GM00161"), n("GM00118"), n("GM00119"), n("GM00226"), n("GM00088"), n("GM00048"), n("DK07297"), n("DK00646"), n("DK31884")],
    [n("DK22755"), n("DK06419"), n("DK22579"), n("DK23689"), n("DK31927"), n("DK31684"), n("DK16430"), n("DK23636"), n("DK25521"), n("DK07223")],
    [n("DK30074"), n("DK10200"), n("DK21919"), n("DK07824"), n("DK22829"), n("DK31782"), n("DK00370"), n("DK17966"), n("DK01810")],
    [n("DK22173"), n("DK07983"), n("DK05579"), n("DK11790"), n("DK02955"), n("DK27338"), n("DK24332"), n("DK29749"), n("SY00014")],
    [n("SY00014"), n("DK29615"), n("DK10665"), n("DK25247"), n("SY00192"), n("DK23608"), n("DK19746"), n("DK00711"), n("DK20369")],
    [n("DK26830"), n("DK11459"), n("DK31774"), n("DK31774"), n("DK17294"), n("DK13751"), n("DK04728"), n("DK22177"), n("DK13908")],
    [n("DK05864"), n("DK00105"), n("DK25523"), n("DK18673"), n("DK31895"), n("DK30137"), n("DK25874"), n("DK24281"), n("DK03875"), n("DK16164"), n("DK17994")],
    [n("DK13662"), n("DK04436"), n("DK05259"), n("DK19529"), n("DK25792"), n("DK13021"), n("DK13664"), n("DK13791"), n("DK23365")],
    [n("DK26151"), n("DK26292"), n("DK19104"), n("DK04936"), n("DK16577"), n("DK16977"), n("DK29066"), n("DK14235"), n("DK00943"), n("DK25414")],
    [n("SY00239"), n("DK30704"), n("DK09062"), n("DK17115"), n("DK31538"), n("DK25702"), n("DK29983"), n("DK17914"), n("DK03588"), n("DK17873"), n("DK22745"),],
    [n("ST04473"), n("GM00109"), n("ST01870"), n("ST04691"), n("ow:WL00448-2"),n("ow:WL02036-3"), n("ow:WL05791-16@1"), n("ow:WL04956-0"), n("ow:WL03437-9"), n("WL02926-0___1"), n("WL03433-3___8"), n("WL03433-3___8")],
    [n("loc:268243,491536"), n("ST03624"), n("ST00811"), n("loc:238834,496508"), n("loc:241738,513291")],
)
# fmt: on


manual_flow_control_nodes = list(flow_control_nodes)
manual_supply_nodes = list(supply_nodes)
manual_drain_nodes = list(drain_nodes)

lhm_gemaal_supply_nodes = get_lhm_gemaal_supply_nodes(model=model, lhm_gemaal_gdb=lhm_gemaal_gdb)

if IS_SUPPLY_NODE_COLUMN not in model.node.df.columns:
    model.node.df[IS_SUPPLY_NODE_COLUMN] = False
model.node.df.loc[lhm_gemaal_supply_nodes, IS_SUPPLY_NODE_COLUMN] = True

print_supply_role_conflicts(
    label="LHM GEMAAL inlaten",
    source_supply_nodes=lhm_gemaal_supply_nodes,
    manual_drain_nodes=manual_drain_nodes,
    manual_flow_control_nodes=manual_flow_control_nodes,
)

supply_nodes, drain_nodes, flow_control_nodes = combine_control_node_roles(
    kunstwerk_supply_nodes=lhm_gemaal_supply_nodes,
    kunstwerk_drain_nodes=[],
    manual_supply_nodes=manual_supply_nodes,
    manual_drain_nodes=manual_drain_nodes,
    manual_flow_control_nodes=manual_flow_control_nodes,
)

print_node_list_diff("supply_nodes", manual_supply_nodes, supply_nodes)


# Per gebied: links die intersecten die we kunnen negeren.

# fmt: off
supply_area_ignore_links = {
    "Boven Regge": [lk("ow:WL01803-3", "up:ow:WL01863-18"), lk("ow:WL01863-10", "up:ST04918"), lk("ow:WL01823-5", "up:ST03711"), lk("ow:WL01891-4@1", "up:ow:WL01891-4@2"), lk("ow:WL01698-3_2", "up:WL01698-3_4_inlaat"), lk("ow:WL01863-9", "up:WL01698-3_4_inlaat"), lk("up:ow:WL01863-18", "ow:WL01863-18")],
    "Regge": [
        lk("DK01810", "up:ST03541"), lk("DK04546", "up:ST02237"), lk("DK01390", "up:ST02237"), lk("DK13262", "up:ST02237"), lk("DK06236", "up:ST00803"), lk("DK11840", "up:ow:WL01181-5"), lk("ow:WL01181-0", "up:ow:WL01181-5"), lk("ow:WL01202-2", "up:ow:WL01181-5"),
        lk("ow:WL01374-2", "up:ST00610"), lk("DK01495", "up:ST02860"), lk("ow:WL01917", "up:ow:WL01909-9"), lk("ow:WL02102-15", "up:WL02311"), lk("ow:WL07399", "up:GM00224"), lk("ow:WL02036-3", "up:GM00224"),
        lk("ow:WL01959-14", "up:GM00224"), lk("ow:WL01993-1", "up:ow:WL01959-14"), lk("ow:WL01803-3", "up:ow:WL01863-18"), lk("ow:WL01863-10", "up:ST04918"), lk("WL07174-3___7", "up:ST00296"), lk("ow:WL01052-11", "up:ow:WL01181-5"), lk("ow:WL01052-8", "up:ow:WL01052-11"), lk("ow:WL02056-18", "up:ow:WL01959-75"),
        lk("up:ST00106", "ow:WL01998-5"), lk("up:DK29791", "DK29791"), lk("up:WL02311", "WL02311"), lk("up:ow:WL01863-18", "ow:WL01863-18"), lk("up:ow:WL07399", "ow:WL07399"), lk("up:ow:WL01959-14", "ow:WL01959-14"), lk("up:WL01698-3_4_inlaat", "ow:WL01863-10"),
    ],
    "Lateraalkanaal": [lk("DK01390", "up:ST02237"), lk("ow:WL01256-3", "up:ST00848"), lk("ow:WL01265-14", "up:ST00848"), lk("DK11840", "up:ow:WL01181-5"), lk("ow:WL01181-0", "up:ow:WL01181-5"), lk("ow:WL01202-2", "up:ow:WL01181-5"), lk("DK28416", "up:ST00610"), lk("ow:WL01480-4", "up:ST01228"), lk("ow:WL01711-4", "up:DK11814"), lk("ow:WL05851-16", "up:DK01629"), lk("ow:WL01604-5", "up:DK06237"), lk("WL07174-3___7", "up:ST00296"), lk("ow:WL01052-11", "up:ow:WL01181-5"), lk("ow:WL01391-35", "up:ST01683"), lk("up:ST02237", "VW035_95"), lk("up:ST00610", "VW037_135"), lk("up:ow:WL01711-4", "ow:WL01711-4")],
    "Bolscherbeek": [],
    "Vriezenveen": [lk("ow:WL01480-4", "up:ST01228"), lk("DK00105", "up:ST00220"), lk("DK29066", "up:ST00220"), lk("ow:WL01711-4", "up:DK11814"), lk("up:DK04366", "DK01810"), lk("up:ow:WL01711-4", "ow:WL01711-4")],
    "Dooze": [lk("WL05569___3", "up:ST03194"), lk("DK21305", "up:DK17873"), lk("ow:WL02973-11", "up:ST03541"), lk("up:ST02994", "WL05569___3"), lk("up:ow:WL03046-5", "ow:WL03046-5")],
    "Schipbeek": [lk("ow:WL00847-2", "up:DK05134")],
    "Dinkel": [],
    "Overijsselsch Kanaal noord": [lk("WL05569___3", "up:ST03194"), lk("bnd:6", "up:DK28701")],
    "Vecht": [lk("ow:WL04956-0", "up:ow:WL02747-62"), lk("ow:WL04844", "up:SL00017"), lk("ow:WL02815", "up:SL00017"), lk("DK23689", "up:SL00018"), lk("DK25521", "up:SL00013"), lk("ow:WL02926-4", "up:SL00013"), lk("ow:WL02747-10", "up:SL00013")],
    "Coevorden-Zwinderen": [lk("DK24281", "up:DK20616"), lk("up:DK21646", "ow:WL03182-7"), lk("up:DK25247", "DK25247"), lk("up:DK27007", "WL03433-3___8"), lk("up:DK22015", "WL02926-0___1")],
    "Geesbrug": [lk("ow:WL03158-2", "up:DK20656")],
    "Braambergersloot": [lk("WL03433-3___8", "up:DK21438"), lk("DK23608", "up:ST03222"), lk("up:DK21646", "ow:WL03182-7"), lk("ow:WL03158-2", "up:DK20656")],
    "Nieuwe Drostendiep": [lk("WL03433-3___8", "up:DK21438"), lk("up:ST03198", "ow:WL03296-2"), lk("up:ST03459", "DK17636"),lk("ow:WL03676-1", "up:DK20656"), lk("DK17636", "up:DK17636"), lk("up:DK18155", "DK18155"), lk("up:DK20656", "ow:WL03161"), lk("ow:WL03158-2", "up:DK20656")],
    "Schoonebeek": [],
    "Oranjekanaal": [lk("DK25487", "up:ow:WL03457-8"), lk("up:SL00004", "ow:WL03440-3"), lk("up:DK20656", "ow:WL03161")],
    "Oosterwijk": [lk("up:ST03211", "ow:WL03775-1")],
    "Oosterhesselen": [lk("ow:WL03676-1", "up:DK20656"), lk("ow:WL03158-2", "up:DK20656")],
}
# fmt: on

for area_name, ignore_intersecting_links in supply_area_ignore_links.items():
    print(f"Toevoegen {area_name}")
    add_supply_area_control(
        model=model,
        aanvoergebieden_df=aanvoergebieden_df,
        area_name=area_name,
        ignore_intersecting_links=ignore_intersecting_links,
        supply_nodes=supply_nodes,
        drain_nodes=drain_nodes,
        flow_control_nodes=flow_control_nodes,
    )


# %%
# Add all remaining inlets/outlets

add_controllers_to_uncontrolled_connector_nodes(
    model=model,
    supply_nodes=supply_nodes,
    flow_control_nodes=flow_control_nodes,
    drain_nodes=drain_nodes,
    flushing_nodes={},
    exclude_nodes=list(EXCLUDE_NODES),
    flow_rate_aanvoer=20.0,
    max_flow_rate_aanvoer=max_flow_rate_aanvoer_by_node_id,
    flow_rate_afvoer=100.0,
    max_flow_rate_afvoer=outlet_max_flow_rate_afvoer_by_node_id,
)

# %%
# Inlaat 26 Noordseschutsluis stopt 1 cm onder het benedenstroomse streefpeil.

lower_outlet_max_downstream_level(model=model, node_id=n("bnd:26"), offset=0.01)


# %%
# EXCLUDE_NODES op 0 m3/s zetten

mask = model.outlet.static.df.node_id.isin(EXCLUDE_NODES)
model.outlet.static.df.loc[mask, ["flow_rate", "min_flow_rate", "max_flow_rate"]] = 0.0

# %%
# Junctionify(!)

model = junctionify(model)


# %%
# Laatste handmatige correcties

# Gemaal Orveltersluis
assert model.pump.static.df is not None
mask = model.pump.static.df.node_id == n("GM00232")
model.pump.static.df.loc[mask, "min_upstream_level"] = 14.86
mark_level_update_protected(model.pump.static.df, mask, model=model)

aanvoer_only_node_ids = set(supply_nodes) - set(drain_nodes) - set(flow_control_nodes)

# Aanvoer-cap: doorlaten/inlaten mogen in aanvoer niet de hoge afvoercapaciteit gebruiken.
assert model.outlet.static.df is not None
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

zero_pump_node_ids = {node_id for node_id, max_flow_rate in pump_max_flow_rate_by_node_id.items() if max_flow_rate == 0}
zero_pump_mask = model.pump.static.df.node_id.isin(zero_pump_node_ids)
model.pump.static.df.loc[zero_pump_mask, ["flow_rate", "max_flow_rate"]] = 0.0

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
protected_max_flow_rate_node_ids = set(EXCLUDE_NODES) | flow_demand_controlled_node_ids | manual_max_flow_rate_node_ids
for static_df in (model.outlet.static.df, model.pump.static.df):
    assert static_df is not None
    afvoer_mask = (
        static_df["control_state"].eq("afvoer")
        & static_df["flow_rate"].fillna(0).gt(0)
        & ~static_df["node_id"].isin(protected_max_flow_rate_node_ids)
    )
    static_df.loc[afvoer_mask, "max_flow_rate"] = static_df.loc[afvoer_mask, "max_flow_rate"].fillna(10).clip(lower=10)

for static_df in (model.outlet.static.df, model.pump.static.df):
    assert static_df is not None
    afvoer_mask = static_df["control_state"].eq("afvoer") & static_df["node_id"].isin(aanvoer_only_node_ids)
    static_df.loc[afvoer_mask, ["flow_rate", "max_flow_rate"]] = 0.0

# %%
# Model run


# write the dry and wet steady state scenarios and the full control model
model = write_steady_state_scenarios(
    model,
    AUTHORITY,
    SHORT_NAME,
    dry_evaporation_mm_per_day=0.5,
    fill_missing_level_boundaries=True,
    model_exec=MODEL_EXEC,
    qlr_path=qlr_path,
)

# %%
