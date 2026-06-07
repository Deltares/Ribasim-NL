# %%
from pathlib import Path

import geopandas as gpd
from peilbeheerst_model.controle_output import Control
from ribasim import Node
from ribasim.nodes import level_boundary
from ribasim_nl.control import (
    add_controllers_to_supply_area,
    add_controllers_to_uncontrolled_connector_nodes,
    mark_level_update_protected,
)
from ribasim_nl.junctions import junctionify
from ribasim_nl.parametrization.basin_tables import update_basin_static

from ribasim_nl import CloudStorage, Model

# execute model run
MODEL_EXEC: bool = True

# model settings
AUTHORITY: str = "Vechtstromen"
SHORT_NAME: str = "vechtstromen"
CONTROL_NODE_TYPES = ["Outlet", "Pump"]
IS_SUPPLY_NODE_COLUMN: str = "meta_supply_node"

# Sluizen die geen rol hebben in de waterverdeling (aanvoer/afvoer), maar wel in het model zitten
EXCLUDE_NODES = {38, 40, 996}
LEVEL_UPDATE_PROTECTION_COLUMN = "meta_level_update_protected"


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


def clean_database_sidecars(input_dir: Path) -> None:
    for suffix in ["-wal", "-shm"]:
        try:
            input_dir.joinpath(f"database.gpkg{suffix}").unlink(missing_ok=True)
        except PermissionError as error:
            raise PermissionError(
                f"Kan database sidecars niet opschonen in {input_dir}. "
                "Sluit QGIS of andere programma's die de database gebruiken."
            ) from error


def run_model_and_control(model: Model, ribasim_toml, qlr_path):
    ribasim_toml = Path(ribasim_toml)
    input_dir = ribasim_toml.parent / model.input_dir

    clean_database_sidecars(input_dir)
    fill_missing_level_boundary_static_levels(model)
    model.write(ribasim_toml)
    clean_database_sidecars(input_dir)

    if MODEL_EXEC:
        model.run()
        Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path).run_all()
        return Model.read(ribasim_toml)

    return model


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


def fill_missing_level_boundary_static_levels(model: Model, default_level: float = 0.0) -> None:
    missing_level_mask = model.level_boundary.static.df["level"].isna()
    model.level_boundary.static.df.loc[missing_level_mask, "level"] = default_level


def lower_outlet_max_downstream_level(model: Model, node_id: int, offset: float = 0.01) -> None:
    downstream_basin_ids = model.link.df.loc[
        (model.link.df.from_node_id == node_id) & model.link.df.to_node_id.isin(model.basin.node.df.index),
        "to_node_id",
    ].unique()
    if len(downstream_basin_ids) != 1:
        raise ValueError(f"Inlaat {node_id}: verwacht 1 benedenstrooms basin, gevonden {downstream_basin_ids}")

    downstream_basin_id = int(downstream_basin_ids[0])
    downstream_target_level = model.basin.area.df.loc[
        model.basin.area.df.node_id == downstream_basin_id,
        "meta_streefpeil",
    ].dropna()
    if downstream_target_level.empty:
        raise ValueError(f"Inlaat {node_id}: geen streefpeil gevonden voor benedenstrooms basin {downstream_basin_id}")

    mask = model.outlet.static.df.node_id == node_id
    if "control_state" in model.outlet.static.df.columns:
        aanvoer_mask = mask & (model.outlet.static.df.control_state == "aanvoer")
        if aanvoer_mask.any():
            mask = aanvoer_mask

    if LEVEL_UPDATE_PROTECTION_COLUMN not in model.outlet.static.df.columns:
        model.outlet.static.df[LEVEL_UPDATE_PROTECTION_COLUMN] = False
    model.outlet.static.df.loc[mask, "max_downstream_level"] = float(downstream_target_level.iloc[0]) - offset
    model.outlet.static.df.loc[mask, LEVEL_UPDATE_PROTECTION_COLUMN] = True


def duplicate_level_boundary_for_link(model: Model, source_node_id: int, link_id: int) -> int | None:
    if link_id not in model.link.df.index:
        return None

    current_from_node_id = int(model.link.df.at[link_id, "from_node_id"])
    current_to_node_id = int(model.link.df.at[link_id, "to_node_id"])
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
    boundary_node = model.level_boundary.add(
        Node(geometry=source_node.geometry, name=new_name),
        tables=[level_boundary.Static(level=[0.0])],
    )

    new_node_id = boundary_node.node_id

    node_columns = [column for column in model.node.df.columns if column not in {"node_type", "name", "geometry"}]
    model.node.df.loc[new_node_id, node_columns] = model.node.df.loc[source_node_id, node_columns]

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

cloud.synchronize(filepaths=[aanvoergebieden_gpkg, qlr_path])
cloud.synchronize(filepaths=[lhm_gemaal_gdb], overwrite=not lhm_gemaal_gdb.is_dir())


# %%
# Read data

model = Model.read(ribasim_toml)
aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")

# Splits de bestaande boundary bij node 15 zodat link 2822 een eigen boundary node krijgt.
duplicate_level_boundary_for_link(model=model, source_node_id=15, link_id=2822)

reverse_existing_links(model, [2682, 2823, 2822, 1337, 27, 1891])

# Gemaal Orveltersluis was opggedeeld
model.remove_node(node_id=665, remove_links=True)
model.remove_node(node_id=969, remove_links=True)

model.redirect_link(link_id=53, to_node_id=1878)


# %%
# Topologie fixes


# %%
# Max-capaciteiten inlaten
# Uit waterakkoord

outlet_max_flow_rate_by_node_id = {
    553: 1.75,  # punt 902 - Vroomshoop
    2337: 0.63,  # punt 56 - Banisschuiven
    193: 0.8,  # punt 62 - Geerdijk (Hammerflier)
    989: 1.5,  # punt 68 - Radewijkerbeek
    260: 0.35,  # punt 87 - Twickelervaart Delden
    498: 0.5,  # punt 106 - Twickelervaart
    640: 0.1,  # punt 52 - Bolscherbeek
    2344: 0.28,  # punt 60 - Vriezenveense Veenkanaal
    1026: 0.16,  # punt 64 - Brucht
    1016: 0.18,  # punt 65 - Zilverveen
    695: 0.03,  # punt 66 - Emtenbroek
    1011: 0.02,  # punt 67 - Eggenweg/Hoogeweg
    986: 1.0,  # punt 77 - De Stouwe
    997: 0.3,  # punt 90 - Boerendijk of Lijntje (Dooze)
    1027: 0.06,  # punt 109 - Roskam
    1009: 0.06,  # punt 911 - Steenmaat
    1010: 0.02,  # punt 912 - Koenderink
    44: 8,  # Sluis Aadorp
    326: 6.21,  # aflaat punt 1 - Usselerstroom
    386: 0.81,  # aflaat punt 2 - Schoolbeek
    493: 4.76,  # aflaat punt 7 - Nieuwe Oelerbeek
    256: 12.87,  # aflaat punt 9/15 - Hagmolenbeek / Oude Hagmolenbeek
    458: 0.22,  # aflaat punt 10 - waterleiding vd Exterkottenlanden
    183: 2.54,  # aflaat punt 16 - Wienerveldsleiding
    214: 0.06,  # aflaat punt 17 - Bentelerbeek
    190: 48.0,  # aflaat punt 19/30 - Bolscherbeek / Overstort Bolksbeek
    411: 2.61,  # aflaat punt 23 - Boven-Regge/Diepenh. Molenbeek
    394: 8.98,  # aflaat punt 21 - Poelsbeek (doorlaat)
    2334: 7.84,  # aflaat punt 31 - Grote Waterleiding (doorlaat)
}
outlet_max_flow_rate_parameterized_zero_by_node_id = {
    1269: 100.0,  # xlsx max_flow_rate nul; voorkom afvoerblokkade
    2324: 100.0,  # xlsx max_flow_rate nul; voorkom afvoerblokkade
}
outlet_max_flow_rate_from_results = {
    26: 2,  # naam onbekend
    41: 2,  # SL00010
    75: 2,  # Inlaat Kleine Rietplas
    90: 11,  # Stuw Rompelman Linderbeek
    128: 2,  # Inlaat Nijboer
    207: 2,  # Inlaatschuif Witten Noord
    211: 11,  # Overlaat Assenhoek
    236: 2,  # Inlaatschuif Omvloed
    237: 2,  # Drempel bypass Vispassage Melodiestraat
    254: 5,  # Overlaat Groot Boerman
    271: 6,  # Stuw RW 34
    310: 11,  # Stuw Linderdijk
    422: 2,  # Overlaat Esbeek-Vinkebeek
    425: 2,  # Stuw Melodiestraat
    435: 2,  # Stuw Zuivering Enschede West
    450: 11,  # Stuw Het Bergje
    472: 2,  # Overlaat Broeierdweg
    476: 12,  # Stuw Pelmolen
    478: 8,  # Stuw Dalerveense straat
    500: 6,  # Stuw Galgaten
    506: 2,  # Inlaatschuif Havenstraat
    507: 30,  # Stuw Bremmer 1 bij Russendijk
    519: 5,  # Overlaat Energiefabriek Hengelo
    548: 3,  # Stuw Heemafstraat
    676: 2,  # DK31294
    686: 2,  # naam onbekend
    700: 2,  # naam onbekend
    707: 15,  # naam onbekend
    717: 2,  # naam onbekend
    718: 2,  # naam onbekend
    721: 2,  # naam onbekend
    722: 2,  # naam onbekend
    724: 2,  # naam onbekend
    731: 2,  # naam onbekend
    739: 2,  # naam onbekend
    740: 2,  # naam onbekend
    745: 2,  # naam onbekend
    746: 2,  # naam onbekend
    747: 3,  # naam onbekend
    754: 2,  # naam onbekend
    758: 2,  # naam onbekend
    759: 2,  # naam onbekend
    763: 2,  # naam onbekend
    768: 3,  # naam onbekend
    770: 2,  # naam onbekend
    774: 2,  # naam onbekend
    775: 3,  # naam onbekend
    779: 2,  # naam onbekend
    782: 2,  # naam onbekend
    788: 2,  # naam onbekend
    798: 2,  # naam onbekend
    800: 2,  # naam onbekend
    803: 2,  # naam onbekend
    806: 2,  # naam onbekend
    808: 2,  # naam onbekend
    810: 2,  # naam onbekend
    811: 2,  # naam onbekend
    814: 2,  # naam onbekend
    815: 2,  # naam onbekend
    819: 2,  # naam onbekend
    824: 2,  # naam onbekend
    825: 2,  # naam onbekend
    826: 2,  # naam onbekend
    827: 2,  # naam onbekend
    832: 2,  # naam onbekend
    843: 2,  # naam onbekend
    848: 2,  # naam onbekend
    851: 2,  # naam onbekend
    861: 2,  # naam onbekend
    865: 2,  # naam onbekend
    874: 2,  # naam onbekend
    878: 2,  # naam onbekend
    882: 2,  # naam onbekend
    894: 2,  # naam onbekend
    895: 2,  # naam onbekend
    900: 2,  # naam onbekend
    905: 2,  # naam onbekend
    910: 2,  # naam onbekend
    912: 2,  # naam onbekend
    917: 2,  # naam onbekend
    919: 2,  # naam onbekend
    922: 2,  # naam onbekend
    924: 2,  # naam onbekend
    935: 2,  # naam onbekend
    936: 2,  # naam onbekend
    937: 3,  # naam onbekend
    938: 2,  # naam onbekend
    941: 2,  # naam onbekend
    953: 5,  # naam onbekend
    963: 11,  # naam onbekend
    965: 2,  # naam onbekend
    974: 2,  # naam onbekend
    987: 29,  # naam onbekend
    992: 2,  # naam onbekend
    1004: 2,  # naam onbekend
    1008: 3,  # naam onbekend
    1019: 2,  # naam onbekend
    1020: 3,  # naam onbekend
    1029: 5,  # naam onbekend
    1034: 5,  # naam onbekend
    1040: 3,  # DK21233
    1053: 2,  # naam onbekend
    1066: 6,  # naam onbekend
    1082: 2,  # naam onbekend
    1137: 27,  # naam onbekend
    1140: 3,  # naam onbekend
    1153: 5,  # naam onbekend
    1156: 2,  # naam onbekend
    1167: 2,  # naam onbekend
    1168: 2,  # naam onbekend
    1169: 2,  # naam onbekend
    1177: 2,  # naam onbekend
    1237: 2,  # naam onbekend
    1238: 2,  # naam onbekend
    1304: 2,  # naam onbekend
    1329: 2,  # naam onbekend
    1336: 2,  # naam onbekend
    1337: 2,  # naam onbekend
    1340: 3,  # naam onbekend
    1353: 81,  # naam onbekend
    1368: 2,  # naam onbekend
    1369: 2,  # naam onbekend
    2342: 3,  # Uitlaat Westerhaar
}
outlet_max_flow_rate_coupled_by_node_id = {
    98: 6,  # Debietbegrenzer Duitslandweg; gekoppeld max=3.36, parameterized=0.91
    211: 32,  # Overlaat Assenhoek; gekoppeld max=20.82, parameterized=3.27
    279: 30,  # gekoppeld max=19.71, huidige max=0.00, link=4400599
    323: 30,  # gekoppeld max=19.79, huidige max=4.05, link=4400650
    310: 20,  # Stuw Linderdijk; gekoppeld max=12.70, parameterized=3.58
    477: 12,  # gekoppeld max=7.28, huidige max=0.00, link=4400871
    478: 11,  # Stuw Dalerveense straat; gekoppeld max=6.21, parameterized=2.01
    500: 9,  # Stuw Galgaten; gekoppeld max=5.90, parameterized=1.51
    554: 3,  # Inlaat Daarleschebeek; gekoppeld max=1.50, parameterized=0.40
    734: 6,  # gekoppeld max=3.78, huidige max=0.00, link=4401061
    839: 2,  # parameterized nul; gekoppeld max=0.15
    963: 32,  # gekoppeld max=20.82, parameterized=0.06
    992: 5,  # gekoppeld max=2.12, parameterized=0.35
    1006: 12,  # gekoppeld max=7.11, huidige max=0.00, link=4400826
    1018: 11,  # gekoppeld max=6.98, huidige max=0.00, link=4401176
    1008: 147,  # gekoppeld max=97.96, parameterized=0.23
    1080: 30,  # gekoppeld max=20.00, huidige max=0.00, link=8000941
    1084: 3,  # gekoppeld max=1.81, parameterized=0.40
    2318: 2,  # gekoppeld max=0.58, huidige max=0.00, link=8000976
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
outlet_max_flow_rate_aanvoer_by_node_id = dict.fromkeys(model.outlet.static.df.node_id.astype(int), 10.0)

# Handmatige inlaatcapaciteiten gelden ook in aanvoer; niet terugvallen op de default van 10 m3/s.
outlet_max_flow_rate_aanvoer_by_node_id.update(outlet_max_flow_rate_by_node_id)

pump_max_flow_rate_by_node_id = {
    629: 0.42,  # punt 53 - Stokkumerflier
    626: 1.2,  # punt 904 - Stieltjeskanaalsluis
    560: 0.14,  # punt 54 - Herikerflier
    625: 0.2,  # punt 74 - Junne (vecht)
    615: 0.15,  # punt 104 - Oude Poelsebeek
    596: 0.0,  # punt 107 - Hedeman
    2341: 0.88,  # punt 905 - Westerhaar
    701: 0.65,  # punt 103 - Oude Hagmolenbeek
    587: 10.0,  # aflaat punt 13 - Banisgemaal
    674: 0.33,  # aflaat punt 24 - Leidebeek
    662: 0.92,  # aflaat punt 35 - Afwatering vd Bierkamp
    642: 0.56,  # aflaat punt 22 - Stokkumerflier (doorlaat)
    607: 5.8,  # Ericasluis
    664: 1.2,  # Orveltersluis
    606: 1.2,  # Oranjesluis
}
pump_max_flow_rate_from_results = {
    564: 20,  # gekoppeld max=19.80, huidige max=0.00, link=4400277
}
max_flow_rate_aanvoer_by_node_id = {
    **outlet_max_flow_rate_aanvoer_by_node_id,
    **pump_max_flow_rate_by_node_id,
}


set_max_flow_rate(model.outlet.static.df, outlet_max_flow_rate_by_node_id)
set_max_flow_rate(model.pump.static.df, pump_max_flow_rate_by_node_id)
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


def print_node_list_diff(label: str, before_nodes: list[int], after_nodes: list[int]) -> None:
    before_nodes = set(before_nodes)
    after_nodes = set(after_nodes)

    added_nodes = sorted(after_nodes - before_nodes)
    removed_nodes = sorted(before_nodes - after_nodes)

    print(f"{label}: {len(added_nodes)} toegevoegd, {len(removed_nodes)} verwijderd t.o.v. handmatig")
    print(f"{label} toegevoegd: {added_nodes}")
    print(f"{label} verwijderd: {removed_nodes}")


def print_supply_role_conflicts(
    label: str,
    source_supply_nodes: list[int],
    manual_drain_nodes: list[int],
    manual_flow_control_nodes: list[int],
) -> None:
    source_supply_nodes = set(source_supply_nodes)
    manual_drain_nodes = set(manual_drain_nodes)
    manual_flow_control_nodes = set(manual_flow_control_nodes)

    print(
        f"{label} overruled door handmatig: "
        f"flow_control={sorted(source_supply_nodes & manual_flow_control_nodes)}, "
        f"drain={sorted(source_supply_nodes & manual_drain_nodes)}"
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


model.update_node(node_id=1367, node_type="Outlet")
model.update_node(node_id=1316, node_type="Outlet")
model.update_node(node_id=1295, node_type="Outlet")


# %%
#
# #Toevoegen aanvoergebieden
# Handmatige indeling control-, supply- en drain-nodes.

# fmt: off
flow_control_nodes = node_list(
    [42, 81, 89, 135, 156, 159, 198, 201, 231],
    [260, 271, 282, 286, 312, 317, 331, 337, 349, 359, 361, 364],
    [365, 373, 405, 428, 446, 464, 478, 500],
    [509, 513, 518, 522, 539, 632, 635, 672, 672],
    [700, 765, 840, 853, 908, 963, 971, 998],
    [1004, 1041, 1074, 1083, 1088,1289,1290, 1314, 1316, 2334],
)

supply_nodes = node_list(
    [26, 44, 53, 75, 102, 155, 193, 199, 200, 221, 250],
    [253, 297, 325, 362,363,389,399, 403, 447],
    [459, 465, 467, 468, 469, 477, 481, 492, 498, 501],
    [508, 547, 553, 554, 560, 564, 571, 580, 584, 589, 595, 596],
    [606, 611, 615, 621, 623, 625, 626, 629, 640, 648],
    [658, 664, 670, 676, 677, 680, 684, 690, 694, 695],
    [701, 709, 709, 720, 726, 730, 734, 734, 736, 760],
    [762, 846, 914, 961, 964, 973, 976, 978],
    [979, 983, 985, 986, 988, 989, 990, 991, 993, 997],
    [1000, 1001, 1003, 1006, 1007, 1009, 1010, 1011, 1012, 1013, 1013],
    [1016, 1017, 1018, 1020, 1025, 1025, 1026, 1027, 1032, 1037],
    [1057, 1065, 1069, 1080, 1087, 1301, 1307, 2318, 2337, 2339],
    [2340, 2341, 2343, 2344, 2346],
)

drain_nodes = node_list(
    [36, 39, 40, 41, 43, 45, 46, 47, 52, 79],
    [87, 88, 90, 91, 92, 94, 98, 99, 101, 104],
    [106, 106, 112, 113, 118, 124, 134, 136, 142, 144],
    [150, 154, 157, 160, 161, 162, 163, 169, 171, 174],
    [181, 185, 191, 195, 203, 208, 209, 210, 211, 212],
    [215, 222, 224, 225, 226, 228, 233, 240, 241],
    [243, 247, 249, 257, 258, 266, 273, 276, 281, 283],
    [284, 296, 302, 303, 303, 304, 306, 309, 333, 334],
    [334, 339, 342, 348, 350, 353, 354, 370, 376, 379],
    [384, 388, 398, 400, 401, 402, 408, 409, 412, 416],
    [423, 424, 429, 432, 433, 440, 443, 444, 445, 449],
    [450, 451, 452, 453, 455, 473, 475, 487, 490],
    [491, 494, 504, 505, 510, 511, 512, 514, 517],
    [519, 529, 530, 531, 533, 540, 548, 549, 550, 552],
    [556, 557, 558, 559, 565, 568, 577, 578, 582, 583],
    [586, 592, 593, 594, 597, 598, 600, 604, 605, 610],
    [614, 617, 618, 620, 628, 637, 642, 643, 645, 649],
    [650, 653, 661, 662, 666, 667, 674, 675, 686, 703],
    [704, 707, 712, 713, 714, 715, 722, 723, 724, 729],
    [733, 738, 740, 741, 743, 761, 763, 764, 768],
    [772, 773, 776, 780, 781, 783, 790, 791, 792],
    [792, 795, 798, 801, 805, 806, 807, 817, 820, 822],
    [829, 831, 845, 845, 850, 852, 855, 856, 860],
    [864, 869, 873, 876, 878, 880, 881, 882, 890, 891, 892],
    [895, 906, 907, 909, 913, 919, 921, 928, 930, 932],
    [933, 935, 947, 949, 950, 952, 957, 958, 960, 965, 966, 968],
    [971, 975, 981, 1002, 1005, 1008, 1050, 1051, 1064, 1066, 1068,],
    [1071, 1089, 1095, 1102, 1132,1245, 1269, 1295, 1316, 1353, 1367, 1367],
    [2324, 2335, 2336, 2338, 2342, 2345, 2918],
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
    "Boven Regge": [868, 1072, 1257, 1258, 1259, 1260, 2508, 2640],
    "Regge": [
        27, 91, 92, 93, 392, 403, 404, 405,
        429, 472, 473, 474, 512, 612, 622, 623,
        624, 800, 868, 1072, 1156, 1243, 1249, 1250,
        1938, 2193, 2499, 2508, 2580, 2582, 2594, 2641,
    ],
    "Lateraalkanaal": [92, 269, 270, 403, 404, 405, 428, 436, 992, 1058, 1113, 1156, 1243, 1252, 1394, 1396, 2479],
    "Bolscherbeek": [],
    "Vriezenveen": [436, 487, 489, 491, 492, 992, 1291, 1891, 2479],
    "Dooze": [133, 918, 1236, 1783, 2411, 2629],
    "Schipbeek": [541],
    "Dinkel": [],
    "Overijsselsch Kanaal noord": [133, 1172, 2671],
    "Vecht": [17, 41, 42, 258, 1044, 1046, 1047],
    "Coevorden-Zwinderen": [1136, 2109, 2234, 2398, 2425, 2436],
    "Geesbrug": [2777],
    "Braambergersloot": [71, 659, 2109, 2777],
    "Nieuwe Drostendiep": [71, 255, 1183,1253, 1993, 2362, 2404, 2445, 2604, 2623,2777],
    "Schoonebeek": [],
    "Oranjekanaal": [598, 2414, 2604],
    "Oosterwijk": [2633],
    "Oosterhesselen": [1253, 2777],
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

lower_outlet_max_downstream_level(model=model, node_id=26, offset=0.01)


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
mask = model.pump.static.df.node_id == 664
model.pump.static.df.loc[mask, "min_upstream_level"] = 14.86
mark_level_update_protected(model.pump.static.df, mask)

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
    afvoer_mask = (
        static_df["control_state"].eq("afvoer")
        & static_df["flow_rate"].fillna(0).gt(0)
        & ~static_df["node_id"].isin(protected_max_flow_rate_node_ids)
    )
    static_df.loc[afvoer_mask, "max_flow_rate"] = (
        static_df.loc[afvoer_mask, "max_flow_rate"].fillna(0.5).clip(lower=0.5)
    )

for static_df in (model.outlet.static.df, model.pump.static.df):
    afvoer_mask = static_df["control_state"].eq("afvoer") & static_df["node_id"].isin(aanvoer_only_node_ids)
    static_df.loc[afvoer_mask, ["flow_rate", "max_flow_rate"]] = 0.0

# %%
# Model run

ribasim_toml_wet = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_wet", f"{SHORT_NAME}.toml")
ribasim_toml_dry = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_dry", f"{SHORT_NAME}.toml")
ribasim_toml = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model", f"{SHORT_NAME}.toml")

# hoofd run met verdamping
update_basin_static(model=model, evaporation_mm_per_day=0.5)
model = run_model_and_control(model, ribasim_toml_dry, qlr_path)

# prerun om het model te initialiseren met neerslag
update_basin_static(model=model, precipitation_mm_per_day=2)
model = run_model_and_control(model, ribasim_toml_wet, qlr_path)

# hoofd run
update_basin_static(model=model, precipitation_mm_per_day=1.5)
model = run_model_and_control(model, ribasim_toml, qlr_path)

# %%
