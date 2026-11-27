# %%
from typing import cast

import geopandas as gpd
import pandas as pd
from ribasim import Node
from ribasim.nodes import basin, level_boundary, outlet, tabulated_rating_curve
from ribasim_nl.geometry import connected_parts, polygon_parts, split_basin_multi_polygon
from ribasim_nl.reset_static_tables import reset_static_tables
from ribasim_nl.sanitize_node_table import sanitize_node_table
from shapely.geometry import LineString, MultiPolygon, Point
from shapely.geometry.base import BaseGeometry

from ribasim_nl import CloudStorage, Model, NetworkValidator

# Initialize cloud storage and set authority/model parameters
cloud = CloudStorage()
authority = "StichtseRijnlanden"
name = "hdsr"

# Define the path to the Ribasim model configuration file
ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_2024_6_3")
ribasim_toml = ribasim_dir / "model.toml"
model_edits_gpkg = cloud.joinpath(authority, "verwerkt/model_edits.gpkg")
hydamo_gpkg = cloud.joinpath(authority, "verwerkt/4_ribasim/hydamo.gpkg")
verbeteringen_gpkg = cloud.joinpath("StichtseRijnlanden/verwerkt/modelfouten_met_verbeter_acties_BD_311024.gpkg")
areas_gpkg = cloud.joinpath(authority, "verwerkt/3_input/areas.gpkg")

# %% read
model = Model.read(ribasim_toml)
# nodes and links are referred to by their key, see docs/dev/node-keys.qmd
n = model.node_id
lk = model.link_id
network_validator = NetworkValidator(model)
split_lijnen_df = gpd.read_file(verbeteringen_gpkg, layer="area_split_lijnen", fid_as_index=True)
extra_area_df = gpd.read_file(verbeteringen_gpkg, layer="nieuwe_areas_voor_opvulling", fid_as_index=True)
hydroobject_gdf = gpd.read_file(hydamo_gpkg, layer="hydroobject", fid_as_index=True)
afsluitmiddel_gdf = gpd.read_file(hydamo_gpkg, layer="afsluitmiddel", fid_as_index=True)
areas_gdf = gpd.read_file(areas_gpkg, layer="areas")

# %% some stuff we'll need again
level_data = level_boundary.Static(level=[0])

basin_data = [
    basin.Profile(level=[0.0, 1.0], area=[0.01, 1000.0]),
    basin.Static(
        drainage=[0.0],
        potential_evaporation=[0.001 / 86400],
        infiltration=[0.0],
        precipitation=[0.005 / 86400],
    ),
    basin.State(level=[0]),
]
outlet_data = outlet.Static(flow_rate=[5.0])

tabulated_rating_curve_data = tabulated_rating_curve.Static(level=[0.0, 5], flow_rate=[0, 0.1])

# HIER KOMEN ISSUES

# %%
model.remove_links(link_ids=[lk("up:bnd:38", "bnd:38")])
for node_id in [n("bnd:52"), n("bnd:53"), n("down:ST1194"), n("down:ST1034"), n("up:I6105"), n("SL2001")]:
    model.remove_node(node_id, remove_links=True)
# the basins of LevelBoundaries 38, 52 and 53, which the lumping adds back, but the old fix script removed with their
# links first
for node_id in [n("up:bnd:38"), n("up:bnd:52"), n("up:bnd:53")]:
    model.remove_node(node_id, remove_links=True)

model.link.add(model.tabulated_rating_curve[n("ST1194")], model.level_boundary[n("bnd:51")])
model.link.add(model.tabulated_rating_curve[n("ST1034")], model.level_boundary[n("bnd:51")])
model.link.add(model.level_boundary[n("bnd:38")], model.outlet[n("I6105")])


# %% see: https://github.com/Deltares/Ribasim-NL/issues/153#issuecomment-2456843484

# basins verwijderen

# basins samenvoegen
for basin_id, to_basin_id in [
    [n("up:H073109___6"), n("up:ST2037")],
    [n("up:ST0557"), n("up:G4065")],
    [n("up:I1487"), n("up:G4065")],
    [n("up:ST5142"), n("up:I1372")],
    [n("up:I6104"), n("up:G4060")],
    [n("up:G4041"), n("up:OW655")],
    [n("up:G4039"), n("up:OW611")],
    [n("up:OW610"), n("up:OW611")],
    [n("up:ST4071"), n("up:OW616")],
    [n("up:D13729"), n("up:D13730")],
    [n("up:AF0081"), n("up:H038337___5")],
    [n("up:OW606"), n("up:G0096")],
    [n("up:OW668"), n("up:G3024")],
    [n("up:I9998"), n("up:ST4902")],
    [n("up:OW581"), n("up:G5001")],
    [n("up:OW624"), n("up:H039753___13")],
    [n("up:OW698"), n("up:I1601")],
    [n("up:ST0833"), n("up:ST3017")],
    [n("up:ST1011"), n("up:ST1006")],
    [n("up:I1689"), n("up:H071905___2")],
    [n("up:SY7007"), n("up:ST6098")],
]:
    model.merge_basins(node_id=basin_id, to_node_id=to_basin_id)

model.merge_basins(node_id=n("up:H000016"), to_node_id=n("up:ST1730"), are_connected=False)
model.update_node(
    node_id=n("H000016"),
    node_type="TabulatedRatingCurve",
    data=[tabulated_rating_curve_data],
    node_properties={"name": "ST1999", "meta_object_type": "stuw"},
)

# verwijderen knopen
for node_id in [n("up:OW693"), n("OW693")]:
    model.remove_node(node_id, remove_links=True)

# toevoegen link
model.reverse_link(link_id=lk("G1013", "down:G1013"))
model.link.add(model.pump[n("G1013")], model.level_boundary[n("bnd:56")])

# updaten naar level_boundary
model.update_node(n("up:I1284"), "LevelBoundary", data=[level_data])


# %% https://github.com/Deltares/Ribasim-NL/issues/153#issuecomment-2456913415

# opheffen ST3122
geometry = Point(117995, 443062.5)
model.move_node(node_id=n("ST3122"), geometry=geometry)
model.node.df.loc[n("ST3122"), "name"] = "ST1985"

for link_id in [lk("ST1180", "up:ST3122"), lk("ST5013", "up:ST3122"), lk("ST2913", "up:ST3122")]:
    model.redirect_link(link_id=link_id, to_node_id=n("up:H058524___12"))

assert model.basin.area.df is not None
result = split_basin_multi_polygon(
    MultiPolygon([model.basin.area[n("up:H058524___12")].geometry.union_all().buffer(0.1).buffer(-0.1)]),
    line=cast(LineString, split_lijnen_df.at[28, "geometry"]),
)
model.basin.area.df = model.basin.area.df[model.basin.area.df.node_id != n("up:H058524___12")]
model.add_basin_area(result[1], node_id=n("up:H058524___12"))
model.add_basin_area(result[0], node_id=n("up:I0006"))

# %% https://github.com/Deltares/Ribasim-NL/issues/153#issuecomment-2457283397

# oplossen Loperiksingel/Nassaukade bij rand HHSK
# the western part of area AW0838, across the line, is of Schieland en de Krimpenerwaard
loperiksingel, schieland = split_basin_multi_polygon(
    MultiPolygon(polygon_parts(areas_gdf.loc[areas_gdf["code"] == "AW0838"].union_all())),
    line=cast(LineString, split_lijnen_df.at[1, "geometry"]),
)
boundary_node = model.level_boundary.add(Node(geometry=model.level_boundary[n("bnd:75")].geometry), tables=[level_data])

outlet_geom, basin_geom = cast(LineString, hydroobject_gdf.at[10466, "geometry"]).boundary.geoms
model.move_node(n("bnd:75"), geometry=basin_geom)
model.update_node(node_id=n("bnd:75"), node_type="Basin", data=basin_data)

outlet_node = model.outlet.add(Node(geometry=outlet_geom), tables=[outlet_data])
model.link.add(model.basin[n("bnd:75")], outlet_node)
model.link.add(outlet_node, boundary_node)
model.add_basin_area(loperiksingel, node_id=n("bnd:75"))


# %% https://github.com/Deltares/Ribasim-NL/issues/153#issuecomment-2457334403

# Toevoegen inlaat en basin bij Sportpark Het Hofland Schoonhoven
hydro_object = cast(LineString, hydroobject_gdf.at[8099, "geometry"])
afsluitmiddel = afsluitmiddel_gdf.loc[91]
outlet_node = model.outlet.add(
    Node(
        geometry=hydro_object.interpolate(hydro_object.project(cast(Point, afsluitmiddel.geometry))),
        name=afsluitmiddel.code,
        meta_object_type="afsluitmiddel",
    ),
    tables=[outlet_data],
)

basin_node = model.basin.add(
    Node(geometry=cast(LineString, hydroobject_gdf.at[7950, "geometry"]).boundary.geoms[0]), tables=basin_data
)
model.redirect_link(link_id=lk("bnd:74", "H057674___3"), from_node_id=basin_node.node_id)
model.link.add(model.level_boundary[n("bnd:74")], outlet_node)
model.link.add(outlet_node, basin_node)
# the area around it, which the lumping gives to a neighbouring basin
model.add_basin_area(areas_gdf.loc[areas_gdf["code"] == "AW0657"].union_all(), node_id=basin_node.node_id)

# %% https://github.com/Deltares/Ribasim-NL/issues/153#issuecomment-2457447764

# Split Basin / Area met area_split_lijnen


# the pieces that split lines cut off a basin area, by basin and number, and the areas without basin yet
pieces: dict[tuple[int, int], BaseGeometry] = {}
split_lijnen_todo_df = split_lijnen_df[split_lijnen_df.opmerkingen.isna()]
# the basins in the order of their node IDs
for basin_node_id, split_lijnen_select_df in split_lijnen_todo_df.groupby(split_lijnen_todo_df["node_key"].map(n)):
    # iterate trough split_lines per basin creating a series with geometries
    basin_node_id = int(cast(int, basin_node_id))
    series = gpd.GeoSeries(
        [MultiPolygon(polygon_parts(model.basin.area[basin_node_id].geometry.union_all()))], crs=model.crs
    )

    for row in split_lijnen_select_df.itertuples():
        line = cast(LineString, row.geometry)
        if len(series[series.intersects(line)]) != 1:
            raise ValueError(f"line with fid {row.Index} intersects basin {basin_node_id} more than once")

        # select polygon
        fid = series[series.intersects(line)].index[0]
        try:
            result = split_basin_multi_polygon(series.loc[fid], line=line)
        except ValueError:
            result = split_basin_multi_polygon(series.loc[fid], line=LineString(line.boundary.geoms))

        series.loc[fid] = result[0]
        series = pd.concat([series, gpd.GeoSeries([result[1]], crs=series.crs)], ignore_index=True)

    # the piece with the basin node remains its basin area, the others have no basin yet
    model.basin.area.df = model.basin.area.df[model.basin.area.df.node_id != basin_node_id]
    with_node = series.contains(model.basin[basin_node_id].geometry)
    if with_node.any():
        model.add_basin_area(series[with_node].iloc[0], node_id=basin_node_id)
    pieces |= {(basin_node_id, k): geometry for k, geometry in enumerate(series[~with_node])}

# %% https://github.com/Deltares/Ribasim-NL/issues/153#issuecomment-2457597461

# Basins toevoegen met nieuwe_areas_voor_opvulling
extra_areas = extra_area_df[extra_area_df.samenvoegen_met_node_key.isna()].geometry

# Mergen van nieuwe basin / area's met de toegewezen basin
extra_area_todo_df = extra_area_df[extra_area_df.samenvoegen_met_node_key.notna()]
# the basins in the order of their node IDs
for basin_node_id, df in extra_area_todo_df.groupby(extra_area_todo_df["samenvoegen_met_node_key"].map(n)):
    model.add_basin_area(cast(gpd.GeoDataFrame, df).union_all(), node_id=int(cast(int, basin_node_id)))

# Vlakken zonder node mergen met de juiste vlakken mét node
pieces[(n("up:H059728"), 2)] = pieces[(n("up:H059728"), 2)].union(pieces.pop((n("up:H057673___11"), 1)))
model.add_basin_area(extra_areas.pop(1), node_id=n("up:G8002"))
model.add_basin_area(model.basin.area[n("up:G4032")].geometry.union_all(), node_id=n("up:ST4058"))
extra_areas[17] = extra_areas[17].union(extra_areas.pop(50))
model.add_basin_area(model.basin.area[n("down:I0875")].geometry.union_all(), node_id=n("up:G3025"))

# 2 niet aansluitende vlakken toekennen aan de juiste nodes bij Nieuwegein geinoord
model.add_basin_area(areas_gdf.loc[areas_gdf["code"] == "AW1134"].union_all(), node_id=n("up:I1595"))
model.add_basin_area(extra_areas.pop(29), node_id=n("up:H063301"))

# Handmatig fixen van verkeerd toegewezen node_ids aan areas
model.add_basin_area(model.basin.area[n("up:AF0073")].geometry.union_all(), node_id=n("up:G4057"))
model.add_basin_area(extra_areas.pop(14), node_id=n("up:AF0073"))
model.add_basin_area(model.basin.area[n("up:ST6080")].geometry.union_all(), node_id=n("up:ST1873"))
model.add_basin_area(model.basin.area[n("up:H070570___1")].geometry.union_all(), node_id=n("up:G6028"))
model.remove_node(node_id=n("ST6080"), remove_links=True)

# Een aantal basins mergen
model.merge_basins(node_id=n("up:H059729___4"), to_node_id=n("up:ow:H059735"))
model.merge_basins(node_id=n("up:ST6080"), to_node_id=n("up:G4034"), are_connected=False)
model.merge_basins(node_id=n("down:I2337"), to_node_id=n("up:G3025"), are_connected=False)
model.merge_basins(node_id=n("down:I0875"), to_node_id=n("up:G3025"), are_connected=False)


# 2x een basin area met niet aansluitende vlakken "exploden" en toekennen aan de juiste nodes (gelijk aan Nieuwegein Geinoord)
# de delen die niet aansluiten op het deel met de basin-knoop worden losse vlakken
missing_geoms = []
for basin_node_id in [n("up:I1580"), n("up:AF0062")]:
    geometry, detached = connected_parts(
        model.basin.area[basin_node_id].geometry.union_all(), model.basin[basin_node_id].geometry
    )
    model.basin.area.df = model.basin.area.df[model.basin.area.df.node_id != basin_node_id]
    model.add_basin_area(geometry, node_id=basin_node_id)
    missing_geoms += detached

# the pieces, extra areas and detached parts get the basin within or near them, as basin areas without basin first
geometries = [*pieces.values(), *extra_areas, *missing_geoms]
df = gpd.GeoDataFrame({"node_id": [None] * len(geometries)}, geometry=geometries, crs=model.crs)
df.index = pd.RangeIndex(model.basin.area.df.index.max() + 1, model.basin.area.df.index.max() + 1 + len(df), name="fid")
model.basin.area.df = pd.concat([model.basin.area.df, df])
model.fix_unassigned_basin_area()
model.fix_unassigned_basin_area(method="closest")
model.fix_unassigned_basin_area()
# the ones that got a basin take their area from the other basin areas
assigned = model.basin.area.df[
    model.basin.area.df.index.isin(df.index) & model.basin.area.df.node_id.isin(model.basin.node.df.index)
]
model.basin.area.df = model.basin.area.df.drop(index=assigned.index)
for row in assigned.itertuples():
    model.add_basin_area(cast(MultiPolygon, row.geometry), node_id=int(cast(int, row.node_id)))

# bufferen met 0.1 en -0.1 om slivers op te lossen
model.basin.area.df.loc[:, "geometry"] = model.basin.area.df.buffer(0.1).buffer(-0.1)

# %% https://github.com/Deltares/Ribasim-NL/issues/153#issuecomment-2459498845

# Link-richtingen omdraaien
for link_id in [
    lk("I1448", "up:AF0061"),
    lk("I1553", "up:AF0061"),
    lk("I8009", "up:AF0061"),
    lk("I0035", "up:H055782___3"),
    lk("I1121", "up:AF0062"),
    lk("G8013", "up:G3012"),
    lk("ST1615", "up:AF0063"),
    lk("I6141", "up:H072580"),
    lk("I1103", "up:D13730"),
    lk("ST1174", "up:I6125"),
    lk("I6191", "up:G4065"),
    lk("ST6041", "down:I6043"),
    lk("ST6092", "down:I6043"),
    lk("ST1798", "up:ST0769"),
    lk("I1305", "up:I0427"),
    lk("ST1307", "up:I0427"),
    lk("I7079", "up:H041354___1"),
    lk("I6017", "up:H038337___5"),
    lk("G6025", "up:G0039"),
    lk("ST2905", "up:I6191"),
    lk("G6009", "up:OW671"),
    lk("G1004", "up:G1006"),
    lk("ST6512", "up:G2014"),
    lk("ST1878", "up:D2024"),
    lk("ST8011", "up:I1372"),
    lk("ST1066", "up:H055573___0"),
    lk("up:I6191", "I6191"),
    lk("up:I1465", "I1465"),
    lk("up:G1017", "ST1042"),
    lk("G3003", "bnd:9"),
    lk("G4026", "bnd:16"),
    lk("G4031", "bnd:23"),
    lk("I1572", "bnd:30"),
    lk("I1201", "bnd:39"),
    lk("I1523", "bnd:45"),
    lk("up:I1325", "I1325"),
    lk("I1325", "up:ST0016"),
    lk("I1670", "up:G4032"),
    lk("up:I1670", "I1670"),
]:
    model.reverse_link(link_id=link_id)

# fix 2 incorrecte links
model.link.df.loc[lk("ow:H041341-0", "up:I1237"), "from_node_id"] = n("ow:H041341-0")
model.link.df.loc[lk("ow:H038341", "up:H038374___0"), "from_node_id"] = n("ow:H038341")

# %% https://github.com/Deltares/Ribasim-NL/issues/153#issuecomment-2457447764

# Split Basin / Area met area_split_lijnen

# nog een aantal basins mergen
model.merge_basins(node_id=n("up:H034275"), to_node_id=n("up:D4320"))
model.merge_basins(node_id=n("up:H034549___1"), to_node_id=n("up:D4320"))
model.merge_basins(node_id=n("up:H059727___1"), to_node_id=n("up:H059731___3"))
model.merge_basins(node_id=n("up:H034273"), to_node_id=n("up:H040078"))
model.merge_basins(node_id=n("up:H040385-1"), to_node_id=n("up:H040078"))
model.merge_basins(node_id=n("up:OW595"), to_node_id=n("up:H038345___1"))
model.merge_basins(node_id=n("up:H040094-0___2"), to_node_id=n("up:H040093___1"))
model.merge_basins(node_id=n("up:AF0070"), to_node_id=n("up:H040093___1"))
model.merge_basins(node_id=n("up:ow:H040102"), to_node_id=n("up:H040340___2"))
model.merge_basins(node_id=n("up:H040344-0___4"), to_node_id=n("up:H040340___2"))
model.merge_basins(node_id=n("up:OW625"), to_node_id=n("up:H039753___13"))
model.merge_basins(node_id=n("up:H040349___3"), to_node_id=n("up:H077673___1"))
model.merge_basins(node_id=n("up:OW598"), to_node_id=n("up:H077673___1"))
model.merge_basins(node_id=n("up:H057673___11"), to_node_id=n("up:H057673___0"))
model.merge_basins(node_id=n("up:H041365-0___0"), to_node_id=n("up:OW686"))
model.merge_basins(node_id=n("up:H041364-1___18"), to_node_id=n("up:OW686"))
model.merge_basins(node_id=n("up:ST1888"), to_node_id=n("up:H034560___0"))
model.merge_basins(node_id=n("up:I0798"), to_node_id=n("up:H071458___1"))
model.merge_basins(node_id=n("up:OW686"), to_node_id=n("up:H041365-1___1"))
model.merge_basins(node_id=n("up:H012373___2"), to_node_id=n("up:H072046"))
model.merge_basins(node_id=n("up:AF0084"), to_node_id=n("up:H041347___1"))
model.merge_basins(node_id=n("up:H040090___4"), to_node_id=n("up:H040091-1___0"))
model.merge_basins(node_id=n("up:ST6013"), to_node_id=n("up:OW612"))
model.merge_basins(node_id=n("up:H034552___9"), to_node_id=n("up:H034561___1"))
model.merge_basins(node_id=n("up:H071908___6"), to_node_id=n("up:H071905___2"))
model.merge_basins(node_id=n("up:G2021"), to_node_id=n("up:I1042"))
model.merge_basins(node_id=n("up:H056120___0"), to_node_id=n("up:H055789___0"))
model.merge_basins(node_id=n("up:H040091-1___0"), to_node_id=n("up:AF0073"))
model.merge_basins(node_id=n("up:G8000"), to_node_id=n("up:H055781___4"))
model.merge_basins(node_id=n("up:H055782___3"), to_node_id=n("up:H055781___4"))
model.merge_basins(node_id=n("up:H055774-2___3"), to_node_id=n("up:I0041"))
model.merge_basins(node_id=n("up:I6098"), to_node_id=n("up:OW674"))
model.merge_basins(node_id=n("up:H057655___2"), to_node_id=n("up:OW674"))
model.merge_basins(node_id=n("up:OW674"), to_node_id=n("up:H057658-0"))
model.merge_basins(node_id=n("up:I0008"), to_node_id=n("up:H055784"))
model.merge_basins(node_id=n("up:AF0095"), to_node_id=n("up:H055784"))
model.merge_basins(node_id=n("up:H057637-0___1"), to_node_id=n("up:H038039___0"))
model.merge_basins(node_id=n("up:H038039___0"), to_node_id=n("up:I2318"))
model.merge_basins(node_id=n("up:ow:H057403"), to_node_id=n("up:I0543"))
model.merge_basins(node_id=n("up:H057663-0___1"), to_node_id=n("up:I0543"))
model.merge_basins(node_id=n("up:H038044-1"), to_node_id=n("up:I0543"))
model.merge_basins(node_id=n("up:H057651-0___0"), to_node_id=n("up:I0543"))
model.merge_basins(node_id=n("up:I1317"), to_node_id=n("up:I2101"))
model.merge_basins(node_id=n("up:H070570___1"), to_node_id=n("up:I2101"))
model.merge_basins(node_id=n("up:H071905___2"), to_node_id=n("up:H071910___7"))
model.merge_basins(node_id=n("up:I6028"), to_node_id=n("up:H038323___0"))
model.merge_basins(node_id=n("up:I0819"), to_node_id=n("up:H038323___0"))
model.merge_basins(node_id=n("up:H041354___1"), to_node_id=n("up:H038323___0"))
model.merge_basins(node_id=n("up:H038327___0"), to_node_id=n("up:H041348___2"))
model.merge_basins(node_id=n("up:H038357-0"), to_node_id=n("up:H038337___5"))
model.merge_basins(node_id=n("up:H071450"), to_node_id=n("up:H071458___1"))
model.merge_basins(node_id=n("up:H071464-0"), to_node_id=n("up:H071458___1"))
model.merge_basins(node_id=n("up:I1251"), to_node_id=n("up:H071458___1"))
model.merge_basins(node_id=n("up:G0087"), to_node_id=n("up:G4032"))
model.merge_basins(node_id=n("up:I0880"), to_node_id=n("up:I0901"), are_connected=False)
model.merge_basins(node_id=n("up:I6116"), to_node_id=n("up:ST6046"), are_connected=False)
model.merge_basins(node_id=n("up:I1670"), to_node_id=n("up:G6005"))
model.merge_basins(node_id=n("up:G6005"), to_node_id=n("up:ST0142"), are_connected=False)
model.merge_basins(node_id=n("up:I1654"), to_node_id=n("up:G4311"))
model.merge_basins(node_id=n("down:I1142"), to_node_id=n("up:ST4102"), are_connected=False)
model.merge_basins(node_id=n("down:I0168"), to_node_id=n("up:I6206"), are_connected=False)
model.merge_basins(node_id=n("up:I2230"), to_node_id=n("up:ST1562"), are_connected=False)
model.merge_basins(node_id=n("up:I1611"), to_node_id=n("up:G4038"))
model.merge_basins(node_id=n("up:H039753___13"), to_node_id=n("up:I6044"))
model.merge_basins(node_id=n("up:I6044"), to_node_id=n("up:H040093___1"))
model.merge_basins(node_id=n("up:I6067"), to_node_id=n("up:H063301"))
model.merge_basins(node_id=n("up:I7086"), to_node_id=n("up:G5004"))
model.merge_basins(node_id=n("down:I1317"), to_node_id=n("up:ST1707"), are_connected=False)
model.merge_basins(node_id=n("up:ST0142"), to_node_id=n("up:I1111"), are_connected=False)
model.merge_basins(node_id=n("down:I0810"), to_node_id=n("up:G4056"), are_connected=False)
model.merge_basins(node_id=n("up:ST0016"), to_node_id=n("up:G0005"))
model.merge_basins(node_id=n("down:I2075"), to_node_id=n("up:G2901"), are_connected=False)
model.merge_basins(node_id=n("up:I1325"), to_node_id=n("up:ST1170"))
model.merge_basins(node_id=n("up:I1709"), to_node_id=n("up:G4066"))
model.remove_node(node_id=n("OW604"), remove_links=True)
model.remove_node(node_id=n("ST0016"), remove_links=True)
model.remove_node(node_id=n("I9997"), remove_links=True)
model.remove_node(node_id=n("ST1766"), remove_links=True)
model.remove_node(node_id=n("OW714"), remove_links=True)
model.remove_node(node_id=n("H063301"), remove_links=True)
model.redirect_link(link_id=lk("up:G3024", "ST2913"), from_node_id=n("up:I0006"))
model.redirect_link(link_id=lk("G0091", "up:I1595"), to_node_id=n("up:H063301"))
model.merge_basins(node_id=n("up:I1595"), to_node_id=n("up:G0091"), are_connected=False)
model.remove_node(node_id=n("OW542"), remove_links=True)
model.remove_node(node_id=n("I6084"), remove_links=True)
model.remove_node(node_id=n("OW553"), remove_links=True)
model.remove_node(node_id=n("OW700"), remove_links=True)
model.merge_basins(node_id=n("up:OW677"), to_node_id=n("up:G0007"))
model.merge_basins(node_id=n("up:OW617"), to_node_id=n("up:G3021"))
model.merge_basins(node_id=n("up:ST1023"), to_node_id=n("up:I0040"))
model.merge_basins(node_id=n("up:OW652"), to_node_id=n("up:I0040"))
model.merge_basins(node_id=n("up:ST1042"), to_node_id=n("up:OW653"))
model.merge_basins(node_id=n("up:H055573___0"), to_node_id=n("up:ST1091"))
model.merge_basins(node_id=n("up:I1435"), to_node_id=n("up:ST8013"))
model.merge_basins(node_id=n("up:OW532"), to_node_id=n("up:ST2012"))
model.merge_basins(node_id=n("up:H013191___13"), to_node_id=n("up:G4451"))
model.merge_basins(node_id=n("up:G4451"), to_node_id=n("up:H017235___1"))
model.merge_basins(node_id=n("up:H017235___1"), to_node_id=n("up:G1003"))
model.merge_basins(node_id=n("up:G1003"), to_node_id=n("up:H012352___1"))
model.merge_basins(node_id=n("up:G0004"), to_node_id=n("up:H012352___1"))
# the Grecht from Oud-Kamerik (G0019) to OW687 is one Basin, without the ManningResistances in between, see #1078
model.merge_basins(node_id=n("up:ow:H041596"), to_node_id=n("up:I0899"))
model.merge_basins(node_id=n("up:I0899"), to_node_id=n("up:OW687"))
# Gemaal Grechtkade (G4043) is no longer there, in HyDAMO and in GKW-HyDAMO. In its place the Grechtkade inlet I6013 lets
# water from the Grecht into polder Oud-Kamerik, see #1078
(grechtkade_inlet,) = afsluitmiddel_gdf.loc[afsluitmiddel_gdf["code"] == "I6013", "geometry"]
model.remove_node(n("G4043"), remove_links=True)
grechtkade_node = model.outlet.add(
    Node(
        geometry=Point(grechtkade_inlet.x, grechtkade_inlet.y),
        name="I6013",
        meta_object_type="afsluitmiddel",
        meta_key="I6013",
    ),
    tables=[outlet_data],
)
model.link.add(model.basin[n("up:G4043")], grechtkade_node)
model.link.add(grechtkade_node, model.basin[n("up:G0019")])
model.redirect_link(link_id=lk("up:H078195", "H078195"), from_node_id=n("up:H058524___12"))
model.update_node(node_id=n("H078195"), node_type="Outlet")
model.update_node(node_id=n("H015594"), node_type="Outlet")
model.update_node(node_id=n("OW575"), node_type="Outlet")
model.update_node(node_id=n("OW689"), node_type="Outlet")
# EINDE ISSUES


# %%
# corrigeren knoop-topologie

# ManningResistance bovenstrooms LevelBoundary naar Outlet
for row in network_validator.link_incorrect_type_connectivity().itertuples():
    model.update_node(row.from_node_id, "Outlet", data=[outlet_data])

# Inlaten van ManningResistance naar Outlet
for row in network_validator.link_incorrect_type_connectivity(
    from_node_type="LevelBoundary", to_node_type="ManningResistance"
).itertuples():
    model.update_node(row.to_node_id, "Outlet", data=[outlet_data])


# %% Reset static tables

# Reset static tables
model = reset_static_tables(model)

# %%
# buffer out small slivers

model.explode_basin_area()  # all multipolygons to singles
actions = [
    #  "remove_basin_area",
    "remove_node",
    "remove_link",
    "add_basin",
    "add_basin_area",
    "update_basin_area",
    "merge_basins",
    "reverse_link",
    "move_node",
    "update_node",
    "connect_basins",
    "redirect_link",
]
actions = [i for i in actions if i in gpd.list_layers(model_edits_gpkg).name.to_list()]
model.apply_edits(model_edits_gpkg, actions, sort_by_order=True)

# the pieces, extra areas and detached parts that got no basin
model.remove_unassigned_basin_area()

# %% corrigeren knoop-topologie
# ManningResistance bovenstrooms LevelBoundary naar Outlet
for row in network_validator.link_incorrect_type_connectivity().itertuples():
    model.update_node(row.from_node_id, "Outlet")

# Inlaten van ManningResistance naar Outlet
for row in network_validator.link_incorrect_type_connectivity(
    from_node_type="LevelBoundary", to_node_type="ManningResistance"
).itertuples():
    model.update_node(row.to_node_id, "Outlet")


# %%

# sanitize node-table
for node_id in model.tabulated_rating_curve.node.df.index:
    model.update_node(node_id=node_id, node_type="Outlet")

# ManningResistance that are duikersifonhevel to outlet
for node_id in model.manning_resistance.node.df[
    model.manning_resistance.node.df["meta_object_type"] == "duikersifonhevel"
].index:
    model.update_node(node_id=node_id, node_type="Outlet")

# basins and outlets we've added do not have category, we fill with hoofdwater
model.node.df.loc[model.basin.node.df.index[model.basin.node.df["meta_categorie"].isna()], "meta_categorie"] = (
    "hoofdwater"
)
model.node.df.loc[model.outlet.node.df.index[model.outlet.node.df["meta_categorie"].isna()], "meta_categorie"] = (
    "hoofdwater"
)

# somehow Sluis Engelen (beheerregister AAM) has been named Henriettesluis
model.node.df.loc[
    (model.node.df["node_type"] == "Outlet") & (model.node.df.name == "Henriëttesluis"),
    "name",
] = "AKW855"

# manning resistance must be outlet
model.update_node(node_id=n("H060192___0"), node_type="Outlet")  # duiker wordt outlet, was manning
model.update_node(node_id=n("OW663"), node_type="Outlet")  # duiker wordt outlet, was manning

# name-column contains the code we want to keep, meta_name the name we want to have
df = pd.concat(
    [
        gpd.read_file(hydamo_gpkg, layer="gemaal"),
        gpd.read_file(hydamo_gpkg, layer="sluis"),
        gpd.read_file(hydamo_gpkg, layer="stuw"),
    ],
    ignore_index=True,
)
df.set_index("code", inplace=True)
names = df["naam"].str.title()

# set meta_gestuwd in basins
model.node.df.loc[model.node.df["node_type"] == "Basin", "meta_gestuwd"] = False
model.node.df.loc[model.node.df["node_type"] == "Outlet", "meta_gestuwd"] = False
model.node.df.loc[model.node.df["node_type"] == "Pump", "meta_gestuwd"] = True

model.basin.area.df[model.basin.area.df.node_id != n("up:I1601")]
# set stuwen als gestuwd

# 1349 was Manning maar is duiker dus outlet
model.update_node(node_id=n("ow:H038675-0"), node_type="Outlet")

model.node.df.loc[
    (model.node.df["node_type"] == "Outlet")
    & model.node.df["meta_object_type"].isin(["stuw", "afsluitmiddel", "sluis"]),
    "meta_gestuwd",
] = True

# remove_nodes
model.remove_node(node_id=n("AF0094"), remove_links=True)
model.remove_node(node_id=n("AF0049"), remove_links=True)
model.remove_node(node_id=n("H078552"), remove_links=True)
model.remove_node(node_id=n("SY6009"), remove_links=True)
model.remove_node(node_id=n("SY0071"), remove_links=True)
model.remove_node(node_id=n("I1582"), remove_links=True)
model.remove_node(node_id=n("I1581"), remove_links=True)
model.remove_node(node_id=n("I6135"), remove_links=True)
model.remove_node(node_id=n("I1463"), remove_links=True)
model.remove_node(node_id=n("ST6058"), remove_links=True)
model.remove_node(node_id=n("ST1190"), remove_links=True)
model.remove_node(node_id=n("SL0027"), remove_links=True)
model.remove_node(node_id=n("bnd:77"), remove_links=True)
model.remove_node(node_id=n("bnd:27"), remove_links=True)
model.remove_node(node_id=n("bnd:68"), remove_links=True)

# Koekoek gaat (direct) naar Broeik (basin 2089)
model.redirect_link(link_id=lk("I1523", "up:ST5019"), from_node_id=n("I1523"), to_node_id=n("up:G5001"))


# set bovenstroomse basins als gestuwd
node_df = model.node.df[model.node.df["meta_gestuwd"] & model.node.df["node_type"].isin(["Outlet", "Pump"])]

upstream_node_ids = [model.upstream_node_id(i) for i in node_df.index]
basin_node_ids = model.basin.node.df.index.intersection(upstream_node_ids)
model.node.df.loc[basin_node_ids, "meta_gestuwd"] = True

# set álle benedenstroomse outlets van gestuwde basins als gestuwd (dus ook duikers en andere objecten)
downstream_node_ids = pd.Series([model.downstream_node_id(i) for i in basin_node_ids]).explode().to_numpy()
model.node.df.loc[model.outlet.node.df.index.intersection(downstream_node_ids), "meta_gestuwd"] = True

sanitize_node_table(
    model,
    meta_columns=["meta_code_waterbeheerder", "meta_categorie", "meta_gestuwd"],
    copy_map=[
        {"node_types": ["Outlet", "Pump"], "columns": {"name": "meta_code_waterbeheerder"}},
        {"node_types": ["Basin", "LevelBoundary", "FlowBoundary", "ManningResistance"], "columns": {"name": ""}},
    ],
    names=names,
)

model.node.df.loc[n("I6073"), "name"] = "Westriool"
model.node.df.loc[n("I6119"), "name"] = "Inlaat Vreeswijk"


# Open water connections between Basins of Stichtse Rijnlanden are ManningResistances, not Outlets that drain their
# upstream Basin, see https://github.com/Deltares/Ribasim-NL/issues/912. They keep their node and link IDs. Those to
# other authorities are converted after coupling, see `outlet_to_manning_resistance` in 09_koppelen_modellen.py.
OPEN_WATER_CODES = ["OW575", "OW663"]
for code in OPEN_WATER_CODES:
    model.update_node(node_id=n(code), node_type="ManningResistance")
# OW713 is not open water, but stuw ST1068
model.node.df.loc[n("OW713"), "meta_code_waterbeheerder"] = "ST1068"

# %% give the holes that the edits left in the basin areas to a basin
model.fill_basin_area_holes(areas_gpkg, excluded=schieland)

#  %% write model
model.use_validation = True
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_fix_model", f"{name}.toml")
model.write(ribasim_toml)
model.report_basin_area()
model.report_internal_basins()
