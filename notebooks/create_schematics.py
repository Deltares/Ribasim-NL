# %%

import geopandas as gpd
import pandas as pd
from ribasim_nl.aquo import waterbeheercode
from ribasim_nl.case_conversions import pascal_to_snake_case
from ribasim_nl.control import get_node_table_with_from_to_node_ids
from shapely.geometry import LineString, MultiPolygon, Polygon, box

from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()

MODEL_POST_FIX = "dynamic_model"
ARROW_LENGTH = 800
AUTHORITIES = ["StichtseRijnlanden"]
ADD_TOP10_NL = {
    "StichtseRijnlanden": [
        "Vecht",
        "Vaartsche Rijn",
        "Amsterdam-Rijnkanaal",
        "Oude Rijn",
        "Hollandsche IJssel",
        "Lek",
        "Kromme Rijn",
        "Merwedekanaal",
        "Lekkanaal",
        "Leidsche Rijn",
        "Caspergouwse",
        "Oude Kromme Rijn",
        "Doorslag",
    ]
}


def get_toml_file(authority, post_fix):
    model_dir = cloud.joinpath(authority, "modellen", f"{authority}_{post_fix}")
    return next(model_dir.glob("*.toml"))


def fix_geometry(geometry):
    geometry = geometry.buffer(0).buffer(0)
    if isinstance(geometry, MultiPolygon):
        return MultiPolygon([Polygon(geom.exterior) for geom in geometry.geoms])
    else:
        return Polygon(geometry.exterior)


for authority in AUTHORITIES:
    # files we read
    sturing_dir = cloud.joinpath(authority, "verwerkt", "sturing")
    aanvoergebieden_gpkg = sturing_dir / "aanvoergebieden.gpkg"
    system_gpkg = sturing_dir / "watersysteem.gpkg"
    table_md = sturing_dir / "afvoertabel.md"
    water_authorities_gpkg = cloud.joinpath("Basisgegevens", "waterschapsgrenzen", "waterschapsgrenzen.gpkg")
    north_see_gpkg = cloud.joinpath("Basisgegevens", "RWS_waterschaps_grenzen", "noordzee.gpkg")
    kunstwerken_gpkg = sturing_dir / "kunstwerken.gpkg"
    system_gpkg.parent.mkdir(exist_ok=True)

    # aanvoergebieden layer
    df = gpd.read_file(aanvoergebieden_gpkg, layer="aanvoergebieden").dissolve(by="aanvoergebied")
    df["geometry"] = df["geometry"].apply(fix_geometry)
    df.reset_index()[["aanvoergebied", "geometry"]].to_file(system_gpkg, layer="aanvoergebieden")

    # maks layer
    df = gpd.read_file(water_authorities_gpkg)
    poly_mask = df.loc[df.nationalCode == waterbeheercode[authority], "geometry"].union_all()
    xmin, ymin, xmax, ymax = poly_mask.bounds
    xmin -= 10000
    ymin -= 10000
    xmax += 10000
    ymax += 10000
    mask_box = box(xmin, ymin, xmax, ymax)
    north_sea_df = gpd.read_file(north_see_gpkg, bbox=poly_mask.bounds)
    if not north_sea_df.empty:
        north_sea_poly = north_sea_df.union_all().buffer(100).buffer(-100)
        north_sea_poly = Polygon(north_sea_poly.exterior)
        north_sea_poly = north_sea_poly.intersection(mask_box)
        poly_mask = poly_mask.difference(north_sea_poly)
    poly_mask = mask_box.difference(poly_mask)
    gpd.GeoSeries([poly_mask], crs=28992).to_file(system_gpkg, layer="mask")

    # get Top10NL
    if authority in ADD_TOP10_NL.keys():
        df = gpd.read_file(
            cloud.joinpath(r"Basisgegevens\Top10NL\top10nl_Compleet.gpkg"),
            layer="top10nl_waterdeel_vlak",
            bbox=mask_box.bounds,
        )
        df = df[df.naamnl.isin(ADD_TOP10_NL[authority])].clip(mask_box)
        df.dissolve("naamnl").to_file(system_gpkg, layer="oppervlaktewater")

    # kunstwerken layer
    model = Model.read(get_toml_file(authority, MODEL_POST_FIX))
    connector_nodes_df = get_node_table_with_from_to_node_ids(model=model)
    control_links_df = model.link.df[model.link.df.link_type == "control"]
    flow_links_df = model.link.df[model.link.df.link_type == "flow"]

    structures_src_df = gpd.read_file(kunstwerken_gpkg, fid_as_index=True)
    structures_src_df.rename(columns={"name": "naam", "meta_code_waterbeheerder": "code"}, inplace=True)
    if "functie" not in structures_src_df.columns:
        structures_src_df["functie"] = pd.Series(dtype=str)

    if "add_label" not in structures_src_df.columns:
        structures_src_df["add_label"] = True
    else:
        if structures_src_df["add_label"].dtype == bool:
            structures_src_df.loc[structures_src_df["add_label"].isna(), ["add_label"]] = False
        else:
            structures_src_df.loc[structures_src_df["add_label"].isna(), ["add_label"]] = 0
            structures_src_df["add_label"] = structures_src_df["add_label"].astype(bool)

    if "aanvoer [m3/s]" not in structures_src_df.columns:
        structures_src_df["aanvoer [m3/s]"] = pd.Series(dtype=float)

    if "afvoer [m3/s]" not in structures_src_df.columns:
        structures_src_df["afvoer [m3/s]"] = pd.Series(dtype=float)

    arrows = []
    for fid, row in structures_src_df.iterrows():
        node_id = row.node_id
        if pd.isna(node_id):
            ValueError(f"node_id is NaN for fid {fid}. Search by name and code not implemented yet")

        # get/set functie
        control_node_ids = control_links_df[control_links_df.to_node_id == node_id].from_node_id.values
        if pd.isna(row.functie):
            if len(control_node_ids) > 1:  # if multiple control_node_ids, one if them is flow demand
                node_func = "uitlaat (doorspoeling)"
            elif len(control_node_ids) == 1:  # else we can strip the function from the name
                discrete_control_node = model.discrete_control.node.df[
                    model.discrete_control.node.df.index.isin(control_node_ids)
                ].iloc[0]
                node_func = discrete_control_node["name"].split(":")[0]
            else:
                node_func = "geen"

            structures_src_df.loc[fid, "functie"] = node_func

        if not node_func == "geen":
            # get static data for next step
            node_type = connector_nodes_df.at[node_id, "node_type"]
            static_df = getattr(model, pascal_to_snake_case(node_type)).static.df
            static_df = static_df[static_df.node_id == node_id]
            # get/set aanvoer
            if pd.isna(row["aanvoer [m3/s]"]):
                if node_func == "uitlaat (doorspoeling)":
                    structures_src_df.loc[fid, "aanvoer [m3/s]"] = model.flow_demand.static.df[
                        model.flow_demand.static.df.node_id.isin(control_node_ids)
                    ].iloc[0]["demand"]
                else:
                    structures_src_df.loc[fid, "aanvoer [m3/s]"] = static_df[
                        static_df.control_state == "aanvoer"
                    ].max_flow_rate.max()

            if pd.isna(row["afvoer [m3/s]"]):
                structures_src_df.loc[fid, "afvoer [m3/s]"] = static_df[
                    static_df.control_state == "afvoer"
                ].max_flow_rate.max()

            # make arrow
            from_link = flow_links_df.set_index("from_node_id").at[node_id, "geometry"]
            if from_link.length >= ARROW_LENGTH:
                arrow = LineString([row.geometry, from_link.interpolate(ARROW_LENGTH)])
            else:
                (x0, y0), (x1, y1) = (
                    (row.geometry.x, row.geometry.y),
                    (from_link.boundary.geoms[1].x, from_link.boundary.geoms[1].y),
                )
                dx = x1 - x0
                dy = y1 - y0
                current_length = (dx**2 + dy**2) ** (1 / 2)
                scale = ARROW_LENGTH / current_length
                to_point = (x0 + dx * scale, y0 + dy * scale)

                arrow = LineString([(x0, y0), to_point])
            arrows += [{"geometry": arrow}]

        # check if we can copy attribute-values
        if pd.isna(row.naam):
            structures_src_df.loc[fid, "naam"] = connector_nodes_df.at[node_id, "name"]
        if pd.isna(row.code):
            structures_src_df.loc[fid, "code"] = connector_nodes_df.at[node_id, "meta_code_waterbeheerder"]

    structures_src_df.sort_values("functie").reset_index(drop=True).to_file(system_gpkg, layer="kunstwerken")
    gpd.GeoDataFrame(arrows, crs=structures_src_df.crs).to_file(system_gpkg, layer="richting")

    mask = structures_src_df.add_label & ~structures_src_df.naam.isna() & structures_src_df.functie != "geen"
    table_md.write_text(
        structures_src_df[structures_src_df.add_label][
            ["naam", "code", "node_id", "functie", "aanvoer [m3/s]", "afvoer [m3/s]"]
        ]
        .sort_values(by=["naam"])
        .to_markdown(index=False)
    )
