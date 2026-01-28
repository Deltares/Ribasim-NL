# %%

import geopandas as gpd
import pandas as pd
from ribasim_nl.aquo import waterbeheercode
from ribasim_nl.control import get_node_table_with_from_to_node_ids
from shapely.geometry import LineString, MultiPolygon, Polygon, box

from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()

MODEL_POST_FIX = "dynamic_model"
ARROW_LENGTH = 1000
AUTHORITIES = ["Noorderzijlvest"]


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
    gpd.GeoSeries([mask_box.difference(poly_mask)], crs=28992).to_file(system_gpkg, layer="mask")

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

    arrows = []
    for row in structures_src_df.itertuples():
        fid = row.Index
        node_id = row.node_id
        if pd.isna(node_id):
            ValueError(f"node_id is NaN for fid {fid}. Search by name and code not implemented yet")

        if pd.isna(row.functie):
            control_node_ids = control_links_df[control_links_df.to_node_id == node_id].from_node_id.values

            if len(control_node_ids) > 1:  # if multiple control_node_ids, one if them is flow demand
                node_func = "uitlaat (doorspoeling)"
            else:  # else we can strip the function from the name
                discrete_control_node = model.discrete_control.node.df[
                    model.discrete_control.node.df.index.isin(control_node_ids)
                ].iloc[0]
                node_func = discrete_control_node["name"].split(":")[0]

            structures_src_df.loc[fid, "functie"] = node_func

        # check if we can copy attribute-values
        if pd.isna(row.naam):
            structures_src_df.loc[fid, "naam"] = connector_nodes_df.at[node_id, "name"]
        if pd.isna(row.code):
            structures_src_df.loc[fid, "code"] = connector_nodes_df.at[node_id, "meta_code_waterbeheerder"]

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

    structures_src_df.sort_values("functie").reset_index(drop=True).to_file(system_gpkg, layer="kunstwerken")
    gpd.GeoDataFrame(arrows, crs=structures_src_df.crs).to_file(system_gpkg, layer="richting")
