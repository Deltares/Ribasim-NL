# %%
import ast
import os

import geopandas as gdf
import numpy as np
import pandas as pd
from ribasim import Model
from spatial_coupling import search_geometry_nodes, search_type_nodes
from shapely import wkt

from ribasim_nl import CloudStorage

# %%
###################
# initialize script - Koppeltabel versie 1
###################

cloud = CloudStorage()

# paths:
base = cloud.joinpath("Landelijk", "resultaatvergelijking", "koppeltabel")

# Draaien vanuit de uitgangskoppeltabel op basis van alle nieuw aangeleverde metingen en feedback
loc_ref_koppeltabel = cloud.joinpath(base, "Koppeltabel_uitgangspunt.xlsx")


# paths:
#!TODO: Nog niet mogelijk om lhm-coupled model op GC in te lezen
# met huidige ribasim dev versie

rws_model_versions = cloud.uploaded_models(authority="Rijkswaterstaat")
latest_lhm_version = sorted(
    [i for i in rws_model_versions if i.model == "lhm_coupled"], key=lambda x: getattr(x, "sorter", "")
)[-1]
model_folder = cloud.joinpath("Rijkswaterstaat", "modellen", latest_lhm_version.path_string)


# Filteren of gebruiken we een gekoppeld model:
filter_waterschappen = False

# Filteren voor een enkel waterschap:
# waterschapsnaam = ["Delfland"]

# Filteren voor meerdere waterschappen:
# waterschapsnaam = ['AmstelGooienVecht',
#                    'Rijnland', 'StichtseRijnlanden', 'Delfland',
#  'SchielandendeKrimpenerwaard', 'HollandseDelta', 'Rijkswaterstaat']

waterschapsnaam = None

toml_naam = "lhm-coupled.toml"

# Als we wel een geometry hadden opgeslagen in de input koppeltabel, maar we kunnen in de buurt in het nieuwe model
# geen connector knoopjes vinden dan hebben we dus geen verbinding meer en willen we wel of niet een nieuwe suggestie op basis van een nieuw gevonden
# ruimtelijke koppeling als deze beschikbaar is.
nieuwe_suggestie_als_oude_geometry_ontbreekt = True

# Het gaat er hier om of de tabel die als input wordt aangeleverd al een revieuw versie heeft gehad of niet
# en dus de kolommen previous_ of new_ bevatten.
# Als dit NIET zo is --> eerste_tabel= True
# Anders --> eerste_tabel= False

eerste_tabel = True

versie = 1
wegschrijven_nieuwe_tabel = cloud.joinpath(base, f"Transformed_koppeltabel_versie{versie}.xlsx")

# synchronize paths
cloud.synchronize([base, model_folder])

#!TODO: weghalen als lhm-coupled met huidige ribasim versie kan worden ingelezen
model_folder_temporary = r"C:\Users\micha.veenendaal\Data\Ribasim LHM validatie\LHM_model_werkend\lhm_coupled"


# %% functions


def ParseList(val):
    """The function `ParseList` checks if a given string represents a list and returns the list or the original value accordingly.

    Parameters
    ----------
    val
        The `ParseList` function takes a single parameter `val`, which is expected to be a string. The
    function checks if the string starts and ends with square brackets `[ ]`, indicating a list-like
    structure. If the string meets these conditions, it attempts to parse the string using
    `ast.literal_eval

    Returns
    -------
        The `ParseList` function is designed to parse a string representation of a list. If the input `val`
    is a string that starts with '[' and ends with ']', it attempts to evaluate the string using
    `ast.literal_eval` to convert it into a Python list. If the evaluation is successful and the result
    is a list, it returns the first element of the list if the list has only one element.

    """
    if isinstance(val, str) and val.strip().startswith("[") and val.strip().endswith("]"):
        try:
            parsed = ast.literal_eval(val)
            if isinstance(parsed, list):
                return parsed[0] if len(parsed) == 1 else parsed
        except Exception:
            return val

    return val


def LaadKoppeltabel(loc_koppeltabel, eerste_tabel=False):
    """
    The function `LaadKoppeltabel` reads an Excel file, parses lists in the 'link_id' column, and converts the 'geometry' column to a geometry object.

    Parameters
    ----------
    loc_koppeltabel
        The `loc_koppeltabel` parameter in the `LaadKoppeltabel` function is expected to be a file location
    pointing to an Excel file that contains data for a koppeltabel (linking table).

    Returns
    -------
        The function returns the updated koppeltabel dataframe with the
    parsed columns 'link_id_parsed' and 'geometry_parsed'.

    """
    koppeltabel = pd.read_excel(loc_koppeltabel)

    # Eerst tabel in het proces was anders gestructureerd
    if eerste_tabel:
        columns_to_parse_simple = [
            "match_nodes",
            "from_node_id",
            "to_node_id",
            "link_id",
            "meta_edge_id_waterbeheerder",
        ]
        # Convert the lists in link_id to lists if possible
        for col in columns_to_parse_simple:
            koppeltabel[col] = koppeltabel[col].apply(ParseList)

        for col in ["from_node_types", "to_node_types"]:
            koppeltabel[col] = (
                koppeltabel[col]
                .astype(str)
                .apply(
                    lambda x: (
                        x.replace("['", "").replace("']", "").split("', '")
                        if isinstance(x, str) and x.strip().startswith("[") and x.strip().endswith("]")
                        else x
                    )
                )
            )

        columns_to_parse_geometry = ["from_node_geometry", "to_node_geometry"]
        for col in columns_to_parse_geometry:
            koppeltabel[col] = (
                koppeltabel[col]
                .astype(str)
                .apply(
                    lambda x: (
                        x.replace("[<", "").replace(">]", "").split(">, <")
                        if isinstance(x, str) and x.strip().startswith("[") and x.strip().endswith("]")
                        else x
                    )
                )
            )
            koppeltabel[col] = koppeltabel[col].apply(lambda x: wkt.loads(x) if x != "nan" else None)
        # Parse the geometry
        koppeltabel["geometry"] = koppeltabel["geometry"].apply(lambda x: wkt.loads(x))

    else:
        # Als we met een koppeltabel werken die al een keer
        # omgezet is voor een nieuwe model versie werken we voor het inlezen
        # met andere kolommen.

        columns_to_parse_simple = ["previous_link_id", "new_link_id"]

        # Convert the lists in link_id to lists if possible
        for col in columns_to_parse_simple:
            koppeltabel[col] = koppeltabel[col].apply(ParseList)

        # for col in ["previous_from_node_types", "previous_to_node_types",
        #             "new_from_node_types", "new_to_node_types"]:

        #     koppeltabel[col] = (
        #         koppeltabel[col]
        #         .astype(str)
        #         .apply(
        #             lambda x: (
        #                 x.replace("['", "").replace("']", "").split("', '")
        #                 if isinstance(x, str)
        #                 and x.strip().startswith("[")
        #                 and x.strip().endswith("]")
        #                 else x
        #             )
        #         )
        #     )

        def parse_list_column(series):
            def safe_eval(x):
                if isinstance(x, str):
                    x = x.strip()
                    if x.startswith("[") and x.endswith("]"):
                        try:
                            return ast.literal_eval(x)
                        except (ValueError, SyntaxError):
                            return x
                return x

            return series.apply(safe_eval)

        columns_to_parse = [
            "previous_from_node_types",
            "previous_to_node_types",
            "new_from_node_types",
            "new_to_node_types",
        ]

        for col in columns_to_parse:
            koppeltabel[col] = parse_list_column(koppeltabel[col])

        columns_to_parse_geometry = [
            "geometry",
            "previous_from_node_geometry",
            "previous_to_node_geometry",
            "new_from_node_geometry",
            "new_to_node_geometry",
        ]

        # for col in columns_to_parse_geometry:
        #     koppeltabel[col] = (
        #         koppeltabel[col]
        #         .astype(str)
        #         .apply(
        #             lambda x: (
        #                 x.replace("[<", "").replace(">]", "").split(">, <")
        #                 if isinstance(x, str)
        #                 and x.strip().startswith("[")
        #                 and x.strip().endswith("]")
        #                 else x
        #             )
        #         )
        #     )
        #     koppeltabel[col] = koppeltabel[col].apply(
        #         lambda x: wkt.loads(x) if x not in ['[NONE', "nan"] else None
        #     )

        # # Parse the geometry
        # koppeltabel["geometry"] = koppeltabel["geometry"].apply(lambda x: wkt.loads(x))

        def parse_geometry_value(val):
            if pd.isna(val) or val in ["nan", "None", "[NONE"]:
                return None

            # Ensure string
            val = str(val).strip()

            # Handle list-like strings, e.g. "[None, <POINT ...>]"
            if val.startswith("[") and val.endswith("]"):
                parts = val[1:-1].split(", ")
                result = []
                for p in parts:
                    p = p.strip()
                    if p in ["None", "NONE", "nan", "[NONE"]:
                        result.append(None)
                    else:
                        # Remove angle brackets
                        p = p.strip("<>")
                        try:
                            result.append(wkt.loads(p))
                        except Exception:
                            result.append(None)
                return result

            # Handle single geometry string
            try:
                return wkt.loads(val.strip("<>"))
            except Exception:
                return None

        for col in columns_to_parse_geometry:
            koppeltabel[col] = koppeltabel[col].apply(parse_geometry_value)

    return koppeltabel


# %%

# Import koppeltabel met meest recent verwerkte feedback van waterschappen

koppeltabel = LaadKoppeltabel(loc_ref_koppeltabel, eerste_tabel=eerste_tabel)

# Als we op waterschappen willen filteren:
if filter_waterschappen:
    # filter op waterschappen die in het model zitten
    koppeltabel = koppeltabel[
        [
            elem in waterschapsnaam  # Aangepast
            for elem in koppeltabel.Waterschap
        ]
    ]


# %% import model

# model = Model.read(os.path.join(model_folder, toml_naam)) # Aangepast van lhm.toml

model = Model.read(os.path.join(model_folder_temporary, toml_naam))  # Aangepast van lhm.toml

links = model.link.df


def combine_nodes_from_model(model, node_types):
    temp_df_list = []
    for node_name in node_types:
        attr = getattr(model, node_name, None)
        if attr and hasattr(attr, "node") and hasattr(attr.node, "df"):
            temp_df = attr.node.df
            temp_df_list.append(temp_df)

    return pd.concat(temp_df_list).reset_index()


connector_nodes_df = combine_nodes_from_model(
    model,
    [
        "tabulated_rating_curve",
        "pump",
        "outlet",
        "manning_resistance",
        "linear_resistance",
    ],
)

all_nodes_df = combine_nodes_from_model(
    model,
    [
        "tabulated_rating_curve",
        "pump",
        "outlet",
        "manning_resistance",
        "linear_resistance",
        "basin",
        "level_boundary",
        "flow_boundary",
        "continuous_control",
    ],
)


# %% spatial match

gdf_koppeling = gdf.GeoDataFrame(koppeltabel, geometry="geometry")

gdf_koppeling = gdf_koppeling[["Waterschap", "MeetreeksC", "Aan/Af", "geometry"]]

lhm_model = model

# List of connector nodes
connector_nodes = [
    "tabulated_rating_curve",
    "pump",
    "outlet",
    "manning_resistance",
    "linear_resistance",
]


# Define buffer range and link columns to extract
buffer_range_nodes = np.arange(0, 100, 5).tolist()

buffer_range_links = np.arange(0, 600, 1).tolist()

# Met een gecombineerd model kunnen we de meta_edge_id_waterbeheerder wel meenemen

link_columns = [
    "from_node_id",
    "to_node_id",
    "link_id",
    "meta_edge_id_waterbeheerder",
    "geometry",
]


koppeling_spatial = spatial_match(
    shape_koppeling_path=gdf_koppeling,
    lhm_model=lhm_model,
    connector_nodes=connector_nodes,
    buffer_range_nodes=buffer_range_nodes,
    buffer_range_links=buffer_range_links,
    link_columns=link_columns,
    apply_mapping=False,
    write_buffer_shp=True,
    output_buffer_shapefile=cloud.joinpath(
        base, "suggestie_automatische_koppeling", f"Buffers_match_metingen_v{versie}.gpkg"
    ),
    # output_buffer_shapefile=os.path.join(loc_model, "Koppeltabellen updaten", "suggestie_automatische_koppeling", "Buffers_match_metingen.shp")
    #!!TODO: uploaden naar de cloud op de juiste manier
    cloud_sync=cloud,
    filter_waterschappen=filter_waterschappen,
    lijst_filter_waterschappen=waterschapsnaam,
)

# %%

#!!TODO: uploaden naar de cloud op de juiste manier

# Wegschrijven van de koppeltabel die automatisch gemaakt wordt op basis van de geometry van de meetpunten

path_excel = cloud.joinpath(base, "suggestie_automatische_koppeling", f"Automatische_spatialmatch_v{versie}.xlsx")

koppeling_spatial.to_excel(path_excel, index=False)
cloud.upload_file(path_excel)


koppeling_spatial.set_crs(epsg=28992, inplace=True)

path_gpkg = cloud.joinpath(base, "suggestie_automatische_koppeling", f"Automatische_spatialmatch_v{versie}.gpkg")
koppeling_spatial.to_file(path_gpkg, driver="GPKG")
cloud.upload_file(path_gpkg)

# %%
# ---------------------------------------------
# Step 1. Drop unnecessary columns
# ---------------------------------------------
drop_cols = ["status", "match_nodes", "meta_edge_id_waterbeheerder", "from_node_id", "to_node_id"]
koppeltabel = koppeltabel.drop(columns=[c for c in drop_cols if c in koppeltabel.columns])

# ---------------------------------------------
# Step 2. Check if "previous" columns exist
# ---------------------------------------------
previous_cols = [
    "previous_from_node_geometry",
    "previous_to_node_geometry",
    "previous_from_node_types",
    "previous_to_node_types",
    "previous_link_id",
]

new_cols = [
    "new_from_node_geometry",
    "new_to_node_geometry",
    "new_from_node_types",
    "new_to_node_types",
    "new_link_id",
    "opmerking_transform",
]

if any(col in koppeltabel.columns for col in previous_cols):
    # Case A: previous already exists -> move current new_ → previous_
    for pcol, ncol in zip(previous_cols, new_cols[:-1]):  # skip opmerking_transform
        if ncol in koppeltabel.columns:
            koppeltabel[pcol] = koppeltabel[ncol]
    # Reinitialize fresh new_* columns
    for col in new_cols:
        koppeltabel[col] = None
else:
    # Case B: first run -> rename originals to previous_*
    rename_map = {
        "from_node_geometry": "previous_from_node_geometry",
        "to_node_geometry": "previous_to_node_geometry",
        "from_node_types": "previous_from_node_types",
        "to_node_types": "previous_to_node_types",
        "link_id": "previous_link_id",
    }
    koppeltabel = koppeltabel.rename(columns={k: v for k, v in rename_map.items() if k in koppeltabel.columns})
    # Initialize new_* columns
    for col in new_cols:
        koppeltabel[col] = None

# %%
# ---------------------------------------------
# Step 3. Your main loop
# ---------------------------------------------
for index, row in koppeltabel.iterrows():
    # print(row["MeetreeksC"])

    # if row["MeetreeksC"] == "Stieltjeskanaalsluis vechtstromen":

    # --- always read from previous_* ---
    prev_from_geom = row.get("previous_from_node_geometry")
    # print(prev_from_geom)
    prev_to_geom = row.get("previous_to_node_geometry")
    # print(prev_to_geom)
    prev_from_types = row.get("previous_from_node_types")
    # print(prev_from_types)
    prev_to_types = row.get("previous_to_node_types")
    # print(prev_to_types)
    prev_link_id = row.get("previous_link_id")
    # print(prev_link_id)

    # als geen koppeling in koppeltabel, zoek kopelling op basis van spatial koppeling
    if prev_from_geom is None:
        koppeltabel.at[index, "new_from_node_geometry"] = koppeling_spatial.loc[index, "from_node_geometry"]

        koppeltabel.at[index, "new_to_node_geometry"] = koppeling_spatial.loc[index, "to_node_geometry"]

        koppeltabel.at[index, "new_from_node_types"] = koppeling_spatial.loc[index, "from_node_types"]
        koppeltabel.at[index, "new_to_node_types"] = koppeling_spatial.loc[index, "to_node_types"]

        koppeltabel.at[index, "new_link_id"] = koppeling_spatial.loc[index, "link_id"]

        if koppeling_spatial.at[index, "link_id"] is None:
            koppeltabel.loc[index, "opmerking_transform"] = (
                "Geen originele koppeling en geen spatial koppeling gevonden met nieuwe model"
            )
        else:
            koppeltabel.at[index, "opmerking_transform"] = "Spatial koppeling meegegeven want geen originele koppeling"

    # als wel een koppeling in koppeltabel, zoek nieuwe ID van koppelelement
    else:
        from_nodes_geom = []
        to_nodes_geom = []
        from_node_types = []
        to_node_types = []
        from_nodes_id = []
        to_nodes_id = []
        link_id = []
        # meta_edge_id_waterbeheerder = []
        opmerking_transform = []

        # loop per rij over de verschillende links (combi van from en to nodes)

        for from_node, to_node, from_type, to_type in zip(prev_from_geom, prev_to_geom, prev_from_types, prev_to_types):
            # default values for this iteration
            # found_from_nodes_geom = None
            # found_to_nodes_geom = None
            # found_from_node_types = None
            # found_to_node_types = None
            # from_node_id = None
            # to_node_id = None
            # found_link_id = None
            message = ""

            # reset per-iteration
            node_to_match = None
            node_type_to_match = None
            search_in = None

            # als zowel from en to nodes connector nodes, dan is dat gek, even kijken wat dan doen voor nu alleen message printen
            if from_type in connector_nodes_df.node_type.unique() and to_type in connector_nodes_df.node_type.unique():
                print("from and to nodes both connector nodes: ", index)

            # als from node connector node, zoeken naar dan zoeken op de from-node
            elif from_type in connector_nodes_df.node_type.unique():
                node_to_match = from_node
                node_type_to_match = from_type
                search_in = "from_node_"

            # als to node connector node, zoeken naar dan zoeken op de to-node
            elif to_type in connector_nodes_df.node_type.unique():
                node_to_match = to_node
                node_type_to_match = to_type
                search_in = "to_node_"

            else:
                print(
                    "node_to_match niet overschreven: ",
                    index,
                    from_node,
                    to_node,
                    from_type,
                    to_type,
                )

            found_nodes = None
            found_nodes_all_types = None

            # zoekrange rondom originele node
            if node_to_match is not None:
                zoek_buffer = node_to_match.buffer(3)

                # alle nodes die rondom originele koppelnode liggen
                found_nodes_all_types = connector_nodes_df[connector_nodes_df.geometry.within(zoek_buffer)]

                # nieuwe koppelnode prioriteren op hetzelfde type als originele node
                found_nodes = found_nodes_all_types[found_nodes_all_types["node_type"] == node_type_to_match]

                # als meer dan 1 node met hetzelfde type gevonden, dan printen
                if len(found_nodes) > 1:
                    print("meer dan 1 node gevonden: ", index, node_to_match)
                    print(found_nodes)

                elif found_nodes.empty and found_nodes_all_types.empty:
                    print("geen match gevonden: ", index, node_to_match, node_type_to_match)
                    from_nodes_geom.append(None)
                    to_nodes_geom.append(None)
                    from_node_types.append(None)
                    to_node_types.append(None)
                    from_nodes_id.append(None)
                    to_nodes_id.append(None)
                    link_id.append(None)
                    # meta_edge_id_waterbeheerder.append(None)
                    opmerking_transform.append(",geen match gevonden,")

                    continue

                # als geen node gevonden op de locatie met hetzelfde type, maar wel nodes gevonden met andere type, voeg ander type toe
                if found_nodes.empty and not found_nodes_all_types.empty:
                    print(
                        "match gevonden met andere type: ",
                        index,
                        node_to_match,
                        node_type_to_match,
                    )
                    found_nodes = found_nodes_all_types
                    message = " >andere type gevonden<"

                # als 1 node gevonden, dan zoeken naar link
                if len(found_nodes) > 1:
                    message = message + " >meer dan 1 node gevonden - eerste gepakt<"

                # Vanuit node kijken welke link ervanaf of naartoe gaat. afhankelijk of connector from or to was
                found_node_id = found_nodes.iloc[0].node_id
                found_link = links[links[search_in + "id"] == found_node_id]

                # We doen nog een filter slag dat we met gevonden links niet ineens
                # Control links meenemen in het zoeken
                found_link = found_link[found_link["link_type"] != "control"]

                if len(found_link) > 1:
                    print("meer dan 1 link gevonden: ", index, node_to_match)
                    message = message + ">meer dan 1 link gevonden obv node - eerste gepakt<"

                    print(found_link)

                found_link_id = found_link.index[0]
                from_node_id = found_link.from_node_id.iloc[0]
                to_node_id = found_link.to_node_id.iloc[0]

                found_from_nodes_geom = all_nodes_df[all_nodes_df.node_id == from_node_id].geometry.iloc[0]
                found_to_nodes_geom = all_nodes_df[all_nodes_df.node_id == to_node_id].geometry.iloc[0]
                found_from_node_types = all_nodes_df[all_nodes_df.node_id == from_node_id].node_type.iloc[0]
                found_to_node_types = all_nodes_df[all_nodes_df.node_id == to_node_id].node_type.iloc[0]
                print(found_link_id)

                from_nodes_geom.append(found_from_nodes_geom)

                to_nodes_geom.append(found_to_nodes_geom)

                from_node_types.append(found_from_node_types)

                to_node_types.append(found_to_node_types)

                from_nodes_id.append(from_node_id)

                to_nodes_id.append(to_node_id)

                link_id.append(int(found_link_id))
                # print(link_id)

                opmerking_transform.append("found match" + message)

            # als helemaal geen node gevonden op zelfde locatie, dan printen en wegschrijven in tabel
            # elif found_nodes.empty and found_nodes_all_types.empty:
            elif (found_nodes is None or found_nodes.empty) and (
                found_nodes_all_types is None or found_nodes_all_types.empty
            ):
                print("geen match gevonden: ", index, node_to_match, node_type_to_match)
                # from_nodes_geom = []
                from_nodes_geom.append(None)
                to_nodes_geom.append(None)
                from_node_types.append(None)
                to_node_types.append(None)
                from_nodes_id.append(None)
                to_nodes_id.append(None)
                link_id.append(None)
                # meta_edge_id_waterbeheerder.append(None)
                opmerking_transform.append(",geen match gevonden,")

                continue

        koppeltabel.at[index, "new_from_node_geometry"] = from_nodes_geom
        koppeltabel.at[index, "new_to_node_geometry"] = to_nodes_geom
        koppeltabel.at[index, "new_from_node_types"] = from_node_types
        koppeltabel.at[index, "new_to_node_types"] = to_node_types
        koppeltabel.at[index, "new_link_id"] = link_id
        koppeltabel.at[index, "opmerking_transform"] = opmerking_transform

        # Als we wel een geometry hadden opgeslagen in de input koppeltabel, maar we kunnen in de buurt in het nieuwe model
        # geen connector knoopjes vinden dan kijken we alsnog naar de spatial koppeling.

        # Dit werk zowel voor None als voor een lege lijst zoals dat nu wordt weggeschreven
        # Als er geen match is gevonden eerder.

        if nieuwe_suggestie_als_oude_geometry_ontbreekt:
            # if not koppeltabel.at[index, "new_from_node_geometry"]:

            # catches NaN and catches [None] or [None, None, ...]
            value = koppeltabel.at[index, "new_from_node_geometry"]

            if (
                value is None
                or (isinstance(value, float) and pd.isna(value))
                or (isinstance(value, list) and all(v is None for v in value))
            ):
                koppeltabel.at[index, "new_from_node_geometry"] = koppeling_spatial.loc[index, "from_node_geometry"]

                koppeltabel.at[index, "new_to_node_geometry"] = koppeling_spatial.loc[index, "to_node_geometry"]

                koppeltabel.at[index, "new_from_node_types"] = koppeling_spatial.loc[index, "from_node_types"]

                koppeltabel.at[index, "new_to_node_types"] = koppeling_spatial.loc[index, "to_node_types"]

                koppeltabel.at[index, "new_link_id"] = koppeling_spatial.loc[index, "link_id"]

                if koppeling_spatial.at[index, "link_id"] is None:
                    koppeltabel.loc[index, "opmerking_transform"] = (
                        "Geen originele koppeling en geen spatial koppeling gevonden met nieuwe model"
                    )

                else:
                    koppeltabel.at[index, "opmerking_transform"] = (
                        "Spatial koppeling meegegeven want geen connector node geometrie in de buurt gevonden"
                    )

        else:
            continue


# %%
##########################
# Wegschrijven koppeltabel
##########################


#!!TODO: uploaden naar de cloud op de juiste manier

# koppeltabel.to_csv(wegschrijven_nieuwe_tabel, index=False)
koppeltabel.to_excel(wegschrijven_nieuwe_tabel, index=False)

cloud.upload_file(wegschrijven_nieuwe_tabel)


gdf = gpd.GeoDataFrame(koppeltabel, geometry="geometry")

# Set CRS (e.g., EPSG:28992 for RD New, modify as needed)
gdf.set_crs(epsg=28992, inplace=True)  # Update EPSG code based on your data's CRS

output_path = os.path.splitext(wegschrijven_nieuwe_tabel)[0] + ".gpkg"

gdf.to_file(output_path, layer="Koppeling_model_meting", driver="GPKG")

cloud.upload_file(output_path)

# %%
