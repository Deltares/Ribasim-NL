# %%

import os

import geopandas as gpd
import pandas as pd

# Packages toegevoegd voor om progress spatial match te tracken
from tqdm import tqdm


#####################################################################################
def search_geometry_nodes(lhm_model, node_id):
    """
    Search for a single node geometry in all attributes of lhm_model that contain 'node' and 'df'.

    Parameters
    ----------
    - lhm_model (Model): The Ribasim model containing various node data.
    - node_id (int or str): A single node ID to search for.

    Returns
    -------
    - geometry (object): Geometry of the node if found, else None.
    """
    # TODO: Hier wil je eigenlijk components toevoegen waarbinnen je de geometrie zoekt
    # Voor nu triggert de loop een FileNotFoundError als we de results_dir niet hebben en
    # Wel deze attribute aanroepen met getattr.

    component_names = [
        "allocation",
        "basin",
        "continuous_control",
        "discrete_control",
        "flow_boundary",
        "flow_demand",
        "junction",
        "level_boundary",
        "level_demand",
        "linear_resistance",
        "link",
        "manning_resistance",
        "node_table",
        "outlet",
        "pid_control",
        "pump",
        "tabulated_rating_curve",
        "terminal",
        "user_demand",
    ]

    components_to_search = [
        getattr(lhm_model, name, None) for name in component_names if getattr(lhm_model, name, None) is not None
    ]

    for component in components_to_search:
        if hasattr(component, "node") and hasattr(component.node, "df"):
            # print(component)

            node_df = component.node.df.copy().reset_index()

            if "node_id" in node_df.columns and "geometry" in node_df.columns:
                matched_node = node_df[node_df["node_id"] == node_id]

                if not matched_node.empty:
                    return matched_node.iloc[0]["geometry"]

    return None

    ###########################
    # Dit stukje code hieronder vervangen door de code hierboven om specifieker de geometry
    # op te zoeken
    ###########################

    # for attr_name in dir(lhm_model):
    #     print(attr_name)

    #     attr = getattr(lhm_model, attr_name, None)
    #     if hasattr(attr, "node") and hasattr(attr.node, "df"):
    #         node_df = attr.node.df.copy()
    #         node_df = (
    #             node_df.reset_index()
    #         )  # Ensure index is reset so node_id is a column

    #         if "node_id" in node_df.columns and "geometry" in node_df.columns:
    #             matched_node = node_df[node_df["node_id"] == node_id]
    #             if not matched_node.empty:
    #                 # ff checken to wkt lukt nog niet
    #                 return matched_node.iloc[0]["geometry"]
    #                 # return matched_node.iloc[0]["geometry"].to_wkt()  # Return the geometry of the first match

    # return None  # Return None if not found


#####################################################################################


def search_type_nodes(lhm_model, node_id):
    """
    Search for a single node type in all attributes of lhm_model that contain 'node' and 'df'.

    Parameters
    ----------
    - lhm_model (Model): The Ribasim model containing various node data.
    - node_id (int or str): A single node ID to search for.

    Returns
    -------
    - node_type from attr.node.df
    """
    component_names = [
        "allocation",
        "basin",
        "continuous_control",
        "discrete_control",
        "flow_boundary",
        "flow_demand",
        "junction",
        "level_boundary",
        "level_demand",
        "linear_resistance",
        "link",
        "manning_resistance",
        "node_table",
        "outlet",
        "pid_control",
        "pump",
        "tabulated_rating_curve",
        "terminal",
        "user_demand",
    ]

    components_to_search = [
        getattr(lhm_model, name, None) for name in component_names if getattr(lhm_model, name, None) is not None
    ]

    for component in components_to_search:
        if hasattr(component, "node") and hasattr(component.node, "df"):
            # print(component)

            node_df = component.node.df.copy().reset_index()

            if "node_id" in node_df.columns and "node_type" in node_df.columns:
                matched_node = node_df[node_df["node_id"] == node_id]

                if not matched_node.empty:
                    return matched_node.iloc[0]["node_type"]

    # for attr_name in dir(lhm_model):
    #     attr = getattr(lhm_model, attr_name, None)
    #     if hasattr(attr, "node") and hasattr(attr.node, "df"):
    #         node_df = attr.node.df.copy()
    #         node_df = (
    #             node_df.reset_index()
    #         )  # Ensure index is reset so node_id is a column

    #         if "node_id" in node_df.columns and "node_type" in node_df.columns:
    #             matched_node = node_df[node_df["node_id"] == node_id]
    #             if not matched_node.empty:
    #                 return matched_node.iloc[0][
    #                     "node_type"
    #                 ]  # Return the node type of the first match

    # return None  # Return None if not found


#####################################################################################


def filter_for_waterboard(lhm_model, connector_nodes, links_gdf, waterboard_name):
    """
    Filter connector nodes and links for a specific waterboard.

    Parameters
    ----------
    - lhm_model (Model): The Ribasim model containing connector nodes and links.
    - connector_nodes (list): List of connector nodes to process.
    - links_gdf (GeoDataFrame): GeoDataFrame containing links with geometry and node IDs.
    - waterboard_name (str): Name of the waterboard to filter data for.

    Returns
    -------
    - filtered_connector_gdfs (dict): Dictionary of filtered connector GeoDataFrames for each node.
    - region_links (GeoDataFrame): Filtered links GeoDataFrame for the specified waterboard.
    """
    filtered_connector_gdfs = {}

    # Filter connector nodes for the specified waterboard
    for node_name in connector_nodes:
        attr = getattr(lhm_model, node_name, None)
        if attr and hasattr(attr, "node") and hasattr(attr.node, "df"):
            connector_df = attr.node.df
            if "geometry" in connector_df.columns and "meta_waterbeheerder" in connector_df.columns:
                # Filter the connector_df by the current waterboard if information is available
                if not connector_df["meta_waterbeheerder"].isna().all():
                    filtered_connector_gdf = connector_df[connector_df["meta_waterbeheerder"] == waterboard_name]

                    if not filtered_connector_gdf.empty:
                        filtered_connector_gdf = filtered_connector_gdf.reset_index()

                    else:
                        # met het lhm_ctwq_compat is in meta_waterbeheerder
                        # alleen "Rijkswaterstaat", we zoeken dus niet meer binnen de
                        # specfieke waterschaps nodes en linkjes.

                        filtered_connector_gdf = connector_df.reset_index()

                    filtered_connector_gdfs[node_name] = filtered_connector_gdf

                else:
                    filtered_connector_gdfs[node_name] = connector_df.reset_index()
            else:
                filtered_connector_gdfs[node_name] = gpd.GeoDataFrame()  # Empty GeoDataFrame if missing columns

    # Filter links for the specified waterboard
    region_links = gpd.GeoDataFrame()
    for node_name, filtered_connector_df in filtered_connector_gdfs.items():
        if not filtered_connector_df.empty:
            # Check for links connected to the nodes in this connector_df
            node_ids = filtered_connector_df["node_id"].unique()
            links_to = links_gdf[links_gdf["to_node_id"].isin(node_ids)]
            links_from = links_gdf[links_gdf["from_node_id"].isin(node_ids)]

            # Concatenate these links into the regional links_gdf
            region_links = pd.concat([region_links, links_to, links_from]).drop_duplicates()

    return filtered_connector_gdfs, region_links


#####################################################################################
def filter_only_flow_links(region_links):
    """
    Filter the region_links GeoDataFrame to include only rows where link_type is "flow".

    Parameters
    ----------
    - region_links (GeoDataFrame): GeoDataFrame containing link information.

    Returns
    -------
    - GeoDataFrame: Filtered GeoDataFrame containing only "flow" links.
    """
    if "link_type" not in region_links.columns:
        raise ValueError("The region_links DataFrame does not have a 'link_type' column.")

    # Filter for rows where link_type is "flow"
    flow_links = region_links[region_links["link_type"] == "flow"]

    return flow_links


#####################################################################################
def filter_connector_nodes_and_links_aan_af(filtered_connector_gdfs, region_links, lhm_model, aan_af):
    """
    Filter connector nodes and region links based on "Aanvoer" or "Afvoer" status.

    Parameters
    ----------
    - filtered_connector_gdfs (dict): Dictionary of filtered connector GeoDataFrames.
    - region_links (GeoDataFrame): GeoDataFrame containing regional links.
    - lhm_model (Model): The LHM Ribasim model containing static tables for each connector type.
    - aan_af (str): Either "Aanvoer" or "Afvoer" indicating the required filtering.

    Returns
    -------
    - dict: Updated filtered_connector_gdfs after applying the filtering.
    - GeoDataFrame: Updated region_links after applying the filtering.
    """
    filtered_connector_gdfs_new = {}
    filtered_region_links = region_links.copy()

    for key, connector_gdf in filtered_connector_gdfs.items():
        if connector_gdf.empty:
            filtered_connector_gdfs_new[key] = connector_gdf
            continue

        # Check if the static table for this connector exists and contains "meta_func_afvoer"
        static_table = getattr(lhm_model, key, None)
        if static_table and hasattr(static_table, "static") and hasattr(static_table.static, "df"):
            static_df = static_table.static.df
            if "meta_func_afvoer" in static_df.columns:
                # Determine required filtering logic
                if aan_af == "Aanvoer":
                    required_func_value = 0
                    valid_node_ids = static_df[static_df["meta_func_afvoer"] == required_func_value]["node_id"].unique()

                elif aan_af == "Afvoer":
                    required_func_value = 1
                    valid_node_ids = static_df[static_df["meta_func_afvoer"] == required_func_value]["node_id"].unique()

                else:
                    valid_node_ids = static_df["node_id"].unique()  # Include all nodes

                connector_gdf = connector_gdf[connector_gdf["node_id"].isin(valid_node_ids)]

        # Update the filtered_connector_gdfs with the newly filtered GeoDataFrame
        filtered_connector_gdfs_new[key] = connector_gdf

    # Filter the region_links based on the updated filtered_connector_gdfs
    # lijst maken van alle

    # Filter links for the specified waterboard

    valid_node_ids = [
        node_id
        for connector_gdf in filtered_connector_gdfs_new.values()
        if not connector_gdf.empty
        for node_id in connector_gdf["node_id"].unique()
    ]

    filtered_region_links = filtered_region_links[
        (filtered_region_links["from_node_id"].isin(valid_node_ids))
        | (filtered_region_links["to_node_id"].isin(valid_node_ids))
    ]

    return filtered_connector_gdfs_new, filtered_region_links


def match_with_connector_nodes(row, filtered_connector_gdfs, region_links, buffer_range_nodes, link_columns):
    """
    Match measurement points with connector nodes using buffers.

    Parameters
    ----------
    - row (GeoSeries): Current measurement point row.
    - filtered_connector_gdfs (dict): Filtered connector GeoDataFrames.
    - region_links (GeoDataFrame): GeoDataFrame of region links.
    - buffer_range_nodes (list): Buffer sizes for connector node matching.
    - link_columns (list): Columns to extract from the links GeoDataFrame of the model.

    Returns
    -------
    - dict: Contains matched_nodes, matched_links, match_found, and matched_buffer.
    """
    matched_nodes = []
    matched_links = {col: [] for col in link_columns}
    matched_buffer = None
    match_found = False

    for buffer_size in buffer_range_nodes:
        # Create a buffer around the measurement point
        measurement_buffer = row.geometry.buffer(buffer_size)

        for node_name, filtered_connector_df in filtered_connector_gdfs.items():
            if not filtered_connector_df.empty:
                connector_gdf = gpd.GeoDataFrame(filtered_connector_df, geometry="geometry")

                # Find connector nodes within the buffer
                # max range toepassen en vervolgens de dichtsbijzijnde zoeken
                # joinen op min distance --> sjoin_nearest geopandas.

                nearby_connectors = connector_gdf[connector_gdf.geometry.within(measurement_buffer)]
                if not nearby_connectors.empty:
                    # Append matched connector nodes
                    matched_nodes.extend(nearby_connectors["node_id"].tolist())

                    # Handle "Aan/Afvoer" logic
                    if row["Aan/Af"] == "Aanvoer":
                        links = region_links[region_links["to_node_id"].isin(nearby_connectors["node_id"])]
                    elif row["Aan/Af"] == "Afvoer":
                        links = region_links[region_links["from_node_id"].isin(nearby_connectors["node_id"])]
                    else:
                        links = gpd.GeoDataFrame()  # Empty if "Aan/Af" is missing

                        # als we dus een geval hebben zonder Aanvoer of Afvoer column dan zetten we deze naar een lege list matched nodes
                        # eerst nam die bijv voor de ericasluizen alle knoopjes mee die de kon vinden per geanalyseerde buffer size --> wordt
                        # een lange list.
                        matched_nodes = []

                    # Append link information
                    if not links.empty:
                        links = links.reset_index()  # Resets index to make link_id a column
                        for col in link_columns:
                            matched_links[col].extend(links[col].tolist())
                        match_found = True
                        matched_buffer = {
                            "geometry": measurement_buffer,
                            "buffer_size": buffer_size,
                        }
                        break
        if match_found:
            break

    return {
        "matched_nodes": matched_nodes,
        "matched_links": matched_links,
        "match_found": match_found,
        "matched_buffer": matched_buffer,
    }


def match_with_links(row, region_links, buffer_range_links, link_columns):
    """
    Match measurement points with region links using buffers.

    Parameters
    ----------
    - row (GeoSeries): Current measurement point row.
    - region_links (GeoDataFrame): GeoDataFrame of region links.
    - buffer_range_links (list): Buffer sizes for link matching.
    - link_columns (list): Columns to extract from the links GeoDataFrame of the model.

    Returns
    -------
    - dict: Contains matched_links, match_found, and matched_buffer.
    """
    matched_links = {col: [] for col in link_columns}
    matched_buffer = None
    match_found = False

    for buffer_size in buffer_range_links:
        # Create a buffer around the measurement point
        measurement_buffer = row.geometry.buffer(buffer_size)

        # Find links intersecting with the buffer
        # ook gebruiken sjoin_nearest
        nearby_links = region_links[region_links.geometry.intersects(measurement_buffer)]
        if not nearby_links.empty:
            # Ensure link_id is included in the output
            nearby_links = nearby_links.reset_index()  # Resets index to make link_id a column
            for col in link_columns:
                matched_links[col].extend(nearby_links[col].tolist())
            matched_buffer = {
                "geometry": measurement_buffer,
                "buffer_size": buffer_size,
            }
            match_found = True
            break

    return {
        "matched_links": matched_links,
        "match_found": match_found,
        "matched_buffer": matched_buffer,
    }


def spatial_match(
    shape_koppeling_path,
    lhm_model,
    connector_nodes,
    buffer_range_nodes,
    buffer_range_links,
    link_columns,
    apply_mapping=False,
    waterschap_mapping=None,
    write_buffer_shp=False,
    output_buffer_shapefile=None,
    cloud_sync=None,
    filter_waterschappen=False,
    lijst_filter_waterschappen=None,
):
    """
    Perform spatial matching between measurement points and connector nodes or links.

    Parameters
    ----------
    - shape_koppeling_path (str): Path to the shapefile with measurement points.
    - lhm_model (Model): The Ribasim model containing connector nodes and links.
    - connector_nodes (list): List of connector nodes to process.
    - buffer_range_nodes (list): List of buffer sizes to iterate through for connector node matching.
    - buffer_range_links (list): List of buffer sizes to iterate through for link matching.
    - link_columns (list): List of columns to extract from the links GeoDataFrame.
    - apply_mapping (bool): True or false when a mapping on the names of the waterschappen should be applied
    - waterschap_mapping (dict): dict with the mapping of the names that should be changed
    - write_buffer_shp (bool): True or False to write a buffer shapefile
    - output_buffer_shapefile (str): Path to save the shapefile of matched measurement buffers.
    - filter_waterschappen (bool): True or False to filter if you have a model of a specific waterboard/waterboards.
    - lijst_filter_waterschappen (list): List of waterschappen to filter

    Returns
    -------
    - Updated GeoDataFrame with matching results.
    """
    # Load measurement points shapefile
    if isinstance(shape_koppeling_path, str):
        shape_koppeling = gpd.read_file(shape_koppeling_path)
    elif isinstance(shape_koppeling_path, gpd.GeoDataFrame):
        shape_koppeling = shape_koppeling_path
    else:
        raise ValueError("shape_koppeling_path must be a string or a GeoDataFrame")

    # Verwijderen locaties zonder geometry
    shape_koppeling.dropna(subset=["geometry"], inplace=True)

    if apply_mapping:
        shape_koppeling["Waterschap"] = shape_koppeling["Waterschap"].map(waterschap_mapping)

    # Filter if you have a model of a specific waterboard/waterboards and therefore also only
    # Want to filter for the measurement locations of that specific waterboard
    if filter_waterschappen:
        shape_koppeling = shape_koppeling[shape_koppeling["Waterschap"].isin(lijst_filter_waterschappen)]

    # Check if "Aan/Af" column exists
    has_aan_af_column = "Aan/Af" in shape_koppeling.columns

    # Initialize columns to store results
    shape_koppeling["status"] = "Not Found"
    shape_koppeling["match_nodes"] = None  # List of matched nodes
    shape_koppeling["from_node_geometry"] = None
    shape_koppeling["to_node_geometry"] = None
    shape_koppeling["from_node_types"] = None
    shape_koppeling["to_node_types"] = None

    # Initialize separate columns for each link column
    for col in link_columns:
        shape_koppeling[f"Link_{col}"] = None

    # Get the links GeoDataFrame from the model
    links_gdf = gpd.GeoDataFrame(lhm_model.link.df, geometry="geometry")

    # Store matched buffer geometries and sizes
    matched_buffers = []

    # Iterate through each point
    for idx, row in tqdm(
        shape_koppeling.iterrows(), total=len(shape_koppeling), desc="🔄 Verwerken ruimtelijke koppeling van meetpunten"
    ):
        # if idx == 1:

        # print(idx)
        # print(row)

        match_found = False
        matched_nodes = []
        matched_links = {col: [] for col in link_columns}

        # Use the filter_for_waterboard function
        filtered_connector_gdfs, region_links = filter_for_waterboard(
            lhm_model, connector_nodes, links_gdf, row["Waterschap"]
        )

        # Filter region_links to include only "flow" links
        region_links = filter_only_flow_links(region_links)

        # Determine whether we are dealing with "Aanvoer" or "Afvoer"
        # obv de meta kolom "meta_func_afvoer" --> gevuld voor peilgestuurd door HKV
        aan_af_status = row["Aan/Af"]
        if has_aan_af_column:
            filtered_connector_gdfs, region_links = filter_connector_nodes_and_links_aan_af(
                filtered_connector_gdfs, region_links, lhm_model, aan_af_status
            )
        # Method 1: Match with connector nodes using buffer_range_nodes

        method1_results = match_with_connector_nodes(
            row, filtered_connector_gdfs, region_links, buffer_range_nodes, link_columns
        )
        matched_nodes = method1_results["matched_nodes"]
        matched_links = method1_results["matched_links"]
        match_found = method1_results["match_found"]

        if match_found:
            shape_koppeling.at[idx, "status"] = "Found, via connector nodes"
            matched_buffers.append(method1_results["matched_buffer"])

        # for buffer_size in buffer_range_nodes:
        #     # Create a buffer around the measurement point
        #     measurement_buffer = row.geometry.buffer(buffer_size)

        #     for node_name, filtered_connector_df in filtered_connector_gdfs.items():
        #         if not filtered_connector_df.empty:
        #             connector_gdf = gpd.GeoDataFrame(filtered_connector_df, geometry="geometry")

        #             # Find connector nodes within the buffer
        #             nearby_connectors = connector_gdf[connector_gdf.geometry.within(measurement_buffer)]
        #             if not nearby_connectors.empty:
        #                 # Append matched connector nodes
        #                 matched_nodes.extend(nearby_connectors["node_id"].tolist())

        #                 # Handle "Aan/Afvoer" logic
        #                 if row["Aan/Af"] == "Aanvoer":
        #                     links = region_links[region_links["to_node_id"].isin(nearby_connectors["node_id"])]
        #                 elif row["Aan/Af"] == "Afvoer":
        #                     links = region_links[region_links["from_node_id"].isin(nearby_connectors["node_id"])]
        #                 else:
        #                     links = gpd.GeoDataFrame()  # Empty in case of missing "Aan/Af"

        #                 # Append link information
        #                 if not links.empty:
        #                     links = links.reset_index()  # Resets index to make link_id a column
        #                     for col in link_columns:
        #                         matched_links[col].extend(links[col].tolist())
        #                     match_found = True
        #                     shape_koppeling.at[idx, "status"] = "Found, via connector nodes"
        #                     matched_buffers.append({"geometry": measurement_buffer, "buffer_size": buffer_size})
        #                     break
        #     if match_found:
        #         break

        # Method 2: Match with links using buffer_range_links
        if not match_found:
            method2_results = match_with_links(row, region_links, buffer_range_links, link_columns)
            matched_links = method2_results["matched_links"]
            match_found = method2_results["match_found"]

            if match_found:
                shape_koppeling.at[idx, "status"] = "Found, via link overlap"
                matched_buffers.append(method2_results["matched_buffer"])

        # if not match_found:
        #     for buffer_size in buffer_range_links:
        #         measurement_buffer = row.geometry.buffer(buffer_size)

        #         # Use the filtered region_links for searching
        #         nearby_links = region_links[region_links.geometry.intersects(measurement_buffer)]
        #         if not nearby_links.empty:
        #             # Ensure link_id is included in the output
        #             nearby_links = nearby_links.reset_index()  # Resets index to make link_id a column
        #             for col in link_columns:
        #                 matched_links[col].extend(nearby_links[col].tolist())
        #             shape_koppeling.at[idx, "status"] = "Found, via link overlap"
        #             matched_buffers.append({"geometry": measurement_buffer, "buffer_size": buffer_size})
        #             match_found = True
        #             break

        # Store results
        # Als er op connector knoop is gematch, deze opnemen in de tabel
        shape_koppeling.at[idx, "match_nodes"] = matched_nodes if matched_nodes else None

        # Alle Link info meenemen in de tabel
        for col in link_columns:
            shape_koppeling.at[idx, f"Link_{col}"] = matched_links[col] if matched_links[col] else None

        # Extract from_node_id and to_node_id from matched links (if found)
        from_node_ids = matched_links.get("from_node_id", [])
        # print(from_node_ids)
        to_node_ids = matched_links.get("to_node_id", [])
        # print(to_node_ids)

        # Fetch geometries in list format
        from_node_geometries = [search_geometry_nodes(lhm_model, node_id) for node_id in from_node_ids]

        to_node_geometries = [search_geometry_nodes(lhm_model, node_id) for node_id in to_node_ids]

        # verkrijgen node types in lijst
        from_node_types = [search_type_nodes(lhm_model, node_id) for node_id in from_node_ids]
        to_node_types = [search_type_nodes(lhm_model, node_id) for node_id in to_node_ids]

        # print(from_node_geometries)
        # print(to_node_geometries)

        # Store geometries in shape_koppeling
        shape_koppeling.at[idx, "from_node_geometry"] = from_node_geometries if from_node_geometries else None
        shape_koppeling.at[idx, "to_node_geometry"] = to_node_geometries if to_node_geometries else None

        shape_koppeling.at[idx, "from_node_types"] = from_node_types if from_node_types else None
        shape_koppeling.at[idx, "to_node_types"] = to_node_types if to_node_types else None

    # Rename columns back to original names except for Link_geometry
    rename_columns = {f"Link_{col}": col for col in link_columns if col != "geometry"}
    shape_koppeling = shape_koppeling.rename(columns=rename_columns)

    # Save matched buffers as a shapefile
    if matched_buffers and write_buffer_shp and output_buffer_shapefile and cloud_sync:
        buffer_gdf = gpd.GeoDataFrame(matched_buffers, geometry="geometry", crs=shape_koppeling.crs)
        os.makedirs(os.path.dirname(output_buffer_shapefile), exist_ok=True)
        buffer_gdf.to_file(output_buffer_shapefile)
        buffer_gdf.to_file(output_buffer_shapefile)
        cloud_sync.upload_file(output_buffer_shapefile)

    return shape_koppeling


# %%
