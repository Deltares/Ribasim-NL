# import packages and functions
import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd


def read_gpkg_layers(gpkg_path, variables, engine="fiona", print_var=False):
    """
    Read specified layers from a GeoPackage (GPKG) file and return them as a dictionary.

    Parameters
    ----------
        gpkg_path (str): The file path to the GeoPackage (GPKG) file to read from.
        variables (list): A list of layer names to read from the GeoPackage.
        print_var (bool, optional): If True, print the name of each variable as it is read. Default is False.

    Returns
    -------
        dict: A dictionary containing the GeoDataFrames, with layer names as keys.

    This function reads specified layers from a GeoPackage (GPKG) file and returns them as a dictionary. You can
    choose to print the names of variables as they are read by setting `print_var` to True.
    """
    data = {}
    for variable in variables:
        if print_var:
            print(variable)
        data_temp = gpd.read_file(gpkg_path, layer=variable, engine=engine)
        data[variable] = data_temp

    return data


def show_layers_and_columns(waterschap):
    """
    Display Information About Layers and Columns in a Geospatial Dataset.

    Parameters
    ----------
        waterschap (dict): A dictionary containing geospatial datasets as GeoDataFrames.

    Returns
    -------
        None

    This function prints the names of all layers and the columns within each layer of a geospatial dataset stored
    in a dictionary.

    """
    for key in waterschap.keys():
        print(key)
        print(waterschap[str(key)].columns.values)
        print("type = ", type(waterschap[str(key)]))
        print("crs = ", waterschap[str(key)].crs)
        print()


def store_data(waterschap, output_gpkg_path):
    """
    Store Geospatial Data to a GeoPackage (GPKG) File.

    Parameters
    ----------
        waterschap (dict): A dictionary containing GeoDataFrames to be stored in the GPKG file.
        output_gpkg_path (str): The file path (including the file name without extension) to save the GPKG file.

    Returns
    -------
        None

    This function stores geospatial data from a dictionary of GeoDataFrames into a GeoPackage (GPKG) file.

    Parameters
    ----------
    - waterschap: A dictionary where the keys represent layer names, and the values are GeoDataFrames.
    - output_gpkg_path: The file path for the output GPKG file. The '.gpkg' extension is added automatically.
    """
    for key in waterschap.keys():
        waterschap[str(key)].to_file(output_gpkg_path + ".gpkg", layer=str(key), driver="GPKG")


def overlapping_peilgebieden(waterschap_peilgebieden):
    """
    Identify and calculate the percentage of overlapping peilgebieden.

    Parameters
    ----------
        waterschap_peilgebieden (geopandas.GeoDataFrame): A GeoDataFrame containing polygons (the peilgebieden).

    Returns
    -------
        geopandas.GeoDataFrame: A GeoDataFrame with overlapping polygons and their overlap percentages.

    This function analyzes a GeoDataFrame of peilgebied polygons to find overlapping polygons and calculate
    the percentage of overlap between them. It returns a GeoDataFrame with information about the overlapping
    polygons, including their overlap percentages.

    Parameters
    ----------
    - waterschap_peilgebieden: A GeoDataFrame containing the peilgebieden polygons.
    """
    peilgebied = waterschap_peilgebieden
    peilgebied.geometry = peilgebied.buffer(distance=0)  # make invalid geometries valid
    peilgebied.set_crs(crs="EPSG:28992", inplace=True)

    # Create an empty GeoDataFrame to store the overlapping polygons and additional information
    overlapping_polygons = gpd.GeoDataFrame(columns=peilgebied.columns)

    # Iterate through each polygon in peilgebied
    for index, row in peilgebied.iterrows():
        current_polygon = peilgebied.iloc[[index]]  # select the current polygon
        other_polygons = peilgebied.drop(index)  # create a GeoDataFrame without the current polygon
        overlaps = other_polygons[
            other_polygons.geometry.overlaps(current_polygon.geometry.iloc[0])
        ]  # check for overlaps with other polygons

        if not overlaps.empty:
            # calculate the percentage of overlap, and add this to the gdf including the overlapping indexes
            current_overlap_percentage = (
                overlaps.geometry.intersection(current_polygon.geometry.iloc[0]).area
                / current_polygon.geometry.iloc[0].area
                * 100
            )

            overlaps["overlap_percentage"], overlaps["source_globalid"] = pd.NA, pd.NA  # create columns

            # fill columns
            overlaps["overlap_percentage"] = (
                current_overlap_percentage  # multiple peilgebieden will be added to the temporal gdf if there are multiple overlapping polygons
            )
            overlaps["source_globalid"] = current_polygon["globalid"].values[
                0
            ]  # add the global id of the current polygon.

            # add to the results
            overlapping_polygons = pd.concat([overlapping_polygons, overlaps])

    return overlapping_polygons


def plot_histogram_overlap(overlapping_polygons):
    """
    Plots a histogram of the overlapping polygons in a DataFrame.

    Parameters
    ----------
        overlapping_polygons (pd.DataFrame): A DataFrame containing information about overlapping polygons.
            It should have a 'overlap_percentage' column to represent the percentage of overlap between polygons.

    Returns
    -------
        None

    The function calculates a histogram of overlapping percentages, providing insights into the distribution of overlaps
    between polygons. It handles potential NaN values in the 'overlap_percentage' column and creates bins ranging
    from 0% to 100% in 10% increments for the histogram. The number of overlapping polygons is displayed in the title.

    """
    overlapping_polygons["overlap_percentage"] = overlapping_polygons["overlap_percentage"].fillna(
        0
    )  # Handle potential NaN values
    bins = range(0, 101, 10)  # Create bins from 0% to 100% in 10% increments

    # Create the histogram
    plt.hist(overlapping_polygons["overlap_percentage"], bins=bins, color="cornflowerblue", edgecolor="k")

    # Set labels and title
    plt.xlabel("Overlap [%]")
    plt.ylabel("Frequency [#]")  # Update the y-axis label
    # plt.yscale('log')  # Set the y-axis scale to 'log'
    plt.ylim(0, 15)
    plt.suptitle("Histogram of overlapping percentages")
    plt.title(f"Number of overlapping polygons = {len(overlapping_polygons)}", fontsize=8)
    plt.show()


def plot_overlapping_peilgebieden(peilgebied, overlapping_polygons, minimum_percentage):
    """
    Plot Overlapping Peilgebieden on a map, including a Minimum Percentage of Overlap to show.

    Parameters
    ----------
        peilgebied (geopandas.GeoDataFrame): A GeoDataFrame representing the peilgebied polygons.
        overlapping_polygons (geopandas.GeoDataFrame): A GeoDataFrame containing information about overlapping polygons/peilgebieden.
        minimum_percentage (float or int): The minimum overlap percentage required for polygons to be displayed.

    Returns
    -------
        None

    This function creates a plot to visualize overlapping peilgebieden based on a specified minimum overlap percentage.
    It displays a subset of overlapping polygons with a percentage greater than the specified minimum.

    Parameters
    ----------
    - peilgebied: The entire peilgebieden GeoDataFrame serving as the background.
    - overlapping_polygons: GeoDataFrame containing information about overlapping polygons.
    - minimum_percentage: The minimum overlap percentage required for polygons to be displayed.

    """
    # make a subsect of overlapping polygons, based on a percentage
    overlap_subset = overlapping_polygons.loc[overlapping_polygons["overlap_percentage"] > minimum_percentage]

    # plot
    fig, ax = plt.subplots()
    peilgebied.plot(ax=ax, color="lightgray")  # background
    overlap_subset.plot(
        ax=ax, cmap="coolwarm", column=overlap_subset.overlap_percentage, label="Percentage of overlap", legend=True
    )

    plt.show()


# def intersect_using_spatial_index(peilgebied_praktijk, peilgebied_afwijking, check):
#     """
#     Conduct spatial intersection using spatial index for candidates GeoDataFrame to make queries faster.
#     Note, with this function, you can have multiple Polygons in the 'intersecting_gdf' and it will return all the points
#     intersect with ANY of those geometries.
#     """
#     peilgebied_praktijk_sindex = peilgebied_praktijk.sindex
#     possible_matches_index = []

#     # 'itertuples()' function is a faster version of 'iterrows()'
#     for other in peilgebied_afwijking.itertuples():
#         bounds = other.geometry.bounds
#         c = list(peilgebied_praktijk_sindex.intersection(bounds))
#         possible_matches_index += c

#     # Get unique candidates
#     unique_candidate_matches = list(set(possible_matches_index))
#     possible_matches = peilgebied_praktijk.iloc[unique_candidate_matches]

#     possible_matches.to_file('possible_matches_Rijnland.shp')
#     un_un = possible_matches.intersects(peilgebied_afwijking.unary_union)
# #     print('un_un =')
# #     display(un_un)
# #     print()

# #     print('possible_matches =')
# #     display(possible_matches)
# #     print()

# #     print('overlapping_pg_praktijk =')
# #     display(possible_matches[un_un])

# #     possible_matches[un_un].to_file('peilgebied_afwijking_unary_union_Rijnland.shp')


#     # Conduct the actual intersect
#     overlapping_pg_praktijk = possible_matches.loc[un_un] #the entire peilgebied praktijk polygons


#     #remove the peilgebied afwijking from the peilgebied praktijk
#     intersection = gpd.overlay(overlapping_pg_praktijk, peilgebied_afwijking, how='intersection')

#     #fix possible invalid geometries
#     overlapping_pg_praktijk['geometry'] = overlapping_pg_praktijk.buffer(distance = 0)
#     peilgebied_afwijking['geometry'] = peilgebied_afwijking.buffer(distance = 0)

#     overlapping_updated = gpd.overlay(peilgebied_praktijk, intersection, how='symmetric_difference') ##remove the difference between pg_praktijk and pg_afwijking
#     peilgebied = overlapping_updated.append(intersection, ignore_index=True) #add the removed difference, but now only the intersected part of pg_afwijking


#     if check:
#         peilgebied_praktijk.to_file('Checks/Rivierenland/peilgebied_praktijk.gpkg', driver='GPKG')
#         peilgebied_afwijking.to_file('Checks/Rivierenland/peilgebied_afwijking.gpkg', driver='GPKG')

#         intersection.to_file('Checks/Rivierenland/intersection.gpkg', driver='GPKG')
#         overlapping_updated.to_file('Checks/Rivierenland/overlapping_updated.gpkg', driver='GPKG')
#         peilgebied.to_file('Checks/Rivierenland/peilgebied.gpkg', driver='GPKG')

#     return peilgebied


def burn_in_peilgebieden(base_layer, overlay_layer, plot=True):
    # remove the overlapping parts from the base_layer
    base_layer_without_overlapping = gpd.overlay(
        base_layer, overlay_layer, how="symmetric_difference", keep_geom_type=False
    )  ##remove the difference between pg_praktijk and pg_afwijking

    # fill each column
    base_layer_without_overlapping.code_1.fillna(value=base_layer_without_overlapping.code_2, inplace=True)
    base_layer_without_overlapping.nen3610id_1.fillna(value=base_layer_without_overlapping.nen3610id_2, inplace=True)
    base_layer_without_overlapping.globalid_1.fillna(value=base_layer_without_overlapping.globalid_2, inplace=True)
    # base_layer_without_overlapping.waterhoogte_1.fillna(value = base_layer_without_overlapping.waterhoogte, inplace=True)

    if (
        "waterhoogte_1" in base_layer_without_overlapping.keys()
    ):  # sometimes a waterhoogte is present in the peilgebieden. Manage this.
        base_layer_without_overlapping.rename(
            columns={
                "code_1": "code",
                "nen3610id_1": "nen3610id",
                "globalid_1": "globalid",
                "waterhoogte_1": "waterhoogte",
            },
            inplace=True,
        )
        base_layer_without_overlapping.drop(
            columns=["code_2", "nen3610id_2", "globalid_2", "waterhoogte_2"], inplace=True
        )

    else:
        base_layer_without_overlapping.rename(
            columns={"code_1": "code", "nen3610id_1": "nen3610id", "globalid_1": "globalid"}, inplace=True
        )
        base_layer_without_overlapping.drop(columns=["code_2", "nen3610id_2", "globalid_2"], inplace=True)

    burned_base_layer = pd.concat([pd.DataFrame(base_layer_without_overlapping), pd.DataFrame(overlay_layer)], axis=0)

    burned_base_layer = burned_base_layer.drop_duplicates(subset="globalid", keep="last")

    if plot:
        fig, ax = plt.subplots()
        base_layer.plot(ax=ax, color="cornflowerblue")
        overlay_layer.plot(ax=ax, color="blue")

    return burned_base_layer
