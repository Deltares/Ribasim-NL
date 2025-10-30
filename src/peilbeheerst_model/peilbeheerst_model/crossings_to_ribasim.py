import itertools
import os
import shutil

import geopandas as gpd
import numpy as np
import pandas as pd
import ribasim
from bokeh.palettes import Category10
from shapely.geometry import LineString, MultiPolygon, Point, Polygon
from shapely.wkt import loads

from ribasim_nl import CloudStorage, settings


class CrossingsToRibasim:
    """Create a Ribasim network from crossings

    A class to create a Ribasim network directly from a pre defined post
    processed layer, and a 'crossings layer'.
    """

    def __init__(self, model_characteristics):
        """Initialize the crossings_to_Ribasim object

        Parameters
        ----------
        model_characteristics : _type_
            _description_
        """
        self.model_characteristics = model_characteristics

    #         #paths
    #         self.model_characteristics['path_postprocessed_data'] = path_postprocessed_data
    #         self.model_characteristics['path_crossings'] = path_crossings

    #         #determine which layers and which filtering should be applied
    #         self.model_characteristics['crossings_layer'] = crossings_layer
    #         self.model_characteristics['in_use'] = in_use
    #         self.model_characteristics['agg1_used'] = agg1_used

    #         #model properties
    #         self.model_characteristics['modelname'] = modelname
    #         self.model_characteristics['modeltype'] = modeltype

    def read_gpkg_layers(
        self,
        print_var=False,
        data=None,
    ):
        """Read specified layers from a GeoPackage file and store them in the object.

        Parameters
        ----------
        print_var : bool, optional
            Flag to print each layer name when reading, by default False
        data : _type_, optional
            An already existing dictionary to append the data to, by default
            None

        Returns
        -------
        dict
            Dictionary containing GeoDataFrames for each specified layer
        """
        if data is None:
            data = {}
        gpkg_path = self.model_characteristics["path_postprocessed_data"]
        layers = gpd.list_layers(gpkg_path)
        for layer in layers.name:
            if print_var:
                print(layer)
            data_temp = gpd.read_file(gpkg_path, layer=layer)
            data[layer] = data_temp

        return data

    def read_files(self):
        """Read and filter the postprocessed and crossings data

        Returns
        -------
        _type_
            _description_
        """
        # load in the post processed data
        post_processed_data = self.read_gpkg_layers(data=None)  # add data, based on delivered data

        # Load in the crossings data, and apply filtering
        crossings = gpd.read_file(
            self.model_characteristics["path_crossings"], layer=self.model_characteristics["crossings_layer"]
        )

        if self.model_characteristics["in_use"]:
            crossings = crossings[crossings.in_use].reset_index(drop=True)  # only use the crossings in use

        if self.model_characteristics["agg_links_in_use"]:
            crossings = crossings[crossings.agg_links_in_use].reset_index(drop=True)  # only use the crossings in use

        if self.model_characteristics["agg_areas_in_use"]:
            crossings = crossings[crossings.agg_areas_in_use].reset_index(drop=True)  # only use the crossings in use

        crossings["geometry"] = gpd.GeoSeries(
            gpd.points_from_xy(x=crossings["geometry"].x, y=crossings["geometry"].y)
        )  # prevent strange geometries

        if self.model_characteristics["aggregation"]:
            aggregation_areas = gpd.read_file(
                self.model_characteristics["path_crossings"], layer="aggregation_area"
            ).to_crs(crs="EPSG:28992")
        else:
            aggregation_areas = None
        post_processed_data["aggregation_areas"] = aggregation_areas

        # display(crossings)
        # stop
        return post_processed_data, crossings

    def routing_processor(
        self, post_processed_data, crossings
    ):  # , crossings toevoegen. Daarnaast crossings returnen, en een copy van maken in regel 10 bij de aantekeningen
        """Define the representative point for basins and merge with crossings data

        Parameters
        ----------
        post_processed_data : _type_
            _description_
        crossings : _type_
            _description_

        Returns
        -------
        _type_
            _description_
        """
        # define the most representative point where the basins will be located. This is always within the peilgebied polygon.

        # save the original peilgebied_from and _to for boezem purposes later, before changing anything due to the aggregation
        crossings["peilgebied_from_original"] = crossings["peilgebied_from"]
        crossings["peilgebied_to_original"] = crossings["peilgebied_to"]

        if self.model_characteristics["aggregation"]:
            # not all peilgebieden are part of a aggregation area. Add the missing peilgebieden to the agg_area column, so no areas will be left out
            crossings["agg_area_from"].fillna(crossings["peilgebied_from"], inplace=True)
            crossings["agg_area_to"].fillna(crossings["peilgebied_to"], inplace=True)

            # change administrative detailed peilgebieden to aggregation areas
            crossings["peilgebied_from"] = crossings["agg_area_from"]
            crossings["peilgebied_to"] = crossings["agg_area_to"]

            post_processed_data["aggregation_areas"] = post_processed_data["aggregation_areas"].rename(
                columns={"CODE": "code"}
            )  # globalid is mandatory for later in the algorithm
            post_processed_data["aggregation_areas"]["globalid"] = post_processed_data["aggregation_areas"][
                "code"
            ]  # globalid is mandatory for later in the algorithm

            # instead of copy pasting the aggregation areas to the peilgebieden, concat it. By doing so, also the peilgebieden which do not fall into an aggregation area are taken into consideration
            post_processed_data["peilgebied"] = pd.concat(
                [
                    post_processed_data["aggregation_areas"][["code", "globalid", "geometry"]],
                    post_processed_data["peilgebied"][["code", "globalid", "peilgebied_cat", "geometry"]],
                ]
            )
            post_processed_data["peilgebied"] = gpd.GeoDataFrame(post_processed_data["peilgebied"], geometry="geometry")

        post_processed_data["peilgebied"]["centroid_geometry"] = post_processed_data[
            "peilgebied"
        ].representative_point()

        from_centroid_geometry = crossings.merge(
            post_processed_data["peilgebied"],
            how="left",
            left_on=crossings["peilgebied_from"],
            right_on="globalid",
        )["centroid_geometry"]  # merge from geometry

        to_centroid_geometry = crossings.merge(
            post_processed_data["peilgebied"],
            how="left",
            left_on="peilgebied_to",
            right_on="globalid",
        )["centroid_geometry"]  # merge to geometry

        crossings["from_centroid_geometry"] = from_centroid_geometry
        crossings["to_centroid_geometry"] = to_centroid_geometry
        # also add the polygons for visualisation purposes
        from_polygon_geometry = crossings.merge(
            post_processed_data["peilgebied"],
            how="left",
            left_on=crossings["peilgebied_from"],
            right_on="globalid",
        )["geometry_y"]  # merge from geometry

        to_polygon_geometry = crossings.merge(
            post_processed_data["peilgebied"],
            how="left",
            left_on="peilgebied_to",
            right_on="globalid",
        )["geometry_y"]  # merge to geometry

        crossings["from_polygon_geometry"] = from_polygon_geometry
        crossings["to_polygon_geometry"] = to_polygon_geometry

        return post_processed_data, crossings

    def assign_node_ids(self, crossings):
        """Assign node IDs to the crossings based on the order of the global ID

        The node id's are (for now) index based and sorted by the global id's.
        The global id's may contain actual global id's, but also dummies and
        just numbers. It was mandatory to maintain a consecutive order in the
        node_ids in the early releases of Ribasim. The node id numbering ranges
        thus from 1 to n.

        Parameters
        ----------
        crossings : _type_
            _description_

        Returns
        -------
        _type_
            _description_
        """
        # identify globalids which contain letters
        letters_from = crossings[crossings["peilgebied_from"].str.contains("[a-zA-Z_]", na=False)]
        letters_to = crossings[crossings["peilgebied_to"].str.contains("[a-zA-Z_]", na=False)]
        letters_df = pd.concat([letters_from["peilgebied_from"], letters_to["peilgebied_to"]])
        letters_df = letters_df.unique()

        # identify globalids which contain numbers / any other symbols
        numbers_from = crossings[~crossings["peilgebied_from"].str.contains("[a-zA-Z_]", na=False)]
        numbers_to = crossings[~crossings["peilgebied_to"].str.contains("[a-zA-Z_]", na=False)]
        numbers_df = pd.concat([numbers_from["peilgebied_from"], numbers_to["peilgebied_to"]])
        numbers_df = numbers_df.unique()
        if numbers_df[0] is not None and not np.isnan(numbers_df[0]):  # detect the largest number
            max_number = max(numbers_df.astype(int))
            if max_number < 0:
                max_number = 0
        else:
            max_number = 0

        # renumber each node id, which has no number yet
        for count, globalid_str in enumerate(letters_df):
            peilgebied_n = count + max_number + 1
            indices_to_replace = crossings.index[crossings["peilgebied_from"] == globalid_str]
            crossings.loc[indices_to_replace, "peilgebied_from"] = peilgebied_n

            indices_to_replace = crossings.index[crossings["peilgebied_to"] == globalid_str]
            crossings.loc[indices_to_replace, "peilgebied_to"] = peilgebied_n

        # retrieve max node id, avoid problems with Nones
        basin_from_max = [value for value in crossings.peilgebied_from.to_numpy() if value is not None]
        basin_to_max = [value for value in crossings.peilgebied_to.to_numpy() if value is not None]

        # basin_from_max = [int(x) for x in basin_from_max]
        # basin_to_max = [int(x) for x in basin_to_max]

        basin_max = max(max(basin_from_max), max(basin_to_max))

        crossings = crossings.reset_index(drop=True)
        crossings["node_id"] = crossings.index + basin_max + 1

        # something is sometimes going wrong with the streefpeilen of HHSK. Fill them up.
        if self.model_characteristics["waterschap"] == "SchielandendeKrimpenerwaard":
            refill_streefpeilen = crossings.copy(deep=True)
            refill_streefpeilen_from = refill_streefpeilen.dropna(subset="streefpeil_from")[
                ["peilgebied_from", "streefpeil_from"]
            ]
            refill_streefpeilen_from.rename(
                columns={"peilgebied_from": "peilgebied", "streefpeil_from": "streefpeil"}, inplace=True
            )
            refill_streefpeilen_to = refill_streefpeilen.dropna(subset="streefpeil_to")[
                ["peilgebied_to", "streefpeil_to"]
            ]
            refill_streefpeilen_to.rename(
                columns={"peilgebied_to": "peilgebied", "streefpeil_to": "streefpeil"}, inplace=True
            )
            refill_streefpeilen = pd.concat([refill_streefpeilen_from, refill_streefpeilen_to])
            refill_streefpeilen = refill_streefpeilen.dropna(subset="peilgebied").reset_index(drop=True)

            # now, fill the entire df up again
            crossings = crossings.merge(
                refill_streefpeilen, left_on="peilgebied_from", right_on="peilgebied", how="left"
            )
            crossings["streefpeil_from"] = crossings["streefpeil_from"].fillna(crossings["streefpeil"])
            crossings = crossings.drop(columns="streefpeil")

            crossings = crossings.merge(refill_streefpeilen, left_on="peilgebied_to", right_on="peilgebied", how="left")
            crossings["streefpeil_to"] = crossings["streefpeil_to"].fillna(crossings["streefpeil"])
            crossings = crossings.drop(columns="streefpeil")
            crossings = crossings.drop_duplicates(subset=["hydroobject", "peilgebieden", "stuw", "gemaal"])
            crossings = crossings.reset_index(drop=True)

        return crossings

    def create_links(self, crossings):
        """Create links for Ribasim network

        Create the links, based on the peilgebied_from and peilgebied_to
        column. Next to the basins ('peilgebied_from' or 'peilgebied_to') it
        also creates a connection at the border of a peilgebied. This is a
        point where a hydroobject crosses the peilgebied border. These nodes
        are denoted with the 'node_id'.

        Parameters
        ----------
        crossings : _type_
            _description_

        Returns
        -------
        _type_
            _description_
        """
        # concat the lines which goes from the basin nodes, and from the TRC nodes
        lines_from_id = pd.concat(
            [
                crossings.peilgebied_from.astype(np.int64, errors="ignore"),
                crossings.node_id.astype(np.int64, errors="ignore"),
            ]
        )

        lines_to_id = pd.concat(
            [
                crossings.node_id.astype(np.int64, errors="ignore"),
                crossings.peilgebied_to.astype(np.int64, errors="ignore"),
            ]
        )

        lines_from_coord = pd.concat([crossings.from_centroid_geometry, crossings.geometry])
        lines_to_coord = pd.concat([crossings.geometry, crossings.to_centroid_geometry])

        lines_from_id = lines_from_id.reset_index(drop=True)
        lines_to_id = lines_to_id.reset_index(drop=True)
        lines_from_coord = lines_from_coord.reset_index(drop=True)
        lines_to_coord = lines_to_coord.reset_index(drop=True)

        # put all node_id and their corresponding coordinates in a df
        links = pd.DataFrame()
        links["from"] = lines_from_id
        links["to"] = lines_to_id
        links["from_coord"] = lines_from_coord
        links["to_coord"] = lines_to_coord

        # remove NaN's, as these rows do not contain any coordinates
        links = links.dropna()
        links = links.drop(links[(links["from"] == -999) | (links["to"] == -999)].index)
        links = links.reset_index(drop=True)

        # change the 'Point Z' to a regular 'Point' geometry.
        links["from_x"] = links["from_coord"].apply(lambda geom: geom.x)
        links["from_y"] = links["from_coord"].apply(lambda geom: geom.y)

        links["to_x"] = links["to_coord"].apply(lambda geom: geom.x)
        links["to_y"] = links["to_coord"].apply(lambda geom: geom.y)

        links["from_coord"] = gpd.GeoSeries(gpd.points_from_xy(x=links["from_x"], y=links["from_y"]))
        links["to_coord"] = gpd.GeoSeries(gpd.points_from_xy(x=links["to_x"], y=links["to_y"]))

        # create the LineStrings (=links), based on the df with coordinates
        links["line_geom"] = links.apply(lambda row: LineString([row["from_coord"], row["to_coord"]]), axis=1)

        return links

    def create_nodes(self, crossings, links):
        """_summary_

        Parameters
        ----------
        crossings : _type_
            _description_

        Returns
        -------
        _type_
            _description_

        Raises
        ------
        ValueError
            _description_
        """
        # define the basins
        basins = pd.concat([crossings["peilgebied_from"], crossings["peilgebied_to"]])
        basins_geom = pd.concat([crossings["from_centroid_geometry"], crossings["to_centroid_geometry"]])
        basins_area_geom = pd.concat([crossings["from_polygon_geometry"], crossings["to_polygon_geometry"]])
        basins_streefpeil = pd.concat([crossings["streefpeil_from"], crossings["streefpeil_to"]])

        nodes_basins = pd.DataFrame()
        nodes_basins["node_id"] = basins
        nodes_basins["type"] = "Basin"
        nodes_basins["geometry"] = basins_geom
        nodes_basins["basins_area_geom"] = basins_area_geom
        nodes_basins["streefpeil"] = basins_streefpeil.fillna(value="Onbekend streefpeil")

        nodes_basins = nodes_basins.loc[nodes_basins.node_id != -999].dropna()
        nodes_basins = nodes_basins.drop_duplicates(subset="node_id").reset_index(drop=True)

        # crossings.loc[(crossings.from_polygon_geometry.isna()) | (crossings.to_polygon_geometry.isna()), 'crossing_type'] = '01'
        # create TabulatedRatingCurves (TRC's) or FlowBoundaries (FB's), depending whether the model should be a 'poldermodel' or a 'boezemmodel'
        if self.model_characteristics["modeltype"] == "poldermodel":
            TRC = crossings[
                ~crossings["crossing_type"].astype(str).str.contains("1|2")
            ]  # the '~' makes sure only zero's are selected (peilgebied to peilgebied crossings)
            boundary = crossings[
                crossings["crossing_type"].astype(str).str.contains("1|2")
            ]  # the '~' makes sure only zero's are selected (peilgebied to peilgebied crossings)

        elif self.model_characteristics["modeltype"] == "boezemmodel":
            # TRC = crossings[
            #     ~crossings["crossing_type"].astype(str).str.contains("-10|2")
            # ]  # the '~' makes sure only zero's are selected (peilgebied to peilgebied crossings)
            # boundary = crossings[
            #     crossings["crossing_type"].astype(str).str.contains("-10|2")
            # ]

            TRC = crossings[crossings["peilgebied_from"].notna() & crossings["peilgebied_to"].notna()]
            boundary = crossings[~(crossings["peilgebied_from"].notna() & crossings["peilgebied_to"].notna())]

        else:
            raise ValueError("Invalid 'modeltype'. Please use 'poldermodel' or 'boezemmodel'.")

        # seperate the ManningResistances from the TRC by checking if the streefpeilen are the same
        boundary = boundary.reset_index(drop=True)  # can be deleted later, 20240403
        FB = boundary.loc[boundary.from_polygon_geometry.isna()]  # flow boundaries let water flow into the model
        TRC_terminals = boundary.loc[
            ~boundary.from_polygon_geometry.isna()
        ]  # terminals let water flow out of the model. they can not be connected to a basin, so create TRC's first. Connect a Terminal to it afterwards.

        ###########################
        # display(FB)
        # display(boundary.loc[boundary.peilgebied_from.isna()])
        FB = boundary.loc[boundary.peilgebied_from.isna()]  # flow boundaries let water flow into the model
        TRC_terminals = boundary.loc[~boundary.peilgebied_from.isna()]
        ###########################

        # terminals = boundary.loc[boundary.to_polygon_geometry.isna()] #terminals let water flow out of the model
        FB = FB.reset_index(drop=True)  # can be deleted later, 20240403
        TRC_terminals = TRC_terminals.reset_index(drop=True)  # can be deleted later, 20240403

        # print('FB = ')
        # display(FB)
        # print()
        # print('TRC_ter = ')
        # display(TRC_terminals)

        # display(TRC_terminals.loc[~TRC_terminals.peilgebied_to.isna()])

        # stop
        # Define the MR
        # Select MR based on similar streefpeil
        MR = TRC.loc[TRC.streefpeil_from == TRC.streefpeil_to]

        # Drop MR node in case of peilgebied_cat -1
        MR = MR[~MR["crossing_type"].astype(str).str.contains("-1")]

        # Make sure peilgebied_from is active
        MR = MR.loc[~MR["peilgebied_from"].isna()]
        MR = MR.loc[~MR["peilgebied_to"].isna()]

        # Drop MR nodes from TRC nodes
        mask = TRC["node_id"].isin(MR["node_id"])
        TRC = TRC[~mask]

        # Create MR dataframe
        nodes_MR = pd.DataFrame()
        nodes_MR["node_id"] = MR["node_id"]
        nodes_MR["type"] = "ManningResistance"
        nodes_MR["geometry"] = MR["geometry"]
        nodes_MR.drop_duplicates(subset="node_id").reset_index(drop=True)

        # define the TRC
        # TRC = crossings.loc[(crossings.peilgebied_from != -999) & (crossings.peilgebied_to != -999)]
        nodes_TRC = pd.DataFrame()
        nodes_TRC["node_id"] = TRC["node_id"]
        nodes_TRC["type"] = "TabulatedRatingCurve"
        nodes_TRC["geometry"] = TRC["geometry"]
        nodes_TRC.drop_duplicates(subset="node_id").reset_index(drop=True)

        # define the FlowBoundaries
        # LB = crossings.loc[(crossings.peilgebied_from == -999) | (crossings.peilgebied_to == -999)]
        nodes_FB = pd.DataFrame()
        nodes_FB["node_id"] = FB.node_id
        nodes_FB["type"] = "FlowBoundary"
        nodes_FB["geometry"] = FB["geometry"]
        nodes_FB = nodes_FB.drop_duplicates(subset="node_id")

        # # define the TRC to the Terminals
        nodes_TRC_ter = pd.DataFrame()
        nodes_TRC_ter["node_id"] = TRC_terminals.node_id
        nodes_TRC_ter["type"] = "TabulatedRatingCurve"
        nodes_TRC_ter["geometry"] = TRC_terminals["geometry"]
        nodes_TRC_ter = nodes_TRC_ter.drop_duplicates(subset="node_id")

        # combine
        # nodes = pd.concat([nodes_basins, nodes_TRC, nodes_FB])
        nodes = pd.concat([nodes_basins, nodes_MR, nodes_TRC, nodes_FB, nodes_TRC_ter])
        nodes = nodes.reset_index(drop=True)
        nodes.index = nodes.node_id.to_numpy()

        # embed the pumps
        pump_nodes = crossings.dropna(subset="gemaal").node_id.to_numpy()
        # nodes.loc[nodes.node_id.isin(pump_nodes) & nodes['type'].isna(), "type"] = "Pump"
        nodes.loc[nodes.node_id.isin(pump_nodes), "type"] = (
            "Pump"  # stond eerst als de regel hierboven. Heb niet meer scherp waarom de nodes['type'] leeg MOET zijn; zo ver ik weet wordt die altijd ingevuld met een waarden (zie hierboven). Voor nu even weggehaald.
        )

        if len(nodes_TRC_ter) > 0:
            # first, properly embed the Terminals in the nodes
            nodes_max = max(nodes.node_id) + 1  # add +1 as we need to add new nodes
            nodes_ter = pd.DataFrame()
            nodes_ter["node_id"] = np.arange(nodes_max, nodes_max + len(nodes_TRC_ter))
            nodes_ter["type"] = "Terminal"
            # TRC_terminals = TRC_terminals.drop_duplicates(subset=['hydroobject', 'peilgebieden']).reset_index(drop=True)
            nodes_ter["geometry"] = [
                Point(point.x + 1, point.y) for point in TRC_terminals["geometry"]
            ]  # move one meter to the right to place the Terminal. Use the original points
            # nodes_ter['geometry'] = gpd.GeoSeries(nodes_ter['geometry'])
            nodes_ter = gpd.GeoDataFrame(nodes_ter)
            # nodes_ter["geometry"] = TRC_terminals["geometry"] #copy paste the geometry points of the TRC which go to the terminal
            nodes_ter = nodes_ter.drop_duplicates(subset="node_id")

            # second, properly embed the Terminals in the links. Fill in the entire df for completion purposes
            links_ter = pd.DataFrame()
            links_ter["from"] = TRC_terminals.node_id
            links_ter["to"] = nodes_ter.node_id.to_numpy()
            links_ter["from_coord"] = TRC_terminals["geometry"]
            links_ter["to_coord"] = nodes_ter["geometry"].to_numpy()
            links_ter["from_x"] = TRC_terminals["geometry"].x
            links_ter["from_y"] = TRC_terminals["geometry"].y
            links_ter["to_x"] = nodes_ter["geometry"].x.to_numpy()
            links_ter["to_y"] = nodes_ter["geometry"].y.to_numpy()

            links_ter["line_geom"] = links_ter.apply(
                lambda row: LineString([row["from_coord"], row["to_coord"]]), axis=1
            )
            links_ter.reset_index(drop=True, inplace=True)

            # append the nodes and the links to the main df
            nodes = pd.concat([nodes, nodes_ter]).reset_index(drop=True)
            links = pd.concat([links, links_ter]).reset_index(drop=True)

        nodes = nodes.sort_values(by="node_id")
        nodes = nodes.reset_index(drop=True)

        # add the level boundaries, on the locations where a pump is located at a boundary
        nodes_pump_LB = crossings.dropna(subset="gemaal")  # select all gemalen
        nodes_pump_LB = nodes_pump_LB.loc[nodes_pump_LB.peilgebied_from.isna()]
        # display(nodes_pump_LB)

        # repeat same procedure as the implementation of the Terminals, for the Level Boundaries
        if len(nodes_pump_LB) > 0:
            # first, properly embed the Terminals in the nodes
            nodes_max = max(nodes.node_id) + 1  # add +1 as we need to add new nodes
            nodes_LB = pd.DataFrame()
            nodes_LB["node_id"] = np.arange(nodes_max, nodes_max + len(nodes_pump_LB))
            nodes_LB["type"] = "LevelBoundary"
            nodes_LB["geometry"] = [
                Point(point.x - 1, point.y) for point in nodes_pump_LB["geometry"]
            ]  # move one meter to the left to place the terminal. Use the original points
            # nodes_LB['geometry'] = gpd.GeoSeries(nodes_LB['geometry'])
            nodes_LB = gpd.GeoDataFrame(nodes_LB)
            # nodes_LB["geometry"] = nodes_pump_LB["geometry"] #copy paste the geometry points of the TRC which go to the terminal
            nodes_LB = nodes_LB.drop_duplicates(subset="node_id")

            # second, properly embed the Terminals in the links. Fill in the entire df for completion purposes
            links_LB = pd.DataFrame()
            links_LB["to"] = nodes_pump_LB.node_id
            links_LB["from"] = nodes_LB.node_id.to_numpy()
            links_LB["to_coord"] = nodes_pump_LB["geometry"]
            links_LB["from_coord"] = nodes_LB["geometry"].to_numpy()
            links_LB["from_x"] = nodes_pump_LB["geometry"].x.to_numpy()
            links_LB["from_y"] = nodes_pump_LB["geometry"].y.to_numpy()
            links_LB["to_x"] = nodes_LB["geometry"].x
            links_LB["to_y"] = nodes_LB["geometry"].y

            links_LB["line_geom"] = links_LB.apply(lambda row: LineString([row["from_coord"], row["to_coord"]]), axis=1)
            links_LB.reset_index(drop=True, inplace=True)

            # append the nodes and the links to the main df
            nodes = pd.concat([nodes, nodes_LB]).reset_index(drop=True)
            links = pd.concat([links, links_LB]).reset_index(drop=True)

        nodes.index += 1

        return nodes, links

    def embed_boezems(self, links, post_processed_data, crossings):
        def discard_duplicate_boezems(boezems):
            temp_boezems = boezems.dropna(subset=["shortest_path"]).copy()

            # Perform the operations on the temporary DataFrame
            temp_boezems["shortest_path"] = temp_boezems["shortest_path"].astype(str).apply(loads)
            temp_boezems["first_geom_coordinate"] = temp_boezems["shortest_path"].apply(
                lambda geom: Point(geom.coords[0])
            )
            temp_boezems["last_geom_coordinate"] = temp_boezems["shortest_path"].apply(
                lambda geom: Point(geom.coords[-1])
            )

            temp_boezems["distance_first_from"] = temp_boezems.apply(
                lambda row: row["first_geom_coordinate"].distance(row["from_coord"]), axis=1
            )
            temp_boezems["distance_last_to"] = temp_boezems.apply(
                lambda row: row["last_geom_coordinate"].distance(row["to_coord"]), axis=1
            )
            temp_boezems["correct_distance"] = temp_boezems["distance_first_from"] + temp_boezems["distance_last_to"]

            temp_boezems["distance_first_to"] = temp_boezems.apply(
                lambda row: row["first_geom_coordinate"].distance(row["to_coord"]), axis=1
            )
            temp_boezems["distance_last_from"] = temp_boezems.apply(
                lambda row: row["last_geom_coordinate"].distance(row["from_coord"]), axis=1
            )
            temp_boezems["incorrect_distance"] = temp_boezems["distance_first_from"] + temp_boezems["distance_last_to"]

            temp_boezems["shortest_distance"] = temp_boezems[["correct_distance", "incorrect_distance"]].min(axis=1)
            temp_boezems.sort_values(by="shortest_distance", inplace=True)
            temp_boezems.drop_duplicates(subset=["from", "to"], keep="first", inplace=True)
            temp_boezems.reset_index(drop=True, inplace=True)
            # Merge the temporary DataFrame back into the original DataFrame
            boezems.update(temp_boezems)

            return boezems

        if self.model_characteristics["path_boezem"] is not None:
            boezems = gpd.read_file(self.model_characteristics["path_boezem"]).set_crs(crs="EPSG:28992")
            boezem_globalids = (
                post_processed_data["peilgebied"]
                .loc[post_processed_data["peilgebied"].peilgebied_cat == 1, "globalid"]
                .drop_duplicates()
            )

            # hydroobject = '{a20599ac-ac03-410a-8719-0f5329c64d5b}'
            # hydroobject = "{ea981cb2-ab98-4bde-b31a-34f159f0d681}"
            # display(boezems.loc[boezems.hydroobject == hydroobject])
            # Correct order #######################################
            boezems_df_correct_order = boezems.loc[boezems.peilgebied_from.isin(boezem_globalids)]

            # retrieve starting point coordinates
            temp_crossings = boezems_df_correct_order.merge(
                crossings,
                left_on=["hydroobject", "peilgebied_from"],
                right_on=["hydroobject", "peilgebied_from_original"],
                how="left",
            )

            links_with_correct_SP = links.merge(
                temp_crossings,
                left_on=["from_coord", "to"],
                right_on=["from_centroid_geometry", "node_id_y"],
                how="left",
            )

            links_with_correct_SP = discard_duplicate_boezems(links_with_correct_SP)

            # INCORRECT order #######################################
            boezems_df_incorrect_order = boezems.loc[boezems.peilgebied_to.isin(boezem_globalids)]

            # retrieve starting point coordinates
            temp_crossings = boezems_df_incorrect_order.merge(
                crossings,
                left_on=["hydroobject", "peilgebied_to"],
                right_on=["hydroobject", "peilgebied_to_original"],
                how="left",
            )

            links_with_incorrect_SP = links.merge(
                temp_crossings,
                left_on=["to_coord", "from"],
                right_on=["to_centroid_geometry", "node_id_y"],
                how="left",
            )
            links_with_incorrect_SP = discard_duplicate_boezems(links_with_incorrect_SP)

            links_with_correct_SP.drop_duplicates(subset=["from", "to"], inplace=True)
            links_with_incorrect_SP.drop_duplicates(subset=["from", "to"], inplace=True)

            links_with_correct_SP.reset_index(drop=True, inplace=True)
            links_with_incorrect_SP.reset_index(drop=True, inplace=True)

            # add the shortest paths columns to the links
            links.rename(columns={"line_geom": "line_geom_oud"}, inplace=True)
            links["line_geom"] = np.nan

            links_temp_incorrect = links.merge(
                links_with_incorrect_SP, how="left", on=["from", "to"], suffixes=("", "_incorrect_SP")
            )
            links.loc[links_temp_incorrect["shortest_path"].notna(), "line_geom"] = links_temp_incorrect[
                "shortest_path"
            ]
            links_temp_correct = links.merge(
                links_with_correct_SP, how="left", on=["from", "to"], suffixes=("", "_correct_SP")
            )
            links.loc[links_temp_correct["shortest_path"].notna(), "line_geom"] = links_temp_correct["shortest_path"]

            # showcase = links.loc[links['to'] == 452]
            # showcase['line_geom'] = showcase['line_geom'].astype(str).apply(loads) #change string to geometry object
            # gpd.GeoDataFrame(showcase, geometry = 'line_geom').plot()

            # the direction of the links are correct on administrative level, but not yet on geometric level. Revert the lines.
            reverse_gemaal = crossings.copy()  # create copy of the crossings
            reverse_gemaal = reverse_gemaal.loc[
                reverse_gemaal.peilgebied_to_original.isin(boezem_globalids)
            ]  # select the rows which are a crossing with the boezem
            reverse_gemaal = reverse_gemaal.loc[
                ~reverse_gemaal.gemaal.isna()
            ]  # select the crossings on the boezem with a gemaal
            reverse_gemaal = reverse_gemaal.node_id.unique()  # select the crossings where the lines should be reverted. Revert after changing the string to a geometry object

            # add a column if a shortest path is found
            links["bool_SP"] = links["line_geom"]
            links["bool_SP"].loc[links["bool_SP"].isna()] = False
            # links["bool_SP"].loc[links["bool_SP"]] = True
            links["bool_SP"].loc[links["bool_SP"] != False] = True  # noqa: E712

            # fill the line geoms with the previous geoms if no shortest path is found
            links.line_geom = links.line_geom.fillna(links.line_geom_oud)
            links["line_geom"] = links["line_geom"].astype(str).apply(loads)  # change string to geometry object

            check_SP = links.loc[links.bool_SP].copy()
            check_SP["start_SP"] = check_SP["line_geom"].apply(lambda geom: Point(geom.coords[0]))
            check_SP["end_SP"] = check_SP["line_geom"].apply(lambda geom: Point(geom.coords[-1]))

            # display(check_SP)
            # define distances, both the logical ones (start_from and end_to) as well as the unlogical ones
            check_SP["distance_start_from"] = check_SP.apply(
                lambda row: row["start_SP"].distance(row["from_coord"]), axis=1
            )
            check_SP["distance_end_to"] = check_SP.apply(lambda row: row["end_SP"].distance(row["to_coord"]), axis=1)
            check_SP["distance_start_to"] = check_SP.apply(
                lambda row: row["start_SP"].distance(row["to_coord"]), axis=1
            )
            check_SP["distance_end_from"] = check_SP.apply(
                lambda row: row["end_SP"].distance(row["from_coord"]), axis=1
            )

            check_SP["line_geom"] = check_SP.apply(
                lambda row: LineString(list(row["line_geom"].coords)[::-1])
                if (
                    row["distance_start_from"] + row["distance_end_to"]
                    > row["distance_start_to"] + row["distance_end_from"]
                )
                else row["line_geom"],
                axis=1,
            )
            links.update(check_SP)

        return links

    def change_link(self, links, from_node_id_to_change, to_node_id_to_change, from_node_id_geom, to_node_id_geom):
        # find the geometries to use
        geometry_of_interest = links.loc[
            (links["from"] == from_node_id_geom) & (links["to"] == to_node_id_geom), "line_geom"
        ]

        # replace the found geometry
        links.loc[(links["from"] == from_node_id_to_change) & (links["to"] == to_node_id_to_change), "line_geom"] = (
            geometry_of_interest
        )

        return links

    def change_boezems_manually(self, links):
        if self.model_characteristics["waterschap"] == "HollandsNoorderkwartier":
            links = self.change_link(
                links, from_node_id_to_change=447, to_node_id_to_change=3, from_node_id_geom=83, to_node_id_geom=447
            )

            links = self.change_link(
                links, from_node_id_to_change=395, to_node_id_to_change=3, from_node_id_geom=68, to_node_id_geom=395
            )

            links = self.change_link(
                links, from_node_id_to_change=571, to_node_id_to_change=3, from_node_id_geom=126, to_node_id_geom=571
            )

            links = self.change_link(
                links, from_node_id_to_change=1287, to_node_id_to_change=3, from_node_id_geom=226, to_node_id_geom=1287
            )

            links = self.change_link(
                links, from_node_id_to_change=911, to_node_id_to_change=3, from_node_id_geom=101, to_node_id_geom=911
            )

            links = self.change_link(
                links, from_node_id_to_change=383, to_node_id_to_change=3, from_node_id_geom=65, to_node_id_geom=383
            )

            links = self.change_link(
                links, from_node_id_to_change=817, to_node_id_to_change=3, from_node_id_geom=119, to_node_id_geom=817
            )

            links = self.change_link(
                links, from_node_id_to_change=366, to_node_id_to_change=3, from_node_id_geom=57, to_node_id_geom=366
            )

            links = self.change_link(
                links, from_node_id_to_change=717, to_node_id_to_change=3, from_node_id_geom=13, to_node_id_geom=717
            )

            links = self.change_link(
                links, from_node_id_to_change=487, to_node_id_to_change=3, from_node_id_geom=98, to_node_id_geom=487
            )

            links = self.change_link(
                links, from_node_id_to_change=481, to_node_id_to_change=3, from_node_id_geom=96, to_node_id_geom=481
            )

            links = self.change_link(
                links, from_node_id_to_change=1027, to_node_id_to_change=3, from_node_id_geom=197, to_node_id_geom=1027
            )

        return links

        # links = self.change_link(links,
        #                          from_node_id_to_change=,
        #                          to_node_id_to_change=,
        #                          from_node_id_geom=,
        #                          to_node_id_geom=)


#####################################################################################################
#####################################################################################################
#####################################################################################################
#####################################################################################################


class RibasimNetwork:
    def __init__(self, nodes, links, model_characteristics):
        """_summary_

        Parameters
        ----------
        nodes : _type_
            _description_
        links : _type_
            _description_
        model_characteristics : _type_
            _description_
        """
        self.nodes = nodes
        self.links = links
        self.model_characteristics = model_characteristics

    # def node(self):
    #     """_summary_

    #     Returns
    #     -------
    #     _type_
    #         _description_
    #     """
    #     display(self.nodes)
    #     node = ribasim.Node(
    #         df=gpd.GeoDataFrame(
    #             data={"node_type": self.nodes["type"]},
    #             index=pd.Index(self.nodes["node_id"], name="fid"),
    #             geometry=self.nodes.geometry,
    #             crs="EPSG:28992",
    #         )
    #     )

    #     return node

    def link(self):
        """_summary_

        Returns
        -------
        _type_
            _description_
        """
        link = gpd.GeoDataFrame()

        # fix the from nodes
        from_node_type = self.links.merge(self.nodes, left_on="from", right_on="node_id")["type"].to_numpy()

        link["from_node_id"] = self.links["from"]  # from node ids
        link["meta_from_node_type"] = from_node_type

        # fix the to nodes
        to_node_type = self.links.merge(self.nodes, left_on="to", right_on="node_id")["type"].to_numpy()

        link["to_node_id"] = self.links["to"]  # to node ids
        link["meta_to_node_type"] = to_node_type

        # fill in the other columns
        link["link_type"] = "flow"
        link["name"] = None
        link["geometry"] = self.links["line_geom"]

        # comply to Ribasim 2024.11
        link = link.reset_index(drop=True)
        link["link_id"] = link.index.astype(int)
        link = link.set_index("link_id")

        return link

    def basin(self):
        """_summary_

        Returns
        -------
        _type_
            _description_
        """
        basin_nodes = self.nodes.loc[self.nodes["type"] == "Basin"][
            ["node_id", "streefpeil", "geometry", "basins_area_geom"]
        ]
        basin_nodes = basin_nodes.reset_index(drop=True)

        basin_node = pd.DataFrame()
        basin_node["node_id"] = basin_nodes["node_id"]
        basin_node["node_type"] = "Basin"
        basin_node["name"] = np.nan
        basin_node["subnetwork_id"] = np.nan
        basin_node["geometry"] = basin_nodes["geometry"]

        basins_area = self.nodes.loc[self.nodes["type"] == "Basin"]
        basins_area = basins_area[["node_id", "streefpeil", "basins_area_geom"]]
        basins_area.rename(columns={"basins_area_geom": "geom"}, inplace=True)

        profile_zero = pd.DataFrame(
            data={"node_id": self.nodes["node_id"].loc[self.nodes["type"] == "Basin"], "area": 0.1, "level": -9.999}
        )
        profile_one = pd.DataFrame(
            data={"node_id": self.nodes["node_id"].loc[self.nodes["type"] == "Basin"], "area": 3000, "level": 3}
        )
        basin_profile = pd.concat([profile_zero, profile_one])

        basin_state = pd.DataFrame()  # take the streefpeil as initial conditions
        basin_state["node_id"] = basins_area["node_id"]
        basin_state["level"] = basins_area["streefpeil"]
        basin_state.loc[basin_state["level"] == "Onbekend streefpeil", "level"] = (
            9.999  # insert initial water level of 9.999 if the streefpeil is unknown
        )

        basin_static = pd.DataFrame()
        basin_static["node_id"] = basin_nodes["node_id"]
        basin_static["drainage"] = 0
        basin_static["potential_evaporation"] = 0
        basin_static["infiltration"] = 0
        basin_static["precipitation"] = 0
        # basin_static["urban_runoff"] = 0

        # display(basin_nodes)
        basin_area = basin_nodes[["node_id", "streefpeil", "basins_area_geom"]]
        basin_area["geometry"] = basin_area["basins_area_geom"]
        basin_area["meta_streefpeil"] = basin_area["streefpeil"]
        basin_area = basin_area[["node_id", "meta_streefpeil", "geometry"]]
        basin_area = gpd.GeoDataFrame(basin_area, geometry="geometry").to_crs(crs="EPSG:28992")

        # Convert all geometries in the GeoDataFrame to MultiPolygons to comply with Ribasim 2024.11
        basin_area["geometry"] = basin_area["geometry"].apply(
            lambda geom: MultiPolygon([geom]) if isinstance(geom, Polygon) else geom
        )

        # comply to Ribasim 2024.11
        basin_node["meta_node_id"] = basin_node["node_id"].copy().astype(int)
        basin_area["meta_node_id"] = basin_area["node_id"].copy().astype(int)
        basin_node = basin_node.set_index("node_id")
        return basin_node, basin_profile, basin_static, basin_state, basin_area

    def tabulated_rating_curve(self):
        """_summary_

        Returns
        -------
        _type_
            _description_
        """
        TRC_nodes = self.nodes.loc[self.nodes["type"] == "TabulatedRatingCurve"][["node_id", "geometry"]]
        TRC_nodes = TRC_nodes.reset_index(drop=True)

        # dummy data to construct the rating curves
        Qh1 = pd.DataFrame()
        Qh1["node_id"] = TRC_nodes["node_id"]
        Qh1["level"] = 0.0
        Qh1["flow_rate"] = 0.0

        Qh2 = pd.DataFrame()
        Qh2["node_id"] = TRC_nodes["node_id"]
        Qh2["level"] = 1.0
        Qh2["flow_rate"] = 1.0

        Qh = pd.concat([Qh1, Qh2])
        Qh = Qh.sort_values(by=["node_id", "level"]).reset_index(drop=True)
        Qh.index += 1

        rating_curve_node = pd.DataFrame()
        rating_curve_node["node_id"] = TRC_nodes["node_id"]
        rating_curve_node["node_type"] = "TabulatedRatingCurve"
        rating_curve_node["name"] = np.nan
        rating_curve_node["subnetwork_id"] = np.nan
        rating_curve_node["geometry"] = TRC_nodes["geometry"]
        # rating_curve_node = rating_curve_node.reset_index(drop=True)

        rating_curve_static = pd.DataFrame()
        rating_curve_static["node_id"] = Qh["node_id"]
        rating_curve_static["control_state"] = np.nan
        rating_curve_static["level"] = Qh["level"]
        rating_curve_static["flow_rate"] = Qh["flow_rate"]
        rating_curve_static["control_state"] = np.nan
        # rating_curve_static = rating_curve_static.reset_index(drop=True)

        # comply to Ribasim 2024.11
        rating_curve_node["meta_node_id"] = rating_curve_node["node_id"].copy().astype(int)
        rating_curve_node = rating_curve_node.set_index("node_id")

        return rating_curve_node, rating_curve_static

    def pump(self):
        """_summary_

        Returns
        -------
        _type_
            _description_
        """
        pump_nodes = self.nodes.loc[self.nodes["type"] == "Pump"][["node_id", "geometry"]]  # .node_id.to_numpy()
        pump_nodes = pump_nodes.reset_index(drop=True)

        pump_node = pd.DataFrame()
        pump_node["node_id"] = pump_nodes["node_id"]
        pump_node["node_type"] = "Pump"
        pump_node["name"] = np.nan
        pump_node["subnetwork_id"] = np.nan
        pump_node["geometry"] = pump_nodes["geometry"]

        pump_static = pd.DataFrame()
        pump_static["node_id"] = pump_nodes["node_id"]
        pump_static["active"] = np.nan
        pump_static["flow_rate"] = 0
        pump_static["min_flow_rate"] = np.nan
        pump_static["max_flow_rate"] = np.nan
        pump_static["control_state"] = np.nan

        # comply to Ribasim 2024.11
        pump_node["meta_node_id"] = pump_node["node_id"].copy().astype(int)
        pump_node = pump_node.set_index("node_id")

        return pump_node, pump_static

    def level_boundary(self):
        """_summary_

        Returns
        -------
        _type_
            _description_
        """
        level_boundary_nodes = self.nodes.loc[self.nodes["type"] == "LevelBoundary"][
            ["node_id", "geometry"]
        ]  # .node_id.to_numpy()
        level_boundary_nodes = level_boundary_nodes.reset_index(drop=True)

        level_boundary_node = pd.DataFrame()
        level_boundary_node["node_id"] = level_boundary_nodes["node_id"]
        level_boundary_node["node_type"] = "LevelBoundary"
        level_boundary_node["name"] = np.nan
        level_boundary_node["subnetwork_id"] = np.nan
        level_boundary_node["geometry"] = level_boundary_nodes["geometry"]

        level_boundary_static = pd.DataFrame()
        level_boundary_static["node_id"] = level_boundary_nodes["node_id"]
        level_boundary_static["active"] = np.nan
        level_boundary_static["level"] = 0

        # comply to Ribasim 2024.11
        level_boundary_node["meta_node_id"] = level_boundary_node["node_id"].copy().astype(int)
        level_boundary_node = level_boundary_node.set_index("node_id")

        return level_boundary_node, level_boundary_static

    def flow_boundary(self):
        """_summary_

        Returns
        -------
        _type_
            _description_
        """
        flow_boundary_nodes = self.nodes.loc[self.nodes["type"] == "FlowBoundary"][
            ["node_id", "geometry"]
        ]  # .node_id.to_numpy()
        flow_boundary_nodes = flow_boundary_nodes.reset_index(drop=True)

        flow_boundary_node = pd.DataFrame()
        flow_boundary_node["node_id"] = flow_boundary_nodes["node_id"]
        flow_boundary_node["node_type"] = "FlowBoundary"
        flow_boundary_node["name"] = np.nan
        flow_boundary_node["subnetwork_id"] = np.nan
        flow_boundary_node["geometry"] = flow_boundary_nodes["geometry"]

        flow_boundary_static = pd.DataFrame()
        flow_boundary_static["node_id"] = flow_boundary_nodes["node_id"]
        flow_boundary_static["active"] = np.nan
        flow_boundary_static["flow_rate"] = 0

        # comply to Ribasim 2024.11
        flow_boundary_node["meta_node_id"] = flow_boundary_node["node_id"].copy().astype(int)
        flow_boundary_node = flow_boundary_node.set_index("node_id")

        return flow_boundary_node, flow_boundary_static

    def linear_resistance(self):
        """_summary_

        Returns
        -------
        _type_
            _description_
        """
        linear_resistance = ribasim.LinearResistance(static=pd.DataFrame(data={"node_id": [], "resistance": []}))

        return linear_resistance

    def manning_resistance(self):
        """_summary_

        Returns
        -------
        _type_
            _description_
        """
        manning_resistance_nodes = self.nodes.loc[self.nodes["type"] == "ManningResistance"][["node_id", "geometry"]]
        manning_resistance_nodes = manning_resistance_nodes.reset_index(drop=True)

        manning_resistance_node = pd.DataFrame()
        manning_resistance_node["node_id"] = manning_resistance_nodes["node_id"]
        manning_resistance_node["node_type"] = "ManningResistance"
        manning_resistance_node["name"] = np.nan
        manning_resistance_node["subnetwork_id"] = np.nan
        manning_resistance_node["geometry"] = manning_resistance_nodes["geometry"]

        manning_resistance_static = pd.DataFrame()
        manning_resistance_static["node_id"] = manning_resistance_nodes["node_id"]
        manning_resistance_static["active"] = np.nan
        manning_resistance_static["length"] = 1000
        manning_resistance_static["manning_n"] = 0.02
        manning_resistance_static["profile_width"] = 2
        manning_resistance_static["profile_slope"] = 3

        # comply to Ribasim 2024.11
        manning_resistance_node["meta_node_id"] = manning_resistance_node["node_id"].copy().astype(int)
        manning_resistance_node = manning_resistance_node.set_index("node_id")

        return manning_resistance_node, manning_resistance_static

    def terminal(self):
        """_summary_

        Returns
        -------
        _type_
            _description_
        """
        # terminal = ribasim.Terminal(static=pd.DataFrame(data={"node_id": self.nodes.loc[self.nodes["type"] == "Terminal"]["node_id"]}))
        terminal_nodes = self.nodes.loc[self.nodes["type"] == "Terminal"][["node_id", "geometry"]]
        terminal_nodes = terminal_nodes.reset_index(drop=True)

        terminal_node = pd.DataFrame()
        terminal_node["node_id"] = terminal_nodes["node_id"]
        terminal_node["node_type"] = "Terminal"
        terminal_node["name"] = np.nan
        terminal_node["subnetwork_id"] = np.nan
        terminal_node["geometry"] = terminal_nodes["geometry"]

        # comply to Ribasim 2024.11
        terminal_node["meta_node_id"] = terminal_node["node_id"].copy().astype(int)
        terminal_node = terminal_node.set_index("node_id")

        return terminal_node

    def outlet(self, model):
        """_summary_

        Returns
        -------
        _type_
            _description_
        """
        outlet = ribasim.Outlet(static=pd.DataFrame(data={"node_id": [], "flow_rate": []}))
        return outlet

    def discrete_control(self):
        """_summary_

        Returns
        -------
        _type_
            _description_
        """
        condition = pd.DataFrame(
            data={
                "node_id": [],
                "listen_feature_id": [],
                "variable": "level",
                "threshold_high": 0.52,
            }
        )

        logic = pd.DataFrame(
            data={
                "node_id": [],
                "truth_state": ["T", "F"],
                "control_state": ["divert", "close"],
            }
        )

        discrete_control = ribasim.DiscreteControl(condition=condition, logic=logic)

        return discrete_control

    def Pid_control(self):
        """_summary_

        Returns
        -------
        _type_
            _description_
        """
        pid_control = ribasim.PidControl(
            time=pd.DataFrame(
                data={
                    "node_id": [],
                    "time": [],
                    "listen_node_id": [],
                    "target": [],
                    "proportional": [],
                    "integral": [],
                    "derivative": [],
                }
            )
        )
        return pid_control

    def create(
        self,
        node=None,
        link=None,
        basin=None,
        pump=None,
        tabulated_rating_curve=None,
        level_boundary=None,
        flow_boundary=None,
        linear_resistance=None,
        manning_resistance=None,
        terminal=None,
        outlet=None,
        discrete_control=None,
        pid_control=None,
    ):
        """_summary_

        Parameters
        ----------
        node : _type_, optional
            _description_, by default None
        link : _type_, optional
            _description_, by default None
        basin : _type_, optional
            _description_, by default None
        pump : _type_, optional
            _description_, by default None
        tabulated_rating_curve : _type_, optional
            _description_, by default None
        level_boundary : _type_, optional
            _description_, by default None
        flow_boundary : _type_, optional
            _description_, by default None
        linear_resistance : _type_, optional
            _description_, by default None
        manning_resistance : _type_, optional
            _description_, by default None
        terminal : _type_, optional
            _description_, by default None
        outlet : _type_, optional
            _description_, by default None
        discrete_control : _type_, optional
            _description_, by default None
        pid_control : _type_, optional
            _description_, by default None

        Returns
        -------
        _type_
            _description_
        """
        model = ribasim.Model(
            modelname=self.model_characteristics["waterschap"]
            + "_"
            + self.model_characteristics["modelname"]
            + "_"
            + self.model_characteristics["modeltype"],
            network=ribasim.Network(node=node, link=link),
            basin=basin,
            flow_boundary=flow_boundary,
            pump=pump,
            tabulated_rating_curve=tabulated_rating_curve,
            terminal=terminal,
            starttime=self.model_characteristics["starttime"],
            endtime=self.model_characteristics["endtime"],
            level_boundary=level_boundary,
            #                               linear_resistance=linear_resistance,
            manning_resistance=manning_resistance,
            #                               outlet=outlet,
            #                               discrete_control=discrete_control,
            #                               pid_control=pid_control,
            #                               solver= self.model_characteristics['solver'],
            #                               logging= self.model_characteristics['logging'],
        )

        return model

    def check(self, model, post_processed_data, crossings):
        """_summary_

        Parameters
        ----------
        post_processed_data : _type_
            _description_

        Returns
        -------
        _type_
            _description_
        """
        checks = {}

        # prevent having multiple geometries columns
        nodes = self.nodes[["node_id", "type", "geometry"]]

        ##### identify the sinks #####
        unique_first = np.unique(self.links["from"].astype(int))  # Get the unique nodes from the first column
        unique_second = np.unique(self.links["to"].astype(int))  # Get the unique integers from the second column
        peilgebied_to_sink = (
            np.setdiff1d(unique_second, unique_first) + 1
        )  # = the basins which are assumed to be Terminals. Plus one due to indexing

        sinks = nodes.loc[nodes.node_id.isin(peilgebied_to_sink)]
        sinks = sinks.loc[sinks["type"] == "Basin"]
        sinks = gpd.GeoDataFrame(sinks, geometry="geometry")

        peilgebied_to_source = np.setdiff1d(unique_first, unique_second)  # = the basins which are assumed to be sources
        sources = nodes.loc[nodes.node_id.isin(peilgebied_to_source)]
        sources = sources.loc[sources["type"] == "Basin"]
        sources = gpd.GeoDataFrame(sources, geometry="geometry")

        checks["sinks"] = sinks
        checks["sources"] = sources

        ##### identify basins without connections #####
        actual_ptp = crossings
        pg_from_to = pd.concat([actual_ptp.peilgebied_from, actual_ptp.peilgebied_to])  # .to_numpy().astype(int)
        pg_from_to = pd.to_numeric(
            pg_from_to, errors="coerce"
        )  # handle NaNs, while converting all string values to integers

        basins_without_connection = nodes[~nodes.node_id.isin(pg_from_to)]
        basins_without_connection = basins_without_connection.loc[basins_without_connection["type"] == "Basin"]
        basins_without_connection = gpd.GeoDataFrame(basins_without_connection, geometry="geometry")
        checks["basins_without_connection"] = basins_without_connection

        ##### identify uncoupled pumps #####
        # Create an additional column to identify which gemalen are used. Default is set to False. Load in the original gdf without any filters
        routing_for_gemaal = gpd.read_file(
            self.model_characteristics["path_crossings"], layer="crossings_hydroobject_filtered"
        )  # all crossings, no filtering on in_use or agg1_used
        post_processed_data["gemaal"]["in_use"] = False  # add column

        # Identify the unused pumps. Set to true if the globalid of a pump occurs in the crossing table
        gemaal_used = routing_for_gemaal.dropna(subset="gemaal").gemaal  # extract the global id's
        post_processed_data["gemaal"].loc[post_processed_data["gemaal"].globalid.isin(gemaal_used), "in_use"] = True
        checks["gemaal_in_use_True_False"] = post_processed_data["gemaal"]

        ##### collect the peilgebieden #####
        pg_st = post_processed_data["peilgebied"].merge(  # pg_st = peilgebied with streefpeil
            post_processed_data["streefpeil"],
            left_on="globalid",
            right_on="globalid",
            suffixes=("", "_streefpeil"),
        )[["waterhoogte", "code", "globalid", "geometry"]]
        checks["peilgebied_met_streefpeil"] = pg_st

        # RHWS_NHWS = post_processed_data['peilgebied'].loc[post_processed_data['peilgebied'].peilgebied_cat != 0, 'globalid'].unique()

        # #peilgebieden which afwater on the boezem / NHWS
        # afwateren_RHWS_NHWS = crossings.loc[crossings.peilgebied_to_original.isin(RHWS_NHWS) | pd.isna(crossings['peilgebied_to_original'])] #also select Nones
        # afwateren_RHWS_NHWS = afwateren_RHWS_NHWS.loc[afwateren_RHWS_NHWS.crossing_type.str.contains('01|-10|02|-20')].reset_index(drop=True) #only select regular peilgebieden
        # try:
        #     afwateren_RHWS_NHWS = post_processed_data['aggregation_areas'].loc[post_processed_data['aggregation_areas'].globalid.isin(afwateren_RHWS_NHWS.agg_area_from)]
        # except Exception:
        #     print("Warning: no afwaterende peilgebieden found in checks['peilgebied_met_afwatering_op_hoofdwatersysteem']")
        #     afwateren_RHWS_NHWS = gpd.GeoDataFrame()
        #     afwateren_RHWS_NHWS['geometry'] = None

        # checks['peilgebied_met_afwatering_op_hoofdwatersysteem'] = afwateren_RHWS_NHWS#[['code', 'globalid', 'geometry', 'peilgebied_cat']]

        # #peilgebieden which inlaat water from the boezem / NHWS
        # inlaten_RHWS_NHWS = crossings.loc[crossings.peilgebied_from_original.isin(RHWS_NHWS) | pd.isna(crossings['peilgebied_to_original'])] #also select Nones
        # inlaten_RHWS_NHWS = inlaten_RHWS_NHWS.loc[inlaten_RHWS_NHWS.crossing_type.str.contains('01|-10|02|-20')].reset_index(drop=True) #only select regular peilgebieden
        # # inlaten_RHWS_NHWS = post_processed_data['aggregation_areas'].loc[post_processed_data['aggregation_areas'].globalid.isin(inlaten_RHWS_NHWS.agg_area_to)]
        # try:
        #     inlaten_RHWS_NHWS = post_processed_data['aggregation_areas'].loc[post_processed_data['aggregation_areas'].globalid.isin(inlaten_RHWS_NHWS.agg_area_to)]
        # except Exception:
        #     print("Warning: no inlatende peilgebieden found in checks['peilgebied_met_inlaat_van_hoofdwatersysteem']")
        #     inlaten_RHWS_NHWS = gpd.GeoDataFrame()
        #     inlaten_RHWS_NHWS['geometry'] = None

        # checks['peilgebied_met_inlaat_van_hoofdwatersysteem'] = inlaten_RHWS_NHWS#[['code', 'globalid', 'geometry', 'peilgebied_cat']]

        # peilgebieden which are the boezem
        checks["boezem"] = post_processed_data["peilgebied"].loc[post_processed_data["peilgebied"].peilgebied_cat > 0][
            ["code", "globalid", "geometry", "peilgebied_cat"]
        ]

        # also store the crossings. Change geometry type to string for the unnused geometry columns
        checks["HKV_internal_crossings"] = crossings.copy()
        checks["HKV_internal_crossings"] = checks["HKV_internal_crossings"].drop(
            columns=["from_centroid_geometry", "to_centroid_geometry", "from_polygon_geometry", "to_polygon_geometry"]
        )

        checks["duikersifonhevel"] = post_processed_data["duikersifonhevel"]
        checks["hydroobjecten"] = post_processed_data["hydroobject"]

        # nodes which are
        # 	  1. Inlaat
        #     2. Uitlaat stuw
        #     3. Uitlaat gemaal
        #     4. Verbindingen tussen boezems
        #     5. Verbindingen met buitenwater
        # CHECKS INLAAT UITLAAT WEGHALEN

        basin_nodes = (
            model.basin.state.df.copy()
        )  # .loc[model.basin.state.df['node_type'] == 'Basin'] #select all basins
        # model.basin.node.df.index += 1 #RB: outcommented plus one
        basin_nodes["geometry"] = model.basin.node.df.geometry  # add geometry column
        basin_nodes = gpd.GeoDataFrame(basin_nodes, geometry="geometry")  # convert from pd go gpd

        points_within = gpd.sjoin(
            basin_nodes, checks["boezem"], how="inner", predicate="within"
        )  # find the basins which are within a peilgebied (found in the checks)
        boezem_nodes = model.basin.state.df.node_id.loc[points_within.index]

        # display(boezem_nodes)
        # the boezem nodes have been identified. Now determine the five different categories.

        # determine from and to boezem nodes
        nodes_from_boezem = model.link.df.loc[
            model.link.df.from_node_id.isin(boezem_nodes)
        ]  # select ALL NODES which originate FROM the boezem
        nodes_to_boezem = model.link.df.loc[
            model.link.df.to_node_id.isin(boezem_nodes)
        ]  # select ALL NODES which go TO the boezem

        # inlaten_TRC
        inlaten_TRC = nodes_from_boezem.loc[
            (nodes_from_boezem.meta_to_node_type == "TabulatedRatingCurve")
            | (nodes_from_boezem.meta_to_node_type == "Outlet")
        ]
        inlaten_TRC = inlaten_TRC["to_node_id"]
        inlaten_TRC = model.tabulated_rating_curve.node.df.loc[
            model.tabulated_rating_curve.node.df.index.isin(inlaten_TRC)  # df.node_id --> df.index
        ]

        # add the outlets if this code is already ran before
        if model.outlet.node.df is not None:
            inlaten_outlet = model.outlet.node.df.loc[model.outlet.node.df.node_id.isin(inlaten_TRC)]
            inlaten_TRC = pd.concat([inlaten_TRC, inlaten_outlet])

        inlaten_TRC["meta_type_verbinding"] = "Inlaat"

        # inlaten_gemalen
        inlaten_gemalen = nodes_from_boezem.loc[nodes_from_boezem.meta_to_node_type == "Pump"]
        inlaten_gemalen = inlaten_gemalen["to_node_id"]
        inlaten_gemalen = model.pump.node.df.loc[
            model.pump.node.df.index.isin(inlaten_gemalen)
        ]  # df.node_id --> df.index
        inlaten_gemalen["meta_type_verbinding"] = "Inlaat"

        # inlaten_flowboundary
        inlaten_flowboundary = nodes_to_boezem.loc[nodes_to_boezem.meta_from_node_type == "FlowBoundary"]
        inlaten_flowboundary = inlaten_flowboundary["from_node_id"]
        inlaten_flowboundary = model.flow_boundary.node.df.loc[
            model.flow_boundary.node.df.index.isin(inlaten_flowboundary)  # df.node_id --> df.index
        ]
        inlaten_flowboundary["meta_type_verbinding"] = "Inlaat boundary"

        # uitlaten_TRC
        uitlaten_TRC = nodes_to_boezem.loc[
            (nodes_to_boezem.meta_from_node_type == "TabulatedRatingCurve")
            | (nodes_to_boezem.meta_from_node_type == "Outlet")
        ]
        uitlaten_TRC = uitlaten_TRC["from_node_id"]
        uitlaten_TRC = model.tabulated_rating_curve.node.df.loc[
            model.tabulated_rating_curve.node.df.index.isin(uitlaten_TRC)  # df.node_id --> df.index
        ]

        # uitlaten_gemalen
        uitlaten_gemalen = nodes_to_boezem.loc[nodes_to_boezem.meta_from_node_type == "Pump"]
        uitlaten_gemalen = uitlaten_gemalen["from_node_id"]
        uitlaten_gemalen = model.pump.node.df.loc[
            model.pump.node.df.index.isin(uitlaten_gemalen)
        ]  # df.node_id --> df.index
        uitlaten_gemalen["meta_type_verbinding"] = "Uitlaat"

        # add the outlets if this code is already ran before
        if model.outlet.node.df is not None:
            uitlaten_outlet = model.outlet.node.df.loc[model.outlet.node.df.node_id.isin(uitlaten_TRC)]
            uitlaten_TRC = pd.concat([uitlaten_TRC, uitlaten_outlet])

        uitlaten_TRC["meta_type_verbinding"] = "Uitlaat"

        # uitlaten_flowboundary
        uitlaten_flowboundary = nodes_to_boezem.loc[nodes_to_boezem.meta_from_node_type == "FlowBoundary"]
        uitlaten_flowboundary = uitlaten_flowboundary["from_node_id"]
        uitlaten_flowboundary = model.flow_boundary.node.df.loc[
            model.flow_boundary.node.df.index.isin(uitlaten_flowboundary)  # df.node_id --> df.index
        ]
        uitlaten_flowboundary["meta_type_verbinding"] = "Inlaat boundary"

        inlaten_uitlaten = pd.concat(
            [inlaten_TRC, inlaten_gemalen, inlaten_flowboundary, uitlaten_TRC, uitlaten_gemalen, uitlaten_flowboundary]
        )

        # verbinding_boezems1 = nodes_from_boezem.loc[
        #     nodes_from_boezem.to_node_type == "ManningResistance"
        # ]  # retrieve the nodes from the boezems in both ways (this line: from)
        # verbinding_boezems2 = nodes_to_boezem.loc[
        #     nodes_to_boezem.to_node_type == "ManningResistance"
        # ]  # retrieve the nodes from the boezems in both ways (this line: to)
        # verbinding_boezems = gpd.GeoDataFrame(
        #     pd.concat([verbinding_boezems1, verbinding_boezems2])
        #     .drop_duplicates(subset=["from_node_id", "to_node_id"])
        #     .reset_index(drop=True),
        #     geometry="geometry",
        # )

        # repeat for the boezems which are connected with the buitenwater.
        # this has to be done for the FlowBoundary, LevelBoundary and Terminal
        # for both from_boezem as well as to_boezem

        # the difference here is that the boundary nodes are not 'connecting nodes'
        # we first need to identify the connecting nodes, such as TRC and pumps, which originate from the boundaries
        # after that has been done, these nodes should be filtered based on whether they are connected with the nodes_from/to_boezem
        # BCN = Boundary Connection Nodes
        condition_BCN_to_pump = nodes_from_boezem.meta_to_node_type == "Pump"
        condition_BCN_to_TRC = nodes_from_boezem.meta_to_node_type == "TabulatedRatingCurve"
        condition_BCN_to_outlet = nodes_from_boezem.meta_to_node_type == "Outlet"
        condition_BCN_from_pump = nodes_to_boezem.meta_from_node_type == "Pump"
        condition_BCN_from_TRC = nodes_to_boezem.meta_from_node_type == "TabulatedRatingCurve"
        condition_BCN_from_outlet = nodes_to_boezem.meta_from_node_type == "Outlet"

        BCN_from = nodes_from_boezem.loc[
            condition_BCN_to_pump | condition_BCN_to_TRC | condition_BCN_to_outlet
        ]  # retrieve the BCN from the boezems in both ways (this line: from)
        BCN_to = nodes_to_boezem.loc[
            condition_BCN_from_pump | condition_BCN_from_TRC | condition_BCN_from_outlet
        ]  # retrieve the BCN to the boezems in both ways (this line: to)

        # BCN_FROM
        # collect the nodes in from_node_id (step 1), and insert them again in the to_node_id (step 2)
        # By doing so, a filter can be applied on the FROM_node_id with the boundary nodes (step 3).
        BCN_from = BCN_from.to_node_id  # step 1
        BCN_from = model.link.df.loc[model.link.df.from_node_id.isin(BCN_from)]  # step 2
        BCN_from = BCN_from.loc[
            (BCN_from.meta_to_node_type == "FlowBoundary")
            | (BCN_from.meta_to_node_type == "LevelBoundary")
            | (BCN_from.meta_to_node_type == "Terminal")
        ]

        # look the node ids up in each table.
        BCN_from_TRC = model.tabulated_rating_curve.node.df.loc[
            model.tabulated_rating_curve.node.df.index.isin(BCN_from.from_node_id)  # df.node_id --> df.index
        ]
        BCN_from_pump = model.pump.node.df.loc[
            model.pump.node.df.index.isin(BCN_from.from_node_id)
        ]  # df.node_id --> df.index

        if model.outlet.node.df is not None:
            BCN_from_outlet = model.outlet.node.df.loc[
                model.outlet.node.df.index.isin(BCN_from)
            ]  # df.node_id --> df.index
            BCN_from = pd.concat([BCN_from_TRC, BCN_from_pump, BCN_from_outlet])
        else:
            BCN_from = pd.concat([BCN_from_TRC, BCN_from_pump])
        BCN_from["meta_type_verbinding"] = "Uitlaat boundary"

        # BCN_TO
        # collect the nodes in from_node_id (step 1), and insert them again in the to_node_id (step 2)
        # By doing so, a filter can be applied on the FROM_node_id with the boundary nodes (step 3).
        BCN_to = BCN_to.from_node_id  # step 1
        BCN_to = model.link.df.loc[model.link.df.to_node_id.isin(BCN_to)]  # step 2
        BCN_to = BCN_to.loc[
            (BCN_to.meta_from_node_type == "FlowBoundary")
            | (BCN_to.meta_from_node_type == "LevelBoundary")
            | (BCN_to.meta_from_node_type == "Terminal")
        ]
        BCN_to["meta_type_verbinding"] = "Inlaat boundary"

        # look the node ids up in each table.
        BCN_to_TRC = model.tabulated_rating_curve.node.df.loc[
            model.tabulated_rating_curve.node.df.index.isin(BCN_to.to_node_id)  # df.node_id --> df.index
        ]
        BCN_to_pump = model.pump.node.df.loc[
            model.pump.node.df.index.isin(BCN_to.to_node_id)
        ]  # df.node_id --> df.index

        if model.outlet.node.df is not None:
            BCN_to_outlet = model.outlet.node.df.loc[model.outlet.node.df.index.isin(BCN_to)]  # df.node_id --> df.index
            BCN_to = pd.concat([BCN_to_TRC, BCN_to_pump, BCN_to_outlet])
        else:
            BCN_to = pd.concat([BCN_to_TRC, BCN_to_pump])
        BCN_to["meta_type_verbinding"] = "Inlaat boundary"

        inlaten_uitlaten = pd.concat([inlaten_uitlaten, BCN_from, BCN_to])
        inlaten_uitlaten = inlaten_uitlaten.reset_index(drop=True)
        inlaten_uitlaten = gpd.GeoDataFrame(inlaten_uitlaten, geometry="geometry", crs="EPSG:28992")
        checks["inlaten_uitlaten_boezems"] = inlaten_uitlaten

        return checks

    def add_meta_data(self, model, checks, post_processed_data, crossings):
        # ### insert meta_data of the peilgebied_category ###
        model.basin.state.df["meta_categorie"] = (
            "doorgaand"  # set initially all basins to be a regular peilgebied (= peilgebied_cat 0, or 'bergend')
        )

        basin_nodes = (
            model.basin.state.df.copy()
        )  # .loc[model.basin.state.df['node_type'] == 'Basin'] #select all basins
        # model.basin.node.df.index += 1
        basin_nodes["geometry"] = model.basin.node.df.geometry  # add geometry column
        basin_nodes = gpd.GeoDataFrame(basin_nodes, geometry="geometry")  # convert from pd go gpd

        points_within = gpd.sjoin(
            basin_nodes, checks["boezem"], how="inner", predicate="within"
        )  # find the basins which are within a peilgebied (found in the checks)
        model.basin.state.df.meta_categorie.loc[points_within.index] = (
            "hoofdwater"  # set these basins to become peilgebied_cat == 1, or 'doorgaand'
        )

        ### insert meta_data of the gemaal type ###
        crossings_pump = crossings.loc[~crossings.gemaal.isna()][["node_id", "gemaal", "geometry"]]
        coupled_crossings_pump = crossings_pump.merge(
            post_processed_data["gemaal"][["globalid", "func_afvoer", "func_aanvoer", "func_circulatie"]],
            left_on="gemaal",
            right_on="globalid",
        )

        pump_function = coupled_crossings_pump.merge(model.pump.node.df, on="geometry", suffixes=("", "_duplicate"))[
            ["node_id", "func_afvoer", "func_aanvoer", "func_circulatie"]
        ]
        coupled_pump_function = model.pump.static.df.merge(pump_function, left_on="node_id", right_on="node_id")
        coupled_pump_function = coupled_pump_function.rename(
            columns={
                "func_afvoer": "meta_func_afvoer",
                "func_aanvoer": "meta_func_aanvoer",
                "func_circulatie": "meta_func_circulatie",
            }
        )
        coupled_pump_function.index.name = "fid"
        coupled_pump_function = coupled_pump_function.drop_duplicates(subset="node_id")

        # deze koppelen aan temp_pump_static
        temp_pump_static = model.pump.static.df.copy()
        temp_pump_static = temp_pump_static.merge(
            right=coupled_pump_function[["node_id", "meta_func_afvoer", "meta_func_aanvoer", "meta_func_circulatie"]],
            on="node_id",
            how="left",
        )
        temp_pump_static.index.name = "fid"
        model.pump.static.df = temp_pump_static

        ### add the peilgebied_cat flag to the links as well ###
        # first assign all links to become 'bergend'. Adjust the boezem links later.
        model.link.df["meta_categorie"] = "doorgaand"

        # find the basins which are boezems
        nodeids_boezem = model.basin.state.df.loc[model.basin.state.df.meta_categorie == "hoofdwater", "node_id"]
        model.link.df.loc[
            (model.link.df.from_node_id.isin(nodeids_boezem)) | (model.link.df.to_node_id.isin(nodeids_boezem)),
            "meta_categorie",
        ] = "hoofdwater"

        # some pumps do not have a func afvoer/aanvoer/circulatie yet (occurs rarely though)
        model.pump.static.df["meta_func_afvoer"].fillna(value=False)
        model.pump.static.df["meta_func_aanvoer"].fillna(value=False)
        model.pump.static.df["meta_func_circulatie"].fillna(value=False)

        # # if the function is not known, then choose func_afvoer
        model.pump.static.df.loc[
            (~model.pump.static.df["meta_func_afvoer"].astype(bool))
            & (~model.pump.static.df["meta_func_aanvoer"].astype(bool))
            & (~model.pump.static.df["meta_func_circulatie"].astype(bool)),
            "meta_func_afvoer",
        ] = True

        ### add a random color to the basins ###
        color_cycle = itertools.cycle(Category10[10])
        color_list = []
        for _ in range(len(model.basin.area.df)):
            color_list.append(next(color_cycle))

        # Add the color_list as a new column to the DataFrame
        model.basin.area.df["meta_color"] = color_list

        ########################################################################
        # add meta data whether some nodes are connected with a boezem.
        # This is important as i.e. these TRC's will be converted to Outlets
        # merge each table with a part of the checks['inlaten_uitlaten'] dataframe

        # TabulatedRatingCurve
        model.tabulated_rating_curve.static.df = model.tabulated_rating_curve.static.df.merge(
            checks["inlaten_uitlaten_boezems"][["meta_type_verbinding"]],
            left_index=True,
            right_index=True,
            # left_on=["node_id"],
            # right_on=["node_id"],
            how="left",
        )

        # Pump
        model.pump.static.df = model.pump.static.df.merge(
            checks["inlaten_uitlaten_boezems"][["meta_type_verbinding"]],
            left_index=True,
            right_index=True,
            # left_on=["node_id"],
            # right_on=["node_id"],
            how="left",
        )

        # FlowBoundary
        model.flow_boundary.static.df = model.flow_boundary.static.df.merge(
            checks["inlaten_uitlaten_boezems"][["meta_type_verbinding"]],
            left_index=True,
            right_index=True,
            # left_on=["node_id"],
            # right_on=["node_id"],
            how="left",
        )
        return model

    def add_relevant_names(self, model, post_processed_data, crossings):
        """Add relevant names based on the meta_name column."""
        # gemaal
        # step 1a: retrieve the coordinates of the pumps in the pump .node table
        # step 1b: retrieve the coordinates of the pumps in the crossings table
        # step 2: these coordinates should be the same as the coordinates in the crossings. Merge the globalids
        # step 3: merge the meta_name based on the globalid
        # step 4: replace the globalids for the meta_name

        # step 1a: retrieve the coordinates of the pumps in the pump .node table
        coordinates_gemaal_ribasim = model.pump.node.df[["geometry"]].copy().reset_index()

        # step 1b: retrieve the coordinates of the pumps in the crossings table
        coordinates_gemaal_crossings = crossings.loc[~crossings.gemaal.isna()]
        coordinates_gemaal_crossings = coordinates_gemaal_crossings.rename(columns={"gemaal": "globalid"})

        # step 2: these coordinates should be the same as the coordinates in the crossings. Merge the globalids
        coordinates_gemaal_ribasim = coordinates_gemaal_ribasim.merge(
            right=coordinates_gemaal_crossings[["geometry", "globalid"]], how="left", on="geometry"
        )

        # step 3: merge the meta_name based on the globalid
        coordinates_gemaal_ribasim = coordinates_gemaal_ribasim.merge(
            right=post_processed_data["gemaal"][["globalid", "meta_name"]], on="globalid", how="left"
        )
        coordinates_gemaal_ribasim = coordinates_gemaal_ribasim.set_index("node_id")

        # step 4: replace the globalids for the meta_name
        pump_node = model.pump.node.df.copy()
        pump_node = pump_node.drop(columns="name")
        pump_node = pump_node.merge(
            right=coordinates_gemaal_ribasim[["meta_name"]], left_index=True, right_index=True, how="left"
        )
        pump_node = pump_node.rename(columns={"meta_name": "name"})
        pump_node = pump_node.drop_duplicates()
        model.pump.node.df = pump_node

        # aggregation areas (AA)
        # step 1: retrieve the coordinates of the aggregation areas in the basin.area table
        # step 2: these coordinates should be the same as the coordinates in the basin.area. Merge the globalids
        # merge with the basin_node table

        # step 1: retrieve the coordinates of the aggregation areas in the basin.area table
        coordinates_AA_ribasim = model.basin.area.df[["node_id", "geometry"]].copy()

        # step 2: these coordinates should be the same as the coordinates in the basin.area. Merge the globalids
        coordinates_AA_ribasim = coordinates_AA_ribasim.merge(
            right=post_processed_data["aggregation_areas"][["meta_name", "geometry"]], how="left", on="geometry"
        )

        # fix column names
        coordinates_AA_ribasim = coordinates_AA_ribasim.rename(columns={"meta_name": "name"})

        # merge with the basin_node table
        coordinates_AA_ribasim = coordinates_AA_ribasim.set_index("node_id")[["name"]]
        coordinates_AA_ribasim = model.basin.node.df.merge(
            right=coordinates_AA_ribasim, left_index=True, right_index=True, how="left", suffixes=("_to_remove", "")
        )

        coordinates_AA_ribasim = coordinates_AA_ribasim.drop(columns="name_to_remove")
        coordinates_AA_ribasim = coordinates_AA_ribasim.drop_duplicates()

        model.basin.node.df = coordinates_AA_ribasim

        return model

    def store_data(data, output_path):
        """_summary_

        Parameters
        ----------
        data : _type_
            _description_
        output_path : _type_
            _description_
        """
        for key in data.keys():
            data[str(key)].to_file(output_path + ".gpkg", layer=str(key), driver="GPKG")

        return

    def WriteResults(self, model, checks):
        """_summary_

        Parameters
        ----------
        model : _type_
            _description_
        checks : _type_
            _description_
        """
        dir_path = settings.ribasim_nl_data_dir
        dir_path = os.path.join(dir_path, self.model_characteristics["waterschap"])

        ##### write the model to the Z drive #####
        if self.model_characteristics["write_Zdrive"]:
            dir_path = f"../../../../Ribasim_networks/Waterschappen/{self.model_characteristics['waterschap']}/modellen/{self.model_characteristics['waterschap']}_{self.model_characteristics['modeltype']}"

            if not os.path.exists(dir_path):
                os.makedirs(dir_path)
            else:
                for filename in os.listdir(dir_path):  # delete outdated models in de original folder
                    file_path = os.path.join(dir_path, filename)
                    if os.path.isfile(file_path):
                        os.remove(file_path)

            path_ribasim = os.path.join(
                dir_path,
                "",
                "modellen",
                "",
                self.model_characteristics["waterschap"] + "_" + self.model_characteristics["modeltype"],
                "ribasim.toml",
            )
            model.write(path_ribasim)

        ##### write to the P drive #####
        if self.model_characteristics["write_Pdrive"]:
            P_path = self.model_characteristics["path_Pdrive"]
            P_path = os.path.join(
                P_path,
                self.model_characteristics["waterschap"],
                "modellen",
                self.model_characteristics["waterschap"] + "_" + self.model_characteristics["modeltype"],
                self.model_characteristics["waterschap"]
                + "_"
                + self.model_characteristics["modelname"]
                + "_"
                + self.model_characteristics["modeltype"],
            )

            if not os.path.exists(P_path):
                os.makedirs(P_path)

            P_path = os.path.join(
                P_path,
                f"{self.model_characteristics['waterschap']}_{self.model_characteristics['modelname']}_{self.model_characteristics['modeltype']}_ribasim.toml",
            )

            model.write(P_path)

            # write checks to the P drive
            RibasimNetwork.store_data(
                data=checks,
                output_path=str(P_path + "visualisation_checks"),
            )

        ##### copy symbology for the Ribasim model #####
        if self.model_characteristics["write_symbology"]:
            checks_symbology_path = os.path.join(
                settings.ribasim_nl_data_dir, "Basisgegevens/QGIS_qlr/visualisation_Ribasim.qlr"
            )
            checks_symbology_path_new = os.path.join(
                dir_path,
                "modellen",
                self.model_characteristics["waterschap"] + "_" + self.model_characteristics["modeltype"],
                "visualisation_Ribasim.qlr",
            )

            # check if the directory exists, if not create it
            if not os.path.exists(os.path.dirname(checks_symbology_path_new)):
                os.makedirs(os.path.dirname(checks_symbology_path_new))
            # copy checks_symbology file from old dir to new dir
            shutil.copy(src=checks_symbology_path, dst=checks_symbology_path_new)

            if self.model_characteristics["write_Pdrive"]:
                # write Ribasim model to the P drive
                P_path = self.model_characteristics["path_Pdrive"]
                P_path = os.path.join(
                    P_path,
                    self.model_characteristics["waterschap"],
                    "modellen",
                    self.model_characteristics["waterschap"] + "_" + self.model_characteristics["modeltype"],
                    self.model_characteristics["waterschap"]
                    + "_"
                    + self.model_characteristics["modelname"]
                    + "_"
                    + self.model_characteristics["modeltype"],
                )

                if not os.path.exists(P_path):
                    os.makedirs(P_path)

                P_path_ribasim = os.path.join(P_path, "ribasim.toml")
                model.write(P_path_ribasim)

                shutil.copy(
                    src=checks_symbology_path_new,
                    dst=os.path.join(
                        P_path,
                        "visualisation_Ribasim.qlr",
                    ),
                )

        # write model locally
        output_locally = os.path.join(
            dir_path,
            "modellen",
            self.model_characteristics["waterschap"] + "_" + self.model_characteristics["modeltype"],
        )
        if not os.path.exists(output_locally):
            os.makedirs(output_locally)
        model.write(filepath=output_locally + "/ribasim.toml")

        ##### write the checks #####
        if self.model_characteristics["write_checks"]:
            print("dir_path = ", dir_path)
            print(
                os.path.join(
                    dir_path,
                    "modellen",
                    self.model_characteristics["waterschap"] + "_" + self.model_characteristics["modeltype"],
                    "database_checks",
                )
            )
            print("Done")

            RibasimNetwork.store_data(
                data=checks,
                output_path=os.path.join(
                    dir_path,
                    "modellen",
                    self.model_characteristics["waterschap"] + "_" + self.model_characteristics["modeltype"],
                    "database_checks",
                ),
            )

        ##### copy symbology for the CHECKS data #####
        if self.model_characteristics["write_symbology"]:
            # dont change the paths below!

            checks_symbology_path = os.path.join(
                settings.ribasim_nl_data_dir, "Basisgegevens/QGIS_qlr/visualisation_checks.qlr"
            )

            checks_symbology_path_new = os.path.join(
                dir_path,
                "modellen",
                self.model_characteristics["waterschap"] + "_" + self.model_characteristics["modeltype"],
                "visualisation_checks.qlr",
            )

            # copy checks_symbology file from old dir to new dir
            shutil.copy(src=checks_symbology_path, dst=checks_symbology_path_new)

            if self.model_characteristics["write_Pdrive"]:
                # write Ribasim model to the P drive
                P_path = self.model_characteristics["path_Pdrive"]
                P_path = os.path.join(
                    P_path,
                    self.model_characteristics["waterschap"],
                    "modellen",
                    self.model_characteristics["waterschap"] + "_" + self.model_characteristics["modeltype"],
                    self.model_characteristics["waterschap"]
                    + "_"
                    + self.model_characteristics["modelname"]
                    + "_"
                    + self.model_characteristics["modeltype"],
                )

                if not os.path.exists(P_path):
                    os.makedirs(P_path)

                P_path_ribasim = os.path.join(P_path, "ribasim.toml")
                model.write(P_path_ribasim)

                shutil.copy(
                    src=checks_symbology_path_new,
                    dst=os.path.join(
                        P_path,
                        "visualisation_Ribasim.qlr",
                    ),
                )

        if self.model_characteristics["write_goodcloud"]:
            cloud_storage = CloudStorage()

            cloud_storage.upload_model(
                authority=self.model_characteristics["waterschap"],
                model=self.model_characteristics["waterschap"] + "_" + self.model_characteristics["modeltype"],
            )

        print("Done")
