import os
import shutil

import geopandas as gpd
import numpy as np
import pandas as pd
import ribasim
from ribasim_nl import CloudStorage
from shapely.geometry import Point, LineString
from shapely.wkt import loads


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
        self, variables=["hydroobject", "gemaal", "stuw", "peilgebied", "streefpeil"], print_var=False, data=None
    ):
        """Read specified layers from a GeoPackage file and store them in the object.

        Parameters
        ----------
        variables : list, optional
            List of layer names to be read from the GeoPackage, by default
            ["hydroobject", "gemaal", "stuw", "peilgebied", "streefpeil", "aggregation_area"]
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
        for variable in variables:
            if print_var:
                print(variable)
            data_temp = gpd.read_file(self.model_characteristics["path_postprocessed_data"], layer=variable)
            data[variable] = data_temp

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
            crossings = crossings[crossings.in_use == True].reset_index(drop=True)  # only use the crossings in use
        if self.model_characteristics["agg_links_in_use"]:
            crossings = crossings[crossings.agg_links_in_use == True].reset_index(drop=True)  # only use the crossings in use
        if self.model_characteristics["agg_areas_in_use"]:
            crossings = crossings[crossings.agg_areas_in_use == True].reset_index(drop=True)  # only use the crossings in use

        crossings["geometry"] = gpd.GeoSeries(
            gpd.points_from_xy(x=crossings["geometry"].x, y=crossings["geometry"].y)
        )  # prevent strange geometries

        if self.model_characteristics['aggregation'] == True:
            aggregation_areas = gpd.read_file(self.model_characteristics["path_crossings"], layer='aggregation_area').to_crs(crs='EPSG:28992')
        else:
            aggregation_areas = None
        post_processed_data['aggregation_areas'] = aggregation_areas

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

        #save the original peilgebied_from and _to for boezem purposes later, before changing anything due to the aggregation
        crossings['peilgebied_from_original'] = crossings['peilgebied_from']
        crossings['peilgebied_to_original'] = crossings['peilgebied_to']
        

        
        if self.model_characteristics['aggregation'] == True:
            #not all peilgebieden are part of a aggregation area. Add the missing peilgebieden to the agg_area column, so no areas will be left out
            crossings['agg_area_from'].fillna(crossings['peilgebied_from'], inplace = True)
            crossings['agg_area_to'].fillna(crossings['peilgebied_to'], inplace = True)
            
            #change administrative detailed peilgebieden to aggregation areas
            crossings['peilgebied_from'] = crossings['agg_area_from']
            crossings['peilgebied_to'] = crossings['agg_area_to']

            post_processed_data["aggregation_areas"] = post_processed_data["aggregation_areas"].rename(columns={'CODE': 'code'}) #globalid is mandatory for later in the algorithm
            post_processed_data["aggregation_areas"]['globalid'] = post_processed_data["aggregation_areas"]['code'] #globalid is mandatory for later in the algorithm
            
            #instead of copy pasting the aggregation areas to the peilgebieden, concat it. By doing so, also the peilgebieden which do not fall into an aggregation area are taken into consideration
            post_processed_data["peilgebied"] = pd.concat([post_processed_data["aggregation_areas"][['code', 'globalid', 'geometry']], post_processed_data["peilgebied"][['code', 'globalid', 'peilgebied_cat', 'geometry']]])
            post_processed_data["peilgebied"] = gpd.GeoDataFrame(post_processed_data["peilgebied"], geometry = 'geometry')

            if self.model_characteristics['waterschap'] != 'Delfland':
                print('Warning: a data specific algorithm has been made for Delfland. This will be changed later. See line below with CODE and globalid')

            
        
        post_processed_data["peilgebied"]["centroid_geometry"] = post_processed_data[
            "peilgebied"
        ].representative_point()

        # import matplotlib.pyplot as plt
        # fig, ax = plt.subplots()
        # post_processed_data["peilgebied"].plot(ax=ax)
        # post_processed_data["peilgebied"]["centroid_geometry"].plot(ax=ax, color='red')

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
        letters_from = crossings[crossings["peilgebied_from"].str.contains("[a-zA-Z]", na=False)]
        letters_to = crossings[crossings["peilgebied_to"].str.contains("[a-zA-Z]", na=False)]
        letters_df = pd.concat([letters_from["peilgebied_from"], letters_to["peilgebied_to"]])
        letters_df = letters_df.unique()

        # identify globalids which contain numbers / any other symbols
        numbers_from = crossings[~crossings["peilgebied_from"].str.contains("[a-zA-Z]", na=False)]
        numbers_to = crossings[~crossings["peilgebied_to"].str.contains("[a-zA-Z]", na=False)]
        numbers_df = pd.concat([numbers_from["peilgebied_from"], numbers_to["peilgebied_to"]])
        numbers_df = numbers_df.unique()

        if numbers_df[0] is not None:  # detect the largest number
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
        basin_max = max(max(basin_from_max), max(basin_to_max))

        crossings = crossings.reset_index(drop=True)
        crossings["node_id"] = crossings.index + basin_max + 1

        return crossings

    def create_edges(self, crossings):
        """Create edges for Ribasim network

        Create the edges, based on the peilgebied_from and peilgebied_to
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
        edges = pd.DataFrame()
        edges["from"] = lines_from_id
        edges["to"] = lines_to_id
        edges["from_coord"] = lines_from_coord
        edges["to_coord"] = lines_to_coord

        # remove NaN's, as these rows do not contain any coordinates
        edges = edges.dropna()
        edges = edges.drop(edges[(edges["from"] == -999) | (edges["to"] == -999)].index)
        edges = edges.reset_index(drop=True)

        # change the 'Point Z' to a regular 'Point' geometry.
        # print(edges["from_coord"].x)
        edges['from_x'] = edges['from_coord'].apply(lambda geom: geom.x)
        edges['from_y'] = edges['from_coord'].apply(lambda geom: geom.y)
        
        edges['to_x'] = edges['to_coord'].apply(lambda geom: geom.x)
        edges['to_y'] = edges['to_coord'].apply(lambda geom: geom.y)
        
        edges["from_coord"] = gpd.GeoSeries(
            gpd.points_from_xy(x=edges['from_x'], y=edges['from_y'])
        )
        edges["to_coord"] = gpd.GeoSeries(
            gpd.points_from_xy(x=edges['to_x'], y=edges['to_y'])
        )
        # create the LineStrings (=edges), based on the df with coordinates
        edges["line_geom"] = edges.apply(lambda row: LineString([row["from_coord"], row["to_coord"]]), axis=1)

        # edges = edges.dropna(subset=['from', 'to'])

        return edges

    def create_nodes(self, crossings, edges):
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
            TRC = crossings[
                ~crossings["crossing_type"].astype(str).str.contains("-10|2")
            ]  # the '~' makes sure only zero's are selected (peilgebied to peilgebied crossings)
            boundary = crossings[
                crossings["crossing_type"].astype(str).str.contains("-10|2")
            ]

        else:
            raise ValueError("Invalid 'modeltype'. Please use 'poldermodel' or 'boezemmodel'.")
        
        boundary = boundary.reset_index(drop=True) #can be deleted later, 20240403
        FB = boundary.loc[boundary.from_polygon_geometry.isna()] #flow boundaries let water flow into the model
        # terminals = boundary.loc[boundary.to_polygon_geometry.isna()] #terminals let water flow out of the model
        TRC_terminals = boundary.loc[~boundary.from_polygon_geometry.isna()] #terminals let water flow out of the model. they can not be connected to a basin, so create TRC's first. Connect a Terminal to it afterwards.
        FB = FB.reset_index(drop=True) #can be deleted later, 20240403
        TRC_terminals = TRC_terminals.reset_index(drop=True) #can be deleted later, 20240403

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
        nodes = pd.concat([nodes_basins, nodes_TRC, nodes_FB, nodes_TRC_ter])
        nodes = nodes.reset_index(drop=True)
        nodes.index = nodes.node_id.to_numpy()


        # embed the pumps
        pump_nodes = crossings.dropna(subset="gemaal").node_id.to_numpy()
        # nodes.loc[nodes.node_id.isin(pump_nodes) & nodes['type'].isna(), "type"] = "Pump" 
        nodes.loc[nodes.node_id.isin(pump_nodes), "type"] = "Pump" #stond eerst als de regel hierboven. Heb niet meer scherp waarom de nodes['type'] leeg MOET zijn; zo ver ik weet wordt die altijd ingevuld met een waarden (zie hierboven). Voor nu even weggehaald.
        
        if len(nodes_TRC_ter) > 0:
            #first, properly embed the Terminals in the nodes
            nodes_max = max(nodes.node_id) + 1 #add +1 as we need to add new nodes
            nodes_ter = pd.DataFrame()
            nodes_ter["node_id"] = np.arange(nodes_max, nodes_max + len(nodes_TRC_ter))
            nodes_ter["type"] = "Terminal" 
            nodes_ter['geometry'] = [Point(point.x - 1, point.y) for point in TRC_terminals["geometry"]] #move one meter to the left to place the terminal. Use the original points
            # nodes_ter['geometry'] = gpd.GeoSeries(nodes_ter['geometry'])
            nodes_ter = gpd.GeoDataFrame(nodes_ter)
            # nodes_ter["geometry"] = TRC_terminals["geometry"] #copy paste the geometry points of the TRC which go to the terminal
            nodes_ter = nodes_ter.drop_duplicates(subset="node_id")

            #second, properly embed the Terminals in the edges. Fill in the entire df for completion purposes 
            edges_ter = pd.DataFrame()
            edges_ter['from'] = TRC_terminals.node_id
            edges_ter['to'] = nodes_ter.node_id.values
            edges_ter['from_coord'] = TRC_terminals["geometry"]
            edges_ter['to_coord'] = nodes_ter['geometry'].values
            edges_ter['from_x'] = TRC_terminals["geometry"].x
            edges_ter['from_y'] = TRC_terminals["geometry"].y
            edges_ter['to_x'] = nodes_ter["geometry"].x.values
            edges_ter['to_y'] = nodes_ter['geometry'].y.values

            edges_ter['line_geom'] = edges_ter.apply(lambda row: LineString([row["from_coord"], row["to_coord"]]), axis=1)
            edges_ter.reset_index(drop=True,inplace=True)
            
            #append the nodes and the edges to the main df
            nodes = pd.concat([nodes, nodes_ter]).reset_index(drop=True)
            edges = pd.concat([edges, edges_ter]).reset_index(drop=True)

        nodes = nodes.sort_values(by='node_id')
        nodes = nodes.reset_index(drop=True)
        nodes.index +=1 
        
        return nodes, edges



    def embed_boezems(self, edges, post_processed_data, crossings): 

        if self.model_characteristics['path_boezem'] is not None:
            boezems = gpd.read_file(self.model_characteristics['path_boezem']).set_crs(crs='EPSG:28992')
            boezem_globalids = post_processed_data['peilgebied'].loc[post_processed_data['peilgebied'].peilgebied_cat == 1, 'globalid'].drop_duplicates()

            #Correct order #######################################
            boezems_df_correct_order = boezems.loc[boezems.peilgebied_from.isin(boezem_globalids)]
            
            #retrieve starting point coordinates
            temp_crossings = pd.merge(left=boezems_df_correct_order,
                                      right = crossings,
                                      left_on = ['hydroobject','peilgebied_from'],
                                      right_on = ['hydroobject','peilgebied_from_original'],
                                      how = 'left')
            
            edges_with_correct_SP = pd.merge(left = edges,
                                     right = temp_crossings,
                                     left_on = ['from_coord', 'to'],
                                     right_on = ['from_centroid_geometry', 'node_id_y'],
                                     how = 'left')

            #INCORRECT order #######################################
            boezems_df_incorrect_order = boezems.loc[boezems.peilgebied_to.isin(boezem_globalids)]

                        #retrieve starting point coordinates
            temp_crossings = pd.merge(left=boezems_df_incorrect_order,
                                      right = crossings,
                                      left_on = ['hydroobject','peilgebied_to'],
                                      right_on = ['hydroobject','peilgebied_to_original'],
                                      how = 'left')

            edges_with_incorrect_SP = pd.merge(left = edges,
                                     right = temp_crossings,
                                     left_on = ['to_coord', 'from'],
                                     right_on = ['to_centroid_geometry', 'node_id_y'],
                                     how = 'left')    
            
            edges_with_correct_SP.drop_duplicates(subset=['from', 'to'], inplace = True)
            edges_with_incorrect_SP.drop_duplicates(subset=['from', 'to'], inplace = True)
            
            edges_with_correct_SP.reset_index(drop=True, inplace=True)
            edges_with_incorrect_SP.reset_index(drop=True, inplace=True)

            #add the shortest paths columns to the edges
            edges.rename(columns={'line_geom':'line_geom_oud'}, inplace=True)
            edges['line_geom'] = np.nan
     
            edges_temp_incorrect = pd.merge(edges, edges_with_incorrect_SP, how='left', on=['from', 'to'], suffixes=('', '_incorrect_SP'))
            edges.loc[edges_temp_incorrect['shortest_path'].notnull(), 'line_geom'] = edges_temp_incorrect['shortest_path']
            edges_temp_correct = pd.merge(edges, edges_with_correct_SP, how='left', on=['from', 'to'], suffixes=('', '_correct_SP'))
            edges.loc[edges_temp_correct['shortest_path'].notnull(), 'line_geom'] = edges_temp_correct['shortest_path']            
            
            #the direction of the edges are correct on administrative level, but not yet on geometric level. Revert the lines. 
            reverse_gemaal = crossings.copy() #create copy of the crossings
            reverse_gemaal = reverse_gemaal.loc[reverse_gemaal.peilgebied_to_original.isin(boezem_globalids)] #select the rows which are a crossing with the boezem
            reverse_gemaal = reverse_gemaal.loc[~reverse_gemaal.gemaal.isna()] #select the crossings on the boezem with a gemaal
            reverse_gemaal = reverse_gemaal.node_id.unique() #select the crossings where the lines should be reverted. Revert after changing the string to a geometry object

            #fill the line geoms with the previous geoms if no shortest path is found
            edges.line_geom = edges.line_geom.fillna(edges.line_geom_oud)
            edges['line_geom'] = edges['line_geom'].astype(str).apply(loads) #change string to geometry object
            
            #revert the geometry objects on the found locations which should be reverted
            edges['line_geom'] = edges.apply(lambda row: LineString(row['line_geom'].coords[::-1]) if row['from'] in reverse_gemaal else row['line_geom'], axis=1) 
        
        return edges



            


    def important(self, edges, post_processed_data, crossings):

        if self.model_characteristics['path_boezem'] is not None:
            boezems = gpd.read_file(self.model_characteristics['path_boezem']).set_crs(crs='EPSG:28992')

            boezem_globalids = post_processed_data['peilgebied'].loc[post_processed_data['peilgebied'].peilgebied_cat == 1, 'globalid'].drop_duplicates()
            boezems = boezems.loc[boezems.crossing_type == '01']

            #Correct order #######################################
            boezems_df_correct_order = boezems.loc[boezems.peilgebied_from.isin(boezem_globalids)]

            #retrieve starting point coordinates
            temp_crossings = pd.merge(left=boezems_df_correct_order,
                                      right = crossings,
                                      left_on = ['hydroobject','peilgebied_from'],
                                      right_on = ['hydroobject','peilgebied_from_original'],
                                      how = 'left')

            edges_with_correct_SP = pd.merge(left = edges,
                                     right = temp_crossings,
                                     left_on = ['from_coord', 'to'],
                                     right_on = ['from_centroid_geometry', 'node_id_y'],
                                     how = 'left')

            #INCORRECT order #######################################
            boezems_df_incorrect_order = boezems.loc[boezems.peilgebied_to.isin(boezem_globalids)]

            #retrieve starting point coordinates
            temp_crossings = pd.merge(left=boezems_df_incorrect_order,
                                      right = crossings,
                                      left_on = ['hydroobject','peilgebied_to'],
                                      right_on = ['hydroobject','peilgebied_to_original'],
                                      how = 'left')
            # temp_crossings = temp_crossings[['peilgebied_to_x', 'to_centroid_geometry', 'shortest_path']] #behapbaar houden
            edges_with_incorrect_SP = pd.merge(left = edges,
                                     right = temp_crossings,
                                     left_on = ['to_coord', 'from'],
                                     right_on = ['to_centroid_geometry', 'node_id_y'],
                                     how = 'left')       

            edges_with_correct_SP.drop_duplicates(subset=['from', 'to'], inplace = True)
            edges_with_incorrect_SP.drop_duplicates(subset=['from', 'to'], inplace = True)
            
            # print(edges_with_correct_SP.loc[edges_with_correct_SP.node_id_y == 830, 'shortest_path'].iloc[-1])
            edges_with_correct_SP.reset_index(drop=True, inplace=True)
            edges_with_incorrect_SP.reset_index(drop=True, inplace=True)

            #add the shortest paths to the edges
            # display(edges)
            edges.rename(columns={'line_geom':'line_geom_oud'}, inplace=True)
            edges['line_geom'] = np.nan

            edges_temp_incorrect = pd.merge(edges, edges_with_incorrect_SP, how='left', on=['from', 'to'], suffixes=('', '_incorrect_SP'))
            edges.loc[edges_temp_incorrect['shortest_path'].notnull(), 'line_geom'] = edges_temp_incorrect['shortest_path']
            edges_temp_correct = pd.merge(edges, edges_with_correct_SP, how='left', on=['from', 'to'], suffixes=('', '_correct_SP'))
            edges.loc[edges_temp_correct['shortest_path'].notnull(), 'line_geom'] = edges_temp_correct['shortest_path']
            edges.line_geom = edges.line_geom.fillna(edges.line_geom_oud)
            
            edges['line_geom'] = edges['line_geom'].astype(str).apply(loads)
            # gpd.GeoDataFrame(edges, geometry='line_geom').plot()
            # display(edges)
            stop
            return edges

#####################################################################################################
#####################################################################################################
#####################################################################################################
#####################################################################################################


class RibasimNetwork:
    def __init__(self, nodes, edges, model_characteristics):
        """_summary_

        Parameters
        ----------
        nodes : _type_
            _description_
        edges : _type_
            _description_
        model_characteristics : _type_
            _description_
        """
        self.nodes = nodes
        self.edges = edges
        self.model_characteristics = model_characteristics


    def node(self):
        """_summary_

        Returns
        -------
        _type_
            _description_
        """

        node = ribasim.Node(
            df=gpd.GeoDataFrame(
                data={"node_type": self.nodes["type"]},
                index=pd.Index(self.nodes.node_id, name="fid"),
                geometry=self.nodes.geometry,
                crs="EPSG:28992",
            )
        )

        return node

    def edge(self):
        """_summary_

        Returns
        -------
        _type_
            _description_
        """
        edge = ribasim.Edge(
            df=gpd.GeoDataFrame(
                data={"from_node_id": self.edges["from"], "to_node_id": self.edges["to"], "edge_type": "flow"},
                geometry=self.edges["line_geom"],
                crs="EPSG:28992",
            )
        )
        return edge

    def basin(self):
        """_summary_

        Returns
        -------
        _type_
            _description_
        """
        basins_area = self.nodes.loc[self.nodes["type"] == "Basin"]
        basins_area = basins_area[["node_id", "streefpeil", "basins_area_geom"]]
        #         basins_area = basins_area[['node_id', 'basins_area_geom']]
        basins_area.rename(columns={"basins_area_geom": "geom"}, inplace=True)
        # basin = ribasim.Basin(
        #     profile=pd.DataFrame(
        #         data={"node_id": self.nodes["node_id"].loc[self.nodes["type"] == "Basin"], "area": 0.1, "level": 0.1}
        #     ),
        #     area=gpd.GeoDataFrame(
        #         data={"node_id": basins_area.node_id, "geometry": basins_area.geom, "meta_peil": basins_area.streefpeil}
        #     ).set_crs("EPSG:28992"),
        # )
        profile_zero = pd.DataFrame(data={"node_id": self.nodes["node_id"].loc[self.nodes["type"] == "Basin"], "area": 0.1, "level": 0.1})
        profile_one = pd.DataFrame(data={"node_id": self.nodes["node_id"].loc[self.nodes["type"] == "Basin"], "area": 3000, "level": 3})
        profile = pd.concat([profile_zero, profile_one])
        
        basin = ribasim.Basin(
            profile=profile,
            area=gpd.GeoDataFrame(
                data={"node_id": basins_area.node_id, "geometry": basins_area.geom, "meta_peil": basins_area.streefpeil}
            ).set_crs("EPSG:28992"),
        )


        return basin

    def tabulated_rating_curve(self):
        """_summary_

        Returns
        -------
        _type_
            _description_
        """
        # dummy data to construct the rating curves
        Qh1 = pd.DataFrame()
        Qh1["node_id"] = self.nodes.node_id.loc[self.nodes["type"] == "TabulatedRatingCurve"].reset_index(drop=True)
        Qh1["level"] = 0.0
        Qh1["flow_rate"] = 0.0

        Qh2 = pd.DataFrame()
        Qh2["node_id"] = self.nodes.node_id.loc[self.nodes["type"] == "TabulatedRatingCurve"].reset_index(drop=True)
        Qh2["level"] = 1.0
        Qh2["flow_rate"] = 1.0

        Qh = pd.concat([Qh1, Qh2])
        Qh = Qh.sort_values(by=["node_id", "level"]).reset_index(drop=True)
        Qh.index += 1

        #         rating_curve = ribasim.TabulatedRatingCurve(static=pd.DataFrame(data={"node_id": Qh.node_id,
        #                                                                               "level": Qh.level,
        #                                                                               "flow_rate": Qh.flow_rate}))

        rating_curve = ribasim.TabulatedRatingCurve(
            static=pd.DataFrame(data={"node_id": Qh.node_id, "level": Qh.level, "flow_rate": Qh.flow_rate})
        )

        return rating_curve

    def pump(self):
        """_summary_

        Returns
        -------
        _type_
            _description_
        """
        pump_nodes = self.nodes.loc[self.nodes["type"] == "Pump"].node_id.to_numpy()

        pump = ribasim.Pump(static=pd.DataFrame(data={"node_id": pump_nodes, "flow_rate": 0.5 / 3600}))
        return pump

    def level_boundary(self):
        """_summary_

        Returns
        -------
        _type_
            _description_
        """
        level_boundary = ribasim.LevelBoundary(
            static=pd.DataFrame(
                data={"node_id": self.nodes.loc[self.nodes["type"] == "LevelBoundary"]["node_id"], "level": 999.9}
            )
        )
        return level_boundary

    def flow_boundary(self):
        """_summary_

        Returns
        -------
        _type_
            _description_
        """
        flow_boundary = ribasim.FlowBoundary(
            static=pd.DataFrame(
                                data={"node_id": self.nodes.loc[self.nodes["type"] == "FlowBoundary"]["node_id"], 
                                      "flow_rate": 0}))
        return flow_boundary

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
        manning_resistance = ribasim.ManningResistance(
            static=pd.DataFrame(
                data={"node_id": [], "length": [], "manning_n": [], "profile_width": [], "profile_slope": []}
            )
        )
        return manning_resistance

    def fractional_flow(self):
        """_summary_

        Returns
        -------
        _type_
            _description_
        """
        fractional_flow = ribasim.FractionalFlow(static=pd.DataFrame(data={"node_id": [], "fraction": []}))
        return fractional_flow

    def terminal(self):
        """_summary_

        Returns
        -------
        _type_
            _description_
        """
        terminal = ribasim.Terminal(static=pd.DataFrame(data={"node_id": self.nodes.loc[self.nodes["type"] == "Terminal"]["node_id"]}))

        return terminal

    def outlet(self):
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
                "greater_than": 0.52,
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
        edge=None,
        basin=None,
        pump=None,
        tabulated_rating_curve=None,
        level_boundary=None,
        flow_boundary=None,
        linear_resistance=None,
        manning_resistance=None,
        fractional_flow=None,
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
        edge : _type_, optional
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
        fractional_flow : _type_, optional
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
            modelname=self.model_characteristics["waterschap"] + "_" + self.model_characteristics["modelname"] + "_" + self.model_characteristics["modeltype"],
            network=ribasim.Network(node=node, edge=edge),
            basin=basin,
            flow_boundary=flow_boundary,
            pump=pump,
            tabulated_rating_curve=tabulated_rating_curve,
            terminal=terminal,
            starttime=self.model_characteristics["starttime"],
            endtime=self.model_characteristics["endtime"],
            # level_boundary=level_boundary,
            #                               linear_resistance=linear_resistance,
            #                               manning_resistance=manning_resistance,
            #                               fractional_flow=fractional_flow,
            #                               outlet=outlet,
            #                               discrete_control=discrete_control,
            #                               pid_control=pid_control,
            #                               solver= self.model_characteristics['solver'],
            #                               logging= self.model_characteristics['logging'],
        )

        return model

    def check(self, post_processed_data, crossings):
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
        # Create a dictionary to store all the checks
        checks = {}

        # prevent having multiple geometries columns
        self.nodes = self.nodes[["node_id", "type", "geometry"]]

        ##### identify the sinks #####
        unique_first = np.unique(self.edges["from"].astype(int))  # Get the unique nodes from the first column
        unique_second = np.unique(self.edges["to"].astype(int))  # Get the unique integers from the second column
        peilgebied_to_sink = (
            np.setdiff1d(unique_second, unique_first) + 1
        )  # = the basins which are assumed to be Terminals. Plus one due to indexing

        sinks = self.nodes.loc[self.nodes.node_id.isin(peilgebied_to_sink)]
        sinks = sinks.loc[sinks["type"] != "LevelBoundary"]
        sinks = sinks.loc[sinks["type"] != "FlowBoundary"]
        sinks = sinks.loc[sinks["type"] != "Pump"]
        sinks = sinks.loc[sinks["type"] != "TabulatedRatingCurve"]
        sinks = gpd.GeoDataFrame(sinks, geometry="geometry")
        checks["sinks"] = sinks

        ##### identify basins without connections #####
        actual_ptp = crossings
        pg_from_to = pd.concat([actual_ptp.peilgebied_from, actual_ptp.peilgebied_to])  # .to_numpy().astype(int)
        pg_from_to = pd.to_numeric(
            pg_from_to, errors="coerce"
        )  # handle NaNs, while converting all string values to integers

        basins_without_connection = self.nodes[~self.nodes.node_id.isin(pg_from_to)]
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
        post_processed_data["gemaal"].loc[post_processed_data["gemaal"].globalid.isin(gemaal_used), "in_use"] = (
            True  # swtich in_use from False to True
        )
        checks["gemaal_in_use_True_False"] = post_processed_data["gemaal"]

        ##### collect the hydroobjects and peilgebieden #####
        pg_st = post_processed_data["peilgebied"].merge(  # pg_st = peilgebied with streefpeil
            post_processed_data["streefpeil"],
            left_on="globalid",
            right_on="globalid",
            suffixes=("", "_streefpeil"),
        )[["waterhoogte", "code", "geometry"]]
        checks["peilgebied_met_streefpeil"] = pg_st
        checks["hydroobjecten"] = post_processed_data["hydroobject"]

        return checks

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
        path = f"../../../../Ribasim_networks/Waterschappen/{self.model_characteristics['waterschap']}"
        #         path = os.path.join(path, '', 'modellen', '', self.model_characteristics['waterschap']  + '_' + self.model_characteristics['modeltype'])

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


            path_ribasim = os.path.join(path, "", "modellen", "", self.model_characteristics["waterschap"] + "_" + self.model_characteristics["modeltype"], "ribasim.toml")
            model.write(path_ribasim)
            
            
            
            # print('Edges after writing to Z drive:')
            # display(model.network.edge.df)
            # gpd.GeoDataFrame(model.network.edge.df.geometry).plot(color='red')
            # model.network.edge.df.to_file('zzl_test.gpkg')
            # model.network.edge.plot()

    

        ##### write the checks #####
        if self.model_characteristics["write_checks"]:
            RibasimNetwork.store_data(
                data=checks,
                #                                       output_path = str(path + self.model_characteristics['waterschap'] + '_' + self.model_characteristics['modelname'] + '_' + self.model_characteristics['modeltype'] + '_checks'))
                output_path=os.path.join(
                    path,
                    "modellen",
                    self.model_characteristics["waterschap"] + "_" + self.model_characteristics["modeltype"],
                    self.model_characteristics["waterschap"]
                    + "_"
                    + self.model_characteristics["modelname"]
                    + "_"
                    + self.model_characteristics["modeltype"]
                    + "_checks",
                ),
            )




        
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
                output_path=str(
                    P_path
                    + self.model_characteristics["waterschap"]
                    + "_"
                    + self.model_characteristics["modelname"]
                    + "_"
                    + self.model_characteristics["modeltype"]
                    + "_checks"
                ),
            )

        ##### copy symbology for the RIBASIM model #####
        if self.model_characteristics["write_symbology"]:
            # dont change the paths below!
            checks_symbology_path = (
                r"../../../../Ribasim_networks/Waterschappen/Symbo_feb/modellen/Symbo_feb_poldermodel/Symbo_feb_20240219_Ribasimmodel.qlr"
            )
            checks_symbology_path_new = os.path.join(
                path,
                "modellen",
                self.model_characteristics["waterschap"] + "_" + self.model_characteristics["modeltype"],
                self.model_characteristics["waterschap"]
                + "_"
                + self.model_characteristics["modelname"]
                + "_"
                + self.model_characteristics["modeltype"]
                + "_Ribasim.qlr",
            )

            # dummy string, required to replace string in the file
            checks_path_old = r"../../symbology/symbology__poldermodel_Ribasim/symbology__poldermodel.gpkg"
            #             checks_path_new = os.path.join(self.model_characteristics['waterschap'] + '_' + self.model_characteristics['modelname'] + '_' + self.model_characteristics['modeltype'] + '.gpkg')
            checks_path_new = os.path.join("database.gpkg")

            # copy checks_symbology file from old dir to new dir
            shutil.copy(src=checks_symbology_path, dst=checks_symbology_path_new)

            # read file
            with open(checks_symbology_path_new, encoding="utf-8") as file:
                qlr_contents = file.read()

            # change paths in the .qlr file
            qlr_contents = qlr_contents.replace(checks_path_old, checks_path_new)

            # write updated file
            with open(checks_symbology_path_new, "w", encoding="utf-8") as file:
                file.write(qlr_contents)

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
                        self.model_characteristics["waterschap"]
                        + "_"
                        + self.model_characteristics["modelname"]
                        + "_"
                        + self.model_characteristics["modeltype"]
                        + "_Ribasim.qlr",
                    ),
                )

        ##### copy symbology for the CHECKS data #####
        if self.model_characteristics["write_symbology"]:
            # dont change the paths below!
            checks_symbology_path = r"../../../../Ribasim_networks/Waterschappen/Symbo_feb/modellen/Symbo_feb_poldermodel/Symbo_feb_20240219_checks.qlr"
            checks_symbology_path_new = os.path.join(
                path,
                "modellen",
                self.model_characteristics["waterschap"] + "_" + self.model_characteristics["modeltype"],
                self.model_characteristics["waterschap"]
                + "_"
                + self.model_characteristics["modelname"]
                + "_"
                + self.model_characteristics["modeltype"]
                + "_checks.qlr",
            )

            # dummy string, required to replace string in the file
            checks_path_old = r"../../symbology/symbology__poldermodel_Ribasim/symbology__poldermodel_checks.gpkg"
            #             checks_path_new = os.path.join(self.model_characteristics['waterschap'] + '_' + self.model_characteristics['modelname'] + '_' + self.model_characteristics['modeltype'] + '.gpkg')
            checks_path_new = os.path.join("HollandseDelta_classtest_poldermodel_checks.gpkg")

            # copy checks_symbology file from old dir to new dir
            shutil.copy(src=checks_symbology_path, dst=checks_symbology_path_new)

            # read file
            with open(checks_symbology_path_new, encoding="utf-8") as file:
                qlr_contents = file.read()

            # change paths in the .qlr file
            qlr_contents = qlr_contents.replace(checks_path_old, checks_path_new)

            # write updated file
            with open(checks_symbology_path_new, "w", encoding="utf-8") as file:
                file.write(qlr_contents)

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
                        self.model_characteristics["waterschap"]
                        + "_"
                        + self.model_characteristics["modelname"]
                        + "_"
                        + self.model_characteristics["modeltype"]
                        + "_Ribasim.qlr",
                    ),
                )

        if self.model_characteristics["write_goodcloud"]:
            with open(self.model_characteristics['path_goodcloud_password'], 'r') as file:
                password = file.read()
    
            cloud_storage = CloudStorage(
                password=password,
                data_dir=r"../../../../Ribasim_networks/Waterschappen/",  # + waterschap + '_'+ modelname + '_' + modeltype,
            )

            cloud_storage.upload_model(
                authority=self.model_characteristics["waterschap"],
                model=self.model_characteristics["waterschap"] + "_" + self.model_characteristics["modeltype"],
            )

        print("Done")
