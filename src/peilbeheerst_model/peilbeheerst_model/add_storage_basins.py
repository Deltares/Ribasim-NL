import pandas as pd
from shapely.geometry import LineString


class AddStorageBasins:
    def __init__(self, ribasim_model, exclude_hoofdwater, additional_basins_to_exclude, distance_bergende_basin=10):
        self.ribasim_model = ribasim_model
        self.exclude_hoofdwater = exclude_hoofdwater
        self.additional_basins_to_exclude = additional_basins_to_exclude
        self.distance_bergende_basin = distance_bergende_basin

    # duplicate the doorgaande basins:
    # retrieve the ids of the doorgaande basins, exlude the basins_to_exclude
    # copy the .node, .static, .state, .profile table

    def create_bergende_basins(self):
        doorgaande_basin_ids = self.ribasim_model.basin.state.df.copy()

        # exclude (possibly) hoofdwater basins, as the majority of the area is meant for doorgaand water
        if self.exclude_hoofdwater:
            doorgaande_basin_ids = doorgaande_basin_ids.loc[doorgaande_basin_ids.meta_categorie == "doorgaand"]

        # exclude (possibly) other basins to create a storage basin
        if self.additional_basins_to_exclude is not None and len(self.additional_basins_to_exclude) > 0:
            doorgaande_basin_ids = doorgaande_basin_ids.loc[
                ~doorgaande_basin_ids.node_id.isin(self.additional_basins_to_exclude)
            ]

        # convert to numbers
        doorgaande_basin_ids = doorgaande_basin_ids.node_id.to_numpy()

        # retrieve the max node_id
        max_node_id = self.get_current_max_nodeid()

        # duplicate all the tables
        bergende_node = self.ribasim_model.basin.node.df.loc[
            self.ribasim_model.basin.node.df.index.isin(doorgaande_basin_ids)
        ].copy()
        bergende_static = self.ribasim_model.basin.static.df.loc[
            self.ribasim_model.basin.static.df.node_id.isin(doorgaande_basin_ids)
        ].copy()
        bergende_state = self.ribasim_model.basin.state.df.loc[
            self.ribasim_model.basin.state.df.node_id.isin(doorgaande_basin_ids)
        ].copy()
        bergende_profile = self.ribasim_model.basin.profile.df.loc[
            self.ribasim_model.basin.profile.df.node_id.isin(doorgaande_basin_ids)
        ].copy()
        bergende_area = self.ribasim_model.basin.area.df.loc[
            self.ribasim_model.basin.area.df.node_id.isin(doorgaande_basin_ids)
        ].copy()

        # store the linked node_id of the bergende basin, and add the found max node_id. Plus one as we need to start counting from the next node_id
        bergende_node["doorgaand_id"] = bergende_node.index.copy()
        bergende_node.index = (
            max_node_id + bergende_node.index
        )  # + 1 dont add the plus one here, as the index already starts at 1
        bergende_static.node_id = max_node_id + bergende_static.node_id  # + 1
        bergende_state.node_id = max_node_id + bergende_state.node_id  # + 1
        bergende_profile.node_id = max_node_id + bergende_profile.node_id  # + 1
        bergende_area.node_id = max_node_id + bergende_area.node_id  # + 1

        # change the meta_categorie and column names from doorgaand to bergend
        bergende_state.meta_categorie = "bergend"

        # add the geometry information for the bergende basin and the manning_resistance
        bergende_node = bergende_node.rename(
            columns={"geometry": "geometry_doorgaand"}
        )  # store the geometry of the doorgaande basin
        bergende_node["geometry_bergend"] = bergende_node["geometry_doorgaand"].translate(
            xoff=self.distance_bergende_basin, yoff=0
        )  # create a bergende basin x meters to the right
        bergende_node["geometry_manning"] = bergende_node["geometry_doorgaand"].translate(
            xoff=self.distance_bergende_basin / 2, yoff=0
        )  # create a bergende manning resistance in the middle (halfway, thus divided by 2)
        bergende_node["manning_id"] = (
            bergende_node.index.max() + bergende_node.index + 1
        )  # retrieve new max node id for the manning node

        # create links from the nodes
        def create_linestring(row, from_col, to_col):
            return LineString([row[from_col], row[to_col]])

        bergende_node["geometry_link_bergend_to_MR"] = bergende_node.apply(
            create_linestring, axis=1, from_col="geometry_bergend", to_col="geometry_manning"
        )
        bergende_node["geometry_link_MR_to_doorgaand"] = bergende_node.apply(
            create_linestring, axis=1, from_col="geometry_manning", to_col="geometry_doorgaand"
        )

        # create the manning resistance node table, conform Ribasim style
        manning_node = bergende_node[["manning_id", "geometry_manning"]].copy().reset_index(drop=True)
        manning_node = manning_node.rename(columns={"manning_id": "node_id", "geometry_manning": "geometry"})
        manning_node = manning_node.set_index("node_id")
        manning_node["node_type"] = "ManningResistance"
        manning_node["meta_node_id"] = manning_node.index

        # create the manning resistance static table, conform Ribasim style
        manning_static = manning_node.reset_index()[["node_id"]].copy()
        manning_static["length"] = 1000
        manning_static["manning_n"] = 0.02
        manning_static["profile_width"] = 2.0
        manning_static["profile_slope"] = 3.0
        manning_static["meta_categorie"] = "bergend"

        # create the links table which goes from the bergende basin to the ManningResistance (MR)
        link_bergend_MR = pd.DataFrame()
        link_bergend_MR["from_node_id"] = (
            bergende_node.index.copy()
        )  # the index is the bergende node_id, which is the starting point
        link_bergend_MR["to_node_id"] = bergende_node.manning_id.to_numpy()  # it goes to the manning node
        link_bergend_MR["geometry"] = (
            bergende_node.geometry_link_bergend_to_MR.values
        )  # link geometry was already created
        link_bergend_MR["link_type"] = "flow"
        link_bergend_MR["meta_from_node_type"] = "Basin"  # include metadata
        link_bergend_MR["meta_to_node_type"] = "ManningResistance"  # include metadata
        link_bergend_MR["meta_categorie"] = "bergend"  # include metadata

        # repeat the same, but then from the ManningResistance (MR) to the doorgaande
        link_MR_doorgaand = pd.DataFrame()
        link_MR_doorgaand["from_node_id"] = (
            bergende_node.manning_id.to_numpy()
        )  # the starting point is the ManningResistance node
        link_MR_doorgaand["to_node_id"] = bergende_node.doorgaand_id.to_numpy()  # it goes to the doorgaande basin
        link_MR_doorgaand["geometry"] = (
            bergende_node.geometry_link_MR_to_doorgaand.values
        )  # link geometry was already created
        link_MR_doorgaand["link_type"] = "flow"
        link_MR_doorgaand["meta_from_node_type"] = "ManningResistance"  # include metadata
        link_MR_doorgaand["meta_to_node_type"] = "Basin"  # include metadata
        link_MR_doorgaand["meta_categorie"] = "bergend"  # include metadata. This is still bergend.

        # combine the link tables
        link_bergend_all = pd.concat([link_bergend_MR, link_MR_doorgaand]).reset_index(drop=True)
        link_bergend_all["link_id"] = (
            link_bergend_all.index.copy() + self.ribasim_model.link.df.index.max() + 1
        )  # start counting from the highest link_id
        link_bergend_all = link_bergend_all.set_index("link_id")

        # clean the new node table, update the meta_node_id column
        bergende_node = bergende_node[["node_type", "meta_node_id", "geometry_bergend"]]
        bergende_node = bergende_node.rename(columns={"geometry_bergend": "geometry"})
        bergende_node["meta_node_id"] = bergende_node.index.copy()

        # concat all the new tables to the existing model
        self.ribasim_model.basin.node.df = pd.concat([self.ribasim_model.basin.node.df, bergende_node])
        self.ribasim_model.basin.static = pd.concat([self.ribasim_model.basin.static.df, bergende_static]).reset_index(
            drop=True
        )
        self.ribasim_model.basin.state = pd.concat([self.ribasim_model.basin.state.df, bergende_state]).reset_index(
            drop=True
        )
        self.ribasim_model.basin.profile = pd.concat(
            [self.ribasim_model.basin.profile.df, bergende_profile]
        ).reset_index(drop=True)
        self.ribasim_model.basin.area = pd.concat([self.ribasim_model.basin.area.df, bergende_area]).reset_index(
            drop=True
        )

        self.ribasim_model.manning_resistance.node.df = pd.concat(
            [self.ribasim_model.manning_resistance.node.df, manning_node]
        )
        self.ribasim_model.manning_resistance.static = pd.concat(
            [self.ribasim_model.manning_resistance.static.df, manning_static]
        ).reset_index(drop=True)

        self.ribasim_model.link.df = pd.concat([self.ribasim_model.link.df, link_bergend_all])

    def get_current_max_nodeid(self):
        """Get the current maximum node ID from the model where node_id is stored as an index."""
        max_ids = []
        for k, v in self.ribasim_model.__dict__.items():
            if hasattr(v, "node"):
                # Check if the DataFrame's index is named 'meta_node_id'
                if v.node.df.index.name == "node_id":
                    mid = v.node.df.index.max()
                    if not pd.isna(mid):
                        max_ids.append(int(mid))
        if len(max_ids) == 0:
            raise ValueError("No node ids found")
        max_id = max(max_ids)
        return max_id
