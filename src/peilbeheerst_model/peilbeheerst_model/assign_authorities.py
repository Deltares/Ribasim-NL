import geopandas as gpd
import pandas as pd


class AssignAuthorities:
    """
    Assign authority polygons to LevelBoundary nodes in a RIBASIM model.

    This includes assigning Waterschappen and Rijkswaterstaat polygons based on spatial
    intersection. Priority is given to 'Rijkswaterstaat' when multiple polygons overlap.

    Manual overrides can be applied using the `custom_nodes` parameter.
    """

    def __init__(
        self,
        ribasim_model,
        waterschap,
        ws_grenzen_path,
        RWS_grenzen_path,
        ws_buffer=1000,
        RWS_buffer=1000,
        custom_nodes=None,
    ):
        self.ws_grenzen_path = ws_grenzen_path
        self.RWS_grenzen_path = RWS_grenzen_path

        self.ws_buffer = ws_buffer
        self.RWS_buffer = RWS_buffer

        self.ribasim_model = ribasim_model
        self.waterschap = waterschap
        self.custom_nodes = custom_nodes

    def assign_authorities(self):
        authority_borders = self.retrieve_geodataframe()
        ribasim_model = self.embed_authorities_in_model(
            ribasim_model=self.ribasim_model, waterschap=self.waterschap, authority_borders=authority_borders
        )
        if self.custom_nodes is not None:
            ribasim_model = self.adjust_custom_nodes(ribasim_model=ribasim_model, custom_nodes=self.custom_nodes)
        return ribasim_model

    def adjust_custom_nodes(self, ribasim_model, custom_nodes):
        """Adjust nodes"""
        temp_node_id = ribasim_model.level_boundary.node.df.reset_index(drop=False)
        for node_id, authority in custom_nodes.items():
            temp_node_id.loc[
                temp_node_id["node_id"] == node_id,
                "meta_couple_authority",
            ] = authority

        # check if all LevelBoundaries have an authority. Raise a soft warning otherwise. Note that sea / other countries are actual boundaries, so having NaN values is not necessarily wrong.
        if temp_node_id["meta_couple_authority"].isna().any():
            print(temp_node_id.loc[temp_node_id["meta_couple_authority"].isna()])
            print("Warning! Not all LevelBoundary nodes were assigned to an authority.")

        ribasim_model.level_boundary.node.df = temp_node_id.set_index("node_id")
        return ribasim_model

    def retrieve_geodataframe(self):
        """Main function which calls the other functions."""
        ws_grenzen, RWS_grenzen = self.load_data()
        authority_borders = self.clip_and_buffer(ws_grenzen, RWS_grenzen)
        authority_borders = self.extent_authority_borders(authority_borders)

        return authority_borders

    def load_data(self):
        """Loads and processes the authority areas of the waterschappen and RWS."""
        ws_grenzen = gpd.read_file(self.ws_grenzen_path)
        RWS_grenzen = gpd.read_file(self.RWS_grenzen_path)

        # Removing "\n", "waterschap", "Hoogheemraadschap", "van" and spaces and commas
        ws_grenzen["naam"] = ws_grenzen["naam"].str.replace(r"\n", "", regex=True)
        ws_grenzen["naam"] = ws_grenzen["naam"].str.replace("Waterschap", "", regex=False)
        ws_grenzen["naam"] = ws_grenzen["naam"].str.replace("Hoogheemraadschap", "", regex=False)
        ws_grenzen["naam"] = ws_grenzen["naam"].str.replace("De S", "S", regex=False)  # HDSR
        ws_grenzen["naam"] = ws_grenzen["naam"].str.replace("â", "a", regex=False)  # WF
        ws_grenzen["naam"] = ws_grenzen["naam"].str.replace("van", "", regex=False)
        ws_grenzen["naam"] = ws_grenzen["naam"].str.replace(",", "", regex=False)
        ws_grenzen["naam"] = ws_grenzen["naam"].str.replace("'", "", regex=False)
        ws_grenzen["naam"] = ws_grenzen["naam"].str.replace(" ", "", regex=False)

        ws_grenzen = ws_grenzen.sort_values(by="naam").reset_index(drop=True)
        self.ws_grenzen_OG = ws_grenzen.copy()

        # get rid of irrelvant polygons
        ws_grenzen = ws_grenzen.explode()
        ws_grenzen["area"] = ws_grenzen.area
        ws_grenzen = ws_grenzen.loc[ws_grenzen.area > 10000000]  # remove some small polygons
        ws_grenzen.reset_index(drop=True, inplace=True)

        # add RWS_grenzen. Buffer and dissolve it
        RWS_grenzen["geometry"] = RWS_grenzen.buffer(self.RWS_buffer)
        RWS_grenzen = RWS_grenzen.dissolve()[["geometry"]]

        return ws_grenzen, RWS_grenzen

    def clip_and_buffer(self, ws_grenzen, RWS_grenzen):
        """Clips the waterboard boundaries by removing the RWS areas and applies a buffer to the remaining polygons."""
        # Remove the RWS area in each WS
        ws_grenzen_cut_out = gpd.overlay(ws_grenzen, RWS_grenzen, how="symmetric_difference", keep_geom_type=True)
        ws_grenzen_cut_out.dropna(subset="area", inplace=True)

        # add a name to the RWS area
        RWS_grenzen["naam"] = "Rijkswaterstaat"

        # add a buffer to each waterschap. Within this strip an authority will be found.
        ws_grenzen_cut_out["geometry"] = ws_grenzen_cut_out.buffer(self.ws_buffer)

        # add the two layers together
        authority_borders = pd.concat([ws_grenzen_cut_out, RWS_grenzen])
        authority_borders = authority_borders.reset_index(drop=True)
        authority_borders = gpd.GeoDataFrame(authority_borders, geometry="geometry").set_crs(crs="EPSG:28992")

        return authority_borders

    def extent_authority_borders(self, authority_borders):
        """Extends the authority borders by combining them with the original waterboard boundaries and dissolving the geometries based on the name."""
        # Add a bit more area by dissolving it with the original gdf
        authority_borders = pd.concat([authority_borders, self.ws_grenzen_OG])
        authority_borders = gpd.GeoDataFrame(authority_borders, geometry="geometry").set_crs(crs="EPSG:28992")
        authority_borders = authority_borders.dissolve(by="naam", as_index=False)
        authority_borders = authority_borders[["naam", "geometry"]]

        return authority_borders

    def embed_authorities_in_model(self, ribasim_model, waterschap, authority_borders):
        """Assigns authority to each LevelBoundary node using spatial intersection."""
        # create a temp copy of the level boundary df
        temp_LB_node = ribasim_model.level_boundary.node.df.copy().reset_index()
        temp_LB_node = temp_LB_node[["node_id", "node_type", "geometry"]]

        # perform a spatial join
        joined = gpd.sjoin(temp_LB_node, authority_borders, how="left", predicate="intersects")

        # discard all rows where the waterschap itself occurs, as we only want to know the other waterschap
        joined = joined.loc[joined.naam != waterschap]

        # if authority areas overlap, duplicates may form. Retain the Rijkswaterstaat one
        joined = pd.concat([joined.loc[joined.naam == "Rijkswaterstaat"], joined.loc[joined.naam != "Rijkswaterstaat"]])
        joined = joined.drop_duplicates(subset="node_id", keep="first")
        joined = joined.sort_values(by="node_id")

        joined = joined.rename(columns={"naam": "meta_couple_authority"})

        # place the meta categories in the node table
        LB_node = (
            ribasim_model.level_boundary.node.df.reset_index()
            .merge(right=joined[["node_id", "meta_couple_authority"]], on="node_id", how="left")
            .set_index("node_id")
        )
        ribasim_model.level_boundary.node.df = LB_node

        return ribasim_model
