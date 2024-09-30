import geopandas as gpd
import numpy as np
import pandas as pd


class AssignAuthorities:
    def __init__(self, ribasim_model, waterschap, ws_grenzen_path, RWS_grenzen_path, ws_buffer=1025, RWS_buffer=1000):
        self.ws_grenzen_path = ws_grenzen_path
        self.RWS_grenzen_path = RWS_grenzen_path

        self.ws_buffer = ws_buffer
        self.RWS_buffer = RWS_buffer

        self.ribasim_model = ribasim_model
        self.waterschap = waterschap

    def assign_authorities(self):
        authority_borders = self.retrieve_geodataframe()
        ribasim_model = self.embed_authorities_in_model(
            ribasim_model=self.ribasim_model, waterschap=self.waterschap, authority_borders=authority_borders
        )
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
        ws_grenzen_cut_out = gpd.overlay(ws_grenzen, RWS_grenzen, how="symmetric_difference")
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
        # create a temp copy of the level boundary df
        temp_LB_node = ribasim_model.level_boundary.node.df.copy()
        temp_LB_node = temp_LB_node[["node_id", "node_type", "geometry"]]
        ribasim_model.level_boundary.static.df = ribasim_model.level_boundary.static.df[["node_id", "level"]]

        # perform a spatial join
        joined = gpd.sjoin(temp_LB_node, authority_borders, how="left", op="intersects")

        # #find whether the LevelBoundary flows inward and outward the waterschap
        FB_inward = ribasim_model.edge.df.loc[ribasim_model.edge.df.from_node_id.isin(joined.node_id.values)].copy()
        FB_outward = ribasim_model.edge.df.loc[ribasim_model.edge.df.to_node_id.isin(joined.node_id.values)].copy()

        # add the current waterschap name in the correct column
        FB_inward["meta_to_authority"], FB_outward["meta_from_authority"] = waterschap, waterschap

        temp_LB_node = temp_LB_node.merge(
            right=FB_inward[["from_node_id", "meta_to_authority"]],
            left_on="node_id",
            right_on="from_node_id",
            how="left",
        )

        temp_LB_node = temp_LB_node.merge(
            right=FB_outward[["to_node_id", "meta_from_authority"]],
            left_on="node_id",
            right_on="to_node_id",
            how="left",
        )

        # #replace the current waterschaps name in the joined layer to NaN, and drop those
        joined["naam"].replace(to_replace=waterschap, value=np.nan, inplace=True)
        joined = joined.dropna(subset="naam").reset_index(drop=True)

        # now fill the meta_from_authority and meta_to_authority columns. As they already contain the correct position of the current waterschap, the remaining 'naam' will be placed correctly as well
        temp_LB_node = temp_LB_node.merge(right=joined[["node_id", "naam"]], on="node_id", how="left")
        temp_LB_node.meta_from_authority.fillna(temp_LB_node["naam"], inplace=True)
        temp_LB_node.meta_to_authority.fillna(temp_LB_node["naam"], inplace=True)

        # only select the relevant columns
        temp_LB_node = temp_LB_node[["node_id", "node_type", "geometry", "meta_from_authority", "meta_to_authority"]]
        temp_LB_node = temp_LB_node.drop_duplicates(subset="node_id").reset_index(drop=True)

        # place the meta categories to the static table
        ribasim_model.level_boundary.static.df = ribasim_model.level_boundary.static.df.merge(
            right=temp_LB_node[["node_id", "meta_from_authority", "meta_to_authority"]], on="node_id", how="left"
        ).reset_index(drop=True)

        return ribasim_model
