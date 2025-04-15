import geopandas as gpd
import pandas as pd
from shapely.ops import nearest_points


class AssignAuthorities:
    """
    Assigns LevelBoundary nodes in a RIBASIM model to the most relevant neighboring authority polygon.

    Each node is assigned to a polygon based on spatial containment or, if none contain it, the nearest polygon.
    When multiple polygons contain a node, preference is given to the polygon named 'Rijkswaterstaat'.

    The Rijkswaterstaat polygon may be buffered using the `RWS_buffer` parameter to increase the likelihood
    that nearby LevelBoundary nodes are assigned to it.
    """

    def __init__(self, ribasim_model, waterschap, ws_grenzen_path, RWS_grenzen_path, RWS_buffer=0):
        self.ws_grenzen_path = ws_grenzen_path
        self.RWS_grenzen_path = RWS_grenzen_path

        self.ribasim_model = ribasim_model
        self.waterschap = waterschap

        self.RWS_buffer = RWS_buffer

    def assign_authorities(self):
        """
        Main function that assigns external authorities to LevelBoundary nodes in the model.

        Returns
        -------
            Updated RIBASIM model with `meta_couple_authority` added to level_boundary.static.df.
        """
        authority_polygons = self.load_data()
        ribasim_model = self.embed_authorities_in_model(
            ribasim_model=self.ribasim_model, waterschap=self.waterschap, authority_polygons=authority_polygons
        )
        return ribasim_model

    def load_data(self):
        """
        Loads and preprocesses waterschap and Rijkswaterstaat boundaries.

        Returns
        -------
            GeoDataFrame containing cleaned and buffered authority polygons (EPSG:28992).
        """
        ws_grenzen = gpd.read_file(self.ws_grenzen_path)
        RWS_grenzen = gpd.read_file(self.RWS_grenzen_path)

        # Removing "\n", "waterschap", "Hoogheemraadschap", "van" and spaces and commas to align names from file with GoodCloud names
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

        # get rid of irrelvant polygons
        ws_grenzen = ws_grenzen.explode()
        ws_grenzen["area"] = ws_grenzen.area
        ws_grenzen = ws_grenzen.loc[ws_grenzen.area > 10000000]  # remove some small polygons
        ws_grenzen.reset_index(drop=True, inplace=True)

        # add RWS_grenzen. Buffer and dissolve it
        RWS_grenzen["geometry"] = RWS_grenzen.buffer(self.RWS_buffer)
        RWS_grenzen = RWS_grenzen.dissolve()[["geometry"]]

        # add the two layers together
        authority_polygons = pd.concat([ws_grenzen, RWS_grenzen])
        authority_polygons = authority_polygons.reset_index(drop=True)
        authority_polygons = gpd.GeoDataFrame(authority_polygons, geometry="geometry").set_crs(crs="EPSG:28992")

        return authority_polygons

    def snap_points_to_nearest_polygon(self, LB_gdf, authority_polygons, waterschap):
        """
        Snaps each LevelBoundary point to the nearest relevant authority polygon.

        Preference is given to polygons that contain the point, and to 'Rijkswaterstaat'
        in case of overlap. The current waterschap is excluded from snapping.

        Parameters
        ----------
            LB_gdf (GeoDataFrame): LevelBoundary node table with geometries.
            authority_polygons (GeoDataFrame): All authority polygons.
            waterschap (str): Name of the current water authority.

        Returns
        -------
            GeoDataFrame with updated geometry and 'meta_couple_authority' column.
        """
        authority_polygons = authority_polygons.loc[
            authority_polygons.naam != waterschap
        ]  # do not snap the LevelBoundaries to its own waterschap; discard it from the polygons

        def assign_polygon(row):
            # Step 1: find all polygons that fall within the point
            containing = authority_polygons[authority_polygons.contains(row.geometry)]

            if not containing.empty:
                if "Rijkswaterstaat" in containing["naam"].values:
                    selected = containing[containing["naam"] == "Rijkswaterstaat"].iloc[
                        0
                    ]  # chose RWS if multiple polygons are there (assumption)
                else:
                    selected = containing.iloc[0]  # fallback to first if RWS not in list
            else:
                # Step 2: if point not in any polygon, fallback to nearest polygon
                distances = authority_polygons.geometry.distance(row.geometry)
                nearest_idx = distances.idxmin()
                selected = authority_polygons.loc[nearest_idx]

            snapped_point = nearest_points(row.geometry, selected.geometry)[1]
            return snapped_point, selected["naam"]

        snapped_results = LB_gdf.apply(assign_polygon, axis=1, result_type="expand")
        LB_gdf["geometry"] = snapped_results[0]
        LB_gdf["meta_couple_authority"] = snapped_results[1]

        return LB_gdf

    def embed_authorities_in_model(self, ribasim_model, waterschap, authority_polygons):
        """
        Embeds authority information into the RIBASIM model's level_boundary static table.

        Parameters
        ----------
            ribasim_model: The RIBASIM model object.
            waterschap (str): The current water authority to exclude from assignments.
            authority_polygons (GeoDataFrame): The cleaned and combined authority polygons.

        Returns
        -------
            Updated RIBASIM model with `meta_couple_authority` assigned.
        """
        # create a temp copy of the level boundary df
        temp_LB_node = ribasim_model.level_boundary.node.df.copy().reset_index()
        temp_LB_node = temp_LB_node[["node_id", "node_type", "geometry"]]
        ribasim_model.level_boundary.static.df = ribasim_model.level_boundary.static.df[["node_id", "level"]]

        # snap LevelBoundary points to nearest polygon
        snapped_nodes = self.snap_points_to_nearest_polygon(temp_LB_node, authority_polygons, waterschap)

        # place the meta categories to the static table
        LB_static = ribasim_model.level_boundary.static.df.merge(
            right=snapped_nodes[["node_id", "meta_couple_authority"]], on="node_id", how="left"
        ).reset_index(drop=True)
        LB_static.index.name = "fid"
        ribasim_model.level_boundary.static.df = LB_static

        return ribasim_model
