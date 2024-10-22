from peilbeheerst_model import shortest_path_waterschap

waterschap = "Wetterskip"
gdf_out = shortest_path_waterschap(waterschap)
gdf_out.to_file(
    f"/DATAFOLDER/projects/4750_30/Data_shortest_path/{waterschap}/{waterschap}_shortest_path.gpkg", driver="GPKG"
)
