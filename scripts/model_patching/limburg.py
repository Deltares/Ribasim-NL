# %%
from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()


toml_file = cloud.joinpath(r"Limburg\modellen\Limburg_dynamic_model\limburg.toml")
model = Model.read(toml_file)

model.endtime = "2017-02-01T00:00:00"

model.merge_basins(node_id=1841, to_node_id=1406)
model.remove_node(node_id=4516, remove_links=True)
model.remove_node(node_id=4517, remove_links=True)

model.write(f"{toml_file.parent.with_name('Limburg_performmance_test').joinpath(toml_file.name)}")

model.run()
