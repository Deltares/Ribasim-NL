# %%
import time

from ribasim_nl import CloudStorage, Model, concat, prefix_index

cloud = CloudStorage()

ribasim_toml = cloud.joinpath("Noorderzijlvest", "modellen", "Noorderzijlvest_2025_3_1", "nzv.toml")


data = {}
for prefix in range(23, 100):
    # read original model
    model = Model.read(ribasim_toml)

    # define parallel model
    if prefix == 0:
        parallel_model = model
    else:
        model = prefix_index(model=model, prefix_id=prefix)
        parallel_model = concat([parallel_model, model], keep_original_index=True)

    # write parallel model
    toml_file = ribasim_toml.parent.with_name(f"nzv_{prefix}") / "nzv.toml"
    parallel_model.write(toml_file)

    # time simulation
    t0 = time.time()
    parallel_model.run()
    simulation_time = time.time() - t0
    nbr_basins = len(parallel_model.basin.node.df)
    print(prefix, simulation_time, nbr_basins)
    data[prefix] = {"simulation_time": simulation_time, "basins": nbr_basins}
