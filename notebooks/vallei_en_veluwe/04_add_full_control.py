# %%
"""
Addition of settings for 'wateraanvoer' by means of `ContinuousControl`-nodes and 'wateraanvoergebieden'.

NOTE: This is a non-working dummy file to provide guidance on how to implement these workflows.

Author: Gijs G. Hendrickx
"""

from peilbeheerst_model import ribasim_parametrization
from peilbeheerst_model.controle_output import Control
from ribasim_nl import CloudStorage, Model, check_basin_level

# execute model run
MODEL_EXEC: bool = False

# model settings
AUTHORITY: str = "ValleienVeluwe"
SHORT_NAME: str = "venv"
MODEL_ID: str = "2025_5_0"

# connect with the GoodCloud
cloud = CloudStorage()

# collect relevant data from the GoodCloud
ribasim_model_dir = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_parameterized_model")
ribasim_toml = ribasim_model_dir / f"{SHORT_NAME}.toml"
qlr_path = cloud.joinpath("Basisgegevens", "QGIS_qlr", "output_controle_202502.qlr")
aanvoer_path = cloud.joinpath(
    AUTHORITY, "verwerkt", "1_ontvangen_data", "Na_levering_202401", "wateraanvoer", "Inlaatgebieden.shp"
)

cloud.synchronize(
    filepaths=[
        qlr_path,
        aanvoer_path,
    ]
)

# read model
model = Model.read(ribasim_toml)
original_model = model.copy(deep=True)

# TODO: Remember to set the forcing conditions to be representative for a drought ('aanvoer'-conditions), or for
#  changing conditions (e.g., 1/3 precipitation, 2/3 evaporation).
# set forcing conditions

# re-parameterize
ribasim_parametrization.set_aanvoer_flags(model, str(aanvoer_path), overruling_enabled=False)
ribasim_parametrization.determine_min_upstream_max_downstream_levels(model, AUTHORITY)

# TODO: The addition of `ContinuousControl`-nodes is subsequently a minor modification:
"""To allow the addition of `ContinuousControl`-nodes, the branch 'continuous_control' must be merged first to access
the required function: `ribasim_parametrization.add_continuous_control(<model>)`. The expansion of adding the continuous
control requires a proper working schematisation of both 'afvoer'- and 'aanvoer'-situations, and so these should be
fixed and up-and-running beforehand.
"""
# ribasim_parametrization.add_continuous_control(model)

"""For the addition of `ContinuousControl`-nodes, it might be necessary to set `model.basin.static.df=None`, as the
`ContinuousControl`-nodes require `Time`-tables instead of `Static`-tables. If both are defined (for the same node,
Ribasim will raise an error and thus not execute.
"""

# write model
ribasim_toml = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model", f"{SHORT_NAME}.toml")
check_basin_level.add_check_basin_level(model=model)
model.write(ribasim_toml)

# run model
if MODEL_EXEC:
    # TODO: Different ways of executing the model; choose the one that suits you best:
    ribasim_parametrization.tqdm_subprocess(["ribasim", ribasim_toml], print_other=False, suffix="init")
    # exit_code = model.run()
    # assert exit_code == 0

    """Note that currently, the Ribasim-model is unstable but it does execute, i.e., the model re-parametrisation is
    successful. This might be due to forcing the schematisation with precipitation while setting the 'sturing' of the
    outlets on 'aanvoer' instead of the more suitable 'afvoer'. This should no longer be a problem once the next step of
    adding `ContinuousControl`-nodes is implemented.
    """

    controle_output = Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path)
    indicators = controle_output.run_afvoer()
