# %% Import necessary libraries
import subprocess
from pathlib import Path

from ribasim.delwaq import parse, plot_fraction

# %% Set path of Ribasim model
model_path = Path("c:/Users/leeuw_je/Projecten/LWKM_Ribasim/lhm_rwzi_delwaq_Dommel")
toml_path = model_path / "lhm_rwzi_delwaq.toml"
output_path = model_path / "delwaq"
assert toml_path.is_file()


# %% run delwaq from python code

# if not working use the command line
# dimr_path should probably also become an environment variable

dimr_path = Path(
    "C:/Program Files/Deltares/Delft3D FM Suite 2025.02 HMWQ/plugins/DeltaShell.Dimr/kernels/x64/bin/run_dimr.bat"
)
dimr_config_path = output_path / "dimr_config.xml"

result = subprocess.run([dimr_path, dimr_config_path], cwd=output_path, capture_output=True, encoding="utf-8")

print(result.stdout)
print(result.stderr)
result.check_returncode()


# %% before parsing model: include manually added substance/load
substances.add("N")
substances.add("P")


# %% parse delwaq results
nmodel = parse(toml_path, graph, substances, output_folder=output_path)


# %% check added loads
plot_fraction(nmodel, 99991010, ["N"])  # node where load is added
plot_fraction(nmodel, 99991360, ["N"])  # a node somewhat downstream of the added load
plot_fraction(nmodel, 99991543, ["N"])  # a node far downstream of the added load

plot_fraction(nmodel, 99991010, ["P"])  # node where load is added
plot_fraction(nmodel, 99991360, ["P"])  # a node somewhat downstream of the added load
plot_fraction(nmodel, 99991680, ["P"])  # a node far downstream of the added load

# %% plot fractions of all concentrations in a node of choice
plot_fraction(nmodel, 99991910)  # , ['Initial'])

# %% display data in tabular view
display(nmodel.basin.concentration_external)
t = nmodel.basin.concentration_external.df  # display all concentrations
t[t.time == t.time.unique()[2]]  # check concentration at a specific time step
