# %%
"""Add the dynamic forcing to the models of the vrij afwaterend water authorities, and run them.

The forcing, inflows and final steps are shared with the peilbeheerst water authorities, see
`ribasim_nl.regional_model`.
"""

import sys

from ribasim_nl.aquo import waterbeheercode
from ribasim_nl.delwaq import compute_delwaq
from ribasim_nl.network_snapping import snap_to_waterways, straight_flow_link_ids
from ribasim_nl.regional_model import add_inflows, add_lhm_forcing, finish_regional_model
from ribasim_nl.settings import settings

from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()

assign_budget_fractions: bool = False  # compute (sub-) fractions from budgets-table
compute_fractions: bool = False

top10nl_path = settings.ribasim_nl_data_dir / "Basisgegevens/Top10NL/top10nl_Compleet.gpkg"

FIND_POST_FIXES = ["bergend_model"]
# pass authorities as arguments, or edit list here
SELECTION: set = {"BrabantseDelta"}  # , "Limburg", "DeDommel"}


def snap(model: Model) -> Model:
    """Route the links that are still straight along the Top10NL waterways, which only changes the layout."""
    return snap_to_waterways(model, top10nl_path, straight_flow_link_ids(model))


# authorities provided as arguments, else in SELECTION, else all
authorities = cloud.select_authorities(sys.argv[1:], fallback=SELECTION)

# %%
for authority in authorities:
    model_dir = cloud.find_model_dir(authority, FIND_POST_FIXES)
    if model_dir is None:
        raise FileNotFoundError(f"No model of {authority} found with post fixes {FIND_POST_FIXES}")
    print(authority)
    toml_file = next(model_dir.glob("*.toml"))

    # write dynamic model
    dst_model_dir = cloud.model_dir(authority, "dynamic_model")
    dst_toml_file = dst_model_dir / toml_file.name
    model = Model.read(toml_file)

    # add categorie to basin / state
    series = model.basin.node.df["meta_categorie"]  # type: ignore
    uncategorized_basins = series[series.isna()].index.values
    assert model.node.df is not None
    if len(uncategorized_basins) > 0:
        print(f"uncategorized basins: {uncategorized_basins}, will be set to doorgaand")
        model.node.df.loc[uncategorized_basins, "meta_categorie"] = "doorgaand"

    model = add_lhm_forcing(
        model,
        dst_model_dir,
        sprinkling="primary",
        assign_fractions=assign_budget_fractions,
        fraction_prefix=str(waterbeheercode[authority]),
    )

    # merging the RWZI model requires meta_waterbeheerder
    assert model.basin.node is not None and model.basin.node.df is not None
    assert model.node.df is not None
    model.node.df.loc[model.basin.node.df.index, "meta_waterbeheerder"] = authority
    model = add_inflows(model, authority, dst_model_dir)

    # TODO add flow_boundary to the tables written to NetCDF after we can run core versions with
    # https://github.com/Deltares/Ribasim/pull/3033
    model = finish_regional_model(model, dst_toml_file, authority, snap=snap)

    # DELWAQ(!)
    if compute_fractions:
        print("compute DELWAQ fractions")
        compute_delwaq(model)
