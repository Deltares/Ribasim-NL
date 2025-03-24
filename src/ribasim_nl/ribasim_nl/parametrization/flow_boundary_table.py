from ribasim_nl.model import Model
from ribasim_nl.parametrization.empty_table import empty_table_df


def update_flow_boundary_static(
    model: Model,
    code_column: str = "meta_code_waterbeheerder",
    meta_values: dict[str] = {"meta_categorie": "Aanvoer Buitenland"},
    default_values: dict = {"flow_rate": 0},
):
    """Update FlowBoundary table

    Args:
        model (Model): Ribasim model
        code_column: (str) column in node_table corresponding with code column in static_data_xlsx

    Returns
    -------
        pd.DataFrame DataFrame in format of static table (ignoring NoData)
    """
    # start with an empty static_df with the correct columns and meta_code_waterbeheerder
    static_df = empty_table_df(model=model, node_type="LevelBoundary", table_type="Static", meta_columns=[code_column])

    # add default values
    for k, v in default_values:
        static_df.loc[:, [k]] = v

    # add meta_columns
    for k, v in meta_values.items():
        static_df[k] = v

    model.level_boundary.static.df = static_df
