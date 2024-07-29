import pandas as pd
import ribasim
from ribasim import Model

from ribasim_nl import reset_index
from ribasim_nl.case_conversions import pascal_to_snake_case


def concat(models: list[Model]) -> Model:
    """Concat existing models to one Ribasim-model

    Parameters
    ----------
    models : list[Model]
        List with ribasim.Model

    Returns
    -------
    Model
        concatenated ribasim.Model
    """

    # models will be concatenated to first model.
    model = reset_index(models[0])
    # determine node_start of next model
    node_start = model.node_table().df.node_id.max() + 1

    # concat all other models into model
    for merge_model in models[1:]:
        # reset index
        merge_model = reset_index(merge_model, node_start)

        # determine node_start of next model
        node_start = model.node_table().df.node_id.max() + 1

        # merge network
        # model.network.node = ribasim.Node(
        #     df=pd.concat([model.network.node.df, merge_model.network.node.df])
        # )
        model.edge = ribasim.EdgeTable(
            df=pd.concat([model.edge.df, merge_model.edge.df], ignore_index=True).reset_index(drop=True)
        )

        # merge tables
        for node_type in model.node_table().df.node_type.unique():
            model_node = getattr(model, pascal_to_snake_case(node_type))
            merge_model_node = getattr(merge_model, pascal_to_snake_case(node_type))
            for attr in model_node.model_fields.keys():
                model_node_table = getattr(model_node, attr)
                model_df = model_node_table.df
                merge_model_df = getattr(merge_model_node, attr).df
                if merge_model_df is not None:
                    if model_df is not None:
                        # make sure we concat both df's into the correct ribasim-object
                        df = pd.concat([model_df, merge_model_df], ignore_index=True)
                    else:
                        df = merge_model_df
                    model_node_table.df = df
                elif model_df is not None:
                    model_node_table.df = model_df

    return model
