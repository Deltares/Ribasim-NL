import pandas as pd
from ribasim import Model

from ribasim_nl.case_conversions import pascal_to_snake_case
from ribasim_nl.reset_index import reset_index


def concat(models: list[Model], keep_original_index: bool = False) -> Model:
    """Concat existing models to one Ribasim-model

    Parameters
    ----------
    models : list[Model]
        List with ribasim.Model
    keep_original_index: bool
        Boolean for keeping original index. If not indices will be reset to avoid duplicate indices

    Returns
    -------
    Model
        concatenated ribasim.Model
    """
    # models will be concatenated to first model.
    if not keep_original_index:
        model = reset_index(models[0])
    else:
        model = models[0]

    # concat all other models into model
    for merge_model in models[1:]:
        if not keep_original_index:
            # reset index of mergemodel, node_start is max node_id
            node_start = model.node_table().df.index.max() + 1
            merge_model = reset_index(merge_model, node_start)

        # concat edges
        edge_df = pd.concat([model.edge.df, merge_model.edge.df], ignore_index=True)
        edge_df.index.name = "edge_id"
        model.edge.df = edge_df

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
                        if "node_id" in model_df.columns:
                            df = pd.concat([model_df, merge_model_df], ignore_index=True)
                            df.index.name = "fid"
                        elif model_df.index.name == "node_id":
                            df = pd.concat([model_df, merge_model_df], ignore_index=False)
                        else:
                            raise Exception(f"{node_type} / {attr} cannot be merged")
                    else:
                        df = merge_model_df
                    model_node_table.df = df
                elif model_df is not None:
                    model_node_table.df = model_df

    return model
