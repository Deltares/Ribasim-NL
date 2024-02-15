import pandas as pd
import ribasim
from ribasim import Model

from ribasim_nl import reset_index
from ribasim_nl.case_conversions import pascal_to_snake_case
from ribasim_nl.model import CLASS_TABLES, get_table


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
    node_start = model.network.node.df.index.max() + 1
    # concat all other models into model

    for merge_model in models[1:]:
        # reset index
        merge_model = reset_index(merge_model, node_start)

        # determine node_start of next model
        node_start = model.network.node.df.index.max() + 1

        # merge network
        model.network.node = ribasim.Node(
            df=pd.concat([model.network.node.df, merge_model.network.node.df])
        )
        model.network.edge = ribasim.Edge(
            df=pd.concat(
                [model.network.edge.df, merge_model.network.edge.df], ignore_index=True
            ).reset_index()
        )

        # merge all non-spatial tables
        for class_name, attrs in CLASS_TABLES.items():
            # convert class-name to model attribute
            class_attr = pascal_to_snake_case(class_name)

            # read all tables and temporary store them in a dict
            tables = {}
            for attr in attrs:
                # table string
                table = f"{class_attr}.{attr}.df"

                # see if there is a table to concatenate
                merge_model_df = get_table(merge_model, table)
                model_df = get_table(model, table)

                if merge_model_df is not None:
                    if model_df is not None:
                        # make sure we concat both df's into the correct ribasim-object
                        df = pd.concat([model_df, merge_model_df], ignore_index=True)
                    else:
                        df = merge_model_df
                    tables[attr] = df
                elif model_df is not None:
                    tables[attr] = model_df

            # now we gently update the Ribasim class with new tables
            if tables:
                table_class = getattr(ribasim, class_name)
                setattr(model, class_attr, table_class(**tables))

    return model
