from ribasim import Model

from ribasim_nl import reset_index


def concat(filepaths: list, attributes: dict | None = None) -> Model:
    """Concat existing models to one Ribasim-model

    Parameters
    ----------
    filepaths : List[Path]
        List of Paths to toml-files. Models will be merged first-to-last
    attributes: dict
        Dictionary with attributes (key) and their values as list. Length of values should
        match the length of filepaths as every item in list is assigned as a constant value
        to the ribasim.Model.network.node and ribasim.Model.network.edge.

    Returns
    -------
    Model
        Resulting Ribasim-model
    """

    def add_attributes(model, idx):
        if attributes is not None:
            for k in attributes.keys():
                # for now we have to make sure every attribute startswith meta_
                if not k.startswith("meta_"):
                    column = f"meta_{k}"
                else:
                    column = k

                model.network.node.df[column] = attributes[k][idx]
                model.network.edge.df[column] = attributes[k][idx]
        return model

    # check if attributes match length of list
    if attributes is not None:
        for k, v in attributes.items():
            if not isinstance(v, list):
                raise TypeError(
                    f"value of attribute '{k}' is not a list but '{type(v)}'"
                )

            if len(v) != len(filepaths):
                raise ValueError(
                    f"length of attribute-list '{k}', not equal to length of filepaths: {len(v)} != {len(filepaths)}"
                )

    # read first model to merge the rest into
    filepath = filepaths[0]
    model = Model.read(filepath)
    model = reset_index(model)
    model = add_attributes(model, 0)
    # start_index = model.network.node.df.index.max() + 1

    # merge other models into model
    for idx in range(1, len(filepaths)):
        filepath = filepaths[idx]
        # add_model = reset_index(Model.read(filepath), start_index)
        model = add_attributes(model, idx)
        # model = merge_models(model, add_model)

    return model