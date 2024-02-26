from pandas import DataFrame


def control_state(name: str, length: int) -> list[str]:
    return [f"{name}_{i+1}" for i in range(length)]


def condition(
    values: list[float],
    node_id: int,
    listen_feature_id: int,
    variable: str = "flow_rate",
    name: str | None = None,
) -> DataFrame:
    df = DataFrame({"greater_than": values})
    df.loc[:, ["node_id"]] = node_id
    df.loc[:, ["listen_feature_id"]] = listen_feature_id
    df.loc[:, ["variable"]] = variable
    df.loc[:, ["remarks"]] = control_state(name, len(df))
    return df


def logic(
    node_id: int,
    length: int,
    name: str | None = None,
) -> DataFrame:
    df = DataFrame(
        {
            "truth_state": [
                "".join(["T"] * i + ["F"] * length)[0:length] for i in range(length)
            ]
        }
    )
    df.loc[:, ["node_id"]] = node_id
    df.loc[:, ["control_state"]] = control_state(name, len(df))
    return df


def node_table(values: list[float], variable: str, name: str, **kwargs) -> DataFrame:
    df = DataFrame({variable: values})
    df.loc[:, ["control_state"]] = control_state(name, len(df))
    for k, v in kwargs.items():
        df.loc[:, [k]] = v

    return df
