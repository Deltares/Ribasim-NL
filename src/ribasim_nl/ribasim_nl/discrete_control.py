from pandas import DataFrame


def control_state(name: str, length: int) -> list[str]:
    return [f"{name}_{i + 1:03d}" for i in range(length)]


def logic(
    node_id: int,
    length: int,
    name: str | None = None,
) -> DataFrame:
    df = DataFrame({"truth_state": ["".join(["T"] * i + ["F"] * length)[0:length] for i in range(1, length + 1)]})
    df.loc[:, ["node_id"]] = node_id
    assert name is not None
    df.loc[:, ["control_state"]] = control_state(name, len(df))
    return df


def node_table(values: list[float], variable: str, name: str, **kwargs) -> DataFrame:
    df = DataFrame({variable: values})
    df.loc[:, ["control_state"]] = control_state(name, len(df))
    for k, v in kwargs.items():
        df.loc[:, [k]] = v

    return df
