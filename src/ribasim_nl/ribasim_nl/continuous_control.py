from pandas import DataFrame


def function(
    input: list[float],
    output: list[float],
    controlled_variable: str = "flow_rate",
) -> DataFrame:
    df = DataFrame({"input": input, "output": output})
    df.loc[:, ["controlled_variable"]] = controlled_variable
    return df


def variable(
    listen_node_id: int,
    variable: str = "flow_rate",
) -> DataFrame:
    return DataFrame({"listen_node_id": [listen_node_id], "variable": [variable]})
