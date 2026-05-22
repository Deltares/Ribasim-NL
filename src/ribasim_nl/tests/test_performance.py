import pandas as pd
import xarray as xr
from ribasim_nl.performance import write_performance


def test_write_performance_from_toml(tmp_path):
    model_dir = tmp_path / "ExampleAuthority" / "modellen" / "example_model"
    results_dir = model_dir / "results"
    results_dir.mkdir(parents=True)

    toml_path = model_dir / "example.toml"
    toml_path.write_text(
        "\n".join(
            [
                "starttime = 2020-01-01T00:00:00",
                "endtime = 2020-01-02T00:00:00",
                'crs = "EPSG:28992"',
                'input_dir = "input"',
                'results_dir = "results"',
            ]
        ),
        encoding="utf-8",
    )

    xr.Dataset(
        data_vars={
            "dt": ("time", [60.0, 120.0]),
            "computation_time": ("time", [1000.0, 2000.0]),
        },
        coords={"time": pd.to_datetime(["2020-01-01", "2020-01-02"])},
    ).to_netcdf(results_dir / "solver_stats.nc")

    xr.Dataset(
        data_vars={
            "convergence": (
                ("time", "node_id"),
                [[0.1, 0.4], [0.3, 0.2]],
            )
        },
        coords={
            "time": pd.to_datetime(["2020-01-01", "2020-01-02"]),
            "node_id": [1, 2],
        },
    ).to_netcdf(results_dir / "basin.nc")

    output_path = write_performance(toml_path)

    assert output_path == model_dir / "performance.xlsx"
    assert output_path.exists()

    time_df = pd.read_excel(output_path, sheet_name="time")
    nodes_df = pd.read_excel(output_path, sheet_name="nodes")

    assert list(time_df.columns) == ["time", "dt", "computation_time"]
    assert list(nodes_df.columns) == [
        "node_id",
        "median_convergence",
        "mean_convergence",
        "max_convergence",
    ]
    assert nodes_df["node_id"].tolist() == [2, 1]
