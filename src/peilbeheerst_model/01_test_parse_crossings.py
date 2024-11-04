import pathlib
import warnings

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import shapely.geometry
import shapely.validation
import tqdm.auto as tqdm
from matplotlib.patches import Polygon

from peilbeheerst_model import ParseCrossings

polygons = {
    "perfect fit": {
        "peilgebieden": [
            shapely.geometry.Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
            shapely.geometry.Polygon([(2, 0), (4, 0), (4, 2), (2, 2)]),
        ]
    },
    "perfect fit star 1": {
        "peilgebieden": [
            shapely.geometry.Polygon([(0, 0), (1, 0), (2, 1.5), (1.5, 2), (0, 2)]),
            shapely.geometry.Polygon([(3, 0), (4, 0), (4, 2), (2.5, 2), (2, 1.5)]),
            shapely.geometry.Polygon([(1, 0), (3, 0), (2, 1.5)]),
            shapely.geometry.Polygon([(1.5, 2), (2.5, 2), (2, 1.5)]),
        ]
    },
    "perfect fit star 2": {
        "peilgebieden": [
            shapely.geometry.Polygon([(0, 0), (1, 0), (2, 0.5), (1.5, 2), (0, 2)]),
            shapely.geometry.Polygon([(3, 0), (4, 0), (4, 2), (2.5, 2), (2, 0.5)]),
            shapely.geometry.Polygon([(1, 0), (3, 0), (2, 0.5)]),
            shapely.geometry.Polygon([(1.5, 2), (2.5, 2), (2, 0.5)]),
        ]
    },
    "perfect fit on edge": {
        "peilgebieden": [
            shapely.geometry.Polygon([(0, 0), (2, 0), (2, 1.5), (0, 1.5)]),
            shapely.geometry.Polygon([(2, 0), (4, 0), (4, 2), (2, 2)]),
        ]
    },
    "narrow gap": {
        "peilgebieden": [
            shapely.geometry.Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
            shapely.geometry.Polygon([(2.1, 0), (4, 0), (4, 2), (2.1, 2)]),
        ]
    },
    "wide gap": {
        "peilgebieden": [
            shapely.geometry.Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
            shapely.geometry.Polygon([(3, 0), (4, 0), (4, 2), (3, 2)]),
        ]
    },
    "narrow overlap": {
        "peilgebieden": [
            shapely.geometry.Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
            shapely.geometry.Polygon([(1.9, 0), (4, 0), (4, 2), (1.9, 2)]),
        ]
    },
    "wide overlap": {
        "peilgebieden": [
            shapely.geometry.Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
            shapely.geometry.Polygon([(1, 0), (4, 0), (4, 2), (1, 2)]),
        ]
    },
    "single cross wide": {
        "peilgebieden": [
            shapely.geometry.Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
        ]
    },
    "single cross narrow": {
        "peilgebieden": [
            shapely.geometry.Polygon([(0, 0), (3.4, 0), (3.4, 2), (0, 2)]),
        ]
    },
    "single cross at edge": {
        "peilgebieden": [
            shapely.geometry.Polygon([(0, 0), (3.5, 0), (3.5, 2), (0, 2)]),
        ]
    },
    "single cross on edge": {
        "peilgebieden": [
            shapely.geometry.Polygon([(0, 0), (2, 0), (2, 1.5), (0, 1.5)]),
        ]
    },
    "perfect fit with complete overlap": {
        "peilgebieden": [
            shapely.geometry.Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
            shapely.geometry.Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
            shapely.geometry.Polygon([(2, 0), (4, 0), (4, 2), (2, 2)]),
        ]
    },
    "single cross wide with complete overlap": {
        "peilgebieden": [
            shapely.geometry.Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
            shapely.geometry.Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
        ]
    },
    "single cross at edge with complete overlap": {
        "peilgebieden": [
            shapely.geometry.Polygon([(0, 0), (3.5, 0), (3.5, 2), (0, 2)]),
            shapely.geometry.Polygon([(0, 0), (3.5, 0), (3.5, 2), (0, 2)]),
        ]
    },
    "polygon within polygon 1": {
        "peilgebieden": [
            shapely.geometry.Polygon([(0, 0), (3.0, 0), (3.0, 2), (0, 2)]),
            shapely.geometry.Polygon([(0.1, 0.1), (2.8, 0.1), (2.8, 1.9), (0.1, 1.9)]),
        ]
    },
    "polygon within polygon 2": {
        "peilgebieden": [
            shapely.geometry.Polygon([(0, 0), (3.4, 0), (3.4, 2), (0, 2)]),
            shapely.geometry.Polygon([(0.1, 0.1), (2.8, 0.1), (2.8, 1.9), (0.1, 1.9)]),
        ]
    },
    "polygon within polygon 3": {
        "peilgebieden": [
            shapely.geometry.Polygon([(0, 0), (4.0, 0), (4.0, 2), (0, 2)]),
            shapely.geometry.Polygon([(0.1, 0.1), (2.8, 0.1), (2.8, 1.9), (0.1, 1.9)]),
        ]
    },
    "polygon butterfly 1a": {
        "peilgebieden": [
            shapely.validation.make_valid(
                shapely.geometry.Polygon([(0.0, 0.0), (2.0, 0.5), (4.0, 0.0), (4.0, 2.0), (2.0, 0.5), (0.0, 2.0)])
            ),
        ]
    },
    "polygon butterfly 1b": {
        "peilgebieden": [
            shapely.validation.make_valid(
                shapely.geometry.Polygon([(0.0, 0.0), (2.0, 0.5), (4.0, 0.0), (4.0, 2.0), (2.0, 0.5), (0.0, 2.0)])
            ),
            shapely.geometry.Polygon([(0.0, 0.0), (2.0, 0.5), (4.0, 0.0)]),
        ]
    },
    "polygon butterfly 2a": {
        "peilgebieden": [
            shapely.validation.make_valid(
                shapely.geometry.Polygon([(0.0, 0.0), (2.0, 0.4), (4.0, 0.0), (4.0, 2.0), (2.0, 0.4), (0.0, 2.0)])
            ),
        ]
    },
    "polygon butterfly 2b": {
        "peilgebieden": [
            shapely.validation.make_valid(
                shapely.geometry.Polygon([(0.0, 0.0), (2.0, 0.4), (4.0, 0.0), (4.0, 2.0), (2.0, 0.4), (0.0, 2.0)])
            ),
            shapely.geometry.Polygon([(0.0, 0.0), (2.0, 0.4), (4.0, 0.0)]),
        ]
    },
    "polygon butterfly 2c": {
        "peilgebieden": [
            shapely.validation.make_valid(
                shapely.geometry.Polygon([(0.0, 0.0), (2.0, 0.6), (4.0, 0.0), (4.0, 2.0), (2.0, 0.6), (0.0, 2.0)])
            ),
        ]
    },
    "polygon butterfly 2d": {
        "peilgebieden": [
            shapely.validation.make_valid(
                shapely.geometry.Polygon([(0.0, 0.0), (2.0, 0.6), (4.0, 0.0), (4.0, 2.0), (2.0, 0.6), (0.0, 2.0)])
            ),
            shapely.geometry.Polygon([(0.0, 0.0), (2.0, 0.6), (4.0, 0.0)]),
        ]
    },
    "polygon butterfly 2e": {
        "peilgebieden": [
            shapely.validation.make_valid(
                shapely.geometry.Polygon([(0.0, 0.0), (2.0, 0.55), (4.0, 0.0), (4.0, 2.0), (2.0, 0.55), (0.0, 2.0)])
            ),
            shapely.geometry.Polygon([(0.0, 0.0), (2.0, 0.55), (4.0, 0.0)]),
        ]
    },
    "polygon butterfly 3a": {
        "peilgebieden": [
            shapely.geometry.Polygon([(0.0, 0.0), (2.0, 0.5), (0.0, 2.0)]),
            shapely.validation.make_valid(
                shapely.geometry.Polygon([(0.0, 0.0), (4.0, 0.0), (2.0, 0.5), (4.0, 2.0), (0.0, 2.0), (2.0, 0.5)])
            ),
        ]
    },
    "polygon butterfly 3b": {
        "peilgebieden": [
            shapely.geometry.Polygon([(0.0, 0.0), (2.0, 0.4), (0.0, 2.0)]),
            shapely.validation.make_valid(
                shapely.geometry.Polygon([(0.0, 0.0), (4.0, 0.0), (2.0, 0.4), (4.0, 2.0), (0.0, 2.0), (2.0, 0.4)])
            ),
        ]
    },
}

linelist = [
    shapely.geometry.LineString([(0.5, 0.5), (3.5, 0.5)]),
    shapely.geometry.LineString([(0.5, 0.7), (2.0, 0.7)]),
    shapely.geometry.LineString([(2.0, 0.7), (3.5, 0.7)]),
    shapely.geometry.LineString([(0.5, 0.9), (2.0, 0.9)]),
    shapely.geometry.LineString([(3.5, 0.9), (2.0, 0.9)]),
    shapely.geometry.LineString([(3.8, 0.9), (3.5, 0.9)]),
    shapely.geometry.LineString([(3.5, 1.5), (0.5, 1.5)]),
]

filterlist = [
    shapely.geometry.LineString([(1.0, 0.7), (2.0, 0.7)]),
    shapely.geometry.LineString([(2.0, 0.7), (3.0, 0.7)]),
    shapely.geometry.LineString([(1.0, 0.9), (3.0, 0.9)]),
]


nofilter = polygons.copy()
withfilter = polygons.copy()
for testname, options in nofilter.items():
    options["hydroobjecten"] = linelist.copy()
    nofilter[testname] = options.copy()

    options["duikersifonhevel"] = filterlist.copy()
    withfilter[testname] = options.copy()


nofilter["driehoek 1a"] = {
    "peilgebieden": [
        shapely.geometry.Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
        shapely.geometry.Polygon([(2, 0), (4, 0), (4, 2), (2, 2)]),
    ],
    "hydroobjecten": [
        shapely.geometry.LineString([(0.5, 0.5), (2, 1)]),
        shapely.geometry.LineString([(0.5, 1.5), (2, 1)]),
        shapely.geometry.LineString([(2, 1), (3.5, 0.5)]),
    ],
}
nofilter["driehoek 1b"] = {
    "peilgebieden": [
        shapely.geometry.Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
        shapely.geometry.Polygon([(2, 0), (4, 0), (4, 2), (2, 2)]),
    ],
    "hydroobjecten": [
        shapely.geometry.LineString([(0.5, 0.5), (2.1, 1)]),
        shapely.geometry.LineString([(0.5, 1.5), (2.1, 1)]),
        shapely.geometry.LineString([(2.1, 1), (3.5, 0.5)]),
    ],
}
nofilter["driehoek 1c"] = {
    "peilgebieden": [
        shapely.geometry.Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
        shapely.geometry.Polygon([(2, 0), (4, 0), (4, 2), (2, 2)]),
    ],
    "hydroobjecten": [
        shapely.geometry.LineString([(0.5, 0.5), (1.9, 1)]),
        shapely.geometry.LineString([(0.5, 1.5), (1.9, 1)]),
        shapely.geometry.LineString([(1.9, 1), (3.5, 0.5)]),
    ],
}
nofilter["driehoek 2"] = {
    "peilgebieden": [
        shapely.geometry.Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
        shapely.geometry.Polygon([(2, 0), (4, 0), (4, 2), (2, 2)]),
    ],
    "hydroobjecten": [
        shapely.geometry.LineString([(3.5, 1.5), (2, 1)]),
        shapely.geometry.LineString([(3.5, 0.5), (2, 1)]),
        shapely.geometry.LineString([(2, 1), (0.5, 0.5)]),
    ],
}
nofilter["driehoek 3"] = {
    "peilgebieden": [
        shapely.geometry.Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
        shapely.geometry.Polygon([(2, 0), (4, 0), (4, 2), (2, 2)]),
    ],
    "hydroobjecten": [
        shapely.geometry.LineString([(3.5, 1.5), (2, 1)]),
        shapely.geometry.LineString([(3.5, 0.5), (2, 1)]),
        shapely.geometry.LineString([(0.5, 0.5), (2, 1)]),
    ],
}
nofilter["volgorde groep 1"] = {
    "peilgebieden": [
        shapely.geometry.Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
        shapely.geometry.Polygon([(2, 0), (2.2, 0), (2.2, 2), (2, 2)]),
        shapely.geometry.Polygon([(2.2, 0), (4, 0), (4, 2), (2.2, 2)]),
    ],
    "hydroobjecten": [
        shapely.geometry.LineString([(0.5, 0.5), (2.1, 0.5)]),
        shapely.geometry.LineString([(2.1, 0.5), (3.5, 0.5)]),
    ],
}
nofilter["volgorde groep 2"] = {
    "peilgebieden": [
        shapely.geometry.Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
        shapely.geometry.Polygon([(2, 0), (2.2, 0), (2.2, 2), (2, 2)]),
        shapely.geometry.Polygon([(2.2, 0), (4, 0), (4, 2), (2.2, 2)]),
    ],
    "hydroobjecten": [
        shapely.geometry.LineString([(2.1, 0.5), (0.5, 0.5)]),
        shapely.geometry.LineString([(3.5, 0.5), (2.1, 0.5)]),
    ],
}
nofilter["volgorde groep 3"] = {
    "peilgebieden": [
        shapely.geometry.Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
        shapely.geometry.Polygon([(2, 0), (2.2, 0), (2.2, 2), (2, 2)]),
        shapely.geometry.Polygon([(2.2, 0), (4, 0), (4, 2), (2.2, 2)]),
    ],
    "hydroobjecten": [
        shapely.geometry.LineString([(0.5, 0.5), (2.1, 0.5)]),
        shapely.geometry.LineString([(3.5, 0.5), (2.1, 0.5)]),
    ],
}
nofilter["volgorde groep 4"] = {
    "peilgebieden": [
        shapely.geometry.Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
        shapely.geometry.Polygon([(2, 0), (2.2, 0), (2.2, 2), (2, 2)]),
        shapely.geometry.Polygon([(2.2, 0), (4, 0), (4, 2), (2.2, 2)]),
    ],
    "hydroobjecten": [
        shapely.geometry.LineString([(2.1, 0.5), (0.5, 0.5)]),
        shapely.geometry.LineString([(2.1, 0.5), (3.5, 0.5)]),
    ],
}


testdir = pathlib.Path("tests/data")
if not testdir.exists():
    testdir.mkdir()

for testlist in tqdm.tqdm([nofilter, withfilter]):
    for test_name, options in testlist.items():
        polyid = [f"poly_{i+1}" for i in range(len(options["peilgebieden"]))]
        polywl = [float(i + 1) for i in range(len(options["peilgebieden"]))]
        # df_peil = gpd.GeoDataFrame(dict(globalid=polyid, geometry=options["peilgebieden"]), crs="epsg:28992")
        # df_streef = gpd.GeoDataFrame(dict(globalid=polyid, waterhoogte=polywl, geometry=len(options["peilgebieden"]) * [None]), crs="epsg:28992")
        # lineid = [f"line_{i+1}" for i in range(len(options["hydroobjecten"]))]
        # df_hydro = gpd.GeoDataFrame(dict(globalid=lineid, geometry=options["hydroobjecten"]), crs="epsg:28992")

        df_peil = gpd.GeoDataFrame({"globalid": polyid, "geometry": options["peilgebieden"]}, crs="epsg:28992")
        df_streef = gpd.GeoDataFrame(
            {"globalid": polyid, "waterhoogte": polywl, "geometry": len(options["peilgebieden"]) * [None]},
            crs="epsg:28992",
        )

        lineid = [f"line_{i+1}" for i in range(len(options["hydroobjecten"]))]
        df_hydro = gpd.GeoDataFrame({"globalid": lineid, "geometry": options["hydroobjecten"]}, crs="epsg:28992")

        if "duikersifonhevel" not in options:
            # Empty filter
            gpkg_path1 = testdir.joinpath(f"nofilter_{test_name}.gpkg")
            df_hydro.to_file(gpkg_path1, layer="hydroobject")
            df_peil.to_file(gpkg_path1, layer="peilgebied")
            df_streef.to_file(gpkg_path1, layer="streefpeil")
            with warnings.catch_warnings():
                warnings.simplefilter(action="ignore", category=UserWarning)
                gpd.GeoDataFrame(columns=["globalid", "geometry"]).to_file(gpkg_path1, layer="stuw")
                gpd.GeoDataFrame(columns=["globalid", "geometry"]).to_file(gpkg_path1, layer="gemaal")
                gpd.GeoDataFrame(columns=["globalid", "geometry"]).to_file(gpkg_path1, layer="duikersifonhevel")
        else:
            # With filter
            gpkg_path2 = testdir.joinpath(f"withfilter_{test_name}.gpkg")
            df_hydro.to_file(gpkg_path2, layer="hydroobject")
            df_peil.to_file(gpkg_path2, layer="peilgebied")
            df_streef.to_file(gpkg_path2, layer="streefpeil")
            polyfl = [f"dsh_{i+1}" for i in range(len(options["duikersifonhevel"]))]
            # df_filter = gpd.GeoDataFrame(dict(globalid=polyfl, geometry=options["duikersifonhevel"]), crs="epsg:28992")
            df_filter = gpd.GeoDataFrame(
                {"globalid": polyfl, "geometry": options["duikersifonhevel"]}, crs="epsg:28992"
            )

            df_filter.to_file(gpkg_path2, layer="duikersifonhevel")
            with warnings.catch_warnings():
                warnings.simplefilter(action="ignore", category=UserWarning)
                gpd.GeoDataFrame(columns=["globalid", "geometry"]).to_file(gpkg_path2, layer="stuw")
                gpd.GeoDataFrame(columns=["globalid", "geometry"]).to_file(gpkg_path2, layer="gemaal")


def make_plot(df_peil, df_hydro, df_streef, df_filter, df_crossings):
    plt.close("all")
    fig, ax = plt.subplots(figsize=(8, 5), dpi=100)

    # old_len = len(df_peil)
    dfp = df_peil.copy().explode(index_parts=True)
    dfs = df_streef.set_index("globalid", inplace=False)
    for i, row in enumerate(dfp.itertuples()):
        coords = row.geometry.exterior.coords
        #         if old_len == len(dfp):
        xtext = row.geometry.centroid.x - 0.5
        ytext = row.geometry.centroid.y
        #         else:
        #             xtext = coords[0][0] + 0.05
        #             ytext = coords[0][1]
        if (i % 2) == 0:
            ytext += 0.05
        else:
            ytext -= 0.05

        ax.text(xtext, ytext, f"{row.globalid}, wl={dfs.waterhoogte.at[row.globalid]}m", alpha=0.5)
        ax.add_patch(Polygon(coords, alpha=0.5, lw=1, facecolor="powderblue", edgecolor="skyblue"))

    dfh = df_hydro.explode(index_parts=True)
    for row in dfh.itertuples():
        coords = np.array(row.geometry.coords)
        x, y = coords[:, 0], coords[:, 1]
        offset = row.geometry.interpolate(0.1, normalized=True)
        ax.arrow(
            x[0],
            y[0],
            offset.x - x[0],
            offset.y - y[0],
            shape="full",
            lw=0,
            length_includes_head=True,
            head_width=0.05,
            color="steelblue",
        )
        ax.text(offset.x, offset.y + 0.05, row.globalid)
        ax.plot(x, y, marker=".", markersize=5, lw=1, color="steelblue")

    for row in df_crossings.itertuples():
        if row.crossing_type == "-10":
            ax.plot(row.geometry.x, row.geometry.y, marker="s", markersize=5, color="olivedrab")
        elif row.crossing_type == "00":
            ax.plot(row.geometry.x, row.geometry.y, marker="s", markersize=5, color="indianred")
        else:
            print(f"{row.crossing_type=} not implemented")
            ax.plot(row.geometry.x, row.geometry.y, marker="s", markersize=5, color="yellow")

    dff = df_filter.explode(index_parts=True)
    for row in dff.itertuples():
        coords = np.array(row.geometry.coords)
        x, y = coords[:, 0], coords[:, 1]
        ax.plot(x, y, marker=".", markersize=5, lw=2, alpha=0.3, color="purple")

    ax.set_xlim([-0.1, 4.1])
    ax.set_ylim([-0.1, 2.1])
    ax.set_aspect("equal")
    fig.tight_layout()
    plt.show()


for i, gpkg_path in enumerate(sorted(testdir.glob("nofilter_*.gpkg"))):
    if gpkg_path.is_file() and gpkg_path.suffix == ".gpkg":
        #         if "butterfly 3b" not in gpkg_path.stem and "polygon within polygon 1" not in gpkg_path.stem:
        #             continue
        #         if "perfect fit on edge" not in gpkg_path.stem:
        #             continue
        #         if "polygon within polygon 1" not in gpkg_path.stem:
        #             continue
        # if "narrow gap" not in gpkg_path.stem:
        #     continue
        #         if "driehoek" not in gpkg_path.stem:
        #             continue
        #         if "nofilter_narrow gap" not in gpkg_path.stem:
        #             continue
        #         if "volgorde groep" not in gpkg_path.stem and "nofilter_polygon butterfly 2e" not in gpkg_path.stem and "nofilter_perfect fit star 2" not in gpkg_path.stem:
        #             continue
        #         if "nofilter_polygon butterfly 2e" not in gpkg_path.stem and "nofilter_perfect fit star 2" not in gpkg_path.stem:
        #             continue

        print(f"Test {i+1:02d}: {gpkg_path.stem}")
        cross = ParseCrossings(gpkg_path, disable_progress=True, show_log=True)
        df_crossings = cross.find_crossings_with_peilgebieden(
            "hydroobject", group_stacked=True, filterlayer=None, agg_links=False
        )
        df_crossings_valid = df_crossings[df_crossings.in_use].copy()

        test_output = df_crossings_valid.copy()
        test_output["geom_x"] = np.round(test_output.geometry.x, 8)
        test_output["geom_y"] = np.round(test_output.geometry.y, 8)
        test_output = test_output.drop(columns="geometry", inplace=False)

        # Make static test output
        test_output.to_csv(testdir.joinpath(f"output_{gpkg_path.stem}.csv"), index=False)

        print(df_crossings)
        print(df_crossings_valid)
        make_plot(
            cross.df_gpkg["peilgebied"],
            cross.df_gpkg["hydroobject"],
            cross.df_gpkg["streefpeil"],
            cross.df_gpkg["duikersifonhevel"],
            df_crossings_valid,
        )


for i, gpkg_path in enumerate(sorted(testdir.glob("withfilter_*.gpkg"))):
    if gpkg_path.is_file() and gpkg_path.suffix == ".gpkg":
        #         if "withfilter_polygon butterfly 1a" not in gpkg_path.stem:
        #             continue
        # if "withfilter_narrow gap" not in gpkg_path.stem:
        #     continue
        #         if "scheldestromen" not in gpkg_path.stem:
        #             continue

        print(f"Test {i+1:02d}: {gpkg_path.stem}")

        cross = ParseCrossings(gpkg_path, disable_progress=True, show_log=True)
        _, df_filter, df_crossings = cross.find_crossings_with_peilgebieden(
            "hydroobject", group_stacked=True, filterlayer="duikersifonhevel", agg_links=False
        )
        df_crossings_valid = df_crossings[df_crossings.in_use].copy()

        test_output = df_crossings_valid.copy()
        test_output["geom_x"] = np.round(test_output.geometry.x, 8)
        test_output["geom_y"] = np.round(test_output.geometry.y, 8)
        test_output = test_output.drop(columns="geometry", inplace=False)

        # Make static test output
        test_output.to_csv(testdir.joinpath(f"output_{gpkg_path.stem}.csv"), index=False)

        print(df_crossings)
        print(df_filter)
        print(df_crossings_valid)
        if "scheldestromen" not in gpkg_path.stem:
            make_plot(
                cross.df_gpkg["peilgebied"],
                cross.df_gpkg["hydroobject"],
                cross.df_gpkg["streefpeil"],
                cross.df_gpkg["duikersifonhevel"],
                df_crossings_valid,
            )
