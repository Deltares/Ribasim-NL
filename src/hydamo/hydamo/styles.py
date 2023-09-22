from pathlib import Path
import sqlite3
import fiona
from datetime import datetime
import re

STYLES_DIR = Path(__file__).parent.joinpath("data", "styles")

CREATE_TABLE_SQL = """
CREATE TABLE "layer_styles" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "f_table_catalog" TEXT(256),
    "f_table_schema" TEXT(256),
    "f_table_name" TEXT(256),
    "f_geometry_column" TEXT(256),
    "styleName" TEXT(30),
    "styleQML" TEXT,
    "styleSLD" TEXT,
    "useAsDefault" BOOLEAN,
    "description" TEXT,
    "owner" TEXT(30),
    "ui" TEXT(30),
    "update_time" DATETIME DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
"""
DROP_TABLE_SQL = """DROP TABLE IF EXISTS "layer_styles";"""

INSERT_ROW_SQL = """
INSERT INTO "main"."layer_styles" (
    "f_table_catalog",
    "f_table_schema",
    "f_table_name",
    "f_geometry_column",
    "styleName",
    "styleQML",
    "styleSLD",
    "useAsDefault",
    "description",
    "owner",
    "ui",
    "update_time"
)
VALUES (
    '',
    '',
    '{layer}',
    'geom',
    '{layer}',
    '{style_qml}',
    '{style_sld}',
    '1',
    '{description}',
    '',
    '',
    '{update_date_time}'
);
"""

def read_style(style_path: Path) -> str:
    """
    To make style-text sql-compatible, we need to replace single ' to ''.
    Example 'http://mrcc.com/qgis.dtd -> ''http://mrcc.com/qgis.dtd''

    Parameters
    ----------
    style_path : Path
        Path to sld-file

    Returns
    -------
    str
        style-string for SQL

    """
    style_txt = style_path.read_text()

    pattern = r"'(.*?)'"
    style_txt = re.sub(pattern, lambda m: f"''{m.group(1)}''", style_txt)

    return style_txt
    

def add_styles_to_geopackage(gpkg_path: Path):
    """
    Add styles to a HyDAMO GeoPackage

    Parameters
    ----------
    gpkg_path : Path
        Path to HyDAMO GeoPackage

    Returns
    -------
    None.

    """

    with sqlite3.connect(gpkg_path) as conn:

        # create table
        conn.execute(DROP_TABLE_SQL)
        conn.execute(CREATE_TABLE_SQL)

        # add style per layer
        for layer in fiona.listlayers(gpkg_path):
            style_qml = (STYLES_DIR / f"{layer}.qml")
            style_sld = (STYLES_DIR / f"{layer}.sld")

            # check if style exists
            if style_qml.exists() and style_sld.exists():
                description = f"HyDAMO style for layer: {layer}"
                update_date_time = f"{datetime.now().isoformat()}Z"

                # push to GeoPackage
                conn.execute(INSERT_ROW_SQL.format(
                    layer=layer,
                    style_qml=read_style(style_qml),
                    style_sld= read_style(style_sld),
                    description=description,
                    update_date_time=update_date_time
                    ))