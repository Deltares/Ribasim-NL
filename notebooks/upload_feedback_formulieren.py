# %%
from datetime import date

import xlwings as xw

from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()

WATER_AUTHORITIES = [
    "DrentsOverijsselseDelta",
]

FEEDBACK_XLS = cloud.joinpath("Basisgegevens", "feedbackformulier", "Feedback Formulier.xlsx")

for authority in WATER_AUTHORITIES:
    print(authority)
    model_dir = cloud.joinpath(authority, "modellen", authority)
    toml_file = next(model_dir.glob("*.toml"))
    model = Model.read(toml_file)

    # load feedback xlsx
    app = xw.App(visible=False)
    workbook = xw.Book(FEEDBACK_XLS)

    # add authority to Feedback_Formulier
    sheet = workbook.sheets["Feedback_Formulier"]
    sheet.range("B1").value = authority
    sheet.range("B3").value = date.today()

    # add Node_Data
    sheet = workbook.sheets["Node_Data"]
    df = model.node_table().df.reset_index()[["node_id", "name", "node_type", "subnetwork_id"]]
    sheet.range("A2").value = df.to_numpy()

    # add Edge_Data
    sheet = workbook.sheets["Edge_Data"]
    df = model.link.df
    df.loc[:, "from_node_type"] = model.edge_from_node_type
    df.loc[:, "to_node_type"] = model.edge_to_node_type
    df = model.link.df.reset_index()[
        [
            "link_id",
            "name",
            "from_node_type",
            "from_node_id",
            "to_node_type",
            "to_node_id",
            "edge_type",
            "subnetwork_id",
        ]
    ]
    sheet.range("A2").value = df.to_numpy()

    # write copy for authority
    excel_file = cloud.joinpath(authority, "verwerkt", FEEDBACK_XLS.name)
    workbook.save(excel_file)
    workbook.close()
    app.quit()

    # upload file to cloud
    cloud.upload_file(excel_file)
