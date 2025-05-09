"""
Addition of settings for 'wateraanvoer' by means of `ContinuousControl`-nodes and 'wateraanvoergebieden'.

NOTE: This is a non-working dummy file to provide guidance on how to implement these workflows.

Author: Gijs G. Hendrickx
"""

from peilbeheerst_model import ribasim_parametrization
from ribasim_nl import CloudStorage, Model


class ReplacementProcessor:
    """Replacement-class for `RibasimFeedbackProcessor` as a stand-in.

    The `RibasimFeedbackProcessor`-class deals with processing the feedback forms and translating its content to model
    modifications. Part of this class contains data-extraction focussed on 'aanvoer'-settings:
     -  `get_basin_aanvoer_corrections()`:
        Extract corrections on basin 'aanvoer'-flagging from the feedback forms. This information is provided in the
        sheet 'Aan_afvoer_basins' of the feedback form.

     -  `get_outlet_aanvoer_corrections()`:
        Extract corrections on outlet 'aanvoer'-flagging from the feedback forms. This information is provided in the
        sheet 'Aan_afvoer_outlets' of the feedback form.

     -  `basin_aanvoer_on`:
        Basins that must have 'aanvoer'-flagging: True.

     -  `basin_aanvoer_off`:
        Basins that must have 'aanvoer'-flagging: False.

     -  `outlet_aanvoer_on`:
        Outlets that must have 'aanvoer'-flagging: True.

     -  `outlet_aanvoer_off`:
        Outlets that must have 'aanvoer'-flagging: False.

    The `RibasimFeedbackProcessor` can be found here:

        src/peilbeheerst_model/peilbeheerst_model/ribasim_feedback_processor.py

    The above methods/properties are on lines 530-631 for further information and context.


    This class is added here to contain the properties that are called within the 'aanvoer'-flagging function in
    `ribasim_parametrization.py`:

        src/peilbeheerst_model/peilbeheerst_model/ribasim_parametrization.py

    Namely, the function `set_aanvoer_flags()` (lines 1097-1185).

    I recommend reading through its documentation to see all the possible tuning, especially related to handling
    geospatial data of the 'aanvoergebieden', which are not always provided as a nice-to-work-with *.shp-file. Within
    the `set_aanvoer_flags`-function, you can provide key-worded arguments for dealing with *.gpkg-files, but for more
    complex definitions of the 'aanvoergebieden', have a look at the `special_load_geometry()`-function (lines 464-685):

        src/peilbeheerst_model/peilbeheerst_model/supply.py

    This functions includes various methods to extract the relevant data from provided geospatial data/files.
    """

    @property
    def basin_aanvoer_on(self) -> tuple[int, ...]:
        """Basin 'aanvoer'-flagging: True.

        :return: basin-IDs
        :rtype: tuple[int, ...]
        """
        return ()

    @property
    def basin_aanvoer_off(self) -> tuple[int, ...]:
        """Basin 'aanvoer'-flagging: False.

        :return: basin-IDs
        :rtype: tuple[int, ...]
        """
        return ()

    @property
    def outlet_aanvoer_on(self) -> tuple[int, ...]:
        """Outlet 'aanvoer'-flagging: True.

        :return: outlet-IDs
        :rtype: tuple[int, ...]
        """
        return ()

    @property
    def outlet_aanvoer_off(self) -> tuple[int, ...]:
        """Outlet 'aanvoer'-flagging: False.

        :return: outlet-IDs
        :rtype: tuple[int, ...]
        """
        return ()


# model settings
waterschap = "ValleienVeluwe"
short_name = "venv"
model_id = "2025_4_0"

# connect with the GoodCloud
cloud = CloudStorage()

# collect relevant data from the GoodCloud
ribasim_model_dir = cloud.joinpath(waterschap, "modellen", f"{waterschap}_parameterized_model_{model_id}")
ribasim_toml = ribasim_model_dir / f"{short_name}.toml"
qlr_path = cloud.joinpath("Basisgegevens", "QGIS_qlr", "output_controle_202502.qlr")
aanvoer_path = cloud.joinpath(
    waterschap, "verwerkt", "1_ontvangen_data", "Na_levering_202401", "wateraanvoer", "Inlaatgebieden.shp"
)

cloud.synchronize(
    filepaths=[
        ribasim_model_dir,
        qlr_path,
        aanvoer_path,
    ]
)

# TODO: Check the ability to use the `RibasimFeedbackProcessor`-object.
# load feedback processor
processor = ReplacementProcessor()

# read model
model = Model.read(ribasim_toml)

# TODO: Remember to set the forcing conditions to be representative for a drought ('aanvoer'-conditions), or for
#  changing conditions (e.g., 1/3 precipitation, 2/3 evaporation).
# set forcing conditions

# re-parameterize
# TODO: The function-calls below do not properly work (yet) due to differences in workflow:
"""There are two ways to go about:
 1. Include all column-names to `model.pump.static.df` and `model.outlet.static.df` that are listed in the
    `find_upstream_downstream_target_levels()`-function (lines 1974-2095):

        src/peilbeheerst_model/peilbeheerst_model/ribasim_parametrization.py

    These are listed in lines 1994-2004 for `model.pump.static.df`, and in lines 1980-1987 for `model.outlet.static.df`.

 2. Include the columns 'meta_from_node_id', 'meta_to_node_id', 'meta_from_level', and 'meta_to_level' to
    `model.pump.static.df` and `model.outlet.static.df` in a similar fashion as done in the
     `find_upstream_downstream_target_levels()`-function (lines 1974-2095):

        src/peilbeheerst_model/peilbeheerst_model/ribasim_parametrization.py

    This can be found in lines 2008-2024, where the to and from node IDs are determined using the `Link`-data.

With option 1, the two commented calls of `find_upstream_downstream_target_levels()` should be uncommented, i.e., should
be called; with option 2, `find_upstream_downstream_target_levels()` is not required, but another stand-in function must
be written (and used).
"""
# ribasim_parametrization.find_upstream_downstream_target_levels(model, 'pump')
# ribasim_parametrization.find_upstream_downstream_target_levels(model, 'outlet')
ribasim_parametrization.set_aanvoer_flags(model, str(aanvoer_path), processor)
ribasim_parametrization.determine_min_upstream_max_downstream_levels(model, waterschap)

# TODO: The addition of `ContinuousControl`-nodes is subsequently a minor modification:
# ribasim_parametrization.add_continuous_control(model)

"""For the addition of `ContinuousControl`-nodes, it might be necessary to set `model.basin.static.df=None`, as the
`ContinuousControl`-nodes require `Time`-tables instead of `Static`-tables. If both are defined (for the same node,
Ribasim will raise an error and thus not execute.
"""
