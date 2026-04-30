# """Upload latest Ribasim-model to the GoodCloud as standalone functionality."""
#
# from ribasim_nl import CloudStorage
#
#
# def _extract_version(folder: str) -> tuple[int, int, int]:
#     """Extract version number from folder name.
#
#     :param folder: folder name, constructed as "{water_authority}_parameterized_{year}_{month}_{number}".
#
#     :return: version number split in a three-integer tuple
#     """
#     *_, year, month, number = folder.split("_")
#     return int(year), int(month), int(number)
#
#
# def _find_latest(water_authority: str, cloud: CloudStorage = CloudStorage()) -> str:
#     """Find the latest model scheme based on version.
#
#     :param water_authority: water authority
#     :param cloud: the GoodCloud
#
#     :return: latest model scheme version
#     """
#     wd = cloud.joinpath(water_authority, "modellen")
#     prefix = f"{water_authority}_parameterized"
#     models: list[tuple[int, int, int]] = [_extract_version(f) for f in wd.glob(f"{prefix}_*")]
#     latest = sorted(models, reverse=True)[0]
#     return "_".join(map(str, (prefix, *latest)))
#
#
# # def upload()
