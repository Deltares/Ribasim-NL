import logging
import os
import re
import shutil
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from xml.etree import ElementTree

import requests

logger = logging.getLogger(__name__)

RIBASIM_NL_CLOUD_PASS = os.getenv("RIBASIM_NL_CLOUD_PASS")
RIBASIM_NL_DATA_DIR = os.getenv("RIBASIM_NL_DATA_DIR")
RIBASIM_NL_CLOUD_USER = "nhi_api"
WEBDAV_URL = "https://deltares.thegood.cloud/remote.php/dav"
BASE_URL = rf"{WEBDAV_URL}/files/{RIBASIM_NL_CLOUD_USER}/Ribasim modeldata"

WATER_AUTHORITIES = [
    "AaenMaas",
    "AmstelGooienVecht",
    "BrabantseDelta",
    "DeDommel",
    "Delfland",
    "DrentsOverijsselseDelta",
    "HollandseDelta",
    "HollandsNoorderkwartier",
    "HunzeenAas",
    "Limburg",
    "Noorderzijlvest",
    "RijnenIJssel",
    "Rijnland",
    "Rivierenland",
    "Rijkswaterstaat",
    "Scheldestromen",
    "SchielandendeKrimpenerwaard",
    "StichtseRijnlanden",
    "ValleienVeluwe",
    "Vechtstromen",
    "WetterskipFryslan",
    "Zuiderzeeland",
]

HIDDEN_DIRS = ["D-HYDRO modeldata"]  # somehow this dir-name still exists :-(


def is_dir(item):
    """Check if path suggests a directory (even if it doesn't exist yet)"""
    return Path(item).suffix == ""


@dataclass
class ModelVersion:
    model: str
    year: int
    month: int
    revision: int

    @property
    def version(self):
        return f"{self.year}.{self.month}.{self.revision}"

    @property
    def path_string(self):
        return f"{self.model}_{self.year}_{self.month}_{self.revision}"

    @property
    def sorter(self):
        return f"{self.year}.{str(self.month).zfill(2)}.{str(self.revision).zfill(3)}"


@dataclass
class CloudStorage:
    """Connect a local 'data_dir` to cloud-storage."""

    data_dir: str | Path = RIBASIM_NL_DATA_DIR
    user: str = RIBASIM_NL_CLOUD_USER
    url: list[str] = BASE_URL
    password: str = field(repr=False, default=RIBASIM_NL_CLOUD_PASS)

    def __post_init__(self):
        # check if user and password are specified
        if self.user is None:
            raise ValueError("""'user' is None. Provide it or set environment variable RIBASIM_NL_CLOUD_USER.""")
        if self.password is None:
            raise ValueError("""'password' is None. Provide it or set environment variable RIBASIM_NL_CLOUD_PASS.""")
        # check if we have correct credentials
        response = requests.get(self.url, auth=self.auth)
        if response.ok:
            logger.info("valid credentials")
        else:
            response.raise_for_status()

        # check if data_dir is specified
        if self.data_dir is None:
            raise ValueError("""'data_dir' is None. Provide it or set environment variable RIBASIM_NL_DATA_DIR.""")
        else:
            self.data_dir = Path(self.data_dir)

        # create data_dir if it doesn't exist
        if not self.data_dir.exists():
            self.data_dir.mkdir(parents=True)
            logger.info(f"{self.data_dir} is created")

    @property
    def source_data(self) -> list[str]:
        """List of all source_data (directories) in sub-folder 'Basisgegevens`."""
        url = self.joinurl("Basisgegevens")
        return self.content(url)

    @property
    def auth(self) -> tuple[str, str]:
        """Auth tuple for requests"""
        return (self.user, self.password)

    @property
    def water_authorities(self) -> list[str]:
        """List of all water authorities (directories)"""
        return WATER_AUTHORITIES

    def validate_authority(self, authority):
        if authority not in self.water_authorities:
            raise ValueError(f"""'{authority}' not in {self.water_authorities}""")

    def file_url(self, file_path: str | Path) -> str:
        relative_path = Path(file_path).relative_to(self.data_dir)

        return f"{self.url}/{relative_path.as_posix()}"

    def relative_url(self, file_url: str) -> str:
        return file_url[len(self.url) + 1 :]

    def file_path(self, file_url):
        relative_url = self.relative_url(file_url)
        return self.data_dir.joinpath(relative_url)

    def relative_path(self, file_path: str | Path):
        return Path(file_path).relative_to(self.data_dir)

    def joinurl(self, *args: str):
        if args:
            return f"{self.url}/{'/'.join(args)}"
        else:
            return self.url

    def joinpath(self, *args: str):
        return self.data_dir.joinpath(*args)

    def upload_file(self, file_path: Path):
        # get url
        url = self.file_url(file_path)

        # read file and upload
        with open(file_path, "rb") as f:
            r = requests.put(url, data=f, auth=self.auth)
        r.raise_for_status()

    def download_file(self, file_url: str):
        # get local file-path
        file_path = self.file_path(file_url)

        # download file
        r = requests.get(file_url, auth=self.auth)
        r.raise_for_status()

        # make directory
        file_path.parent.mkdir(exist_ok=True, parents=True)

        # write file
        with open(file_path, "wb") as f:
            f.write(r.content)

    def content(self, url) -> list[str] | None:
        """List all content in a directory

        User can specify a path to the directory with additional arguments.

        Examples
        --------
        >>> cloud = Cloud()
        >>> cloud.dirs()
            ["AaenMaas", "AmselGooienVecht", ...]
        >>> cloud.dirs("AaenMaas")
            ["aangeleverd", "modellen", "verwerkt"]

        Returns
        -------
        list[str]
            List of all content directories in a specified path
        """

        headers = {"Depth": "1", "Content-Type": "application/xml"}

        xml_data = """
        <D:propfind xmlns:D="DAV:">
        <D:prop>
            <D:displayname />
        </D:prop>
        </D:propfind>
        """

        response = requests.request("PROPFIND", url, headers=headers, auth=self.auth, data=xml_data)

        if response.status_code != 207:
            response.raise_for_status()

        xml_tree = ElementTree.fromstring(response.text)
        namespaces = {"D": "DAV:"}
        excluded_content = ["..", Path(url).name] + HIDDEN_DIRS
        content = [
            elem.text
            for elem in xml_tree.findall(".//D:displayname", namespaces=namespaces)
            if elem.text not in excluded_content  # Exclude the parent directory
        ]

        return content

    def dirs(self, *args) -> list[str]:
        """List sub-directories in a directory

        User can specify a path to the directory with additional arguments.

        Examples
        --------
        >>> cloud = Cloud()
        >>> cloud.dirs()
            ["AaenMaas", "AmselGooienVecht", ...]
        >>> cloud.dirs("AaenMaas")
            ["aangeleverd", "modellen", "verwerkt"]

        Returns
        -------
        list[str]
            List of directories in a specified path
        """

        content = self.content(*args)

        return [item for item in content if is_dir(item)]

    def create_dir(self, *args):
        if args:
            url = self.joinurl(*args)
            requests.request(
                "MKCOL",
                url,
                headers={
                    "Depth": "0",
                },
                auth=self.auth,
            )

    def download_content(self, url, overwrite: bool = False):
        """Download content of a directory recursively."""

        # get all content (files and directories from url)
        content = self.content(url)

        # iterate over content
        for item in content:
            item_url = f"{url}/{item}"
            relative_url = self.relative_url(item_url)
            path = self.data_dir.joinpath(relative_url)
            # if it is a directory we (re)create it (if it doesn't exist)
            if is_dir(item):
                if overwrite and path.exists():  # remove if we want to overwrite
                    shutil.rmtree(path)
                logger.info(f"making dir {path}")
                path.mkdir(parents=True, exist_ok=True)  # create if it doesn't exist
                self.download_content(item_url)  # get content of directory
            else:
                if overwrite and path.exists():  # delete if exists and we want to overwrite
                    path.unlink()
                if not path.exists():  # download if it doesn't exist
                    logger.info(f"downloading file {path}")
                    self.download_file(file_url=item_url)

    def upload_content(self, dir_path: Path, overwrite: bool = False):
        """Upload content of a directory recursively."""

        # get all remote content
        content = self.content(self.joinurl(self.relative_path(dir_path).as_posix()))

        # get al local directories and files.
        dirs = [i for i in dir_path.glob("*") if i.is_dir()]
        files = [i for i in dir_path.glob("*") if i.is_file()]

        # add files
        for file_path in files:
            if (file_path.stem not in content) or overwrite:
                logger.info(f"uploading file {file_path}")
                self.upload_file(file_path)

        # add dirs and upload their content recursively
        for path in dirs:
            if path.stem not in content:
                remote_path = self.relative_path(path).as_posix()
                self.create_dir(remote_path)
                self.upload_content(dir_path=dir_path.joinpath(path.stem), overwrite=overwrite)

    def download_aangeleverd(self, authority: str, overwrite: bool = False):
        """Download all files in folder 'aangeleverd'"""
        self.validate_authority(authority)

        url = self.joinurl(authority, "aangeleverd")
        self.download_content(url, overwrite=overwrite)

    def download_verwerkt(self, authority: str, overwrite: bool = False):
        """Download all files in folder 'verwerkt'"""
        self.validate_authority(authority)

        url = self.joinurl(authority, "verwerkt")
        self.download_content(url, overwrite=overwrite)

    def download_basisgegevens(self, bronnen: list[str] = [], overwrite=True):
        """Download sources in the folder 'Basisgegevens'"""
        source_data = self.source_data
        if not bronnen:
            bronnen = source_data

        for bron in bronnen:
            if bron not in source_data:
                raise ValueError(f"""{bron} not in {source_data}""")
            else:
                url = self.joinurl("Basisgegevens", bron)
                self.download_content(url, overwrite=overwrite)

    def upload_verwerkt(self, authority: str, overwrite: bool = False):
        """Upload all files in folder 'verwerkt'"""
        self.validate_authority(authority)

        dir_path = self.joinpath(authority, "verwerkt")
        self.upload_content(dir_path, overwrite=overwrite)

    def download_all(self, authority, overwrite: bool = False):
        """Download all files for authority."""
        url = self.joinurl(authority)
        self.download_content(url, overwrite=overwrite)

    def uploaded_models(self, authority):
        """Get all model versions uploaded for an authority"""

        # function to strip version from a models dir
        def strip_version(dir: str):
            pattern = r"^(.*)_([\d]+)_([\d]+)_([\d]+)$"
            match = re.match(pattern, dir)
            return ModelVersion(
                match.group(1),
                int(match.group(2)),
                int(match.group(3)),
                int(match.group(4)),
            )

        # get uploaded_models
        models_url = self.joinurl(authority, "modellen")
        uploaded_models = self.content(models_url)

        return [strip_version(i) for i in uploaded_models]

    def upload_model(self, authority: str, model: str, include_results=False, include_plots=False):
        """Upload a model to a water authority

        Parameters
        ----------
        authority : str
            Water authority to upload a model for
        model : str
            name of the model (directory) to upload
        include_results: bool, optional
            to include results dir in upload; yes/no = True/False. defaults to False.
        include_plots: bool, optional
            to include plots dir in upload; yes/no = True/False. defaults to False.

        Raises
        ------
        ValueError
            If model does not exist locally
        """

        # get today, so we can later derive a version
        today = date.today()

        # check if model-directory exists locally
        model_dir = self.joinpath(authority, "modellen", model)

        if not model_dir.exists():
            raise ValueError(f"""model at '{model_dir}' does not exis.""")

        # check previously uploaded models to get a revision number
        uploaded_models = self.uploaded_models(authority=authority)
        monthly_revisions = [
            i.revision
            for i in uploaded_models
            if (i.model == model) and (i.year == today.year) and (i.month == today.month)
        ]

        if monthly_revisions:
            revision = max(monthly_revisions) + 1
        else:
            revision = 0

        # create local version_directory
        model_version_dir = model_dir.parent.joinpath(f"{model}_{today.year}_{today.month}_{revision}")
        if model_version_dir.exists():
            shutil.rmtree(model_version_dir)
        model_version_dir.mkdir()

        # copy model content to version dir
        for file in model_dir.glob("*.*"):
            if file.suffix not in ["", ".mypy_cache", ".tmp", ".bak"]:
                out_file = model_version_dir / file.name
                out_file.write_bytes(file.read_bytes())

        # if results, copy too
        if include_results and (model_dir.joinpath("results").exists()):
            files = list(model_dir.joinpath("results").glob("*.*"))
            if files:
                results_dir = model_version_dir.joinpath("results")
                results_dir.mkdir()
                for file in files:
                    out_file = results_dir / file.name
                    out_file.write_bytes(file.read_bytes())

        # if plots, copy too
        if include_plots and (model_dir.joinpath("plots").exists()):
            files = list(model_dir.joinpath("plots").glob("*.*"))
            if files:
                plots_dir = model_version_dir.joinpath("plots")
                plots_dir.mkdir()
                for file in files:
                    out_file = plots_dir / file.name
                    out_file.write_bytes(file.read_bytes())

        # create dir in CloudStorage and upload content
        self.create_dir(authority, "modellen", model_version_dir.name)
        self.upload_content(model_version_dir)

        return ModelVersion(model, today.year, today.month, revision)
