import logging
import os
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Tuple, Union
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
]  # noqa


def is_dir(item):
    return Path(item).suffix == ""


@dataclass
class Cloud:
    data_dir: Union[str, Path] = RIBASIM_NL_DATA_DIR
    user: str = RIBASIM_NL_CLOUD_USER
    url: List[str] = BASE_URL
    password: str = field(repr=False, default=RIBASIM_NL_CLOUD_PASS)

    def __post_init__(self):
        # check if user and password are specified
        if self.user is None:
            raise ValueError(
                """'user' is None. Provide it or set environment variable RIBASIM_NL_CLOUD_USER."""
            )
        if self.password is None:
            raise ValueError(
                """'password' is None. Provide it or set environment variable RIBASIM_NL_CLOUD_PASS."""
            )
        # check if we have correct credentials
        response = requests.get(self.url, auth=self.auth)
        if response.ok:
            logger.info("valid credentials")
        else:
            response.raise_for_status()

        # check if data_dir is specified
        if self.data_dir is None:
            raise ValueError(
                """'data_dir' is None. Provide it or set environment variable RIBASIM_NL_DATA_DIR."""
            )
        else:
            self.data_dir = Path(self.data_dir)

        # create data_dir if it doesn't exist
        if not self.data_dir.exists():
            self.data_dir.mkdir(parents=True)
            logger.info(f"{self.data_dir} is created")

    @property
    def source_data(self) -> List[str]:
        """List of all source_data (directories) in sub-folder 'Basisgegevens`."""
        return self.dirs("Basisgegevens")

    @property
    def auth(self) -> Tuple[str, str]:
        """Auth tuple for requests"""
        return (self.user, self.password)

    @property
    def water_authorities(self) -> List[str]:
        """List of all water authorities (directories)"""
        return WATER_AUTHORITIES

    def validate_authority(self, authority):
        if authority not in self.water_authorities:
            raise ValueError(f"""'{authority}' not in {self.water_authorities}""")

    def file_url(self, file_path: Union[str, Path]) -> str:
        relative_path = Path(file_path).relative_to(self.data_dir)

        return f"{self.url}/{relative_path}"

    def relative_url(self, file_url: str) -> str:
        return file_url[len(self.url) + 1 :]

    def file_path(self, file_url):
        relative_url = self.relative_url(file_url)
        return self.data_dir.joinpath(relative_url)

    def relative_path(self, file_path: Union[str, Path]):
        return Path(file_path).relative_to(self.data_dir)

    def joinurl(self, *args: str):
        if args:
            return f"{self.url}/{'/'.join(args)}"
        else:
            return self.url

    def joinpath(self, *args: str):
        self.data_dir.joinpath(*args)

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

    def content(self, url) -> Union[List[str], None]:
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
        List[str]
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

        response = requests.request(
            "PROPFIND", url, headers=headers, auth=self.auth, data=xml_data
        )

        if response.status_code != 207:
            response.raise_for_status()

        xml_tree = ElementTree.fromstring(response.text)
        namespaces = {"D": "DAV:"}
        content = [
            elem.text
            for elem in xml_tree.findall(".//D:displayname", namespaces=namespaces)
            if elem.text not in ["..", Path(url).name]  # Exclude the parent directory
        ]

        return content

    def dirs(self, *args) -> List[str]:
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
        List[str]
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
                if (
                    overwrite and path.exists()
                ):  # delete if exists and we want to overwrite
                    path.unlink()
                if not path.exists():  # download if it doesn't exist
                    logger.info(f"downloading file {path}")
                    self.download_file(file_url=item_url)

    def download_aangeleverd(self, authority: str, overwrite: bool = False):
        """Download all files in folder 'aangeleverd'"""
        self.validate_authority(authority)

        url = self.joinurl(authority, "aangeleverd")
        self.download_content(url, overwrite=overwrite)

    def download_all(self, authority, overwrite: bool = False):
        """Download all files for authority."""
        url = self.joinurl(authority)
        self.download_content(url, overwrite=overwrite)
