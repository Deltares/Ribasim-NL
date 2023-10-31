# Connecting The Good Cloud

## modules
Just `os`, `pathlib`, `requests` are required to get started
```
Import os
from pathlib import Path
import requests
```

## global variables
We define a few global variables, `RIBASIM_NL_CLOUD_PASS` is to be supplied as an OOS environment variable.
You need to get one from Deltares first.

```
#
RIBASIM_NL_CLOUD_PASS = os.getenv("RIBASIM_NL_CLOUD_PASS")
RIBASIM_NL_CLOUD_USER = "nhi_api"
WEBDAV_URL = "https://deltares.thegood.cloud/remote.php/dav"
BASE_URL = f"{WEBDAV_URL}/files/{RIBASIM_NL_CLOUD_USER}/D-HYDRO modeldata"

try:
    assert RIBASIM_NL_CLOUD_PASS is not None
except AssertionError:
    raise ValueError(
        f"Put RIBASIM_NL_CLOUD_PASS in your os environment first."
    )
```

## local-path and remote url
An example-file, `my_file.ext` to upload. Please be aware to use a file_name without spaces (`" "`)
```
path = Path("my_file.ext")
url = f"{BASE_URL}/test_files/{path.name}"
```

## Uploading a file


```
def upload_file(url, path):
    with open(path, "rb") as f:
        r = requests.put(
            url, data=f, auth=(RIBASIM_NL_CLOUD_USER, RIBASIM_NL_CLOUD_PASS)
        )
    r.raise_for_status()

upload_file(url, path)
```

## Downloading a file

```
def download_file(url, path):
    r = requests.get(url, auth=(RIBASIM_NL_CLOUD_USER, RIBASIM_NL_CLOUD_PASS))
    r.raise_for_status()
    with open(path, "wb") as f:
        f.write(r.content)

download_file(url, path)
```
