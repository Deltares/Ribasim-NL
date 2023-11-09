# Connecting The Good Cloud

## OS environment variables
We recommend to set the following os environment variables:
- `RIBASIM_NL_CLOUD_PASS`: password for the cloud, to be requested at Deltares
- `RIBASIM_DATA_DIR`: directory with your local copy of data in de RIBASM_nl cloud

## Init the cloud
Import the cloud and initialize it
```
from ribasim_nl import CloudStorage
```

If you have set os environment variables:
```
cloud_store = CloudStorage()
```

And else
```
cloud_storage = CloudStorage(password=password, data_dir=my_data_dir)
```

## Find water authorities
To find available water authorities:
```
cloud_storage.water_authorities
```

## Download water authority data-sets
```
authority = "Rijkswaterstaat"

# to download external data (aangeleverd) only
cloud_storage.download_aangeleverd(authority)

# to download manipulated data (verwerkt) only
cloud_storage.download_verwerkt(authority)

# to download all
cloud_storage.download_all(authority)
