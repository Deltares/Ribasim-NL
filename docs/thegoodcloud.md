# Connecting The Good Cloud

## OS environment variables
We recommend to set the following OS environment variables:
- `RIBASIM_NL_CLOUD_PASS`: password for the cloud, to be requested at Deltares
- `RIBASIM_NL_DATA_DIR`: directory with your local copy of data in the Ribasim-NL cloud

## Initialize the cloud
Import the `Cloud`` and initialize it
```
from ribasim_nl import Cloud
```

If you have set OS environment variables:
```
cloud = Cloud()
```

And else
```
cloud = Cloud(password=password, data_dir=my_data_dir)
```

## Find water authorities
To find available water authorities:
```
cloud.water_authorities
```

## Download water authority datasets
```
authority = "Rijkswaterstaat"

# to download external data (aangeleverd) only
cloud.download_aangeleverd(authority)

# to download manipulated data (verwerkt) only
cloud.download_verwerkt(authority)

# to download all
cloud.download_all(authority)
