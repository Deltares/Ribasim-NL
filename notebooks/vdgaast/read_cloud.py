## Read VdGaast deliverables to local directory
from ribasim_nl import CloudStorage

cloud = CloudStorage()

path = cloud.joinpath("Basisgegevens", "VanDerGaast_QH")

if path.exists():
    url = cloud.joinurl("Basisgegevens", "VanDerGaast_QH")
    cloud.download_content(url)
