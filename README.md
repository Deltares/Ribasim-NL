# Ribasim-NL

**Documentation: https://deltares.github.io/Ribasim-NL/**

[Ribasim](https://deltares.github.io/Ribasim/) is a water resources model. It includes the Python package [`ribasim`](https://deltares.github.io/Ribasim/python/), which aims to make it easy to build, update and analyze Ribasim models programmatically.

For the application of Ribasim in the Netherlands, specific tools need to be developed, to work with existing databases and models. This repository exists to collaborate on such tools. It aims to integrate and share existing efforts by Sweco, HKV and D2Hydro:

- [harm-nomden-sweco/ribasim_lumping](https://github.com/harm-nomden-sweco/ribasim_lumping)
- [rbruijnshkv/TKI-ribasim](https://github.com/rbruijnshkv/TKI-ribasim)
- [d2hydro/lhm-ribasim](https://github.com/d2hydro/lhm-ribasim)

The approach is to identify common functionalities, and add them one-by-one, directly adding documentation and tests. The goal is to be able to run entire workflows from base data to Ribasim model using the functions from Ribasim-NL.

Where needed developments will be coordinated with the generic functionality in [Deltares/Ribasim](https://github.com/Deltares/Ribasim).
