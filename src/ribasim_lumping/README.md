# ribasim-lumping

This python package is used to develop an aggregated Ribasim network (Deltares, https://github.com/Deltares/Ribasim). Documentation: https://deltares.github.io/Ribasim/

This code was integrated into the Ribasim-NL repository after development in https://github.com/harm-nomden-sweco/ribasim_lumping.

### Objective
This python-package provides functions to translate a D-Hydro or a HyDAMO network into a simplified (aggregated/lumped) Ribasim-network:
- The user provides a list of locations where the network should be split, resulting into sub-networks which are called 'basins'';
- For these basins relations regarding waterlevel-watersurface-watervolume are calculated;
- Exchange of watervolume (flow) between basins takes place via these split locations.
- Stage-discharge relations are generated to define flow-rates between basins.

### Dependencies
Most important dependencies:
- NETWORKX (https://networkx.org/, Hagberg et al., 2008)
- UGRID (https://github.com/Deltares/UGridPy)
- XUGRID (https://github.com/Deltares/xugrid)

### Installation
We will make this package accessible via pypi. It is recommended to clone this repository because it is under development and it includes some example notebooks. We are still working on tests and test data, etc.

### Development, contributions and licences
This package is developed by Sweco (contributors: Harm Nomden and Tessa Andringa) when working on a TKI-project (top consortia for knowledge and innovation) within the NHI programme (Dutch Hydrological Instruments programme). This focuses on the development, testing and application of the new Ribasim-model (https://tkideltatechnologie.nl/project/oppervlaktewatermodule-nhi/).
It is possible to contribute, create issues, start discussions. We will respond as soon as possible.
This package is developed under the MIT license. Reference to this package: Ribasim-Lumping (Sweco, 2023).

### References:
Aric A. Hagberg, Daniel A. Schult and Pieter J. Swart, “Exploring network structure, dynamics, and function using NetworkX”, in Proceedings of the 7th Python in Science Conference (SciPy2008), Gäel Varoquaux, Travis Vaught, and Jarrod Millman (Eds), (Pasadena, CA USA), pp. 11–15, Aug 2008
