"""Lump a detailed HyDAMO or D-HYDRO network into a Ribasim model.

This makes the input models `<authority>_2024_6_3` of the 11 gravity controlled water authorities, which their
`01_fix_model.py` scripts start from. It is a port of the HyDAMO and D-HYDRO parts of `ribasim_lumping` by Sweco
(Harm Nomden and Tessa Andringa), the version of July 2024 that made these models. Since the node and link IDs of the
input models follow from the order of the operations, the port keeps that order, emulating the library versions of then,
see `docs/dev/reproducing-models.qmd`.

1. `ribasim_nl.lumping.hydamo.apply_hydamo_changes` applies the manual changes to the HyDAMO data.
2. `ribasim_nl.lumping.hydamo.read_hydamo_network` or `ribasim_nl.lumping.dhydro.read_dhydro_network` reads the network.
3. `ribasim_nl.lumping.network.lump_network` splits it at the split nodes into basins.
4. `ribasim_nl.lumping.model.lumped_model` converts the lumped network into a Ribasim model.
"""
