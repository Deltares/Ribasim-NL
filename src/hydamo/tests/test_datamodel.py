from pathlib import Path

import hydamo


def test_initialize():
    damo = hydamo.HyDAMO()
    assert damo.version == "2.2"
    assert "stuw" in damo.layers
    assert isinstance(damo.stuw, hydamo.ExtendedGeoDataFrame)
    assert "kruinbreedte" in damo.stuw.columns
    # no data is loaded
    assert len(damo.stuw) == 0
    # no file is written
    path = Path("hydamo.gpkg")
    damo.to_geopackage(path)
    assert not path.is_file()
