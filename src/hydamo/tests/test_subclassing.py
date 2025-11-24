"""Test GeoPandas subclassing functionality for ExtendedGeoDataFrame."""

from hydamo.datamodel import ExtendedGeoDataFrame
from shapely.geometry import Point


def test_extended_geodataframe_copy():
    """Test that copy operations preserve ExtendedGeoDataFrame type and metadata."""
    # Create an ExtendedGeoDataFrame with metadata
    validation_schema = [{"id": "test_col", "dtype": "str", "required": True}]
    egdf = ExtendedGeoDataFrame(
        validation_schema=validation_schema,
        geotype=None,  # Use None instead of list for simpler testing
        layer_name="test_layer",
        required_columns=["test_col", "geometry"],
    )

    # Add some data
    egdf["test_col"] = ["value1", "value2"]
    egdf["geometry"] = [Point(0, 0), Point(1, 1)]

    # Test copy operation
    copied = egdf.copy()

    # Verify type preservation
    assert isinstance(copied, ExtendedGeoDataFrame), f"Copy returned {type(copied)} instead of ExtendedGeoDataFrame"

    # Verify metadata preservation
    assert copied.validation_schema == validation_schema
    assert copied.geotype is None
    assert copied.layer_name == "test_layer"
    assert copied.required_columns == ["test_col", "geometry"]

    # Verify data preservation
    assert list(copied["test_col"]) == ["value1", "value2"]
    assert len(copied.geometry) == 2


def test_extended_geodataframe_slice():
    """Test that slicing operations work correctly."""
    # Create an ExtendedGeoDataFrame
    egdf = ExtendedGeoDataFrame(validation_schema=[], geotype=None, layer_name="test", required_columns=[])

    # Add some data
    egdf["test_col"] = [1, 2, 3, 4, 5]
    egdf["geometry"] = [Point(i, i) for i in range(5)]

    # Test slicing
    sliced = egdf.iloc[1:3]

    # Should return ExtendedGeoDataFrame for multi-row slice
    assert isinstance(sliced, ExtendedGeoDataFrame)
    assert len(sliced) == 2
    assert list(sliced["test_col"]) == [2, 3]


def test_extended_geodataframe_operations():
    """Test that common GeoPandas operations preserve the subclass."""
    # Create an ExtendedGeoDataFrame
    egdf = ExtendedGeoDataFrame(
        validation_schema=[{"id": "value", "dtype": "int"}],
        geotype=None,
        layer_name="test_ops",
        required_columns=["value"],
    )

    # Add data
    egdf["value"] = [1, 2, 3]
    egdf["geometry"] = [Point(i, i) for i in range(3)]

    # Filter operation should preserve type
    filtered = egdf[egdf["value"] > 1]
    assert isinstance(filtered, ExtendedGeoDataFrame)
    assert filtered.layer_name == "test_ops"

    # Reset index should preserve type
    reset = egdf.reset_index()
    assert isinstance(reset, ExtendedGeoDataFrame)


def test_extended_geodataframe_empty():
    """Test that empty ExtendedGeoDataFrame works correctly."""
    egdf = ExtendedGeoDataFrame(validation_schema=[], geotype=None, layer_name="empty_test", required_columns=[])

    # Should be empty but have correct type
    assert isinstance(egdf, ExtendedGeoDataFrame)
    assert egdf.empty
    assert egdf.layer_name == "empty_test"

    # Copy of empty should work
    copied = egdf.copy()
    assert isinstance(copied, ExtendedGeoDataFrame)
    assert copied.empty
    assert copied.layer_name == "empty_test"


def test_extended_geodataframe_constructor_flexibility():
    """Test that the constructor handles various parameter combinations."""
    # Test with minimal parameters
    egdf1 = ExtendedGeoDataFrame()
    assert isinstance(egdf1, ExtendedGeoDataFrame)
    assert egdf1.validation_schema == []
    assert egdf1.geotype is None
    assert egdf1.layer_name == ""
    assert egdf1.required_columns == []

    # Test with all parameters
    egdf2 = ExtendedGeoDataFrame(
        validation_schema=[{"test": "value"}], geotype=None, layer_name="full_test", required_columns=["col1", "col2"]
    )
    assert egdf2.validation_schema == [{"test": "value"}]
    assert egdf2.geotype is None
    assert egdf2.layer_name == "full_test"
    assert egdf2.required_columns == ["col1", "col2"]
