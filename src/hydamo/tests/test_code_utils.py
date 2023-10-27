"""Tests for hydamo.code_utils"""
from hydamo import code_utils


def test_init():
    assert code_utils.CODES_CSV.exists()
    codes_gdf = code_utils.get_codes_df()
    assert not codes_gdf.empty
    assert code_utils.CODES_DF.equals(codes_gdf)


def test_find_codes():
    # check if we have all water authorities
    assert len(code_utils.find_codes("waterschap")) == 15
    assert len(code_utils.find_codes("hoogheemraadschap")) == 5
    assert len(code_utils.find_codes("wetterskip")) == 1


def test_generate_model_id():
    # see if we can find Zuid-Holland
    result = code_utils.find_codes("Zuid-Holland")
    assert result["wbh_code"] == code_utils.bgt_to_wbh_code(result["bgt_code"])

    model_id = code_utils.generate_model_id(code="test", bgt_code=result["bgt_code"])
    assert model_id == "NL.WBHCODE.69.test"
    assert model_id == code_utils.generate_model_id(
        code="test", wbh_code=result["wbh_code"]
    )
