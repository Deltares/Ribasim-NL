from hydamo import code_utils


def test_bgt_init():
    """Test BGT_codes"""
    assert code_utils.BGT_CSV.exists()

    result = code_utils.find_bgt_code("groningen")
    assert isinstance(result, dict)
    assert len(result) == 2


def test_wbh_init():
    """Test initialization of global WBH_DF first time requested"""
    assert code_utils.WBH_CSV.exists()

    code_utils.find_wbh_code("groningen")


def test_bgt_to_wbh_code():
    result = code_utils.bgt_to_wbh_code("P0020")
    assert 61 in result.keys()
