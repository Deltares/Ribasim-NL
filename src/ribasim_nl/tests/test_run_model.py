import pytest
from ribasim_nl.run_model import core_info

README = """# Ribasim

Ribasim is a water resources model.

```toml
version = "2026.1.2-114-g954a73d"
commit = "https://github.com/Deltares/Ribasim/tree/954a73d126"
branch = "https://github.com/Deltares/Ribasim/tree/manning-critical-flow"
```
"""


def test_core_info(tmp_path):
    (tmp_path / "README.md").write_text(README)
    assert core_info(tmp_path) == {
        "version": "2026.1.2-114-g954a73d",
        "commit": "https://github.com/Deltares/Ribasim/tree/954a73d126",
        "branch": "https://github.com/Deltares/Ribasim/tree/manning-critical-flow",
    }


def test_core_info_incomplete(tmp_path):
    with pytest.raises(AssertionError, match=r"No README\.md"):
        core_info(tmp_path)
    (tmp_path / "README.md").write_text("# Ribasim\n")
    with pytest.raises(AssertionError, match="No TOML block"):
        core_info(tmp_path)
    (tmp_path / "README.md").write_text(README.replace('branch = "', 'other = "'))
    with pytest.raises(AssertionError, match="branch"):
        core_info(tmp_path)
