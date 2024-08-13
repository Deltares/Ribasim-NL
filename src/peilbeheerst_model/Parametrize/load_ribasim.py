import os
import sys
from pathlib import Path

# load ribasim_nl
module_path = Path.cwd() / "../../ribasim_nl/"
sys.path.append(str(module_path))

current_dir = os.getcwd()
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)
