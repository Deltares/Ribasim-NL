"""Generate DVC DAG mermaid file with left-to-right layout."""

import subprocess
import sys

result = subprocess.run(["dvc", "dag", "--mermaid"], capture_output=True, text=True)
if result.returncode != 0:
    print(result.stderr, file=sys.stderr)
    sys.exit(result.returncode)

print(result.stdout.replace("flowchart TD", "flowchart LR"), end="")
