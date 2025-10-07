# %%
import csv
import re
from pathlib import Path

# ==== CONFIGURE THESE ====
DIR_PATH = Path(
    "c:/Users/leeuw_je/Projecten/LWKM_Ribasim/IM_input_conversie/"
)  # directory with input.csv and source.txt
MAPPING_CSV = Path(DIR_PATH, "IM_riba_mapping.csv")
BOUNDWQ_PATH = Path(DIR_PATH, "BOUNDWQ.DAT")  # the big text file to search in
OUTPUT_PATH = Path(DIR_PATH, "B5_Bounddata.inc")  # where to write results

DELIM = ";"  # your example uses semicolons
SKIP_HEADER = False  # set True if the CSV has a header row
CASE_INSENSITIVE = False  # set True if matching should ignore case


# ========= IMPLEMENTATION =========
source = BOUNDWQ_PATH.read_text(encoding="utf-8", errors="ignore")

flags = re.MULTILINE | (re.IGNORECASE if CASE_INSENSITIVE else 0)


# Regex to locate: ITEM 'SECOND' (preserve quote style via named group q)
# e.g., matches: ITEM 'NL25_210708'
def item_pattern_for(second: str):
    return re.compile(rf"^\s*ITEM\s*(?P<q>['\"]) *{re.escape(second)} *(?P=q)", flags=flags)


# Regex to find the start of the next ITEM block (block delimiter)
NEXT_ITEM_START = re.compile(r"^\s*ITEM\b", flags=re.MULTILINE)

rows_processed = 0
found_in_text = 0
headers_rewritten = 0
missing = []


# %%
with OUTPUT_PATH.open("w", encoding="utf-8") as out, MAPPING_CSV.open(newline="", encoding="utf-8") as f:
    reader = csv.reader(f, delimiter=DELIM, skipinitialspace=True)
    for i, row in enumerate(reader):
        if SKIP_HEADER and i == 0:
            continue
        if not row or len(row) < 2:
            continue

        rows_processed += 1
        first = (row[0] or "").strip()
        second = (row[1] or "").strip()
        if not first or not second:
            continue

        # 1) Find the ITEM header line for this code
        pat = item_pattern_for(second)
        m = pat.search(source)
        if not m:
            missing.append(second)
            continue

        found_in_text += 1

        # 2) Determine block: from header start to next ITEM (or EOF)
        start = m.start()
        next_m = NEXT_ITEM_START.search(source, m.end())
        stop = next_m.start() if next_m else len(source)
        block = source[start:stop]

        # 3) Rewrite just that header to: ITEM 'FIRST' ; 'SECOND'
        # Reuse the same pattern but limit to one substitution in the block
        def repl(mh):
            q = mh.group("q")
            return f"ITEM {q}{first}{q} ; {q}{second}{q}"

        block_edited, nsubs = pat.subn(repl, block, count=1)
        headers_rewritten += nsubs

        out.write(block_edited.rstrip() + "\n\n")

print(f"Processed mapping rows: {rows_processed}")
print(f"Found in BOUNDWQ.DAT : {found_in_text}")
print(f"ITEM headers rewritten: {headers_rewritten}")
print(f"Not found in BOUNDWQ.DAT: {len(missing)}")
if missing:
    print("Missing codes (first 20): " + ", ".join(missing[:20]) + ("..." if len(missing) > 20 else ""))
# %%
