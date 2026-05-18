# %%
import pandas as pd
from ribasim_nl.aquo import waterbeheercode

prefix_to_authority = {v: k for k, v in waterbeheercode.items()}

df = pd.DataFrame(
    [
        {"Van LevelBoundary": 5900026, "Naar LevelBoundary": 206453, "Opmerking": "heeft zowel inflows als outflows"},
        {"Van LevelBoundary": 5900026, "Naar LevelBoundary": 207169, "Opmerking": "heeft zowel inflows als outflows"},
        {"Van LevelBoundary": 1101728, "Naar LevelBoundary": 1302562, "Opmerking": "wrong direction"},
        {"Van LevelBoundary": 1302093, "Naar LevelBoundary": 1500956, "Opmerking": "wrong direction"},
        {"Van LevelBoundary": 1500862, "Naar LevelBoundary": 3901198, "Opmerking": "wrong direction"},
        {"Van LevelBoundary": 2700002, "Naar LevelBoundary": 3800043, "Opmerking": "outlet + junction"},
        {"Van LevelBoundary": 3800032, "Naar LevelBoundary": 6000144, "Opmerking": "wrong direction"},
        {"Van LevelBoundary": 3800035, "Naar LevelBoundary": 6000122, "Opmerking": "junction + outlet"},
        {"Van LevelBoundary": 3800036, "Naar LevelBoundary": 6000003, "Opmerking": "meerdere outlets"},
        {"Van LevelBoundary": 3800054, "Naar LevelBoundary": 6000006, "Opmerking": "junction + outlet"},
        {"Van LevelBoundary": 5900067, "Naar LevelBoundary": 4400011, "Opmerking": "multiple inlets/outlets"},
        {"Van LevelBoundary": 4400015, "Naar LevelBoundary": 5900072, "Opmerking": "outlet + pump"},
        {"Van LevelBoundary": 4402320, "Naar LevelBoundary": 5900062, "Opmerking": "wrong direction"},
    ]
)

prefix_to_authority = {v: k for k, v in waterbeheercode.items()}

df["Van Waterschap"] = (df["Van LevelBoundary"] // 100000).map(prefix_to_authority)
df["Naar Waterschap"] = (df["Naar LevelBoundary"] // 100000).map(prefix_to_authority)


node_ids = [210320, 210643, 1303834, 3402608, 3402743, 3402744]
control_df = pd.DataFrame({"node_id": node_ids})
control_df["waterschap_code"] = control_df["node_id"] // 100000
control_df["waterschap"] = control_df["waterschap_code"].map(prefix_to_authority)
