from pathlib import Path
import numpy as np
import pandas as pd
import pickle



def main():
    log_dir = Path("logs")
    csvs = list(log_dir.glob("node_*_log.csv"))
    node_csvs = [c for c in csvs if "leader" not in c.name]
    node_dfs = []
    for c in node_csvs:
        node_id = int(c.name.strip("node_log.csv"))
        c_df = pd.read_csv(c)
        c_df["node id"] = node_id
        node_dfs.append(c_df)

    leader_csvs = [c for c in csvs if "leader" in c.name]
    leader_dfs = []
    for c in leader_csvs:
        node_id = int(c.name.strip("node_leader_log.csv"))
        c_df = pd.read_csv(c)
        c_df["leader id"] = node_id
        leader_dfs.append(c_df)
    node_df = pd.concat(node_dfs)
    leader_df = pd.concat(leader_dfs)
    return



if __name__ == "__main__":
    main()




