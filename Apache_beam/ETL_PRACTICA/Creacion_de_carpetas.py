import os

folders = [
    "afs_process/configure",
    "afs_process/query",
    "afs_process/transformation",
    "afs_process/util",
]
files = [
    "afs_process/main.py",
    "afs_process/run_df.sh",
    "afs_process/setup.py",
    "afs_process/query/query.py",
    "afs_process/transformation/credit_financial_debt.py",
    "afs_process/transformation/process.py",
]

for folder in folders:
    os.makedirs(folder, exist_ok=True)

for file in files:
    with open(file, 'w') as f:
        f.write("")
