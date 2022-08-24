import os

def safe_mkdir(path: str) -> None:
    if not os.path.exists(path):
        os.mkdir(path)