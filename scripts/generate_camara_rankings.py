from runpy import run_path
from pathlib import Path


if __name__ == "__main__":
    target = Path(__file__).resolve().parent / "federal" / "deputados" / "generate_camara_rankings.py"
    run_path(str(target), run_name="__main__")
