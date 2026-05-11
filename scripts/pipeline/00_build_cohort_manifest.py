from pathlib import Path
import sys

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from cohorts import cohort_manifest_rows  # noqa: E402


def main() -> None:
    project_root = Path(__file__).resolve().parents[2]
    output_path = project_root / "data" / "cohorts" / "cohort_manifest.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(cohort_manifest_rows(project_root)).to_csv(output_path, index=False)
    print(f"Cohort manifest written: {output_path}")


if __name__ == "__main__":
    main()
