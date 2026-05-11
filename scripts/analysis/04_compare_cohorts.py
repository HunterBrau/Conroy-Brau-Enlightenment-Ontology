from pathlib import Path
import sys
from collections import Counter, defaultdict

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from cohorts import COMPARISON_COHORT_IDS, cohort_paths  # noqa: E402
from common import percentage, read_csv_if_exists, split_pipe_values  # noqa: E402


def build_summary_rows(project_root: Path) -> pd.DataFrame:
    rows = []
    for cohort_id in COMPARISON_COHORT_IDS:
        paths = cohort_paths(project_root, cohort_id)
        diagnostic_df = read_csv_if_exists(paths.interim_dir / "dataset_diagnostics_summary.csv")
        enrichment_df = read_csv_if_exists(paths.enrichment_summary_path)
        geographic_df = read_csv_if_exists(paths.processed_dir / "geographic_scope_summary.csv")

        for source_name, df in [
            ("dataset_diagnostics", diagnostic_df),
            ("wikidata_enrichment", enrichment_df),
            ("geographic_scope", geographic_df),
        ]:
            if df.empty:
                rows.append(
                    {
                        "cohort_id": cohort_id,
                        "source": source_name,
                        "metric": "missing_source_table",
                        "value": pd.NA,
                        "pct_of_rows": pd.NA,
                        "pct_of_entities": pd.NA,
                        "notes": f"Expected table was not found for {cohort_id}.",
                    }
                )
                continue

            for row in df.itertuples(index=False):
                rows.append(
                    {
                        "cohort_id": cohort_id,
                        "source": source_name,
                        "metric": getattr(row, "metric"),
                        "value": getattr(row, "value", getattr(row, "entity_count", pd.NA)),
                        "pct_of_rows": getattr(row, "pct_of_rows", pd.NA),
                        "pct_of_entities": getattr(row, "pct_of_entities", getattr(row, "pct", pd.NA)),
                        "notes": getattr(row, "notes", pd.NA),
                    }
                )

    return pd.DataFrame(rows)


def build_country_citizenship_rows(project_root: Path) -> pd.DataFrame:
    rows = []
    for cohort_id in COMPARISON_COHORT_IDS:
        paths = cohort_paths(project_root, cohort_id)
        enriched_df = read_csv_if_exists(paths.enriched_path)
        if enriched_df.empty:
            continue

        entity_df = enriched_df.drop_duplicates("wikidata_id")
        total_entities = entity_df["wikidata_id"].nunique()
        token_entities = {}
        token_label_counts = defaultdict(Counter)

        for row in entity_df.itertuples(index=False):
            ids = split_pipe_values(getattr(row, "citizenship_ids", pd.NA))
            labels = split_pipe_values(getattr(row, "citizenship_labels", pd.NA))
            label_lookup = dict(zip(ids, labels, strict=False))
            for token_id in ids:
                token_entities.setdefault(token_id, set()).add(row.wikidata_id)
                if label_lookup.get(token_id):
                    weight = 10 if len(ids) == 1 and len(labels) == 1 else 1
                    token_label_counts[token_id][label_lookup[token_id]] += weight

        for token_id, entity_ids in token_entities.items():
            label_counter = token_label_counts.get(token_id, Counter())
            token_label = label_counter.most_common(1)[0][0] if label_counter else pd.NA
            rows.append(
                {
                    "cohort_id": cohort_id,
                    "citizenship_id": token_id,
                    "citizenship_label": token_label,
                    "entity_count": len(entity_ids),
                    "cohort_total_entities": total_entities,
                    "cohort_pct": percentage(len(entity_ids), total_entities),
                    "wikidata_url": f"https://www.wikidata.org/wiki/{token_id}",
                }
            )

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).sort_values(["cohort_id", "entity_count"], ascending=[True, False])


def build_geographic_scope_rows(project_root: Path) -> pd.DataFrame:
    frames = []
    for cohort_id in COMPARISON_COHORT_IDS:
        paths = cohort_paths(project_root, cohort_id)
        summary_df = read_csv_if_exists(paths.processed_dir / "geographic_scope_summary.csv")
        if summary_df.empty:
            continue
        summary_df = summary_df.copy()
        summary_df.insert(0, "cohort_id", cohort_id)
        frames.append(summary_df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def build_language_representation_rows(project_root: Path) -> pd.DataFrame:
    frames = []
    for cohort_id in COMPARISON_COHORT_IDS:
        paths = cohort_paths(project_root, cohort_id)
        language_df = read_csv_if_exists(paths.processed_dir / "representation_language_summary.csv")
        if language_df.empty:
            continue
        language_df = language_df.copy()
        language_df.insert(0, "cohort_id", cohort_id)
        frames.append(language_df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def main() -> None:
    project_root = Path(__file__).resolve().parents[2]
    output_dir = project_root / "data" / "processed"
    output_dir.mkdir(parents=True, exist_ok=True)

    outputs = [
        (output_dir / "cohort_comparison_summary.csv", build_summary_rows(project_root)),
        (output_dir / "cohort_comparison_country_citizenship.csv", build_country_citizenship_rows(project_root)),
        (output_dir / "cohort_comparison_geographic_scope.csv", build_geographic_scope_rows(project_root)),
        (output_dir / "cohort_comparison_language_representation.csv", build_language_representation_rows(project_root)),
    ]

    for output_path, output_df in outputs:
        output_df.to_csv(output_path, index=False)

    print("Cohort comparison outputs complete.")
    for output_path, output_df in outputs:
        print(f"{output_path}: {len(output_df)} rows")


if __name__ == "__main__":
    main()
