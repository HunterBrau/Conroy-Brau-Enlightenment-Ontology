"""
Build review tables for the political-entity crosswalk.

The seed crosswalk is intentionally human-authored. This script keeps its
manual review labels separate from calculated evidence. It adds punchcard-style
review flags and reports whether each QID is active in the current cohort
evidence.
"""

from argparse import ArgumentParser
from pathlib import Path
import sys

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from cohorts import DEFAULT_COHORT_ID, cohort_paths  # noqa: E402

from crosswalk import MANUAL_REVIEW_SORT_ORDER, load_political_crosswalk  # noqa: E402


COHORT_IDS = ["french_seed", "global_writers"]
TOKEN_COLUMNS = [
    "direct_country_ids",
    "admin_country_ids",
    "context_country_ids",
    "admin_entity_ids",
    "place_id",
]


def split_pipe_values(value) -> list[str]:
    if pd.isna(value):
        return []
    return [token.strip() for token in str(value).split("|") if token.strip()]


def normalize_qid(value) -> object:
    if pd.isna(value):
        return pd.NA
    token = str(value).strip().rstrip("/")
    if not token:
        return pd.NA
    return token.rsplit("/", 1)[-1]


def count_citizenship_usage(enriched_path: Path, crosswalk_ids: set[str]) -> dict[str, int]:
    if not enriched_path.exists():
        return {}

    df = pd.read_csv(enriched_path, usecols=["wikidata_id", "citizenship_ids"])
    entity_tokens = {}
    for row in df.drop_duplicates("wikidata_id").itertuples(index=False):
        entity_tokens[row.wikidata_id] = set(split_pipe_values(row.citizenship_ids)) & crosswalk_ids

    counts = {qid: 0 for qid in crosswalk_ids}
    for token_ids in entity_tokens.values():
        for token_id in token_ids:
            counts[token_id] += 1
    return counts


def count_place_usage(place_context_path: Path, crosswalk_ids: set[str]) -> dict[str, dict[str, int]]:
    counts = {
        qid: {
            "place_context_row_count": 0,
            "place_context_entity_count": 0,
            "birth_place_context_entity_count": 0,
        }
        for qid in crosswalk_ids
    }
    if not place_context_path.exists():
        return counts

    df = pd.read_csv(place_context_path)
    entity_hits = {qid: set() for qid in crosswalk_ids}
    birth_entity_hits = {qid: set() for qid in crosswalk_ids}

    for row in df.itertuples(index=False):
        row_tokens = set()
        for column in TOKEN_COLUMNS:
            if hasattr(row, column):
                values = split_pipe_values(getattr(row, column))
                if column == "place_id":
                    values = [normalize_qid(value) for value in values]
                row_tokens.update(str(value) for value in values if pd.notna(value))
        matched_ids = row_tokens & crosswalk_ids
        for token_id in matched_ids:
            counts[token_id]["place_context_row_count"] += 1
            entity_hits[token_id].add(row.wikidata_id)
            if row.place_role == "birth_place":
                birth_entity_hits[token_id].add(row.wikidata_id)

    for token_id in crosswalk_ids:
        counts[token_id]["place_context_entity_count"] = len(entity_hits[token_id])
        counts[token_id]["birth_place_context_entity_count"] = len(birth_entity_hits[token_id])

    return counts


def yes_no(value: bool) -> str:
    return "yes" if value else "no"


def build_matrix(project_root: Path, cohort_ids: list[str]) -> pd.DataFrame:
    crosswalk_df = load_political_crosswalk(project_root)
    if crosswalk_df.empty:
        raise SystemExit("Missing political entity crosswalk seed.")

    crosswalk_ids = set(crosswalk_df["wikidata_id"].astype(str))
    matrix_df = crosswalk_df.copy()

    notes = matrix_df["notes"].fillna("").astype(str).str.lower()
    matrix_df["manual_review_needs_attention"] = (
        (matrix_df["manual_review_label"].astype(str).str.lower() == "low")
        | notes.str.contains("verify|careful|edge|not equivalent|not automatically", regex=True)
    )
    matrix_df["review_field_has_affiliation_bucket"] = matrix_df["affiliation_bucket"].notna()
    matrix_df["review_field_has_context_type"] = matrix_df["context_type"].notna()
    matrix_df["review_field_has_modern_country_mapping"] = matrix_df["modern_country_id"].notna()
    matrix_df["review_field_has_geographic_rollup"] = (
        matrix_df["include_in_europe_binary"] | matrix_df["include_in_non_europe_or_colonial"]
    )
    matrix_df["review_field_has_imperial_decision"] = matrix_df["include_in_imperial_context"].notna()
    matrix_df["review_field_has_review_note"] = matrix_df["notes"].notna()

    matrix_df["review_punchcard_direct_state"] = matrix_df["context_type"].isin(
        ["modern_state", "historical_state", "imperial_state", "colonial_state", "dynasty"]
    ).map(yes_no)
    matrix_df["review_punchcard_imperial_context"] = matrix_df["include_in_imperial_context"].map(yes_no)
    matrix_df["review_punchcard_modern_rollup"] = matrix_df["include_in_modern_country_rollup"].map(yes_no)
    matrix_df["review_punchcard_europe_binary"] = matrix_df["include_in_europe_binary"].map(yes_no)
    matrix_df["review_punchcard_non_europe_or_colonial"] = matrix_df[
        "include_in_non_europe_or_colonial"
    ].map(yes_no)
    matrix_df["review_punchcard_review_note"] = matrix_df["notes"].notna().map(yes_no)
    matrix_df["review_punchcard_needs_attention"] = matrix_df["manual_review_needs_attention"].map(yes_no)

    for cohort_id in cohort_ids:
        paths = cohort_paths(project_root, cohort_id)
        citizenship_counts = count_citizenship_usage(paths.enriched_path, crosswalk_ids)
        place_counts = count_place_usage(paths.processed_dir / "place_context_long.csv", crosswalk_ids)

        matrix_df[f"{cohort_id}_citizenship_entity_count"] = matrix_df["wikidata_id"].map(citizenship_counts).fillna(0).astype(int)
        matrix_df[f"{cohort_id}_place_context_row_count"] = (
            matrix_df["wikidata_id"].map(lambda qid: place_counts.get(qid, {}).get("place_context_row_count", 0)).astype(int)
        )
        matrix_df[f"{cohort_id}_place_context_entity_count"] = (
            matrix_df["wikidata_id"].map(lambda qid: place_counts.get(qid, {}).get("place_context_entity_count", 0)).astype(int)
        )
        matrix_df[f"{cohort_id}_birth_place_context_entity_count"] = (
            matrix_df["wikidata_id"].map(lambda qid: place_counts.get(qid, {}).get("birth_place_context_entity_count", 0)).astype(int)
        )
        matrix_df[f"{cohort_id}_active_evidence"] = (
            (
                matrix_df[f"{cohort_id}_citizenship_entity_count"]
                + matrix_df[f"{cohort_id}_place_context_entity_count"]
            )
            > 0
        ).map(yes_no)

    active_columns = [f"{cohort_id}_active_evidence" for cohort_id in cohort_ids]
    matrix_df["active_in_any_current_cohort"] = matrix_df[active_columns].eq("yes").any(axis=1).map(yes_no)

    review_fields = [
        "review_field_has_affiliation_bucket",
        "review_field_has_context_type",
        "review_field_has_modern_country_mapping",
        "review_field_has_geographic_rollup",
        "review_field_has_imperial_decision",
        "review_field_has_review_note",
    ]
    matrix_df["review_tally_checked_count"] = matrix_df[review_fields].sum(axis=1).astype(int)
    matrix_df["review_tally_total_fields"] = len(review_fields)
    matrix_df["review_tally_score"] = (
        matrix_df["review_tally_checked_count"] / matrix_df["review_tally_total_fields"]
    ).round(4)
    matrix_df["review_basis_count"] = matrix_df[
        [
            "review_punchcard_direct_state",
            "review_punchcard_imperial_context",
            "review_punchcard_modern_rollup",
            "review_punchcard_europe_binary",
            "review_punchcard_non_europe_or_colonial",
            "review_punchcard_review_note",
            "active_in_any_current_cohort",
        ]
    ].eq("yes").sum(axis=1)

    return matrix_df.sort_values(["review_group", "review_tally_score", "wikidata_id"], ascending=[True, False, True])


def build_summary(matrix_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for review_label, rank in MANUAL_REVIEW_SORT_ORDER.items():
        group = matrix_df.loc[matrix_df["manual_review_label"].astype(str).str.lower() == review_label]
        rows.append(
            {
                "manual_review_label": review_label,
                "manual_review_rank": rank,
                "crosswalk_rows": len(group),
                "active_rows": int((group["active_in_any_current_cohort"] == "yes").sum()),
                "needs_attention_rows": int(group["manual_review_needs_attention"].sum()),
                "mean_review_tally_score": round(group["review_tally_score"].mean(), 4) if len(group) else pd.NA,
            }
        )
    return pd.DataFrame(rows)


def parse_args() -> object:
    parser = ArgumentParser(description="Build political-entity crosswalk review tables.")
    parser.add_argument(
        "--cohort-id",
        action="append",
        choices=COHORT_IDS,
        help="Cohort to include. Can be repeated. Default: both cohorts.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    project_root = Path(__file__).resolve().parents[2]
    cohort_ids = args.cohort_id or COHORT_IDS
    output_dir = project_root / "data" / "processed"
    output_dir.mkdir(parents=True, exist_ok=True)

    matrix_df = build_matrix(project_root, cohort_ids)
    summary_df = build_summary(matrix_df)

    matrix_path = output_dir / "political_entity_crosswalk_review_matrix.csv"
    summary_path = output_dir / "political_entity_crosswalk_review_summary.csv"
    matrix_df.to_csv(matrix_path, index=False)
    summary_df.to_csv(summary_path, index=False)

    print("Political-entity crosswalk review tables complete.")
    print(f"Matrix rows: {len(matrix_df)}")
    print(f"Matrix: {matrix_path}")
    print(f"Summary: {summary_path}")
    print(summary_df.to_string(index=False))


if __name__ == "__main__":
    main()
