"""
Build formula-backed cultural-affiliation evidence scores.

This matrix counts explicit evidence fields instead of assigning qualitative
confidence directly. For each person/candidate affiliation, the score is:

    supporting evidence fields / total evidence fields

It also reports support over available mapped evidence fields, so the project
can distinguish weak evidence from missing metadata.
"""

from argparse import ArgumentParser
from pathlib import Path
import importlib.util
import sys

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from cohorts import DEFAULT_COHORT_ID, cohort_paths  # noqa: E402
from common import (  # noqa: E402
    join_values,
    normalize_blank_strings,
    qid_from_uri,
    qid_uri,
    ratio,
    split_pipe_values,
)
from crosswalk import load_country_affiliation_mapping  # noqa: E402


TOKEN_COLUMNS = [
    "direct_country_ids",
    "admin_country_ids",
    "context_country_ids",
    "admin_entity_ids",
]

PERSON_EVIDENCE_FIELDS = [
    "citizenship",
    "native_language",
    "spoken_written_language",
    "writing_language",
]

PLACE_EVIDENCE_FIELDS = [
    "birth_place",
    "death_place",
    "residence",
    "work_location",
]

EVIDENCE_FIELDS = PERSON_EVIDENCE_FIELDS + PLACE_EVIDENCE_FIELDS


def load_representation_constants(project_root: Path) -> tuple[dict[str, str], dict[str, str]]:
    source_path = project_root / "scripts" / "analysis" / "01_build_representation_matrices.py"
    spec = importlib.util.spec_from_file_location("representation_matrices", source_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load mappings from {source_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    country_mapping = load_country_affiliation_mapping(project_root, dict(module.COUNTRY_ID_TO_AFFILIATION))
    language_mapping = dict(module.LANGUAGE_ID_TO_AFFILIATION)
    return country_mapping, language_mapping


def collect_mapped_affiliations(tokens: list[str], mapping: dict[str, str]) -> set[str]:
    return {mapping[token] for token in tokens if mapping.get(token)}


def collect_entity_rows(enriched_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for wikidata_id, group in enriched_df.groupby("wikidata_id", dropna=False):
        row = {
            "wikidata_id": wikidata_id,
            "person_id": qid_from_uri(wikidata_id),
            "name": group["name"].dropna().iloc[0] if group["name"].notna().any() else pd.NA,
        }
        for column in [
            "citizenship_ids",
            "native_language_ids",
            "spoken_written_language_ids",
            "writing_language_ids",
        ]:
            tokens = []
            if column in group.columns:
                for value in group[column]:
                    tokens.extend(split_pipe_values(value))
            row[column] = join_values(tokens)
        rows.append(row)
    return pd.DataFrame(rows)


def build_person_field_affiliations(
    entity_df: pd.DataFrame,
    country_mapping: dict[str, str],
    language_mapping: dict[str, str],
) -> dict[str, dict[str, set[str]]]:
    field_affiliations = {
        row.wikidata_id: {field: set() for field in EVIDENCE_FIELDS}
        for row in entity_df.itertuples(index=False)
    }
    for row in entity_df.itertuples(index=False):
        field_affiliations[row.wikidata_id]["citizenship"] = collect_mapped_affiliations(
            split_pipe_values(row.citizenship_ids),
            country_mapping,
        )
        field_affiliations[row.wikidata_id]["native_language"] = collect_mapped_affiliations(
            split_pipe_values(row.native_language_ids),
            language_mapping,
        )
        field_affiliations[row.wikidata_id]["spoken_written_language"] = collect_mapped_affiliations(
            split_pipe_values(row.spoken_written_language_ids),
            language_mapping,
        )
        field_affiliations[row.wikidata_id]["writing_language"] = collect_mapped_affiliations(
            split_pipe_values(row.writing_language_ids),
            language_mapping,
        )
    return field_affiliations


def place_row_affiliations(row, country_mapping: dict[str, str]) -> set[str]:
    direct_country_ids = split_pipe_values(row.direct_country_ids)
    direct_affiliations = collect_mapped_affiliations(direct_country_ids, country_mapping)

    tokens = []
    tokens.extend(direct_country_ids)
    tokens.append(row.place_id)
    tokens.extend(split_pipe_values(row.admin_entity_ids))
    if not direct_affiliations:
        tokens.extend(split_pipe_values(row.admin_country_ids))
        tokens.extend(split_pipe_values(row.context_country_ids))

    return collect_mapped_affiliations(tokens, country_mapping)


def append_place_field_affiliations(
    field_affiliations: dict[str, dict[str, set[str]]],
    place_df: pd.DataFrame,
    country_mapping: dict[str, str],
) -> None:
    for row in place_df.itertuples(index=False):
        if row.wikidata_id not in field_affiliations or row.place_role not in PLACE_EVIDENCE_FIELDS:
            continue
        field_affiliations[row.wikidata_id][row.place_role].update(
            place_row_affiliations(row, country_mapping)
        )


def build_evidence_tables(
    entity_df: pd.DataFrame,
    field_affiliations: dict[str, dict[str, set[str]]],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    matrix_rows = []
    best_rows = []

    total_fields = len(EVIDENCE_FIELDS)
    for entity in entity_df.itertuples(index=False):
        fields = field_affiliations.get(entity.wikidata_id, {})
        available_fields = [field for field in EVIDENCE_FIELDS if fields.get(field)]
        candidates = sorted({candidate for values in fields.values() for candidate in values})

        person_rows = []
        for candidate in candidates:
            support_fields = [field for field in EVIDENCE_FIELDS if candidate in fields.get(field, set())]
            row = {
                "wikidata_id": entity.wikidata_id,
                "person_id": entity.person_id,
                "name": entity.name,
                "candidate_affiliation": candidate,
                "supporting_evidence_count": len(support_fields),
                "available_mapped_evidence_count": len(available_fields),
                "total_evidence_fields": total_fields,
                "score_over_total_fields": ratio(len(support_fields), total_fields),
                "score_over_available_fields": ratio(len(support_fields), len(available_fields)),
                "supporting_evidence_fields": join_values(support_fields),
                "available_mapped_evidence_fields": join_values(available_fields),
                "score_formula": f"{len(support_fields)} / {total_fields}",
                "available_score_formula": f"{len(support_fields)} / {len(available_fields)}"
                if available_fields else pd.NA,
            }
            for field in EVIDENCE_FIELDS:
                row[f"{field}_available"] = bool(fields.get(field))
                row[f"{field}_support"] = candidate in fields.get(field, set())
                row[f"{field}_candidate_affiliations"] = join_values(sorted(fields.get(field, set())))
            person_rows.append(row)

        matrix_rows.extend(person_rows)
        if person_rows:
            sorted_rows = sorted(
                person_rows,
                key=lambda item: (
                    -item["supporting_evidence_count"],
                    -(item["score_over_total_fields"] if pd.notna(item["score_over_total_fields"]) else -1),
                    item["candidate_affiliation"],
                ),
            )
            top_score = sorted_rows[0]["supporting_evidence_count"]
            top_rows = [row for row in sorted_rows if row["supporting_evidence_count"] == top_score]
            best_candidate = top_rows[0]["candidate_affiliation"] if len(top_rows) == 1 else pd.NA
            top_candidates = [row["candidate_affiliation"] for row in top_rows]
            second_score = sorted_rows[len(top_rows)]["supporting_evidence_count"] if len(sorted_rows) > len(top_rows) else 0
            best_rows.append(
                {
                    "wikidata_id": entity.wikidata_id,
                    "person_id": entity.person_id,
                    "name": entity.name,
                    "candidate_affiliation_count": len(person_rows),
                    "best_candidate_affiliation": best_candidate,
                    "top_candidate_affiliations": join_values(top_candidates),
                    "top_supporting_evidence_count": top_score,
                    "second_supporting_evidence_count": second_score,
                    "top_candidate_tie_count": len(top_candidates),
                    "available_mapped_evidence_count": len(available_fields),
                    "total_evidence_fields": total_fields,
                    "top_score_over_total_fields": ratio(top_score, total_fields),
                    "top_score_over_available_fields": ratio(top_score, len(available_fields)),
                    "score_formula": f"{top_score} / {total_fields}",
                    "available_score_formula": f"{top_score} / {len(available_fields)}"
                    if available_fields else pd.NA,
                }
            )
        else:
            best_rows.append(
                {
                    "wikidata_id": entity.wikidata_id,
                    "person_id": entity.person_id,
                    "name": entity.name,
                    "candidate_affiliation_count": 0,
                    "best_candidate_affiliation": pd.NA,
                    "top_candidate_affiliations": pd.NA,
                    "top_supporting_evidence_count": 0,
                    "second_supporting_evidence_count": 0,
                    "top_candidate_tie_count": 0,
                    "available_mapped_evidence_count": 0,
                    "total_evidence_fields": total_fields,
                    "top_score_over_total_fields": 0.0,
                    "top_score_over_available_fields": pd.NA,
                    "score_formula": f"0 / {total_fields}",
                    "available_score_formula": pd.NA,
                }
            )

    matrix_df = pd.DataFrame(matrix_rows)
    best_df = pd.DataFrame(best_rows)
    summary_df = build_summary(best_df)
    return matrix_df, best_df, summary_df


def build_summary(best_df: pd.DataFrame) -> pd.DataFrame:
    total_entities = len(best_df)
    rows = [
        {"metric": "total_entities", "value": total_entities, "pct": 100.0},
        {
            "metric": "entities_with_any_mapped_affiliation_candidate",
            "value": int((best_df["candidate_affiliation_count"] > 0).sum()),
            "pct": round((best_df["candidate_affiliation_count"] > 0).mean() * 100, 2) if total_entities else 0.0,
        },
        {
            "metric": "entities_with_unique_top_candidate",
            "value": int(best_df["best_candidate_affiliation"].notna().sum()),
            "pct": round(best_df["best_candidate_affiliation"].notna().mean() * 100, 2) if total_entities else 0.0,
        },
        {
            "metric": "entities_with_tied_top_candidate",
            "value": int((best_df["top_candidate_tie_count"] > 1).sum()),
            "pct": round((best_df["top_candidate_tie_count"] > 1).mean() * 100, 2) if total_entities else 0.0,
        },
    ]
    return pd.DataFrame(rows)


def parse_args() -> object:
    parser = ArgumentParser(description="Build formula-backed affiliation evidence matrix.")
    parser.add_argument(
        "--cohort-id",
        default=DEFAULT_COHORT_ID,
        choices=["french_seed", "global_writers"],
        help=f"Cohort to analyze. Default: {DEFAULT_COHORT_ID}.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    project_root = Path(__file__).resolve().parents[2]
    paths = cohort_paths(project_root, args.cohort_id)
    enriched_path = paths.enriched_path
    place_path = paths.processed_dir / "place_context_long.csv"
    output_dir = paths.processed_dir

    if not enriched_path.exists():
        raise SystemExit(
            f"Missing enriched cohort. Run python scripts/pipeline/04_merge_wikidata_enrichment.py --cohort-id {paths.cohort_id}"
        )
    if not place_path.exists():
        raise SystemExit(
            f"Missing place context. Run python scripts/analysis/02_build_place_affiliation_context.py --cohort-id {paths.cohort_id}"
        )

    country_mapping, language_mapping = load_representation_constants(project_root)
    enriched_df = normalize_blank_strings(pd.read_csv(enriched_path))
    place_df = normalize_blank_strings(pd.read_csv(place_path))

    entity_df = collect_entity_rows(enriched_df)
    field_affiliations = build_person_field_affiliations(entity_df, country_mapping, language_mapping)
    append_place_field_affiliations(field_affiliations, place_df, country_mapping)
    matrix_df, best_df, summary_df = build_evidence_tables(entity_df, field_affiliations)

    outputs = [
        (output_dir / "cultural_affiliation_evidence_matrix.csv", matrix_df),
        (output_dir / "cultural_affiliation_evidence_best.csv", best_df),
        (output_dir / "cultural_affiliation_evidence_summary.csv", summary_df),
    ]
    output_dir.mkdir(parents=True, exist_ok=True)
    for output_path, output_df in outputs:
        output_df.to_csv(output_path, index=False)

    print("Formula-backed affiliation evidence matrix complete.")
    print(f"Cohort: {paths.cohort_id}")
    print(f"Entities: {len(entity_df)}")
    print(f"Candidate rows: {len(matrix_df)}")
    print(f"Evidence fields: {', '.join(EVIDENCE_FIELDS)}")
    print(summary_df.to_string(index=False))


if __name__ == "__main__":
    main()
