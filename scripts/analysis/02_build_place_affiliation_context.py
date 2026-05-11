"""
Build place-derived affiliation context from Wikidata person-place evidence.

This layer keeps geography separate from the core language/citizenship
affiliation score. It maps birth, death, residence, and work-location places
to provisional affiliations where country or historical-state context is
available, while preserving unmapped tokens for review.
"""

from pathlib import Path
from argparse import ArgumentParser
import importlib.util
import sys
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from cohorts import DEFAULT_COHORT_ID, cohort_paths  # noqa: E402
from common import (  # noqa: E402
    join_example_values,
    join_values as join_pipe_values,
    normalize_blank_strings,
    percentage,
    qid_uri,
    qid_url,
    ratio,
    split_pipe_values,
)
from crosswalk import load_country_affiliation_mapping  # noqa: E402


PLACE_ROLES = ["birth_place", "death_place", "residence", "work_location"]


def load_country_mapping(project_root: Path) -> dict[str, str]:
    source_path = project_root / "scripts" / "analysis" / "01_build_representation_matrices.py"
    spec = importlib.util.spec_from_file_location("representation_matrices", source_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load affiliation mappings from {source_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return load_country_affiliation_mapping(project_root, dict(module.COUNTRY_ID_TO_AFFILIATION))


def join_values(values) -> object:
    return join_pipe_values(values, sort=True)


def classify_place_review_status(top_score: int, second_score: int, tie_count: int) -> str:
    if top_score == 0:
        return "no_mapped_place_affiliation"
    if tie_count > 1:
        return "mixed_place_top_score"
    if top_score >= 3 and (top_score - second_score) >= 2:
        return "strong_place"
    if top_score >= 3 and (top_score - second_score) >= 1:
        return "moderate_place"
    if top_score >= 2:
        return "tentative_place"
    return "single_place_signal"


def load_name_lookup(entity_path: Path) -> dict[str, str]:
    if not entity_path.exists():
        return {}

    entity_df = pd.read_csv(entity_path, usecols=["wikidata_id", "name"])
    entity_df["person_id"] = entity_df["wikidata_id"].astype(str).str.extract(r"(Q\d+)$", expand=False)
    return dict(zip(entity_df["person_id"], entity_df["name"], strict=False))


def build_context_long(raw_df: pd.DataFrame, name_lookup: dict[str, str]) -> pd.DataFrame:
    context_df = raw_df.copy()
    context_df["wikidata_id"] = context_df["person_id"].apply(qid_uri)
    context_df["name"] = context_df["person_id"].map(name_lookup).fillna(context_df["person_label"])
    sort_columns = ["name", "person_id", "place_role", "place_label", "place_id"]
    front_columns = ["wikidata_id", "person_id", "name", "place_role", "place_id", "place_label"]
    other_columns = [column for column in context_df.columns if column not in front_columns]
    return context_df[front_columns + other_columns].sort_values(sort_columns)


def mapped_affiliations_for_row(row, country_mapping: dict[str, str]) -> tuple[dict[str, set[str]], list[dict]]:
    mapped_by_affiliation = {}
    unmapped_rows = []

    direct_country_ids = split_pipe_values(row.direct_country_ids)
    direct_country_labels = split_pipe_values(row.direct_country_labels)
    admin_country_ids = split_pipe_values(row.admin_country_ids)
    admin_country_labels = split_pipe_values(row.admin_country_labels)
    direct_mapped = {country_mapping[token_id] for token_id in direct_country_ids if country_mapping.get(token_id)}

    token_specs = [
        ("direct_country", direct_country_ids, direct_country_labels),
        ("place_entity", [row.place_id], [row.place_label]),
        ("admin_entity", split_pipe_values(row.admin_entity_ids), split_pipe_values(row.admin_entity_labels)),
    ]
    if not direct_mapped:
        token_specs.append(("admin_country", admin_country_ids, admin_country_labels))

    for token_family, token_ids, token_labels in token_specs:
        label_lookup = dict(zip(token_ids, token_labels, strict=False))
        for token_id in token_ids:
            affiliation = country_mapping.get(token_id)
            if affiliation:
                mapped_by_affiliation.setdefault(affiliation, set()).add(token_id)
            elif token_family == "direct_country" and not direct_mapped:
                unmapped_rows.append(
                    {
                        "token_family": token_family,
                        "token_id": token_id,
                        "token_label": label_lookup.get(token_id, token_id),
                        "token_wikidata_url": qid_url(token_id),
                        "wikidata_id": row.wikidata_id,
                        "name": row.name,
                        "place_role": row.place_role,
                        "place_id": row.place_id,
                        "place_label": row.place_label,
                    }
                )
            elif token_family == "admin_country":
                unmapped_rows.append(
                    {
                        "token_family": token_family,
                        "token_id": token_id,
                        "token_label": label_lookup.get(token_id, token_id),
                        "token_wikidata_url": qid_url(token_id),
                        "wikidata_id": row.wikidata_id,
                        "name": row.name,
                        "place_role": row.place_role,
                        "place_id": row.place_id,
                        "place_label": row.place_label,
                    }
                )

    return mapped_by_affiliation, unmapped_rows


def build_place_affiliation_tables(
    context_df: pd.DataFrame,
    country_mapping: dict[str, str],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    role_support = {}
    unmapped_rows = []

    for row in context_df.itertuples(index=False):
        mapped_by_affiliation, row_unmapped = mapped_affiliations_for_row(row, country_mapping)
        unmapped_rows.extend(row_unmapped)

        entity_key = (row.wikidata_id, row.name)
        entity_support = role_support.setdefault(
            entity_key,
            {
                "roles_with_place_evidence": set(),
                "roles_with_mapped_affiliation": set(),
                "candidate_roles": {},
                "candidate_places": {},
                "candidate_tokens": {},
            },
        )
        entity_support["roles_with_place_evidence"].add(row.place_role)

        if mapped_by_affiliation:
            entity_support["roles_with_mapped_affiliation"].add(row.place_role)

        for affiliation, token_ids in mapped_by_affiliation.items():
            entity_support["candidate_roles"].setdefault(affiliation, set()).add(row.place_role)
            entity_support["candidate_places"].setdefault(affiliation, set()).add(
                f"{row.place_label} ({row.place_id})"
            )
            entity_support["candidate_tokens"].setdefault(affiliation, set()).update(token_ids)

    candidate_rows = []
    best_rows = []

    for (wikidata_id, name), support in sorted(role_support.items(), key=lambda item: (item[0][1], item[0][0])):
        possible_roles = len(support["roles_with_place_evidence"])
        mapped_roles = len(support["roles_with_mapped_affiliation"])
        candidate_affiliations = sorted(support["candidate_roles"])

        person_candidate_rows = []
        for affiliation in candidate_affiliations:
            roles = support["candidate_roles"][affiliation]
            candidate_row = {
                "wikidata_id": wikidata_id,
                "name": name,
                "candidate_affiliation": affiliation,
                "possible_place_role_count": possible_roles,
                "mapped_place_role_count": mapped_roles,
                "checked_place_role_count": len(roles),
                "place_score_share": ratio(len(roles), possible_roles),
                "supporting_place_roles": join_values(list(roles)),
                "supporting_places": join_values(list(support["candidate_places"][affiliation])),
                "support_token_ids": join_values(list(support["candidate_tokens"][affiliation])),
            }
            for place_role in PLACE_ROLES:
                candidate_row[f"{place_role}_support"] = place_role in roles
            person_candidate_rows.append(candidate_row)

        candidate_rows.extend(person_candidate_rows)

        if person_candidate_rows:
            sorted_candidates = sorted(
                person_candidate_rows,
                key=lambda item: (
                    -item["checked_place_role_count"],
                    -(item["place_score_share"] if pd.notna(item["place_score_share"]) else -1),
                    item["candidate_affiliation"],
                ),
            )
            top_score = int(sorted_candidates[0]["checked_place_role_count"])
            second_score = int(sorted_candidates[1]["checked_place_role_count"]) if len(sorted_candidates) > 1 else 0
            top_candidates = [
                item["candidate_affiliation"]
                for item in sorted_candidates
                if item["checked_place_role_count"] == top_score
            ]
            best_candidate = top_candidates[0] if len(top_candidates) == 1 else pd.NA
            review_status = classify_place_review_status(top_score, second_score, len(top_candidates))
            top_score_share = ratio(top_score, possible_roles)
        else:
            top_score = 0
            second_score = 0
            top_candidates = []
            best_candidate = pd.NA
            review_status = "no_mapped_place_affiliation"
            top_score_share = pd.NA

        best_rows.append(
            {
                "wikidata_id": wikidata_id,
                "name": name,
                "place_evidence_role_count": possible_roles,
                "mapped_place_role_count": mapped_roles,
                "place_candidate_affiliation_count": len(person_candidate_rows),
                "best_place_candidate_affiliation": best_candidate,
                "top_place_candidate_affiliations": join_values(top_candidates),
                "top_place_checked_role_count": top_score,
                "second_place_checked_role_count": second_score,
                "top_place_score_share": top_score_share,
                "top_place_candidate_tie_count": len(top_candidates),
                "place_affiliation_review_status": review_status,
            }
        )

    candidate_df = pd.DataFrame(candidate_rows)
    best_df = pd.DataFrame(best_rows)

    unmapped_df = pd.DataFrame(unmapped_rows)
    if not unmapped_df.empty:
        unmapped_df = (
            unmapped_df.groupby(["token_family", "token_id"], dropna=False)
            .agg(
                token_label=("token_label", "first"),
                token_wikidata_url=("token_wikidata_url", "first"),
                occurrence_count=("token_id", "size"),
                entity_count=("wikidata_id", "nunique"),
                place_count=("place_id", "nunique"),
                example_places=("place_label", join_example_values),
                example_names=("name", join_example_values),
            )
            .reset_index()
            .sort_values(["occurrence_count", "token_family", "token_id"], ascending=[False, True, True])
        )

    role_summary_rows = []
    total_entities = context_df["wikidata_id"].nunique()
    mapped_by_role = set()
    for row in candidate_rows:
        for place_role in PLACE_ROLES:
            if row[f"{place_role}_support"]:
                mapped_by_role.add((row["wikidata_id"], place_role))

    for place_role, group in context_df.groupby("place_role"):
        entities_with_role = group["wikidata_id"].nunique()
        entities_with_mapped_role = len({entity_id for entity_id, role in mapped_by_role if role == place_role})
        role_summary_rows.append(
            {
                "place_role": place_role,
                "place_rows": len(group),
                "entities_with_role": entities_with_role,
                "entities_with_role_pct": percentage(entities_with_role, total_entities),
                "entities_with_mapped_affiliation": entities_with_mapped_role,
                "entities_with_mapped_affiliation_pct": percentage(entities_with_mapped_role, total_entities),
                "unique_places": group["place_id"].nunique(),
            }
        )
    role_summary_df = pd.DataFrame(role_summary_rows).sort_values("entities_with_role", ascending=False)

    return candidate_df, best_df, unmapped_df, role_summary_df


def parse_args() -> object:
    parser = ArgumentParser(description="Build place-derived affiliation context for a cohort.")
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
    input_path = paths.raw_person_place_context_path
    output_dir = paths.processed_dir

    if not input_path.exists():
        raise SystemExit(
            "Missing place-context raw export. Run:\n"
            f"python scripts/queries/18_fetch_wikidata_person_place_context.py --cohort-id {paths.cohort_id}"
        )

    raw_df = normalize_blank_strings(pd.read_csv(input_path))
    name_lookup = load_name_lookup(output_dir / "representation_entities.csv")
    country_mapping = load_country_mapping(project_root)
    context_df = build_context_long(raw_df, name_lookup)
    candidate_df, best_df, unmapped_df, role_summary_df = build_place_affiliation_tables(
        context_df,
        country_mapping,
    )

    output_paths = [
        (output_dir / "place_context_long.csv", context_df),
        (output_dir / "place_affiliation_candidates_long.csv", candidate_df),
        (output_dir / "place_affiliation_best_candidates.csv", best_df),
        (output_dir / "place_affiliation_unmapped_tokens.csv", unmapped_df),
        (output_dir / "place_affiliation_role_summary.csv", role_summary_df),
    ]

    output_dir.mkdir(parents=True, exist_ok=True)
    for output_path, output_df in output_paths:
        output_df.to_csv(output_path, index=False)

    print("Place affiliation context complete.")
    print(f"Cohort: {paths.cohort_id}")
    print(f"Input rows: {len(raw_df)}")
    print(f"Entity rows with place evidence: {context_df['wikidata_id'].nunique()}")
    print(f"Place-affiliation candidate rows: {len(candidate_df)}")
    print(f"Outputs written: {len(output_paths)}")
    print(role_summary_df.to_string(index=False))


if __name__ == "__main__":
    main()
