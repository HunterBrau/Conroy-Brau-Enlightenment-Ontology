"""
Build the core findings packet for the current scoped project.

The packet is intentionally narrow: it summarizes the reproducible
`global_writers` cohort, the reviewed context slices, representation across
Wikipedia language editions, gender, occupation buckets, and data-friction
metrics. It does not introduce new sources.
"""

from pathlib import Path
import sys

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from cohorts import DEFAULT_COHORT_ID, cohort_paths  # noqa: E402
from common import WIKI_COLUMNS, bool_value, percentage  # noqa: E402


OUTPUT_FILENAMES = {
    "key_metrics": "core_findings_key_metrics.csv",
    "context_slices": "core_findings_context_slices.csv",
    "language_by_slice": "core_findings_language_by_slice.csv",
    "gender_by_slice": "core_findings_gender_by_slice.csv",
    "occupation_by_slice": "core_findings_occupation_buckets_by_slice.csv",
    "data_friction": "core_findings_data_friction.csv",
}


def read_processed(paths) -> dict[str, pd.DataFrame]:
    processed_dir = paths.processed_dir
    return {
        "entities": pd.read_csv(processed_dir / "representation_entities.csv"),
        "context_membership": pd.read_csv(processed_dir / "context_slice_membership.csv"),
        "context_summary": pd.read_csv(processed_dir / "context_slice_summary.csv"),
        "geographic_summary": pd.read_csv(processed_dir / "geographic_scope_summary.csv"),
        "affiliation_summary": pd.read_csv(processed_dir / "cultural_affiliation_evidence_summary.csv"),
        "affiliation_best": pd.read_csv(processed_dir / "cultural_affiliation_evidence_best.csv"),
        "language_summary": pd.read_csv(processed_dir / "representation_language_summary.csv"),
        "label_coverage": pd.read_csv(processed_dir / "wikidata_label_coverage_by_language.csv"),
        "occupation_summary": pd.read_csv(processed_dir / "occupation_bucket_summary.csv"),
        "occupation_long": pd.read_csv(processed_dir / "occupation_bucket_entities_long.csv"),
    }


def metric_value(df: pd.DataFrame, metric: str, value_column: str = "value") -> object:
    match = df.loc[df["metric"] == metric]
    if match.empty:
        return pd.NA
    return match[value_column].iloc[0]


def make_key_metrics(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    entities = tables["entities"]
    geographic = tables["geographic_summary"]
    affiliation = tables["affiliation_summary"]
    context = tables["context_summary"]
    language = tables["language_summary"]
    occupation = tables["occupation_summary"]

    total_entities = len(entities)
    rows = [
        {
            "section": "cohort",
            "metric": "global_writer_entities",
            "value": total_entities,
            "pct": 100.0,
            "notes": "Distinct Wikidata QIDs in the active 1675-1775 writer/subclass cohort.",
        },
        {
            "section": "data_friction",
            "metric": "missing_country_of_citizenship",
            "value": metric_value(geographic, "country_of_citizenship_missing", "entity_count"),
            "pct": metric_value(geographic, "country_of_citizenship_missing", "pct"),
            "notes": "Entities without Wikidata P27 in the enrichment export.",
        },
        {
            "section": "data_friction",
            "metric": "any_mapped_affiliation_candidate",
            "value": metric_value(affiliation, "entities_with_any_mapped_affiliation_candidate"),
            "pct": metric_value(affiliation, "entities_with_any_mapped_affiliation_candidate", "pct"),
            "notes": "Entities with at least one mapped affiliation candidate across the evidence matrix.",
        },
        {
            "section": "data_friction",
            "metric": "unique_top_affiliation_candidate",
            "value": metric_value(affiliation, "entities_with_unique_top_candidate"),
            "pct": metric_value(affiliation, "entities_with_unique_top_candidate", "pct"),
            "notes": "Entities whose formula-backed evidence has one top affiliation candidate.",
        },
    ]

    for row in context.itertuples(index=False):
        rows.append(
            {
                "section": "context_slice",
                "metric": f"{row.slice_id}_any_citizenship_or_place_evidence",
                "value": row.entities_with_any_slice_evidence,
                "pct": row.entities_with_any_slice_evidence_pct,
                "notes": f"{row.review_group} slice membership from citizenship or place context.",
            }
        )

    top_languages = language.sort_values("represented_entities", ascending=False).head(5)
    for row in top_languages.itertuples(index=False):
        rows.append(
            {
                "section": "language_representation",
                "metric": f"{row.language_edition}_represented_entities",
                "value": row.represented_entities,
                "pct": row.representation_pct,
                "notes": "Wikipedia language edition article coverage for the global cohort.",
            }
        )

    top_buckets = occupation.sort_values("entity_count", ascending=False).head(8)
    for row in top_buckets.itertuples(index=False):
        rows.append(
            {
                "section": "occupation_bucket",
                "metric": row.granular_bucket,
                "value": row.entity_count,
                "pct": row.entity_pct,
                "notes": row.bucket_family,
            }
        )

    return pd.DataFrame(rows)


def make_context_slice_table(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    context = tables["context_summary"].copy()
    context["citizenship_coverage_within_slice_pct"] = context.apply(
        lambda row: percentage(row["entities_with_citizenship_evidence"], row["entities_with_any_slice_evidence"]),
        axis=1,
    )
    context["place_context_coverage_within_slice_pct"] = context.apply(
        lambda row: percentage(row["entities_with_place_context_evidence"], row["entities_with_any_slice_evidence"]),
        axis=1,
    )
    context["place_only_within_slice_pct"] = context.apply(
        lambda row: percentage(row["entities_with_place_context_only"], row["entities_with_any_slice_evidence"]),
        axis=1,
    )
    context["citizenship_only_within_slice_pct"] = context.apply(
        lambda row: percentage(row["entities_with_citizenship_only"], row["entities_with_any_slice_evidence"]),
        axis=1,
    )
    return context.sort_values("entities_with_any_slice_evidence", ascending=False)


def make_language_by_slice(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    entities = tables["entities"].copy()
    membership = tables["context_membership"].copy()
    language_summary = tables["language_summary"].set_index("language_edition")
    entity_lookup = entities.set_index("wikidata_id")
    rows = []

    for slice_id, group in membership.groupby("slice_id", sort=True):
        slice_entities = group["wikidata_id"].drop_duplicates()
        slice_df = entity_lookup.loc[entity_lookup.index.intersection(slice_entities)]
        for language_edition, column in WIKI_COLUMNS.items():
            represented = int(slice_df[column].apply(bool_value).sum()) if column in slice_df else 0
            slice_total = len(slice_df)
            global_pct = (
                language_summary.loc[language_edition, "representation_pct"]
                if language_edition in language_summary.index
                else pd.NA
            )
            slice_pct = percentage(represented, slice_total)
            rows.append(
                {
                    "slice_id": slice_id,
                    "review_group": group["review_group"].iloc[0],
                    "language_edition": language_edition,
                    "slice_entities": slice_total,
                    "represented_entities": represented,
                    "slice_representation_pct": slice_pct,
                    "global_representation_pct": global_pct,
                    "slice_to_global_representation_index": round(slice_pct / global_pct, 4)
                    if pd.notna(global_pct) and global_pct
                    else pd.NA,
                }
            )

    return pd.DataFrame(rows).sort_values(["slice_id", "represented_entities"], ascending=[True, False])


def make_gender_by_slice(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    entities = tables["entities"].copy()
    membership = tables["context_membership"].copy()
    entity_lookup = entities.set_index("wikidata_id")
    global_counts = entities["gender_category"].fillna("unknown").value_counts().to_dict()
    total_entities = len(entities)
    rows = []

    for slice_id, group in membership.groupby("slice_id", sort=True):
        slice_entities = group["wikidata_id"].drop_duplicates()
        slice_df = entity_lookup.loc[entity_lookup.index.intersection(slice_entities)]
        slice_total = len(slice_df)
        slice_counts = slice_df["gender_category"].fillna("unknown").value_counts().to_dict()
        for gender_category in sorted(set(global_counts) | set(slice_counts)):
            rows.append(
                {
                    "slice_id": slice_id,
                    "review_group": group["review_group"].iloc[0],
                    "gender_category": gender_category,
                    "slice_entities": slice_counts.get(gender_category, 0),
                    "slice_total_entities": slice_total,
                    "slice_pct": percentage(slice_counts.get(gender_category, 0), slice_total),
                    "global_entities": global_counts.get(gender_category, 0),
                    "global_pct": percentage(global_counts.get(gender_category, 0), total_entities),
                }
            )

    return pd.DataFrame(rows).sort_values(["slice_id", "slice_entities"], ascending=[True, False])


def make_occupation_by_slice(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    membership = tables["context_membership"].copy()
    occupation_long = tables["occupation_long"].copy()
    rows = []

    for slice_id, group in membership.groupby("slice_id", sort=True):
        slice_ids = set(group["wikidata_id"])
        slice_total = len(slice_ids)
        slice_occupations = occupation_long.loc[occupation_long["wikidata_id"].isin(slice_ids)]
        bucket_counts = (
            slice_occupations.groupby(["granular_bucket", "bucket_family"])["wikidata_id"]
            .nunique()
            .reset_index(name="slice_entities")
        )
        bucket_counts["slice_id"] = slice_id
        bucket_counts["review_group"] = group["review_group"].iloc[0]
        bucket_counts["slice_total_entities"] = slice_total
        bucket_counts["slice_pct"] = bucket_counts["slice_entities"].apply(lambda count: percentage(count, slice_total))
        rows.append(bucket_counts)

    if not rows:
        return pd.DataFrame()

    output = pd.concat(rows, ignore_index=True)
    return output[
        [
            "slice_id",
            "review_group",
            "granular_bucket",
            "bucket_family",
            "slice_entities",
            "slice_total_entities",
            "slice_pct",
        ]
    ].sort_values(["slice_id", "slice_entities"], ascending=[True, False])


def make_data_friction(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    entities = tables["entities"].copy()
    membership = tables["context_membership"].copy()
    affiliation_best = tables["affiliation_best"].copy()
    affiliation_lookup = affiliation_best.set_index("wikidata_id")
    entity_lookup = entities.set_index("wikidata_id")

    def friction_rows(label: str, group_name: str, wikidata_ids: list[str]) -> list[dict]:
        subset = entity_lookup.loc[entity_lookup.index.intersection(wikidata_ids)].copy()
        affiliation_subset = affiliation_lookup.loc[affiliation_lookup.index.intersection(wikidata_ids)].copy()
        total = len(subset)
        metrics = [
            (
                "missing_country_of_citizenship",
                int(subset["citizenship_ids"].isna().sum()),
                "No Wikidata P27 value in the enrichment export.",
            ),
            (
                "no_place_context",
                int(subset["place_evidence_role_count"].isna().sum()),
                "No birth/death/residence/work-location context rows recovered.",
            ),
            (
                "no_wikipedia_article_in_tracked_editions",
                int((subset["wikipedia_sitelink_count"].fillna(0).astype(int) == 0).sum()),
                "No Wikipedia sitelink in the tracked language-edition matrix.",
            ),
            (
                "unresolved_name_label",
                int(subset["name_is_qid"].apply(bool_value).sum()),
                "Name still appears as a raw Wikidata QID.",
            ),
            (
                "no_mapped_affiliation_candidate",
                int((affiliation_subset["candidate_affiliation_count"].fillna(0).astype(int) == 0).sum()),
                "No mapped candidate in the formula-backed affiliation evidence table.",
            ),
            (
                "top_affiliation_single_field_or_less",
                int((affiliation_subset["top_supporting_evidence_count"].fillna(0).astype(int) <= 1).sum()),
                "Top affiliation candidate is supported by zero or one evidence field.",
            ),
        ]
        return [
            {
                "scope_id": label,
                "scope_label": group_name,
                "metric": metric,
                "entity_count": count,
                "denominator": total,
                "pct": percentage(count, total),
                "notes": notes,
            }
            for metric, count, notes in metrics
        ]

    rows = friction_rows("global_writers", "Global writers", entities["wikidata_id"].drop_duplicates().tolist())
    for slice_id, group in membership.groupby("slice_id", sort=True):
        rows.extend(friction_rows(slice_id, group["review_group"].iloc[0], group["wikidata_id"].drop_duplicates().tolist()))

    return pd.DataFrame(rows)


def fmt_count(value) -> str:
    if pd.isna(value):
        return "n/a"
    return f"{int(value):,}"


def fmt_pct(value) -> str:
    if pd.isna(value):
        return "n/a"
    return f"{float(value):.2f}%"


def markdown_table(df: pd.DataFrame, columns: list[str], limit: int | None = None) -> str:
    display_df = df.loc[:, columns].head(limit).copy() if limit else df.loc[:, columns].copy()
    header = "| " + " | ".join(columns) + " |"
    divider = "| " + " | ".join("---" for _ in columns) + " |"
    rows = []
    for row in display_df.itertuples(index=False):
        values = []
        for value in row:
            if pd.isna(value):
                values.append("")
            elif isinstance(value, float):
                values.append(f"{value:.2f}".rstrip("0").rstrip("."))
            else:
                values.append(str(value))
        rows.append("| " + " | ".join(values) + " |")
    return "\n".join([header, divider, *rows])


def build_markdown_report(tables: dict[str, pd.DataFrame], outputs: dict[str, Path]) -> str:
    key = pd.read_csv(outputs["key_metrics"])
    context = pd.read_csv(outputs["context_slices"])
    language = pd.read_csv(outputs["language_by_slice"])
    gender = pd.read_csv(outputs["gender_by_slice"])
    occupation = pd.read_csv(outputs["occupation_by_slice"])
    friction = pd.read_csv(outputs["data_friction"])
    label_coverage = tables["label_coverage"].sort_values("wikipedia_article_entities", ascending=False)

    total = key.loc[key["metric"] == "global_writer_entities", "value"].iloc[0]
    missing_cit = key.loc[key["metric"] == "missing_country_of_citizenship"].iloc[0]
    top_languages = tables["language_summary"].sort_values("represented_entities", ascending=False).head(5)
    top_occupations = tables["occupation_summary"].sort_values("entity_count", ascending=False).head(8)

    context_brief = context[
        [
            "review_group",
            "entities_with_any_slice_evidence",
            "entities_with_any_slice_evidence_pct",
            "entities_with_citizenship_evidence",
            "entities_with_place_context_evidence",
            "entities_with_place_context_only",
        ]
    ].copy()
    context_brief.columns = [
        "slice",
        "any evidence",
        "share of global",
        "citizenship",
        "place context",
        "place-only",
    ]

    global_friction = friction.loc[friction["scope_id"] == "global_writers"]

    return f"""# Core Findings Packet

Generated from the active `global_writers` analytical spine and the reviewed
context-slice layer. BnF and other external authority sources are out of scope
for the current project phase.

## TL;DR

- The active corpus contains {fmt_count(total)} Wikidata humans born 1675-1775
  with occupation `writer` or a subclass of writer.
- Country of citizenship is missing for {fmt_count(missing_cit.value)}
  entities ({fmt_pct(missing_cit.pct)}), so it cannot carry the historical
  argument by itself.
- Context slices are now comparable because France, Germany, British, and
  China/Qing are all derived from the same global cohort and the same reviewed
  political-entity crosswalk.
- Place context materially changes the British and France/Germany slices:
  British has 645 place-only entities, France has 1,258, and Germany has 2,343.
  China/Qing is the contrast case: it is mostly recovered through citizenship
  evidence in the current Wikidata data.
- The top Wikipedia language editions by article coverage are enwiki, dewiki,
  frwiki, ruwiki, and itwiki.
- Occupation evidence is not just "writer": Religion / Theology and Education /
  Scholarship / Humanities are both large enough to support conference-facing
  claims about intellectual networks.

## Context Slices

{markdown_table(context_brief, list(context_brief.columns))}

## Language Representation

{markdown_table(top_languages, ["language_edition", "represented_entities", "representation_pct"])}

Language coverage by slice is written to
`{outputs["language_by_slice"].relative_to(Path.cwd()).as_posix()}`. Use it for
heatmaps or punchcards comparing how each Wikipedia language edition represents
France, Germany, British, and China/Qing contexts.

## Gender

{markdown_table(gender, ["slice_id", "gender_category", "slice_entities", "slice_pct", "global_pct"], limit=16)}

## Occupation Buckets

{markdown_table(top_occupations, ["granular_bucket", "bucket_family", "entity_count", "entity_pct"], limit=8)}

Slice-level occupation bucket counts are written to
`{outputs["occupation_by_slice"].relative_to(Path.cwd()).as_posix()}`.

## Data Friction

{markdown_table(global_friction, ["metric", "entity_count", "denominator", "pct", "notes"])}

## Label And Description Coverage

{markdown_table(label_coverage, ["language_code", "wiki_edition", "wikidata_label_pct", "wikidata_description_pct", "wikipedia_article_pct"], limit=12)}

## Current Argument Surface

The strongest current argument is methodological: the project can show how
different evidence choices alter the Enlightenment map. A single field such as
country of citizenship undercounts or distorts imperial, dynastic, colonial,
and missing-data contexts. The reproducible context slices let us compare
France, Germany, British, and China/Qing on the same footing while keeping each
supporting field visible.

## Purposeful Next Work

1. Review the political-entity crosswalk rows used in the context slices.
2. Review the occupation-bucket crosswalk rows marked for manual review.
3. Turn the packet tables into a small set of visualizations: context-slice bar
   charts, citizenship-vs-place punchcards, language-edition heatmaps, and
   occupation-bucket comparisons.
4. Do not add external sources until a specific conference claim cannot be
   answered from the current Wikidata/Wikipedia evidence.
"""


def main() -> None:
    project_root = Path(__file__).resolve().parents[2]
    paths = cohort_paths(project_root, DEFAULT_COHORT_ID)
    if paths.cohort_id != "global_writers":
        raise SystemExit("Core findings packet expects the global_writers cohort to be the analytical default.")

    tables = read_processed(paths)
    output_dir = paths.processed_dir
    outputs = {key: output_dir / filename for key, filename in OUTPUT_FILENAMES.items()}

    dataframes = {
        "key_metrics": make_key_metrics(tables),
        "context_slices": make_context_slice_table(tables),
        "language_by_slice": make_language_by_slice(tables),
        "gender_by_slice": make_gender_by_slice(tables),
        "occupation_by_slice": make_occupation_by_slice(tables),
        "data_friction": make_data_friction(tables),
    }

    for key, df in dataframes.items():
        df.to_csv(outputs[key], index=False)

    report_path = project_root / "docs" / "core_findings_packet.md"
    report_path.write_text(build_markdown_report(tables, outputs), encoding="utf-8")

    print("Core findings packet complete.")
    print(f"Cohort: {paths.cohort_id}")
    print(f"Report: {report_path}")
    for output_path in outputs.values():
        print(f"Output: {output_path}")


if __name__ == "__main__":
    main()
