from pathlib import Path
from argparse import ArgumentParser
import re

import pandas as pd


QID_PATTERN = re.compile(r"(Q\d+)$")
BEGIN_MARKER = "  # BEGIN COHORT VALUES"
END_MARKER = "  # END COHORT VALUES"
CHUNK_SIZE = 250
QUERY_TEMPLATE_SPECS = [
    {
        "template_name": "02_wikidata_affiliation_enrichment.rq",
        "full_output_name": "wikidata_affiliation_enrichment_query.rq",
        "chunk_dir_name": "wikidata_affiliation_queries",
        "chunk_prefix": "wikidata_affiliation_enrichment_part_",
    },
    {
        "template_name": "03_wikidata_current_cohort_geography_export.rq",
        "full_output_name": "wikidata_current_cohort_geography_query.rq",
        "chunk_dir_name": "wikidata_current_cohort_geography_queries",
        "chunk_prefix": "wikidata_current_cohort_geography_part_",
    },
    {
        "template_name": "04_wikidata_current_cohort_viaf_export.rq",
        "full_output_name": "wikidata_current_cohort_viaf_query.rq",
        "chunk_dir_name": "wikidata_current_cohort_viaf_queries",
        "chunk_prefix": "wikidata_current_cohort_viaf_part_",
    },
    {
        "template_name": "05_wikidata_affiliation_death_place.rq",
        "full_output_name": "wikidata_affiliation_death_place_query.rq",
        "chunk_dir_name": "wikidata_affiliation_death_place_queries",
        "chunk_prefix": "wikidata_affiliation_death_place_part_",
        "chunk_size": 20,
    },
    {
        "template_name": "06_wikidata_affiliation_languages.rq",
        "full_output_name": "wikidata_affiliation_languages_query.rq",
        "chunk_dir_name": "wikidata_affiliation_languages_queries",
        "chunk_prefix": "wikidata_affiliation_languages_part_",
    },
    {
        "template_name": "07_wikidata_wikipedia_representation.rq",
        "full_output_name": "wikidata_wikipedia_representation_query.rq",
        "chunk_dir_name": "wikidata_wikipedia_representation_queries",
        "chunk_prefix": "wikidata_wikipedia_representation_part_",
    },
    {
        "template_name": "08_wikidata_affiliation_residence.rq",
        "full_output_name": "wikidata_affiliation_residence_query.rq",
        "chunk_dir_name": "wikidata_affiliation_residence_queries",
        "chunk_prefix": "wikidata_affiliation_residence_part_",
    },
    {
        "template_name": "09_wikidata_affiliation_work_location.rq",
        "full_output_name": "wikidata_affiliation_work_location_query.rq",
        "chunk_dir_name": "wikidata_affiliation_work_location_queries",
        "chunk_prefix": "wikidata_affiliation_work_location_part_",
    },
    {
        "template_name": "10_wikidata_affiliation_citizenship.rq",
        "full_output_name": "wikidata_affiliation_citizenship_query.rq",
        "chunk_dir_name": "wikidata_affiliation_citizenship_queries",
        "chunk_prefix": "wikidata_affiliation_citizenship_part_",
    },
]


def extract_qids(df: pd.DataFrame) -> list[str]:
    qids = []

    for value in df["wikidata_id"].dropna().astype(str).unique():
        match = QID_PATTERN.search(value.strip())
        if match:
            qids.append(match.group(1))

    return sorted(set(qids))


def build_values_block(qids: list[str], base_indent: str = "") -> str:
    values_lines = [f"{base_indent}VALUES ?person {{"]
    values_lines.extend(f"{base_indent}  wd:{qid}" for qid in qids)
    values_lines.append(f"{base_indent}}}")
    return "\n".join(values_lines)


def inject_values_block(template_text: str, qids: list[str]) -> str:
    values_block = build_values_block(qids, base_indent="  ")
    pattern = re.compile(
        rf"{re.escape(BEGIN_MARKER)}\n.*?\n{re.escape(END_MARKER)}",
        flags=re.DOTALL,
    )
    replacement = f"{BEGIN_MARKER}\n{values_block}\n{END_MARKER}"
    updated_text, replacements = pattern.subn(replacement, template_text, count=1)

    if replacements != 1:
        raise ValueError(
            "Could not find the cohort VALUES markers in the Wikidata query template."
        )

    return updated_text


def chunked(values: list[str], chunk_size: int) -> list[list[str]]:
    return [
        values[start:start + chunk_size]
        for start in range(0, len(values), chunk_size)
    ]


def render_query_outputs(
    project_root: Path,
    qids: list[str],
    template_name: str,
    full_output_name: str,
    chunk_dir_name: str,
    chunk_prefix: str,
    chunk_size: int,
) -> tuple[Path, Path, int, int]:
    template_path = project_root / "scripts" / "queries" / template_name
    full_query_path = project_root / "outputs" / full_output_name
    chunk_dir = project_root / "outputs" / chunk_dir_name

    template_text = template_path.read_text(encoding="utf-8")
    full_query_text = inject_values_block(template_text, qids)
    full_query_path.write_text(full_query_text, encoding="utf-8")

    chunk_dir.mkdir(parents=True, exist_ok=True)
    for stale_path in chunk_dir.glob(f"{chunk_prefix}*.rq"):
        stale_path.unlink()

    query_chunks = chunked(qids, chunk_size)
    for chunk_index, qid_chunk in enumerate(query_chunks, start=1):
        chunk_query_text = inject_values_block(template_text, qid_chunk)
        chunk_query_path = chunk_dir / f"{chunk_prefix}{chunk_index:03d}.rq"
        chunk_query_path.write_text(chunk_query_text, encoding="utf-8")

    return full_query_path, chunk_dir, len(query_chunks), chunk_size


def parse_args() -> object:
    parser = ArgumentParser(
        description="Generate cohort VALUES blocks and query files for Wikidata."
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=CHUNK_SIZE,
        help=f"Number of QIDs per chunked query file. Default: {CHUNK_SIZE}",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    project_root = Path(__file__).resolve().parents[2]

    input_path = project_root / "data" / "interim" / "writers_cleaned.csv"
    output_path = project_root / "outputs" / "wikidata_cohort_values_block.txt"

    if not input_path.exists():
        raise FileNotFoundError(
            "Missing cleaned dataset. Run the pipeline first:\n"
            "python scripts/pipeline/01_build_merged_dataset.py\n"
            "python scripts/pipeline/02_clean_structural_fields.py"
        )

    df = pd.read_csv(input_path, usecols=["wikidata_id"])
    qids = extract_qids(df)
    values_block = build_values_block(qids) + "\n"

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(values_block, encoding="utf-8")

    rendered_outputs = []
    for template_spec in QUERY_TEMPLATE_SPECS:
        full_query_path, chunk_dir, chunk_count, chunk_size = render_query_outputs(
            project_root=project_root,
            qids=qids,
            template_name=template_spec["template_name"],
            full_output_name=template_spec["full_output_name"],
            chunk_dir_name=template_spec["chunk_dir_name"],
            chunk_prefix=template_spec["chunk_prefix"],
            chunk_size=template_spec.get("chunk_size", args.chunk_size),
        )
        rendered_outputs.append((full_query_path, chunk_dir, chunk_count, chunk_size))

    print(f"Wrote {len(qids)} Wikidata IDs to: {output_path}")
    for full_query_path, chunk_dir, chunk_count, chunk_size in rendered_outputs:
        print(f"Wrote full query to: {full_query_path}")
        print(
            f"Wrote {chunk_count} chunked queries to: {chunk_dir} "
            f"(chunk size {chunk_size})"
        )
    print(
        "Run the generated full or chunked queries in the Wikidata Query "
        "Service, or use scripts/queries/00_run_wikidata_sparql_query.py "
        "to export them directly."
    )


if __name__ == "__main__":
    main()
