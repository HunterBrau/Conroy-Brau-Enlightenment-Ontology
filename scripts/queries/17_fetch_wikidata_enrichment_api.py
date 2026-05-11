from argparse import ArgumentParser
from pathlib import Path
import json
import sys

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from cohorts import DEFAULT_COHORT_ID, cohort_paths  # noqa: E402

from wikidata_api import (
    claim_qids,
    fetch_entities,
    first_coordinate,
    join_ordered_values,
    join_values,
    label_for,
    normalize_qid,
    qid_uri,
)


LANGUAGE_CODES = ["fr", "en", "de", "it", "es", "pl", "ru", "uk", "nl", "pt", "sv", "da"]
WIKI_COLUMNS = {
    "frwiki": "has_frwiki",
    "enwiki": "has_enwiki",
    "dewiki": "has_dewiki",
    "itwiki": "has_itwiki",
    "eswiki": "has_eswiki",
    "plwiki": "has_plwiki",
    "ruwiki": "has_ruwiki",
    "ukwiki": "has_ukwiki",
    "nlwiki": "has_nlwiki",
    "ptwiki": "has_ptwiki",
    "svwiki": "has_svwiki",
    "dawiki": "has_dawiki",
}
NON_WIKIPEDIA_WIKI_SITES = {
    "commonswiki",
    "incubatorwiki",
    "mediawikiwiki",
    "metawiki",
    "outreachwiki",
    "specieswiki",
    "test2wiki",
    "testwiki",
    "wikidatawiki",
}
WRITER_QID = "Q36180"

DIRECT_PROPERTY_SPECS = {
    "death_place": ("P20", "death_place_ids", "death_place_labels"),
    "citizenship": ("P27", "citizenship_ids", "citizenship_labels"),
    "residence": ("P551", "residence_ids", "residence_labels"),
    "work_location": ("P937", "work_location_ids", "work_location_labels"),
    "native_language": ("P103", "native_language_ids", "native_language_labels"),
    "spoken_written_language": ("P1412", "spoken_written_language_ids", "spoken_written_language_labels"),
    "writing_language": ("P6886", "writing_language_ids", "writing_language_labels"),
    "gender": ("P21", "gender_ids", "gender_labels"),
    "ethnic_group": ("P172", "ethnic_group_ids", "ethnic_group_labels"),
    "occupation": ("P106", "occupation_ids", "occupation_labels"),
    "notable_work": ("P800", "notable_work_ids", "notable_work_labels"),
}


def load_entity_cache(cache_path: Path) -> dict[str, dict]:
    if not cache_path.exists():
        return {}
    return json.loads(cache_path.read_text(encoding="utf-8"))


def save_entity_cache(cache_path: Path, cache: dict[str, dict]) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(
        json.dumps(cache, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )


def fetch_entities_cached(
    qids: set[str] | list[str],
    cache_path: Path,
    *,
    props: str,
    languages: str,
    chunk_size: int = 25,
    pause_seconds: float = 0.5,
) -> dict[str, dict]:
    cache = load_entity_cache(cache_path)
    requested_qids = sorted(set(qids))
    missing_qids = [qid for qid in requested_qids if qid not in cache]

    if missing_qids:
        print(f"Fetching {len(missing_qids)} Wikidata entities into {cache_path.name}...")
    else:
        print(f"Using cached Wikidata entities from {cache_path.name}.")

    for start in range(0, len(missing_qids), chunk_size):
        qid_chunk = missing_qids[start:start + chunk_size]
        fetched = fetch_entities(
            qid_chunk,
            props=props,
            languages=languages,
            chunk_size=chunk_size,
            pause_seconds=pause_seconds,
        )
        cache.update(fetched)
        save_entity_cache(cache_path, cache)
        print(f"  cached {min(start + chunk_size, len(missing_qids))}/{len(missing_qids)}")

    return {qid: cache[qid] for qid in requested_qids if qid in cache}


def extract_person_qids(input_path: Path) -> list[str]:
    df = pd.read_csv(input_path, usecols=["wikidata_id"])
    qids = [qid for qid in df["wikidata_id"].map(normalize_qid).dropna()]
    return sorted(set(qids))


def language_list(entity: dict, field: str) -> list[str]:
    values = entity.get(field, {})
    return [language for language in LANGUAGE_CODES if values.get(language, {}).get("value")]


def label_list(qids: list[str], entity_lookup: dict[str, dict]) -> object:
    return join_ordered_values([label_for(entity_lookup.get(qid, {}), qid) for qid in qids])


def coordinate_list(qids: list[str], entity_lookup: dict[str, dict]) -> object:
    values = []
    for qid in qids:
        lat, lon = first_coordinate(entity_lookup.get(qid, {}))
        if pd.notna(lat) and pd.notna(lon):
            values.append(f"Point({lon} {lat})")
    return join_ordered_values(values)


def is_wikipedia_site(site: str) -> bool:
    return site.endswith("wiki") and site not in NON_WIKIPEDIA_WIKI_SITES


def collect_direct_reference_ids(person_entities: dict[str, dict]) -> set[str]:
    reference_ids = set()
    for entity in person_entities.values():
        for property_id, _ids_column, _labels_column in DIRECT_PROPERTY_SPECS.values():
            reference_ids.update(claim_qids(entity, property_id))
    return reference_ids


def extend_occupation_subclass_graph(
    occupation_ids: set[str],
    entity_lookup: dict[str, dict],
    max_depth: int,
    reference_cache_path: Path,
) -> None:
    frontier = set(occupation_ids)
    seen = set()

    for _depth in range(max_depth):
        missing = frontier - set(entity_lookup)
        if missing:
            entity_lookup.update(
                fetch_entities_cached(
                    missing,
                    reference_cache_path,
                    props="labels|claims",
                    languages="fr|en",
                    chunk_size=25,
                    pause_seconds=0.5,
                )
            )

        next_frontier = set()
        for qid in frontier:
            if qid in seen:
                continue
            seen.add(qid)
            next_frontier.update(claim_qids(entity_lookup.get(qid, {}), "P279"))

        frontier = next_frontier - seen
        if not frontier:
            break


def occupation_ancestors(qid: str, entity_lookup: dict[str, dict]) -> set[str]:
    ancestors = set()
    stack = claim_qids(entity_lookup.get(qid, {}), "P279")

    while stack:
        parent_id = stack.pop()
        if parent_id in ancestors:
            continue
        ancestors.add(parent_id)
        stack.extend(claim_qids(entity_lookup.get(parent_id, {}), "P279"))

    return ancestors


def is_writerly_occupation(qid: str, entity_lookup: dict[str, dict]) -> bool:
    return qid == WRITER_QID or WRITER_QID in occupation_ancestors(qid, entity_lookup)


def collect_work_genre_form_ids(work_ids: set[str], entity_lookup: dict[str, dict]) -> tuple[set[str], set[str]]:
    genre_ids = set()
    form_ids = set()

    for work_id in work_ids:
        work_entity = entity_lookup.get(work_id, {})
        genre_ids.update(claim_qids(work_entity, "P136"))
        form_ids.update(claim_qids(work_entity, "P7937"))

    return genre_ids, form_ids


def build_enrichment_rows(
    person_qids: list[str],
    person_entities: dict[str, dict],
    reference_entities: dict[str, dict],
) -> list[dict]:
    rows = []

    for person_qid in person_qids:
        entity = person_entities.get(person_qid, {})
        row = {
            "person": qid_uri(person_qid),
            "personLabel": label_for(entity, person_qid),
        }

        for _field_name, (property_id, ids_column, labels_column) in DIRECT_PROPERTY_SPECS.items():
            qids = claim_qids(entity, property_id)
            row[ids_column] = join_ordered_values(qids)
            row[labels_column] = label_list(qids, reference_entities)

        death_place_ids = claim_qids(entity, "P20")
        row["death_coords"] = coordinate_list(death_place_ids, reference_entities)

        occupation_ids = claim_qids(entity, "P106")
        writerly_ids = [
            qid for qid in occupation_ids
            if is_writerly_occupation(qid, reference_entities)
        ]
        nonwriter_ids = [qid for qid in occupation_ids if qid not in set(writerly_ids)]
        row["writerly_occupation_ids"] = join_ordered_values(writerly_ids)
        row["writerly_occupation_labels"] = label_list(writerly_ids, reference_entities)
        row["nonwriter_occupation_ids"] = join_ordered_values(nonwriter_ids)
        row["nonwriter_occupation_labels"] = label_list(nonwriter_ids, reference_entities)

        work_ids = claim_qids(entity, "P800")
        genre_ids = sorted(
            {
                genre_id
                for work_id in work_ids
                for genre_id in claim_qids(reference_entities.get(work_id, {}), "P136")
            }
        )
        form_ids = sorted(
            {
                form_id
                for work_id in work_ids
                for form_id in claim_qids(reference_entities.get(work_id, {}), "P7937")
            }
        )
        row["notable_work_count"] = len(work_ids)
        row["notable_work_genre_ids"] = join_values(genre_ids)
        row["notable_work_genre_labels"] = label_list(genre_ids, reference_entities)
        row["notable_work_form_ids"] = join_values(form_ids)
        row["notable_work_form_labels"] = label_list(form_ids, reference_entities)

        label_languages = language_list(entity, "labels")
        description_languages = language_list(entity, "descriptions")
        row["european_label_count"] = len(label_languages)
        row["entity_label_languages"] = join_values(label_languages)
        row["european_description_count"] = len(description_languages)
        row["entity_description_languages"] = join_values(description_languages)

        sitelinks = entity.get("sitelinks", {})
        for wiki_code, column in WIKI_COLUMNS.items():
            row[column] = wiki_code in sitelinks
        row["wikipedia_sitelink_count"] = len(
            [site for site in sitelinks if is_wikipedia_site(site)]
        )

        rows.append(row)

    return rows


def parse_args() -> object:
    parser = ArgumentParser(
        description="Fetch the Step 04 Wikidata enrichment export through the Wikidata entity API."
    )
    parser.add_argument(
        "--cohort-id",
        default=DEFAULT_COHORT_ID,
        choices=["french_seed", "global_writers"],
        help=f"Cohort to fetch. Default: {DEFAULT_COHORT_ID}.",
    )
    parser.add_argument(
        "--input",
        default=None,
        help="Input cleaned cohort CSV. Default: data/interim/writers_cleaned.csv",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output enrichment CSV. Default: data/raw/wikidata_affiliation_enrichment.csv",
    )
    parser.add_argument(
        "--max-occupation-depth",
        type=int,
        default=2,
        help="Maximum occupation subclass depth for writerly classification. Default: 2.",
    )
    parser.add_argument(
        "--cache-dir",
        default=None,
        help="Directory for resumable Wikidata API entity caches. Default: data/raw/cache",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    project_root = Path(__file__).resolve().parents[2]
    paths = cohort_paths(project_root, args.cohort_id)
    input_path = project_root / args.input if args.input else paths.cleaned_path
    output_path = project_root / args.output if args.output else paths.raw_enrichment_path
    if args.cache_dir:
        cache_dir = project_root / args.cache_dir
    elif paths.cohort_id == "french_seed":
        cache_dir = project_root / "data" / "raw" / "cache"
    else:
        cache_dir = project_root / "data" / "raw" / "cache" / paths.cohort_id
    person_cache_path = cache_dir / "wikidata_api_person_entities.json"
    reference_cache_path = cache_dir / "wikidata_api_reference_entities.json"

    if not input_path.exists():
        raise SystemExit(f"Missing input cohort: {input_path}")

    person_qids = extract_person_qids(input_path)
    person_entities = fetch_entities_cached(
        person_qids,
        person_cache_path,
        props="labels|descriptions|claims|sitelinks",
        languages="|".join(LANGUAGE_CODES),
        chunk_size=25,
        pause_seconds=0.5,
    )

    reference_ids = collect_direct_reference_ids(person_entities)
    reference_entities = fetch_entities_cached(
        reference_ids,
        reference_cache_path,
        props="labels|claims",
        languages="fr|en",
        chunk_size=25,
        pause_seconds=0.5,
    )

    occupation_ids = {
        qid
        for entity in person_entities.values()
        for qid in claim_qids(entity, "P106")
    }
    extend_occupation_subclass_graph(
        occupation_ids=occupation_ids,
        entity_lookup=reference_entities,
        max_depth=args.max_occupation_depth,
        reference_cache_path=reference_cache_path,
    )

    work_ids = {
        qid
        for entity in person_entities.values()
        for qid in claim_qids(entity, "P800")
    }
    genre_ids, form_ids = collect_work_genre_form_ids(work_ids, reference_entities)
    missing_genre_form_ids = (genre_ids | form_ids) - set(reference_entities)
    if missing_genre_form_ids:
        reference_entities.update(
            fetch_entities_cached(
                missing_genre_form_ids,
                reference_cache_path,
                props="labels|claims",
                languages="fr|en",
                chunk_size=25,
                pause_seconds=0.5,
            )
        )

    rows = build_enrichment_rows(person_qids, person_entities, reference_entities)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(output_path, index=False)

    print("Wikidata API enrichment fetch complete.")
    print(f"Cohort: {paths.cohort_id}")
    print(f"People queried: {len(person_qids)}")
    print(f"Reference entities fetched: {len(reference_entities)}")
    print(f"Output rows: {len(rows)}")
    print(f"Output CSV: {output_path}")


if __name__ == "__main__":
    main()
