from argparse import ArgumentParser
from pathlib import Path
import sys
import json

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from cohorts import COHORT_IDS, DEFAULT_COHORT_ID, cohort_paths  # noqa: E402

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


CHUNK_SIZE = 50

PLACE_PROPERTIES = {
    "birth_place": "P19",
    "death_place": "P20",
    "residence": "P551",
    "work_location": "P937",
}


def load_entity_cache(cache_path: Path) -> dict[str, dict]:
    if not cache_path.exists():
        return {}
    return json.loads(cache_path.read_text(encoding="utf-8"))


def save_entity_cache(cache_path: Path, cache: dict[str, dict]) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(cache, ensure_ascii=False, sort_keys=True), encoding="utf-8")


def fetch_entities_cached(
    qids: set[str] | list[str],
    cache_path: Path,
    *,
    chunk_size: int = CHUNK_SIZE,
) -> dict[str, dict]:
    cache = load_entity_cache(cache_path)
    requested_qids = sorted(set(qids))
    missing_qids = [qid for qid in requested_qids if qid not in cache]
    if missing_qids:
        print(f"Fetching {len(missing_qids)} entities into {cache_path.name}...")
    else:
        print(f"Using cached entities from {cache_path.name}.")

    for start in range(0, len(missing_qids), chunk_size):
        qid_chunk = missing_qids[start:start + chunk_size]
        cache.update(fetch_entities(qid_chunk, chunk_size=chunk_size))
        save_entity_cache(cache_path, cache)
        print(f"  cached {min(start + chunk_size, len(missing_qids))}/{len(missing_qids)}")

    return {qid: cache[qid] for qid in requested_qids if qid in cache}


def entity_ids_to_uris(qids: list[str]) -> object:
    return join_ordered_values([qid_uri(qid) for qid in qids])


def collect_person_place_rows(person_entities: dict[str, dict]) -> list[dict]:
    rows = []
    for person_qid, entity in sorted(person_entities.items()):
        person_label = label_for(entity, person_qid)
        for place_role, property_id in PLACE_PROPERTIES.items():
            for place_qid in claim_qids(entity, property_id):
                rows.append(
                    {
                        "person": qid_uri(person_qid),
                        "person_id": person_qid,
                        "person_label": person_label,
                        "place_role": place_role,
                        "place": qid_uri(place_qid),
                        "place_id": place_qid,
                    }
                )
    return rows


def fetch_place_and_admin_entities(place_ids: set[str], max_admin_depth: int, cache_path: Path) -> dict[str, dict]:
    fetched = {}
    frontier = set(place_ids)

    for _depth in range(max_admin_depth + 1):
        missing = frontier - set(fetched)
        if missing:
            fetched.update(fetch_entities_cached(missing, cache_path))
        next_frontier = set()
        for qid in frontier:
            entity = fetched.get(qid, {})
            next_frontier.update(claim_qids(entity, "P131"))
        frontier = next_frontier - set(fetched)
        if not frontier:
            break

    country_ids = set()
    for entity in fetched.values():
        country_ids.update(claim_qids(entity, "P17"))

    missing_countries = country_ids - set(fetched)
    if missing_countries:
        fetched.update(fetch_entities_cached(missing_countries, cache_path))

    return fetched


def collect_admin_ancestors(
    place_id: str,
    entity_lookup: dict[str, dict],
    max_admin_depth: int,
) -> list[str]:
    ancestors = []
    seen = set()
    frontier = claim_qids(entity_lookup.get(place_id, {}), "P131")

    for _depth in range(max_admin_depth):
        next_frontier = []
        for admin_id in frontier:
            if admin_id in seen:
                continue
            seen.add(admin_id)
            ancestors.append(admin_id)
            next_frontier.extend(claim_qids(entity_lookup.get(admin_id, {}), "P131"))
        frontier = next_frontier
        if not frontier:
            break

    return ancestors


def build_place_context_rows(
    place_ids: set[str],
    entity_lookup: dict[str, dict],
    max_admin_depth: int,
) -> tuple[list[dict], dict[str, dict]]:
    rows = []
    context_by_place = {}

    for place_id in sorted(place_ids):
        place_entity = entity_lookup.get(place_id, {})
        admin_ids = collect_admin_ancestors(place_id, entity_lookup, max_admin_depth)
        direct_country_ids = claim_qids(place_entity, "P17")

        admin_country_ids = []
        for admin_id in admin_ids:
            admin_country_ids.extend(claim_qids(entity_lookup.get(admin_id, {}), "P17"))

        context_country_ids = sorted(set(direct_country_ids + admin_country_ids))
        lat, lon = first_coordinate(place_entity)

        context = {
            "place_id": place_id,
            "place_label": label_for(place_entity, place_id),
            "place_lat": lat,
            "place_lon": lon,
            "direct_country_ids": direct_country_ids,
            "direct_country_labels": [label_for(entity_lookup.get(qid, {}), qid) for qid in direct_country_ids],
            "admin_country_ids": sorted(set(admin_country_ids)),
            "admin_country_labels": [
                label_for(entity_lookup.get(qid, {}), qid)
                for qid in sorted(set(admin_country_ids))
            ],
            "context_country_ids": context_country_ids,
            "context_country_labels": [
                label_for(entity_lookup.get(qid, {}), qid)
                for qid in context_country_ids
            ],
            "admin_entity_ids": admin_ids,
            "admin_entity_labels": [label_for(entity_lookup.get(qid, {}), qid) for qid in admin_ids],
        }
        context_by_place[place_id] = context
        rows.append(
            {
                "place": qid_uri(place_id),
                "place_id": place_id,
                "place_label": context["place_label"],
                "place_lat": lat,
                "place_lon": lon,
                "direct_country_ids": join_ordered_values(direct_country_ids),
                "direct_country_labels": join_ordered_values(context["direct_country_labels"]),
                "admin_country_ids": join_values(context["admin_country_ids"]),
                "admin_country_labels": join_ordered_values(context["admin_country_labels"]),
                "context_country_ids": join_values(context_country_ids),
                "context_country_labels": join_ordered_values(context["context_country_labels"]),
                "admin_entity_ids": join_ordered_values(admin_ids),
                "admin_entity_labels": join_ordered_values(context["admin_entity_labels"]),
            }
        )

    return rows, context_by_place


def build_person_place_context_rows(
    person_place_rows: list[dict],
    context_by_place: dict[str, dict],
) -> list[dict]:
    rows = []
    for row in person_place_rows:
        context = context_by_place[row["place_id"]]
        rows.append(
            {
                **row,
                "place_label": context["place_label"],
                "place_lat": context["place_lat"],
                "place_lon": context["place_lon"],
                "direct_country_ids": join_ordered_values(context["direct_country_ids"]),
                "direct_country_labels": join_ordered_values(context["direct_country_labels"]),
                "admin_country_ids": join_values(context["admin_country_ids"]),
                "admin_country_labels": join_ordered_values(context["admin_country_labels"]),
                "context_country_ids": join_values(context["context_country_ids"]),
                "context_country_labels": join_ordered_values(context["context_country_labels"]),
                "admin_entity_ids": join_ordered_values(context["admin_entity_ids"]),
                "admin_entity_labels": join_ordered_values(context["admin_entity_labels"]),
            }
        )
    return rows


def parse_args() -> object:
    parser = ArgumentParser(
        description="Fetch Wikidata birth/death/residence/work-location context through the Wikidata API."
    )
    parser.add_argument(
        "--cohort-id",
        default=DEFAULT_COHORT_ID,
        choices=COHORT_IDS,
        help=f"Cohort to fetch. Default: {DEFAULT_COHORT_ID}.",
    )
    parser.add_argument(
        "--input",
        default=None,
        help="Input cohort CSV. Default: data/interim/writers_wikidata_enriched.csv",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output person-place context CSV.",
    )
    parser.add_argument(
        "--place-output",
        default=None,
        help="Output unique-place context CSV.",
    )
    parser.add_argument(
        "--max-admin-depth",
        type=int,
        default=3,
        help="Maximum P131 administrative ancestor depth to follow. Default: 3.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    project_root = Path(__file__).resolve().parents[2]
    paths = cohort_paths(project_root, args.cohort_id)
    input_path = project_root / args.input if args.input else paths.enriched_path
    output_path = project_root / args.output if args.output else paths.raw_person_place_context_path
    place_output_path = (
        project_root / args.place_output
        if args.place_output
        else paths.raw_place_context_entities_path
    )

    if not input_path.exists():
        raise SystemExit(f"Missing input cohort: {input_path}")

    cache_dir = (
        project_root / "data" / "raw" / "cache"
        if paths.cohort_id == "french_seed"
        else project_root / "data" / "raw" / "cache" / paths.cohort_id
    )
    person_cache_path = cache_dir / "wikidata_api_person_entities.json"
    place_cache_path = cache_dir / "wikidata_api_place_context_entities.json"

    df = pd.read_csv(input_path, usecols=["wikidata_id"])
    person_qids = sorted({qid for qid in df["wikidata_id"].map(normalize_qid).dropna()})
    person_entities = fetch_entities_cached(person_qids, person_cache_path)
    person_place_rows = collect_person_place_rows(person_entities)
    place_ids = {row["place_id"] for row in person_place_rows}

    place_entities = fetch_place_and_admin_entities(place_ids, args.max_admin_depth, place_cache_path)
    place_context_rows, context_by_place = build_place_context_rows(
        place_ids=place_ids,
        entity_lookup=place_entities,
        max_admin_depth=args.max_admin_depth,
    )
    person_place_context_rows = build_person_place_context_rows(person_place_rows, context_by_place)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(person_place_context_rows).to_csv(output_path, index=False)
    pd.DataFrame(place_context_rows).to_csv(place_output_path, index=False)

    print("Wikidata person-place context fetch complete.")
    print(f"Cohort: {paths.cohort_id}")
    print(f"People queried: {len(person_qids)}")
    print(f"Person-place rows: {len(person_place_context_rows)}")
    print(f"Unique places: {len(place_context_rows)}")
    print(f"Person-place output: {output_path}")
    print(f"Place context output: {place_output_path}")


if __name__ == "__main__":
    main()
