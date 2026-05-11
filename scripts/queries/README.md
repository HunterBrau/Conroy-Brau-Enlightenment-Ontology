# Query and Source-Acquisition Scripts

This folder stores reproducible source-acquisition helpers. The active
Wikidata workflow is API-first.

## Active Wikidata API Helpers

- `wikidata_api.py` contains shared Wikidata entity API utilities: chunking,
  retry behavior, label selection, QID normalization, claim extraction, and
  coordinate extraction.
- `17_fetch_wikidata_enrichment_api.py` fetches the Step 04 enrichment export
  for the current cohort and writes `data/raw/wikidata_affiliation_enrichment.csv`.
- `18_fetch_wikidata_person_place_context.py` fetches birth, death, residence,
  and work-location place context and writes:
  - `data/raw/wikidata_person_place_context.csv`
  - `data/raw/wikidata_place_context_entities.csv`
- `19_discover_global_writers.py` discovers the global 1675-1775
  writer/subclass cohort and writes
  `data/raw/global_writers_1675_1775_discovery.csv`.
- `12_build_wikidata_label_corrections.py` builds correction tables for
  unresolved person-name and birth-place labels.

Run the main enrichment fetch after Steps 01 and 02:

```powershell
python scripts/queries/17_fetch_wikidata_enrichment_api.py
python scripts/queries/17_fetch_wikidata_enrichment_api.py --cohort-id global_writers
```

Then merge it locally:

```powershell
python scripts/pipeline/04_merge_wikidata_enrichment.py
```

The API fetcher writes resumable entity caches under `data/raw/cache/`. That
directory is ignored by git.

Run the place-context fetch after Step 04:

```powershell
python scripts/queries/18_fetch_wikidata_person_place_context.py
python scripts/queries/18_fetch_wikidata_person_place_context.py --cohort-id global_writers
```

Run the global discovery before the global pipeline:

```powershell
python scripts/queries/19_discover_global_writers.py
```

## Label Correction Helper

Run this after Steps 01 and 02, and usually after Step 03 diagnostics:

```powershell
python scripts/queries/12_build_wikidata_label_corrections.py
```

It reads `data/interim/writers_cleaned.csv` and writes:

- `data/processed/person_name_label_corrections.csv`
- `data/processed/birth_place_label_corrections.csv`

The helper does not change the cleaned cohort. It records readable Wikidata
labels, fallback languages, and descriptions so a later deterministic pipeline
step can apply corrections without losing the original QID values.

## Legacy SPARQL Artifacts

The original pilot raw inputs came from Wikidata Query Service exports, and the
repo still keeps SPARQL scripts/templates for provenance, reconstruction, and
small diagnostic checks:

- `00_run_wikidata_sparql_query.py`
- `01_export_wikidata_values_block.py`
- `02_wikidata_affiliation_enrichment.rq`
- `03_wikidata_current_cohort_geography_export.rq`
- `04_wikidata_current_cohort_viaf_export.rq`
- `05_wikidata_affiliation_death_place.rq`
- `06_wikidata_affiliation_languages.rq`
- `07_wikidata_wikipedia_representation.rq`
- `08_wikidata_affiliation_residence.rq`
- `09_wikidata_affiliation_work_location.rq`
- `10_wikidata_affiliation_citizenship.rq`
- `11_run_wikidata_query_batch.py`

These are no longer the recommended Step 04 workflow for the full cohort. The
Wikidata entity API is more reliable for this project because it avoids large
multivalued SPARQL joins and can resume from local caches.
