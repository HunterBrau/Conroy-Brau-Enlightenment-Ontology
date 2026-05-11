# Project Level Set

Last updated: 2026-05-10.

This note is the current working map of the repository after the French-seed
to global-writer bridge. It separates active project truth from legacy
provenance, generated data, local caches, and open decisions.

## Current Scholarly Scope

- Canonical date range: 1675-1775.
- Identity spine: Wikidata QIDs.
- Current source family: Wikidata, with VIAF retained as supporting metadata
  for the original French-seed export.
- Not yet active: BnF or additional external authority files.
- Primary comparison frame approved in discussion: France, British/British
  imperial context, China/Qing context, and Germany.

The project now has two explicit cohorts:

| Cohort | Meaning | Current distinct people |
|---|---:|---:|
| `french_seed` | Original French country-of-citizenship seed cohort, born 1675-1775, writer/subclass occupation | 1,638 |
| `global_writers` | Global Wikidata humans born 1675-1775 with writer or subclass occupation | 14,377 |

The original conference-script count of 18,697 is best understood as a
person-birth-occupation row count, not a unique-person count. The current
person spine keeps one row per QID and carries multiple occupation labels as
multi-value fields.

## Active Pipeline

The active workflow is API-first after global discovery:

1. Build or refresh the cohort manifest.
2. Discover the global writer cohort through SPARQL.
3. Build merged cohort tables.
4. Clean structural fields.
5. Diagnose the cohort.
6. Fetch Wikidata API enrichment.
7. Merge enrichment.
8. Fetch person-place context.
9. Build place-derived affiliation context.
10. Build representation matrices.
11. Build geographic-scope diagnostics.
12. Compare cohorts.

Commands are documented in:

- `docs/pipeline.md`
- `scripts/queries/README.md`
- `scripts/analysis/README.md`

## Active Data Spine

| Path | Status | Role |
|---|---|---|
| `data/cohorts/cohort_manifest.csv` | active | declares cohorts and output locations |
| `data/reference/political_entity_affiliation_crosswalk_seed.csv` | active review input | British/Qing/Germany/France political-entity buckets |
| `data/reference/occupation_bucket_crosswalk_seed.csv` | active review input | granular occupation buckets |
| `data/raw/18thcentury_french_writers_table.csv` | source | original French-seed discovery export |
| `data/raw/18thcentury_writers_wikidata_viaf.csv` | source | VIAF sidecar for French seed |
| `data/raw/global_writers_1675_1775_discovery.csv` | generated source | reproducible global discovery output |
| `data/raw/*wikidata*_enrichment*.csv` | generated source | Wikidata API enrichment exports |
| `data/raw/*place_context*.csv` | generated source | Wikidata API place-context exports |
| `data/interim/` | generated, ignored | merge/clean/diagnostic working tables |
| `data/processed/` | generated deliverables | analysis-ready outputs, correction tables, comparison tables |
| `data/raw/cache/` | local cache, ignored | resumable API responses; large but useful |

## Active Scripts

| Path | Status | Role |
|---|---|---|
| `scripts/cohorts.py` | active | cohort registry and path contract |
| `scripts/pipeline/00_build_cohort_manifest.py` | active | writes cohort manifest |
| `scripts/pipeline/01_build_merged_dataset.py` | active | builds cohort merge table |
| `scripts/pipeline/02_clean_structural_fields.py` | active | structural cleanup and flags |
| `scripts/pipeline/03_diagnose_dataset.py` | active | diagnostics and reports |
| `scripts/pipeline/04_merge_wikidata_enrichment.py` | active | joins API enrichment |
| `scripts/queries/wikidata_api.py` | active | shared Wikidata API helper |
| `scripts/queries/17_fetch_wikidata_enrichment_api.py` | active | person-level enrichment fetch |
| `scripts/queries/18_fetch_wikidata_person_place_context.py` | active | place-context fetch |
| `scripts/queries/19_discover_global_writers.py` | active | global writer discovery |
| `scripts/queries/12_build_wikidata_label_corrections.py` | active support | correction-table builder |
| `scripts/analysis/01_build_representation_matrices.py` | active | representation and affiliation matrices |
| `scripts/analysis/02_build_place_affiliation_context.py` | active | place-derived affiliation layer |
| `scripts/analysis/03_build_geographic_scope_analysis.py` | active | Europe/non-Europe and special-context diagnostics |
| `scripts/analysis/04_compare_cohorts.py` | active | French/global comparison outputs |
| `scripts/analysis/00_build_crosswalk_review_matrix.py` | active review | punchcard review table for the political crosswalk |
| `scripts/analysis/05_build_affiliation_evidence_matrix.py` | active | formula-backed affiliation evidence tallies |
| `scripts/analysis/06_build_occupation_bucket_tables.py` | active | occupation bucket crosswalk and bucket representation tables |

## Legacy Or Provenance Scripts

These files are not the recommended full-cohort route anymore, but they should
not be deleted casually. They document the earlier SPARQL-batch approach and
can still be useful for small checks or provenance.

| Path | Status |
|---|---|
| `scripts/queries/00_run_wikidata_sparql_query.py` | legacy helper |
| `scripts/queries/01_export_wikidata_values_block.py` | legacy helper |
| `scripts/queries/02_wikidata_affiliation_enrichment.rq` | legacy SPARQL |
| `scripts/queries/03_wikidata_current_cohort_geography_export.rq` | provenance/reconstruction |
| `scripts/queries/04_wikidata_current_cohort_viaf_export.rq` | provenance/reconstruction |
| `scripts/queries/05_wikidata_affiliation_death_place.rq` | legacy SPARQL |
| `scripts/queries/06_wikidata_affiliation_languages.rq` | legacy SPARQL |
| `scripts/queries/07_wikidata_wikipedia_representation.rq` | legacy/small-check SPARQL |
| `scripts/queries/08_wikidata_affiliation_residence.rq` | legacy SPARQL |
| `scripts/queries/09_wikidata_affiliation_work_location.rq` | legacy SPARQL |
| `scripts/queries/10_wikidata_affiliation_citizenship.rq` | legacy SPARQL |
| `scripts/queries/11_run_wikidata_query_batch.py` | legacy batch runner |

## Ignored Local Bloat

These paths are intentionally ignored by Git:

| Path | Current role |
|---|---|
| `data/raw/cache/` | large local API cache; useful for reruns |
| `data/interim/` | reproducible working tables |
| `outputs/` | ignored presentation/scratch directory; currently kept empty except `.gitkeep` |

The API cache is large, but it is valuable while queries are still changing.
Delete it only when disk space matters more than fast, resumable reruns.

The `outputs/` directory is no longer part of the active data spine. It has
been emptied except for `.gitkeep`; active tables now live under
`data/processed/`.

## Current Core Results

| Metric | `french_seed` | `global_writers` |
|---|---:|---:|
| Distinct people | 1,638 | 14,377 |
| Missing country of citizenship | 49 / 2.99% | 5,302 / 36.88% |
| China/Qing citizenship context | 0 | 404 |
| China/Qing place context | 1 | 104 |
| British imperial/colonial citizenship context | 2 | 14 |
| British imperial/colonial place context | 9 | 189 |
| European share among binary-classified birth contexts | 98.52% | 94.18% |
| Non-European/colonial share among binary-classified birth contexts | 1.48% | 5.82% |

## Decisions Already Made

- Keep 1675-1775 as canon.
- Do not broaden beyond writer/subclass until the current two-track Wikidata
  analysis is stable.
- Use Wikidata API enrichment rather than large multivalued SPARQL joins.
- Treat VIAF as supporting metadata, not as the current discovery source.
- Preserve `french_seed`; do not relabel it as global.
- Treat country of citizenship as one evidence field, not a complete historical
  nationality answer.
- Keep imperial categories visible because they are central to the argument.

## Open Decisions

- Review the political-entity crosswalk review matrix and decide which manual
  review rows need revision.
- Review the occupation bucket crosswalk, especially rows marked
  `needs_review`.
- Decide high/medium/low presentation thresholds for the formula-backed
  affiliation scores after inspecting the tally outputs.
- Decide whether generated `data/processed/` CSVs should be committed as
  research artifacts or regenerated locally as needed.
- Decide whether to empty ignored `outputs/` now or keep it as temporary
  historical scratch.
- Decide which language editions appear in final visualizations versus full
  analysis tables.
- Decide when to add BnF after the Wikidata tracks are stable.

## Recommended Next Cleanup

1. Review `data/processed/political_entity_crosswalk_review_matrix.csv`.
2. Continue moving hard-coded China/Qing, British, German, French, and
   geographic-scope ID mappings out of Python and into the reference table as
   the crosswalk grows.
3. Keep `outputs/` ignored and empty unless a presentation asset is actively
   being generated.
4. Keep `data/raw/cache/` ignored; delete it only for local disk cleanup.
5. Commit active scripts, docs, manifest, and selected processed outputs only
   after deciding which CSV artifacts belong in version control.
