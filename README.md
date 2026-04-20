# Enlightenment Characters 1675-1775

Computational humanities pipeline for studying Enlightenment-era cultural
representation across Wikidata, VIAF, and related bibliographic systems.

## Overview

This project builds a reproducible data pipeline for a slow, inspectable
"who's who" of Enlightenment-era cultural figures. The current pilot dataset
starts with writer-like Wikidata query exports and VIAF identifiers, but the
broader research direction includes people associated with literature, music,
opera, poetry, letters, sculpture, painting, and adjacent cultural work.

The project is inspired by the difference between simply counting cultural
records and using computational methods to transform, model, and compare those
records. In this repository:

- Quantitative work means descriptive counts and proportions.
- Qualitative work means interpretive judgment about historical and cultural meaning.
- Computational work means a reproducible sequence of transformations,
  classifications, comparisons, and models that can produce new analyzable
  structures from source data.

The line between those categories is not fixed. This project treats the
pipeline itself as part of the research method, and it keeps interpretive
decisions visible instead of hiding them inside one-off cleaning steps.

## Research Questions

Current and planned questions include:

- How are Enlightenment-era cultural figures represented across Wikidata,
  Wikipedia, VIAF, and related sources?
- Which kinds of people appear when the project starts from writer-like
  occupations, and which figures are missing or mislabeled?
- How do language, geography, occupation, and bibliographic authority data
  shape the apparent Enlightenment "network"?
- Can language-edition representation become an influence map, for example by
  comparing which national or linguistic groups appear in French versus Italian
  or other Wikipedia/Wikidata contexts?
- How do Wikidata-derived proportions compare with BnF-derived or other
  bibliographic datasets?

## Pipeline Concept

This is not a single static dataset. It is an ordered data transformation pipeline:

```text
raw data
-> merge
-> clean
-> diagnose
-> normalize
-> enrich
-> analysis datasets
-> visualizations
```

Each pipeline step should:

- consume one or more documented input files
- produce one or more documented output files
- do one clearly defined task
- avoid in-place mutation
- preserve uncertainty unless a later step explicitly resolves it

## Repository Structure

```text
Chomputation/
|-- README.md
|-- requirements.txt
|-- .gitignore
|-- data/
|   |-- raw/
|   |-- interim/
|   `-- processed/
|-- docs/
|   `-- pipeline.md
|-- figures/
|-- notebooks/
|-- outputs/
`-- scripts/
    |-- analysis/
    |-- pipeline/
    |-- queries/
    `-- visuals/
```

Directory roles:

| Directory | Purpose |
|---|---|
| `data/raw/` | Immutable source exports |
| `data/interim/` | Temporary pipeline outputs, not versioned |
| `data/processed/` | Final clean datasets |
| `docs/` | Pipeline contract and research notes |
| `figures/` | Generated visuals |
| `notebooks/` | Analysis and visualization notebooks only |
| `outputs/` | Tables and presentation assets |
| `scripts/pipeline/` | Deterministic data pipeline steps |
| `scripts/queries/` | Query helpers and source-acquisition scripts |
| `scripts/analysis/` | Analysis helpers that consume processed data |
| `scripts/visuals/` | Figure and visualization generation scripts |

## Current Data

Two primary raw exports are currently used:

1. A Wikidata geography export with entity IDs, names, birth years,
   birthplaces, coordinates, and raw occupation labels.
2. A Wikidata/VIAF export with entity IDs, full birth dates, and VIAF
   identifiers.

These pilot raw CSVs were originally exported manually from the Wikidata Query
Service by a collaborator. The exact original query texts were not preserved,
so the repository now distinguishes between the original manual exports and the
reconstructed current-cohort query templates that support future
reproducibility.

The current cohort should be treated as a 1675-1775 pilot dataset. It is not
yet the final broad Enlightenment character dataset.

The current pilot is sufficient for merge, clean, and diagnostic work, but it
does not yet contain all the fields needed for a cultural-affiliation matrix or
for language-edition influence analysis. Those later steps will require more
Wikidata fields before new authority systems are added.

## Source Strategy

The project should expand outward in a deliberate order:

1. Keep **Wikidata** as the identity spine.
2. Diagnose the current pilot dataset before changing its scope.
3. Add a richer **Wikidata enrichment** export with affiliation and
   representation fields such as death place, citizenship, residence or work
   location, language fields, and Wikipedia language-edition presence.
4. Re-run diagnostics on the enriched Wikidata-centered cohort.
5. Add **BnF** as the first external comparison source.
6. Add domain-specific authorities only if a research question clearly needs
   them.

This source order keeps the project interpretable. It avoids introducing
several competing identity systems before the Wikidata-centered cohort has been
understood.

The fuller source-adoption rationale lives in
[docs/source_strategy.md](docs/source_strategy.md).

## Pipeline Definition

The operational contract is [docs/pipeline.md](docs/pipeline.md).

Codex and human contributors should treat that file as the source of truth for:

- what each pipeline step does
- what each pipeline step reads
- what each pipeline step writes
- what each pipeline step must not do

## Current Pipeline Steps

Step 01: Build merged dataset

- Script: `scripts/pipeline/01_build_merged_dataset.py`
- Input: raw Wikidata geography and VIAF CSVs
- Outputs:
  - `data/interim/writers_merged.csv`
  - `data/interim/viaf_conflicts.csv`
- Behavior:
  - left join on `wikidata_id`
  - preserve geography rows
  - summarize VIAF candidates without inflating rows
  - flag conflicting VIAF metadata for later diagnostics

Step 02: Clean structural fields

- Script: `scripts/pipeline/02_clean_structural_fields.py`
- Input: `data/interim/writers_merged.csv`
- Outputs:
  - `data/interim/writers_cleaned.csv`
  - `data/interim/duplicate_wikidata_ids.csv`
- Behavior:
  - normalize blank values and basic types
  - parse `Point(lon lat)` coordinates
  - parse unambiguous birth dates
  - flag duplicate Wikidata IDs, unresolved QID labels, strict birth-year
    mismatches, birth-year candidate ambiguity, rows without candidate
    support, and VIAF ambiguity
- Non-goals:
  - no deduplication
  - no ontology decisions
  - no row removal

Step 03: Dataset diagnostics

- Script: `scripts/pipeline/03_diagnose_dataset.py`
- Input:
  - `data/interim/writers_cleaned.csv`
  - `data/interim/viaf_conflicts.csv`
- Outputs:
  - `data/interim/dataset_diagnostics_manifest.csv`
  - `data/interim/dataset_diagnostics_summary.csv`
  - `data/interim/dataset_diagnostics_missingness.csv`
  - `data/interim/dataset_diagnostics_occupation_distribution.csv`
  - `data/interim/dataset_diagnostics_birth_place_distribution.csv`
  - `data/interim/dataset_diagnostics_duplicate_entities.csv`
  - `data/interim/dataset_diagnostics_viaf_conflict_entities.csv`
  - `data/interim/dataset_diagnostics_report.md`
- Expected focus:
  - row and entity counts
  - duplicate-entity counts, including rows in duplicate entities and surplus
    duplicate rows
  - VIAF coverage and VIAF conflict summaries
  - occupation distribution
  - birthplace and coordinate coverage
  - disagreement and ambiguity summaries
  - diagnostic outputs that guide later enrichment and normalization
  - CSV tables as the canonical diagnostic format, with Markdown limited to a
    short orientation note

Step 04: Merge Wikidata enrichment export

- Script: `scripts/pipeline/04_merge_wikidata_enrichment.py`
- Query helpers:
  - `scripts/queries/01_export_wikidata_values_block.py`
  - `scripts/queries/11_run_wikidata_query_batch.py`
  - `scripts/queries/02_wikidata_affiliation_enrichment.rq`
  - `scripts/queries/05_wikidata_affiliation_death_place.rq`
  - `scripts/queries/06_wikidata_affiliation_languages.rq`
  - `scripts/queries/07_wikidata_wikipedia_representation.rq`
  - `scripts/queries/08_wikidata_affiliation_residence.rq`
  - `scripts/queries/09_wikidata_affiliation_work_location.rq`
  - `scripts/queries/10_wikidata_affiliation_citizenship.rq`
- Inputs:
  - `data/interim/writers_cleaned.csv`
  - `data/raw/wikidata_affiliation_enrichment.csv`, or one or more CSV
    exports or export folders passed with `--input`
- Outputs:
  - `data/interim/writers_wikidata_enriched.csv`
  - `data/interim/wikidata_enrichment_missing_entities.csv`
  - `data/interim/wikidata_enrichment_extra_entities.csv`
  - `data/interim/wikidata_enrichment_duplicate_rows.csv`
  - `data/interim/wikidata_enrichment_summary.csv`
  - `data/interim/wikidata_enrichment_field_coverage.csv`
- Behavior:
  - merge richer Wikidata fields back onto the current cohort
  - preserve multi-valued affiliation evidence as pipe-delimited fields
  - summarize chunked export duplicates rather than multiplying cohort rows
  - accept split query families and fill absent columns with `NA` until all
    evidence families have been merged
  - add evidence-availability flags without assigning cultural affiliation

For the full 1675-1775 cohort, the recommended Step 04 workflow is the split
query family approach. The all-in-one enrichment query is still kept in the
repository for inspection and for small-cohort tests, but it is more likely to
time out on the full cohort because multivalued properties can create very
large intermediate joins.

## Planned Pipeline Steps

| Step | Purpose |
|---|---|
| 05 | Re-run diagnostics and normalize occupation or cohort categories |
| 06 | Cultural-affiliation evidence matrix |
| 07 | Compare with BnF |
| 08 | Geography and network analysis datasets |
| 09 | Optional domain-specific enrichment |

Step 03 should be reusable and re-run after major data expansions.

Step 04 stays Wikidata-centered. It adds the fields needed for later inference
and visualization before the project introduces other external authority
systems. Because Wikidata Query Service exports are created outside the local
pipeline, Step 04 is implemented as a two-part workflow: generate query files,
then merge the exported CSV back into the cohort.

Step 06 is expected to produce a "punch card" style matrix for each person.
Candidate affiliations can be scored by checking which evidence fields support
them, such as birthplace, death place, citizenship, residence or work location,
language, and Wikipedia language-edition representation. The matrix should keep
two ideas separate: the affiliation score itself and the completeness of the
available evidence.

Step 07 should introduce BnF as the first external comparator. Only after that
should the project decide whether sources such as CERL, Getty, MusicBrainz, or
Europeana are necessary.

## How to Run

From the project root:

```powershell
python scripts/pipeline/01_build_merged_dataset.py
python scripts/pipeline/02_clean_structural_fields.py
python scripts/pipeline/03_diagnose_dataset.py
```

Each script reads from `data/`, writes to `data/`, and prints a summary. No
script should depend on notebook execution.

To prepare the Step 04 enrichment query:

```powershell
python scripts/queries/01_export_wikidata_values_block.py
```

This writes:

- `outputs/wikidata_cohort_values_block.txt`
- `outputs/wikidata_affiliation_enrichment_query.rq`
- `outputs/wikidata_affiliation_queries/`
- `outputs/wikidata_affiliation_death_place_query.rq`
- `outputs/wikidata_affiliation_death_place_queries/`
- `outputs/wikidata_affiliation_residence_query.rq`
- `outputs/wikidata_affiliation_residence_queries/`
- `outputs/wikidata_affiliation_work_location_query.rq`
- `outputs/wikidata_affiliation_work_location_queries/`
- `outputs/wikidata_affiliation_languages_query.rq`
- `outputs/wikidata_affiliation_languages_queries/`
- `outputs/wikidata_affiliation_citizenship_query.rq`
- `outputs/wikidata_affiliation_citizenship_queries/`
- `outputs/wikidata_wikipedia_representation_query.rq`
- `outputs/wikidata_wikipedia_representation_queries/`
- `outputs/wikidata_current_cohort_geography_query.rq`
- `outputs/wikidata_current_cohort_geography_queries/`
- `outputs/wikidata_current_cohort_viaf_query.rq`
- `outputs/wikidata_current_cohort_viaf_queries/`

For the full 1675-1775 cohort, prefer the split Step 04 query families and
their chunked files. The single combined enrichment query is mainly a
reference template and a small-cohort convenience query.

You can also run a generated query file directly:

```powershell
python scripts/queries/00_run_wikidata_sparql_query.py `
  outputs/wikidata_affiliation_enrichment_query.rq `
  data/raw/wikidata_affiliation_enrichment.csv
```

Or export a whole split query family into a folder:

```powershell
python scripts/queries/11_run_wikidata_query_batch.py `
  outputs/wikidata_affiliation_languages_queries `
  data/raw/wikidata_affiliation_languages_parts `
  --skip-existing
```

Save a single combined export as:

- `data/raw/wikidata_affiliation_enrichment.csv`

Then run:

```powershell
python scripts/pipeline/04_merge_wikidata_enrichment.py
```

If you ran chunked queries and saved multiple CSV files in one folder, pass the
folder path:

```powershell
python scripts/pipeline/04_merge_wikidata_enrichment.py --input data/raw/wikidata_affiliation_parts
```

If you ran the recommended split Step 04 query families, pass each family
folder:

```powershell
python scripts/pipeline/04_merge_wikidata_enrichment.py `
  --input data/raw/wikidata_affiliation_death_place_parts `
  --input data/raw/wikidata_affiliation_residence_parts `
  --input data/raw/wikidata_affiliation_work_location_parts `
  --input data/raw/wikidata_affiliation_languages_parts `
  --input data/raw/wikidata_affiliation_citizenship_parts `
  --input data/raw/wikidata_wikipedia_representation_parts
```

## Development Rules

These rules apply to human and Codex contributions:

1. One pipeline step equals one script.
2. Do not mutate raw files in place.
3. Never modify files in `data/raw/`.
4. Do not drop rows without explicit documentation.
5. Keep cleaning, normalization, and interpretation separate.
6. Preserve ambiguity until a later step defines how to resolve it.
7. Scripts must be deterministic: same input, same output.
8. Dependencies should remain minimal and explicit.

## Environment Setup

```powershell
pip install -r requirements.txt
```

## Status

Current cleanup focus:

- Align the repository with the 1675-1775 research scope.
- Preserve VIAF ambiguity explicitly.
- Diagnose the current pilot dataset before expanding interpretation.
- Make richer Wikidata enrichment reproducible before adding additional
  authority systems.
