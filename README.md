# Enlightenment Characters 1675-1775

Computational humanities pipeline for studying Enlightenment-era cultural
representation across Wikidata, VIAF, and related bibliographic systems.

## Overview

This project builds a reproducible data pipeline for a slow, inspectable
"who's who" of Enlightenment-era cultural figures. The current working scope
has two writer-centered Wikidata cohorts: the original French seed cohort and
a global writer/subclass cohort. VIAF remains supporting metadata for the
French seed; it is not the current discovery source.

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
| `data/processed/` | Versioned processed datasets and audit tables |
| `docs/` | Pipeline contract and research notes |
| `figures/` | Generated visuals |
| `notebooks/` | Analysis and visualization notebooks only |
| `outputs/` | Tables and presentation assets |
| `scripts/pipeline/` | Deterministic data pipeline steps |
| `scripts/queries/` | Query helpers and source-acquisition scripts |
| `scripts/analysis/` | Analysis helpers that consume processed data |
| `scripts/visuals/` | Figure and visualization generation scripts |

## Current Data

The repository now has two explicit cohorts declared in
`data/cohorts/cohort_manifest.csv`:

1. `french_seed`: the original French country-of-citizenship seed cohort,
   using `data/raw/18thcentury_french_writers_table.csv` plus the VIAF sidecar
   `data/raw/18thcentury_writers_wikidata_viaf.csv`.
2. `global_writers`: a reproducible Wikidata discovery cohort of humans born
   1675-1775 whose occupation is writer or a subclass of writer, using
   `data/raw/global_writers_1675_1775_discovery.csv`.

The original French-seed raw CSVs were exported manually from the Wikidata
Query Service. The exact original query texts were not preserved, so the
repository keeps reconstruction templates for provenance while the active
global discovery is scripted.

Both cohorts are enriched with Wikidata affiliation, demographic, occupation,
label-coverage, notable-work, place-context, and Wikipedia language-edition
fields. The first analysis layer builds representation, cultural-affiliation,
place-affiliation, geographic-scope, and cohort-comparison tables.

## Source Strategy

The project should expand outward in a deliberate order:

1. Keep **Wikidata** as the identity spine.
2. Preserve the French seed and global writer cohorts as separate tracks.
3. Use API-based Wikidata enrichment for both cohorts.
4. Build explicit comparison tables before adding another source family.
5. Add **BnF** as the first external comparison source only after the Wikidata
   tracks and political-entity crosswalk are stable.
6. Add domain-specific authorities only if a research question clearly needs
   them.

This source order keeps the project interpretable. It avoids introducing
several competing identity systems before the Wikidata-centered cohort has been
understood.

The fuller source-adoption rationale lives in
[docs/source_strategy.md](docs/source_strategy.md).

The current analysis roadmap lives in
[docs/analysis_roadmap.md](docs/analysis_roadmap.md).

The latest conference-script comparison lives in
[docs/conference_script_alignment.md](docs/conference_script_alignment.md).

The country-of-citizenship methodology note lives in
[docs/methodology_country_of_citizenship.md](docs/methodology_country_of_citizenship.md).

The current repo inventory and project reset lives in
[docs/project_level_set.md](docs/project_level_set.md).

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

Label correction support task

- Script: `scripts/queries/12_build_wikidata_label_corrections.py`
- Input: `data/interim/writers_cleaned.csv`
- Outputs:
  - `data/processed/person_name_label_corrections.csv`
  - `data/processed/birth_place_label_corrections.csv`
- Behavior:
  - finds rows where person names or birthplaces still look like raw Wikidata
    QIDs
  - fetches available Wikidata labels and descriptions for those QIDs
  - prefers French labels, then English labels, then other available labels
  - writes correction tables as an audit trail
- Non-goals:
  - does not modify raw data
  - does not overwrite `data/interim/writers_cleaned.csv`
  - does not deduplicate people or choose among conflicting birth data

Step 04: Merge Wikidata enrichment export

- Script: `scripts/pipeline/04_merge_wikidata_enrichment.py`
- Query helpers:
  - `scripts/queries/17_fetch_wikidata_enrichment_api.py`
  - `scripts/queries/wikidata_api.py`
- Inputs:
  - `data/interim/writers_cleaned.csv`
  - `data/raw/wikidata_affiliation_enrichment.csv`
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
  - accepts legacy CSV exports with `--input`, but the default source is the
    API-generated enrichment CSV
  - add evidence-availability flags without assigning cultural affiliation
  - add supplemental coverage flags for gender, occupations, label coverage,
    and notable-work genre evidence
  - treats ethnic group as contextual evidence rather than an affiliation-tally
    input

For the full 1675-1775 cohort, the recommended Step 04 workflow is the
API-first route. It avoids the Wikidata Query Service timeouts caused by large
multivalued SPARQL joins and produces a single raw enrichment CSV.

Cohort manifest

- Script: `scripts/pipeline/00_build_cohort_manifest.py`
- Output: `data/cohorts/cohort_manifest.csv`
- Cohorts:
  - `french_seed`: current flat-file French citizenship seed
  - `global_writers`: global writer/subclass discovery cohort

Global writer discovery

- Script: `scripts/queries/19_discover_global_writers.py`
- Output:
  - `data/raw/global_writers_1675_1775_discovery.csv`
  - `data/raw/global_writers_1675_1775_discovery_summary.csv`
- Behavior:
  - discovers humans born 1675-1775 with occupation `writer` or subclass of
    writer
  - uses year-chunked SPARQL with stable `ORDER BY ?person`, `LIMIT`, and
    `OFFSET`
  - writes the discovery cohort that later API enrichment expands

Analysis Layer 01: Representation and cultural-affiliation matrices

- Script: `scripts/analysis/01_build_representation_matrices.py`
- Input:
  - `data/interim/writers_wikidata_enriched.csv`
  - `data/processed/person_name_label_corrections.csv`
- Outputs:
  - `data/processed/representation_entities.csv`
  - `data/processed/cultural_affiliation_candidates_long.csv`
  - `data/processed/cultural_affiliation_best_candidates.csv`
  - `data/processed/cultural_affiliation_unmapped_tokens.csv`
  - `data/processed/wikipedia_representation_long.csv`
  - `data/processed/representation_language_summary.csv`
  - `data/processed/representation_by_gender.csv`
  - `data/processed/representation_by_affiliation.csv`
  - `data/processed/representation_by_place_affiliation.csv`
  - `data/processed/representation_by_occupation.csv`
  - `data/processed/wikidata_label_coverage_by_language.csv`
  - `data/processed/representation_analysis_manifest.csv`
- Behavior:
  - collapses enriched rows to one row per Wikidata entity
  - creates one row per entity per selected Wikipedia language edition
  - assigns provisional cultural-affiliation candidates from citizenship and
    language evidence
  - joins place-derived affiliation context when available
  - summarizes representation by language edition, gender, core affiliation,
    place-derived affiliation, and occupation

Place Context Layer: Place-derived affiliation evidence

- Source helper: `scripts/queries/18_fetch_wikidata_person_place_context.py`
- Analysis script: `scripts/analysis/02_build_place_affiliation_context.py`
- Raw outputs:
  - `data/raw/wikidata_person_place_context.csv`
  - `data/raw/wikidata_place_context_entities.csv`
- Processed outputs:
  - `data/processed/place_context_long.csv`
  - `data/processed/place_affiliation_candidates_long.csv`
  - `data/processed/place_affiliation_best_candidates.csv`
  - `data/processed/place_affiliation_unmapped_tokens.csv`
  - `data/processed/place_affiliation_role_summary.csv`
- Behavior:
  - recovers Wikidata place IDs for birth, death, residence, and work location
  - fetches place coordinates, country IDs, and administrative context
  - assigns provisional place-derived affiliations separately from the core
    citizenship/language score
  - reports coverage and unmapped country tokens for later curation

Analysis Layer 03: Geographic scope diagnostics

- Script: `scripts/analysis/03_build_geographic_scope_analysis.py`
- Inputs:
  - `data/interim/writers_wikidata_enriched.csv`
  - `data/processed/place_context_long.csv`
- Outputs:
  - `data/processed/geographic_scope_entity_classification.csv`
  - `data/processed/geographic_scope_summary.csv`
  - `data/processed/geographic_scope_special_context_cases.csv`
  - `data/processed/citizenship_missing_entities.csv`
- Behavior:
  - counts entities with and without Wikidata country-of-citizenship evidence
  - compares exact citizenship matches against China/Qing and British imperial
    context recovered through place evidence
  - classifies birth-place context into European, non-European or colonial,
    mixed, and transcontinental/imperial buckets for auditable geography claims

Analysis Layer 05: Formula-backed affiliation evidence

- Script: `scripts/analysis/05_build_affiliation_evidence_matrix.py`
- Inputs:
  - `data/interim/writers_wikidata_enriched.csv`
  - `data/processed/place_context_long.csv`
- Outputs:
  - `data/processed/cultural_affiliation_evidence_matrix.csv`
  - `data/processed/cultural_affiliation_evidence_best.csv`
  - `data/processed/cultural_affiliation_evidence_summary.csv`
- Behavior:
  - tallies explicit evidence fields for each person and candidate affiliation
  - reports `supporting_evidence_count / total_evidence_fields`
  - also reports support over available mapped fields so missing data stays
    visible

Analysis Layer 06: Granular occupation buckets

- Script: `scripts/analysis/06_build_occupation_bucket_tables.py`
- Reference output:
  - `data/reference/occupation_bucket_crosswalk_seed.csv`
- Processed outputs:
  - `data/processed/occupation_bucket_crosswalk_summary.csv`
  - `data/processed/occupation_bucket_entities_long.csv`
  - `data/processed/occupation_bucket_summary.csv`
  - `data/processed/occupation_bucket_language_representation.csv`
- Behavior:
  - maps occupation QIDs to reviewable granular buckets
  - keeps Religion / Theology separate from Philosophy
  - summarizes bucket coverage and language-edition representation

## Planned Pipeline Steps

| Step | Purpose |
|---|---|
| 07 | Compare with BnF |
| 08 | Optional domain-specific enrichment |

Step 03 should be reusable and re-run after major data expansions.

Step 04 stays Wikidata-centered. It adds the fields needed for later inference
and visualization before the project introduces other external authority
systems. Step 04 is implemented as a two-part workflow: fetch a Wikidata API
CSV, then merge that exported CSV back into the cohort.

The analysis layers already produce provisional cultural-affiliation,
formula-backed affiliation evidence, granular occupation-bucket, and Wikipedia
language-edition matrices, with place-derived context kept inspectable. Later
layers should add visual or network outputs without hiding the evidence fields
that support each claim.

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

To build correction tables for unresolved Wikidata labels:

```powershell
python scripts/queries/12_build_wikidata_label_corrections.py
```

This writes small versioned audit tables to `data/processed/`. The correction
tables identify readable replacement labels; they do not mutate the cleaned
cohort in place.

To fetch the Step 04 Wikidata enrichment export:

```powershell
python scripts/queries/17_fetch_wikidata_enrichment_api.py
```

This writes `data/raw/wikidata_affiliation_enrichment.csv` and uses an ignored
local cache under `data/raw/cache/` so interrupted API runs can resume.

Then run:

```powershell
python scripts/pipeline/04_merge_wikidata_enrichment.py
```

Legacy CSV exports can still be merged explicitly:

```powershell
python scripts/pipeline/04_merge_wikidata_enrichment.py --input path/to/export.csv
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
- Keep unresolved-label correction tables as a documented audit trail.
- Make richer Wikidata enrichment reproducible before adding additional
  authority systems.
