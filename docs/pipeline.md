# Data Pipeline Overview

This project constructs a reproducible research pipeline for Enlightenment-era
cultural figures from 1675-1775. The current pilot data begins with writer-like
Wikidata query exports and VIAF identifiers. Later steps may expand the cohort
toward a broader "who's who" of Enlightenment figures across literature, music,
opera, letters, painting, sculpture, and adjacent cultural work.

The pipeline is designed to move from quantitative description toward
computational analysis. Counts matter, but the central method is a documented
sequence of data transformations, diagnostics, classifications, comparisons,
and models.

## Current Pipeline Steps

1. **Step 01 - Build Merged Dataset**
   Combine Wikidata geography and VIAF exports while preserving source ambiguity.

2. **Step 02 - Clean Structural Fields**
   Normalize basic types, parse coordinates, and flag quality issues without interpretive cleanup.

3. **Step 03 - Dataset Diagnostics**
   Analyze duplicates, VIAF ambiguity, occupation distribution, and data completeness.

4. **Step 04 - Expand Wikidata Affiliation and Representation Fields**
   Merge richer Wikidata evidence fields back onto the current cohort.

5. **Analysis Layer 01 - Representation and Cultural-Affiliation Matrices**
   Collapse the enriched cohort to entity-level analysis tables and
   language-edition comparison matrices.

6. **Place Context Layer - Place-Derived Affiliation Evidence**
   Recover birth, death, residence, and work-location country context.

7. **Analysis Layer 03 - Geographic Scope Diagnostics**
   Count missing citizenship and classify European, non-European or colonial,
   mixed, and transcontinental birth-place context.

8. **Analysis Layer 05 - Formula-Backed Affiliation Evidence**
   Tally explicit evidence fields for each candidate affiliation.

9. **Analysis Layer 06 - Granular Occupation Buckets**
   Build reviewable occupation buckets and language-edition representation
   summaries.

10. **Context Slice Layer - Reproducible Crosswalk Slices**
    Derive France, Germany, British, and China/Qing slices from the global
    cohort and political-entity crosswalk.

11. **Core Findings Packet**
    Generate a compact report and supporting CSVs from the existing processed
    evidence tables.

12. **Visualization Layer - Findings Visuals** *(planned)*

Between Step 03 and Step 04, the repository also includes a non-numbered
label-correction helper that builds auditable correction tables for unresolved
Wikidata QID labels.

## Cohort Tracks

The repository now has one active analytical corpus track and one legacy
provenance track:

- `french_seed`: the legacy flat-file French-facing seed, with VIAF merged as
  supporting metadata.
- `global_writers`: the global Wikidata writer/subclass cohort born
  1675-1775, discovered without a country-of-citizenship filter.

The machine-readable manifest is:

`data/cohorts/cohort_manifest.csv`

Most current scripts accept `--cohort-id`. The default is `global_writers`;
pass `--cohort-id french_seed` only when refreshing legacy/provenance outputs.
Global outputs are written under:

```text
data/interim/global_writers/
data/processed/global_writers/
data/raw/global_writers/
```

The global discovery entry point is:

```powershell
python scripts/queries/19_discover_global_writers.py
```

It writes `data/raw/global_writers_1675_1775_discovery.csv`. The implemented
query discovers humans born 1675-1775 with occupation writer or a subclass of
writer. It currently returns 14,377 distinct Wikidata entities, which differs
from the script's 18,697 table count and is documented in
`docs/conference_script_alignment.md`.

The original French seed is retained for provenance and backward
compatibility, but it is no longer needed as a discovery source. Every legacy
French-seed QID is present in `global_writers`. New France/Germany/British/
China comparisons should use the reproducible context-slice layer instead.

## Core Rules

- One pipeline step equals one script.
- Raw data is immutable.
- No step should overwrite its input file.
- No step should drop rows without explicit documentation.
- Cleaning, normalization, and interpretation are separate concerns.
- Ambiguity should be preserved until a later step explicitly resolves it.
- Scripts must be deterministic.

## Step 01 - Build Merged Dataset

Script:

`scripts/pipeline/01_build_merged_dataset.py`

### Purpose

Combine the two current raw Wikidata exports into a single working table. This
step creates the first unified dataset containing geography fields and
VIAF-derived bibliographic identifiers.

This step does not clean or interpret the records. It also avoids silently
choosing among conflicting VIAF values.

### Inputs

Geographic dataset:

`data/raw/18thcentury_french_writers_table.csv`

This SPARQL-style export contains a metadata row followed by the real header.
The script reads it with `header=1`.

Current provenance note:

- The pilot file was exported manually from the Wikidata Query Service by a
  project collaborator.
- The exact original query text was not preserved in the repository.
- The repository now includes a reconstructed current-cohort query template for
  future re-export and auditing.

Fields used:

- `person`
- `personLabel`
- `birthYear`
- `birthPlaceLabel`
- `coords`
- `occupations`

VIAF dataset:

`data/raw/18thcentury_writers_wikidata_viaf.csv`

This SPARQL-style export also contains a metadata row followed by the real
header. The script reads it with `header=1`.

Current provenance note:

- The pilot file was exported manually from the Wikidata Query Service by a
  project collaborator.
- The exact original query text was not preserved in the repository.
- The repository now includes a reconstructed current-cohort query template for
  future re-export and auditing.

Fields used:

- `person`
- `birth`
- `viaf`

### Transformations

Step 01 renames raw columns into the shared pipeline schema:

| Raw column | Pipeline column |
|---|---|
| `person` | `wikidata_id` |
| `personLabel` | `name` |
| `birthYear` | `birth_year` |
| `birthPlaceLabel` | `birth_place` |
| `coords` | `coords` |
| `occupations` | `occupation_raw` |
| `birth` | `birth_date` |
| `viaf` | `viaf_id` |

The geography export remains the base table. The merge key is `wikidata_id`.

VIAF data can contain multiple rows for the same `wikidata_id`. Some of those
rows contain conflicting birth dates or multiple VIAF identifiers. To keep the
main dataset from being inflated by a many-to-many merge, Step 01 summarizes
VIAF records to one row per `wikidata_id` before merging:

- `birth_date` is populated only when there is exactly one distinct non-null VIAF birth date.
- `viaf_id` is populated only when there is exactly one distinct non-null VIAF identifier.
- `birth_date_candidates` stores all distinct VIAF birth-date candidates.
- `viaf_id_candidates` stores all distinct VIAF identifier candidates.
- `viaf_record_count` stores the number of VIAF rows for that entity.
- `viaf_birth_date_count` stores the number of distinct non-null VIAF birth dates.
- `viaf_id_count` stores the number of distinct non-null VIAF IDs.
- `viaf_has_conflict` is true when there is more than one distinct birth date or VIAF ID.

This is a structural compromise: it preserves the row count of the geography
dataset and makes VIAF ambiguity visible for diagnostics instead of selecting a
"best" value too early.

### Outputs

Merged dataset:

`data/interim/writers_merged.csv`

VIAF conflict sidecar:

`data/interim/viaf_conflicts.csv`

The conflict sidecar contains raw VIAF rows for ambiguous `wikidata_id` values
that also appear in the geography export. It is for inspection and diagnostics,
not for direct analysis.

### Known Issues Preserved

- Duplicate Wikidata IDs in the geography export
- Conflicting birth years
- Conflicting VIAF dates or identifiers
- Coordinates stored as `Point(lon lat)` strings
- Mixed occupation labels
- Missing VIAF identifiers
- Unresolved entity labels returned as QIDs

## Step 02 - Clean Structural Fields

Script:

`scripts/pipeline/02_clean_structural_fields.py`

### Purpose

Normalize the merged dataset so that fields have consistent formats and are
suitable for diagnostics and downstream analysis.

This step performs structural cleaning only. It does not decide which duplicate
entity row is correct, which occupation category should dominate, or which
nationality/cultural affiliation should be assigned.

No rows are removed.

### Input

`data/interim/writers_merged.csv`

### Transformations

1. Normalize blank values.

Empty strings and whitespace-only cells are converted to `NA`.

2. Standardize basic types.

Fields are coerced into predictable types:

| Field | Type |
|---|---|
| `wikidata_id` | string |
| `name` | string |
| `birth_place` | string |
| `coords` | string |
| `occupation_raw` | string |
| `birth_date` | string after datetime parsing |
| `birth_date_candidates` | string |
| `viaf_id` | string |
| `viaf_id_candidates` | string |
| `birth_year` | nullable integer |
| `viaf_record_count` | nullable integer |
| `viaf_birth_date_count` | nullable integer |
| `viaf_id_count` | nullable integer |
| `viaf_has_conflict` | boolean |

VIAF identifiers remain strings to prevent integer coercion, truncation, or scientific notation.

3. Parse geographic coordinates.

The Wikidata coordinate field uses the form:

```text
Point(longitude latitude)
```

Step 02 parses this into:

| Column | Description |
|---|---|
| `birth_lon` | longitude |
| `birth_lat` | latitude |

The original `coords` field is retained for traceability.

4. Parse unambiguous birth dates.

When `birth_date` contains one unambiguous value, it is parsed into a datetime
value and used to derive:

`birth_year_from_date`

Rows with ambiguous VIAF birth-date candidates keep their candidates, but they
do not receive an arbitrary scalar `birth_date`.

5. Detect birth-year mismatches.

Rows are flagged when the geography birth year and unambiguous VIAF-derived birth year disagree:

`birth_year_mismatch = True`

Rows are also flagged when any VIAF birth-date candidate disagrees with the geography birth year:

`birth_year_candidate_ambiguity_exists = True`

Rows are separately flagged when no VIAF birth-year candidate supports the
geography birth year:

`birth_year_without_candidate_support = True`

6. Detect unresolved labels.

Rows are flagged when the writer name or birthplace label still looks like a raw Wikidata QID:

`Q#####`

Diagnostic columns:

| Column | Meaning |
|---|---|
| `name_is_qid` | writer label appears unresolved |
| `birth_place_is_qid` | birthplace label appears unresolved |

7. Detect duplicate Wikidata entities.

Duplicate records are detected by `wikidata_id`.

Diagnostic columns:

| Column | Meaning |
|---|---|
| `has_duplicate_wikidata_id` | row belongs to a duplicated entity |
| `duplicate_row_count` | number of rows for that entity |

Duplicates are preserved intentionally.

### Outputs

Cleaned dataset:

`data/interim/writers_cleaned.csv`

Duplicate entity sidecar:

`data/interim/duplicate_wikidata_ids.csv`

The duplicate file allows targeted inspection of ambiguous Wikidata entities.

## Step 03 - Dataset Diagnostics

Step 03 should quantify dataset quality and structure without resolving interpretive questions.

Script:

`scripts/pipeline/03_diagnose_dataset.py`

This step should be reusable. It should be run on the current pilot dataset,
and then re-run after any major data expansion such as Wikidata enrichment or a
new external comparison source.

Expected diagnostics:

- total row count
- distinct `wikidata_id` count
- duplicate row and duplicate entity counts
- VIAF coverage
- VIAF ambiguity counts and examples
- birth-date coverage
- birth-year mismatch counts
- birth-year candidate ambiguity counts
- rows without candidate support for the geography birth year
- unresolved label counts
- coordinate coverage
- occupation distribution
- birthplace distribution
- missing-value summary

Likely outputs:

- a machine-readable diagnostic table or tables
- a compact human-readable summary suitable for collaborators
- no changes to the cleaned dataset itself

Current outputs:

- `data/interim/dataset_diagnostics_manifest.csv`
- `data/interim/dataset_diagnostics_summary.csv`
- `data/interim/dataset_diagnostics_missingness.csv`
- `data/interim/dataset_diagnostics_occupation_distribution.csv`
- `data/interim/dataset_diagnostics_birth_place_distribution.csv`
- `data/interim/dataset_diagnostics_duplicate_entities.csv`
- `data/interim/dataset_diagnostics_viaf_conflict_entities.csv`
- `data/interim/dataset_diagnostics_report.md`

The diagnostic CSV files are the canonical outputs. The Markdown report is an
orientation note only; it should not duplicate the full CSV tables.

Duplicate reporting should distinguish between:

- rows that belong to duplicated entities
- surplus duplicate rows beyond the first row per entity

## Label Correction Helper - Unresolved Wikidata Labels

This helper creates audit tables for rows where Step 02 found labels that still
look like raw Wikidata QIDs.

Script:

`scripts/queries/12_build_wikidata_label_corrections.py`

This is intentionally documented as a helper rather than a numbered pipeline
step. It fetches labels from Wikidata and writes small correction tables, but
it does not transform the cohort or resolve interpretive ambiguity.

### Purpose

Some person names and birth-place labels in the original French-facing
Wikidata export were returned as raw QIDs, usually because no French label was
available for the entity at export time. The helper queries Wikidata for
available labels and descriptions so those unresolved labels can be repaired
later in a documented way.

### Input

`data/interim/writers_cleaned.csv`

Fields used:

- `wikidata_id`
- `name`
- `name_is_qid`
- `birth_year`
- `birth_place`
- `birth_place_is_qid`

### Transformations

The helper:

- collects QIDs from unresolved person-name rows
- collects QIDs from unresolved birth-place rows
- queries the Wikidata entity API for labels and descriptions
- chooses a readable label using the language order:

```text
fr, en, de, it, es, nl, pl, ru, pt, sv, da, la, then any available label
```

The chosen label and its language are recorded. French and English labels and
descriptions are also recorded when available.

### Outputs

Person-name correction table:

`data/processed/person_name_label_corrections.csv`

Birth-place correction table:

`data/processed/birth_place_label_corrections.csv`

These are versioned audit tables. They identify replacement labels, but they
do not overwrite `data/interim/writers_cleaned.csv`.

### Non-goals

- no raw-data edits
- no in-place mutation of the cleaned cohort
- no deduplication
- no choice among conflicting birth years or birth places

## Step 04 - Expand Wikidata Affiliation and Representation Fields

Step 04 remains Wikidata-centered. The goal is to enrich the current pilot
dataset with the fields required for affiliation scoring and for
language-edition or influence analysis before bringing in additional authority
systems.

Step 04 has one API source-acquisition step and one deterministic merge step.
The Wikidata entity API fetch is run outside the local merge because it is a
web service, but the output CSV is merged locally in a reproducible way.

For the current 1675-1775 cohort, the operational workflow should prefer the
API route over generated SPARQL query families. Multivalued properties such as
citizenship, residence, work location, occupation, and language can create
large cross-products inside SPARQL joins. The entity API lets the project fetch
one person entity at a time, cache results, and resume interrupted runs.

Active query artifacts:

- `scripts/queries/wikidata_api.py`
- `scripts/queries/17_fetch_wikidata_enrichment_api.py`

Legacy SPARQL templates remain in `scripts/queries/` for provenance,
reconstruction, and small diagnostic checks, but they are no longer the
recommended full-cohort Step 04 workflow.

Merge script:

`scripts/pipeline/04_merge_wikidata_enrichment.py`

### Inputs

Base cohort:

`data/interim/writers_cleaned.csv`

Default Wikidata enrichment export:

`data/raw/wikidata_affiliation_enrichment.csv`

Create it with:

```powershell
python scripts/queries/17_fetch_wikidata_enrichment_api.py
```

The merge script also accepts one or more explicit legacy CSV exports, or one
or more directories of CSV exports, with `--input`.

### Enrichment Fields

The first enrichment pass targets:

| Field | Likely use |
|---|---|
| Place of death | affiliation evidence |
| Country of citizenship | affiliation evidence |
| Residence | affiliation evidence |
| Work location | affiliation evidence |
| Native language | affiliation evidence |
| Languages spoken, written, or signed | affiliation evidence |
| Writing language | affiliation evidence |
| Wikipedia language-edition sitelinks | representation and influence analysis |
| Sex or gender | representation axis |
| Ethnic group | contextual evidence only |
| Occupation | writerly and non-writing professional axes |
| Labels and descriptions by language | Wikidata representation and data-friction evidence |
| Notable-work genres/forms | sparse literary-genre evidence |

### Transformations

Step 04:

- normalizes Wikidata IDs from either full URI or `Q#####` form
- preserves multi-valued fields as pipe-delimited strings
- summarizes duplicate raw export rows without multiplying cohort rows
- fills missing expected columns with `NA` so legacy exports can still be
  merged into one evidence table
- flags which evidence field families are available per row
- counts evidence-field availability without assigning cultural affiliation
- records missing, extra, and duplicate enrichment entities for inspection

This step does not determine nationality or cultural affiliation. It prepares
the evidence grid that the first analysis layer can use.

The API enrichment fetcher targets death place, citizenship, residence, work
location, language evidence, Wikipedia representation, identity and demographic
evidence, occupation profiles, Wikidata label coverage, and notable-work
genres/forms in a single CSV.

The separate place-context helper can recover coordinates and country context
when geography analysis needs them.

### Outputs

Enriched cohort:

`data/interim/writers_wikidata_enriched.csv`

Sidecars and diagnostics:

- `data/interim/wikidata_enrichment_missing_entities.csv`
- `data/interim/wikidata_enrichment_extra_entities.csv`
- `data/interim/wikidata_enrichment_duplicate_rows.csv`
- `data/interim/wikidata_enrichment_summary.csv`
- `data/interim/wikidata_enrichment_field_coverage.csv`

## Analysis Layer 01 - Representation and Cultural-Affiliation Matrices

Script:

`scripts/analysis/01_build_representation_matrices.py`

### Purpose

Build the first interpretation-ready tables from the enriched Wikidata cohort.
This layer keeps the 1675-1775 pilot scope, collapses duplicate rows to one row
per Wikidata entity, and compares entity representation across the selected
European Wikipedia language editions.

The cultural-affiliation logic is provisional and auditable. It counts mapped
citizenship and language evidence, reports support fields, and leaves unmapped
or context-only tokens in a sidecar for later curation.

Place-derived affiliation context is built separately and then joined into the
entity and Wikipedia representation tables. It is not folded into the core
citizenship/language support count in this layer.

Ethnic group remains contextual evidence only and is not included in the
affiliation score.

### Inputs

Enriched cohort:

`data/interim/writers_wikidata_enriched.csv`

Place context, when available:

`data/processed/place_affiliation_best_candidates.csv`

Name-correction table:

`data/processed/person_name_label_corrections.csv`

### Outputs

Entity and affiliation files:

- `data/processed/representation_entities.csv`
- `data/processed/cultural_affiliation_candidates_long.csv`
- `data/processed/cultural_affiliation_best_candidates.csv`
- `data/processed/cultural_affiliation_unmapped_tokens.csv`

Representation matrices and summaries:

- `data/processed/wikipedia_representation_long.csv`
- `data/processed/representation_language_summary.csv`
- `data/processed/representation_by_gender.csv`
- `data/processed/representation_by_affiliation.csv`
- `data/processed/representation_by_place_affiliation.csv`
- `data/processed/representation_by_occupation.csv`
- `data/processed/wikidata_label_coverage_by_language.csv`
- `data/processed/representation_analysis_manifest.csv`

### Current Language Editions

`frwiki`, `enwiki`, `dewiki`, `itwiki`, `eswiki`, `plwiki`, `ruwiki`,
`ukwiki`, `nlwiki`, `ptwiki`, `svwiki`, and `dawiki`.

## Place Context Layer - Place-Derived Affiliation Evidence

Source helper:

`scripts/queries/18_fetch_wikidata_person_place_context.py`

Analysis script:

`scripts/analysis/02_build_place_affiliation_context.py`

### Purpose

Recover place IDs and country or administrative context for birth, death,
residence, and work-location evidence. The original pilot geography export
stored birth-place labels and coordinates but not birth-place QIDs, so this
layer uses the Wikidata entity API to recover place claims from the person
entities directly.

The resulting affiliation assignments are provisional and contextual. They are
useful for geography-aware comparison and for identifying cosmopolitan or
ambiguous cases, but they remain separate from the core citizenship/language
affiliation score.

### Inputs

Enriched cohort:

`data/interim/writers_wikidata_enriched.csv`

### Raw Outputs

- `data/raw/wikidata_person_place_context.csv`
- `data/raw/wikidata_place_context_entities.csv`

### Processed Outputs

- `data/processed/place_context_long.csv`
- `data/processed/place_affiliation_candidates_long.csv`
- `data/processed/place_affiliation_best_candidates.csv`
- `data/processed/place_affiliation_unmapped_tokens.csv`
- `data/processed/place_affiliation_role_summary.csv`

### Current Place Roles

- birth place
- death place
- residence
- work location

## Analysis Layer 03 - Geographic Scope Diagnostics

Script:

`scripts/analysis/03_build_geographic_scope_analysis.py`

### Purpose

Answer geography-facing research questions without hiding the evidence choice.
This layer treats country of citizenship as one signal and birth-place context
as a separate geographic signal. That distinction matters for historical
empires and colonies: China/Qing and British imperial contexts may be absent
from citizenship but visible in place evidence.

The Europe/non-Europe rollup is conservative. It classifies birth-place context
as European, non-European or colonial, mixed, transcontinental/imperial, or
unmapped instead of forcing every historical state into a binary bucket.

### Inputs

- `data/interim/writers_wikidata_enriched.csv`
- `data/processed/place_context_long.csv`

### Outputs

- `data/processed/geographic_scope_entity_classification.csv`
- `data/processed/geographic_scope_summary.csv`
- `data/processed/geographic_scope_special_context_cases.csv`
- `data/processed/citizenship_missing_entities.csv`

## Analysis Layer 05 - Formula-Backed Affiliation Evidence

Script:

`scripts/analysis/05_build_affiliation_evidence_matrix.py`

### Purpose

Build an affiliation evidence matrix where every score is a visible tally. The
default total is eight evidence fields: citizenship, native language,
spoken/written language, writing language, birth place, death place, residence,
and work location.

The output includes both a conservative denominator and an available-data
denominator:

```text
score_over_total_fields = supporting_evidence_count / total_evidence_fields
score_over_available_fields = supporting_evidence_count / available_mapped_evidence_count
```

This lets collaborators debate later high/medium/low presentation thresholds
without hiding the calculation.

### Inputs

- `data/interim/writers_wikidata_enriched.csv`
- `data/processed/place_context_long.csv`

### Outputs

- `data/processed/cultural_affiliation_evidence_matrix.csv`
- `data/processed/cultural_affiliation_evidence_best.csv`
- `data/processed/cultural_affiliation_evidence_summary.csv`

## Source Scope Strategy

The current project phase should not widen its source base. It should use the
existing Wikidata/Wikipedia evidence spine:

1. Keep **Wikidata** as the main identity spine.
2. Use `global_writers` as the active analytical cohort.
3. Derive all national and imperial context slices from the same crosswalk.
4. Keep Wikipedia language-edition sitelinks as representation evidence.
5. Keep BnF and other external authority systems out of scope for now.

This trim is methodological as well as technical. It keeps the cohort
interpretable and avoids premature identity fragmentation.

See also:

`docs/source_strategy.md`

## Analysis Layer 06 - Granular Occupation Buckets

Script:

`scripts/analysis/06_build_occupation_bucket_tables.py`

### Purpose

Build a reviewable occupation-bucket layer from the raw Wikidata API
occupation profile. This layer keeps writerly and non-writerly work visible,
adds Religion / Theology as a first-class bucket, and writes both a crosswalk
seed and cohort-specific bucket summaries.

The buckets are deliberately granular enough for conference visualizations but
still auditable: uncertain or unmapped occupations remain visible instead of
being silently folded into broad categories.

### Inputs

- `data/raw/*/wikidata_affiliation_enrichment.csv` or the cohort equivalent
- `data/reference/occupation_bucket_crosswalk_seed.csv`

### Outputs

- `data/reference/occupation_bucket_crosswalk_seed.csv`
- `data/processed/occupation_bucket_crosswalk_summary.csv`
- `data/processed/occupation_bucket_entities_long.csv`
- `data/processed/occupation_bucket_summary.csv`
- `data/processed/occupation_bucket_language_representation.csv`

For `global_writers`, the processed outputs are written under
`data/processed/global_writers/`.

## Context Slice Layer - Reproducible Crosswalk Slices

Script:

`scripts/analysis/07_build_context_slice_tables.py`

### Purpose

Derive comparison slices from the global writer cohort rather than relying on
the original manual French seed. The script uses
`data/reference/political_entity_affiliation_crosswalk_seed.csv` to identify
France, Germany, British, and China/Qing evidence in country-of-citizenship
and place-context fields.

This layer is the reproducible replacement for new France-facing contrast
claims. The old French seed stays available as provenance, but it should not be
used as the canonical definition of France.

### Inputs

- `data/interim/global_writers/writers_wikidata_enriched.csv`
- `data/processed/global_writers/place_context_long.csv`
- `data/reference/political_entity_affiliation_crosswalk_seed.csv`
- `data/raw/18thcentury_french_writers_table.csv` for the legacy overlap audit

### Outputs

- `data/processed/global_writers/context_slice_membership.csv`
- `data/processed/global_writers/context_slice_summary.csv`
- `data/processed/french_seed_redundancy_audit.csv`

### Current French-Seed Audit

The legacy seed has 1,638 distinct QIDs, and all 1,638 are already present in
the reproducible global writer cohort. It is therefore redundant as discovery
data. It is not equivalent to current exact `country of citizenship = France`
because:

- 311 legacy-seed QIDs are not current exact France/Q142 citizens.
- 238 exact France/Q142 global writers are absent from the legacy seed.
- 286 crosswalk-France citizenship global writers are absent from the legacy
  seed.

## Core Findings Packet

Script:

`scripts/analysis/08_build_core_findings_packet.py`

### Purpose

Create the current conference-facing findings packet from existing processed
tables. This layer is a reporting layer, not a new enrichment step.

### Inputs

- `data/processed/global_writers/context_slice_summary.csv`
- `data/processed/global_writers/context_slice_membership.csv`
- `data/processed/global_writers/representation_entities.csv`
- `data/processed/global_writers/geographic_scope_summary.csv`
- `data/processed/global_writers/cultural_affiliation_evidence_summary.csv`
- `data/processed/global_writers/cultural_affiliation_evidence_best.csv`
- `data/processed/global_writers/occupation_bucket_summary.csv`
- `data/processed/global_writers/occupation_bucket_entities_long.csv`
- `data/processed/global_writers/wikidata_label_coverage_by_language.csv`

### Outputs

- `docs/core_findings_packet.md`
- `data/processed/global_writers/core_findings_key_metrics.csv`
- `data/processed/global_writers/core_findings_context_slices.csv`
- `data/processed/global_writers/core_findings_language_by_slice.csv`
- `data/processed/global_writers/core_findings_gender_by_slice.csv`
- `data/processed/global_writers/core_findings_occupation_buckets_by_slice.csv`
- `data/processed/global_writers/core_findings_data_friction.csv`

## Planned Visualization Layer - Findings Visuals

The analysis layers already produce provisional cultural-affiliation,
place-affiliation, formula-backed affiliation evidence, and Wikipedia
language-edition matrices. A later visualization layer should turn the core
findings packet into presentation-ready views:

- build context-slice bars
- build citizenship/place punchcards
- build language-edition heatmaps
- build occupation-bucket comparisons
- preserve ambiguous or historically contested place mappings rather than
  forcing them into modern nation-state categories

## Later Research Decisions

Several decisions remain intentionally open:

- How to define the broader "Enlightenment character" cohort beyond the
  current writer-like pilot data.
- How to map raw occupations into normalized categories without erasing
  meaningful historical ambiguity.
- Whether place-derived affiliation evidence should remain contextual or later
  modify the core cultural-affiliation score.
- Whether any external source is needed for a concrete claim not answerable
  from the current Wikidata/Wikipedia evidence.

These are interpretive and methodological decisions. They should be made
explicitly in later steps, not hidden inside cleaning or merge scripts.
