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

5. **Step 05 - Re-run Diagnostics and Normalize Occupation or Cohort Categories** *(planned)*

6. **Step 06 - Cultural-Affiliation Evidence Matrix** *(planned)*

7. **Step 07 - Compare with BnF** *(planned)*

8. **Step 08 - Geography and Network Analysis Datasets** *(planned)*

9. **Step 09 - Optional Domain-Specific Enrichment** *(planned)*

Between Step 03 and Step 04, the repository also includes a non-numbered
label-correction helper that builds auditable correction tables for unresolved
Wikidata QID labels.

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

Step 04 has one source-acquisition step and one deterministic merge step. The
query is run outside the local pipeline because the Wikidata Query Service is a
web service, but the export is merged locally in a reproducible way.

For the current 1675-1775 cohort, the operational workflow should prefer split
query families over the single all-fields query. That is not just a performance
hack. Multivalued properties such as citizenship, residence, work location,
and language can create large cross-products inside one combined query. Split
families keep the endpoint work smaller and make failures easier to diagnose.

Query artifacts:

- `scripts/queries/00_run_wikidata_sparql_query.py`
- `scripts/queries/01_export_wikidata_values_block.py`
- `scripts/queries/11_run_wikidata_query_batch.py`
- `scripts/queries/02_wikidata_affiliation_enrichment.rq`
- `scripts/queries/05_wikidata_affiliation_death_place.rq`
- `scripts/queries/06_wikidata_affiliation_languages.rq`
- `scripts/queries/07_wikidata_wikipedia_representation.rq`
- `scripts/queries/08_wikidata_affiliation_residence.rq`
- `scripts/queries/09_wikidata_affiliation_work_location.rq`
- `scripts/queries/10_wikidata_affiliation_citizenship.rq`
- `scripts/queries/03_wikidata_current_cohort_geography_export.rq`
- `scripts/queries/04_wikidata_current_cohort_viaf_export.rq`

Generated query outputs:

- `outputs/wikidata_cohort_values_block.txt`
- `outputs/wikidata_affiliation_enrichment_query.rq`
- `outputs/wikidata_affiliation_queries/`
- `outputs/wikidata_affiliation_death_place_query.rq`
- `outputs/wikidata_affiliation_death_place_queries/`
- `outputs/wikidata_affiliation_languages_query.rq`
- `outputs/wikidata_affiliation_languages_queries/`
- `outputs/wikidata_affiliation_citizenship_query.rq`
- `outputs/wikidata_affiliation_citizenship_queries/`
- `outputs/wikidata_wikipedia_representation_query.rq`
- `outputs/wikidata_wikipedia_representation_queries/`
- `outputs/wikidata_affiliation_residence_query.rq`
- `outputs/wikidata_affiliation_residence_queries/`
- `outputs/wikidata_affiliation_work_location_query.rq`
- `outputs/wikidata_affiliation_work_location_queries/`
- `outputs/wikidata_current_cohort_geography_query.rq`
- `outputs/wikidata_current_cohort_geography_queries/`
- `outputs/wikidata_current_cohort_viaf_query.rq`
- `outputs/wikidata_current_cohort_viaf_queries/`

Merge script:

`scripts/pipeline/04_merge_wikidata_enrichment.py`

### Inputs

Base cohort:

`data/interim/writers_cleaned.csv`

Default Wikidata enrichment export:

`data/raw/wikidata_affiliation_enrichment.csv`

The script also accepts one or more explicit CSV exports, or one or more
directories of CSV exports, with `--input`. This supports both chunked exports
and split query families.

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

### Transformations

Step 04:

- normalizes Wikidata IDs from either full URI or `Q#####` form
- preserves multi-valued fields as pipe-delimited strings
- summarizes duplicate rows from chunked exports without multiplying cohort rows
- fills missing expected columns with `NA` so separate query families can be
  merged into one evidence table
- flags which evidence field families are available per row
- counts evidence-field availability without assigning cultural affiliation
- records missing, extra, and duplicate enrichment entities for inspection

This step does not determine nationality or cultural affiliation. It prepares
the evidence grid that Step 06 can later use.

The combined `02_wikidata_affiliation_enrichment.rq` template remains useful
for auditability and for small pilot subsets. The recommended workflow for the
full current cohort is to run the split families for:

- death place
- citizenship
- residence
- work location
- language evidence
- Wikipedia representation

The current death-place split query intentionally captures death-place IDs and
labels, but not death-place coordinates. Coordinates can be recovered later
from the death-place entities if a later geography step needs them. This keeps
the first enrichment pass reliable enough to run on the current cohort.

The batch helper `scripts/queries/11_run_wikidata_query_batch.py` can export a
whole generated query directory into a CSV directory. That makes the split
workflow reproducible without hand-running dozens of `.rq` files.

### Outputs

Enriched cohort:

`data/interim/writers_wikidata_enriched.csv`

Sidecars and diagnostics:

- `data/interim/wikidata_enrichment_missing_entities.csv`
- `data/interim/wikidata_enrichment_extra_entities.csv`
- `data/interim/wikidata_enrichment_duplicate_rows.csv`
- `data/interim/wikidata_enrichment_summary.csv`
- `data/interim/wikidata_enrichment_field_coverage.csv`

## Source Adoption Strategy

The project should widen its sources in a deliberate order:

1. Keep **Wikidata** as the main identity spine.
2. Diagnose the current pilot dataset before broadening the source graph.
3. Add richer **Wikidata** fields needed for affiliation and representation.
4. Re-run diagnostics on the enriched Wikidata-centered cohort.
5. Add **BnF** as the first external comparison source.
6. Only then decide whether domain-specific authorities are necessary.

This ordering is methodological as well as technical. It keeps the cohort
interpretable, avoids premature identity fragmentation, and makes it easier to
explain how each new source changes the dataset.

See also:

`docs/source_strategy.md`

## Planned Step 05 - Re-run Diagnostics and Normalize Occupation or Cohort Categories

Once the richer Wikidata fields have been added, Step 03 should be run again.
Step 05 can then use those results to define occupation buckets or broader
cohort labels without guessing blindly from the current pilot alone.

This step is where the project should decide whether the cohort remains
writer-centered or expands into a broader Enlightenment character dataset with
clear category rules.

## Later Research Decisions

Several decisions remain intentionally open:

- Whether language comparison should use Wikidata label languages, Wikipedia
  language-edition sitelinks, or both.
- How to define the broader "Enlightenment character" cohort beyond the
  current writer-like pilot data.
- How to map raw occupations into normalized categories without erasing
  meaningful historical ambiguity.
- How to infer cultural affiliation from multiple factors such as birthplace,
  death place, citizenship, language spoken, language written, Wikipedia
  language-edition representation, and any additional criteria drawn from the
  relevant paper.
- How to compare Wikidata-derived data with BnF, Procope, or other
  bibliographic and cultural datasets.

These are interpretive and methodological decisions. They should be made
explicitly in later steps, not hidden inside Step 01 or Step 02.

## Planned Step 06 - Cultural-Affiliation Evidence Matrix

Step 06 should produce a "punch card" style evidence matrix rather than a
single opaque nationality label.

For each person, the pipeline can evaluate candidate cultural affiliations
against a fixed set of evidence fields. A first-pass matrix might use:

| Evidence field | Example source |
|---|---|
| Birthplace affiliation | country or region containing birthplace |
| Death-place affiliation | country or region containing death place |
| Citizenship | Wikidata `country of citizenship` |
| Residence or work location | Wikidata residence and work-location properties |
| Language | native, spoken/written, or writing language |
| Wikipedia language-edition representation | sitelinks or language-edition presence |

The output should separate:

- `affiliation_score`: checked evidence boxes for a candidate affiliation
- `evidence_possible`: evidence boxes available for that person
- `evidence_completeness`: available evidence boxes divided by total expected boxes
- `affiliation_confidence`: a derived tier such as strong, mixed, weak, or ambiguous

This matrix can support presentation graphics by showing selected rows as punch
cards. It can also support aggregate analysis by counting affiliation patterns
across the full dataset.

## Planned Step 07 - Compare with BnF

BnF should be the first external comparison source. It is the most relevant
next authority system for the current French-facing research questions, and it
is a better first comparator than introducing several unrelated external
datasets at once.

The role of Step 07 is to compare coverage, proportions, and category patterns
between the Wikidata-centered cohort and BnF-derived data. It should not
replace Wikidata as the identity spine.

## Planned Step 09 - Optional Domain-Specific Enrichment

Only after the Wikidata-centered cohort has been enriched, re-diagnosed, and
compared with BnF should the project decide whether domain-specific sources are
worth the additional complexity.

Possible later sources include:

- CERL for early modern print culture and variant-name authority work
- Getty vocabularies for visual-art-heavy cohorts
- MusicBrainz for music and opera-heavy cohorts
- Europeana for cultural heritage enrichment and discovery
