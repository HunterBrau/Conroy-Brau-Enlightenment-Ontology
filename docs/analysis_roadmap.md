# Analysis Roadmap

This roadmap translates the April 2026 project correspondence into a concrete
sequence of computational-humanities outputs. The project has enough time to
build beyond a narrow MVP, but the work should still move in layers so each
claim can be inspected.

## Core Argument

Wikidata provides an identity spine and structured evidence about people.
Wikipedia language editions provide a way to compare cultural representation
across language communities. The project should use both:

- Wikidata fields explain who the people are and what evidence is available.
- Wikipedia language editions show where those people are represented.

The main conference-friendly question is:

```text
Which kinds of Enlightenment-era figures are represented by which language
communities, and what evidence supports their inferred cultural affiliation?
```

## Current Scope

- Cohort dates: 1675-1775.
- Current cohorts: `french_seed` and `global_writers`.
- Occupation scope: writer or subclass of writer.
- Corpus expansion beyond writer/subclass is deferred until the two current
  Wikidata tracks and political-entity crosswalk are stable.
- Geographic scope: European language communities first.

## Language Editions

Primary comparison set:

```text
frwiki, enwiki, dewiki, itwiki, eswiki, plwiki, ruwiki, ukwiki
```

Additional comparison set now supported by the query templates:

```text
nlwiki, ptwiki, svwiki, dawiki
```

These editions are included because Dutch, Portuguese, Swedish, Danish, and
Ukrainian contexts are likely to preserve different parts of the Enlightenment
record. If a figure becomes too wide, the main visualization can use a selected
subset while keeping the full matrix available for analysis.

## Phase 1: Enrich the Current Cohorts

Goal: build richer but still inspectable evidence tables for the French seed
and global writer/subclass cohorts.

Field families:

| Family | Wikidata source | Use |
|---|---|---|
| Birth place | current raw export | geographic evidence |
| Death place | `place of death` | geographic evidence |
| Citizenship | `country of citizenship` | affiliation evidence |
| Residence | `residence` | mobility evidence |
| Work location | `work location` | mobility and professional evidence |
| Native language | `native language` | cultural-linguistic evidence |
| Spoken/written language | `languages spoken, written or signed` | cultural-linguistic evidence |
| Writing language | `writing language` | literary evidence |
| Gender | `sex or gender` | representation axis |
| Ethnic group | `ethnic group` | contextual evidence only |
| Occupations | `occupation` | profession and role axis |
| Label coverage | labels/descriptions by language | Wikidata representation axis |
| Wikipedia sitelinks | Wikipedia language editions | Wikipedia representation axis |
| Notable-work genres | notable works plus genre/form | sparse literary-genre evidence |

Ethnic group should be handled cautiously. Wikidata treats it as a field that
requires a high standard of proof, and the project will report it as context
rather than using it in affiliation tallies.

## Phase 2: Build Matrices

Implemented entry point:

```powershell
python scripts/queries/18_fetch_wikidata_person_place_context.py
python scripts/analysis/02_build_place_affiliation_context.py
python scripts/analysis/01_build_representation_matrices.py
python scripts/analysis/03_build_geographic_scope_analysis.py
python scripts/analysis/05_build_affiliation_evidence_matrix.py
```

The analysis layers write entity-level cultural-affiliation candidates,
place-derived affiliation context, formula-backed affiliation evidence tallies,
language-edition representation rows, geographic-scope diagnostics, and summary
tables under `data/processed/`.

### 1. Cultural-Affiliation Matrix

Core score inputs:

```text
country of citizenship
spoken/written language
native language
writing language
```

Contextual evidence:

```text
birth place
death place
ethnic group
residence
work location
```

The current formula-backed layer tallies citizenship, language, and place
evidence as visible fields. Ethnic group is not part of the score.

Outputs:

```text
candidate_affiliation
supporting_evidence_count
available_mapped_evidence_count
total_evidence_fields
score_over_total_fields
score_over_available_fields
score_formula
```

Place-context outputs separately report:

```text
best_place_candidate_affiliation
place_evidence_role_count
mapped_place_role_count
```

Geographic-scope outputs separately report:

```text
country_of_citizenship_missing
place_context_china_or_chinese_state_any_role
place_context_british_empire_or_colonial_state_any_role
birth_scope_europe
birth_scope_non_europe_or_colonial_inclusive
birth_scope_transcontinental_or_imperial_context
```

### 2. Representation Matrix

Compare each person against Wikipedia language-edition presence:

```text
has_frwiki
has_enwiki
has_dewiki
has_itwiki
has_eswiki
has_plwiki
has_ruwiki
has_ukwiki
has_nlwiki
has_ptwiki
has_svwiki
has_dawiki
```

Later extensions can add article length, revision count, or pageviews.

### 3. Professional Matrix

Separate occupations into:

```text
writerly occupations
non-writing occupations
granular occupation buckets
```

This supports questions like whether different language editions emphasize
poets, playwrights, translators, clergy, philosophers, artists, or scientists
unevenly.

The current bucket layer is implemented in
`scripts/analysis/06_build_occupation_bucket_tables.py`. It includes Religion /
Theology as a separate bucket and writes a reviewable crosswalk to
`data/reference/occupation_bucket_crosswalk_seed.csv`.

## Phase 3: Findings and Visualizations

Priority outputs:

- Coverage table by field family.
- Nationality/cultural-affiliation punch card examples.
- Representation rates by language edition and inferred affiliation.
- Representation rates by language edition and place-derived affiliation.
- Europe/non-Europe or colonial birth-scope proportions.
- China/Qing and British imperial context cases recovered through place evidence.
- Representation rates by language edition and gender.
- Representation rates by language edition and occupation bucket.
- Short list of highly cosmopolitan figures.
- Short list of data-friction cases: duplicates, unresolved labels, ambiguous
  VIAF metadata, sparse labels.

## Phase 4: Controlled Corpus Expansion

The repository now has the first controlled expansion: `global_writers`, built
with:

```sparql
?person wdt:P106/wdt:P279* wd:Q36180 .
```

Broader Enlightenment characters can come after the writerly cohort is
understood. The next cleanup task is not a broader corpus; it is a reviewed
political-entity crosswalk for the current cohorts.

## Open Decisions

- Which occupation buckets should be manually curated for presentation.
- Whether notable-work genre coverage is rich enough for analysis or only a
  supplemental table.
- Whether classical/contextual languages such as Latin, Ancient Greek, and
  Neo-Latin should stay outside the cultural-affiliation score or become a
  separate learned/classical-humanist category.
