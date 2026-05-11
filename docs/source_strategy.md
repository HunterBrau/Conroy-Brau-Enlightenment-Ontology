# Source Strategy

This document records the source-adoption plan for the project so that new data
is added in a deliberate order rather than as a collection of interesting but
incompatible datasets.

## Guiding Principle

Use **Wikidata as the identity spine** for the project unless there is a clear
reason not to. New sources should enrich, compare against, or diagnose the
Wikidata-centered cohort before they are allowed to redefine it.

This keeps the pipeline:

- easier to explain
- easier to debug
- easier to reproduce
- less likely to fragment into competing identity systems too early

## Immediate Sequence

1. Keep the legacy French seed as provenance, but use `global_writers` as the
   discovery spine for new comparative claims.
2. Maintain Wikidata API enrichment as the shared route for both cohorts.
3. Build and review the political-entity crosswalk for France, Germany,
   British/British imperial contexts, and China/Qing contexts.
4. Use the context-slice layer to derive France, Germany, British, and
   China/Qing comparison groups from the global cohort.
5. Re-run diagnostics and representation matrices after crosswalk changes.
6. Add BnF as the first external comparison source only after the Wikidata
   tracks are stable.
7. Decide whether domain-specific external authorities are worth the added
   complexity.

## Tier 1: Wikidata-Centered Expansion

The next data expansion should stay within Wikidata and Wikimedia-linked data.
The goal is to support the cultural-affiliation matrix and the future
influence-map questions without introducing a second identity spine too early.

The first enrichment pass should prioritize:

| Field family | Why it matters |
|---|---|
| Place of death | affiliation evidence |
| Country of citizenship | affiliation evidence |
| Residence and work location | affiliation evidence |
| Native language and languages spoken/written | affiliation evidence |
| Writing language | affiliation evidence |
| Wikipedia language-edition sitelinks | representation and influence analysis |
| Sex or gender | representation axis |
| Ethnic group | contextual evidence only; interpret cautiously |
| Occupation | writerly and non-writing professional axes |
| Labels and descriptions by language | Wikidata representation and data-friction evidence |
| Notable-work genres/forms | sparse literary-genre evidence |

Wikipedia language-edition presence should be treated as representation
evidence, not as nationality evidence. It answers which language communities
have articles about a person, not what nationality that person had.

Ethnic group should not participate in the cultural-affiliation tally. It may
be useful as contextual evidence or for data-availability analysis.

## Tier 2: BnF Comparison

BnF is the first external comparison source because the project remains
French-facing in its conference framing, even now that the repository also has
a global writer/subclass comparison cohort.

BnF should be used to:

- compare coverage and omissions
- compare proportions across national or occupational categories
- test whether Wikidata and bibliographic authority data tell similar stories

BnF should not replace Wikidata as the identity spine. It should be a
comparison and enrichment layer.

## Tier 3: Optional Domain-Specific Sources

Only add these if a clear research question demands them:

| Source | Best use case |
|---|---|
| CERL | early modern print culture, variant names, book-history authority work |
| Getty vocabularies | visual-art-heavy cohorts |
| MusicBrainz | music, opera, and composer-heavy cohorts |
| Europeana | cultural heritage discovery and enrichment |

These sources are powerful, but they add complexity. They should come after the
Wikidata-centered cohort is stable and after BnF comparison has been scoped.

## What This Means for the Pipeline

- Step 03 is not a one-time report. It should be re-run after major data
  expansions.
- Step 04 should enrich Wikidata fields before new external comparisons.
- The first analysis layer should build cultural-affiliation and representation
  matrices from explicit evidence fields.
- The occupation-bucket layer should keep writerly, non-writerly, and
  religion/theology occupations visible for review.
- The context-slice layer should replace the manual French seed for new
  France/Germany/British/China comparisons.
- A later visualization layer should turn the matrices into map and network
  datasets.
- Step 07 should introduce BnF.
- Step 09 should remain optional until a concrete question justifies it.
