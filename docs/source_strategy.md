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

1. Diagnose the current pilot dataset.
2. Add richer Wikidata fields needed for cultural-affiliation and
   representation analysis.
3. Re-run diagnostics on the enriched cohort.
4. Add BnF as the first external comparison source.
5. Decide whether domain-specific external authorities are worth the added
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

## Tier 2: BnF Comparison

BnF is the first external comparison source because the current project is still
French-facing in its questions, even as the broader cohort may become more
European or Enlightenment-wide.

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
- Step 06 should build the cultural-affiliation punch card from explicit
  evidence fields.
- Step 07 should introduce BnF.
- Step 09 should remain optional until a concrete question justifies it.
