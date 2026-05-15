# Source Strategy

This project is scoped around one reproducible Wikidata/Wikipedia evidence
spine. New sources are not planned for the current phase unless a specific
conference claim cannot be answered from the existing data.

## Current Scope

Use `global_writers` as the active analytical cohort:

- humans in Wikidata;
- born 1675-1775;
- occupation `writer` or any subclass of writer;
- one row per Wikidata QID after normalization.

Use the reviewed political-entity crosswalk to derive comparable context
slices from that cohort:

- France;
- Germany;
- British/British imperial contexts;
- China/Qing contexts.

The legacy `french_seed` files remain provenance and backward compatibility
only. They are not a separate national methodology.

## In Scope

- Wikidata API enrichment already present in the repository.
- Wikipedia language-edition representation from sitelinks.
- Country of citizenship as one evidence field, not a complete nationality
  answer.
- Place context from birth, death, residence, and work-location evidence.
- Formula-backed affiliation evidence tallies.
- Gender representation.
- Occupation buckets, including Religion / Theology as its own bucket.
- Data-friction metrics such as missing citizenship, missing place context,
  missing Wikipedia articles, unresolved labels, and low-evidence affiliation
  cases.
- Visualizations generated from the current processed tables.

## Out Of Current Scope

- BnF comparison.
- CERL, Getty, MusicBrainz, Europeana, or other authority systems.
- Expanding beyond writer/subclass occupations.
- Replacing the Wikidata QID spine.
- Assigning high/medium/low affiliation confidence thresholds before reviewing
  the formula-backed tally outputs.

## Why This Trim Helps

The conference argument is strongest when the method stays legible:

1. Start with a reproducible global writer cohort.
2. Derive all national and imperial comparison slices the same way.
3. Show how evidence fields change the map, especially citizenship versus
   place context.
4. Compare representation across Wikipedia language editions.
5. Keep missingness and low-evidence cases visible.

This is enough to make a computational humanities argument without bringing in
another identity system.

## Purposeful Next Work

1. Review the political-entity crosswalk rows that feed the context slices.
2. Review the occupation-bucket crosswalk rows marked for manual review.
3. Build a small visualization set from the core findings packet:
   context-slice bars, citizenship-vs-place punchcards, language heatmaps, and
   occupation-bucket comparisons.
4. Add no new data source unless a concrete claim needs it.
