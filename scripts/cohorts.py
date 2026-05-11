from dataclasses import dataclass
from pathlib import Path


LEGACY_FRENCH_SEED_COHORT_ID = "french_seed"
DEFAULT_COHORT_ID = "global_writers"
ACTIVE_COHORT_IDS = [DEFAULT_COHORT_ID]
LEGACY_COHORT_IDS = [LEGACY_FRENCH_SEED_COHORT_ID]
COHORT_IDS = [DEFAULT_COHORT_ID, LEGACY_FRENCH_SEED_COHORT_ID]
COMPARISON_COHORT_IDS = [LEGACY_FRENCH_SEED_COHORT_ID, DEFAULT_COHORT_ID]


@dataclass(frozen=True)
class CohortPaths:
    cohort_id: str
    description: str
    source_type: str
    raw_discovery_path: Path
    raw_viaf_path: Path | None
    raw_enrichment_path: Path
    raw_person_place_context_path: Path
    raw_place_context_entities_path: Path
    interim_dir: Path
    processed_dir: Path
    date_min: int
    date_max: int
    primary_scope_note: str
    analysis_role: str
    is_active_discovery: bool

    @property
    def merged_path(self) -> Path:
        return self.interim_dir / "writers_merged.csv"

    @property
    def cleaned_path(self) -> Path:
        return self.interim_dir / "writers_cleaned.csv"

    @property
    def duplicate_wikidata_ids_path(self) -> Path:
        return self.interim_dir / "duplicate_wikidata_ids.csv"

    @property
    def viaf_conflicts_path(self) -> Path:
        return self.interim_dir / "viaf_conflicts.csv"

    @property
    def enriched_path(self) -> Path:
        return self.interim_dir / "writers_wikidata_enriched.csv"

    @property
    def enrichment_missing_entities_path(self) -> Path:
        return self.interim_dir / "wikidata_enrichment_missing_entities.csv"

    @property
    def enrichment_extra_entities_path(self) -> Path:
        return self.interim_dir / "wikidata_enrichment_extra_entities.csv"

    @property
    def enrichment_duplicate_rows_path(self) -> Path:
        return self.interim_dir / "wikidata_enrichment_duplicate_rows.csv"

    @property
    def enrichment_summary_path(self) -> Path:
        return self.interim_dir / "wikidata_enrichment_summary.csv"

    @property
    def enrichment_field_coverage_path(self) -> Path:
        return self.interim_dir / "wikidata_enrichment_field_coverage.csv"


def project_root_from_script(script_path: str | Path) -> Path:
    return Path(script_path).resolve().parents[2]


def script_support_root(script_path: str | Path) -> Path:
    return Path(script_path).resolve().parents[1]


def cohort_paths(project_root: Path, cohort_id: str = DEFAULT_COHORT_ID) -> CohortPaths:
    if cohort_id == "french_seed":
        return CohortPaths(
            cohort_id="french_seed",
            description="Legacy manual French-facing writer/subclass seed cohort born 1675-1775.",
            source_type="legacy_manual_wikidata_sparql_export_with_viaf_sidecar",
            raw_discovery_path=project_root / "data" / "raw" / "18thcentury_french_writers_table.csv",
            raw_viaf_path=project_root / "data" / "raw" / "18thcentury_writers_wikidata_viaf.csv",
            raw_enrichment_path=project_root / "data" / "raw" / "wikidata_affiliation_enrichment.csv",
            raw_person_place_context_path=project_root / "data" / "raw" / "wikidata_person_place_context.csv",
            raw_place_context_entities_path=project_root / "data" / "raw" / "wikidata_place_context_entities.csv",
            interim_dir=project_root / "data" / "interim",
            processed_dir=project_root / "data" / "processed",
            date_min=1675,
            date_max=1775,
            primary_scope_note=(
                "Legacy flat-file workflow retained for provenance and backward compatibility. "
                "Use global_writers context slices for new France-facing comparison claims; "
                "VIAF is supporting authority metadata, not the discovery source."
            ),
            analysis_role="legacy_provenance",
            is_active_discovery=False,
        )

    if cohort_id == "global_writers":
        return CohortPaths(
            cohort_id="global_writers",
            description="Global Wikidata humans born 1675-1775 with occupation writer or subclass of writer.",
            source_type="wikidata_sparql_discovery",
            raw_discovery_path=project_root / "data" / "raw" / "global_writers_1675_1775_discovery.csv",
            raw_viaf_path=None,
            raw_enrichment_path=project_root / "data" / "raw" / "global_writers" / "wikidata_affiliation_enrichment.csv",
            raw_person_place_context_path=project_root / "data" / "raw" / "global_writers" / "wikidata_person_place_context.csv",
            raw_place_context_entities_path=project_root / "data" / "raw" / "global_writers" / "wikidata_place_context_entities.csv",
            interim_dir=project_root / "data" / "interim" / "global_writers",
            processed_dir=project_root / "data" / "processed" / "global_writers",
            date_min=1675,
            date_max=1775,
            primary_scope_note="Global writer/subclass discovery cohort; no citizenship filter.",
            analysis_role="active_analytical_spine",
            is_active_discovery=True,
        )

    raise ValueError(f"Unknown cohort_id: {cohort_id}")


def cohort_manifest_rows(project_root: Path) -> list[dict]:
    rows = []
    for cohort_id in COHORT_IDS:
        paths = cohort_paths(project_root, cohort_id)
        rows.append(
            {
                "cohort_id": paths.cohort_id,
                "description": paths.description,
                "source_type": paths.source_type,
                "raw_discovery_path": paths.raw_discovery_path.relative_to(project_root).as_posix(),
                "interim_clean_path": paths.cleaned_path.relative_to(project_root).as_posix(),
                "processed_output_dir": paths.processed_dir.relative_to(project_root).as_posix(),
                "date_min": paths.date_min,
                "date_max": paths.date_max,
                "analysis_role": paths.analysis_role,
                "is_active_discovery": paths.is_active_discovery,
                "primary_scope_note": paths.primary_scope_note,
            }
        )
    return rows
