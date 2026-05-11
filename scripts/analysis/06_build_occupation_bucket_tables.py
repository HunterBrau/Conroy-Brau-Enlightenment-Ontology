"""
Build reviewable occupation bucket tables.

This layer maps Wikidata occupation QIDs into granular buckets for analysis.
The mapping is intentionally a seed crosswalk: it gives the project a stable
starting point, but the final categories remain reviewable by collaborators.
"""

from argparse import ArgumentParser
from pathlib import Path
import re
import sys
import unicodedata

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from cohorts import DEFAULT_COHORT_ID, cohort_paths  # noqa: E402


COHORT_IDS = ["french_seed", "global_writers"]
REFERENCE_PATH = Path("data/reference/occupation_bucket_crosswalk_seed.csv")

WIKI_COLUMNS = {
    "frwiki": "has_frwiki",
    "enwiki": "has_enwiki",
    "dewiki": "has_dewiki",
    "itwiki": "has_itwiki",
    "eswiki": "has_eswiki",
    "plwiki": "has_plwiki",
    "ruwiki": "has_ruwiki",
    "ukwiki": "has_ukwiki",
    "nlwiki": "has_nlwiki",
    "ptwiki": "has_ptwiki",
    "svwiki": "has_svwiki",
    "dawiki": "has_dawiki",
}

BUCKET_FAMILIES = {
    "Writing / Literature": "Literary and textual production",
    "Translation / Philology / Languages": "Literary and textual production",
    "Print / Publishing / Journalism": "Literary and textual production",
    "Philosophy": "Intellectual systems",
    "Religion / Theology": "Intellectual systems",
    "Education / Scholarship / Humanities": "Intellectual systems",
    "Science / Natural History": "Scientific knowledge",
    "Medicine / Health": "Scientific knowledge",
    "Engineering / Technology": "Scientific knowledge",
    "Travel / Geography / Exploration": "Mobility and empire",
    "Politics / Statecraft / Diplomacy": "Institutional power",
    "Law / Administration": "Institutional power",
    "Military": "Institutional power",
    "Economics / Commerce": "Institutional power",
    "Visual Arts / Architecture / Design": "Arts and performance",
    "Music / Performance / Theatre": "Arts and performance",
    "Sociability / Patronage / Court": "Social and cultural mediation",
    "Social Reform / Activism": "Social and cultural mediation",
    "Craft / Trade / Industry": "Material production",
    "Unmapped / Review": "Needs review",
}

EXACT_QID_BUCKETS = {
    "Q36180": ("Writing / Literature", "writer"),
    "Q49757": ("Writing / Literature", "poet"),
    "Q214917": ("Writing / Literature", "playwright"),
    "Q6625963": ("Writing / Literature", "novelist"),
    "Q8178443": ("Writing / Literature", "librettist"),
    "Q11774202": ("Writing / Literature", "essayist"),
    "Q18814623": ("Writing / Literature", "autobiographer"),
    "Q333634": ("Translation / Philology / Languages", "translator"),
    "Q1930187": ("Print / Publishing / Journalism", "journalist"),
    "Q2516866": ("Print / Publishing / Journalism", "editor"),
    "Q4964182": ("Philosophy", "philosopher"),
    "Q1234713": ("Religion / Theology", "theologian"),
    "Q201788": ("Education / Scholarship / Humanities", "historian"),
    "Q1622272": ("Education / Scholarship / Humanities", "university professor"),
    "Q37226": ("Education / Scholarship / Humanities", "teacher"),
    "Q901": ("Science / Natural History", "scientist"),
    "Q11063": ("Science / Natural History", "astronomer"),
    "Q169470": ("Science / Natural History", "physicist"),
    "Q593644": ("Science / Natural History", "chemist"),
    "Q2374149": ("Science / Natural History", "botanist"),
    "Q864503": ("Science / Natural History", "biologist"),
    "Q350979": ("Science / Natural History", "zoologist"),
    "Q18805": ("Science / Natural History", "naturalist"),
    "Q39631": ("Medicine / Health", "physician"),
    "Q205375": ("Engineering / Technology", "inventor"),
    "Q901402": ("Travel / Geography / Exploration", "geographer"),
    "Q11900058": ("Travel / Geography / Exploration", "explorer"),
    "Q82955": ("Politics / Statecraft / Diplomacy", "politician"),
    "Q193391": ("Politics / Statecraft / Diplomacy", "diplomat"),
    "Q185351": ("Law / Administration", "jurist"),
    "Q212238": ("Law / Administration", "civil servant"),
    "Q47064": ("Military", "military person"),
    "Q188094": ("Economics / Commerce", "economist"),
    "Q1028181": ("Visual Arts / Architecture / Design", "painter"),
    "Q42973": ("Visual Arts / Architecture / Design", "architect"),
    "Q1281618": ("Visual Arts / Architecture / Design", "sculptor"),
    "Q36834": ("Music / Performance / Theatre", "composer"),
    "Q639669": ("Music / Performance / Theatre", "musician"),
}

RULE_BUCKETS = [
    (
        "Religion / Theology",
        [
            "theologien",
            "theologienne",
            "pretre",
            "pretresse",
            "pasteur",
            "ministre du culte",
            "rabbin",
            "moine",
            "moniale",
            "religieux",
            "religieuse",
            "jesuite",
            "eveque",
            "abbe",
            "cardinal",
            "missionnaire",
            "predicateur",
            "predicatrice",
            "apologete",
            "psalmiste",
            "hymnographe",
            "quaker",
            "clerc",
            "cure",
            "faqih",
            "chanoine",
            "church writer",
            "frere",
            "oulema",
            "ecclesiastique",
            "diacre",
            "vicaire",
            "mystique",
            "soufi",
            "terton",
            "chapelain",
            "bibliste",
            "confesseur",
            "lama",
            "muhaddith",
            "moufassir",
            "mufti",
            "prelat",
            "protoiereus",
            "catechiste",
            "confucianiste",
            "exegete",
        ],
    ),
    (
        "Translation / Philology / Languages",
        [
            "traducteur",
            "traductrice",
            "interprete",
            "philologue",
            "linguiste",
            "lexicographe",
            "grammairien",
            "grammairienne",
            "orientaliste",
            "sinologue",
            "helleniste",
            "latiniste",
            "etymologiste",
            "romaniste",
            "arabisant",
            "arabisante",
            "hispaniste",
            "egyptologue",
            "hebraisant",
            "kokugaku scholar",
            "english-german translator",
            "english-german translator",
            "sanskritiste",
        ],
    ),
    (
        "Print / Publishing / Journalism",
        [
            "journaliste",
            "editeur",
            "editrice",
            "imprimeur",
            "imprimeuse",
            "publiciste",
            "libraire",
            "typographe",
            "redacteur",
            "redactrice",
            "publisher",
        ],
    ),
    (
        "Writing / Literature",
        [
            "ecrivain",
            "ecrivaine",
            "poete",
            "poetesse",
            "dramaturge",
            "romancier",
            "romanciere",
            "essayiste",
            "diariste",
            "autobiographe",
            "biographe",
            "fabuliste",
            "librettiste",
            "epistolier",
            "epistoliere",
            "prosateur",
            "prosatrice",
            "auteur",
            "autrice",
            "nouvelliste",
            "memorialiste",
            "homme ou femme de lettres",
            "critique litteraire",
            "conteur",
            "conteuse",
            "chansonnier",
            "parolier",
            "pamphletaire",
            "chroniqueur",
            "chroniqueuse",
            "satiriste",
            "compilateur",
            "compilatrice",
            "specialiste de la litterature",
            "homme de lettres",
            "femme de lettres",
            "hagiographe",
            "orateur",
            "oratrice",
            "rheteur",
            "rhetrice",
            "scenariste",
            "narrateur",
            "scribe",
            "collecteur ou collectrice de textes traditionnels",
            "kyoka poet",
            "polemiste",
            "commentateur",
            "gesakusha",
            "glosser",
            "mythographe",
            "polygraphe",
            "copiste",
            "collecteur ou collectrice de chanson traditionnelle",
            "rhetoricien",
            "akyn",
        ],
    ),
    (
        "Philosophy",
        [
            "philosophe",
            "moraliste",
            "logicien",
            "logicienne",
            "metaphysicien",
            "metaphysicienne",
            "epistemologue",
        ],
    ),
    (
        "Medicine / Health",
        [
            "medecin",
            "chirurgien",
            "chirurgienne",
            "pharmacien",
            "pharmacienne",
            "anatomiste",
            "psychiatre",
            "veterinaire",
            "apothicaire",
            "obstetricien",
            "obstetricienne",
            "psychologue",
            "sage-femme",
            "gynecologue",
            "pharmacologue",
            "ophtalmologue",
            "guerisseur",
            "guerisseuse",
            "infirmiere",
            "infirmier",
        ],
    ),
    (
        "Travel / Geography / Exploration",
        [
            "explorateur",
            "exploratrice",
            "voyageur",
            "voyageuse",
            "chroniqueur de voyage",
            "geographe",
            "cartographe",
            "navigateur",
            "navigatrice",
            "globe-trotteur",
            "topographe",
            "marin",
            "aventurier",
            "aventuriere",
            "alpiniste",
            "geodesiste",
        ],
    ),
    (
        "Science / Natural History",
        [
            "scientifique",
            "naturaliste",
            "physicien",
            "physicienne",
            "chimiste",
            "biologiste",
            "botaniste",
            "zoologiste",
            "geologue",
            "astronome",
            "mathematicien",
            "mathematicienne",
            "mineralogiste",
            "paleontologue",
            "meteorologue",
            "ornithologue",
            "entomologiste",
            "mycologue",
            "oceanographe",
            "climatologue",
            "malacologiste",
            "ichtyologiste",
            "agronome",
            "geometre",
            "statisticien",
            "statisticienne",
            "physiologiste",
            "horticulteur",
            "horticultrice",
            "apiculteur",
            "apicultrice",
            "bryologiste",
            "lepidopteriste",
            "pteridologue",
            "arachnologue",
            "forestier",
            "forestiere",
        ],
    ),
    (
        "Engineering / Technology",
        [
            "ingenieur",
            "ingenieure",
            "inventeur",
            "inventrice",
            "mecanicien",
            "mecanicienne",
            "horloger",
            "horlogere",
            "technicien",
            "technicienne",
        ],
    ),
    (
        "Education / Scholarship / Humanities",
        [
            "professeur",
            "enseignant",
            "enseignante",
            "pedagogue",
            "historien",
            "historienne",
            "archeologue",
            "anthropologue",
            "ethnologue",
            "bibliothecaire",
            "archiviste",
            "antiquaire",
            "erudit",
            "erudite",
            "academicien",
            "academicienne",
            "bibliographe",
            "encyclopediste",
            "educateur",
            "educatrice",
            "numismate",
            "bibliophile",
            "etudiant",
            "maitre d'ecole",
            "precepteur",
            "hofmeister",
            "universitaire",
            "politologue",
            "sociologue",
            "ethnographe",
            "genealogiste",
            "heraldiste",
            "chercheur independant",
            "polymathe",
            "critique",
            "directeur ou directrice d'ecole",
            "inspecteur des ecoles",
            "instituteur",
            "institutrice",
            "maitre de conferences",
            "maitre ou maitresse de conferences",
            "intellectuel",
            "intellectuelle",
            "folkloriste",
            "epigraphiste",
            "tuteur",
            "tutrice",
            "formateur",
            "formatrice",
            "gouvernante",
            "recteur",
        ],
    ),
    (
        "Social Reform / Activism",
        [
            "abolitionniste",
            "feministe",
            "militant",
            "militante",
            "activiste",
            "reformateur",
            "reformatrice",
            "philanthrope",
            "suffrag",
            "national revival activist",
        ],
    ),
    (
        "Politics / Statecraft / Diplomacy",
        [
            "personnalite politique",
            "politicien",
            "politicienne",
            "diplomate",
            "homme ou femme d'etat",
            "souverain",
            "souveraine",
            "monarque",
            "revolutionnaire",
            "theoricien ou theoricienne politique",
        ],
    ),
    (
        "Law / Administration",
        [
            "juriste",
            "avocat",
            "avocate",
            "juge",
            "notaire",
            "magistrat",
            "fonctionnaire",
            "administrateur",
            "administratrice",
            "procureur",
            "gouverneur",
            "gouverneure",
            "barrister",
            "secretaire",
            "surintendant",
            "ministre",
            "conseiller royal",
            "maire",
            "espion",
            "espionne",
            "amtmann",
            "cadi",
            "censeur",
            "doyen",
            "jurisconsulte",
            "ambassadeur",
            "legationsrat",
            "assesseur",
            "canoniste",
            "censeur royal",
            "chancelier",
            "conseiller juridique",
            "dirigeant",
            "dirigeante",
            "defenseur",
            "prevot",
            "agent administratif",
            "bureaucrate",
            "conseiller de gouvernement",
            "employe ou employee de bureau",
            "inspecteur des impots",
            "maitre de poste",
            "directeur ou directrice",
            "justiciar",
        ],
    ),
    (
        "Military",
        [
            "militaire",
            "soldat",
            "officier",
            "generale",
            "general",
            "amiral",
            "marechal",
            "colonel",
            "capitaine",
            "commandant",
            "bushi",
        ],
    ),
    (
        "Economics / Commerce",
        [
            "economiste",
            "banquier",
            "banquiere",
            "marchand",
            "marchande",
            "commercant",
            "commercante",
            "entrepreneur",
            "entrepreneuse",
            "financier",
            "financiere",
            "negociant",
            "negociante",
            "personnalite du monde des affaires",
            "proprietaire terrien",
            "proprietaire de plantation",
            "comptable",
            "industriel",
            "industrielle",
            "actuaire",
            "planteur",
            "proprietaire de biens",
            "receveur des finances",
            "trader",
        ],
    ),
    (
        "Music / Performance / Theatre",
        [
            "musicien",
            "musicienne",
            "compositeur",
            "compositrice",
            "chanteur",
            "chanteuse",
            "acteur",
            "actrice",
            "danseur",
            "danseuse",
            "metteur en scene",
            "directeur de theatre",
            "directrice de theatre",
            "theatre director",
            "musicologue",
            "organiste",
            "theoricien ou theoricienne de la musique",
            "artiste lyrique",
            "chef ou cheffe d'orchestre",
            "chef ou cheffe de choeur",
            "metteur ou metteuse en scene",
            "realisateur",
            "realisatrice",
            "goguettier",
            "ashik",
            "cantor",
            "choregraphe",
            "critique de musique",
            "violoniste",
            "claveciniste",
            "pianiste",
            "luthiste",
            "flutiste",
            "maitre de chapelle",
            "marionnettiste",
            "maitre a danser",
            "guslar",
            "bertsolari",
            "impresario",
            "producteur ou productrice de theatre",
            "theatrologue",
            "violoncelliste",
        ],
    ),
    (
        "Visual Arts / Architecture / Design",
        [
            "artiste peintre",
            "peintre",
            "sculpteur",
            "sculptrice",
            "architecte",
            "graveur",
            "graveuse",
            "dessinateur",
            "dessinatrice",
            "aquarelliste",
            "illustrateur",
            "illustratrice",
            "artiste visuel",
            "artiste visuelle",
            "designer",
            "decorateur",
            "decoratrice",
            "calligraphe",
            "photographe",
            "orfevre",
            "artiste",
            "artiste graphique",
            "aquafortiste",
            "critique d'art",
            "lithographe",
            "ukiyo-e",
            "hattat",
            "collectionneur ou collectionneuse d'oeuvres d'art",
            "conservateur ou conservatrice de musee",
            "caricaturiste",
            "paysagiste",
            "theoricien ou theoricienne de l'art",
            "theoricien ou theoricienne de la litterature",
            "enlumineur",
            "maitre d'oeuvre",
            "medailleur",
            "medailleuse",
            "xylographe",
        ],
    ),
    (
        "Sociability / Patronage / Court",
        [
            "salonniere",
            "salonnier",
            "courtisan",
            "courtisane",
            "mecene",
            "patron",
            "aristocrate",
            "noble",
            "dame de compagnie",
            "hotesse",
            "collectionneur ou collectionneuse",
            "chambellan",
            "membre de la famille royale",
            "seigneur",
            "personnalite",
            "socialite",
            "dilettante",
            "collectionneur de livres",
            "joueur ou joueuse d'echecs",
        ],
    ),
    (
        "Craft / Trade / Industry",
        [
            "artisan",
            "artisane",
            "fabricant",
            "fabricante",
            "manufacturier",
            "manufacturiere",
            "ouvrier",
            "ouvriere",
            "agriculteur",
            "agricultrice",
            "jardinier",
            "jardiniere",
            "tisserand",
            "tisserande",
            "cordonnier",
            "cordonniere",
            "relieur",
            "relieuse",
            "drapier",
            "charpentier",
            "charpentiere",
            "faussaire",
            "metallurgiste",
            "stenographe",
            "tailleur",
            "tailleuse",
            "domestique",
            "femme de menage",
            "paysan",
            "paysanne",
            "forgeron",
            "forgeronne",
            "garde du corps",
            "escrimeur",
            "escrimeuse",
            "eleveur",
        ],
    ),
]


def split_pipe_values(value) -> list[str]:
    if pd.isna(value):
        return []
    return [token.strip() for token in str(value).split("|") if token.strip()]


def join_values(values) -> object:
    cleaned = []
    seen = set()
    for value in values:
        if pd.isna(value):
            continue
        token = str(value).strip()
        if token and token not in seen:
            seen.add(token)
            cleaned.append(token)
    if not cleaned:
        return pd.NA
    return " | ".join(cleaned)


def normalize_label(value) -> str:
    if pd.isna(value):
        return ""
    raw_text = (
        str(value)
        .lower()
        .replace("\u0153", "oe")
        .replace("\u00e6", "ae")
        .replace("\u2013", "-")
        .replace("\u2014", "-")
        .replace("\u2011", "-")
        .replace("\u2019", "'")
    )
    normalized = unicodedata.normalize("NFKD", raw_text)
    ascii_text = "".join(char for char in normalized if not unicodedata.combining(char))
    return re.sub(r"\s+", " ", ascii_text).strip()


def qid_from_uri(value) -> object:
    if pd.isna(value):
        return pd.NA
    return str(value).strip().rstrip("/").rsplit("/", 1)[-1]


def qid_uri(qid: str) -> str:
    return f"http://www.wikidata.org/entity/{qid}"


def bool_value(value) -> bool:
    if isinstance(value, bool):
        return value
    if pd.isna(value):
        return False
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def classify_bucket(occupation_id: str, occupation_label: str) -> tuple[str, str, str]:
    if occupation_id in EXACT_QID_BUCKETS:
        bucket, basis = EXACT_QID_BUCKETS[occupation_id]
        return bucket, BUCKET_FAMILIES[bucket], f"exact_qid:{basis}"

    normalized = normalize_label(occupation_label)
    for bucket, patterns in RULE_BUCKETS:
        for pattern in patterns:
            if pattern in normalized:
                return bucket, BUCKET_FAMILIES[bucket], f"label_rule:{pattern}"

    return "Unmapped / Review", BUCKET_FAMILIES["Unmapped / Review"], "unmapped"


def load_raw_occupation_rows(project_root: Path, cohort_id: str) -> pd.DataFrame:
    paths = cohort_paths(project_root, cohort_id)
    if not paths.raw_enrichment_path.exists():
        raise SystemExit(f"Missing raw enrichment CSV for {cohort_id}: {paths.raw_enrichment_path}")

    columns = [
        "person",
        "personLabel",
        "occupation_ids",
        "occupation_labels",
        "writerly_occupation_ids",
        "nonwriter_occupation_ids",
    ]
    df = pd.read_csv(paths.raw_enrichment_path, usecols=columns).replace(r"^\s*$", pd.NA, regex=True)
    rows = []
    for row in df.itertuples(index=False):
        occupation_ids = split_pipe_values(row.occupation_ids)
        occupation_labels = split_pipe_values(row.occupation_labels)
        writerly_ids = set(split_pipe_values(row.writerly_occupation_ids))
        nonwriter_ids = set(split_pipe_values(row.nonwriter_occupation_ids))
        for index, occupation_id in enumerate(occupation_ids):
            label = occupation_labels[index] if index < len(occupation_labels) else pd.NA
            rows.append(
                {
                    "cohort_id": cohort_id,
                    "wikidata_id": row.person,
                    "person_id": qid_from_uri(row.person),
                    "name": row.personLabel,
                    "occupation_id": occupation_id,
                    "occupation_label": label,
                    "occupation_wikidata_url": f"https://www.wikidata.org/wiki/{occupation_id}",
                    "is_writerly_occupation": occupation_id in writerly_ids,
                    "is_nonwriter_occupation": occupation_id in nonwriter_ids,
                }
            )
    return pd.DataFrame(rows)


def build_crosswalk(project_root: Path, cohort_ids: list[str]) -> pd.DataFrame:
    occupation_rows = pd.concat(
        [load_raw_occupation_rows(project_root, cohort_id) for cohort_id in cohort_ids],
        ignore_index=True,
    )

    label_lookup = (
        occupation_rows.dropna(subset=["occupation_label"])
        .groupby("occupation_id")["occupation_label"]
        .agg(lambda values: values.value_counts().index[0])
        .to_dict()
    )
    rows = []
    for occupation_id, group in occupation_rows.groupby("occupation_id"):
        label = label_lookup.get(occupation_id, pd.NA)
        bucket, family, basis = classify_bucket(occupation_id, label)
        row = {
            "occupation_id": occupation_id,
            "occupation_label": label,
            "occupation_wikidata_url": f"https://www.wikidata.org/wiki/{occupation_id}",
            "granular_bucket": bucket,
            "bucket_family": family,
            "bucket_method": basis,
            "manual_review_status": "suggested" if bucket != "Unmapped / Review" else "needs_review",
            "is_writerly_seen": bool(group["is_writerly_occupation"].any()),
            "is_nonwriter_seen": bool(group["is_nonwriter_occupation"].any()),
            "all_cohorts_entity_count": int(group["wikidata_id"].nunique()),
            "all_cohorts_assignment_count": int(len(group)),
            "notes": pd.NA,
        }
        for cohort_id in cohort_ids:
            cohort_group = group.loc[group["cohort_id"] == cohort_id]
            row[f"{cohort_id}_entity_count"] = int(cohort_group["wikidata_id"].nunique())
            row[f"{cohort_id}_assignment_count"] = int(len(cohort_group))
        rows.append(row)

    crosswalk_df = pd.DataFrame(rows)
    existing_path = project_root / REFERENCE_PATH
    if existing_path.exists():
        existing_df = pd.read_csv(existing_path).replace(r"^\s*$", pd.NA, regex=True)
        preserved_columns = ["granular_bucket", "bucket_family", "manual_review_status", "notes"]
        preserved = existing_df.set_index("occupation_id")
        for index, row in crosswalk_df.iterrows():
            occupation_id = row["occupation_id"]
            if occupation_id not in preserved.index:
                continue
            old_status = str(preserved.at[occupation_id, "manual_review_status"]).strip().lower()
            if old_status in {"reviewed", "override"}:
                for column in preserved_columns:
                    if column in preserved.columns and pd.notna(preserved.at[occupation_id, column]):
                        crosswalk_df.at[index, column] = preserved.at[occupation_id, column]
                crosswalk_df.at[index, "bucket_method"] = "manual_override"

    return crosswalk_df.sort_values(
        ["manual_review_status", "all_cohorts_entity_count", "occupation_label"],
        ascending=[True, False, True],
    )


def load_crosswalk(project_root: Path) -> pd.DataFrame:
    path = project_root / REFERENCE_PATH
    if not path.exists():
        raise SystemExit(f"Missing occupation bucket crosswalk: {path}")
    return pd.read_csv(path).replace(r"^\s*$", pd.NA, regex=True)


def build_entity_rows(project_root: Path, cohort_id: str, crosswalk_df: pd.DataFrame) -> pd.DataFrame:
    occupation_rows = load_raw_occupation_rows(project_root, cohort_id)
    crosswalk_columns = [
        "occupation_id",
        "granular_bucket",
        "bucket_family",
        "manual_review_status",
        "bucket_method",
    ]
    rows = occupation_rows.merge(crosswalk_df[crosswalk_columns], on="occupation_id", how="left")
    rows["granular_bucket"] = rows["granular_bucket"].fillna("Unmapped / Review")
    rows["bucket_family"] = rows["bucket_family"].fillna(BUCKET_FAMILIES["Unmapped / Review"])
    rows["manual_review_status"] = rows["manual_review_status"].fillna("needs_review")
    rows["bucket_method"] = rows["bucket_method"].fillna("missing_crosswalk")
    return rows.sort_values(["name", "person_id", "granular_bucket", "occupation_label"])


def build_summary(entity_rows: pd.DataFrame) -> pd.DataFrame:
    total_entities = entity_rows["wikidata_id"].nunique()
    total_assignments = len(entity_rows)
    rows = []
    for bucket, group in entity_rows.groupby("granular_bucket", dropna=False):
        entity_count = int(group["wikidata_id"].nunique())
        assignment_count = int(len(group))
        rows.append(
            {
                "granular_bucket": bucket,
                "bucket_family": group["bucket_family"].dropna().iloc[0] if group["bucket_family"].notna().any() else pd.NA,
                "entity_count": entity_count,
                "entity_pct": round((entity_count / total_entities) * 100, 2) if total_entities else 0.0,
                "occupation_assignment_count": assignment_count,
                "assignment_pct": round((assignment_count / total_assignments) * 100, 2) if total_assignments else 0.0,
                "unique_occupation_count": int(group["occupation_id"].nunique()),
                "writerly_assignment_count": int(group["is_writerly_occupation"].sum()),
                "nonwriter_assignment_count": int(group["is_nonwriter_occupation"].sum()),
                "needs_review_occupation_count": int(
                    group.loc[group["manual_review_status"] == "needs_review", "occupation_id"].nunique()
                ),
            }
        )
    return pd.DataFrame(rows).sort_values(["entity_count", "granular_bucket"], ascending=[False, True])


def build_language_representation(
    project_root: Path,
    cohort_id: str,
    entity_rows: pd.DataFrame,
) -> pd.DataFrame:
    paths = cohort_paths(project_root, cohort_id)
    columns = ["wikidata_id", *WIKI_COLUMNS.values()]
    enriched_df = pd.read_csv(paths.enriched_path, usecols=columns).drop_duplicates("wikidata_id")
    enriched_df["person_id"] = enriched_df["wikidata_id"].map(qid_from_uri)
    for column in WIKI_COLUMNS.values():
        enriched_df[column] = enriched_df[column].apply(bool_value)

    entity_bucket_df = (
        entity_rows[["wikidata_id", "granular_bucket", "bucket_family"]]
        .drop_duplicates()
        .merge(enriched_df, on="wikidata_id", how="left")
    )
    total_entities = enriched_df["wikidata_id"].nunique()
    rows = []
    for wiki_code, column in WIKI_COLUMNS.items():
        language_represented_entities = int(enriched_df[column].sum())
        for bucket, group in entity_bucket_df.groupby("granular_bucket", dropna=False):
            bucket_entities = int(group["wikidata_id"].nunique())
            represented_entities = int(group.loc[group[column], "wikidata_id"].nunique())
            rows.append(
                {
                    "language_edition": wiki_code,
                    "granular_bucket": bucket,
                    "bucket_family": group["bucket_family"].dropna().iloc[0] if group["bucket_family"].notna().any() else pd.NA,
                    "represented_entities": represented_entities,
                    "bucket_entities": bucket_entities,
                    "language_represented_entities": language_represented_entities,
                    "cohort_total_entities": total_entities,
                    "bucket_entity_share_pct": round((bucket_entities / total_entities) * 100, 2) if total_entities else 0.0,
                    "language_bucket_share_pct": (
                        round((represented_entities / language_represented_entities) * 100, 2)
                        if language_represented_entities else 0.0
                    ),
                    "representation_rate_pct": (
                        round((represented_entities / bucket_entities) * 100, 2) if bucket_entities else 0.0
                    ),
                }
            )
    return pd.DataFrame(rows).sort_values(["language_edition", "represented_entities"], ascending=[True, False])


def build_crosswalk_summary(crosswalk_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for bucket, group in crosswalk_df.groupby("granular_bucket", dropna=False):
        rows.append(
            {
                "granular_bucket": bucket,
                "bucket_family": group["bucket_family"].dropna().iloc[0] if group["bucket_family"].notna().any() else pd.NA,
                "occupation_count": len(group),
                "occupation_entity_count_sum": int(group["all_cohorts_entity_count"].sum()),
                "needs_review_count": int((group["manual_review_status"] == "needs_review").sum()),
                "top_occupation_labels": join_values(
                    group.sort_values("all_cohorts_entity_count", ascending=False)["occupation_label"].head(8)
                ),
            }
        )
    return pd.DataFrame(rows).sort_values(["occupation_entity_count_sum", "granular_bucket"], ascending=[False, True])


def parse_args() -> object:
    parser = ArgumentParser(description="Build occupation bucket crosswalk and analysis tables.")
    parser.add_argument(
        "--cohort-id",
        action="append",
        choices=COHORT_IDS,
        help="Cohort to build processed outputs for. Can be repeated. Default: both cohorts.",
    )
    parser.add_argument(
        "--crosswalk-only",
        action="store_true",
        help="Only regenerate data/reference/occupation_bucket_crosswalk_seed.csv.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    project_root = Path(__file__).resolve().parents[2]
    cohort_ids = args.cohort_id or COHORT_IDS

    reference_path = project_root / REFERENCE_PATH
    reference_path.parent.mkdir(parents=True, exist_ok=True)
    crosswalk_df = build_crosswalk(project_root, COHORT_IDS)
    crosswalk_df.to_csv(reference_path, index=False)

    crosswalk_summary = build_crosswalk_summary(crosswalk_df)
    crosswalk_summary_path = project_root / "data" / "processed" / "occupation_bucket_crosswalk_summary.csv"
    crosswalk_summary_path.parent.mkdir(parents=True, exist_ok=True)
    crosswalk_summary.to_csv(crosswalk_summary_path, index=False)

    if not args.crosswalk_only:
        crosswalk_df = load_crosswalk(project_root)
        for cohort_id in cohort_ids:
            paths = cohort_paths(project_root, cohort_id)
            paths.processed_dir.mkdir(parents=True, exist_ok=True)
            entity_rows = build_entity_rows(project_root, cohort_id, crosswalk_df)
            summary_df = build_summary(entity_rows)
            language_df = build_language_representation(project_root, cohort_id, entity_rows)

            entity_rows.to_csv(paths.processed_dir / "occupation_bucket_entities_long.csv", index=False)
            summary_df.to_csv(paths.processed_dir / "occupation_bucket_summary.csv", index=False)
            language_df.to_csv(paths.processed_dir / "occupation_bucket_language_representation.csv", index=False)

    print("Occupation bucket tables complete.")
    print(f"Crosswalk rows: {len(crosswalk_df)}")
    print(f"Crosswalk: {reference_path}")
    print(f"Crosswalk summary: {crosswalk_summary_path}")
    if not args.crosswalk_only:
        print(f"Cohorts: {', '.join(cohort_ids)}")
    print(crosswalk_summary.head(12).to_string(index=False))


if __name__ == "__main__":
    main()

