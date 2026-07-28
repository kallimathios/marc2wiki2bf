"""
MARC / Spreadsheet -> Wikidata Reconciliation -> BIBFRAME RDF
================================================================

Purpose
-------
Take local bibliographic records (MARC or a CSV/JSON spreadsheet),
reconcile them against Wikidata to pick up stable external links and
disambiguated identifiers, then emit proper BIBFRAME RDF (Work +
Instance, per the LC BIBFRAME 2.x model) in Turtle, JSON-LD, RDF/XML,
or N-Triples.

This follows the modeling logic in Mathios, Doğan & Nelson (2026),
"Refining Wikidata's Bibliographic Domain to Increase Reuse in GLAMs"
(JOHD, DOI 10.5334/johd.454): match at the most specific level you can
(identifiers first), keep Work-level and Instance-level entities
distinct rather than collapsing them, and never auto-write an
uncertain match -- route it to human review instead.

Note on BIBFRAME vs FRBR/LRM: BIBFRAME's core model has three tiers --
Work, Instance, and Item (Item, physical/holdings-level copy
information, is out of scope here since MARC bibliographic records
don't carry it). This pipeline only deals with Work and Instance, so
the Wikidata "edition or translation" class (Q3331189, or a subclass)
maps to bf:Instance, and Wikidata "work" (Q386724), "film" (Q11424),
or "film series" (Q24856) -- or any of their subclasses -- map to
bf:Work. (BIBFRAME also defines a fourth entity, bf:Hub, which is
NOT equivalent to a FRBR Work and isn't produced by this pipeline.)

Pipeline stages
---------------
1. load_records()          MARC (.mrc) or spreadsheet (.csv/.json) -> Record
2. find_candidates() /
   score_candidates()      reconcile each Record against Wikidata
3. build_bibframe_graph()  emit bf:Work + bf:Instance nodes into an
                           rdflib.Graph, with rdfs:seeAlso links to
                           Wikidata for confident matches only
4. serialize                write out as turtle / json-ld / xml / nt
5. write_review_csv()      side-channel report for anything that
                           needs human eyes before it's trusted

Usage
-----
    python bibframe_pipeline.py catalog.mrc \
        --out catalog.ttl --format turtle \
        --review-out needs_review.csv

    python bibframe_pipeline.py catalog.csv \
        --out catalog.jsonld --format json-ld

Spreadsheet input columns (any subset):
    title, author, pub_date, isbn13, isbn10, issn, oclc, lccn, doi, viaf_id

MARC input: standard ISO 2709 (.mrc) binary MARC21, read via pymarc.
"""

import argparse
import csv
import json
import re
import sys
import time
import unicodedata
import uuid
from dataclasses import dataclass, field
from typing import Optional

import requests
from rapidfuzz import fuzz
from rdflib import Graph, Namespace, URIRef, Literal, BNode
from rdflib.namespace import RDF, RDFS

try:
    import pymarc
except ImportError:
    pymarc = None

# ---------------------------------------------------------------------------
# NAMESPACES
# ---------------------------------------------------------------------------

BF = Namespace("http://id.loc.gov/ontologies/bibframe/")
BFLC = Namespace("http://id.loc.gov/ontologies/bflc/")
WD = Namespace("http://www.wikidata.org/entity/")
RDAROLE = Namespace("http://id.loc.gov/vocabulary/relators/")

# LC vocabulary bases used by the additional field mappings below, per
# the LC marc2bibframe2 XSLT stylesheet (variables.xsl and the
# ConvSpec-3XX.xsl / ConvSpec-010-048.xsl / ConvSpec-050-088.xsl rules).
CONTENT_TYPES_BASE = "http://id.loc.gov/vocabulary/contentTypes/"
MEDIA_TYPES_BASE = "http://id.loc.gov/vocabulary/mediaTypes/"
CARRIERS_BASE = "http://id.loc.gov/vocabulary/carriers/"
LANGUAGES_BASE = "http://id.loc.gov/vocabulary/languages/"

# Resource URI patterns for the target database, e.g.:
#   https://dev.bcld.info/works/b3dbc4bb-0bb5-4c1b-987b-2352e49109ee
#   https://dev.bcld.info/instances/41163846-570e-4d62-90d7-5ccb50edc4e2
WORK_BASE = "https://dev.bcld.info/works/"
INSTANCE_BASE = "https://dev.bcld.info/instances/"
WORK_NS = Namespace(WORK_BASE)
INSTANCE_NS = Namespace(INSTANCE_BASE)

WIKIDATA_API = "https://www.wikidata.org/w/api.php"
WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
USER_AGENT = "BiblioReconciliationBot/0.2 (contact: you@example.org)"

# Wikidata identifier properties, in priority order (highest confidence first)
ID_PROPERTIES = {
    "isbn13": "P212",
    "isbn10": "P957",
    "issn": "P236",
    "oclc": "P243",
    "lccn": "P244",
    "doi": "P356",
    "viaf_id": "P214",
}

COLUMN_MAP = {  # rename values if your spreadsheet uses different headers
    "title": "title",
    "author": "author",
    "pub_date": "pub_date",
    "isbn13": "isbn13",
    "isbn10": "isbn10",
    "issn": "issn",
    "oclc": "oclc",
    "lccn": "lccn",
    "doi": "doi",
    "viaf_id": "viaf_id",
}

# Fallback header names tried (case-insensitively) when the exact
# COLUMN_MAP value isn't present in the source file's actual headers.
# This covers common real-world export formats -- e.g. Goodreads CSV
# exports use "Title", "Author", "ISBN13", "ISBN" (10-digit), and
# "Original Publication Year" / "Year Published" rather than this
# pipeline's lowercase/underscore convention -- without requiring
# every user to rename their columns first. COLUMN_MAP above still
# takes priority if you've explicitly customized it.
COLUMN_ALIASES = {
    "title": ["title", "book title", "name"],
    "author": ["author", "author l-f", "authors", "creator"],
    "pub_date": ["original publication year", "publication year", "pub_date",
                 "year published", "publish date", "date published", "date"],
    "isbn13": ["isbn13", "isbn-13", "isbn 13"],
    "isbn10": ["isbn10", "isbn-10", "isbn 10", "isbn"],
    "issn": ["issn"],
    "oclc": ["oclc", "oclc number", "oclc#"],
    "lccn": ["lccn"],
    "doi": ["doi"],
    "viaf_id": ["viaf_id", "viaf", "viaf id"],
}

# ---------------------------------------------------------------------------
# SAMPLE FILES
# ---------------------------------------------------------------------------
# Real-data samples for testing/demoing this pipeline (see README.md
# "Sample files" for a full description of each). All MARC samples
# below (except the two called out individually) are drawn from
# distinct, non-overlapping slices of real bibliographic export
# files, so running through several of them exercises a broad range
# of MARC cataloging styles rather than one narrow slice.
SAMPLE_FILES = [
    "sample_input.csv",                # 3 spreadsheet rows
    "sample_MARC_inventory-2443.mrc",  # 3 real MARC21 book records
    "sample_MARC_av_batch-6.mrc",      # 6 real MARC21 AV/film records
    "sample_MARC_batch2-7.mrc",        # 7 more real MARC21 records (2nd batch)
    "MARC-sample-20260722-2455.mrc",   # 10 real MARC21 book/media records
    # 8 batches of 25 records each, taken from the first 200 records
    # of BIB250403.mrc (records 0-199; everything after record 199 in
    # that source file was intentionally not sampled from):
    "sample_BIB250403_batch1-25.mrc",   # records 0-24
    "sample_BIB250403_batch2-25.mrc",   # records 25-49
    "sample_BIB250403_batch3-25.mrc",   # records 50-74
    "sample_BIB250403_batch4-25.mrc",   # records 75-99
    "sample_BIB250403_batch5-25.mrc",   # records 100-124
    "sample_BIB250403_batch6-25.mrc",   # records 125-149
    "sample_BIB250403_batch7-25.mrc",   # records 150-174
    "sample_BIB250403_batch8-25.mrc",   # records 175-199
    # 8 more batches of 25, but selected differently: BIB250403.mrc has
    # 68,100 total records, and these 200 are the highest-scoring ones
    # from across the WHOLE file, ranked by (identifiers our pipeline
    # extracts: 010/020/022/035) + (count of $0/$1 subfields anywhere
    # in the record). Roughly 70-85% of records in each batch have a
    # populated author_identifiers (vs. ~10-20% in the plain-sequential
    # batches above), making these better for exercising the person-
    # identity verification path in find_candidates()/score_candidates().
    "sample_BIB250403_richIDs_batch1-25.mrc",
    "sample_BIB250403_richIDs_batch2-25.mrc",
    "sample_BIB250403_richIDs_batch3-25.mrc",
    "sample_BIB250403_richIDs_batch4-25.mrc",
    "sample_BIB250403_richIDs_batch5-25.mrc",
    "sample_BIB250403_richIDs_batch6-25.mrc",
    "sample_BIB250403_richIDs_batch7-25.mrc",
    "sample_BIB250403_richIDs_batch8-25.mrc",
    # 8 more batches of 25: academic journals specifically, selected as
    # records where Leader/07 == 's' (serial), 008/21 == 'p'
    # (periodical, not newspaper/monographic series/etc.), and the
    # title contains "journal" (case-insensitive) -- with "e-flux"
    # explicitly excluded per request (though this filter combination
    # already excludes it naturally, since e-flux Journal issues are
    # cataloged as individual monographs, not a continuing periodical
    # record). 200 records sampled evenly across all 1,449 records in
    # BIB250403.mrc that matched this filter, for topical diversity
    # across disciplines rather than clustering near the file's start.
    "sample_BIB250403_journals_batch1-25.mrc",
    "sample_BIB250403_journals_batch2-25.mrc",
    "sample_BIB250403_journals_batch3-25.mrc",
    "sample_BIB250403_journals_batch4-25.mrc",
    "sample_BIB250403_journals_batch5-25.mrc",
    "sample_BIB250403_journals_batch6-25.mrc",
    "sample_BIB250403_journals_batch7-25.mrc",
    "sample_BIB250403_journals_batch8-25.mrc",
]

AUTO_ACCEPT_THRESHOLD = 100        # identifier match only
AUTO_ACCEPT_PERSON_THRESHOLD = 90  # title+person-identity match (see find_candidates)
REVIEW_THRESHOLD = 78              # fuzzy score above this -> "needs review"


# ---------------------------------------------------------------------------
# DATA MODEL
# ---------------------------------------------------------------------------

@dataclass
class Candidate:
    qid: str
    label: str = ""
    description: str = ""
    match_type: str = ""      # "identifier" | "fuzzy"
    matched_property: str = ""
    score: float = 0.0
    entity_role: str = ""     # "work" | "instance" | "unknown"
    person_verified: bool = False  # candidate's creator/director confirmed via
                                    # an authority identifier ($0/$1), not just
                                    # fuzzy title text -- see find_candidates()


@dataclass
class Record:
    raw: dict
    local_id: str = ""        # e.g. MARC 001 control number, or row index
    title: str = ""
    subtitle: str = ""        # 245 $b, kept separate from title -- real LC BIBFRAME
                               # output uses distinct bf:mainTitle/bf:subtitle
                               # properties on the Title node, not one concatenated string
    author: str = ""
    pub_date: str = ""
    publisher: str = ""
    identifiers: dict = field(default_factory=dict)
    # Identifiers for the AUTHOR/CONTRIBUTOR (a person or corporate body),
    # extracted from $0 (authority record URI) / $1 (Real World Object URI)
    # on the same MARC field the `author` name came from. These identify a
    # PERSON, not the bibliographic work -- used to verify title-search
    # candidates against a known creator, not as a work-level identifier.
    author_identifiers: dict = field(default_factory=dict)

    # --- Additional BIBFRAME-mappable fields, per the LC marc2bibframe2
    # XSLT stylesheet (see README "Additional BIBFRAME properties" for
    # the exact MARC-tag -> bf:property mapping each of these follows) ---
    edition_statement: str = ""              # 250 $a -> bf:editionStatement
    extent: str = ""                         # 300 $a/$f/$g -> bf:extent/bf:Extent
    dimensions: str = ""                     # 300 $c -> bf:dimensions
    content_type: dict = field(default_factory=dict)  # 336 -> bf:content/bf:Content
    media_type: dict = field(default_factory=dict)    # 337 -> bf:media/bf:Media
    carrier_type: dict = field(default_factory=dict)  # 338 -> bf:carrier/bf:Carrier
    languages: list = field(default_factory=list)      # 041 -> bf:language/bf:Language
    notes: list = field(default_factory=list)           # 500 -> bf:note/bf:Note
    summaries: list = field(default_factory=list)        # 520 -> bf:summary/bf:Summary
    table_of_contents: str = ""              # 505 -> bf:tableOfContents/bf:TableOfContents
    subjects: list = field(default_factory=list)          # 650/651/655 -> bf:subject|bf:genreForm
    series_statements: list = field(default_factory=list)  # 490 -> bf:seriesStatement
    classifications: list = field(default_factory=list)     # 050/082/060 -> bf:classification

    # --- Additional fields, mapped against the Blue Core Sinopia resource
    # templates for bf:Work (bluecore:bf2:Monograph:Work) and bf:Instance
    # (bluecore:bf2:Monograph:Instance) -- see README "Blue Core resource
    # template alignment" for the full property-by-property mapping. ---
    work_title: str = ""             # 130/240 uniform title, when present
                                      # (distinct from Instance's 245 title,
                                      # per the profile's Work/Instance
                                      # title split); falls back to `title`
                                      # (245) when no 130/240 exists.
    variant_titles: list = field(default_factory=list)      # 246 -> bf:title (variant)
    contributors: list = field(default_factory=list)         # 700/710 (not chosen as
                                      # primary `author`) -> bf:contribution
                                      # (AdditionalContribution), Work-level
    government_pub_type: str = ""    # 008/28 -> bflc:governmentPubType
    origin_date: str = ""            # 008 date1 -> bf:originDate
    origin_place_code: str = ""      # 008/15-17 -> bf:place (nested in provisionActivity)
    intended_audience_code: str = "" # 008/22 -> bf:intendedAudience
    illustrative_content_codes: list = field(default_factory=list)  # 008/18-21
    dissertation_note: str = ""      # 502 -> bf:dissertation
    responsibility_statement: str = ""  # 245 $c -> bf:responsibilityStatement (Instance)
    copyright_date: str = ""         # 264 ind2=4 $c -> bf:copyrightDate (Instance)
    issuance_code: str = ""          # Leader/07 -> bf:issuance (Instance, REQUIRED)
    electronic_locators: list = field(default_factory=list)  # 856 $u -> bf:electronicLocator
    other_identifiers: dict = field(default_factory=dict)     # EAN (024), National
                                      # Bibliography Number (015) -> bf:identifiedBy;
                                      # NOT used for Wikidata reconciliation (unlike
                                      # `identifiers`), RDF-output only.

    candidates: list = field(default_factory=list)
    decision: str = "no_match"   # "auto_accept" | "needs_review" | "no_match"
    best: Optional[Candidate] = None


# ---------------------------------------------------------------------------
# 1. LOAD  (MARC or spreadsheet)
# ---------------------------------------------------------------------------

def load_records(path: str) -> list[Record]:
    if path.lower().endswith((".mrc", ".marc", ".dat")):
        return _load_marc(path)
    elif path.lower().endswith(".json"):
        with open(path, encoding="utf-8") as f:
            rows = json.load(f)
        return [normalize_spreadsheet_row(row, i) for i, row in enumerate(rows)]
    else:  # assume CSV
        with open(path, encoding="utf-8-sig", newline="") as f:
            rows = list(csv.DictReader(f))
        return [normalize_spreadsheet_row(row, i) for i, row in enumerate(rows)]


def _clean_text(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s).strip()
    return " ".join(s.split())


def _chop_isbd_punct(text: str) -> str:
    """Strip trailing ISBD delimiter punctuation (colon, semicolon,
    comma, slash, and surrounding whitespace) -- matches the LC
    XSLT's tChopPunct pattern. Used when splitting 245 $a/$b so the
    ISBD delimiter that introduced the next part doesn't stay attached
    to the text (confirmed against real id.loc.gov BIBFRAME output,
    e.g. "Masked by trust" rather than "Masked by trust :"). Doesn't
    strip a bare trailing period, since that can be meaningful text
    (an abbreviation, or genuinely part of the title) rather than an
    ISBD delimiter."""
    return re.sub(r"\s*[:;,/]+\s*$", "", text or "").strip()


def _clean_isbn(s: str) -> str:
    return "".join(ch for ch in (s or "") if ch.isalnum())


def _normalize_header(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def _get_column_value(row_lc: dict, key: str) -> str:
    """Look up a canonical field (e.g. 'title', 'isbn13') in a
    case-insensitively-keyed row dict. Tries the explicit COLUMN_MAP
    override first (if the user customized it away from the identity
    mapping), then falls back through COLUMN_ALIASES in order, all
    matched case-insensitively against the source file's actual
    headers. Returns '' if nothing matches."""
    override = COLUMN_MAP.get(key, key)
    if override != key:
        val = row_lc.get(_normalize_header(override))
        if val:
            return val
    for alias in COLUMN_ALIASES.get(key, [key]):
        val = row_lc.get(_normalize_header(alias))
        if val:
            return val
    return ""


def normalize_spreadsheet_row(row: dict, idx: int) -> Record:
    row_lc = {_normalize_header(k): v for k, v in row.items()}
    get = lambda key: _clean_text(_get_column_value(row_lc, key))
    id_field = row_lc.get("id") or row_lc.get("book id") or ""
    r = Record(
        raw=row,
        local_id=_clean_text(id_field) or f"row{idx}",
        title=get("title"),
        author=get("author"),
        pub_date=get("pub_date"),
    )
    for key in ID_PROPERTIES:
        val = _get_column_value(row_lc, key)
        if val:
            val = _clean_isbn(val) if "isbn" in key else _clean_text(val)
            if val:
                r.identifiers[key] = val
    return r


def _load_marc(path: str) -> list[Record]:
    """Parse binary MARC21 (.mrc) using pymarc into Record objects.

    Field mapping used here (standard MARC21 bibliographic tags):
      001        control number -> local_id
      020 $a     ISBN (10 or 13, cleaned to determine which)
      022 $a     ISSN
      010 $a     LCCN
      035 $a     OCLC / other system control number (best-effort)
      100/110/111 $a  main author/creator (books, most print material)
      700 $a (role=director, else first) / 710 $a  -- fallback author
                 for audiovisual/moving-image records with no 1XX
                 main entry (single primary contributor only; does
                 not capture full cast/crew across repeated 700/710s)
      245 $a $b  title (+ subtitle)
      260/264 $b $c  publisher / date
    """
    if pymarc is None:
        raise ImportError("pymarc is required to read MARC files: "
                           "pip install pymarc")
    records = []
    with open(path, "rb") as fh:
        reader = pymarc.MARCReader(fh, to_unicode=True, force_utf8=True)
        for i, marc in enumerate(reader):
            if marc is None:
                continue
            records.append(_marc_to_record(marc, i))
    return records


def _marc_field_value(marc, tag: str, code: str = "a") -> str:
    try:
        field_ = marc[tag]
    except KeyError:
        return ""
    if field_ is None:
        return ""
    try:
        return _clean_text(field_[code] or "")
    except (KeyError, TypeError):
        return ""


_LC_NAF_RE = re.compile(r"id\.loc\.gov/authorities/names/([a-z0-9]+)", re.IGNORECASE)
_VIAF_RE = re.compile(r"viaf\.org/viaf/(\d+)", re.IGNORECASE)
_ISNI_RE = re.compile(r"isni\.org/isni/([0-9xX]+)", re.IGNORECASE)
_WIKIDATA_ENTITY_RE = re.compile(r"wikidata\.org/entity/(Q\d+)", re.IGNORECASE)


def _extract_agent_identifiers(marc_field) -> dict:
    """Parse $0 (authority record URI) and $1 (Real World Object URI)
    on a MARC field into structured, Wikidata-queryable identifiers for
    the person/corporate body that field names.

    Handles the shapes actually seen in real MARC data:
      - http://id.loc.gov/authorities/names/n2001021740
            -> LC NAF ID ("lccn_authority"; maps to Wikidata P244, the
               same property used for LCCN on bibliographic records)
      - http://viaf.org/viaf/12345          -> VIAF ID ("viaf_id", P214)
      - http://isni.org/isni/0000000123456789 -> ISNI ("isni", P213)
      - http://www.wikidata.org/entity/Q12345 -> direct QID
        ("wikidata_qid" -- already resolved, no query needed)
      - https://id.oclc.org/worldcat/entity/...
            -> OCLC WorldCat Entity URI -- NOT mapped to any current
               Wikidata property, intentionally ignored
      - (SIRSI)12345 or similar bracketed local system control numbers
            -- purely local to the source ILS, not externally
               resolvable, intentionally ignored

    Returns {} if nothing recognized (e.g. only a local control number
    was present). Never raises on unexpected input.
    """
    out = {}
    try:
        values = marc_field.get_subfields("0") + marc_field.get_subfields("1")
    except AttributeError:
        return out
    for val in values:
        if not val:
            continue
        m = _WIKIDATA_ENTITY_RE.search(val)
        if m:
            out.setdefault("wikidata_qid", m.group(1))
            continue
        m = _VIAF_RE.search(val)
        if m:
            out.setdefault("viaf_id", m.group(1))
            continue
        m = _ISNI_RE.search(val)
        if m:
            out.setdefault("isni", m.group(1))
            continue
        m = _LC_NAF_RE.search(val)
        if m:
            out.setdefault("lccn_authority", m.group(1))
            continue
        # Unrecognized (OCLC WorldCat Entity URIs, local (SIRSI)
        # control numbers, etc.) -- no Wikidata property to query
        # these against, so they're not stored.
    return out


def _first_field_with_identifiers(marc, tag: str, code: str = "a",
                                   require_subfield_e_contains: str = None) -> tuple:
    """Like the old _first_repeated_field_subfield, but also returns
    the $0/$1-derived agent identifiers from the SAME field instance
    the name subfield came from -- so the identifiers are guaranteed
    to describe the same person, not just any field with the tag."""
    try:
        fields = marc.get_fields(tag)
    except AttributeError:
        return "", {}
    for f in fields:
        if require_subfield_e_contains:
            try:
                relator = (f["e"] or "").lower()
            except (KeyError, TypeError):
                relator = ""
            if require_subfield_e_contains.lower() not in relator:
                continue
        try:
            val = f[code]
        except (KeyError, TypeError):
            val = None
        if val:
            return _clean_text(val), _extract_agent_identifiers(f)
    return "", {}


def _extract_content_media_carrier(marc, tag: str) -> dict:
    """336/337/338 all follow the same $a (label) + $b (controlled
    code) shape per ConvSpec-3XX.xsl's shared 'rdaResource' template.
    Returns {'code': ..., 'label': ...} from the first occurrence, or
    {} if the tag isn't present. Only the first occurrence is taken,
    matching this pipeline's existing single-value-per-field pattern."""
    try:
        f = marc[tag]
    except KeyError:
        return {}
    if f is None:
        return {}
    try:
        code = f["b"]
    except (KeyError, TypeError):
        code = None
    if not code:
        return {}
    try:
        label = f["a"]
    except (KeyError, TypeError):
        label = None
    out = {"code": _clean_text(code)}
    if label:
        out["label"] = _clean_text(label)
    return out


def _extract_languages(marc) -> list:
    """041 -> bf:language. Real MARC 041 can stack multiple 3-letter
    codes in one $a (e.g. 'engfre' = English + French); this splits
    on that convention. Does not attempt the full ConvSpec-010-048.xsl
    logic (translation direction via $h/$k, ISO 639-1 vs -2 source
    disambiguation, etc.) -- just the common case of one or more
    stacked MARC language codes in $a."""
    try:
        f = marc["041"]
    except KeyError:
        return []
    if f is None:
        return []
    try:
        values = f.get_subfields("a")
    except AttributeError:
        return []
    codes = []
    for val in values:
        val = re.sub(r"\s+", "", val or "")
        if len(val) >= 3 and len(val) % 3 == 0:
            codes.extend(val[i:i + 3] for i in range(0, len(val), 3))
        elif val:
            codes.append(val)
    seen = set()
    out = []
    for c in codes:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


def _extract_notes(marc) -> list:
    """500 (general note) -> bf:note. Only the general note tag; the
    many specialized 5XX note types (501, 545, 555, etc. -- see
    ConvSpec-5XX.xsl for the full list with per-tag mnotetype codes)
    aren't broken out separately here."""
    try:
        fields = marc.get_fields("500")
    except AttributeError:
        return []
    out = []
    for f in fields:
        try:
            val = f["a"]
        except (KeyError, TypeError):
            val = None
        if val:
            out.append(_clean_text(val))
    return out


def _extract_summaries(marc) -> list:
    """520 ($a/$b/$c concatenated) -> bf:summary."""
    try:
        fields = marc.get_fields("520")
    except AttributeError:
        return []
    out = []
    for f in fields:
        parts = []
        for code in ("a", "b", "c"):
            try:
                parts.extend(f.get_subfields(code))
            except AttributeError:
                pass
        text = _clean_text(" ".join(parts))
        if text:
            out.append(text)
    return out


def _extract_table_of_contents(marc) -> str:
    """505 ($a/$g/$r/$t concatenated) -> bf:tableOfContents. Only the
    first occurrence is taken."""
    try:
        f = marc["505"]
    except KeyError:
        return ""
    if f is None:
        return ""
    parts = []
    for code in ("a", "g", "r", "t"):
        try:
            parts.extend(f.get_subfields(code))
        except AttributeError:
            pass
    return _clean_text(" ".join(parts))


def _extract_subjects(marc) -> list:
    """650 (topic) / 651 (place) / 655 (genre-form) -> bf:subject or
    bf:genreForm. Subdivisions ($v/$x/$y/$z) are joined with '--',
    matching how LCSH strings are conventionally displayed. 655 maps
    to bf:genreForm only when it has no subdivision subfields, per
    ConvSpec-600-662.xsl's rule; with subdivisions it's treated as a
    topical subject instead.

    650/651 are filtered to LCSH-sourced headings only (second
    indicator '0'), per the Blue Core profile's "Subject--LCSH only"
    property label -- headings from other thesauri (FAST, MeSH, local,
    etc., indicated by a different ind2 value or a $2 subfield) are
    excluded. 655 (genre/form) has no such restriction in the profile,
    so it's left unfiltered.

    Only 650/651/655 are handled -- the more complex name-based
    subject entries (600 person, 610 corporate body, 611 meeting,
    630 uniform title) are out of scope here since they'd need the
    same authority-matching machinery as the main author field."""
    out = []
    try:
        fields = marc.get_fields("650", "651", "655")
    except AttributeError:
        return out
    for f in fields:
        if f.tag in ("650", "651") and getattr(f, "indicator2", None) != "0":
            continue  # not LCSH -- skip per "Subject--LCSH only"
        try:
            main_parts = f.get_subfields("a")
        except AttributeError:
            main_parts = []
        try:
            subdivisions = f.get_subfields("v") + f.get_subfields("x") + \
                           f.get_subfields("y") + f.get_subfields("z")
        except AttributeError:
            subdivisions = []
        label_parts = [p for p in main_parts + subdivisions if p]
        label = _clean_text("--".join(label_parts))
        if not label:
            continue
        if f.tag == "651":
            subject_type = "Place"
        elif f.tag == "655" and not subdivisions:
            subject_type = "GenreForm"
        else:
            subject_type = "Topic"
        out.append({"type": subject_type, "label": label})
    return out


def _extract_series_statements(marc) -> list:
    """490 $a -> bf:seriesStatement (a plain literal per BIBFRAME,
    confirmed against the LC stylesheet's use of the property).
    Doesn't attempt the 490-vs-8XX de-duplication logic in
    ConvSpec-Process6-Series.xsl (which avoids double-counting a
    series statement that also has a linked 8XX added entry) --
    every 490 present is captured."""
    try:
        fields = marc.get_fields("490")
    except AttributeError:
        return []
    out = []
    for f in fields:
        try:
            val = f["a"]
        except (KeyError, TypeError):
            val = None
        if val:
            out.append(_clean_text(val))
    return out


def _extract_classifications(marc) -> list:
    """050 (LCC) / 082 (Dewey) / 060 (NLM) -> bf:classification. Returns
    a list of {'scheme': 'lcc'|'ddc'|'nlm', 'classification_portion': ..,
    'item_portion': ..} dicts."""
    out = []
    try:
        fields = marc.get_fields("050", "082", "060")
    except AttributeError:
        return out
    scheme_map = {"050": "lcc", "082": "ddc", "060": "nlm"}
    for f in fields:
        try:
            portions = f.get_subfields("a")
        except AttributeError:
            portions = []
        if not portions:
            continue
        try:
            item_portions = f.get_subfields("b")
        except AttributeError:
            item_portions = []
        entry = {
            "scheme": scheme_map[f.tag],
            "classification_portion": _clean_text(portions[0]),
        }
        if item_portions:
            entry["item_portion"] = _clean_text(item_portions[0])
        out.append(entry)
    return out


# ---------------------------------------------------------------------------
# Blue Core resource template alignment: additional extraction functions
# ---------------------------------------------------------------------------
# Each of these maps to a specific property in Blue Core's Sinopia resource
# templates for bf:Work (bluecore:bf2:Monograph:Work) and bf:Instance
# (bluecore:bf2:Monograph:Instance). See README "Blue Core resource
# template alignment" for the full property-by-property mapping table.

# LC vocabulary bases for 008 fixed-field coded values.
COUNTRIES_BASE = "http://id.loc.gov/vocabulary/countries/"
AUDIENCE_BASE = "http://id.loc.gov/vocabulary/maudience/"
ILLUSTRATIONS_BASE = "http://id.loc.gov/vocabulary/millus/"
GOVT_PUB_TYPE_BASE = "http://id.loc.gov/vocabulary/mgovtpubtype/"
ISSUANCE_BASE = "http://id.loc.gov/vocabulary/issuance/"


def _get_008(marc) -> str:
    try:
        f008 = marc["008"]
    except KeyError:
        return ""
    return f008.data if f008 else ""


def _extract_work_title(marc, fallback_title: str) -> str:
    """130 (uniform title, no author) or 240 (uniform title, with
    author) -> the Work's actual title, per the profile's distinction
    between Work Title and Instance Title (245). Falls back to the
    245-derived title (same one used for the Instance) when neither
    130 nor 240 is present -- which is the common case for most
    monographs, where there's no separate uniform title."""
    val = _marc_field_value(marc, "130", "a") or _marc_field_value(marc, "240", "a")
    return val or fallback_title


def _extract_variant_titles(marc) -> list:
    """246 (varying form of title) -> bf:title (variant), repeatable."""
    try:
        fields = marc.get_fields("246")
    except AttributeError:
        return []
    out = []
    for f in fields:
        try:
            parts = f.get_subfields("a", "b")
        except AttributeError:
            parts = []
        text = _clean_text(" ".join(parts))
        if text:
            out.append(text)
    return out


def _extract_contributors(marc, exclude_name: str) -> list:
    """700/710/711 (added entries) other than the one already chosen as
    the primary `author` -> bf:contribution (AdditionalContribution).
    `exclude_name` prevents double-counting the field already used as
    the primary contributor (relevant for AV records, where the
    "author" fallback itself comes from a 700)."""
    out = []
    try:
        fields = marc.get_fields("700", "710", "711")
    except AttributeError:
        return out
    for f in fields:
        try:
            name = f["a"]
        except (KeyError, TypeError):
            name = None
        if not name:
            continue
        name = _clean_text(name)
        if name == exclude_name:
            continue
        try:
            role = f["e"]
        except (KeyError, TypeError):
            role = None
        out.append({"name": name, "role": _clean_text(role) if role else "",
                     "identifiers": _extract_agent_identifiers(f)})
    return out


def _extract_government_pub_type(marc) -> str:
    """008/28 -> bflc:governmentPubType. A single character code (or
    a blank/'|' meaning not specified, which is treated as absent)."""
    data = _get_008(marc)
    if len(data) > 28:
        code = data[28]
        if code not in (" ", "|", ""):
            return code
    return ""


def _extract_origin_date(marc) -> str:
    """008 positions 07-10 (date1) -> bf:originDate. This is the
    Work's own origin/creation date per the fixed field, distinct
    from the Instance's provisionActivity date (260/264 $c) -- for
    a reprint of a classic work, these can differ substantially."""
    data = _get_008(marc)
    if len(data) >= 11:
        date1 = data[7:11]
        if date1 and date1 not in ("    ", "||||", "uuuu"):
            return date1
    return ""


def _extract_origin_place_code(marc) -> str:
    """008 positions 15-17 -> bf:originPlace. A 3-character MARC
    country code (e.g. 'nyu' = New York)."""
    data = _get_008(marc)
    if len(data) >= 18:
        code = data[15:18].strip()
        if code and code not in ("|||", "xx"):
            return code
    return ""


def _extract_intended_audience_code(marc) -> str:
    """008/22 -> bf:intendedAudience. Only meaningful for certain
    MARC types (books); a blank/'|' means not specified."""
    data = _get_008(marc)
    if len(data) > 22:
        code = data[22]
        if code not in (" ", "|", ""):
            return code
    return ""


def _extract_illustrative_content_codes(marc) -> list:
    """008 positions 18-21 -> bf:illustrativeContent. Up to 4 single-
    character codes (e.g. 'a' = illustrations, 'b' = maps)."""
    data = _get_008(marc)
    out = []
    if len(data) >= 22:
        for code in data[18:22]:
            if code not in (" ", "|", ""):
                out.append(code)
    return out


def _extract_dissertation_note(marc) -> str:
    """502 -> bf:dissertation. Modern practice uses subfields ($a
    Dissertation note, $b Degree type, $c Granting institution,
    $d Year); older records just have one $a. Concatenate whatever's
    present rather than requiring the newer structured form."""
    try:
        f = marc["502"]
    except KeyError:
        return ""
    if f is None:
        return ""
    parts = []
    for code in ("a", "b", "c", "d"):
        try:
            parts.extend(f.get_subfields(code))
        except AttributeError:
            pass
    return _clean_text(" ".join(parts))


def _extract_responsibility_statement(marc) -> str:
    """245 $c -> bf:responsibilityStatement (Instance-level)."""
    return _marc_field_value(marc, "245", "c")


def _extract_copyright_date(marc) -> str:
    """264 with second indicator '4' (copyright notice date), $c ->
    bf:copyrightDate. Older 260-only records don't carry this as a
    separate field (copyright date is often folded into 260 $c's free
    text instead, e.g. "c1995") -- that legacy case isn't parsed out
    here, only the explicit 264 _4 field used in modern RDA records.

    The profile specifies an EDTF datatype for this property, but the
    raw MARC text is a transcription that often includes the (c)/©
    symbol itself (e.g. "©2025") -- stripped here since EDTF expects
    a bare date, not the copyright symbol."""
    try:
        fields = marc.get_fields("264")
    except AttributeError:
        return ""
    for f in fields:
        if getattr(f, "indicator2", None) == "4":
            try:
                val = f["c"]
            except (KeyError, TypeError):
                val = None
            if val:
                cleaned = re.sub(r"[^\d\-uX]", "", val)  # strip (c)/©/brackets/etc.
                return cleaned or _clean_text(val)
    return ""


def _extract_issuance_code(marc) -> str:
    """Leader/07 (bibliographic level) -> bf:issuance. Mapped to the
    LC issuance vocabulary's codes rather than raw MARC leader values,
    per how id.loc.gov/vocabulary/issuance/ is keyed."""
    leader = str(marc.leader) if marc.leader else ""
    if len(leader) <= 7:
        return ""
    code = leader[7]
    return {
        "a": "mono", "c": "mono", "d": "mono", "m": "mono",  # monographic
        "b": "serial", "s": "serial",                          # serial
        "i": "integrating",                                     # integrating resource
    }.get(code, "")


def _extract_electronic_locators(marc) -> list:
    """856 $u -> bf:electronicLocator (repeatable URI)."""
    try:
        fields = marc.get_fields("856")
    except AttributeError:
        return []
    out = []
    for f in fields:
        try:
            urls = f.get_subfields("u")
        except AttributeError:
            urls = []
        out.extend(u.strip() for u in urls if u and u.strip())
    return out


def _extract_other_identifiers(marc) -> dict:
    """024 with first indicator '3' (EAN/UPC) -> 'ean'; 015 $a
    (National Bibliography Number) -> 'nbn'. These are captured for
    bf:identifiedBy RDF output only -- unlike `identifiers`
    (isbn/issn/oclc/lccn/doi/viaf_id), they are NOT used as Wikidata
    reconciliation lookups (no corresponding property is wired into
    ID_PROPERTIES for them)."""
    out = {}
    try:
        for f in marc.get_fields("024"):
            if getattr(f, "indicator1", None) == "3":
                try:
                    val = f["a"]
                except (KeyError, TypeError):
                    val = None
                if val:
                    out["ean"] = _clean_isbn(val)
                    break
    except AttributeError:
        pass
    nbn = _marc_field_value(marc, "015", "a")
    if nbn:
        out["nbn"] = nbn
    return out


def _marc_to_record(marc, idx: int) -> Record:
    try:
        control_field = marc["001"]
    except KeyError:
        control_field = None
    control_no = _clean_text(control_field.value() if control_field else "") or f"marc{idx}"

    title = _chop_isbd_punct(_marc_field_value(marc, "245", "a"))
    subtitle = _chop_isbd_punct(_marc_field_value(marc, "245", "b"))

    # Personal/corporate/meeting main entry (books, most print material).
    author = ""
    author_identifiers = {}
    for tag in ("100", "110", "111"):
        try:
            f = marc[tag]
        except KeyError:
            f = None
        if f is None:
            continue
        try:
            val = f["a"]
        except (KeyError, TypeError):
            val = None
        if val:
            author = _clean_text(val)
            author_identifiers = _extract_agent_identifiers(f)
            break

    # Fallback for audiovisual/moving-image material, which typically
    # has NO 1XX main entry at all -- attribution instead lives in
    # repeated 700 (personal added entry, e.g. director/actor/producer)
    # and 710 (corporate added entry, e.g. studio) fields with a
    # relator term in $e. This is a heuristic covering only a single
    # "primary" contributor (matching this pipeline's one-contribution
    # bf:contribution mapping) -- it does NOT capture the full cast/
    # crew credited across every 700/710 field in the record.
    if not author:
        author, author_identifiers = _first_field_with_identifiers(marc, "700", "a", "director")
        if not author:
            author, author_identifiers = _first_field_with_identifiers(marc, "700", "a")
        if not author:
            author, author_identifiers = _first_field_with_identifiers(marc, "710", "a")

    publisher = _marc_field_value(marc, "264", "b") or _marc_field_value(marc, "260", "b")
    pub_date = _marc_field_value(marc, "264", "c") or _marc_field_value(marc, "260", "c")
    pub_date = re.sub(r"[^\d]", "", pub_date)[:4]  # crude: first 4 digits

    isbn_raw = _marc_field_value(marc, "020", "a")
    isbn_clean = _clean_isbn(isbn_raw.split()[0] if isbn_raw else "")

    edition_statement = _marc_field_value(marc, "250", "a")

    extent_parts = []
    try:
        f300 = marc["300"]
    except KeyError:
        f300 = None
    if f300 is not None:
        for code in ("a", "f", "g"):
            try:
                extent_parts.extend(f300.get_subfields(code))
            except AttributeError:
                pass
    extent = _clean_text(" ".join(extent_parts))
    dimensions = _marc_field_value(marc, "300", "c")

    r = Record(
        raw={"marc_control_number": control_no},
        local_id=control_no,
        title=title,
        subtitle=subtitle,
        author=author,
        pub_date=pub_date,
        publisher=publisher,
        author_identifiers=author_identifiers,
        edition_statement=edition_statement,
        extent=extent,
        dimensions=dimensions,
        content_type=_extract_content_media_carrier(marc, "336"),
        media_type=_extract_content_media_carrier(marc, "337"),
        carrier_type=_extract_content_media_carrier(marc, "338"),
        languages=_extract_languages(marc),
        notes=_extract_notes(marc),
        summaries=_extract_summaries(marc),
        table_of_contents=_extract_table_of_contents(marc),
        subjects=_extract_subjects(marc),
        series_statements=_extract_series_statements(marc),
        classifications=_extract_classifications(marc),
        work_title=_extract_work_title(marc, title),
        variant_titles=_extract_variant_titles(marc),
        contributors=_extract_contributors(marc, author),
        government_pub_type=_extract_government_pub_type(marc),
        origin_date=_extract_origin_date(marc),
        origin_place_code=_extract_origin_place_code(marc),
        intended_audience_code=_extract_intended_audience_code(marc),
        illustrative_content_codes=_extract_illustrative_content_codes(marc),
        dissertation_note=_extract_dissertation_note(marc),
        responsibility_statement=_extract_responsibility_statement(marc),
        copyright_date=_extract_copyright_date(marc),
        issuance_code=_extract_issuance_code(marc),
        electronic_locators=_extract_electronic_locators(marc),
        other_identifiers=_extract_other_identifiers(marc),
    )
    if isbn_clean:
        r.identifiers["isbn13" if len(isbn_clean) == 13 else "isbn10"] = isbn_clean
    issn = _marc_field_value(marc, "022", "a")
    if issn:
        r.identifiers["issn"] = issn
    lccn = _marc_field_value(marc, "010", "a")
    if lccn:
        r.identifiers["lccn"] = _clean_text(lccn)
    oclc = _marc_field_value(marc, "035", "a")
    if oclc:
        m = re.search(r"\d+", oclc)
        if m:
            r.identifiers["oclc"] = m.group(0)
    return r


# ---------------------------------------------------------------------------
# 2. WIKIDATA RECONCILIATION
# ---------------------------------------------------------------------------

def _get_with_retry(url: str, params: dict, max_retries: int = 4,
                     base_delay: float = 1.0, timeout: int = 30) -> requests.Response:
    """GET with exponential backoff on transient failures (5xx, 429,
    connection errors). Wikidata's public endpoints occasionally throw
    a 502/503 under load -- that's their side, not a sign anything's
    wrong locally, so it's worth a few retries rather than failing the
    whole batch over one blip. Raises the last error if every retry
    is exhausted."""
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, params=params,
                                 headers={"User-Agent": USER_AGENT}, timeout=timeout)
            if resp.status_code == 429 or resp.status_code >= 500:
                last_exc = requests.HTTPError(
                    f"{resp.status_code} error for {url}", response=resp)
                if attempt < max_retries:
                    time.sleep(base_delay * (2 ** (attempt - 1)))
                    continue
                resp.raise_for_status()
            resp.raise_for_status()
            return resp
        except requests.exceptions.ConnectionError as exc:
            last_exc = exc
            if attempt < max_retries:
                time.sleep(base_delay * (2 ** (attempt - 1)))
                continue
            raise
    raise last_exc


def _sparql_select(query: str) -> list[dict]:
    resp = _get_with_retry(WIKIDATA_SPARQL, {"query": query, "format": "json"})
    return resp.json()["results"]["bindings"]


def _sparql_ask(query: str) -> bool:
    resp = _get_with_retry(WIKIDATA_SPARQL, {"query": query, "format": "json"})
    return resp.json().get("boolean", False)


def _identifier_lookup(prop: str, value: str) -> list[Candidate]:
    query = f"""
    SELECT ?item ?itemLabel ?itemDescription WHERE {{
      ?item wdt:{prop} "{value}" .
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    """
    out = []
    for row in _sparql_select(query):
        qid = row["item"]["value"].rsplit("/", 1)[-1]
        out.append(Candidate(
            qid=qid,
            label=row.get("itemLabel", {}).get("value", ""),
            description=row.get("itemDescription", {}).get("value", ""),
            match_type="identifier", matched_property=prop, score=100.0,
        ))
    return out


def _clean_title_for_search(title: str) -> str:
    """Reduce a raw MARC/catalog title to its core searchable title.

    Real MARC 245 fields routinely carry trailing ISBD punctuation
    (e.g. a statement-of-responsibility slash: "Title /") and
    sometimes fold the statement of responsibility directly into the
    title text (e.g. "Hamlet by William Shakespeare.", or a long
    title-page transcription like "Emma: a Novel... By the Author
    Of..."). Wikidata's wbsearchentities does prefix-style matching
    against clean labels ("Hamlet", "Emma"), so leaving that extra
    text in the query breaks the match entirely rather than just
    lowering its score. This trims at the first subtitle marker
    (":"/";") or embedded "by <author>" and strips trailing ISBD
    punctuation, WITHOUT touching the title stored on the record
    itself (bf:mainTitle keeps the full original title).
    """
    if not title:
        return title
    t = re.split(r"[:;]|\s+\bby\b\s+", title, maxsplit=1, flags=re.IGNORECASE)[0]
    t = re.sub(r"[\s/,.;:]+$", "", t)
    return t.strip() or title.strip()


def _search_by_title(title: str, limit: int = 8) -> list[Candidate]:
    params = {"action": "wbsearchentities", "search": title, "language": "en",
              "format": "json", "limit": limit, "type": "item"}
    resp = _get_with_retry(WIKIDATA_API, params)
    return [
        Candidate(qid=hit["id"], label=hit.get("label", ""),
                  description=hit.get("description", ""), match_type="fuzzy")
        for hit in resp.json().get("search", [])
    ]


# Wikidata classes (checked via wdt:P31/wdt:P279*, so subclasses count
# too) that map to each BIBFRAME tier in _classify_entity_role.
_INSTANCE_CLASSES = ("Q3331189",)             # version, edition, or translation
_WORK_CLASSES = ("Q386724", "Q24856", "Q11424")  # work; film series; film


def _classify_entity_role(qid: str) -> str:
    """This pipeline only distinguishes Work vs. Instance (of
    BIBFRAME's three core tiers -- Work, Instance, Item -- Item is out
    of scope, since MARC bibliographic records don't carry item-level
    holdings data). Map Wikidata's edition/translation class
    (Q3331189, or a subclass of it) to 'instance'; map the general
    "work" class (Q386724) or the film-specific classes (Q24856 film
    series, Q11424 film) -- or any of their subclasses -- to 'work'.
    Q386724 is used instead of the narrower "written work" class so
    this also covers non-text works (music, etc.) that a title search
    might legitimately surface."""
    try:
        instance_values = " ".join(f"wd:{c}" for c in _INSTANCE_CLASSES)
        if _sparql_ask(f"ASK {{ wd:{qid} wdt:P31/wdt:P279* ?c . VALUES ?c {{ {instance_values} }} }}"):
            return "instance"
        work_values = " ".join(f"wd:{c}" for c in _WORK_CLASSES)
        if _sparql_ask(f"ASK {{ wd:{qid} wdt:P31/wdt:P279* ?c . VALUES ?c {{ {work_values} }} }}"):
            return "work"
    except requests.exceptions.RequestException as exc:
        print(f"  [warn] entity-role classification failed for {qid} "
              f"(treating as 'unknown'): {exc}", file=sys.stderr)
    return "unknown"


# "Who made this" properties checked when verifying a title-search
# candidate against a known creator identity (see _person_is_creator_of).
# P50 author, P57 director, P58 screenwriter, P170 creator, P86 composer.
_CREATOR_PROPERTIES = ("P50", "P57", "P58", "P170", "P86")


def _resolve_person_qid(author_identifiers: dict) -> Optional[str]:
    """Resolve a person/corporate body's Wikidata QID from the $0/$1-
    derived identifiers on a Record. A direct Wikidata link (already a
    QID) is used as-is; otherwise falls back to querying by VIAF, LC
    NAF authority ID, or ISNI, in that order. Returns None if nothing
    resolves. Never raises."""
    if not author_identifiers:
        return None
    if "wikidata_qid" in author_identifiers:
        return author_identifiers["wikidata_qid"]
    for key, prop in (("viaf_id", "P214"), ("lccn_authority", "P244"), ("isni", "P213")):
        value = author_identifiers.get(key)
        if not value:
            continue
        try:
            hits = _identifier_lookup(prop, value)
        except requests.exceptions.RequestException:
            continue
        if hits:
            return hits[0].qid
    return None


def _person_is_creator_of(person_qid: str, candidate_qid: str) -> bool:
    """ASK whether `person_qid` is credited as author/director/
    screenwriter/creator/composer of `candidate_qid`. Never raises --
    returns False (i.e. unverified, not an error) on any failure."""
    props = "|".join(f"wdt:{p}" for p in _CREATOR_PROPERTIES)
    query = f"ASK {{ wd:{candidate_qid} ({props}) wd:{person_qid} }}"
    try:
        return _sparql_ask(query)
    except requests.exceptions.RequestException:
        return False


def find_candidates(record: Record) -> None:
    """Look up Wikidata candidates for a record. Never raises -- a
    transient Wikidata failure (their public endpoints occasionally
    500/502/503 under load, even after retrying) degrades to 'no
    candidates found for this step' rather than crashing whatever
    loop is processing potentially thousands of records. Failures are
    printed as warnings so they're visible without stopping the run."""
    for key, prop in ID_PROPERTIES.items():
        value = record.identifiers.get(key)
        if not value:
            continue
        try:
            hits = _identifier_lookup(prop, value)
        except requests.exceptions.RequestException as exc:
            print(f"  [warn] identifier lookup failed for {record.local_id} "
                  f"({key}={value}): {exc}", file=sys.stderr)
            continue
        if hits:
            record.candidates.extend(hits)
            break
        time.sleep(0.2)

    if record.title:
        try:
            record.candidates.extend(_search_by_title(_clean_title_for_search(record.title)))
        except requests.exceptions.RequestException as exc:
            print(f"  [warn] title search failed for {record.local_id} "
                  f"({record.title[:40]!r}): {exc}", file=sys.stderr)
        time.sleep(0.2)

    # If the source record carried an authority identifier ($0/$1) for
    # its author/director, resolve that PERSON's own Wikidata QID once,
    # then check each fuzzy title-search candidate against it. A title
    # match confirmed against a known creator identity is much stronger
    # evidence than fuzzy text alone -- see score_candidates() for how
    # this affects the final decision.
    person_qid = None
    if record.author_identifiers:
        try:
            person_qid = _resolve_person_qid(record.author_identifiers)
        except requests.exceptions.RequestException as exc:
            print(f"  [warn] person identity lookup failed for {record.local_id}: {exc}",
                  file=sys.stderr)

    for cand in record.candidates[:5]:
        cand.entity_role = _classify_entity_role(cand.qid)
        if person_qid and cand.match_type != "identifier":
            cand.person_verified = _person_is_creator_of(person_qid, cand.qid)


def score_candidates(record: Record) -> None:
    clean_title = _clean_title_for_search(record.title)
    for cand in record.candidates:
        if cand.match_type == "identifier":
            cand.score = 100.0
            continue
        title_score = fuzz.token_sort_ratio(clean_title, cand.label)
        author_bonus = 0.0
        if record.author and record.author.split():
            surname = record.author.split()[-1]
            if surname.lower() in (cand.description or "").lower():
                author_bonus = 5.0
        if cand.person_verified and title_score >= 60:
            # Title text alone got this candidate into consideration,
            # but the creator identity is independently confirmed via
            # an authority identifier (VIAF/LCCN-authority/ISNI/direct
            # Wikidata link) -- much stronger evidence than fuzzy text,
            # so it can clear the auto-accept bar even if the raw title
            # score wouldn't on its own. Capped below the strict 100
            # reserved for bibliographic-identifier hits.
            cand.score = max(AUTO_ACCEPT_PERSON_THRESHOLD, min(100.0, title_score + author_bonus))
            if not cand.matched_property:
                cand.matched_property = "person_verified"
        else:
            cand.score = min(100.0, title_score + author_bonus)

    record.candidates.sort(key=lambda c: c.score, reverse=True)
    if not record.candidates:
        record.decision = "no_match"
        return

    best = record.candidates[0]
    record.best = best
    if best.match_type == "identifier" and best.score >= AUTO_ACCEPT_THRESHOLD:
        record.decision = "auto_accept"
    elif best.person_verified and best.score >= AUTO_ACCEPT_PERSON_THRESHOLD:
        record.decision = "auto_accept"
    elif best.score >= REVIEW_THRESHOLD:
        record.decision = "needs_review"
    else:
        record.decision = "no_match"


# ---------------------------------------------------------------------------
# 3. BUILD BIBFRAME GRAPH
# ---------------------------------------------------------------------------

def _mint_uri(base_ns: Namespace, local_id: str, kind: str) -> URIRef:
    """Mint a stable UUID-based URI under the target resource pattern.

    Deterministic (uuid5, seeded from the record's local_id) rather than
    random (uuid4), so re-running the pipeline on the same source data
    produces the same URIs instead of minting duplicate resources every
    run. Swap to uuid.uuid4() if you'd rather mint fresh UUIDs each time
    and let the target system dedupe.
    """
    seed = f"{kind}:{local_id}"
    return base_ns[str(uuid.uuid5(_MINT_NAMESPACE, seed))]


_MINT_NAMESPACE = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")  # fixed seed, arbitrary


def build_bibframe_graph(records: list[Record]) -> Graph:
    g = Graph()
    g.bind("bf", BF)
    g.bind("bflc", BFLC)
    g.bind("wd", WD)

    for r in records:
        work_uri = _mint_uri(WORK_NS, r.local_id, "work")
        instance_uri = _mint_uri(INSTANCE_NS, r.local_id, "instance")

        # --- Work ---
        g.add((work_uri, RDF.type, BF.Work))
        # Work Title vs Instance Title: per the Blue Core profile, these
        # are DISTINCT properties -- the Work's title is the uniform
        # title (130/240) when one exists, falling back to the 245
        # title (same as the Instance's) when it doesn't. work_title
        # already encodes that fallback (see _extract_work_title).
        if r.work_title:
            title_node = BNode()
            g.add((work_uri, BF.title, title_node))
            g.add((title_node, RDF.type, BF.Title))
            g.add((title_node, BF.mainTitle, Literal(r.work_title)))
        if r.author:
            g.add((work_uri, BF.contribution,
                   _contribution_node(g, r.author, "aut", r.author_identifiers)))

        for contributor in r.contributors:
            g.add((work_uri, BF.contribution, _additional_contribution_node(g, contributor)))

        if r.government_pub_type:
            g.add((work_uri, BFLC.governmentPubType,
                   URIRef(f"{GOVT_PUB_TYPE_BASE}{r.government_pub_type}")))

        if r.origin_date:
            g.add((work_uri, BF.originDate, Literal(r.origin_date)))

        # originPlace/intendedAudience/illustrativeContent are all
        # 008-fixed-field-coded vocabulary terms, same shape as
        # content/media/carrier -- so they use the same
        # _vocab_resource_node() pattern: the vocabulary term URI
        # itself is the resource (typed directly), not a separate
        # BNode pointed at the URI via rdfs:seeAlso. Confirmed against
        # the LC marc2bibframe2 XSLT's own intendedAudience/
        # illustrativeContent templates, which emit exactly
        # `<bf:IntendedAudience rdf:about="{vocab URI}">` /
        # `<bf:Illustration rdf:about="{vocab URI}">` directly.
        # Note: the 008 country-of-publication code does NOT map to
        # bf:originPlace here. Confirmed against real id.loc.gov
        # BIBFRAME output: that code consistently lands on bf:place
        # nested inside the Instance's bf:provisionActivity instead
        # (see the Instance section below) -- bf:originPlace wasn't
        # observed being used this way in any live example checked.

        if r.intended_audience_code:
            g.add((work_uri, BF.intendedAudience, _vocab_resource_node(
                g, BF.IntendedAudience, AUDIENCE_BASE, {"code": r.intended_audience_code})))

        for illus_code in r.illustrative_content_codes:
            g.add((work_uri, BF.illustrativeContent, _vocab_resource_node(
                g, BF.Illustration, ILLUSTRATIONS_BASE, {"code": illus_code})))

        if r.dissertation_note:
            dis_node = BNode()
            g.add((work_uri, BF.dissertation, dis_node))
            g.add((dis_node, RDF.type, BF.Dissertation))
            g.add((dis_node, RDFS.label, Literal(r.dissertation_note)))

        # --- Additional Work-level properties (336 content, 041 language,
        # 520 summary, 505 tableOfContents, 650/651/655 subject/genreForm,
        # 050/082/060 classification -- per LC marc2bibframe2 XSLT, these
        # all attach to the Work, not the Instance) ---
        # bf:content is REQUIRED per the Blue Core profile; default to
        # "text" (matching its "ContentType:TextDefault" template) when
        # 336 wasn't present in the source record, since these templates
        # target monographs specifically.
        content = r.content_type or {"code": "txt", "label": "text"}
        g.add((work_uri, BF.content, _vocab_resource_node(
            g, BF.Content, CONTENT_TYPES_BASE, content)))

        for lang_code in r.languages:
            g.add((work_uri, BF.language, URIRef(f"{LANGUAGES_BASE}{lang_code}")))

        for summary_text in r.summaries:
            summary_node = BNode()
            g.add((work_uri, BF.summary, summary_node))
            g.add((summary_node, RDF.type, BF.Summary))
            g.add((summary_node, RDFS.label, Literal(summary_text)))

        if r.table_of_contents:
            toc_node = BNode()
            g.add((work_uri, BF.tableOfContents, toc_node))
            g.add((toc_node, RDF.type, BF.TableOfContents))
            g.add((toc_node, RDFS.label, Literal(r.table_of_contents)))

        for subj in r.subjects:
            subj_node = BNode()
            subj_class = {"Topic": BF.Topic, "Place": BF.Place,
                          "GenreForm": BF.GenreForm}[subj["type"]]
            prop = BF.genreForm if subj["type"] == "GenreForm" else BF.subject
            g.add((work_uri, prop, subj_node))
            g.add((subj_node, RDF.type, subj_class))
            g.add((subj_node, RDFS.label, Literal(subj["label"])))

        for cls in r.classifications:
            cls_node = BNode()
            cls_class = {"lcc": BF.ClassificationLcc, "ddc": BF.ClassificationDdc,
                         "nlm": BF.ClassificationNlm}[cls["scheme"]]
            g.add((work_uri, BF.classification, cls_node))
            g.add((cls_node, RDF.type, cls_class))
            g.add((cls_node, BF.classificationPortion, Literal(cls["classification_portion"])))
            if "item_portion" in cls:
                g.add((cls_node, BF.itemPortion, Literal(cls["item_portion"])))

        for series_text in r.series_statements:
            # Lightweight "Related Work" stub per the profile's bf:relation
            # (Series/Relation). Per the ontology, bf:relation's range is
            # bf:Relation -- a wrapper node -- not the related resource
            # directly; the actual resource is linked via
            # bf:associatedResource ("Suggested value - With Work or Hub"
            # per the ontology's own comment on that property). A minimal
            # Work node with just a title is used as that associated
            # resource, since MARC 490 (uncontrolled/transcribed series)
            # doesn't give us enough to link to an authority-controlled
            # series Work the way a linked 8XX would.
            relation_node = BNode()
            g.add((work_uri, BF.relation, relation_node))
            g.add((relation_node, RDF.type, BF.Relation))
            related_work_node = BNode()
            g.add((relation_node, BF.associatedResource, related_work_node))
            g.add((related_work_node, RDF.type, BF.Work))
            rel_title_node = BNode()
            g.add((related_work_node, BF.title, rel_title_node))
            g.add((rel_title_node, RDF.type, BF.Title))
            g.add((rel_title_node, BF.mainTitle, Literal(series_text)))

        # Inverse of instanceOf, per the profile's explicit bf:hasInstance
        # property on the Work (redundant with instanceOf below, but
        # asserting both directions matches what the profile calls for).
        g.add((work_uri, BF.hasInstance, instance_uri))

        # --- Instance ---
        g.add((instance_uri, RDF.type, BF.Instance))
        g.add((instance_uri, BF.instanceOf, work_uri))
        if r.title:
            title_node = BNode()
            g.add((instance_uri, BF.title, title_node))
            g.add((title_node, RDF.type, BF.Title))
            g.add((title_node, BF.mainTitle, Literal(r.title)))
            if r.subtitle:
                # Kept as a separate bf:subtitle property, not
                # concatenated into mainTitle text -- confirmed
                # against real id.loc.gov BIBFRAME output, which
                # consistently keeps these distinct.
                g.add((title_node, BF.subtitle, Literal(r.subtitle)))

        for variant_text in r.variant_titles:
            # Typed bf:VariantTitle (a bf:Title subclass), not plain
            # bf:Title -- confirmed against real id.loc.gov output for
            # 246-derived variant/cover titles.
            variant_node = BNode()
            g.add((instance_uri, BF.title, variant_node))
            g.add((variant_node, RDF.type, BF.VariantTitle))
            g.add((variant_node, BF.mainTitle, Literal(variant_text)))

        if r.responsibility_statement:
            g.add((instance_uri, BF.responsibilityStatement, Literal(r.responsibility_statement)))

        if r.pub_date or r.publisher or r.origin_place_code:
            prov = BNode()
            g.add((instance_uri, BF.provisionActivity, prov))
            g.add((prov, RDF.type, BF.Publication))
            if r.pub_date:
                g.add((prov, BF.date, Literal(r.pub_date)))
            if r.publisher:
                pub_agent = BNode()
                g.add((prov, BF.agent, pub_agent))
                g.add((pub_agent, RDF.type, BF.Agent))
                g.add((pub_agent, RDFS.label, Literal(r.publisher)))
            if r.origin_place_code:
                # 008 country-of-publication code -> bf:place, nested
                # inside provisionActivity. Confirmed against real
                # id.loc.gov BIBFRAME output (e.g. bf:place pointing
                # to http://id.loc.gov/vocabulary/countries/nyu inside
                # a bf:Publication node) -- not bf:originPlace on the
                # Work, which this pipeline used to (incorrectly) emit.
                g.add((prov, BF.place, _vocab_resource_node(
                    g, BF.Place, COUNTRIES_BASE, {"code": r.origin_place_code})))

        if r.copyright_date:
            g.add((instance_uri, BF.copyrightDate, Literal(r.copyright_date)))

        for series_text in r.series_statements:
            g.add((instance_uri, BF.seriesStatement, Literal(series_text)))

        # --- Additional Instance-level properties (250 edition, 300
        # extent/dimensions, 337 media, 338 carrier, 500 note, Leader/07
        # issuance, 856 electronicLocator -- per LC marc2bibframe2 XSLT
        # and the Blue Core profile, these attach to the Instance) ---
        if r.edition_statement:
            g.add((instance_uri, BF.editionStatement, Literal(r.edition_statement)))

        if r.extent:
            extent_node = BNode()
            g.add((instance_uri, BF.extent, extent_node))
            g.add((extent_node, RDF.type, BF.Extent))
            g.add((extent_node, RDFS.label, Literal(r.extent)))

        if r.dimensions:
            g.add((instance_uri, BF.dimensions, Literal(r.dimensions)))

        # bf:issuance is REQUIRED per the profile; default to "mono"
        # (single unit) when the Leader/07 code wasn't recognized,
        # matching its "ModeOfIssuance:SingleUnit" default template.
        # Confirmed against ConvSpec-LDR.xsl: the LC XSLT emits
        # <bf:issuance rdf:resource="{vocab URI}"/> directly, not a
        # wrapper node pointed at the URI via rdfs:seeAlso.
        issuance = r.issuance_code or "mono"
        g.add((instance_uri, BF.issuance, _vocab_resource_node(
            g, BF.Issuance, ISSUANCE_BASE, {"code": issuance})))

        # bf:media / bf:carrier are also REQUIRED; default to
        # "unmediated"/"volume" per the profile's own default templates.
        media = r.media_type or {"code": "n", "label": "unmediated"}
        g.add((instance_uri, BF.media, _vocab_resource_node(
            g, BF.Media, MEDIA_TYPES_BASE, media)))

        carrier = r.carrier_type or {"code": "nc", "label": "volume"}
        g.add((instance_uri, BF.carrier, _vocab_resource_node(
            g, BF.Carrier, CARRIERS_BASE, carrier)))

        for note_text in r.notes:
            note_node = BNode()
            g.add((instance_uri, BF.note, note_node))
            g.add((note_node, RDF.type, BF.Note))
            g.add((note_node, RDFS.label, Literal(note_text)))

        for url in r.electronic_locators:
            g.add((instance_uri, BF.electronicLocator, URIRef(url)))

        _add_identifiers(g, instance_uri, r.identifiers)
        _add_other_identifiers(g, instance_uri, r.other_identifiers)

        # --- Wikidata linkage: only for auto_accept, and only onto the
        # tier the reconciliation actually classified the match as ---
        if r.decision == "auto_accept" and r.best:
            wd_uri = WD[r.best.qid]
            if r.best.entity_role == "work":
                g.add((work_uri, RDFS.seeAlso, wd_uri))
            elif r.best.entity_role == "instance":
                g.add((instance_uri, RDFS.seeAlso, wd_uri))
            else:
                # Role unknown: attach at Work level with a note rather
                # than guess, since collapsing this incorrectly is
                # exactly the interoperability problem the article warns about.
                g.add((work_uri, RDFS.seeAlso, wd_uri))
                g.add((work_uri, RDFS.comment,
                       Literal("Wikidata match entity-role unresolved; verify Work vs Instance before relying on this link.")))
        elif r.decision == "needs_review" and r.best:
            g.add((work_uri, RDFS.comment,
                   Literal(f"Candidate Wikidata match {r.best.qid} pending human review "
                            f"(score={r.best.score:.0f}); not asserted as rdfs:seeAlso.")))

    return g


def _additional_contribution_node(g: Graph, contributor: dict) -> BNode:
    """Build a bf:Contribution node for a non-primary contributor
    (700/710/711 -> AdditionalContribution). Unlike the primary
    author's _contribution_node, the role here comes from MARC $e as
    free text (e.g. "screenwriter.", "film producer.") rather than a
    controlled 3-letter relator code -- there's no lookup table wired
    in to resolve free text back to the right id.loc.gov/vocabulary/
    relators/ code, so the role is recorded as an rdfs:label rather
    than a bf:role resource URI."""
    contrib = BNode()
    g.add((contrib, RDF.type, BF.Contribution))
    agent = BNode()
    g.add((contrib, BF.agent, agent))
    g.add((agent, RDF.type, BF.Agent))
    g.add((agent, RDFS.label, Literal(contributor["name"])))
    if contributor.get("role"):
        role_node = BNode()
        g.add((contrib, BF.role, role_node))
        g.add((role_node, RDFS.label, Literal(contributor["role"])))
    ids = contributor.get("identifiers") or {}
    if "wikidata_qid" in ids:
        g.add((agent, RDFS.seeAlso, WD[ids["wikidata_qid"]]))
    if "viaf_id" in ids:
        g.add((agent, RDFS.seeAlso, URIRef(f"http://viaf.org/viaf/{ids['viaf_id']}")))
    if "lccn_authority" in ids:
        g.add((agent, RDFS.seeAlso,
               URIRef(f"http://id.loc.gov/authorities/names/{ids['lccn_authority']}")))
    return contrib


def _add_other_identifiers(g: Graph, instance_uri: URIRef, other_identifiers: dict) -> None:
    """EAN / National Bibliography Number -> bf:identifiedBy. Kept
    separate from _add_identifiers since these aren't used for
    Wikidata reconciliation (see Record.other_identifiers)."""
    if "ean" in other_identifiers:
        node = BNode()
        g.add((instance_uri, BF.identifiedBy, node))
        g.add((node, RDF.type, BF.Ean))
        g.add((node, RDF.value, Literal(other_identifiers["ean"])))
    if "nbn" in other_identifiers:
        node = BNode()
        g.add((instance_uri, BF.identifiedBy, node))
        g.add((node, RDF.type, BF.Nbn))
        g.add((node, RDF.value, Literal(other_identifiers["nbn"])))


def _vocab_resource_node(g: Graph, rdf_class: URIRef, uri_base: str, vocab: dict) -> URIRef:
    """Build a controlled-vocabulary resource node (bf:Content,
    bf:Media, or bf:Carrier) at <uri_base><code>, with an optional
    rdfs:label from the term text. Matches ConvSpec-3XX.xsl's shared
    'rdaResource' template: $b (code) -> rdf:about URI, $a (label
    text) -> rdfs:label."""
    node_uri = URIRef(f"{uri_base}{vocab['code']}")
    g.add((node_uri, RDF.type, rdf_class))
    if vocab.get("label"):
        g.add((node_uri, RDFS.label, Literal(vocab["label"])))
    return node_uri


def _contribution_node(g: Graph, name: str, role_code: str,
                        author_identifiers: dict = None) -> BNode:
    """Builds the contribution node for the record's single PRIMARY
    author/contributor (see _additional_contribution_node() for
    everyone else). Typed with both bf:Contribution and
    bf:PrimaryContribution -- confirmed against real id.loc.gov
    BIBFRAME output, which consistently double-types the main-entry
    contribution this way."""
    contrib = BNode()
    g.add((contrib, RDF.type, BF.Contribution))
    g.add((contrib, RDF.type, BF.PrimaryContribution))
    agent = BNode()
    g.add((contrib, BF.agent, agent))
    g.add((agent, RDF.type, BF.Person))
    g.add((agent, RDFS.label, Literal(name)))
    g.add((contrib, BF.role, RDAROLE[role_code]))

    # Surface the authority identifiers parsed from MARC $0/$1 (see
    # _extract_agent_identifiers), if any, on the agent itself. These
    # identify the PERSON, not the bibliographic work -- distinct from
    # the rdfs:seeAlso added at the Work/Instance level elsewhere.
    ids = author_identifiers or {}
    if "wikidata_qid" in ids:
        g.add((agent, RDFS.seeAlso, WD[ids["wikidata_qid"]]))
    if "viaf_id" in ids:
        g.add((agent, RDFS.seeAlso, URIRef(f"http://viaf.org/viaf/{ids['viaf_id']}")))
    if "lccn_authority" in ids:
        g.add((agent, RDFS.seeAlso,
               URIRef(f"http://id.loc.gov/authorities/names/{ids['lccn_authority']}")))
    if "isni" in ids:
        isni_node = BNode()
        g.add((agent, BF.identifiedBy, isni_node))
        g.add((isni_node, RDF.type, BF.Isni))
        g.add((isni_node, RDF.value, Literal(ids["isni"])))

    return contrib


def _add_identifiers(g: Graph, instance_uri: URIRef, identifiers: dict) -> None:
    # OCLC numbers get their own dedicated bf:OclcNumber class in real
    # LC BIBFRAME output (confirmed against live id.loc.gov examples,
    # e.g. an OclcNumber-typed identifier with no bf:source sub-node
    # at all) -- not bf:Local wrapped in a bf:Source[bf:code="OCoLC"]
    # as this pipeline used to emit.
    id_class_map = {
        "isbn13": BF.Isbn, "isbn10": BF.Isbn, "issn": BF.Issn,
        "lccn": BF.Lccn, "oclc": BF.OclcNumber, "doi": BF.Doi,
    }
    for key, value in identifiers.items():
        if key == "viaf_id":
            continue  # VIAF belongs on the agent, not the instance; extend if needed
        bnode = BNode()
        g.add((instance_uri, BF.identifiedBy, bnode))
        g.add((bnode, RDF.type, id_class_map.get(key, BF.Identifier)))
        g.add((bnode, RDF.value, Literal(value)))


# ---------------------------------------------------------------------------
# 4/5. SERIALIZE + REVIEW REPORT
# ---------------------------------------------------------------------------

FORMAT_MAP = {
    "turtle": "turtle", "ttl": "turtle",
    "json-ld": "json-ld", "jsonld": "json-ld",
    "xml": "xml", "rdfxml": "xml", "rdf-xml": "xml",
    "nt": "nt", "ntriples": "nt", "n-triples": "nt",
}


def serialize_graph(g: Graph, out_path: str, fmt: str) -> None:
    rdflib_fmt = FORMAT_MAP.get(fmt.lower())
    if rdflib_fmt is None:
        raise ValueError(f"Unknown format {fmt!r}; choose from {sorted(FORMAT_MAP)}")
    g.serialize(destination=out_path, format=rdflib_fmt)


def write_review_csv(records: list[Record], out_path: str) -> None:
    fieldnames = ["local_id", "title", "author", "pub_date", "decision",
                  "best_qid", "best_label", "entity_role", "score", "alt_candidates"]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in records:
            if r.decision == "auto_accept":
                continue  # only surface things that actually need a human
            best = r.best
            alts = "; ".join(f"{c.qid}({c.score:.0f})" for c in r.candidates[1:4])
            writer.writerow({
                "local_id": r.local_id, "title": r.title, "author": r.author,
                "pub_date": r.pub_date, "decision": r.decision,
                "best_qid": best.qid if best else "",
                "best_label": best.label if best else "",
                "entity_role": best.entity_role if best else "",
                "score": f"{best.score:.1f}" if best else "",
                "alt_candidates": alts,
            })


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description="MARC/spreadsheet -> Wikidata reconciliation -> BIBFRAME RDF")
    ap.add_argument("input", help="Path to .mrc (MARC21) or .csv/.json (spreadsheet)")
    ap.add_argument("--out", required=True, help="Output RDF file path")
    ap.add_argument("--format", default="turtle",
                     help="turtle|json-ld|xml|nt (default: turtle)")
    ap.add_argument("--review-out", default="needs_review.csv")
    ap.add_argument("--limit", type=int, default=None,
                     help="Only process the first N records -- useful for large "
                          "MARC batches (10k+ records means 10k+ rounds of Wikidata "
                          "SPARQL/search calls, which is slow and can hit rate limits)")
    ap.add_argument("--skip-reconciliation", action="store_true",
                     help="Skip Wikidata lookups entirely (fast: just builds local "
                          "BIBFRAME RDF with no rdfs:seeAlso links)")
    args = ap.parse_args()

    records = load_records(args.input)
    if args.limit:
        records = records[:args.limit]
    print(f"Loaded {len(records)} records from {args.input}")

    if args.skip_reconciliation:
        print("Skipping Wikidata reconciliation (--skip-reconciliation).")
    else:
        for i, r in enumerate(records, 1):
            try:
                find_candidates(r)
                score_candidates(r)
            except Exception as exc:
                print(f"[{i}/{len(records)}] {r.title[:50]!r:52} -> ERROR: {exc}",
                      file=sys.stderr)
                continue
            print(f"[{i}/{len(records)}] {r.title[:50]!r:52} -> {r.decision}"
                  + (f" ({r.best.qid}, {r.best.entity_role})" if r.best else ""))

    graph = build_bibframe_graph(records)
    serialize_graph(graph, args.out, args.format)
    write_review_csv(records, args.review_out)

    counts = {}
    for r in records:
        counts[r.decision] = counts.get(r.decision, 0) + 1
    print(f"\nSummary: {counts}")
    print(f"BIBFRAME RDF ({args.format}) written to {args.out}")
    print(f"Human-review queue written to {args.review_out}")


if __name__ == "__main__":
    main()
