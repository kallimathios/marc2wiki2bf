"""
Push Reconciled Records to the BIBFRAME API (bcld)
=====================================================

Takes the same MARC/spreadsheet input and Wikidata reconciliation as
bibframe_pipeline.py, but instead of (or in addition to) writing a
static RDF file, pushes each record directly into the target database
via its REST API: POST /works/, then POST /instances/ linked to the
created Work.

Important API behavior this script relies on (from the OpenAPI spec)
--------------------------------------------------------------------
- The server mints its own `uuid`/`uri` for each Work and Instance on
  creation -- you don't get to pick the URI up front the way the
  static-RDF pipeline does. So this script does NOT pre-mint
  https://dev.bcld.info/works/<uuid>-style URIs; it takes whatever URI
  comes back in the response.
- Work <-> Instance linkage is a relational foreign key (`work_id`,
  an integer, from WorkSchema.id), not (only) the RDF-level
  bf:instanceOf triple. The `application/vnd.sinopia+json` content
  type on POST /instances/ is what carries that FK, so this script
  uses that content type rather than raw application/ld+json.
- Auth: the spec doesn't document a security scheme, so this script
  sends whatever bearer token you provide as a plain `Authorization:
  Bearer <token>` header. Adjust `_auth_headers()` if your deployment
  actually uses Basic auth, an API key header, OAuth, etc.

Usage
-----
    export BCLD_API_TOKEN=xxxxx
    python push_to_bcld.py catalog.csv \
        --base-url https://dev.bcld.info/api \
        --manifest-out push_manifest.csv

    # See exactly what would be sent, without sending anything:
    python push_to_bcld.py catalog.csv --dry-run

Safety notes
------------
- This performs live, non-idempotent POSTs. There is no documented
  "upsert by URI" on this API, so re-running against the same source
  data WILL create duplicate Works/Instances unless you track what's
  already been pushed. `--manifest-out` writes a CSV of exactly what
  was created (local_id -> work uri/id, instance uri/id) specifically
  so you have that record.
- Always run with `--dry-run` first and read the printed payloads
  before pointing this at a real database.
- Only records that reconciled cleanly (`auto_accept`) get a Wikidata
  `rdfs:seeAlso` link written into the payload. `needs_review` records
  are still pushed (so cataloging isn't blocked on reconciliation) but
  without any Wikidata link -- check `needs_review.csv` from
  bibframe_pipeline.py separately if you want to resolve those first.
"""

import argparse
import csv
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field

import requests
from rapidfuzz import fuzz

# Reuse loaders, normalization, and Wikidata reconciliation from the
# existing pipeline instead of duplicating them.
from bibframe_pipeline import (
    Record, Candidate,
    load_records, find_candidates, score_candidates,
    _clean_title_for_search,
)

# ---------------------------------------------------------------------------
# KEYCLOAK AUTH
# ---------------------------------------------------------------------------

# Base Keycloak URL and realm for this deployment (confirmed from the
# admin console's client list: realm "bluecore" under
# https://dev.bcld.info/keycloak/).
KEYCLOAK_BASE_URL = "https://dev.bcld.info/keycloak"
KEYCLOAK_REALM = "bluecore"

# The "bluecore" realm has two application-specific clients (besides
# Keycloak's built-in "account"/"account-console"): "bluecore_api" and
# "bluecore_workflows". "bluecore_api" is the default here since it
# should map to the /works/, /instances/ endpoints this script calls;
# "bluecore_workflows" more likely covers the /batches/ and /export/
# (Airflow-triggered) endpoints instead. Override with
# --keycloak-client-id / BCLD_KEYCLOAK_CLIENT_ID if this guess is wrong.
DEFAULT_KEYCLOAK_CLIENT_ID = "bluecore_api"


def get_keycloak_token(username: str, password: str, client_id: str,
                        client_secret: str = None,
                        keycloak_base_url: str = KEYCLOAK_BASE_URL,
                        realm: str = KEYCLOAK_REALM) -> str:
    """Exchange a username/password for a short-lived access token via
    Keycloak's Direct Access Grant (OAuth2 'password' grant).

    `client_id` must be the application's actual OAuth client -- NOT
    Keycloak's built-in 'account' / 'account-console' clients, which
    only exist for end-users to manage their own Keycloak profile and
    won't be authorized against the BIBFRAME API. In the "bluecore"
    realm, that's "bluecore_api" (default) or possibly
    "bluecore_workflows" if the former is rejected for this API.

    If that client is confidential (has a secret) rather than public,
    pass `client_secret` too -- Keycloak will reject the request with
    'unauthorized_client' otherwise.
    """
    token_url = f"{keycloak_base_url}/realms/{realm}/protocol/openid-connect/token"
    data = {
        "grant_type": "password",
        "client_id": client_id,
        "username": username,
        "password": password,
    }
    if client_secret:
        data["client_secret"] = client_secret

    resp = requests.post(token_url, data=data, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(
            f"Keycloak login failed ({resp.status_code}): {resp.text[:500]}\n"
            f"Common causes: wrong client_id, client requires a secret you "
            f"didn't pass, or the client doesn't have 'Direct access grants' "
            f"enabled in Keycloak (Clients -> <client> -> Settings -> "
            f"Capability config)."
        )
    token_data = resp.json()
    return token_data["access_token"]

# Documents the prefix->namespace mapping the target server applies
# implicitly (confirmed by it rejecting an inline @context dict with
# this exact mapping -- see build_work_jsonld()'s comment). Kept here
# for reference even though it's no longer sent in any payload.
CONTEXT = {
    "bf": "http://id.loc.gov/ontologies/bibframe/",
    "bflc": "http://id.loc.gov/ontologies/bflc/",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
}

ID_TYPE_MAP = {
    "isbn13": "Isbn", "isbn10": "Isbn", "issn": "Issn",
    "lccn": "Lccn", "oclc": "OclcNumber", "doi": "Doi",
}
OTHER_ID_TYPE_MAP = {"ean": "Ean", "nbn": "Nbn"}


# ---------------------------------------------------------------------------
# JSON-LD PAYLOAD BUILDERS
# ---------------------------------------------------------------------------

def build_work_jsonld(record: Record) -> dict:
    """Builds the Work JSON-LD payload for POST /works/.

    IMPORTANT: this must be kept in sync with the Work section of
    build_bibframe_graph() in bibframe_pipeline.py -- that function
    builds a static RDF file, this one builds the equivalent live-API
    payload, and they are NOT the same code path. This function used
    to only include title/contribution/seeAlso, silently missing every
    later enrichment (subjects, classification, content/media/carrier,
    the Blue Core profile fields, the ontology fixes) added to
    build_bibframe_graph() over time -- if you add a new field there,
    add it here too, or records pushed live will be far sparser than
    records written to a static RDF file from the same source data.
    """
    doc = {
        # No "@context" key -- the target server rejects an inline
        # context dict (422: "@context must be absent or a URL
        # bibframe-json ships"), since an inline context could itself
        # reference a remote one. The server applies its own default
        # bf/bflc/rdf/rdfs prefix mapping when @context is omitted.
        "@type": ["Work", "Text"],
        "rdfs:label": record.title,
    }

    work_title = record.work_title or record.title
    if work_title:
        doc["title"] = {"@type": "Title", "mainTitle": work_title}

    contributions = []
    if record.author:
        contrib = {
            "@type": ["Contribution", "PrimaryContribution"],
            "agent": {"@type": "Person", "rdfs:label": record.author},
            "role": {"@id": "http://id.loc.gov/vocabulary/relators/aut"},
        }
        ids = record.author_identifiers or {}
        see_also = []
        if "wikidata_qid" in ids:
            see_also.append({"@id": f"http://www.wikidata.org/entity/{ids['wikidata_qid']}"})
        if "viaf_id" in ids:
            see_also.append({"@id": f"http://viaf.org/viaf/{ids['viaf_id']}"})
        if "lccn_authority" in ids:
            see_also.append({"@id": f"http://id.loc.gov/authorities/names/{ids['lccn_authority']}"})
        if see_also:
            contrib["agent"]["rdfs:seeAlso"] = see_also if len(see_also) > 1 else see_also[0]
        contributions.append(contrib)

    for c in record.contributors:
        additional = {"@type": "Contribution",
                      "agent": {"@type": "Agent", "rdfs:label": c["name"]}}
        if c.get("role"):
            additional["role"] = {"rdfs:label": c["role"]}
        contributions.append(additional)
    if contributions:
        doc["contribution"] = contributions if len(contributions) > 1 else contributions[0]

    if record.government_pub_type:
        doc["bflc:governmentPubType"] = {
            "@id": f"http://id.loc.gov/vocabulary/mgovtpubtype/{record.government_pub_type}"}

    if record.origin_date:
        doc["originDate"] = record.origin_date

    if record.intended_audience_code:
        doc["intendedAudience"] = {
            "@id": f"http://id.loc.gov/vocabulary/maudience/{record.intended_audience_code}"}

    if record.illustrative_content_codes:
        codes = [{"@id": f"http://id.loc.gov/vocabulary/millus/{c}"}
                 for c in record.illustrative_content_codes]
        doc["illustrativeContent"] = codes if len(codes) > 1 else codes[0]

    if record.dissertation_note:
        doc["dissertation"] = {"@type": "Dissertation", "rdfs:label": record.dissertation_note}

    # bf:content is REQUIRED per the Blue Core profile; default to
    # "text" when 336 wasn't present, matching build_bibframe_graph().
    content = record.content_type or {"code": "txt", "label": "text"}
    doc["content"] = {"@id": f"http://id.loc.gov/vocabulary/contentTypes/{content['code']}"}

    if record.languages:
        langs = [{"@id": f"http://id.loc.gov/vocabulary/languages/{code}"} for code in record.languages]
        doc["language"] = langs if len(langs) > 1 else langs[0]

    if record.summaries:
        summaries = [{"@type": "Summary", "rdfs:label": s} for s in record.summaries]
        doc["summary"] = summaries if len(summaries) > 1 else summaries[0]

    if record.table_of_contents:
        doc["tableOfContents"] = {"@type": "TableOfContents", "rdfs:label": record.table_of_contents}

    subjects, genre_forms = [], []
    subj_type_map = {"Topic": "Topic", "Place": "Place", "GenreForm": "GenreForm"}
    for subj in record.subjects:
        node = {"@type": subj_type_map[subj["type"]], "rdfs:label": subj["label"]}
        (genre_forms if subj["type"] == "GenreForm" else subjects).append(node)
    if subjects:
        doc["subject"] = subjects if len(subjects) > 1 else subjects[0]
    if genre_forms:
        doc["genreForm"] = genre_forms if len(genre_forms) > 1 else genre_forms[0]

    classifications = []
    class_type_map = {"lcc": "ClassificationLcc", "ddc": "ClassificationDdc",
                       "nlm": "ClassificationNlm"}
    for cls in record.classifications:
        node = {"@type": class_type_map[cls["scheme"]],
                 "classificationPortion": cls["classification_portion"]}
        if "item_portion" in cls:
            node["itemPortion"] = cls["item_portion"]
        classifications.append(node)
    if classifications:
        doc["classification"] = classifications if len(classifications) > 1 else classifications[0]

    relations = []
    for series_text in record.series_statements:
        relations.append({
            "@type": "Relation",
            "associatedResource": {
                "@type": "Work",
                "title": {"@type": "Title", "mainTitle": series_text},
            },
        })
    if relations:
        doc["relation"] = relations if len(relations) > 1 else relations[0]

    if record.decision == "auto_accept" and record.best and record.best.entity_role == "work":
        doc["rdfs:seeAlso"] = {"@id": f"http://www.wikidata.org/entity/{record.best.qid}"}
    return doc


def build_instance_jsonld(record: Record) -> dict:
    """Builds the Instance JSON-LD payload for the vnd.sinopia+json
    envelope on POST /instances/. See build_work_jsonld()'s docstring
    -- same keep-in-sync warning applies here for the Instance section
    of build_bibframe_graph()."""
    doc = {
        # See build_work_jsonld()'s comment -- same reason for omitting
        # "@context" here.
        "@type": ["Instance"],
        "rdfs:label": record.title,
    }

    if record.title:
        title_node = {"@type": "Title", "mainTitle": record.title}
        if record.subtitle:
            title_node["subtitle"] = record.subtitle
        titles = [title_node]
        for variant_text in record.variant_titles:
            titles.append({"@type": "VariantTitle", "mainTitle": variant_text})
        doc["title"] = titles if len(titles) > 1 else titles[0]

    if record.responsibility_statement:
        doc["responsibilityStatement"] = record.responsibility_statement

    if record.edition_statement:
        doc["editionStatement"] = record.edition_statement

    if record.pub_date or record.publisher or record.origin_place_code:
        prov = {"@type": "Publication"}
        if record.pub_date:
            prov["date"] = record.pub_date
        if record.publisher:
            prov["agent"] = {"@type": "Agent", "rdfs:label": record.publisher}
        if record.origin_place_code:
            prov["place"] = {"@id": f"http://id.loc.gov/vocabulary/countries/{record.origin_place_code}"}
        doc["provisionActivity"] = prov

    if record.copyright_date:
        doc["copyrightDate"] = record.copyright_date

    if record.series_statements:
        doc["seriesStatement"] = (record.series_statements
                                      if len(record.series_statements) > 1
                                      else record.series_statements[0])

    if record.extent:
        doc["extent"] = {"@type": "Extent", "rdfs:label": record.extent}

    if record.dimensions:
        doc["dimensions"] = record.dimensions

    # bf:issuance/media/carrier are REQUIRED per the profile; default
    # when absent, matching build_bibframe_graph().
    issuance = record.issuance_code or "mono"
    doc["issuance"] = {"@id": f"http://id.loc.gov/vocabulary/issuance/{issuance}"}

    media = record.media_type or {"code": "n", "label": "unmediated"}
    doc["media"] = {"@id": f"http://id.loc.gov/vocabulary/mediaTypes/{media['code']}"}

    carrier = record.carrier_type or {"code": "nc", "label": "volume"}
    doc["carrier"] = {"@id": f"http://id.loc.gov/vocabulary/carriers/{carrier['code']}"}

    if record.notes:
        notes = [{"@type": "Note", "rdfs:label": n} for n in record.notes]
        doc["note"] = notes if len(notes) > 1 else notes[0]

    if record.electronic_locators:
        locs = [{"@id": url} for url in record.electronic_locators]
        doc["electronicLocator"] = locs if len(locs) > 1 else locs[0]

    identifiers = []
    for key, value in record.identifiers.items():
        if key == "viaf_id":
            continue  # VIAF belongs on the agent, not the Instance
        identifiers.append({"@type": ID_TYPE_MAP.get(key, "Identifier"), "rdf:value": value})
    for key, value in (record.other_identifiers or {}).items():
        identifiers.append({"@type": OTHER_ID_TYPE_MAP.get(key, "Identifier"), "rdf:value": value})
    if identifiers:
        doc["identifiedBy"] = identifiers if len(identifiers) > 1 else identifiers[0]

    if record.decision == "auto_accept" and record.best and record.best.entity_role == "instance":
        doc["rdfs:seeAlso"] = {"@id": f"http://www.wikidata.org/entity/{record.best.qid}"}
    return doc


# ---------------------------------------------------------------------------
# API CLIENT
# ---------------------------------------------------------------------------

class BcldApiClient:
    def __init__(self, base_url: str, token: str = None, auth_scheme: str = "Bearer",
                 timeout: int = 30, max_retries: int = 3):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = requests.Session()
        if token:
            self.session.headers["Authorization"] = f"{auth_scheme} {token}"
        self.session.headers["User-Agent"] = "BcldPushClient/0.1"

    def _post(self, path: str, content_type: str, body) -> requests.Response:
        url = f"{self.base_url}{path}"
        last_exc = None
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.post(
                    url,
                    data=json.dumps(body) if content_type != "multipart" else None,
                    headers={"Content-Type": content_type} if content_type != "multipart" else None,
                    timeout=self.timeout,
                )
                if resp.status_code >= 500 or resp.status_code == 429:
                    last_exc = RuntimeError(f"{resp.status_code}: {resp.text[:300]}")
                    time.sleep(2 ** attempt)
                    continue
                return resp
            except requests.RequestException as exc:
                last_exc = exc
                time.sleep(2 ** attempt)
        raise RuntimeError(f"POST {url} failed after {self.max_retries} attempts: {last_exc}")

    def _get(self, path: str, params: dict) -> requests.Response:
        url = f"{self.base_url}{path}"
        last_exc = None
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)
                if resp.status_code >= 500 or resp.status_code == 429:
                    last_exc = RuntimeError(f"{resp.status_code}: {resp.text[:300]}")
                    time.sleep(2 ** attempt)
                    continue
                return resp
            except requests.RequestException as exc:
                last_exc = exc
                time.sleep(2 ** attempt)
        raise RuntimeError(f"GET {url} failed after {self.max_retries} attempts: {last_exc}")

    def search(self, query: str, resource_type: str = "all", limit: int = 20) -> list[dict]:
        """GET /search/?q=...&type=...&limit=... -- the API's only
        lookup endpoint for existing Works/Instances (full-text, not an
        identifier index; see find_duplicate_candidates() for how this
        gets used
        to check for identifier matches anyway). Returns the `results`
        list (each a ResourceBaseSchema dict, with a `data` field
        containing the resource's full JSON-LD)."""
        resp = self._get("/search/", {"q": query, "type": resource_type, "limit": limit})
        if resp.status_code != 200:
            raise RuntimeError(f"search failed: {resp.status_code} {resp.text[:500]}")
        return resp.json().get("results", [])

    def create_work(self, work_jsonld: dict) -> dict:
        """POST /works/ with raw JSON-LD. Returns the parsed WorkSchema dict
        (contains 'id' [int, use for FK linking], 'uri', 'uuid')."""
        resp = self._post("/works/", "application/ld+json", work_jsonld)
        if resp.status_code not in (200, 201):
            raise RuntimeError(f"create_work failed: {resp.status_code} {resp.text[:500]}")
        return resp.json()

    def create_instance(self, instance_jsonld: dict, work_id: int) -> dict:
        """POST /instances/ using the vnd.sinopia+json envelope so the
        integer work_id foreign key is carried explicitly, per
        InstanceCreateSchema in the OpenAPI spec."""
        envelope = {"work_id": work_id, "data": json.dumps(instance_jsonld)}
        resp = self._post("/instances/", "application/vnd.sinopia+json", envelope)
        if resp.status_code not in (200, 201):
            raise RuntimeError(f"create_instance failed: {resp.status_code} {resp.text[:500]}")
        return resp.json()


# ---------------------------------------------------------------------------
# DEDUPLICATION -- mirrors bibframe_pipeline's Wikidata reconciliation
# pattern (Candidate dataclass -> find_candidates() -> score_candidates()
# -> tiered decision), applied against the target database instead.
# ---------------------------------------------------------------------------

# Identifier keys checked for an existing match, in priority order
# (matches ID_PROPERTIES' priority in bibframe_pipeline.py).
DEDUP_IDENTIFIER_ORDER = ("isbn13", "isbn10", "issn", "oclc", "lccn", "doi")

# EAN/NBN (record.other_identifiers) -- lower priority than the main
# identifier set above, but still worth checking; not used for
# Wikidata reconciliation (see ID_PROPERTIES in bibframe_pipeline.py),
# but they ARE pushed to bf:identifiedBy on every Instance, so a
# thorough duplicate check should cover them too.
OTHER_DEDUP_IDENTIFIER_ORDER = ("ean", "nbn")

# Fuzzy title-match floor for treating something as a *possible*
# duplicate. Stricter than bibframe_pipeline's Wikidata
# REVIEW_THRESHOLD (78) since the action gated on this is skipping a
# push entirely, not just withholding a seeAlso link -- a wrong guess
# here is more consequential than a wrong guess there.
DUPLICATE_TITLE_THRESHOLD = 85

# Characters that commonly carry special meaning in PostgreSQL tsquery
# syntax (AND/OR/NOT/grouping/label). If the backend builds a raw
# tsquery from the /search/ `q` parameter without escaping these, a
# literal occurrence in the search text -- e.g. the "&" in a real
# journal title like "Journal of Quantitative Spectroscopy & Radiative
# Transfer" -- produces a malformed tsquery and a generic 500 Internal
# Server Error. Stripping them client-side avoids that without needing
# a backend fix, at the minor cost of losing exact word-boundary
# precision around where the character was.
_SEARCH_QUERY_UNSAFE_CHARS = re.compile(r"[&|!():]")


def _sanitize_search_query(text: str) -> str:
    return re.sub(_SEARCH_QUERY_UNSAFE_CHARS, " ", text or "").strip()


@dataclass
class DuplicateCandidate:
    """Mirrors bibframe_pipeline.Candidate, but for a match against
    the target database rather than Wikidata."""
    existing_id: object = None
    existing_uri: str = ""
    existing_uuid: str = ""
    existing_type: str = ""
    match_type: str = ""       # "identifier" | "title"
    matched_field: str = ""    # e.g. "isbn13", or "title"
    matched_value: str = ""    # the identifier value, or the existing title text
    score: float = 0.0          # 100 for identifier; fuzzy 0-100 for title


def _normalize_identifier_value(value: str) -> str:
    return re.sub(r"[\s\-]", "", str(value)).lower()


def _get_ld_value(resource_data: dict, compact_key: str, expanded_key: str):
    """JSON-LD data returned by /search/ could plausibly come back in
    either compact ('bf:title') or expanded (full IRI) key form --
    we don't have a confirmed live sample to know which, so both are
    tried defensively throughout this module. Also unwraps a
    single-item list, since JSON-LD values are often list-wrapped."""
    if not isinstance(resource_data, dict):
        return None
    val = resource_data.get(compact_key)
    if val is None:
        val = resource_data.get(expanded_key)
    if isinstance(val, list):
        val = val[0] if val else None
    return val


def _resource_matches_identifier(resource_data: dict, id_value: str) -> bool:
    """Check whether a Work/Instance's JSON-LD `data` blob contains an
    identifiedBy entry whose rdf:value matches id_value exactly (after
    stripping hyphens/whitespace, since identifiers are often
    transcribed with varying punctuation).

    Uses the BARE property name ("identifiedBy"), not "bf:identifiedBy"
    -- confirmed against a real live /search/ response, whose returned
    `data` uses bare names for core bf: properties (the server's
    default context treats the BIBFRAME namespace as @vocab), while
    other namespaces like rdfs:/bflc:/mads: keep their prefixes."""
    identified_by = _get_ld_value(resource_data, "identifiedBy",
                                   "http://id.loc.gov/ontologies/bibframe/identifiedBy")
    if not identified_by:
        return False
    entries = identified_by if isinstance(identified_by, list) else [identified_by]
    target = _normalize_identifier_value(id_value)
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        val = (entry.get("rdf:value")
               or entry.get("http://www.w3.org/1999/02/22-rdf-syntax-ns#value"))
        if val and _normalize_identifier_value(val) == target:
            return True
    return False


def _extract_resource_title(resource_data: dict) -> str:
    """title/mainTitle -> plain title text, for fuzzy comparison
    against the incoming record's title. Bare property names, per the
    same confirmed @vocab behavior noted on _resource_matches_identifier()."""
    title_obj = _get_ld_value(resource_data, "title",
                               "http://id.loc.gov/ontologies/bibframe/title")
    if not isinstance(title_obj, dict):
        return ""
    main_title = _get_ld_value(title_obj, "mainTitle",
                                "http://id.loc.gov/ontologies/bibframe/mainTitle")
    return str(main_title) if main_title else ""


def _extract_resource_contributor_labels(resource_data: dict) -> list:
    """contribution/agent/rdfs:label -> list of contributor name
    strings, for the same author-surname bonus check score_candidates()
    already does against Wikidata candidates. Bare property names for
    "contribution"/"agent" (confirmed per the @vocab behavior noted on
    _resource_matches_identifier()); rdfs:label keeps its prefix, since
    rdfs: is a separate namespace the server's default context doesn't
    fold into @vocab.

    Deliberately does NOT use the shared _get_ld_value() helper for
    the top-level contribution lookup -- that helper collapses a
    list to its first item, which is correct for a singular property
    like mainTitle but wrong here: contribution is repeatable, and
    collapsing it to one entry would mean only ever checking the
    first contributor against the incoming record's author surname,
    silently missing every other contributor on the resource."""
    if not isinstance(resource_data, dict):
        return []
    contributions = (resource_data.get("contribution")
                      or resource_data.get("http://id.loc.gov/ontologies/bibframe/contribution"))
    if not contributions:
        return []
    if not isinstance(contributions, list):
        contributions = [contributions]
    labels = []
    for c in contributions:
        if not isinstance(c, dict):
            continue
        agent = c.get("agent") or c.get("http://id.loc.gov/ontologies/bibframe/agent")
        if isinstance(agent, list):
            agent = agent[0] if agent else None
        if isinstance(agent, dict):
            label = agent.get("rdfs:label") or agent.get("http://www.w3.org/2000/01/rdf-schema#label")
            if isinstance(label, list):
                label = label[0] if label else None
            if label:
                labels.append(str(label))
    return labels


def find_duplicate_candidates(client: BcldApiClient, record: Record) -> list:
    """Mirrors bibframe_pipeline.find_candidates(): gather every
    plausible duplicate signal -- both identifier hits AND fuzzy title
    hits -- rather than stopping at the first one found, so scoring
    can weigh the best evidence across all of them.

    The API has no dedicated identifier-lookup or title-lookup
    endpoint, only GET /search/ (a full-text index over labels/
    titles). So both passes work the same way: search using the value
    as free text, then verify what comes back --
    - identifier candidates are verified by an exact bf:identifiedBy
      match (see _resource_matches_identifier) -- a text-search hit
      alone is not proof.
    - title candidates are scored by fuzzy similarity against the
      existing resource's actual bf:title/mainTitle (plus a bonus if
      a contributor name matches the incoming record's author
      surname), the same signals score_candidates() uses for Wikidata.

    Never raises -- a search failure for one identifier/title just
    means that signal contributes nothing, not that the whole check
    aborts; failures are printed as warnings."""
    candidates = []

    # Checks both `record.identifiers` (ISBN/ISSN/OCLC/LCCN/DOI) and
    # `record.other_identifiers` (EAN/NBN) -- the latter was added to
    # the Record dataclass and the API payload builders after this
    # dedup check was first written, and hadn't been wired in here
    # until this pass. Combined into one ordered list so ISBN-family
    # identifiers are still checked first (highest confidence/most
    # universally present), with EAN/NBN as a lower-priority fallback.
    #
    # This is the SECOND time a new Record identifier field has been
    # added elsewhere without this dedup check being updated to match
    # (the first was build_work_jsonld()/build_instance_jsonld() --
    # see their docstrings). If you add another identifier field to
    # Record, add it to DEDUP_IDENTIFIER_ORDER/OTHER_DEDUP_IDENTIFIER_ORDER
    # too, or duplicate checking will silently miss it.
    id_candidates = [(k, record.identifiers.get(k)) for k in DEDUP_IDENTIFIER_ORDER]
    id_candidates += [(k, (record.other_identifiers or {}).get(k)) for k in OTHER_DEDUP_IDENTIFIER_ORDER]

    for id_key, id_value in id_candidates:
        if not id_value:
            continue
        try:
            results = client.search(_sanitize_search_query(id_value), resource_type="all", limit=20)
        except Exception as exc:
            print(f"  [warn] duplicate identifier search failed for {id_key}={id_value}: {exc}",
                  file=sys.stderr)
            continue
        for res in results:
            if _resource_matches_identifier(res.get("data"), id_value):
                candidates.append(DuplicateCandidate(
                    existing_id=res.get("id"), existing_uri=res.get("uri"),
                    existing_uuid=res.get("uuid"), existing_type=res.get("type"),
                    match_type="identifier", matched_field=id_key, matched_value=id_value,
                    score=100.0,
                ))

    if record.title:
        clean_title = _clean_title_for_search(record.title)
        try:
            results = client.search(_sanitize_search_query(clean_title), resource_type="all", limit=20)
        except Exception as exc:
            print(f"  [warn] duplicate title search failed for {record.title[:40]!r}: {exc}",
                  file=sys.stderr)
            results = []
        for res in results:
            existing_title = _extract_resource_title(res.get("data"))
            if not existing_title:
                continue
            title_score = fuzz.token_sort_ratio(clean_title, existing_title)
            author_bonus = 0.0
            if record.author and record.author.split():
                surname = record.author.split()[-1]
                contributor_labels = _extract_resource_contributor_labels(res.get("data"))
                if any(surname.lower() in lbl.lower() for lbl in contributor_labels):
                    author_bonus = 10.0
            candidates.append(DuplicateCandidate(
                existing_id=res.get("id"), existing_uri=res.get("uri"),
                existing_uuid=res.get("uuid"), existing_type=res.get("type"),
                match_type="title", matched_field="title", matched_value=existing_title,
                score=min(100.0, title_score + author_bonus),
            ))

    return candidates


def decide_duplicate(candidates: list) -> tuple:
    """Mirrors bibframe_pipeline.score_candidates()'s decision logic.
    Picks the best (highest-scoring) candidate, then classifies:
    - any identifier match -> 'duplicate_confirmed' (score is always
      100 for these, since only verified exact matches become
      candidates in the first place)
    - a fuzzy title match at or above DUPLICATE_TITLE_THRESHOLD ->
      'duplicate_possible'
    - otherwise -> 'none' (best is returned as None in this case, so
      callers don't accidentally treat a below-threshold candidate as
      meaningful)

    Returns (decision, best_candidate_or_None)."""
    if not candidates:
        return "none", None
    # Sort by score, with identifier matches explicitly preferred over
    # title matches on a tie (e.g. an exact-text title match can also
    # score 100) -- rather than relying on insertion order in
    # find_duplicate_candidates() (identifiers happen to be appended
    # first there, but that's an implementation detail this shouldn't
    # depend on).
    candidates.sort(key=lambda c: (c.score, c.match_type == "identifier"), reverse=True)
    best = candidates[0]
    if best.match_type == "identifier":
        return "duplicate_confirmed", best
    if best.score >= DUPLICATE_TITLE_THRESHOLD:
        return "duplicate_possible", best
    return "none", None


# ---------------------------------------------------------------------------
# ORCHESTRATION
# ---------------------------------------------------------------------------

def push_records(records: list[Record], client: BcldApiClient, dry_run: bool = False,
                  check_duplicates: bool = True) -> list[dict]:
    results = []
    for r in records:
        row = {"local_id": r.local_id, "title": r.title, "status": "", "error": "",
               "work_id": "", "work_uri": "", "instance_id": "", "instance_uri": "",
               "duplicate_match_type": "", "duplicate_matched_value": "",
               "duplicate_score": "", "duplicate_of_uri": ""}

        if check_duplicates:
            try:
                candidates = find_duplicate_candidates(client, r)
                decision, best = decide_duplicate(candidates)
            except Exception as exc:
                # find_duplicate_candidates/decide_duplicate are designed
                # not to raise, but guard anyway -- a failure here should
                # never block a push.
                print(f"  [warn] duplicate check errored for {r.local_id}: {exc}", file=sys.stderr)
                decision, best = "none", None

            if decision in ("duplicate_confirmed", "duplicate_possible"):
                row["status"] = decision
                row["duplicate_match_type"] = best.match_type
                row["duplicate_matched_value"] = best.matched_value
                row["duplicate_score"] = f"{best.score:.0f}"
                row["duplicate_of_uri"] = best.existing_uri or best.existing_uuid or ""
                label = "CONFIRMED" if decision == "duplicate_confirmed" else "POSSIBLE"
                print(f"[DUP-{label}] {r.local_id} {r.title!r} -> matches existing "
                      f"{best.existing_type or 'resource'} {row['duplicate_of_uri']} "
                      f"({best.matched_field}={best.matched_value!r}, score={row['duplicate_score']}); "
                      f"not pushed.")
                results.append(row)
                continue

        work_doc = build_work_jsonld(r)
        instance_doc = build_instance_jsonld(r)

        if dry_run:
            print(f"--- DRY RUN: {r.local_id} {r.title!r} ---")
            print("WORK PAYLOAD (application/ld+json):")
            print(json.dumps(work_doc, indent=2))
            print("INSTANCE PAYLOAD (application/vnd.sinopia+json envelope):")
            print(json.dumps({"work_id": "<pending>", "data": json.dumps(instance_doc)}, indent=2))
            row["status"] = "dry_run"
            results.append(row)
            continue

        try:
            work_resp = client.create_work(work_doc)
            work_id = work_resp.get("id")
            row["work_id"] = work_id
            row["work_uri"] = work_resp.get("uri") or work_resp.get("uuid", "")

            instance_resp = client.create_instance(instance_doc, work_id)
            row["instance_id"] = instance_resp.get("id")
            row["instance_uri"] = instance_resp.get("uri") or instance_resp.get("uuid", "")
            row["status"] = "created"
            print(f"[OK] {r.local_id} {r.title!r} -> work {row['work_uri']}, "
                  f"instance {row['instance_uri']}")
        except Exception as exc:
            row["status"] = "error"
            row["error"] = str(exc)
            print(f"[FAIL] {r.local_id} {r.title!r}: {exc}", file=sys.stderr)

        results.append(row)
    return results


def write_manifest(rows: list[dict], out_path: str) -> None:
    fieldnames = ["local_id", "title", "status", "error",
                  "work_id", "work_uri", "instance_id", "instance_uri",
                  "duplicate_match_type", "duplicate_matched_value",
                  "duplicate_score", "duplicate_of_uri"]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_duplicate_review(rows: list[dict], out_path: str) -> int:
    """Writes just the records that were skipped as likely duplicates
    (both 'duplicate_confirmed' -- identifier match -- and
    'duplicate_possible' -- fuzzy title match) for a human to review.
    The `status` column preserves which tier each row was, so a
    reviewer can prioritize checking the fuzzy title matches (more
    likely to be a false positive) over the identifier-confirmed ones.
    Returns the number of rows written."""
    dup_rows = [r for r in rows if r["status"] in ("duplicate_confirmed", "duplicate_possible")]
    fieldnames = ["local_id", "title", "status", "duplicate_match_type",
                  "duplicate_matched_value", "duplicate_score", "duplicate_of_uri"]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in dup_rows:
            writer.writerow({k: row[k] for k in fieldnames})
    return len(dup_rows)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Push reconciled records to the BIBFRAME API")
    ap.add_argument("input", help="Path to .mrc (MARC21) or .csv/.json (spreadsheet)")
    ap.add_argument("--base-url", default=os.environ.get("BCLD_API_BASE", "https://dev.bcld.info/api"),
                     help="API base URL, e.g. https://dev.bcld.info/api (env: BCLD_API_BASE)")

    # Option A: a pre-obtained static token
    ap.add_argument("--token", default=os.environ.get("BCLD_API_TOKEN"),
                     help="Pre-obtained auth token (env: BCLD_API_TOKEN). "
                          "Skip this if using --keycloak-username instead.")
    ap.add_argument("--auth-scheme", default="Bearer",
                     help="Auth header scheme, default 'Bearer'.")

    # Option B: Keycloak username/password -> exchanged for a token at runtime
    ap.add_argument("--keycloak-username", default=os.environ.get("BCLD_KEYCLOAK_USERNAME"))
    ap.add_argument("--keycloak-password", default=os.environ.get("BCLD_KEYCLOAK_PASSWORD"))
    ap.add_argument("--keycloak-client-id",
                     default=os.environ.get("BCLD_KEYCLOAK_CLIENT_ID", DEFAULT_KEYCLOAK_CLIENT_ID),
                     help="The application's OAuth client id in Keycloak -- "
                          "NOT the 'account'/'account-console' built-in clients. "
                          f"Defaults to '{DEFAULT_KEYCLOAK_CLIENT_ID}'; try "
                          "'bluecore_workflows' if that's rejected for these endpoints.")
    ap.add_argument("--keycloak-client-secret", default=os.environ.get("BCLD_KEYCLOAK_CLIENT_SECRET"),
                     help="Only needed if the client is confidential (has a secret).")
    ap.add_argument("--keycloak-base-url", default=KEYCLOAK_BASE_URL)
    ap.add_argument("--keycloak-realm", default=KEYCLOAK_REALM)

    ap.add_argument("--manifest-out", default="push_manifest.csv")
    ap.add_argument("--duplicate-review-out", default="duplicate_review.csv",
                     help="Records skipped as likely duplicates go here (subset of "
                          "--manifest-out, pulled out for easier review)")
    ap.add_argument("--no-check-duplicates", action="store_true",
                     help="Skip the duplicate check entirely and push every record "
                          "regardless of whether a matching identifier or title "
                          "already exists in the target database")
    ap.add_argument("--limit", type=int, default=None, help="Only push the first N records (testing)")
    ap.add_argument("--dry-run", action="store_true",
                     help="Print exactly what would be sent, without making any network calls. "
                          "Duplicate checking (a read-only GET) still runs, so this also previews "
                          "which records would be skipped as duplicates.")
    ap.add_argument("--skip-reconciliation", action="store_true",
                     help="Skip Wikidata lookups (faster iteration when just testing the API push itself)")
    args = ap.parse_args()

    records = load_records(args.input)
    if args.limit:
        records = records[:args.limit]
    print(f"Loaded {len(records)} records from {args.input}")

    if not args.skip_reconciliation:
        for i, r in enumerate(records, 1):
            try:
                find_candidates(r)
                score_candidates(r)
            except Exception as exc:
                print(f"[{i}/{len(records)}] {r.title[:50]!r:52} -> ERROR: {exc}",
                      file=sys.stderr)
                continue
            print(f"[{i}/{len(records)}] reconciled {r.title[:50]!r:52} -> {r.decision}")

    # Resolve the token to use: prefer an explicit Keycloak login if
    # username/password/client_id were given; otherwise fall back to
    # a pre-obtained static --token.
    token = args.token
    auth_scheme = args.auth_scheme
    if args.keycloak_username and args.keycloak_password and args.keycloak_client_id:
        print("Logging in to Keycloak...")
        token = get_keycloak_token(
            username=args.keycloak_username,
            password=args.keycloak_password,
            client_id=args.keycloak_client_id,
            client_secret=args.keycloak_client_secret,
            keycloak_base_url=args.keycloak_base_url,
            realm=args.keycloak_realm,
        )
        auth_scheme = "Bearer"
        print("Keycloak login OK.")
    elif not args.dry_run and not token:
        print("WARNING: no --token and no --keycloak-username/--keycloak-password/"
              "--keycloak-client-id set. Proceeding unauthenticated; expect 401/403.",
              file=sys.stderr)

    client = BcldApiClient(args.base_url, token=token, auth_scheme=auth_scheme)
    rows = push_records(records, client, dry_run=args.dry_run,
                         check_duplicates=not args.no_check_duplicates)
    write_manifest(rows, args.manifest_out)
    n_dupes = write_duplicate_review(rows, args.duplicate_review_out)

    counts = {}
    for row in rows:
        counts[row["status"]] = counts.get(row["status"], 0) + 1
    print(f"\nSummary: {counts}")
    print(f"Manifest written to {args.manifest_out}")
    if n_dupes:
        print(f"{n_dupes} likely duplicate(s) written to {args.duplicate_review_out}")


if __name__ == "__main__":
    main()
