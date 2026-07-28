# MARC / Spreadsheet → Wikidata Reconciliation → BIBFRAME RDF

Ingests local bibliographic records (MARC21 or a CSV/JSON spreadsheet),
reconciles them against Wikidata, and emits proper **BIBFRAME RDF**
(Work + Instance) in Turtle, JSON-LD, RDF/XML, or N-Triples.

Built around the modeling gap discussed in Mathios, Doğan & Nelson
(2026), *Refining Wikidata's Bibliographic Domain to Increase Reuse in
GLAMs* (JOHD, DOI 10.5334/johd.454): match at the most specific level
you can, keep Work and Instance distinct, and never assert an
uncertain match as fact.

## Install

```bash
pip install -r requirements.txt
```

## Run

```bash
# From a spreadsheet
python bibframe_pipeline.py sample_input.csv --out catalog.ttl --format turtle

# From MARC21 binary -- a real 3-record sample is included:
# sample_MARC_inventory-2443.mrc (Around the World in 80 Days, Hamlet, Emma)
python bibframe_pipeline.py sample_MARC_inventory-2443.mrc --out catalog.jsonld --format json-ld

# RDF/XML or N-Triples
python bibframe_pipeline.py catalog.mrc --out catalog.rdf --format xml
python bibframe_pipeline.py catalog.mrc --out catalog.nt  --format nt
```

Each run also writes a `needs_review.csv` (name it with `--review-out`)
listing every record that didn't get a confident identifier match, so
a human can check it before those links are trusted.

## Why BIBFRAME's Work/Instance split matters here

BIBFRAME's core bibliographic model collapses FRBR's four tiers
(Work, Expression, Manifestation, Item) into three: **Work**,
**Instance** (roughly corresponding to FRBR's Expression+Manifestation
combined), and **Item** (physical/holdings-level copy information —
out of scope for this pipeline, since MARC bibliographic records don't
carry item-level data; that lives in separate holdings records).
BIBFRAME also defines a fourth entity, **`bf:Hub`**, used for cases
like an authorized access point representing a musical/literary work
independent of a specific textual expression — but a Hub is **not**
equivalent to a FRBR Work and isn't treated as one here or anywhere
in this pipeline.

Wikidata, by contrast, tends to blur Work and Edition in practice —
which is exactly the interoperability problem the article surveys.
This pipeline keeps Work and Instance separate on the way out:

- Wikidata items classified as `Q3331189` ("version, edition or
  translation"), or a subclass of it → attached to the **bf:Instance** node.
- Wikidata items classified as `Q386724` ("work"), `Q11424` ("film"),
  or `Q24856` ("film series") — or a subclass of any of those → attached
  to the **bf:Work** node. `Q386724` is used rather than the narrower
  "written work" class so this also covers non-text works; `Q11424`/
  `Q24856` close a specific gap where film/AV items otherwise came back
  `entity_role: unknown` even on a correct match.
- If Wikidata's role is ambiguous, the link is still recorded, but on
  the Work node with an explicit `rdfs:comment` flag telling a
  cataloger to verify before relying on it — it does not silently
  collapse into either tier.

## Match confidence → RDF assertion

| Decision        | Trigger                                   | What lands in the RDF |
|------------------|--------------------------------------------|------------------------|
| `auto_accept`    | Exact identifier match (ISBN/ISSN/OCLC/LCCN/DOI/VIAF) | `rdfs:seeAlso` to the Wikidata QID, on the correct tier |
| `needs_review`   | Fuzzy title/author match only              | `rdfs:comment` note naming the candidate QID and score — **no** `rdfs:seeAlso` |
| `no_match`       | Nothing found or score too low             | Plain local BIBFRAME data, no Wikidata reference |

**The decision only controls whether a Wikidata link gets attached —
it never excludes a record from the RDF output or from being pushed.**
Every record, regardless of `auto_accept`/`needs_review`/`no_match`,
gets a full `bf:Work`/`bf:Instance` pair with all its extracted
bibliographic data (title, identifiers, subjects, notes, etc.) in both
`build_bibframe_graph()` and `push_records()` — nothing is filtered
out based on reconciliation status. A `needs_review` or `no_match`
record just comes out with weaker (or no) Wikidata linkage than an
`auto_accept` one; it's still a complete, valid BIBFRAME record.

Nothing links out to a Wikidata resource unless an identifier
confirmed it. That's a deliberate, permanent gate, not a placeholder —
resolving `needs_review` rows should be a manual (or separately
reviewed) step, since this is precisely where Wikidata's own
bibliographic data quality problems (duplicate/blurred Work items, per
the article) tend to get worse if automated.

Note: `rdfs:seeAlso` is a much weaker claim than `owl:sameAs` — it says
"related resource, go look," not "these are the same entity." That's
intentional here: it avoids committing to entity identity across two
independently-maintained graphs (yours and Wikidata's) where the
Work/Instance boundary itself may be drawn differently.

## Title cleaning before Wikidata search

Real MARC 245 titles often carry trailing ISBD punctuation (`"Title
/"`) or fold the statement of responsibility into the title itself
(`"Hamlet by William Shakespeare."`, or a full archaic title-page
transcription for something like *Emma*). Wikidata's search API does
prefix-style matching against clean labels (`"Hamlet"`, `"Emma"`), so
leaving that extra text in untouched breaks the match entirely rather
than just lowering its score.

Before querying Wikidata (and before scoring), `_clean_title_for_search()`
trims at the first subtitle marker (`:`/`;`) or embedded `"... by
<name>"`, and strips trailing ISBD punctuation. **This only affects
the search query and match score — the title stored in the RDF output
(`bf:mainTitle`) always keeps the full original MARC title.**

This is a heuristic, not a real title-page parser, so it can
occasionally over-trim a title that legitimately contains the word
"by" (e.g. *Gone by Morning* → searches for just *Gone*). That's a
bounded risk, though: it can only push a record toward `needs_review`
or `no_match` with a weaker candidate, never toward an incorrect
`auto_accept` — identifier-based matching (ISBN/ISSN/OCLC/LCCN/DOI/VIAF),
which is what actually gates `auto_accept`, doesn't go through this
cleaning step at all.

## Ontology-verified against the actual BIBFRAME RDF/OWL file

Every `bf:` class and property used by this pipeline was cross-checked
against the Library of Congress's own BIBFRAME ontology file (the
authoritative `owl:Class`/`owl:ObjectProperty`/`owl:DatatypeProperty`
definitions, not just the XSLT's usage patterns) — checking that every
class we emit actually exists, and that its `rdfs:domain`/`rdfs:range`
match how we're using it. This caught five real errors, now fixed
(a second round, against real production records, caught six more —
see below):

- **`bf:IllustrativeContent` → `bf:Illustration`**: the property
  `bf:illustrativeContent`'s declared range is `bf:Illustration`, not
  a same-named class.
- **`bf:NationalBibliographyNumber` → `bf:Nbn`**: the ontology's actual
  class for this identifier type is `bf:Nbn`.
- **`bf:source` now wraps in a `bf:Source` node with `bf:code`**,
  rather than attaching a bare literal directly. The ontology declares
  `bf:source`'s range as the class `bf:Source` (a resource, not a
  literal) — confirmed against the LC marc2bibframe2 XSLT's own
  pattern for source-code attribution elsewhere (e.g.
  `bf:source [ a bf:Source ; bf:code "ISO 3166" ]` for geographic
  coverage codes, and against real id.loc.gov output for a DDC
  classification's edition-source code, e.g.
  `bf:source [ a bf:Source ; bf:code "23" ]`).
- **`bf:relation` now wraps in a `bf:Relation` node with
  `bf:associatedResource`**, rather than pointing directly at a
  `bf:Work` node. The ontology declares `bf:relation`'s range as
  `bf:Relation`; the actual related resource — per the ontology's own
  comment on `bf:associatedResource`, "Suggested value - With Work or
  Hub" — is one level deeper.
- **`bf:issuance`, `bf:originPlace`, `bf:intendedAudience`, and
  `bf:illustrativeContent` no longer use a wrapper node +
  `rdfs:seeAlso`** to point at their LC vocabulary term (e.g.
  `<http://id.loc.gov/vocabulary/issuance/serial>`). That pattern was
  wrong — `rdfs:seeAlso` just says "here's a related resource," not
  "this IS that resource." Confirmed against the LC XSLT's own
  templates for `intendedAudience`/`illustrativeContent` (which emit
  `<bf:IntendedAudience rdf:about="{vocab URI}">` /
  `<bf:Illustration rdf:about="{vocab URI}">` directly) and for
  `issuance` (`ConvSpec-LDR.xsl` emits a plain
  `<bf:issuance rdf:resource="{vocab URI}"/>`, no wrapper at all).
  These four now follow the exact same pattern already used correctly
  for `bf:content`/`bf:media`/`bf:carrier`: the vocabulary term URI
  itself is the resource, typed directly via `_vocab_resource_node()`
  — no intermediate node, no `rdfs:seeAlso`.

### Second pass: verified against real production BIBFRAME records

A second round used actual live `id.loc.gov` BIBFRAME JSON-LD output
(real catalog records produced by LC's own marc2bibframe2 v3.0.0/
v3.2-dev pipeline) as ground truth, rather than just the ontology's
class/property definitions. This caught six more real discrepancies:

- **OCLC numbers now use the dedicated `bf:OclcNumber` class**
  (a subclass of `bf:Identifier`), not `bf:Local` wrapped in a
  `bf:Source [bf:code "OCoLC"]` node. Confirmed against a real record
  where an OCLC number is typed exactly `a bf:OclcNumber` with no
  source sub-node at all.
- **The primary contribution node is now double-typed
  `bf:Contribution, bf:PrimaryContribution`**, not just
  `bf:Contribution`. Every real example checked consistently
  double-types the main-entry contribution this way — additional
  (non-primary) contributions are typed `bf:Contribution` alone.
- **The 008 country-of-publication code now lands on `bf:place`,
  nested inside `bf:provisionActivity`** — not on `bf:originPlace` at
  the Work level, which is what this pipeline used to (incorrectly)
  emit. `bf:originPlace` wasn't observed being used this way in any
  live example checked; every one instead nests `bf:place` (pointing
  to the same `id.loc.gov/vocabulary/countries/{code}` vocabulary
  term) directly inside the `bf:Publication` node on the Instance.
- **246-derived variant titles are now typed `bf:VariantTitle`** (a
  `bf:Title` subclass), not plain `bf:Title` — confirmed against real
  cover-title examples.
- **245 `$a`/`$b` (subtitle) are now kept as separate `bf:mainTitle`/
  `bf:subtitle` properties** on the Title node, rather than
  concatenated into one string. Real output consistently keeps them
  distinct — and, notably, the **Work**'s title only ever carries
  `bf:mainTitle` (never a subtitle), even when it falls back to the
  same 245-derived text as the Instance's title, which does get the
  subtitle. This pipeline's Work title construction already matched
  that (subtitle was only ever added to the Instance title node).
- **Trailing ISBD delimiter punctuation (`:`/`;`/`,`/`/`) is now
  stripped from both title and subtitle** — e.g. `"Masked by trust"`
  rather than `"Masked by trust :"`. A new `_chop_isbd_punct()` helper
  handles this, matching the LC XSLT's own `tChopPunct` pattern.
  Deliberately does *not* strip a bare trailing period, since that can
  be meaningful text rather than an ISBD delimiter.

### Confirmed gaps not yet addressed

A few more real discrepancies turned up but weren't fixed, either
because they need infrastructure this pipeline doesn't have yet or
because the payoff seemed too narrow for the added complexity:

- **Relator terms on additional contributors are still free-text
  labels, not controlled relator URIs.** Real output resolves every
  role — even "contributor" — to a proper
  `<http://id.loc.gov/vocabulary/relators/{code}>` URI. This pipeline
  only does that for the single primary author (where the MARC
  `$e`-to-code mapping is hardcoded to `"aut"`); additional
  contributors' `$e` free text has no such lookup table wired in yet.
  Would need a MARC-relator-term → code table (there are ~200+ terms
  in the full list) to close properly.
- **ISBN `$q` (qualifying information, e.g. "pbk", "softcover",
  "acid-free paper") isn't captured** — real output attaches this via
  `bf:qualifier` on the `bf:Isbn` node.
- **`bf:distributionStatement` / `bf:Distribution`-typed
  `provisionActivity`** (seen on a GPO document with separate
  publication vs. distribution info) isn't extracted — only the
  Publication-type 264 is currently parsed, not a Distribution-type
  one.
- **SuDocs classification** (US government documents' own
  classification scheme, distinct from LCC/DDC/NLM) isn't mapped —
  would need a new MARC field to look for (typically 086) alongside
  050/082/060.
- **`vartitletype` sub-typing on variant titles** (e.g. `"cov"` for a
  cover title) isn't attempted — MARC 246's second indicator maps
  directly to this vocabulary, but that mapping isn't implemented.

Everything else already checked out: class hierarchies (`bf:Isbn`/
`bf:Issn`/`bf:Lccn`/`bf:Local`/`bf:Doi`/`bf:Isni`/`bf:Ean`/`bf:Nbn` are
all confirmed `rdfs:subClassOf bf:Identifier`; `bf:ClassificationLcc`/
`Ddc`/`Nlm` are all confirmed `rdfs:subClassOf bf:Classification`;
`bf:Publication rdfs:subClassOf bf:ProvisionActivity`; `bf:Person
rdfs:subClassOf bf:Agent`), and every domain/range pairing this
pipeline relies on (`bf:content`'s domain is `Work`, `bf:carrier`'s is
`Instance`, `bf:hasInstance`/`bf:instanceOf` are confirmed inverses
with the expected `Work`/`Instance` domain-range pairing, etc.).

`bflc:governmentPubType` is intentionally not part of this check — it
lives in a separate `bflc` (BIBFRAME LC) extension ontology that the
uploaded core BIBFRAME file doesn't include, so its absence there
isn't an error.

## MARC field mapping

| MARC tag/subfield | Maps to |
|---|---|
| 001 | local record identifier |
| 010 $a | LCCN |
| 020 $a | ISBN (auto-detects 10 vs 13 digit) |
| 022 $a | ISSN |
| 035 $a | OCLC number (digits extracted) |
| 100/110/111 $a | bf:Work contribution → agent (books, most print material) |
| 700 $a (relator="director", else first) / 710 $a | **fallback** author, only used when there's no 100/110/111 at all |
| 245 $a/$b | bf:Title (main + subtitle) |
| 260/264 $b | Publisher agent |
| 260/264 $c | Publication date |
| 250 $a | bf:editionStatement (Instance) |
| 300 $a/$f/$g | bf:extent → bf:Extent (Instance) |
| 300 $c | bf:dimensions (Instance) |
| 336 $a/$b | bf:content → bf:Content, URI = contentTypes/{$b} (Work) |
| 337 $a/$b | bf:media → bf:Media, URI = mediaTypes/{$b} (Instance) |
| 338 $a/$b | bf:carrier → bf:Carrier, URI = carriers/{$b} (Instance) |
| 041 $a | bf:language → bf:Language, URI = languages/{code} (Work) |
| 500 $a | bf:note → bf:Note (Instance) |
| 520 $a/$b/$c | bf:summary → bf:Summary (Work) |
| 505 $a/$g/$r/$t | bf:tableOfContents → bf:TableOfContents (Work) |
| 650 / 651 / 655 | bf:subject → bf:Topic/bf:Place, or bf:genreForm → bf:GenreForm (Work) |
| 490 $a | bf:seriesStatement (Instance, literal) |
| 050 $a/$b | bf:classification → bf:ClassificationLcc (Work) |
| 082 $a/$b | bf:classification → bf:ClassificationDdc (Work) |

### Audiovisual/film records have no 1XX main entry

Book-style MARC records put the author in a 100 (personal name) field.
**Audiovisual/moving-image records typically don't** — there's no
"main entry" the way a book has one. Attribution instead lives in
repeated 700 fields (director, actors, producers, composer, etc., each
tagged with a relator term in `$e`) and 710 (corporate/studio credits).

For these, the loader falls back to: the first 700 whose `$e` contains
"director", else the first 700 at all, else the first 710. This
captures **one** primary contributor to match this pipeline's
single-`bf:contribution` mapping — it does not attempt to carry the
full cast/crew list across every repeated 700/710 in the record. Extend
`_marc_to_record()` if you need multiple contributions with distinct
roles preserved.

## Blue Core resource template alignment

Beyond the general BIBFRAME/marc2bibframe2 mapping above, the pipeline
also targets Blue Core's actual Sinopia resource templates for
`bf:Work` (`bluecore:bf2:Monograph:Work`) and `bf:Instance`
(`bluecore:bf2:Monograph:Instance`) specifically — filling as many of
the properties those templates define as the MARC record has data
for, so records land in the target system less sparse than a generic
BIBFRAME conversion would leave them.

### Work-level

| Property | MARC source | Notes |
|---|---|---|
| `bf:title` (Work Title) | 130/240 uniform title, falls back to 245 | The profile treats Work Title and Instance Title as *distinct* — for "Around the World in 80 Days," the Work title is the French uniform title ("Tour du monde en quatre-vingts jours"), separate from the Instance's English 245 title. Most monographs have no 130/240, so this falls back to the same title as the Instance in that common case. |
| `bf:contribution` (AdditionalContribution) | 700/710/711, excluding whichever one was already chosen as the primary `author` | Captures the **full** cast/crew/contributor list with roles (from `$e`) and any `$0`/`$1` authority identifiers — not just the single primary contributor. Role is recorded as a plain `rdfs:label`, not resolved to a controlled relator-code URI, since MARC `$e` is free text (e.g. "screenwriter.") and there's no lookup table wired in to map that back to `id.loc.gov/vocabulary/relators/` codes. |
| `bflc:governmentPubType` | 008/28 | |
| `bf:originDate` | 008 positions 07-10 (date1) | Distinct from the Instance's `provisionActivity` date (260/264 `$c`) — for a modern reprint of a classic work, these can differ by centuries. |
| `bf:originPlace` | 008 positions 15-17 | Linked to `id.loc.gov/vocabulary/countries/{code}` via `rdfs:seeAlso`. |
| `bf:intendedAudience` | 008/22 | Linked to `id.loc.gov/vocabulary/maudience/{code}`. |
| `bf:illustrativeContent` | 008 positions 18-21 (up to 4 codes) | Linked to `id.loc.gov/vocabulary/millus/{code}`. |
| `bf:dissertation` | 502 | Concatenates whatever subfields are present (`$a`/`$b`/`$c`/`$d`); older records often only have `$a`. |
| `bf:relation` (Series/Relation) | 490 | A lightweight "related Work" stub — per the ontology, `bf:relation`'s range is `bf:Relation` (a wrapper node), with the actual related resource linked via `bf:associatedResource`, not attached to `bf:relation` directly. MARC 490 (uncontrolled/transcribed series) doesn't carry enough to link to an authority-controlled series Work the way a linked 8XX would, so the associated resource here is just a minimal `bf:Work` node with a title. `bf:seriesStatement` (below, on the Instance) is **also** still emitted from the same 490 — the two properties aren't mutually exclusive, and emitting both maximizes how much of the profile gets filled. |
| `bf:hasInstance` | — | Explicit inverse of the Instance's `bf:instanceOf`, asserted in both directions since the profile lists it as its own property. |
| `bf:content` | 336, **required** | Defaults to "text" (`txt`) when 336 isn't present, matching the profile's own `ContentType:TextDefault` fallback template — these templates target monographs specifically. |

### Instance-level

| Property | MARC source | Notes |
|---|---|---|
| `bf:title` (Parallel & Variant Titles) | 246 | |
| `bf:responsibilityStatement` | 245 `$c` | |
| `bf:copyrightDate` | 264 with second indicator `4`, `$c` | The profile specifies an EDTF datatype for this property; the raw MARC transcription often includes the `©` symbol itself (e.g. `"©2025"`), which is stripped to produce a bare, EDTF-valid date. Legacy records that fold the copyright date into 260 `$c` free text (e.g. `"c1995"`) instead of a separate 264 `_4` field aren't parsed — only the explicit modern field. |
| `bf:issuance`, **required** | Leader/07 | Mapped to `id.loc.gov/vocabulary/issuance/{mono\|serial\|integrating}`. Defaults to `mono` when the Leader code isn't recognized, matching the profile's `ModeOfIssuance:SingleUnit` default. |
| `bf:media`, `bf:carrier`, **required** | 337/338 | Default to "unmediated"/"volume" when absent, matching the profile's own default templates — same reasoning as `bf:content` above. |
| `bf:electronicLocator` | 856 `$u` | |
| `bf:identifiedBy` (EAN, National Bibliography Number) | 024 (first indicator `3`) / 015 `$a` | These two are captured for RDF output only — unlike ISBN/ISSN/OCLC/LCCN/DOI/VIAF, they're **not** used as Wikidata reconciliation lookups (no corresponding property is wired into `ID_PROPERTIES`). |

### Deliberately not mapped

A handful of profile properties are left out, either because the
MARC source is too ambiguous to map reliably or because the property
is effectively system-generated rather than derived from bibliographic
content:

- **`bf:adminMetadata` / `bf:derivedFrom`** — administrative/
  provenance metadata (cataloging source, status, change date) that
  the target system itself typically generates at ingest time, not
  something meaningfully extracted from the MARC record's own content.
- **`bf:notation` (Script)**, **`bf:accompaniedBy`**,
  **`bf:geographicCoverage`**, **`bf:temporalCoverage`**,
  **`bf:colorContent`**, **`bf:supplementaryContent`**,
  **`bf:digitalCharacteristic`** — each would need a specific,
  reliably-populated MARC field this pipeline doesn't yet parse (066,
  043, 045, 344/347, etc.), with low enough real-world frequency in
  typical monograph records that the added complexity isn't clearly
  worth it yet. Worth revisiting if a particular collection turns out
  to populate these heavily.
- **`bf:expressionOf` (LC Hub/SVDE Opus)** — requires linking to an
  existing authority-controlled Hub or Expression resource, which
  needs the same kind of authority-matching infrastructure already
  built for the author field (see "Person-identity verification"
  above), not yet extended to this property.
- **`bf:provisionActivity` (Manufacture/Production/Distribution)** —
  only the Publication activity (260/264 with the publication
  indicator) is mapped; disambiguating manufacture vs. production vs.
  distribution activities from their respective 264 second-indicator
  values isn't implemented.
- **Instance-level `bf:contribution`** (for publishers/manufacturers
  with an explicit role, distinct from the `provisionActivity` agent)
  — the existing `provisionActivity`/`agent` structure already
  captures the publisher; adding a second, separate Contribution node
  for the same entity risked more redundancy than value without a
  clearer source field to differentiate roles.

### Subject filtering: LCSH only

The profile's subject property is explicitly labeled "Subject--LCSH
only." Accordingly, 650 (topic) and 651 (geographic) subjects are now
filtered to second-indicator `0` (Library of Congress Subject
Headings) — headings sourced from other thesauri (FAST, MeSH, RVM/
Répertoire de vedettes-matière, local vocabularies, etc.) are excluded.
655 (genre/form) has no such restriction in the profile and is left
unfiltered.

## Additional BIBFRAME properties (from the LC marc2bibframe2 XSLT)

The mappings above for 250/300/336/337/338/041/500/520/505/650/651/655/
490/050/082 were traced directly against the Library of Congress's own
[marc2bibframe2](https://github.com/lcnetdev/marc2bibframe2) XSLT
stylesheet — the authoritative MARC→BIBFRAME conversion reference —
rather than guessed. Each property/class name and its Work-vs-Instance
placement matches what that stylesheet actually emits:

- **Work-level**: `bf:content`, `bf:language`, `bf:summary`,
  `bf:tableOfContents`, `bf:subject`, `bf:genreForm`, `bf:classification`
- **Instance-level**: `bf:editionStatement`, `bf:extent`,
  `bf:dimensions`, `bf:media`, `bf:carrier`, `bf:note`,
  `bf:seriesStatement`

Controlled-vocabulary fields (336/337/338) become real linked-data
resources at LC's actual vocabulary URIs — e.g. a 337 with `$b` code
`v` becomes `<http://id.loc.gov/vocabulary/mediaTypes/v>`, typed
`bf:Media` with `rdfs:label "video"` from `$a` — not just a literal.

### Deliberately out of scope (for now)

Each of these is a real simplification relative to the full LC
stylesheet, not an oversight — extending any of them means porting
more of the corresponding `ConvSpec-*.xsl` logic:

- **041 stacked/multi-part codes**: only the common "one or more
  3-letter codes in `$a`" case is handled. The full stylesheet also
  distinguishes translation direction (`$h`/`$k`), ISO 639-1 vs
  639-2/639-3 source vocabularies, and per-part language attribution
  (`$b` target audience language, `$d` sung/spoken text, etc.) — see
  `ConvSpec-010-048.xsl`'s `parse041` template.
- **490 vs. 8XX de-duplication**: `ConvSpec-Process6-Series.xsl`
  avoids emitting a series statement twice when a 490 and a linked 8XX
  series added entry describe the same series. This pipeline captures
  every 490 present without that de-duplication check.
- **Name-based subjects (600/610/611/630)**: only topical (650),
  geographic (651), and genre/form (655) subjects are mapped. Person/
  corporate-body/meeting/uniform-title subjects would need the same
  authority-identifier machinery already built for the author field
  (see "Person-identity verification" above) to do properly, rather
  than just a label.
- **500-series note subtypes**: only the generic 500 general note is
  mapped to `bf:note`. The stylesheet's `ConvSpec-5XX.xsl` breaks out
  ~15 more specific note tags (501, 513, 515, 516, 530, 533, 534, 536,
  544, 545, 550, 555, 556, 581, 585, 588) into distinct
  `id.loc.gov/vocabulary/mnotetype/*` note types.
- **Repeated 250/300/336/337/338**: only the first occurrence of each
  is captured, matching this pipeline's existing single-value pattern
  for author/publisher/date. Rare in practice (mostly relevant for
  multi-part items with per-part physical descriptions).

Extend `_marc_to_record()` if you need series (490), subjects (6xx),
or notes fields carried through — they're not mapped by default to
keep the initial output focused on identification and reconciliation.

## Person-identity verification via $0/$1 (VIAF, ISNI, LC NAF, direct Wikidata links)

MARC 1XX/700/710 fields often carry authority-record links in `$0`
and Real World Object links in `$1` — data that's usually just sitting
there unused. This pipeline now extracts it and uses it two ways:

**1. As a stronger reconciliation signal.** If a record's author field
carries a VIAF ID, LC NAF authority ID, ISNI, or a direct Wikidata
link, the pipeline resolves that person's own Wikidata QID once, then
checks each fuzzy title-search candidate: *is this confirmed person*
*credited as its author/director/screenwriter/creator/composer*
(`wdt:P50|P57|P58|P170|P86`)? If so, that candidate's `person_verified`
flag is set and its score is boosted enough to clear
`AUTO_ACCEPT_PERSON_THRESHOLD` (90) — a distinct, more conservative
tier from the identifier-based `AUTO_ACCEPT_THRESHOLD` (100), gated on
a title-fuzzy-score floor of 60 so a completely wrong title still
can't sneak through on a person match alone.

**2. In the RDF output.** The same identifiers land directly on the
`bf:Agent` node as `rdfs:seeAlso` (VIAF, LC NAF, direct Wikidata QID)
or `bf:identifiedBy`/`bf:Isni` (ISNI) — independent of whether
reconciliation found anything for the Work/Instance itself.

Recognized `$0`/`$1` shapes (based on real MARC data surveyed while
building this):

| Pattern | Extracted as | Used for |
|---|---|---|
| `id.loc.gov/authorities/names/n2001021740` | `lccn_authority` | Wikidata P244 lookup |
| `viaf.org/viaf/12345` | `viaf_id` | Wikidata P214 lookup |
| `isni.org/isni/0000...` | `isni` | Wikidata P213 lookup |
| `wikidata.org/entity/Q12345` | `wikidata_qid` | used directly, no lookup needed |
| `id.oclc.org/worldcat/entity/...` | *(ignored)* | no current Wikidata property maps to OCLC's WorldCat Entity IDs |
| `(SIRSI)12345` / other bracketed local control numbers | *(ignored)* | purely local to the source ILS, not externally resolvable |

This only affects contributors extracted from 100/110/111 or the
700/710 AV fallback (see MARC field mapping above) — it doesn't parse
every repeated 700/710 in a record, matching this pipeline's existing
single-primary-contributor scope.



## Sample files

| File | What it is |
|---|---|
| `sample_input.csv` | 3 spreadsheet rows (Odyssey, Faust, Mrs Dalloway) |
| `sample_MARC_inventory-2443.mrc` | 3 real MARC21 book records (Around the World in 80 Days, Hamlet, Emma) |
| `sample_MARC_av_batch-6.mrc` | 6 real MARC21 audiovisual/film records (Bridget Jones, Edge of Tomorrow, etc.) — exercises the 700/710 author fallback above |
| `sample_MARC_batch2-7.mrc` | 7 more real MARC21 records, non-overlapping with the batch above (The Shining, Heart of a Dog, Translations, etc.) — exercises edition statements, series statements, multiple languages (`041`), and includes one sparse anthology record with no extractable author, for edge-case coverage |
| `MARC-sample-20260722-2455.mrc` | 10 real MARC21 book/media records (Anne of Green Gables, Animal Farm, Data Feminism, Algorithms of Oppression, etc.) — mostly book-style with `100` main entries, and several with a `$0` LC NAF authority link on the author, exercising the person-verification path on real book data rather than just the AV samples above |
| `sample_BIB250403_batch1-25.mrc` ... `batch8-25.mrc` | 8 batches of 25 records each — the first 200 records (plain sequential order, records 0-199) of a larger 68,100-record real bibliographic export (`BIB250403.mrc`). Everything after record 199 in that source file was intentionally left unsampled here. Richly-cataloged academic/research material. |
| `sample_BIB250403_richIDs_batch1-25.mrc` ... `richIDs_batch8-25.mrc` | 8 more batches of 25, but selected differently: these are the 200 **highest-scoring** records from across `BIB250403.mrc`'s full 68,100 records, ranked by (identifiers this pipeline extracts: `010`/`020`/`022`/`035`) + (count of `$0`/`$1` subfields anywhere in the record). ~70–85% of records in each batch have a populated `author_identifiers`, versus ~10–20% in the plain-sequential batches above — the best choice if you specifically want to exercise the person-identity verification path. |
| `sample_BIB250403_journals_batch1-25.mrc` ... `journals_batch8-25.mrc` | 8 more batches of 25: academic journals specifically, filtered to records with Leader/07 == `s` (serial), `008`/21 == `p` (periodical — excludes newspapers, monographic series, updating loose-leafs, etc.), and a title containing "journal". `e-flux` is explicitly excluded (though this filter combination already excludes it naturally, since e-flux Journal issues are cataloged as individual monographs rather than a continuing periodical record). 200 records sampled evenly across all 1,449 matching records in `BIB250403.mrc`, for topical diversity across disciplines rather than clustering near the start of the file. |

## Running against a large MARC batch

If your source file has thousands of records, running the full
Wikidata reconciliation against every one of them will be slow (each
record can trigger several SPARQL/search calls) and risks hitting
Wikidata's public rate limits. Two flags help:

```bash
# Process only the first 50 records -- good for a quick test
python bibframe_pipeline.py big_export.mrc --out sample.ttl --limit 50

# Build local BIBFRAME RDF with no Wikidata calls at all (fast --
# no rdfs:seeAlso links, just your own data)
python bibframe_pipeline.py big_export.mrc --out local_only.ttl --skip-reconciliation
```

`push_to_bcld.py` has the same `--limit` / `--skip-reconciliation`
flags for the same reason.

## Spreadsheet columns

Any subset of: `id, title, author, pub_date, isbn13, isbn10, issn,
oclc, lccn, doi, viaf_id`. Column matching is **case-insensitive**
and tries a set of common real-world header aliases before giving up
— e.g. a Goodreads CSV export with headers like `Title`, `Author`,
`ISBN13`, `ISBN` (10-digit), and `Original Publication Year` /
`Year Published` works without renaming anything, since those are
already in `COLUMN_ALIASES` at the top of `bibframe_pipeline.py`.
`id`/`Book Id` is picked up for `local_id` the same way.

If your headers don't match any alias, either rename them, or add
your own alias to the relevant list in `COLUMN_ALIASES` (preferred —
helps future runs too), or set `COLUMN_MAP` for a one-off override.
**Symptom of a column mismatch**: every record comes back `no_match`
with no candidates at all, because there's no title or identifier to
search on in the first place — check `r.title`/`r.identifiers` on a
loaded record if reconciliation results look suspiciously uniform.

## Minting URIs

Work and Instance URIs follow the target database's pattern:

```
https://dev.bcld.info/works/<uuid>
https://dev.bcld.info/instances/<uuid>
```

(`WORK_BASE` / `INSTANCE_BASE` at the top of `bibframe_pipeline.py` —
change these if the target host changes, e.g. moving from `dev.` to a
production domain.)

UUIDs are minted with `uuid.uuid5()`, deterministically seeded from
each record's local identifier (MARC 001, or the `id` column). That
means **re-running the pipeline on the same source data reproduces the
same URIs** instead of minting a new resource every run — useful if
you're re-processing a corrected export. If you'd rather have the
target system mint/dedupe identity itself, swap `uuid.uuid5(...)` for
`uuid.uuid4()` in `_mint_uri()`.

## Writing directly to the database (`push_to_bcld.py`)

`bibframe_pipeline.py` produces a portable RDF file. If you instead
want to write straight into the live database over its REST API,
use `push_to_bcld.py` — a companion script that reuses the same
loaders and Wikidata reconciliation, but POSTs each record as
`Work` then `Instance` instead of serializing RDF to disk.

```bash
export BCLD_API_TOKEN=your-token-here
python push_to_bcld.py catalog.csv \
    --base-url https://dev.bcld.info/api \
    --manifest-out push_manifest.csv
```

**Always dry-run first** — this prints every payload without making
any network call:

```bash
python push_to_bcld.py catalog.csv --dry-run
```

### How it maps to the API

- `POST /works/` with raw `application/ld+json` — matches the
  `bf:Work`/`bf:Text` example in the API docs exactly (title,
  contribution/agent, and `rdfs:seeAlso` to Wikidata when the match
  is `auto_accept` and classified as Work-level).
- The response's integer `id` becomes the `work_id` foreign key.
- `POST /instances/` uses the `application/vnd.sinopia+json` envelope
  (`{"work_id": <int>, "data": "<jsonld string>"}`) rather than raw
  JSON-LD, because that's the content type the spec's
  `InstanceCreateSchema` actually carries the FK on.
- **The server mints its own UUID/URI on creation.** This script does
  not try to pre-assign the `https://dev.bcld.info/works/<uuid>`-style
  URI the way the static RDF pipeline does — it uses whatever URI
  comes back in the response.

### Payload builders now match `build_bibframe_graph()` field-for-field

`build_work_jsonld()`/`build_instance_jsonld()` build the live payload;
`bibframe_pipeline.py`'s `build_bibframe_graph()` builds the equivalent
static RDF file. **These are two separate code paths, not one shared
implementation** — early in this project's development, the JSON-LD
builders only included title/contribution/`identifiedBy`/`seeAlso`,
and were never updated as `build_bibframe_graph()` gained everything
else (subjects, classification, content/media/carrier, the full Blue
Core profile alignment, the ontology corrections). The result: records
pushed live were far sparser than the same source data written to a
static RDF file — a real bug, not an API/database limitation. Fixed by
rewriting both builders to explicitly include every field
`build_bibframe_graph()` does, including the same required-property
defaults (`bf:content`/`issuance`/`media`/`carrier` always populate
even when the source MARC lacks 336/337/338/Leader-derived data) and
the same corrected identifier typing (`bf:OclcNumber`, not `bf:Local`).

**If you add a new field to `build_bibframe_graph()` in
`bibframe_pipeline.py`, add the equivalent to `build_work_jsonld()`/
`build_instance_jsonld()` here too** — nothing currently enforces they
stay in sync, so this can silently drift again. If you notice a pushed
record missing data that the same record's static RDF output has, this
divergence is the first thing to check.

### Duplicate checking

Before pushing each record, the script checks whether the target
database already has a matching Work/Instance — using **both**
bibliographic identifiers (ISBN/ISSN/OCLC/LCCN/DOI, plus EAN/NBN from
`record.other_identifiers`) **and** fuzzy title matching — and if so,
**skips the push entirely** rather than creating a duplicate. Matches
go to a separate `duplicate_review.csv`.

This mirrors the same pattern `bibframe_pipeline.py` already uses for
Wikidata reconciliation — gather every plausible signal into
candidates, score them, then classify by confidence tier — just
pointed at the target database instead of Wikidata:

1. **`find_duplicate_candidates()`** (mirrors `find_candidates()`):
   searches the target database by each identifier value *and* by
   the record's title, collecting every plausible hit rather than
   stopping at the first one.
2. **`decide_duplicate()`** (mirrors `score_candidates()`): picks the
   best candidate and classifies it into one of two tiers.

| Tier | Trigger | What happens |
|---|---|---|
| `duplicate_confirmed` | An exact `bf:identifiedBy` match, verified (not just a text-search hit) | Push skipped |
| `duplicate_possible` | A fuzzy title match ≥ 85 (`DUPLICATE_TITLE_THRESHOLD`) — boosted +10 if a contributor name on the existing resource matches the incoming record's author surname, same signal `score_candidates()` uses for Wikidata | Push skipped |
| *(neither)* | Nothing found, or best fuzzy match below 85 | Pushed normally |

**Both tiers block the push**, per how this was scoped — a fuzzy
title match alone is less certain than an identifier match, but still
enough to hold off rather than risk creating a real duplicate. The
`status` column in the review CSV preserves which tier fired, so a
reviewer can prioritize checking the `duplicate_possible` rows (more
likely to be a false positive) over `duplicate_confirmed` ones.

**How the search itself works, and why it's "best-effort":** the API
has no dedicated identifier-lookup or title-lookup endpoint, only
`GET /search/`, a full-text index over labels/titles — not guaranteed
to even index nested `bf:identifiedBy` values. So both signals work
the same way: search using the value as free text, then **verify**
what comes back —
- identifier hits are verified by an exact `bf:identifiedBy` match
  (hyphens/whitespace stripped, since identifiers get transcribed
  with varying punctuation) — a text-search hit alone is never
  treated as proof.
- title hits are scored by fuzzy similarity (`rapidfuzz`) against the
  existing resource's actual `bf:title`/`mainTitle`, using the same
  title-cleaning (`_clean_title_for_search`) the Wikidata reconciler
  uses, so ISBD punctuation and embedded statements of responsibility
  don't break the comparison here either.

This means the check can have **false negatives** (an existing
duplicate that `/search/` doesn't surface as a text hit in the first
place) but shouldn't produce **false positives** for the confirmed
tier (nothing gets marked `duplicate_confirmed` without a verified
exact identifier match); the `duplicate_possible` tier is fuzzy by
design and *can* occasionally flag something that isn't really a
duplicate — that's exactly why it's routed to a review file rather
than treated as certain.

**A specific failure mode worth knowing about:** search queries are
sanitized (`_sanitize_search_query()`) to strip `&`, `|`, `!`, `(`,
`)`, and `:` before being sent — these carry special meaning in
PostgreSQL's `tsquery` syntax (AND/OR/NOT/grouping), and a title
containing one literally (e.g. a real journal title like *"Journal of
Quantitative Spectroscopy & Radiative Transfer"*) can produce a
malformed query and a generic `500 Internal Server Error` from the
backend if it builds a raw `tsquery` from the search text without
escaping it. Only the *search query* is sanitized — the original text
is still used for this pipeline's own local fuzzy-matching/scoring, so
sanitization doesn't affect match quality, just what gets sent over
the wire.

A search failure (network error, 5xx) for either signal is treated
as "couldn't verify," not "confirmed duplicate" — it prints a warning
and the check moves on, so a transient API problem never silently
blocks a legitimate record.

```bash
# Default: duplicate checking (identifier + title) is on
python push_to_bcld.py catalog.csv --dry-run

# --dry-run still runs the (read-only) duplicate check, so this
# previews both what would be created AND what would be skipped
python push_to_bcld.py catalog.csv --dry-run

# Disable entirely and push every record regardless
python push_to_bcld.py catalog.csv --no-check-duplicates
```

`push_manifest.csv` includes every record either way, with
`duplicate_confirmed` / `duplicate_possible` as two of the possible
`status` values (alongside `created`/`error`/`dry_run`), plus
`duplicate_match_type`, `duplicate_matched_value`, `duplicate_score`,
and `duplicate_of_uri` columns filled in for skipped records.
`duplicate_review.csv` is just that subset (both tiers), pulled out
for easier review — pass `--duplicate-review-out` to change its path.

### Auth

This deployment authenticates via **Keycloak** (realm `bluecore`, at
`https://dev.bcld.info/keycloak/`), not a static API token. The script
exchanges a username/password for a short-lived access token via
Keycloak's Direct Access Grant (OAuth2 `password` grant), then uses
that token as the bearer on every API call.

```bash
python push_to_bcld.py sample_input.csv \
    --keycloak-username your.username \
    --keycloak-password '...'  \
    --keycloak-client-id <the-app-client-id> \
    --dry-run
```

(Or set `BCLD_KEYCLOAK_USERNAME` / `BCLD_KEYCLOAK_PASSWORD` /
`BCLD_KEYCLOAK_CLIENT_ID` as env vars instead of flags, so credentials
don't end up in shell history.)

**Finding the right `client_id`:** open Keycloak's admin console →
realm `bluecore` → Clients. Skip `account` and `account-console` —
those are Keycloak's built-in clients for end-users to manage their
own profile, and won't be authorized against this API. This realm has
two app-specific clients: **`bluecore_api`** (the default this script
uses — it should map to the `/works/`, `/instances/` endpoints this
script calls) and `bluecore_workflows` (more likely covers the
`/batches/` and `/export/` Airflow-triggered endpoints instead — try
this one with `--keycloak-client-id bluecore_workflows` if
`bluecore_api` gets rejected for some reason).

If that client is confidential (has a secret configured) rather than
public, also pass `--keycloak-client-secret` /
`BCLD_KEYCLOAK_CLIENT_SECRET` — Keycloak returns `unauthorized_client`
otherwise. The client also needs **Direct access grants** enabled
(Clients → `<client>` → Settings → Capability config) for this
username/password flow to work at all — Keycloak returns a 400 with
`invalid_grant` or similar if it isn't.

If you already have a token from somewhere else, you can skip
Keycloak entirely and pass it directly with `--token` /
`BCLD_API_TOKEN` — same as before.


### Non-idempotency warning

There is no documented "upsert by URI" on this API. **Re-running this
script against the same source data will create duplicate Works and
Instances** — it doesn't check for existing resources first. That's
why every run writes a manifest CSV (`local_id → work id/uri,
instance id/uri`); keep it, and don't re-push records that are
already in the manifest as `created`.

### Testing the push logic without touching the database

```bash
python push_to_bcld.py catalog.csv --dry-run --skip-reconciliation --limit 3
```

`--skip-reconciliation` skips the Wikidata lookups (faster iteration),
`--limit N` caps how many records are processed — useful for a quick
sanity check before a full run.

## Running it in Jupyter (`bibframe_notebook.ipynb`)

If you'd rather work interactively than run CLI scripts, use the
notebook — it wraps `bibframe_pipeline.py` and `push_to_bcld.py` as
importable modules (their `main()` only fires on direct execution, so
importing is safe) and walks through the same stages as separate
cells:

0. Setup (`%pip install`)
1. Configure input file, RDF output path, API base URL, and token
   (via `getpass`, so it's never typed into a saved cell)
2. Load records → preview as a `pandas` DataFrame
3. Reconcile against Wikidata, with per-record progress printed
4. Inspect anything that needs review before doing anything with it
5a. Build the RDF file, preview the serialized output inline
5b. Push to the live API — a dry-run cell (no network calls) followed
    by a live-push cell gated behind a `CONFIRM_LIVE_PUSH = False`
    flag you have to deliberately flip to `True`

Put `bibframe_notebook.ipynb` in the same folder as the two `.py`
files and your source data, then `jupyter notebook` or open it in
JupyterLab/VS Code.

I tested the notebook's execution path end-to-end (load → reconcile →
build RDF → preview → dry-run push → live-push gate) using mocked
reconciliation results, since this sandbox can't reach Wikidata or
`dev.bcld.info` directly — the cell structure and logic run clean, but
give the real Wikidata-calling cell a moment the first time you run it
for real.

## Rate limits

The public Wikidata SPARQL endpoint throttles bulk querying. For large
catalogs, consider a local Wikidata Query Service mirror (Blazegraph/
QLever) or batch multiple identifiers into one SPARQL query with a
`VALUES` clause rather than querying record-by-record.
