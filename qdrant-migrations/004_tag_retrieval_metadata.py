"""Tag pdf_knowledge_base chunks with retrieval metadata so Personalized-Quiz-Service can ground quiz
generation in the right part of each textbook — see qdrant-migrations/_lib/runner_support.py.

Adds three payload fields to the already-ingested textbook chunks (nothing is re-ingested, no vectors
change, and no existing field is modified):

- content_type: "contents" (a book's contents/index pages), "front_matter" (credits, preface, messages
  before the first chapter), "back_matter" (glossary, revision exercises, tables after the last chapter)
  or "content" (everything else). Retrieval can then skip pages that only *mention* a lesson title.
- lesson: the curriculum lesson a content chunk belongs to, for books whose contents page could be read
  reliably. Lesson names match Personalized-Quiz-Service's app/data/curriculum/grade_{10,11}.json
  exactly (the same canonical-name rule 003 uses for `subject`).
- a full-text index on `text`, so retrieval can add keyword matching ("carbohydrate", "displacement")
  next to the existing dense search without new vectors or re-indexing.

_BOOKS below maps each textbook file to its lessons' first PDF page (0-based, like `page_start`). It was
derived from each book's own contents page (printed page numbers) plus the book's printed-to-PDF page
offset, measured from the page numbers printed in the chunk text, and spot-checked against where each
chapter title actually appears. Books not listed (no readable contents page in the ingested text) only
get content_type, from the front-matter heuristics in _classify — never a guessed lesson.

Fields other services don't know about are ignored by them, so this is invisible to anything that
doesn't filter on them. downgrade() removes the fields and the new indexes again.
"""

import logging
import re
import statistics

from qdrant_client.models import (
    Filter,
    FilterSelector,
    TextIndexParams,
    TextIndexType,
    TokenizerType,
)

REVISION = "004"
DESCRIPTION = "tag pdf_knowledge_base chunks with content_type/lesson and add a full-text index on text"

_COLLECTION = "pdf_knowledge_base"
_FIELDS = ["content_type", "lesson"]

logger = logging.getLogger(__name__)

_BOOKS: dict[str, dict] = {
    'ICT grade 10.pdf': {
        "back_matter_from": None,
        "lessons": [
            ('Information and Communication Technology', 9),
            ('Fundamentals of a computer system', 40),
            ('Data Representation Methods in the Computer system', 76),
            ('Logic Gates with Boolean Functions', 116),
            ('Operating Systems', 137),
            ('Word Processing', 162),
            ('Electronic Spreadsheet', 198),
            ('Electronic Presentations', 238),
            ('Database', 259),
        ],
    },
    'ICT grade 11.pdf': {
        "back_matter_from": None,
        "lessons": [
            ('Programming', 11),
            ('System Development Life Cycle', 63),
            ('The Internet and the Electronic Mail', 74),
            ('Use of Multimedia', 91),
            ('Web Designing Using Multimedia', 135),
            ('Information and Communication Technology and Society', 178),
        ],
    },
    'english grade 11 work book.pdf': {
        "back_matter_from": None,
        "lessons": [
            ('Our Responsibilities', 11),
            ('Facing Challenges', 19),
            ('Great Lanka', 30),
            ('For A Better Tomorrow', 39),
            ('Best Use of Time', 59),
            ('A Moment of Fun', 69),
            ('A Simple Living', 77),
            ('Reading is Fun', 87),
            ('Enigma', 110),
            ('Choices in Life', 119),
        ],
    },
    'geography grade 11.pdf': {
        "back_matter_from": None,
        "lessons": [
            ('Natural resources of the Earth', 11),
            ('Natural resources of Sri Lanka', 30),
            ('The world population', 62),
            ('The population of Sri Lanka', 73),
            ('Development', 82),
            ('Natural hazards in the world', 97),
            ('Natural hazards in Sri Lanka', 113),
            ('Climatic changes', 126),
            ('1: 50 000 Topographical maps of Sri Lanka', 133),
            ('Using graphs to represent data', 162),
        ],
    },
    'health science grade 11.pdf': {
        "back_matter_from": None,
        "lessons": [
            ('Let us build a healthy society', 13),
            ('Let us identify stages in life after childhood', 27),
            ('Let us identify principles of biomechanics to maintain correct postures', 40),
            ('Let us play Volleyball', 52),
            ('Let us play Netball', 56),
            ('Let us play Football', 64),
            ('Let us use equipment adapting correct postures', 75),
            ('Let us engage in outdoor activities', 80),
            ('Let us learn about running events in athletics', 91),
            ('Let us cooperate with management and organizing through sports', 102),
            ('Let us consume nutritious food for a healthy life', 123),
            ('Let us learn about jumping and throwing events in athletics', 139),
            ('Let us understand the musculoskeletal system', 151),
            ('Let us maintain fitness related to motor skills', 171),
            ('Let us maintain good interpersonal relationships', 183),
            ('Let us identify the challenges in adolescence', 189),
            ('Let us prevent sexually transmitted diseases', 198),
        ],
    },
    'histoy grade 10.pdf': {
        "back_matter_from": None,
        "lessons": [
            ('Sources of Studying History', 12),
            ('Ancient Settlements', 21),
            ('Evolution of Political Power in Sri Lanka', 42),
            ('The Ancient Society of Sri Lanka', 55),
            ('The Ancient Science and Technology in Sri Lanka', 74),
            ('Historical Knowledge and Its Practical Application', 88),
            ('Decline of Ancient Cities in the Dry Zone and Origin of New Kingdoms in South West', 102),
            ('Kandyan Kingdom', 117),
            ('Renaissance', 129),
            ('Sri Lanka and the Western World', 138),
        ],
    },
    'histoy grade 11.pdf': {
        "back_matter_from": None,
        "lessons": [
            ('Industrial Revolution', 11),
            ('Establishment of British Power in Sri Lanka', 29),
            ('National Renaissance in Sri Lanka', 51),
            ('Political Changes in Sri Lanka under the British', 61),
            ('Social Changes in Sri Lanka Under the British', 75),
            ('Receiving of Independence to Sri Lanka', 89),
            ('Significant Revolutions in the World', 116),
            ('World Wars and Conventions', 136),
        ],
    },
    'mathematics grade 10 part 1.pdf': {
        "back_matter_from": 196,
        "lessons": [
            ('Perimeter', 10),
            ('Square Root', 23),
            ('Fractions', 32),
            ('Binomial Expressions', 49),
            ('Congruence of Triangles', 57),
            ('Area', 74),
            ('Factors of Quadratic Expressions', 83),
            ('Triangles I', 94),
            ('Triangles II', 107),
            ('Inverse Proportions', 120),
            ('Data Representation', 128),
            ('Least Common Multiple of Algebraic Expressions', 138),
            ('Algebraic Fractions', 143),
            ('Percentages', 147),
            ('Equations', 159),
            ('Parallelograms I', 169),
            ('Parallelograms II', 178),
            ('Sets', 186),
        ],
    },
    'mathematics grade 11 part 1.pdf': {
        "back_matter_from": 114,
        "lessons": [
            ('Real Numbers', 9),
            ('Indices and Logarithms -I', 23),
            ('Indices and Logarithms -II', 35),
            ('Surface Area of Solids', 58),
            ('Volume of Solids', 71),
            ('Binomial Expressions', 82),
            ('Algebraic Fractions', 88),
            ('Areas of Plane Figures between Parallel Lines', 95),
        ],
    },
    'mathematics grade 11 part 2.pdf': {
        "back_matter_from": 145,
        "lessons": [
            ('Percentages', 9),
            ('Share Market', 19),
            ('The Midpoint Theorem', 30),
            ('Graphs', 43),
            ('Equations', 65),
            ('Equiangular Triangles', 84),
            ('Data representation and Interpretation', 108),
            ('Geometric Progressions', 130),
        ],
    },
    'mathematics grade 11 part 3.pdf': {
        "back_matter_from": 153,
        "lessons": [
            ("Pythagoras' Theorem", 10),
            ('Trigonometry', 21),
            ('Matrices', 51),
            ('Inequalities', 66),
            ('Cyclic Quadrilaterals', 72),
            ('Tangents', 88),
            ('Constructions', 110),
            ('Sets', 126),
            ('Probability', 137),
        ],
    },
    'science grade 10 part 1.pdf': {
        "back_matter_from": None,
        "lessons": [
            ('Chemical basis of life', 16),
            ('Motion in a straight line', 38),
            ('Structure of matter', 67),
            ("Newton's laws of motion", 99),
            ('Friction', 113),
            ('Structure and functions of the plant and animal cell', 125),
            ('Quantification of elements and compounds', 138),
            ('Characteristics of organisms', 154),
            ('Resultant force', 171),
            ('Chemical bonds', 183),
            ('Turning effect of a force', 204),
            ('Equilibrium of forces', 216),
        ],
    },
    'science grade 11 part 1.pdf': {
        "back_matter_from": None,
        "lessons": [
            ('Living tissues', 12),
            ('Photosynthesis', 31),
            ('Mixtures', 41),
            ('Waves and their applications', 83),
            ('Geometrical Optics', 116),
            ('Biological processes in human body', 154),
            ('Acids, bases and salts', 201),
            ('Heat changes associated with chemical reactions', 213),
        ],
    },
    'science grade 11 part 2.pdf': {
        "back_matter_from": None,
        "lessons": [
            ('Heat', 12),
            ('Power and Energy of Electric Appliances', 43),
            ('Electronics', 61),
            ('Electrochemistry', 90),
            ('Electromagnetism and Electromagnetic Induction', 126),
            ('Hydrocarbons and Their Derivatives', 162),
            ('Biosphere', 178),
        ],
    },
}

# Untagged books: only the opening pages can be front matter, and only when they look like it.
_FRONT_MATTER_MAX_PAGE = 14
_FRONT_MATTER_MARKERS = re.compile(
    r"technical assistance|language editing|proof reading|board of (editors|writers|compilers)|"
    r"editorial board|message of the|foreword|preface|isbn|printed (and|&) (published|distributed)|"
    r"all rights reserved|national anthem|cover (page|design)|computer (designing|typesetting)",
    re.IGNORECASE,
)
_CONTENTS_HEADING = re.compile(r"(?:^|\n)\s*(?:table of )?(?:contents?|index)\s*(?:\n|$)", re.IGNORECASE)
# Contents entries, one per line: "3. Structure of matter 52", "2.4 Acceleration\n\n32", "6 Word Processing 154".
# The title must be words starting with a capital -- numbered maths/science text ("12 m from A on a bearing of
# 080", "1 protons 1") must not count.
_CONTENTS_ENTRY = re.compile(
    r"^[ \t]*\d{1,2}(?:\.\d{1,2})?\.?[ \t]+[A-Z][A-Za-z'’,:()\-–]*(?:[ \t]+[A-Za-z'’,:()\-–]+){0,12}[ \t]*(?:\n[ \t]*){0,2}\d{1,3}[ \t]*$",
    re.MULTILINE,
)
_CONTENTS_MIN_ENTRIES = 4


def _norm(text: str) -> str:
    return " ".join(re.sub(r"[^a-z0-9]+", " ", text.lower()).split())


def _looks_like_contents(text: str) -> bool:
    return bool(_CONTENTS_HEADING.search(text)) or len(_CONTENTS_ENTRY.findall(text)) >= _CONTENTS_MIN_ENTRIES


def _classify(filename: str, page_start: int, pages: list[int], text: str) -> dict:
    book = _BOOKS.get(filename)
    last_page = max(pages) if pages else page_start

    # pages == [0] means ingestion didn't record a page number, not that the chunk is on page 0 — its
    # position is unknown, so it's only judged by what it looks like and never gets a lesson.
    if book is None or last_page == 0:
        if page_start <= _FRONT_MATTER_MAX_PAGE:
            if _looks_like_contents(text):
                return {"content_type": "contents"}
            if _FRONT_MATTER_MARKERS.search(text):
                return {"content_type": "front_matter"}
        return {"content_type": "content"}

    lessons = book["lessons"]
    first_start = lessons[0][1]
    # Before chapter 1 -- including a contents list whose chunk runs on into chapter 1's first page.
    if page_start < first_start and _looks_like_contents(text):
        return {"content_type": "contents"}
    if last_page < first_start:
        return {"content_type": "front_matter"}
    back_from = book["back_matter_from"]
    if back_from is not None and page_start >= back_from:
        return {"content_type": "back_matter"}

    # A chunk can span 2-4 pages; attribute it to the lesson owning its middle page...
    middle = round(statistics.median(pages or [page_start]))
    lesson = None
    for name, start in lessons:
        if start <= middle:
            lesson = name
    # ...unless it opens with the title of a chapter starting within a couple of pages: page ranges come from
    # printed page numbers and can be a page off at chapter boundaries, the heading itself can't.
    opening = _norm(text[:250])
    for name, start in lessons:
        if abs(start - middle) <= 2 and _norm(name) in opening:
            lesson = name
    return {"content_type": "content", "lesson": lesson} if lesson else {"content_type": "content"}


def upgrade(client) -> None:
    client.create_payload_index(_COLLECTION, field_name="content_type", field_schema="keyword")
    client.create_payload_index(_COLLECTION, field_name="lesson", field_schema="keyword")
    client.create_payload_index(
        _COLLECTION,
        field_name="text",
        field_schema=TextIndexParams(
            type=TextIndexType.TEXT, tokenizer=TokenizerType.WORD, lowercase=True, min_token_len=2, max_token_len=30,
        ),
    )

    counts: dict[str, int] = {}
    lessons_tagged = 0
    offset = None
    while True:
        points, offset = client.scroll(
            _COLLECTION,
            limit=200,
            offset=offset,
            with_payload=["filename", "source", "page_start", "pages", "text"],
            with_vectors=False,
        )
        if not points:
            break

        groups: dict[tuple, list] = {}
        for point in points:
            payload = point.payload or {}
            filename = payload.get("filename") or payload.get("source") or ""
            page_start = payload.get("page_start") or 0
            fields = _classify(filename, page_start, payload.get("pages") or [page_start], payload.get("text") or "")
            groups.setdefault(tuple(sorted(fields.items())), []).append(point.id)

        for key, ids in groups.items():
            fields = dict(key)
            client.set_payload(_COLLECTION, payload=fields, points=ids)
            counts[fields["content_type"]] = counts.get(fields["content_type"], 0) + len(ids)
            if fields.get("lesson"):
                lessons_tagged += len(ids)

        if offset is None:
            break

    logger.info("Tagged content_type on %s; lesson on %d content chunks", counts, lessons_tagged)


def downgrade(client) -> None:
    client.delete_payload(_COLLECTION, keys=_FIELDS, points=FilterSelector(filter=Filter(must=[])))
    for field in [*_FIELDS, "text"]:
        client.delete_payload_index(_COLLECTION, field_name=field)
