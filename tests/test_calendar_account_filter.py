"""Tests for cross-account calendar filtering on list_events,
get_event_details, and find_events_needing_prep.

These tests build a temp SQLite calendar database with the minimal subset of
the Spark schema needed by the methods under test (Event, Collection,
Account, Attendee). They verify that:

1. Each event is enriched with accountEmail + calendarName.
2. The account_email / calendar_name filters scope the result correctly.
3. Events whose collection is missing (orphan) still surface, with
   accountEmail / calendarName == None — LEFT JOIN preserves them.
"""

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from spark_mcp.database import SparkDatabase


def _build_calendar_db(path: Path) -> None:
    """Create the minimum schema and seed three accounts × calendars × events."""
    conn = sqlite3.connect(str(path))
    conn.executescript(
        """
        CREATE TABLE RDCALAPIAccount (
            pk INTEGER PRIMARY KEY,
            kind INTEGER NOT NULL,
            identifier TEXT NOT NULL,
            title TEXT
        );
        CREATE TABLE RDCALAPICollection (
            pk INTEGER PRIMARY KEY,
            refAccountPK INTEGER,
            displayname TEXT
        );
        CREATE TABLE RDCALAPIEvent (
            pk INTEGER PRIMARY KEY,
            refCollectionPK INTEGER,
            summary TEXT,
            descriptionProperty TEXT,
            dstart INTEGER,
            dend INTEGER,
            location TEXT,
            locationTitle TEXT,
            allDay INTEGER DEFAULT 0,
            status INTEGER DEFAULT 0,
            conferenceInfo TEXT,
            url TEXT
        );
        CREATE TABLE RDCALAPIAttendee (
            pk INTEGER PRIMARY KEY,
            refEventPK INTEGER,
            name TEXT,
            email TEXT,
            partStat INTEGER,
            role INTEGER
        );
        CREATE TABLE RDCALAPIOrganizer (
            pk INTEGER PRIMARY KEY,
            refEventPK INTEGER,
            name TEXT,
            email TEXT
        );
        """
    )

    # Two accounts, each with two calendars
    conn.executemany(
        "INSERT INTO RDCALAPIAccount(pk, kind, identifier, title) VALUES (?,?,?,?)",
        [
            (1, 0, "work@example.com", "Work"),
            (2, 0, "personal@example.com", "Personal"),
        ],
    )
    conn.executemany(
        "INSERT INTO RDCALAPICollection(pk, refAccountPK, displayname) VALUES (?,?,?)",
        [
            (10, 1, "Work Main"),
            (11, 1, "Work PMO"),
            (20, 2, "Personal Main"),
            (21, 2, "Holidays"),
        ],
    )

    # Use a fixed reference time well in the future so find_events_needing_prep
    # (which filters dstart >= now()) sees the seeded rows regardless of when
    # the test runs. Each event is at most a few hours apart.
    base = int((datetime.now() + timedelta(hours=1)).timestamp())

    events = [
        # (pk, refCollectionPK, summary, dstart_offset_sec, duration_sec, conferenceInfo)
        (100, 10, "Work meeting", 0, 3600, "https://meet.example/work"),
        (101, 11, "PMO sync", 1800, 1800, None),
        (102, 20, "Dentist", 7200, 1800, None),
        (103, 21, "Labor Day", 10800, 86400, None),
        # Orphan: refCollectionPK points nowhere (collection deleted)
        (104, 999, "Orphan event", 14400, 1800, None),
    ]
    for pk, coll, summary, off, dur, conf in events:
        conn.execute(
            """
            INSERT INTO RDCALAPIEvent(
                pk, refCollectionPK, summary, descriptionProperty,
                dstart, dend, location, locationTitle, allDay, status,
                conferenceInfo, url
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (pk, coll, summary, "", base + off, base + off + dur,
             "", "", 0, 0, conf, ""),
        )

    # Two attendees on the work meeting so it qualifies as "needs prep"
    conn.executemany(
        "INSERT INTO RDCALAPIAttendee(refEventPK, name, email, partStat, role) "
        "VALUES (?,?,?,?,?)",
        [
            (100, "Me", "me@example.com", 1, 1),
            (100, "Boss", "boss@example.com", 1, 1),
        ],
    )

    conn.commit()
    conn.close()


@pytest.fixture
def calendar_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> SparkDatabase:
    """SparkDatabase whose _connect_calendar returns a temp DB seeded with
    multi-account fixtures. Other connections are not patched and would error
    if accidentally exercised — that surfaces unintended cross-DB calls."""
    db_path = tmp_path / "calendarsapi.sqlite"
    _build_calendar_db(db_path)

    db = object.__new__(SparkDatabase)

    def _connect_calendar(self=db):
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        return conn

    monkeypatch.setattr(db, "_connect_calendar", _connect_calendar.__get__(db))
    return db


# ---------------------------------------------------------------- list_events

def test_list_events_enriches_with_account_and_calendar(calendar_db):
    result = calendar_db.list_events(days_ahead=2, limit=50)

    by_pk = {e["eventPk"]: e for e in result["events"]}
    assert by_pk[100]["accountEmail"] == "work@example.com"
    assert by_pk[100]["calendarName"] == "Work Main"
    assert by_pk[102]["accountEmail"] == "personal@example.com"
    assert by_pk[102]["calendarName"] == "Personal Main"


def test_list_events_orphan_event_keeps_null_metadata(calendar_db):
    """LEFT JOIN must keep events whose collection was deleted."""
    result = calendar_db.list_events(days_ahead=2, limit=50)
    by_pk = {e["eventPk"]: e for e in result["events"]}
    assert 104 in by_pk
    assert by_pk[104]["accountEmail"] is None
    assert by_pk[104]["calendarName"] is None


def test_list_events_filter_by_account(calendar_db):
    result = calendar_db.list_events(
        days_ahead=2, limit=50, account_email="work@example.com"
    )
    pks = {e["eventPk"] for e in result["events"]}
    assert pks == {100, 101}
    for ev in result["events"]:
        assert ev["accountEmail"] == "work@example.com"


def test_list_events_filter_by_calendar(calendar_db):
    result = calendar_db.list_events(
        days_ahead=2, limit=50, calendar_name="Holidays"
    )
    assert [e["eventPk"] for e in result["events"]] == [103]


def test_list_events_filter_account_and_calendar(calendar_db):
    result = calendar_db.list_events(
        days_ahead=2,
        limit=50,
        account_email="work@example.com",
        calendar_name="Work PMO",
    )
    assert [e["eventPk"] for e in result["events"]] == [101]


def test_list_events_filter_no_match_returns_empty(calendar_db):
    result = calendar_db.list_events(
        days_ahead=2, limit=50, account_email="nonexistent@example.com"
    )
    assert result == {"events": [], "total": 0}


# ---------------------------------------------------------- get_event_details

def test_get_event_details_includes_account_and_calendar(calendar_db):
    detail = calendar_db.get_event_details(100)
    assert detail is not None
    assert detail["accountEmail"] == "work@example.com"
    assert detail["calendarName"] == "Work Main"
    assert {a["email"] for a in detail["attendees"]} == {
        "me@example.com",
        "boss@example.com",
    }


def test_get_event_details_orphan_event_null_metadata(calendar_db):
    detail = calendar_db.get_event_details(104)
    assert detail is not None
    assert detail["accountEmail"] is None
    assert detail["calendarName"] is None


# -------------------------------------------------- find_events_needing_prep

def test_find_events_needing_prep_includes_metadata(calendar_db):
    result = calendar_db.find_events_needing_prep(hours_ahead=48, limit=50)
    pks = {e["eventPk"] for e in result["events"]}
    # Event 100 qualifies (multiple attendees + conference link)
    assert 100 in pks
    work_evt = next(e for e in result["events"] if e["eventPk"] == 100)
    assert work_evt["accountEmail"] == "work@example.com"
    assert work_evt["calendarName"] == "Work Main"


def test_find_events_needing_prep_filter_by_account(calendar_db):
    result = calendar_db.find_events_needing_prep(
        hours_ahead=48, limit=50, account_email="personal@example.com"
    )
    for ev in result["events"]:
        assert ev["accountEmail"] == "personal@example.com"
