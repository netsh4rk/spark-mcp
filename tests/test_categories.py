"""Regression tests for ``list_emails(categories=...)`` input validation.

Callers can pass an arbitrary list of category labels. Unknown labels must
raise ``ValueError`` deterministically — a silent empty result would hide
typos and make the LLM think the mailbox has no matching emails.

The validation happens before the DB connection is opened, so these tests
do not need a real Spark install.
"""

import pytest

from spark_mcp.database import SparkDatabase


VALID_LABELS = ["priority", "notifications", "newsletter", "other", "uncategorized"]


@pytest.mark.parametrize("bad_label", [
    "prioritary",         # typo
    "PRIORITY",           # wrong case
    "priority ",          # trailing whitespace
    "",                   # empty
    "spam",               # plausible but not defined
    "inbox",              # folder name, not category
])
def test_unknown_label_raises(db_instance: SparkDatabase, bad_label: str) -> None:
    with pytest.raises(ValueError) as exc_info:
        db_instance.list_emails(categories=[bad_label])
    # The error message must contain the full set of valid labels so the
    # LLM can self-correct on the retry.
    msg = str(exc_info.value)
    for label in VALID_LABELS:
        assert label in msg


def test_one_bad_label_in_valid_list_raises(db_instance: SparkDatabase) -> None:
    """A typo among otherwise-valid labels must not pass silently."""
    with pytest.raises(ValueError):
        db_instance.list_emails(categories=["priority", "nope", "newsletter"])


def test_empty_categories_is_accepted(
    db_instance: SparkDatabase,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``categories=None`` and ``categories=[]`` should NOT raise — they mean
    'no filter'. We short-circuit before the DB call by raising from a stub
    ``_connect_messages`` so the test stays DB-free."""
    sentinel = RuntimeError("reached DB, as expected")

    def fake_connect(self):
        raise sentinel

    monkeypatch.setattr(SparkDatabase, "_connect_messages", fake_connect)

    # None
    with pytest.raises(RuntimeError, match="reached DB"):
        db_instance.list_emails(categories=None)
    # Empty list
    with pytest.raises(RuntimeError, match="reached DB"):
        db_instance.list_emails(categories=[])
