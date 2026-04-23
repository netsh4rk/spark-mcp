"""Regression tests for ``SparkDatabase._extract_account_email``.

Spark stores mailbox configuration in an ``additionalInfo`` JSON blob that
contains keychain references, OAuth tokens, SMTP/IMAP endpoints, and other
credentials. The MCP tool surface must only expose the primary email
address — never the surrounding secrets.

If you break this contract, you risk leaking credentials via any tool that
returns account metadata (``list_accounts``, ``list_emails``, ``get_email``).
"""

import json

import pytest

from spark_mcp.database import SparkDatabase


# ---------------------------------------------------------------------------
# Happy path: only accountAddress is extracted.
# ---------------------------------------------------------------------------

def test_extracts_only_primary_address() -> None:
    blob = json.dumps({
        "accountAddress": "marco@example.com",
        "imapPassword": "hunter2",
        "oauthToken": "ya29.supersecret",
        "smtpServer": "smtp.example.com",
        "imapPort": 993,
        "keychainRef": "com.readdle.spark.marco",
    })
    assert SparkDatabase._extract_account_email(blob) == "marco@example.com"


# ---------------------------------------------------------------------------
# Sensitive values never reach the output.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "secret_key,secret_value",
    [
        ("imapPassword", "hunter2"),
        ("oauthToken", "ya29.supersecret"),
        ("refreshToken", "1//0gABCdef"),
        ("keychainRef", "com.readdle.spark.marco"),
        ("smtpPassword", "p4ssw0rd!"),
        ("clientSecret", "GOCSPX-xxxxxxxx"),
    ],
)
def test_secrets_are_not_leaked(secret_key: str, secret_value: str) -> None:
    blob = json.dumps({
        "accountAddress": "work@example.com",
        secret_key: secret_value,
    })
    result = SparkDatabase._extract_account_email(blob)
    assert result == "work@example.com"
    # Defence in depth: even the string representation of the result
    # should not contain any byte of the secret.
    assert secret_value not in str(result)


# ---------------------------------------------------------------------------
# Malformed / missing input is tolerated without raising.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "bad_input",
    [
        None,
        "",
        "not-json",
        "{not valid json",
        json.dumps({"accountAddress": ""}),     # empty string
        json.dumps({"accountAddress": "   "}),  # whitespace only
        json.dumps({"accountAddress": 42}),     # wrong type
        json.dumps({"other": "marco@example.com"}),  # wrong key
        json.dumps([]),                          # array, not object
        json.dumps("marco@example.com"),         # string, not object
    ],
)
def test_malformed_input_returns_none(bad_input) -> None:
    assert SparkDatabase._extract_account_email(bad_input) is None


def test_whitespace_is_trimmed() -> None:
    blob = json.dumps({"accountAddress": "  marco@example.com  "})
    assert SparkDatabase._extract_account_email(blob) == "marco@example.com"


# ---------------------------------------------------------------------------
# Does not crawl nested structures looking for extra fields.
# ---------------------------------------------------------------------------

def test_does_not_traverse_nested_objects() -> None:
    """If an accountAddress-shaped value is nested, we do NOT pick it up
    accidentally — we only read the top-level key. This keeps the
    extraction predictable and avoids false positives from arbitrary
    parser behaviour."""
    blob = json.dumps({
        "someOtherKey": {"accountAddress": "nested@example.com"},
    })
    assert SparkDatabase._extract_account_email(blob) is None
