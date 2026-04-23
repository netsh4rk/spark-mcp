"""Regression tests for ``SparkDatabase._get_attachment_path``.

The attachment filename arrives inside an email's Content-Disposition header
and is therefore attacker-controlled. A naive join with the cache root would
let ``../../etc/passwd`` escape the sandbox. The hardened implementation
collapses the name to ``os.path.basename`` and re-checks that the resolved
path stays inside ``SPARK_CACHE``.

If you break this contract, you reopen CVE-class path traversal.
"""

from pathlib import Path

import pytest

from spark_mcp.database import SparkDatabase


# ---------------------------------------------------------------------------
# Inputs that MUST be rejected outright (return None).
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "bad_name",
    [
        "",                 # empty
        ".",                # current dir
        "..",               # parent dir
        "foo\x00.pdf",      # NUL byte truncation
        "/",                # pure slash -> basename is ""
    ],
    ids=["empty", "dot", "dotdot", "nul_byte", "slash_only"],
)
def test_rejects_obviously_invalid_names(
    db_instance: SparkDatabase,
    fake_cache: Path,
    bad_name: str,
) -> None:
    assert db_instance._get_attachment_path(42, bad_name) is None


# ---------------------------------------------------------------------------
# Inputs that contain traversal tokens but must NOT escape the cache root.
# ``basename()`` collapses them; whatever comes back must stay under
# SPARK_CACHE.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "traversal_name",
    [
        "../../etc/passwd",
        "../../../../../../etc/shadow",
        "subdir/../../../root/.ssh/id_rsa",
        "./../secret",
        "..\\..\\windows\\system32",       # backslashes (no-op on POSIX, but safe)
    ],
)
def test_traversal_attempts_stay_in_cache(
    db_instance: SparkDatabase,
    fake_cache: Path,
    traversal_name: str,
) -> None:
    result = db_instance._get_attachment_path(1, traversal_name)

    # Either the implementation refuses outright (None) or it returns a
    # path that is still under the cache root. Both are acceptable; what
    # is NOT acceptable is returning a path that escapes.
    if result is None:
        return
    resolved = result.resolve(strict=False)
    assert resolved.is_relative_to(fake_cache), (
        f"Path traversal escape: {traversal_name!r} -> {resolved}"
    )


# ---------------------------------------------------------------------------
# Happy path: legitimate filename lands in the expected cache subtree.
# ---------------------------------------------------------------------------

def test_legitimate_filename_lands_in_cache(
    db_instance: SparkDatabase,
    fake_cache: Path,
) -> None:
    result = db_instance._get_attachment_path(42, "invoice.pdf")
    assert result is not None
    assert result.is_relative_to(fake_cache)
    assert result.name == "invoice.pdf"


def test_filename_with_safe_special_chars(
    db_instance: SparkDatabase,
    fake_cache: Path,
) -> None:
    # Spaces, unicode and punctuation that are legitimate in real email
    # attachments must pass through.
    name = "Offerta — Q2 2026 (finale).pdf"
    result = db_instance._get_attachment_path(7, name)
    assert result is not None
    assert result.is_relative_to(fake_cache)
    assert result.name == name


# ---------------------------------------------------------------------------
# Symlink defence: even if an attacker-controlled symlink inside the cache
# points outside it, the resolve() + is_relative_to() check must catch it.
# ---------------------------------------------------------------------------

def test_symlink_escape_is_rejected(
    db_instance: SparkDatabase,
    fake_cache: Path,
    tmp_path: Path,
) -> None:
    # Stage: an existing real file outside the cache.
    outside = tmp_path / "outside_secret.txt"
    outside.write_text("top secret")

    # Build the path Spark would compute for a bogus message 99.
    message_dir = fake_cache / "messagesData" / "1" / "99"
    message_dir.mkdir(parents=True)
    malicious_link = message_dir / "link.pdf"
    malicious_link.symlink_to(outside)

    result = db_instance._get_attachment_path(99, "link.pdf")
    # The symlink must not be followed outside the cache root.
    if result is not None:
        resolved = result.resolve(strict=False)
        assert resolved.is_relative_to(fake_cache.resolve()), (
            f"Symlink escape: {resolved} is outside {fake_cache}"
        )
