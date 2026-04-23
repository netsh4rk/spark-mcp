# Spark MCP Server

MCP server for accessing Spark Desktop transcripts, emails, and calendar through the Model Context Protocol.

> **Fork notice.** This is a security-hardened fork of [feamster/spark-mcp](https://github.com/feamster/spark-mcp) with **all PDF write-tools removed**. The upstream exposes 25 tools including 11 that fill, sign, annotate and template PDFs on the user's filesystem; those tools were found to have no sandboxing (arbitrary read/write paths) and no defence against confused-deputy / prompt-injection from email content. This fork keeps only the read-only surface for transcripts, emails, calendar and attachment inspection.

## Features

- 📝 Access all meeting transcripts (calendar-based and ad-hoc)
- ✉️ Browse and search emails across every Spark mailbox (SQLite FTS5)
- 🏷️ Enumerate configured accounts and scope queries to a single mailbox
- 🗂️ Filter by Spark smart-folder category (priority / notifications / newsletter / other)
- 📅 Read calendar events
- 📎 Inspect email attachments (with text extraction for PDF / DOCX / XLSX)
- 🔍 Full-text search across transcripts and email bodies
- 🔒 Read-only: the server performs **no** writes to your filesystem or to Spark's databases
- ⚡ Fast local SQLite queries - no network required

## Requirements

- macOS with Spark Desktop installed — both the **Mac App Store** build and the **direct download from readdle.com** are supported; the server auto-detects the sandbox layout.
- Python 3.10+

## Installation

```bash
# Install in development mode
pip install -e .
```

## Usage

### With Claude Desktop

Add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "spark": {
      "command": "python3",
      "args": ["-m", "spark_mcp.server"],
      "cwd": "/absolute/path/to/spark-mcp"
    }
  }
}
```

Or if installed via pip:

```json
{
  "mcpServers": {
    "spark": {
      "command": "spark-mcp"
    }
  }
}
```

Restart Claude Desktop, and the tools will be available.

### Standalone Testing

```bash
python -m spark_mcp.server     # stdio MCP server
pip install -e '.[test]'       # install pytest
pytest                         # run the security regression suite
```

## Available Tools

All tools are read-only.

### Transcripts
- `list_meeting_transcripts` — list recent transcripts (supports `before` / `after` ISO filters)
- `get_meeting_transcript` — fetch full transcript by `messagePk`
- `search_meeting_transcripts` — FTS5 search over transcript text
- `get_transcript_statistics` — totals, date range, top senders

### Accounts
- `list_accounts` — list every Spark account with its primary email address. Returns `accountPk`, `title`, `email`, `type`, `ownerFullName`; never returns passwords, tokens, keychain refs or server config.

### Emails
- `list_emails` — browse `inbox` / `sent` / `drafts` / `all`. Filters: `sender`, `unread_only`, `after` / `before` (ISO datetime), `accountPk` (scope to a single mailbox), `categories` (Spark smart-folder labels, see below). Pair `unread_only=true` + `after=<last-check>` to ingest "new mail since last time" across all configured mailboxes in one call. The return dict carries each email's `category` label so the caller can sort on it client-side.
- `search_emails` — FTS5 search over email bodies, optional `sender`, date range, and `sort_by`
- `get_email` — full email by `messagePk`; return dict includes `accountPk`, `accountTitle`, `accountEmail` so the caller can tell which mailbox the message belongs to
- `find_action_items` — emails containing todos (last N days)
- `find_pending_responses` — emails likely needing a reply (last N days)

### Calendar
- `list_events` — upcoming events (`daysAhead`)
- `get_event_details` — event by `eventPk`
- `find_events_needing_prep` — events in next `hoursAhead` that may need prep

### Combined intelligence
- `get_daily_briefing` — today's summary across transcripts / emails / calendar
- `find_context_for_meeting` — emails related to an event

### Smart-folder categories

Spark classifies every inbox message into one of the labels below (stored as an integer in `messages.category`). Pass any subset to `list_emails(categories=[...])`; unknown labels raise `ValueError`.

| Label | Typical content |
|---|---|
| `priority` | Human-to-human correspondence (what Spark surfaces in "Priority") |
| `notifications` | Service notifications (LinkedIn, Atlassian, Power BI digests, ...) |
| `newsletter` | Marketing / updates with `List-Unsubscribe` headers |
| `other` | Work mail that does not fit the other buckets |
| `uncategorized` | Rare — Spark has not classified the message |

**Caveat**: raw category counts in the DB are ~15% higher than what Spark shows in its sidebar. Spark applies additional UI-side filtering (muted conversations, folder visibility, internal bit flags) that we do not replicate. Use this filter for ingest workflows, not to match the UI counter exactly.

`starred=1` is independent of category — it's the Spark *Contrassegnate* section. It ships in the return dict already.

### Attachments
- `list_attachments` — attachments for a given email
- `get_attachment` — attachment bytes, with optional text extraction for PDF / DOCX / XLSX (delegated to `pypdf`, `python-docx`, `openpyxl`)
- `search_attachments` — find emails by attachment filename / MIME type

## Data Sources

The server probes two possible Spark sandbox layouts and uses the first whose `messages.sqlite` exists:

- **Mac App Store build**
  `~/Library/Containers/com.readdle.SparkDesktop.appstore/Data/Library/Application Support/Spark Desktop/core-data/`
- **Direct download from readdle.com**
  `~/Library/Application Support/Spark Desktop/core-data/`

Under that root it reads:

1. **`messages.sqlite`** — messages, transcripts metadata, accounts, conversations, attachments
2. **`search_fts5.sqlite`** — full-text index for transcripts and email bodies
3. **`calendarsapi.sqlite`** — calendar events

All three are opened in `mode=ro` with `PRAGMA query_only = ON`. Every query is parameterised.

## Security notes

This fork removes the PDF write surface and adds three layers of defence. Know them before wiring this MCP into an agent:

1. **Prompt-injection neutralisation.** Every tool response is routed through `spark_mcp.sanitizer.sanitize_response` before being handed to the client. That layer (a) strips invisible Unicode commonly used for smuggling instructions (zero-width chars, bidi overrides, BOM), (b) neutralises LLM control-token lookalikes (`<|system|>`, `[INST]`, `<function_calls>`, role tags, etc.) by replacing their delimiter characters with visually similar but inert Unicode, and (c) wraps every response with a top-level `_untrusted_content_notice` reminding the model that the payload is data, not instructions. This is defence in depth — it does not make the LLM immune to injection, but it closes the deterministic attack vectors. Covered by `tests/test_sanitizer.py`.
2. **Attachment parsers.** `pypdf`, `python-docx`, `openpyxl` have had parsing CVEs in the past. Dependencies in `pyproject.toml` are **pinned exactly** (`==`) for this reason — a surprise upgrade could introduce a regression in parser safety without anyone noticing. Dependabot (`.github/dependabot.yml`) opens a weekly PR for each update so every version bump is reviewed before it lands. Prefer `get_attachment(extractText=false)` for attachments from unknown senders.
3. **Agent posture.** Even with the sanitizer in place, keep the agent in a review-before-act posture for anything triggered by mail/transcript/calendar content. The sanitizer raises the bar; it does not remove the need for human judgement.

Attachment filenames stored in Spark's DB are sender-controlled; `_get_attachment_path` collapses them to a basename and verifies the resolved path stays inside Spark's cache root before returning. Covered by `tests/test_path_traversal.py`, including a symlink-escape scenario.

The `accounts` table Spark keeps also holds auth config (keychain refs, server endpoints, OAuth tokens). `list_accounts` parses **only** the primary email address out of `additionalInfo.accountAddress` and never returns any of the sensitive fields. Covered by `tests/test_account_scrubbing.py`.

## Troubleshooting

**"Failed to connect to Spark databases"**
Verify Spark Desktop is installed and that `messages.sqlite` exists under one of the two layouts listed in **Data Sources**. Both the App Store and the direct-download flavours are supported.

**No transcripts found**
Check that transcripts are marked "kept" (not deleted). Run `get_transcript_statistics` to confirm counts.

**Empty transcript text**
Recent transcripts may still be syncing. Check `hasFullText` in list results.

## Development

```bash
pip install -e '.[test]'
pytest
```

The suite locks in the three security invariants (path traversal, secret
scrubbing, prompt-injection defence) plus parameter validation. It runs
without a real Spark install: every fixture either bypasses
`SparkDatabase.__init__` or points `SPARK_CACHE` at a temp directory.

## License

MIT (inherited from upstream `feamster/spark-mcp`).
