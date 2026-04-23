# Spark MCP Server

MCP server for accessing Spark Desktop transcripts, emails, and calendar through the Model Context Protocol.

> **Fork notice.** This is a security-hardened fork of [feamster/spark-mcp](https://github.com/feamster/spark-mcp) with **all PDF write-tools removed**. The upstream exposes 25 tools including 11 that fill, sign, annotate and template PDFs on the user's filesystem; those tools were found to have no sandboxing (arbitrary read/write paths) and no defence against confused-deputy / prompt-injection from email content. This fork keeps only the read-only surface for transcripts, emails, calendar and attachment inspection.

## Features

- 📝 Access all meeting transcripts (calendar-based and ad-hoc)
- ✉️ Browse and search emails (SQLite FTS5)
- 📅 Read calendar events
- 📎 Inspect email attachments (with text extraction for PDF / DOCX / XLSX)
- 🔍 Full-text search across transcripts and email bodies
- 🔒 Read-only: the server performs **no** writes to your filesystem or to Spark's databases
- ⚡ Fast local SQLite queries - no network required

## Requirements

- macOS (Spark Desktop App Store version)
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
python test_server.py          # smoke-test DB connection
python verify_all_tools.py     # exercise each DB helper
```

## Available Tools

All tools are read-only.

### Transcripts
- `list_meeting_transcripts` — list recent transcripts (supports `before` / `after` ISO filters)
- `get_meeting_transcript` — fetch full transcript by `messagePk`
- `search_meeting_transcripts` — FTS5 search over transcript text
- `get_transcript_statistics` — totals, date range, top senders

### Emails
- `list_emails` — browse `inbox` / `sent` / `all` with optional `sender` filter
- `search_emails` — FTS5 search over email bodies, optional `sender`, date range, and `sort_by`
- `get_email` — full email by `messagePk`
- `find_action_items` — emails containing todos (last N days)
- `find_pending_responses` — emails likely needing a reply (last N days)

### Calendar
- `list_events` — upcoming events (`daysAhead`)
- `get_event_details` — event by `eventPk`
- `find_events_needing_prep` — events in next `hoursAhead` that may need prep

### Combined intelligence
- `get_daily_briefing` — today's summary across transcripts / emails / calendar
- `find_context_for_meeting` — emails related to an event

### Attachments
- `list_attachments` — attachments for a given email
- `get_attachment` — attachment bytes, with optional text extraction for PDF / DOCX / XLSX (delegated to `pypdf`, `python-docx`, `openpyxl`)
- `search_attachments` — find emails by attachment filename / MIME type

## Data Sources

1. **`messages.sqlite`** — message / transcript metadata
   `~/Library/Containers/com.readdle.SparkDesktop.appstore/Data/Library/Application Support/Spark Desktop/core-data/messages.sqlite`
2. **`search_fts5.sqlite`** — full-text index for transcripts and email bodies
   `~/Library/Containers/com.readdle.SparkDesktop.appstore/Data/Library/Application Support/Spark Desktop/core-data/search_fts5.sqlite`

Both are opened in `mode=ro` with `PRAGMA query_only = ON`. All queries are parameterised.

## Security notes

This fork removes the PDF write surface, but two residual risks remain; know them before wiring this MCP into an agent:

1. **Untrusted content in LLM context.** Emails, transcripts, subject lines and attachment filenames are attacker-controlled (anyone can send you an email). Do not auto-execute instructions that appear inside email bodies or attachment text. Keep the agent in a review-before-act posture for anything triggered by this data.
2. **Attachment parsers.** `pypdf`, `python-docx`, `openpyxl` have had parsing CVEs in the past. Pin versions at install time and keep them updated. Prefer `get_attachment(extractText=false)` for attachments from unknown senders.

Path-traversal hardening of `_get_attachment_path` in `database.py` is planned as a follow-up patch.

## Troubleshooting

**"Failed to connect to Spark databases"**
Verify Spark Desktop (App Store build) is installed and the paths above exist.

**No transcripts found**
Check that transcripts are marked "kept" (not deleted). Run `get_transcript_statistics` to confirm counts.

**Empty transcript text**
Recent transcripts may still be syncing. Check `hasFullText` in list results.

## Development

```bash
pip install -e .
pytest   # if/when tests land
```

See `PLAN.md` for the original upstream design notes.

## License

MIT (inherited from upstream `feamster/spark-mcp`).
