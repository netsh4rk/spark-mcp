#!/usr/bin/env python3
"""Spark MCP Server - Access Spark Desktop transcripts, emails, and calendar."""

import asyncio
import json
from typing import Any, Sequence
from mcp.server import Server
from mcp.types import Tool, TextContent
from mcp.server.stdio import stdio_server
from .database import SparkDatabase
from .sanitizer import sanitize_response


def _emit(result: Any) -> list[TextContent]:
    """Render a tool result as a single TextContent after running it
    through the prompt-injection sanitizer.

    Every tool in this server reads content authored by external parties
    (email senders, calendar invitees, attachment text). Routing every
    response through ``sanitize_response`` ensures no tool can accidentally
    bypass the injection defence — even if a new tool is added later and
    the author forgets to sanitize.
    """
    envelope = sanitize_response(result)
    return [TextContent(type="text", text=json.dumps(envelope, indent=2))]


def _emit_error(message: str) -> list[TextContent]:
    """Plain, non-sanitised error channel. Error strings are written by
    this server (not by an external party) so they don't need the
    untrusted-content envelope, and keeping them outside the envelope
    makes them obvious at a glance to both the model and a human
    reviewer reading logs.
    """
    return [TextContent(type="text", text=message)]


# Initialize database (errors will be logged by MCP framework)
db = SparkDatabase()


# Create server instance
server = Server("spark-mcp-server")


# Define tools - optimized with minimal descriptions and small limits
TOOLS: list[Tool] = [
    # TRANSCRIPT TOOLS
    Tool(
        name="list_meeting_transcripts",
        description="List recent meeting transcripts",
        inputSchema={
            "type": "object",
            "properties": {
                "limit": {"type": "number", "description": "Max results", "default": 20},
                "before": {"type": "string", "description": "Return transcripts with meetingStartDate before this ISO datetime (e.g., '2026-01-30T16:00:00')"},
                "after": {"type": "string", "description": "Return transcripts with meetingStartDate after this ISO datetime (e.g., '2026-01-30T13:00:00')"}
            }
        }
    ),
    Tool(
        name="get_meeting_transcript",
        description="Get full transcript by ID",
        inputSchema={
            "type": "object",
            "properties": {
                "messagePk": {"type": "number", "description": "Message ID"}
            },
            "required": ["messagePk"]
        }
    ),
    Tool(
        name="search_meeting_transcripts",
        description="Search transcript content",
        inputSchema={
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search terms"},
                "limit": {"type": "number", "description": "Max results", "default": 10}
            },
            "required": ["query"]
        }
    ),
    Tool(
        name="get_transcript_statistics",
        description="Get transcript stats",
        inputSchema={"type": "object", "properties": {}}
    ),

    # ACCOUNT TOOLS
    Tool(
        name="list_accounts",
        description="""List every email account configured in Spark with its primary address.

Use this first when the user wants to ingest / summarise / filter emails by
mailbox. Returned fields are safe: no passwords, tokens, or server config —
only accountPk, title, email, type, ownerFullName. Pair the accountPk with
list_emails(accountPk=...) to scope downstream queries to a single mailbox.""",
        inputSchema={"type": "object", "properties": {}}
    ),

    # EMAIL TOOLS
    Tool(
        name="list_emails",
        description="""List recent emails from a folder. PREFERRED for finding emails by sender or recent activity.

WHEN TO USE THIS vs search_emails:
- Use list_emails when looking for emails FROM a specific person (use sender filter)
- Use list_emails when looking for recent correspondence
- Use list_emails first to browse recent activity, then search_emails for specific content
- Use unread_only=true + after=<ISO> to pull "new mail since <time>" across mailboxes.
- Use accountPk (from list_accounts) to restrict to a single mailbox.

This tool is more reliable than search_emails for finding threads by correspondent.""",
        inputSchema={
            "type": "object",
            "properties": {
                "folder": {"type": "string", "description": "inbox/sent/drafts/all", "default": "inbox"},
                "sender": {"type": "string", "description": "Filter by sender email or name (partial match)"},
                "unread_only": {"type": "boolean", "description": "Only emails marked unread in Spark", "default": False},
                "after": {"type": "string", "description": "ISO datetime (e.g. '2026-04-23T08:00:00'). Only emails received at/after this instant."},
                "before": {"type": "string", "description": "ISO datetime. Only emails received at/before this instant."},
                "accountPk": {"type": "number", "description": "Spark accountPk to restrict to a single mailbox (see list_accounts)"},
                "categories": {
                    "type": "array",
                    "description": "Restrict to Spark smart-folder categories. E.g. ['priority','other'] to ingest human mail while skipping newsletters and service notifications. Omit for all categories. Derived from Spark's internal classifier — counts can differ ~15% from the Spark sidebar because Spark applies extra UI-side filtering we don't replicate.",
                    "items": {
                        "type": "string",
                        "enum": ["priority", "notifications", "newsletter", "other", "uncategorized"]
                    }
                },
                "limit": {"type": "number", "description": "Max results", "default": 20}
            }
        }
    ),
    Tool(
        name="search_emails",
        description="""Search email body content using SQLite FTS5 full-text search.

IMPORTANT - FTS5 BEHAVIOR:
- Multiple words are AND-ed: "Bittner NetApp" only matches if BOTH words appear in the email body
- This often FAILS for finding threads because names may be in headers/signatures, not body text
- If a multi-word search returns nothing, TRY EACH WORD SEPARATELY

SEARCH STRATEGY (do this in order):
1. First try list_emails with sender filter to find emails from a person
2. If searching for a topic/project, use a SINGLE distinctive keyword, not multiple words
3. If first search fails, try alternative terms (company name, project name, invoice number separately)
4. For phrases, use quotes: "exact phrase here"

EXAMPLES:
- Looking for "Bittner about NetApp"? Use sender="bittner" + query="NetApp" + sort_by="date"
- Looking for invoice #INV-123? Search for "INV-123" alone
- Recent emails about a topic? Use query + sort_by="date" to get newest first""",
        inputSchema={
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search terms. Use single keywords for best results. Multiple words are AND-ed together."},
                "sender": {"type": "string", "description": "Filter by sender email/name (partial match). Combine with query for 'emails from X about Y'."},
                "start_date": {"type": "string", "description": "ISO date (YYYY-MM-DD). Only emails after this date."},
                "end_date": {"type": "string", "description": "ISO date (YYYY-MM-DD). Only emails before this date."},
                "sort_by": {"type": "string", "description": "relevance (default) or date (newest first)", "default": "relevance"},
                "limit": {"type": "number", "description": "Max results", "default": 10}
            },
            "required": ["query"]
        }
    ),
    Tool(
        name="get_email",
        description="Get full email by ID",
        inputSchema={
            "type": "object",
            "properties": {
                "messagePk": {"type": "number", "description": "Message ID"}
            },
            "required": ["messagePk"]
        }
    ),
    Tool(
        name="find_action_items",
        description="Find emails with todos",
        inputSchema={
            "type": "object",
            "properties": {
                "days": {"type": "number", "description": "Days back", "default": 7},
                "limit": {"type": "number", "description": "Max results", "default": 20}
            }
        }
    ),
    Tool(
        name="find_pending_responses",
        description="Find emails needing replies",
        inputSchema={
            "type": "object",
            "properties": {
                "days": {"type": "number", "description": "Days back", "default": 7},
                "limit": {"type": "number", "description": "Max results", "default": 20}
            }
        }
    ),

    # CALENDAR TOOLS
    Tool(
        name="list_events",
        description=(
            "List calendar events across all configured Spark calendars. "
            "Each event is annotated with accountEmail (the owning Spark "
            "calendar account) and calendarName (the specific calendar "
            "within that account). Use accountEmail / calendarName to scope "
            "to a single source — useful when the user asks about a specific "
            "mailbox (e.g. 'meetings on my work calendar')."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "daysAhead": {"type": "number", "description": "Days ahead", "default": 1},
                "limit": {"type": "number", "description": "Max results", "default": 20},
                "accountEmail": {
                    "type": "string",
                    "description": (
                        "Restrict to events from this Spark calendar account "
                        "(matches RDCALAPIAccount.identifier — typically the "
                        "account email)."
                    ),
                },
                "calendarName": {
                    "type": "string",
                    "description": (
                        "Restrict to a single calendar within the chosen "
                        "account (e.g. 'PMO', 'Holidays in Italy')."
                    ),
                },
            }
        }
    ),
    Tool(
        name="get_event_details",
        description="Get event details by ID",
        inputSchema={
            "type": "object",
            "properties": {
                "eventPk": {"type": "number", "description": "Event ID"}
            },
            "required": ["eventPk"]
        }
    ),
    Tool(
        name="find_events_needing_prep",
        description=(
            "Find upcoming events likely needing preparation (external "
            "attendees, conference link, duration > 30min). Optionally "
            "scoped to a single calendar account or named calendar."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "hoursAhead": {"type": "number", "description": "Hours ahead", "default": 24},
                "limit": {"type": "number", "description": "Max results", "default": 20},
                "accountEmail": {
                    "type": "string",
                    "description": "Restrict to events from this Spark calendar account.",
                },
                "calendarName": {
                    "type": "string",
                    "description": "Restrict to a single calendar within the chosen account.",
                },
            }
        }
    ),

    # COMBINED INTELLIGENCE
    Tool(
        name="get_daily_briefing",
        description="Get today's briefing",
        inputSchema={"type": "object", "properties": {}}
    ),
    Tool(
        name="find_context_for_meeting",
        description="Find emails for meeting",
        inputSchema={
            "type": "object",
            "properties": {
                "eventPk": {"type": "number", "description": "Event ID"},
                "daysBack": {"type": "number", "description": "Days back", "default": 30}
            },
            "required": ["eventPk"]
        }
    ),

    # ATTACHMENT TOOLS
    Tool(
        name="list_attachments",
        description="List attachments for an email",
        inputSchema={
            "type": "object",
            "properties": {
                "messagePk": {"type": "number", "description": "Message ID"}
            },
            "required": ["messagePk"]
        }
    ),
    Tool(
        name="get_attachment",
        description="Get attachment content with text extraction for PDFs/docs",
        inputSchema={
            "type": "object",
            "properties": {
                "messagePk": {"type": "number", "description": "Message ID"},
                "attachmentIndex": {"type": "number", "description": "Attachment index (0-based)", "default": 0},
                "extractText": {"type": "boolean", "description": "Extract text from PDFs/docs", "default": True}
            },
            "required": ["messagePk"]
        }
    ),
    Tool(
        name="search_attachments",
        description="Search for emails with attachments",
        inputSchema={
            "type": "object",
            "properties": {
                "filename": {"type": "string", "description": "Filename pattern (use * as wildcard)"},
                "mimeType": {"type": "string", "description": "MIME type filter (e.g., application/pdf)"},
                "limit": {"type": "number", "description": "Max results", "default": 20}
            }
        }
    ),
]


@server.list_tools()
async def list_tools() -> list[Tool]:
    """List available tools."""
    return TOOLS


@server.call_tool()
async def call_tool(name: str, arguments: Any) -> Sequence[TextContent]:
    """Handle tool calls."""
    try:
        # TRANSCRIPT TOOLS
        if name == "list_meeting_transcripts":
            result = db.list_transcripts(
                limit=int(arguments.get("limit", 20)),
                start_date=arguments.get("after"),  # after maps to start_date
                end_date=arguments.get("before"),   # before maps to end_date
                only_kept=True
            )
            return _emit(result)

        elif name == "get_meeting_transcript":
            message_pk = arguments.get("messagePk")
            if not message_pk:
                return _emit_error("Error: messagePk required")
            result = db.get_transcript(message_pk=int(message_pk))
            if result is None:
                return _emit_error("Transcript not found")
            return _emit(result)

        elif name == "search_meeting_transcripts":
            query = arguments.get("query")
            if not query:
                return _emit_error("Error: query required")
            result = db.search_transcripts(
                query=query,
                limit=int(arguments.get("limit", 10))
            )
            return _emit(result)

        elif name == "get_transcript_statistics":
            result = db.get_statistics()
            return _emit(result)

        # ACCOUNT TOOLS
        elif name == "list_accounts":
            result = db.list_accounts()
            return _emit(result)

        # EMAIL TOOLS
        elif name == "list_emails":
            account_pk = arguments.get("accountPk")
            categories = arguments.get("categories")
            result = db.list_emails(
                folder=arguments.get("folder", "inbox"),
                sender=arguments.get("sender"),
                unread_only=bool(arguments.get("unread_only", False)),
                start_date=arguments.get("after"),
                end_date=arguments.get("before"),
                account_pk=int(account_pk) if account_pk is not None else None,
                categories=list(categories) if categories else None,
                limit=int(arguments.get("limit", 20))
            )
            return _emit(result)

        elif name == "search_emails":
            query = arguments.get("query")
            if not query:
                return _emit_error("Error: query required")
            result = db.search_emails(
                query=query,
                sender=arguments.get("sender"),
                start_date=arguments.get("start_date"),
                end_date=arguments.get("end_date"),
                sort_by=arguments.get("sort_by", "relevance"),
                limit=int(arguments.get("limit", 10))
            )
            return _emit(result)

        elif name == "get_email":
            message_pk = arguments.get("messagePk")
            if not message_pk:
                return _emit_error("Error: messagePk required")
            result = db.get_email(int(message_pk))
            if result is None:
                return _emit_error("Email not found")
            return _emit(result)

        elif name == "find_action_items":
            result = db.find_action_items(
                days=int(arguments.get("days", 7)),
                limit=int(arguments.get("limit", 20))
            )
            return _emit(result)

        elif name == "find_pending_responses":
            result = db.find_pending_responses(
                days=int(arguments.get("days", 7)),
                limit=int(arguments.get("limit", 20))
            )
            return _emit(result)

        # CALENDAR TOOLS
        elif name == "list_events":
            account_email = arguments.get("accountEmail")
            calendar_name = arguments.get("calendarName")
            result = db.list_events(
                days_ahead=int(arguments.get("daysAhead", 1)),
                limit=int(arguments.get("limit", 20)),
                account_email=account_email if account_email else None,
                calendar_name=calendar_name if calendar_name else None,
            )
            return _emit(result)

        elif name == "get_event_details":
            event_pk = arguments.get("eventPk")
            if not event_pk:
                return _emit_error("Error: eventPk required")
            result = db.get_event_details(int(event_pk))
            if result is None:
                return _emit_error("Event not found")
            return _emit(result)

        elif name == "find_events_needing_prep":
            account_email = arguments.get("accountEmail")
            calendar_name = arguments.get("calendarName")
            result = db.find_events_needing_prep(
                hours_ahead=int(arguments.get("hoursAhead", 24)),
                limit=int(arguments.get("limit", 20)),
                account_email=account_email if account_email else None,
                calendar_name=calendar_name if calendar_name else None,
            )
            return _emit(result)

        # COMBINED INTELLIGENCE
        elif name == "get_daily_briefing":
            result = db.get_daily_briefing()
            return _emit(result)

        elif name == "find_context_for_meeting":
            event_pk = arguments.get("eventPk")
            if not event_pk:
                return _emit_error("Error: eventPk required")
            result = db.find_context_for_meeting(
                event_pk=int(event_pk),
                days_back=int(arguments.get("daysBack", 30))
            )
            return _emit(result)

        # ATTACHMENT TOOLS
        elif name == "list_attachments":
            message_pk = arguments.get("messagePk")
            if not message_pk:
                return _emit_error("Error: messagePk required")
            result = db.list_attachments(int(message_pk))
            return _emit(result)

        elif name == "get_attachment":
            message_pk = arguments.get("messagePk")
            if not message_pk:
                return _emit_error("Error: messagePk required")
            result = db.get_attachment(
                message_pk=int(message_pk),
                attachment_index=int(arguments.get("attachmentIndex", 0)),
                extract_text=arguments.get("extractText", True)
            )
            if result is None:
                return _emit_error("Attachment not found")
            return _emit(result)

        elif name == "search_attachments":
            result = db.search_attachments(
                filename=arguments.get("filename"),
                mime_type=arguments.get("mimeType"),
                limit=int(arguments.get("limit", 20))
            )
            return _emit(result)

        else:
            return _emit_error(f"Unknown tool: {name}")

    except Exception as e:
        return _emit_error(f"Error: {str(e)}")


async def main():
    """Run the MCP server."""
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options()
        )


def cli() -> None:
    """Sync entry point so the `spark-mcp` console script actually runs."""
    asyncio.run(main())


if __name__ == "__main__":
    cli()
