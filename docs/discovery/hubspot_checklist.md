# HubSpot Authenticated Discovery Checklist

Goal: determine what HubSpot data should feed the lead follow-up MCP database.

## Inspect

- Lead Pipeline dashboard and reports.
- Current Deal Stages report drilldowns.
- Deal detail pages for active, scheduled trial/tour, enrolled, and closed-lost leads.
- Contact detail pages linked to those deals.
- Associated contacts and labels: student, parent/guardian, child/dependent.
- Tasks dashboard and task detail/exports.
- Activity timeline: calls, notes, emails, meetings, Bridge-created events, status changes.
- Report export and unsummarized data export options.

## Capture

- Screenshot or export evidence for each inspected area.
- Exact field labels and internal names if visible.
- Exported CSV headers if available.
- Whether the report can filter from `2025-01-01`.
- Whether updates can be filtered by create date, modified date, task due date, or activity date.

## Decide

- Primary object for leads: expected default is Deals in `Lead Pipeline`.
- Required v1 fields for follow-up queue.
- Whether HubSpot can be the source of truth for tasks and stage history.
- Whether Dialpad activities are synced into HubSpot or must come from Dialpad.
- Whether Bridge stores Pike13 identifiers in HubSpot.
