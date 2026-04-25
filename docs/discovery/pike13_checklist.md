# Pike13 Authenticated Discovery Checklist

Goal: determine what Pike13 can contribute to enrollment outcomes and funnel measurement.

## Inspect

- Client export configuration and available columns.
- Client profile pages for active, trial, inactive, and lost leads if visible.
- Trial lesson/event detail pages.
- Visit history and attendance history.
- Plans/passes/memberships, holds, cancellations, end dates.
- Any leads/inquiries/tasks/follow-up areas.
- Report/export filters by date or updated date.

## Capture

- Screenshot or export evidence for each inspected area.
- Exported CSV headers for client, visit, trial, and plan/pass data where available.
- Stable identifiers: Client ID, Customer ID, household/account IDs.
- Trial lifecycle fields: booked, completed, canceled, no-show.
- Conversion fields: first paid plan/pass, active status, hold, cancellation.
- Earliest reliable history from `2025-01-01`.

## Decide

- Which Pike13 export/browser source determines enrollment outcome.
- Whether trial lessons in the existing `reminders` table are sufficient or need a richer visits/events source.
- Whether daily refresh is full export reload, recent-window scrape, or incremental export.
