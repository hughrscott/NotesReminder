"""Inverted Okta MFA Auth system for NotesReminder.

Instead of the server guessing when you're available and pinging you to approve an
Okta push, YOU trigger the push on your own schedule: a Telegram inline button
("Trigger Okta MFA") fires the auth flow, Okta pushes to your iPhone, and you
approve whenever you see it — seconds or hours later. No timeout, no wake-ups.

Flow:
  1. You press the Telegram button (only your chat is authorized).
  2. auth_trigger launches the shared Playwright profile, fills your Okta
     username/password from env, and submits.
  3. Okta pushes to your phone. We message you "push sent, approve whenever".
  4. We poll OPEN-ENDED (no timeout) until the session URL leaves the Okta
     verify page — i.e. you approved.
  5. The persisted profile (browser_profiles/sor_shared) now holds a warm
     Okta SSO session federating to HubSpot / Dialpad / Gmail.
  6. A .session_ready flag is written so downstream jobs know they're warm.

INTEGRATION NOTES (for run_daily_cron.sh and notesreminder/mcp/tools.py):
  a) Point Playwright at user_data_dir=browser_profiles/sor_shared so they
     inherit the authenticated session.
  b) Before doing work, call okta_auth.session_state.is_session_ready().
     If False, surface a Telegram "Trigger Okta MFA" button to Hugh instead
     of hanging on the Okta login screen in the background.
"""
