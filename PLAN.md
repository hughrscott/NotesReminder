# NotesReminder Next Steps

- Add a `--dry-run` flag to build the report without sending email (and optionally save the output to a file).
- Add a preflight check that validates required env vars and prints a clear error if any are missing.
- Decide whether `node_modules/`, `package.json`, and `package-lock.json` belong in this repo; if not, add ignores and clean them up.
