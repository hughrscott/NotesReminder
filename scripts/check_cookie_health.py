#!/usr/bin/env python3
"""Check Pike13 cookie health and exit non-zero if cookies need refresh."""
import sys
from notesreminder.lib.cookie_auth import check_cookie_freshness


def main():
    result = check_cookie_freshness()
    status = result["status"]
    print(f"Cookie status: {status}")
    for k, v in result.items():
        if k != "status":
            print(f"  {k}: {v}")

    if status == "missing":
        print("ACTION: Run scripts/extract_pike13_cookies.py on your Mac to generate cookies.")
        print("ACTION: Then scp pike13_cookies.json to the server.")
        sys.exit(2)
    elif status == "expired":
        print("ACTION: Cookies have expired. Run scripts/refresh_pike13_cookies.sh on your Mac.")
        sys.exit(1)
    elif result.get("soonest_expiry_days", 999) < 3:
        print(f"WARNING: Cookies expire in {result['soonest_expiry_days']} days. Schedule a refresh soon.")
        sys.exit(0)  # warn but don't fail
    else:
        print("Cookies are fresh.")
        sys.exit(0)


if __name__ == "__main__":
    main()
