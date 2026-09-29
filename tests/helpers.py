"""Helpers shared by unit and integration tests."""

# Crawler options for tests that check something other than politeness:
# without the rate limit, robots.txt and retries they run fast and see only
# the requests they make themselves.
UNTHROTTLED = {"requests_per_second": None, "respect_robots": False, "max_retries": 0}
