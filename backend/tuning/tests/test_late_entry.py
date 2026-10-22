import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from tuning.research_late_entry import bucket  # noqa: E402


def test_bucket_boundaries_match_website():
    assert bucket(100 / 100 - 1) == "green"
    assert bucket(103 / 100 - 1) == "green"      # exactly +3% (float noise rounded away)
    assert bucket(95 / 100 - 1) == "green"       # exactly −5%
    assert bucket(94.9 / 100 - 1) == "red_lo"
    assert bucket(103.1 / 100 - 1) == "yellow"
    assert bucket(108 / 100 - 1) == "yellow"
    assert bucket(108.1 / 100 - 1) == "red_hi"
