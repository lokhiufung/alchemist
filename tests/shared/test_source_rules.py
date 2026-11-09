import re
from pathlib import Path

COLLABORATORS = "execution|gateway|clock|signal|safety|aux|book|seeder|om|shutdown|event_buffer|bar_recorder|trade_recorder"


def test_no_component_checks_a_collaborator_for_none():
    pattern = re.compile(rf"self\.({COLLABORATORS}) (is|is not) None")
    offenders = [f"{p}:{n}" for p in Path("alchemist").rglob("*.py")
                 for n, line in enumerate(p.read_text().splitlines(), 1) if pattern.search(line)]
    assert offenders == []
