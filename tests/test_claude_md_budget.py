"""CLAUDE.md size budget enforcement (ABL-574, following ABL-536).

CLAUDE.md auto-loads into every agent context, so its size is a per-turn tax on
every run that touches ingest. The dashboard repo's copy reached 6,752 lines /
426,865 B and killed runs outright; ABL-536 trimmed it and archived the
narrative to docs/claude/. This repo's copy was at 1,204 lines / 51,430 B and
got the same treatment (ABL-574).

A trim fixes the level, not the slope. Both files grew under a written rule
asking for brevity, which is exactly the arrangement that produced the runaway.
This module is the mechanical half: the budget is asserted, and the failure
message names docs/claude/ so the remedy is the obvious next move rather than
raising the number.

Line endings are normalised before measuring. core.autocrlf=true gives this
Windows working tree CRLF, ~1 B per line heavier than the LF blob git stores.
Without normalisation the same commit would fail here and pass on Linux.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import pytest

CLAUDE_MD = Path(__file__).parent.parent / "CLAUDE.md"

#: KB means KiB throughout this repo.
KB = 1024


@dataclass(frozen=True)
class Budget:
    lines: int
    #: Bytes of the UTF-8 encoding, line endings normalised to LF.
    bytes: int


#: The enforced budget. Must stay in step with the "Hard budget" line in
#: CLAUDE.md's "How to maintain this file" section; check_document_budget
#: fails when it does not.
CLAUDE_MD_BUDGET = Budget(lines=700, bytes=35 * KB)


@dataclass(frozen=True)
class DocumentSize:
    lines: int
    bytes: int


def measure_document(text: str) -> DocumentSize:
    """Measure a document the way ``wc -l -c`` would on an LF checkout.

    A trailing newline terminates the last line rather than starting an empty
    one, and an unterminated final line still counts. Bytes, not characters:
    CLAUDE.md is dense with em dashes (3 bytes each), so a character count
    understates it.
    """
    normalised = text.replace("\r\n", "\n")
    if normalised == "":
        return DocumentSize(lines=0, bytes=0)
    line_count = len(normalised.split("\n"))
    if normalised.endswith("\n"):
        line_count -= 1
    return DocumentSize(lines=line_count, bytes=len(normalised.encode("utf-8")))


@dataclass(frozen=True)
class BudgetProblem:
    #: One of: over-line-budget, over-byte-budget, stated-budget-missing,
    #: stated-budget-mismatch.
    kind: str
    #: 1-based line in CLAUDE.md, or 0 for a problem with the document as a whole.
    doc_line: int
    message: str


#: The prose form of the budget, as CLAUDE.md states it:
#: "**Hard budget: 700 lines / 35 KB.**". Tolerant of bold markers, thousands
#: separators and sentence-wrapping, so reflowing a paragraph cannot fail the suite.
_STATED_BUDGET = re.compile(
    r"Hard budget:\s*([\d,]+)\s*lines?\s*/\s*([\d,]+)\s*KB", re.IGNORECASE
)


@dataclass(frozen=True)
class StatedBudget:
    lines: int
    bytes: int
    #: 1-based line in the document where the statement was found.
    doc_line: int


def parse_stated_budget(text: str) -> Optional[StatedBudget]:
    """Read the budget CLAUDE.md states in prose, or None if it no longer states one."""
    normalised = text.replace("\r\n", "\n")
    match = _STATED_BUDGET.search(normalised)
    if match is None:
        return None
    digits = lambda s: int(s.replace(",", ""))  # noqa: E731
    return StatedBudget(
        lines=digits(match.group(1)),
        bytes=digits(match.group(2)) * KB,
        doc_line=normalised[: match.start()].count("\n") + 1,
    )


REMEDY = (
    "Move a section to docs/claude/ and leave a one-line pointer. Do not raise "
    "the budget to fit -- this file auto-loads into every agent context, so "
    "every line is paid on every turn by every run."
)


def check_document_budget(
    text: str, budget: Budget = CLAUDE_MD_BUDGET
) -> List[BudgetProblem]:
    """Check a document against the budget, and check that its prose statement
    of the budget agrees with the enforced constant."""
    size = measure_document(text)
    problems: List[BudgetProblem] = []

    if size.lines > budget.lines:
        problems.append(
            BudgetProblem(
                kind="over-line-budget",
                doc_line=budget.lines + 1,
                message=(
                    f"CLAUDE.md is {size.lines} lines, "
                    f"{size.lines - budget.lines} over the {budget.lines}-line "
                    f"budget. {REMEDY}"
                ),
            )
        )

    if size.bytes > budget.bytes:
        problems.append(
            BudgetProblem(
                kind="over-byte-budget",
                doc_line=0,
                message=(
                    f"CLAUDE.md is {size.bytes} B, {size.bytes - budget.bytes} B "
                    f"over the {budget.bytes} B ({budget.bytes // KB} KB) budget. "
                    f"{REMEDY}"
                ),
            )
        )

    stated = parse_stated_budget(text)
    if stated is None:
        problems.append(
            BudgetProblem(
                kind="stated-budget-missing",
                doc_line=0,
                message=(
                    "CLAUDE.md no longer states its own size budget. Agents learn "
                    "this rule by reading the file, so restate it in the form "
                    f'"Hard budget: {budget.lines} lines / {budget.bytes // KB} KB" '
                    'in the "How to maintain this file" section.'
                ),
            )
        )
    elif stated.lines != budget.lines or stated.bytes != budget.bytes:
        problems.append(
            BudgetProblem(
                kind="stated-budget-mismatch",
                doc_line=stated.doc_line,
                message=(
                    f"CLAUDE.md states a budget of {stated.lines} lines / "
                    f"{stated.bytes // KB} KB, but {budget.lines} lines / "
                    f"{budget.bytes // KB} KB is enforced (CLAUDE_MD_BUDGET, "
                    "tests/test_claude_md_budget.py). Change both together."
                ),
            )
        )

    return problems


def format_budget_problems(problems: List[BudgetProblem]) -> str:
    """Render problems as one message, each prefixed with a clickable doc location."""
    return "\n".join(
        f"CLAUDE.md:{p.doc_line}  {p.message}" if p.doc_line > 0 else f"  {p.message}"
        for p in problems
    )


# --------------------------------------------------------------------------
# The real document
# --------------------------------------------------------------------------


def test_claude_md_is_within_its_size_budget() -> None:
    text = CLAUDE_MD.read_text(encoding="utf-8")
    problems = check_document_budget(text)
    assert problems == [], "\n" + format_budget_problems(problems)


def test_claude_md_states_the_budget_it_is_held_to() -> None:
    """The prose rule and the enforced constant are two statements of one fact.

    If they drift, an agent reads one budget and the suite enforces another --
    which is how a written-only rule stops being believed.
    """
    stated = parse_stated_budget(CLAUDE_MD.read_text(encoding="utf-8"))
    assert stated is not None, "CLAUDE.md must state its own budget in prose"
    assert (stated.lines, stated.bytes) == (
        CLAUDE_MD_BUDGET.lines,
        CLAUDE_MD_BUDGET.bytes,
    )


def test_claude_md_points_at_the_archive() -> None:
    """A budget with nowhere to put the overflow is just a refusal to document."""
    text = CLAUDE_MD.read_text(encoding="utf-8")
    assert "docs/claude/" in text
    assert (CLAUDE_MD.parent / "docs" / "claude").is_dir()


# --------------------------------------------------------------------------
# Negative controls -- the guard must actually bite
# --------------------------------------------------------------------------

_STATEMENT = "**Hard budget: 700 lines / 35 KB.**"


def _document(lines: int, filler: str = "x") -> str:
    """A synthetic document with a valid budget statement and `lines` lines."""
    body = [_STATEMENT] + [filler] * (lines - 1)
    return "\n".join(body) + "\n"


def test_guard_fails_on_a_document_over_the_line_budget() -> None:
    problems = check_document_budget(_document(CLAUDE_MD_BUDGET.lines + 1))
    assert [p.kind for p in problems] == ["over-line-budget"]
    assert "701 lines, 1 over the 700-line budget" in problems[0].message


def test_guard_fails_on_a_document_over_the_byte_budget() -> None:
    """Bytes are checked independently of lines: 10 very long lines are over
    budget even though 10 is nowhere near 700."""
    fat = "y" * 4096
    problems = check_document_budget(_document(10, filler=fat))
    assert [p.kind for p in problems] == ["over-byte-budget"]
    assert "over the 35840 B (35 KB) budget" in problems[0].message


def test_an_oversized_failure_message_names_the_archive_directory() -> None:
    """The message has to carry the remedy. A bare 'too big' invites the wrong
    fix -- raising the number -- which is what left this file at 51,430 B."""
    for problems in (
        check_document_budget(_document(CLAUDE_MD_BUDGET.lines + 50)),
        check_document_budget(_document(10, filler="y" * 4096)),
    ):
        assert problems
        assert "docs/claude/" in problems[0].message
        assert "Do not raise the budget to fit" in problems[0].message


def test_guard_fails_when_the_document_stops_stating_the_budget() -> None:
    problems = check_document_budget("# CLAUDE.md\n\nno budget stated here\n")
    assert [p.kind for p in problems] == ["stated-budget-missing"]
    assert "Hard budget: 700 lines / 35 KB" in problems[0].message


def test_guard_fails_when_prose_and_constant_disagree() -> None:
    """Raising the prose budget alone must not buy headroom."""
    text = "**Hard budget: 2,000 lines / 100 KB.**\n"
    problems = check_document_budget(text)
    assert [p.kind for p in problems] == ["stated-budget-mismatch"]
    assert "states a budget of 2000 lines / 100 KB" in problems[0].message
    assert "700 lines / 35 KB is enforced" in problems[0].message


def test_a_compliant_document_produces_no_problems() -> None:
    assert check_document_budget(_document(CLAUDE_MD_BUDGET.lines)) == []


# --------------------------------------------------------------------------
# Measurement
# --------------------------------------------------------------------------


def test_crlf_and_lf_measure_identically() -> None:
    """core.autocrlf=true means the same commit is CRLF here and LF on Linux.
    An un-normalised byte count would fail on one and pass on the other."""
    lf = "a\nb\nc\n"
    assert measure_document(lf) == measure_document(lf.replace("\n", "\r\n"))


@pytest.mark.parametrize(
    "text,expected_lines",
    [
        ("", 0),
        ("a", 1),
        ("a\n", 1),
        ("a\nb", 2),
        ("a\nb\n", 2),
        ("\n", 1),
    ],
)
def test_line_counting_matches_wc_l(text: str, expected_lines: int) -> None:
    assert measure_document(text).lines == expected_lines


def test_bytes_not_characters() -> None:
    """Em dashes are 3 bytes each; a character count would understate the file."""
    assert measure_document("—").bytes == 3
