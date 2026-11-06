"""Order state machine: IDLE -> ENTRY_PENDING -> (FILLED | PARTIAL | MISSED) -> EXIT_PENDING -> (CLOSED | RETRYING | STUCK) -> SETTLED.

Every transition is validated against ALLOWED and logged by the layer. MISSED returns the slot to IDLE; SETTLED too."""
from enum import Enum


class S(str, Enum):
    IDLE = "IDLE"
    ENTRY_PENDING = "ENTRY_PENDING"
    FILLED = "FILLED"
    PARTIAL = "PARTIAL"
    MISSED = "MISSED"
    EXIT_PENDING = "EXIT_PENDING"
    RETRYING = "RETRYING"
    CLOSED = "CLOSED"
    STUCK = "STUCK"
    SETTLED = "SETTLED"


ALLOWED = {
    S.IDLE: {S.ENTRY_PENDING},
    S.ENTRY_PENDING: {S.FILLED, S.PARTIAL, S.MISSED},
    S.FILLED: {S.EXIT_PENDING, S.RETRYING, S.CLOSED, S.STUCK},      # RETRYING: the first exit waits for a valid book
    S.PARTIAL: {S.EXIT_PENDING, S.RETRYING, S.CLOSED, S.STUCK},
    S.MISSED: {S.IDLE},
    S.EXIT_PENDING: {S.CLOSED, S.RETRYING, S.STUCK},
    S.RETRYING: {S.EXIT_PENDING, S.CLOSED, S.STUCK},
    S.CLOSED: {S.SETTLED},
    S.STUCK: {S.SETTLED},
    S.SETTLED: {S.IDLE},
}
OPEN = {S.ENTRY_PENDING, S.FILLED, S.PARTIAL, S.EXIT_PENDING, S.RETRYING}      # a position is being worked on; the slot is taken
TERMINAL = {S.MISSED, S.CLOSED, S.STUCK, S.SETTLED, S.IDLE}


class IllegalTransition(Exception):
    pass


def check(frm, to):
    if to not in ALLOWED[frm]:
        raise IllegalTransition(f"{frm.value} -> {to.value}")
