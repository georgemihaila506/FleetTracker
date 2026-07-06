"""Wire message models — the shapes that travel through Redis.

These are pydantic models so that (a) the fields are documented and validated in
one place, and (b) encode/decode to the JSON we put on the wire is trivial and
symmetric.

--------------------------------------------------------------------------------
THE SPINE (ADR-0002): every message here is either STATE or an EVENT.

    Position  = STATE.  The current value of a vehicle, re-sent every tick.
                Safe to lose (next tick supersedes). -> Pub/Sub, positions:{city}.

    (later)   = EVENT.  A thing that happened once (geofence crossing, dropout).
                Must not be lost. -> Streams. Added in M6/M7.
--------------------------------------------------------------------------------

Your task: fill in `Position` below. Think about:
  * what uniquely identifies the vehicle
  * where it is (two coordinates)
  * when this reading was taken (so a late/duplicate message is recognizable)
  * validation you get for free from pydantic (lat in [-90, 90], lon in [-180, 180])

Then add the two wire helpers so producers/consumers never hand-roll JSON:
  * to_wire(self) -> str      # JSON string to PUBLISH
  * from_wire(cls, raw) -> Position   # parse a received message

Hints (no need to import anything not already here):
  * pydantic v2: `model_dump_json()` and `model_validate_json()` do the JSON work.
  * `Field(..., ge=-90, le=90)` adds range validation.
  * epoch seconds as a float `ts` is the simplest timestamp; `time.time()`.
"""

from __future__ import annotations

import time

from pydantic import BaseModel, Field


class Position(BaseModel):
    """A single vehicle's current location — a STATE message.

    Published every tick to ``positions:{city}``. Because it is state, a lost or
    stale copy is harmless: the next tick carries a fresher one. ``ts`` is what
    lets a consumer recognize a late/duplicate reading and keep the newest.
    """

    vehicle_id: str
    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)
    # Epoch seconds this reading was taken. Defaults to "now" at construction so
    # the simulator doesn't have to set it explicitly, but any producer may.
    ts: float = Field(default_factory=time.time)

    # Optional motion fields — carried if the producer knows them, else None.
    speed: float | None = Field(default=None, ge=0)   # metres/second
    heading: float | None = Field(default=None, ge=0, lt=360)  # degrees, 0=N

    def to_wire(self) -> str:
        """JSON string to PUBLISH onto the positions channel."""
        return self.model_dump_json()

    @classmethod
    def from_wire(cls, raw: str) -> "Position":
        """Parse (and validate) a message received off the wire."""
        return cls.model_validate_json(raw)
