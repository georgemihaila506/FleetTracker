"""Runtime configuration, loaded from environment variables (or a .env file).

One ``Settings`` object read once at startup and passed around. Every knob has a
sane default so the app runs with zero config; override any of them by exporting
``FLEET_<NAME>`` or putting it in a local ``.env`` (gitignored).

    FLEET_REDIS_URL=redis://localhost:6379/0
    FLEET_CITY=testcity
    FLEET_TICK_HZ=1.0
    FLEET_VEHICLE_COUNT=50

Channel/stream *names* are derived from the city here (one place), so producers
and consumers can never disagree on where a message lives.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="FLEET_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- Redis -------------------------------------------------------------
    redis_url: str = "redis://localhost:6379/0"

    # --- Domain ------------------------------------------------------------
    city: str = "testcity"
    vehicle_count: int = Field(default=50, ge=1)

    # --- Simulator ---------------------------------------------------------
    # How often each vehicle publishes its current position. State is re-sent
    # every tick, so a dropped message just means "wait one tick" (ADR-0002).
    tick_hz: float = Field(default=1.0, gt=0)

    # --- Streams (durable event path, M6) ----------------------------------
    # Cap the telemetry stream's length so it doesn't grow in RAM forever. Trim
    # is approximate (MAXLEN ~) — cheap, removes whole macro-nodes (ADR-0006).
    stream_maxlen: int = Field(default=10_000, ge=1)

    @property
    def tick_interval(self) -> float:
        """Seconds between ticks (the sleep the simulator loop waits)."""
        return 1.0 / self.tick_hz

    # --- Derived key names (single source of truth) ------------------------
    @property
    def positions_channel(self) -> str:
        """Pub/Sub channel carrying live position *state* for this city."""
        return f"positions:{self.city}"

    @property
    def telemetry_stream(self) -> str:
        """Redis Stream carrying durable position *events* for this city (M6).

        Same payload as ``positions_channel``, opposite delivery semantics: XADD
        appends to a persistent, replayable log (at-least-once for consumers),
        whereas PUBLISH is fire-and-forget (at-most-once). Consumed later by the
        geofence/analytics groups (M7/M8).
        """
        return f"telemetry:{self.city}"

    @property
    def retention_seconds(self) -> float:
        """How far back the telemetry stream reaches: MAXLEN / rate (ADR-0006).

        Production rate = vehicles x tick_hz entries/sec. A consumer offline
        longer than this loses the trimmed entries — the durability guarantee
        only holds *within* this window.
        """
        rate = self.vehicle_count * self.tick_hz
        return self.stream_maxlen / rate if rate else float("inf")

    @property
    def positions_current_key(self) -> str:
        """Redis hash holding the latest Position per vehicle (the read model).

        One field per vehicle_id; HGETALL gives a fresh browser the whole fleet
        instantly (ADR-0005 cold-start snapshot). Maintained by the materializer.
        """
        return f"positions:current:{self.city}"


@lru_cache
def get_settings() -> Settings:
    """Cached accessor so the env is parsed once per process."""
    return Settings()
