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

    # --- Geofence (durable event consumer, M7) -----------------------------
    geofence_group: str = "geofence"

    # --- Replay (M10) ------------------------------------------------------
    @property
    def replay_channel(self) -> str:
        """Isolated channel for replayed history — ONLY the UI subscribes here.

        The whole point of ADR-0008: side-effecting consumers (geofence,
        analytics, dropout) never listen on this, so replaying old data can't
        re-fire their effects. Isolation, not idempotency, is what makes replay
        safe.
        """
        return f"replay:{self.city}"

    # --- Dropout watcher (absence detection, M9) ---------------------------
    # A vehicle silent longer than this is declared offline; the scan runs on its
    # own timer (not the stream), because absence produces no message to react to.
    dropout_threshold_s: float = Field(default=5.0, gt=0)
    dropout_scan_interval_s: float = Field(default=2.0, gt=0)

    @property
    def dropout_stream(self) -> str:
        """Durable log of vehicle offline/online *events* (XADD)."""
        return f"dropouts:{self.city}"

    @property
    def dropout_channel(self) -> str:
        """Pub/Sub channel for live presence updates (grey/ungrey the map)."""
        return f"dropout:{self.city}"

    @property
    def dropout_offline_key(self) -> str:
        """Set of vehicle_ids currently offline — the read model a fresh browser
        loads so it greys the right dots on connect (ADR-0005 cold-start, again)."""
        return f"dropout:offline:{self.city}"

    # --- Analytics (second consumer group, M8) -----------------------------
    analytics_group: str = "analytics"

    @property
    def analytics_key(self) -> str:
        """Redis hash of per-vehicle rollups (distance, avg speed, samples).

        A second read model, maintained by the analytics group reading the SAME
        telemetry stream as geofence but on its own independent cursor (fan-out).
        """
        return f"analytics:{self.city}"

    @property
    def alerts_stream(self) -> str:
        """Durable log of geofence ENTER/EXIT *events* for this city (XADD).

        The durable copy is what enables replay (M10) and sink-side dedup; a
        parallel PUBLISH on ``alerts_channel`` drives live toasts.
        """
        return f"alerts:{self.city}"

    @property
    def alerts_channel(self) -> str:
        """Pub/Sub channel for live alert toasts (best-effort, at-most-once)."""
        return f"alerts:{self.city}"

    @property
    def geofence_inside_key(self) -> str:
        """Durable ``was_inside`` state: hash vehicle_id -> set of zone names.

        Lives in Redis (not the consumer's RAM) so edge detection survives a
        consumer crash — the crux of idempotent, effectively-once processing
        (ADR-0004). Without it, a restarted consumer would re-fire every ENTER.
        """
        return f"geofence:inside:{self.city}"

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
