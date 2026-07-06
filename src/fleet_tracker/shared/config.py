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

    @property
    def tick_interval(self) -> float:
        """Seconds between ticks (the sleep the simulator loop waits)."""
        return 1.0 / self.tick_hz

    # --- Derived key names (single source of truth) ------------------------
    @property
    def positions_channel(self) -> str:
        """Pub/Sub channel carrying live position *state* for this city."""
        return f"positions:{self.city}"


@lru_cache
def get_settings() -> Settings:
    """Cached accessor so the env is parsed once per process."""
    return Settings()
