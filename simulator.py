"""
simulator.py - Application-layer network condition simulation.

Injects artificial packet loss and delay into send paths.
No root required - operates entirely at the application layer by
intercepting packets before they reach the OS network stack.

Usage via CLI:
    /simulate loss 40      -> 40% packet drop rate
    /simulate delay 200    -> 200ms additional latency (+-20% jitter)
    /simulate reset        -> disable all simulation
    /simulate status       -> show current config and stats
"""

import random
import threading


class SimulatorConfig:
    """Thread-safe network condition simulator.

    Injected into send paths (both TCP and UDP).  Before each send:
      1) Call should_drop() - if True, silently discard the packet.
      2) Call get_delay()   - sleep that many seconds before actual send.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._loss_rate: float = 0.0      # 0.0 .. 1.0
        self._delay_ms: float = 0.0       # additional ms
        self._enabled: bool = False
        # Stats
        self._stats_total: int = 0
        self._stats_dropped: int = 0
        self._stats_delayed: int = 0

    # -- Configuration --------------------------------------------------

    @property
    def enabled(self) -> bool:
        with self._lock:
            return self._enabled

    def set_loss(self, rate: float) -> None:
        """Set loss rate in [0.0, 1.0]."""
        with self._lock:
            self._loss_rate = max(0.0, min(1.0, rate))
            self._enabled = self._loss_rate > 0 or self._delay_ms > 0

    def set_delay(self, ms: float) -> None:
        """Set extra delay in milliseconds (>= 0)."""
        with self._lock:
            self._delay_ms = max(0.0, ms)
            self._enabled = self._loss_rate > 0 or self._delay_ms > 0

    def reset(self) -> None:
        """Turn off all simulation and clear stats."""
        with self._lock:
            self._loss_rate = 0.0
            self._delay_ms = 0.0
            self._enabled = False
            self._stats_total = 0
            self._stats_dropped = 0
            self._stats_delayed = 0

    # -- Injection points -----------------------------------------------

    def should_drop(self) -> bool:
        """Returns True if this packet should be silently discarded."""
        with self._lock:
            self._stats_total += 1
            if self._loss_rate > 0 and random.random() < self._loss_rate:
                self._stats_dropped += 1
                return True
            return False

    def get_delay(self) -> float:
        """Returns seconds to sleep before the actual send.
        Adds +/- 20% jitter around the configured delay.
        """
        with self._lock:
            if self._delay_ms > 0:
                self._stats_delayed += 1
                jitter = self._delay_ms * 0.2 * (random.random() * 2 - 1)
                return max(0.0, (self._delay_ms + jitter)) / 1000.0
            return 0.0

    # -- Status ---------------------------------------------------------

    def get_status(self) -> str:
        with self._lock:
            if not self._enabled:
                return "Simulator: OFF"
            drop_pct = (
                f"{self._stats_dropped}/{self._stats_total}"
                if self._stats_total > 0
                else "0/0"
            )
            return (
                f"Simulator: ON\n"
                f"  Loss rate : {self._loss_rate * 100:.1f}%\n"
                f"  Delay     : {self._delay_ms:.0f}ms (+/-20% jitter)\n"
                f"  Packets   : {self._stats_total} total, "
                f"{self._stats_dropped} dropped ({drop_pct}), "
                f"{self._stats_delayed} delayed"
            )
