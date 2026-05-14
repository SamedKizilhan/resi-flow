"""
telemetry.py - Real-time network telemetry monitor.

Sends periodic lightweight heartbeat probes (struct-packed, 13 bytes total)
and continuously calculates RTT and Packet Loss using exponential moving
averages (EMA).  Drives the dynamic protocol pivot decision in node.py.

EMA formula:
    metric_avg = alpha * sample + (1 - alpha) * metric_avg

A rolling window of the last N heartbeats determines instantaneous loss.
"""

import time
import threading
from collections import deque

from protocol import pack_heartbeat_req, pack_heartbeat_ack, unpack_heartbeat


# ---------------------------------------------------------------------------
# Per-peer metrics
# ---------------------------------------------------------------------------

class PeerMetrics:
    __slots__ = ("rtt_ema", "loss_ema", "hb_seq", "window", "rtt_initialized")

    def __init__(self, window_size: int = 10):
        self.rtt_ema: float = 0.0           # milliseconds
        self.loss_ema: float = 0.0           # 0.0 .. 1.0
        self.hb_seq: int = 0                 # incrementing per-peer counter
        # Rolling window: each entry = [ts_ns, acked: bool]
        self.window: deque = deque(maxlen=window_size)
        self.rtt_initialized: bool = False


# ---------------------------------------------------------------------------
# Monitor
# ---------------------------------------------------------------------------

class TelemetryMonitor:
    """Background daemon that probes peers and tracks network health."""

    def __init__(self, send_func, interval: float = 2.0,
                 alpha: float = 0.3, window_size: int = 10):
        """
        Parameters
        ----------
        send_func : callable(target_ip: str, data: bytes) -> None
            Low-level UDP send (goes through simulator if attached).
        interval : float
            Seconds between heartbeat probes to each peer.
        alpha : float
            EMA smoothing factor (higher = more responsive).
        window_size : int
            Number of recent heartbeats used for loss calculation.
        """
        self._send = send_func
        self._interval = interval
        self._alpha = alpha
        self._window_size = window_size
        self._peers: dict[str, PeerMetrics] = {}
        self._lock = threading.Lock()
        self._running = False
        self._thread = None

        # Callback: on_metrics_update(ip, rtt_ms, loss_ratio)
        # Set by node.py to trigger pivot checks.
        self.on_metrics_update = None

    # -- Lifecycle ------------------------------------------------------

    def start(self) -> None:
        self._running = True
        self._thread = threading.Thread(target=self._sender_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False

    # -- Peer management ------------------------------------------------

    def register_peer(self, ip: str) -> None:
        with self._lock:
            if ip not in self._peers:
                self._peers[ip] = PeerMetrics(self._window_size)

    def remove_peer(self, ip: str) -> None:
        with self._lock:
            self._peers.pop(ip, None)

    # -- Incoming packet handlers ---------------------------------------

    def handle_heartbeat_req(self, sender_ip: str, ts_ns: int) -> bytes:
        """Process an incoming heartbeat request.
        Returns a HEARTBEAT_ACK packet (bytes) echoing the timestamp.
        The caller (node.py UDP listener) is responsible for sending it.
        """
        self.register_peer(sender_ip)
        return pack_heartbeat_ack(ts_ns)

    def handle_heartbeat_ack(self, sender_ip: str, echoed_ts_ns: int) -> None:
        """Process an incoming heartbeat ACK.
        Updates RTT EMA and loss EMA for the peer.
        """
        now_ns = time.monotonic_ns()

        with self._lock:
            metrics = self._peers.get(sender_ip)
            if not metrics:
                return

            # ---- RTT ----
            rtt_sample_ms = (now_ns - echoed_ts_ns) / 1_000_000.0
            if rtt_sample_ms < 0:
                # Clock skew / stale ACK - ignore
                return

            if not metrics.rtt_initialized:
                metrics.rtt_ema = rtt_sample_ms
                metrics.rtt_initialized = True
            else:
                metrics.rtt_ema = (
                    self._alpha * rtt_sample_ms
                    + (1 - self._alpha) * metrics.rtt_ema
                )

            # ---- Mark acked in window ----
            for entry in metrics.window:
                if entry[0] == echoed_ts_ns and not entry[1]:
                    entry[1] = True
                    break

            # ---- Loss EMA ----
            self._update_loss(metrics)

            rtt = metrics.rtt_ema
            loss = metrics.loss_ema

        # Notify outside lock
        if self.on_metrics_update:
            self.on_metrics_update(sender_ip, rtt, loss)

    # -- Queries --------------------------------------------------------

    def get_metrics(self, ip: str):
        """Returns (rtt_ms, loss_ratio) for a peer."""
        with self._lock:
            m = self._peers.get(ip)
            if m:
                return m.rtt_ema, m.loss_ema
            return 0.0, 0.0

    def get_all_metrics(self) -> dict:
        """Returns {ip: (rtt_ms, loss_ratio)} for all peers."""
        with self._lock:
            return {ip: (m.rtt_ema, m.loss_ema)
                    for ip, m in self._peers.items()}

    # -- Internal -------------------------------------------------------

    def _update_loss(self, metrics: PeerMetrics) -> None:
        """Recalculate loss EMA from the rolling window."""
        if len(metrics.window) == 0:
            return
        total = len(metrics.window)
        acked = sum(1 for _, a in metrics.window if a)
        loss_sample = 1.0 - (acked / total)

        if total < 3:
            # Not enough data yet - use raw sample
            metrics.loss_ema = loss_sample
        else:
            metrics.loss_ema = (
                self._alpha * loss_sample
                + (1 - self._alpha) * metrics.loss_ema
            )

    def _sender_loop(self) -> None:
        """Background thread: send heartbeats to every registered peer."""
        while self._running:
            with self._lock:
                peer_ips = list(self._peers.keys())

            for ip in peer_ips:
                ts_ns = time.monotonic_ns()
                packet = pack_heartbeat_req(ts_ns)

                with self._lock:
                    metrics = self._peers.get(ip)
                    if metrics:
                        metrics.hb_seq += 1
                        # Record in rolling window (not yet acked)
                        metrics.window.append([ts_ns, False])

                try:
                    self._send(ip, packet)
                except Exception:
                    pass  # send failure is reflected as loss

            time.sleep(self._interval)
