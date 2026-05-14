"""
transport.py - Reliable UDP with Selective-Repeat ARQ.

Extends Workshop 4's sliding-window file transfer into a general-purpose
reliable UDP layer.  Key improvements over Workshop 4:

  * Binary headers (no JSON / Base62 overhead)
  * TRUE Selective-Repeat: receiver NACKs specific missing seqs,
    sender retransmits ONLY those (not Go-Back-N)
  * Priority injection: /sos and /location packets bypass the normal
    sliding window and are sent immediately with aggressive retransmit
  * Adaptive timeout: base * (1 + retries * 0.5), capped at max_timeout
"""

import threading
import time

from protocol import (
    pack_data, unpack_data,
    pack_data_ack, unpack_data_ack,
)


# ---------------------------------------------------------------------------
# Sender
# ---------------------------------------------------------------------------

class _PendingPacket:
    """Tracks a packet awaiting acknowledgement."""
    __slots__ = ("raw", "send_time", "retries")

    def __init__(self, raw: bytes, send_time: float):
        self.raw = raw
        self.send_time = send_time
        self.retries: int = 0


class ReliableUDPSender:
    """Manages outgoing reliable UDP stream to one peer.

    The timer thread runs every 50 ms:
      1) Drain the priority queue (SOS / location bypass packets).
      2) Send any unsent packets within the current window.
      3) Retransmit timed-out packets.
    """

    def __init__(self, send_func, window_size: int = 10,
                 base_timeout: float = 0.5, max_timeout: float = 5.0,
                 simulator=None):
        """
        Parameters
        ----------
        send_func : callable(data: bytes) -> None
            Sends a UDP datagram to the target peer.
        """
        self._send_raw = send_func
        self._window_size = window_size
        self._base_timeout = base_timeout
        self._max_timeout = max_timeout
        self._simulator = simulator

        self._lock = threading.Lock()
        self._next_seq: int = 1
        self._send_base: int = 1          # oldest unacked seq
        self._buffer: dict[int, _PendingPacket] = {}
        self._done_event = threading.Event()

        # Priority bypass queue (for SOS / location in CRISIS)
        self._pq: list[bytes] = []
        self._pq_lock = threading.Lock()

        self._running = False
        self._thread = None

    # -- Lifecycle ------------------------------------------------------

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._timer_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        self._done_event.set()

    # -- Public API -----------------------------------------------------

    def send(self, data_payload: bytes, flags: int = 0,
             priority: bool = False) -> int:
        """Queue data for reliable delivery.

        Parameters
        ----------
        data_payload : bytes
            The inner packet bytes (e.g. a packed MESSAGE or SOS).
        flags : int
            Protocol flags forwarded into the DATA header.
        priority : bool
            If True, inject into the priority queue for immediate send
            (bypasses normal window flow).

        Returns the assigned sequence number.
        """
        with self._lock:
            seq = self._next_seq
            self._next_seq += 1
            raw = pack_data(seq, data_payload, flags)
            self._buffer[seq] = _PendingPacket(raw, 0)  # 0 = not yet sent
            self._done_event.clear()

        if priority:
            with self._pq_lock:
                self._pq.append(raw)
            # Mark as sent immediately so the timer doesn't double-send
            with self._lock:
                pkt = self._buffer.get(seq)
                if pkt:
                    pkt.send_time = time.monotonic()

        return seq

    def handle_ack(self, acked_seq: int, rwnd: int, nacks: list) -> None:
        """Process an incoming DATA_ACK (selective repeat)."""
        with self._lock:
            # Remove the acked packet
            self._buffer.pop(acked_seq, None)

            # Advance send_base
            while (self._send_base not in self._buffer
                   and self._send_base < self._next_seq):
                self._send_base += 1

            # Adjust window from receiver's advertised rwnd
            if rwnd > 0:
                self._window_size = rwnd

            # Immediately retransmit NACKed sequences
            for nack_seq in nacks:
                pkt = self._buffer.get(nack_seq)
                if pkt:
                    self._do_send(pkt.raw)
                    pkt.send_time = time.monotonic()
                    pkt.retries += 1

            # Signal completion if buffer is empty
            if not self._buffer:
                self._done_event.set()

    def wait_complete(self, timeout: float = 30.0) -> bool:
        """Block until all queued packets are acknowledged (or timeout)."""
        return self._done_event.wait(timeout)

    # -- Timer thread ---------------------------------------------------

    def _timer_loop(self) -> None:
        while self._running:
            now = time.monotonic()

            # 1) Drain priority queue
            with self._pq_lock:
                pq_copy = list(self._pq)
                self._pq.clear()
            for raw in pq_copy:
                self._do_send(raw)

            with self._lock:
                # 2) Send unsent packets within window
                window_end = self._send_base + self._window_size
                for seq in range(self._send_base, min(window_end, self._next_seq)):
                    pkt = self._buffer.get(seq)
                    if pkt and pkt.send_time == 0:
                        self._do_send(pkt.raw)
                        pkt.send_time = now

                # 3) Retransmit timed-out packets
                for seq, pkt in list(self._buffer.items()):
                    if pkt.send_time == 0:
                        continue  # not yet sent (outside window)
                    timeout = min(
                        self._base_timeout * (1 + pkt.retries * 0.5),
                        self._max_timeout,
                    )
                    if now - pkt.send_time >= timeout:
                        self._do_send(pkt.raw)
                        pkt.send_time = now
                        pkt.retries += 1

            time.sleep(0.05)  # 50 ms tick

    def _do_send(self, data: bytes) -> None:
        """Send with optional simulator injection."""
        if self._simulator and self._simulator.enabled:
            if self._simulator.should_drop():
                return
            delay = self._simulator.get_delay()
            if delay > 0:
                time.sleep(delay)
        try:
            self._send_raw(data)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Receiver
# ---------------------------------------------------------------------------

class ReliableUDPReceiver:
    """Manages incoming reliable UDP stream from one peer.

    Buffers out-of-order packets, delivers in-order via callback,
    and builds selective ACKs with explicit NACKs for missing sequences.
    """

    def __init__(self, on_deliver, max_rwnd: int = 10):
        """
        Parameters
        ----------
        on_deliver : callable(seq: int, data: bytes, flags: int) -> None
            Called when a packet is delivered in order.
        """
        self._on_deliver = on_deliver
        self._max_rwnd = max_rwnd

        self._lock = threading.Lock()
        self._expected_seq: int = 1
        self._buffer: dict[int, tuple] = {}  # seq -> (chunk_bytes, flags)
        self._delivered: set = set()

    def handle_data(self, seq: int, data: bytes, flags: int) -> bytes:
        """Process incoming DATA packet.

        Returns a DATA_ACK packet (bytes) to be sent back to the sender.
        """
        with self._lock:
            # Duplicate detection
            if seq in self._delivered:
                return self._build_ack(seq)

            # Buffer the packet
            self._buffer[seq] = (data, flags)

            # Deliver consecutive packets in order
            while self._expected_seq in self._buffer:
                d, f = self._buffer.pop(self._expected_seq)
                self._delivered.add(self._expected_seq)
                try:
                    self._on_deliver(self._expected_seq, d, f)
                except Exception:
                    pass
                self._expected_seq += 1

            # Trim delivered set to prevent unbounded growth
            if len(self._delivered) > 1000:
                cutoff = self._expected_seq - 500
                self._delivered = {s for s in self._delivered if s >= cutoff}

            return self._build_ack(seq)

    def _build_ack(self, acked_seq: int) -> bytes:
        """Build a selective ACK reporting gaps (NACKs) in the receive window."""
        nacks = []
        if self._buffer:
            max_buffered = max(self._buffer.keys())
            for s in range(self._expected_seq, max_buffered):
                if s not in self._buffer and s not in self._delivered:
                    nacks.append(s)

        rwnd = max(1, self._max_rwnd - len(self._buffer))
        return pack_data_ack(acked_seq, rwnd, nacks)
