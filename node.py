"""
node.py - ResilienceNode: the orchestrator.

Manages peer discovery, protocol pivot state machine, and all subsystems.

State Machine:
    NORMAL  --[loss > 30% OR rtt > 500ms]--> CRISIS
    CRISIS  --[loss < 15% AND rtt < 250ms]--> NORMAL   (hysteresis)

In NORMAL mode all messages go over TCP.
In CRISIS  mode all messages go over Reliable UDP (Selective-Repeat ARQ).
The UDP socket is pre-allocated at startup so the pivot has zero delay.
"""

import enum
import os
import socket
import sys
import threading
import time

from protocol import (
    HEADER_SIZE,
    PKT_HEARTBEAT_REQ, PKT_HEARTBEAT_ACK,
    PKT_ASK, PKT_REPLY, PKT_MESSAGE,
    PKT_DATA, PKT_DATA_ACK,
    PKT_SOS, PKT_LOCATION,
    PKT_FILE_META, PKT_FILE_CHUNK,
    FLAG_EOF,
    unpack_header,
    pack_ask, unpack_ask,
    pack_reply, unpack_reply,
    pack_message, unpack_message,
    pack_sos, unpack_sos,
    pack_location, unpack_location,
    pack_file_meta, unpack_file_meta,
    pack_file_chunk, unpack_file_chunk,
    unpack_data, unpack_data_ack,
    unpack_heartbeat,
)
from telemetry import TelemetryMonitor
from transport import ReliableUDPSender, ReliableUDPReceiver
from simulator import SimulatorConfig


# ---------------------------------------------------------------------------
# State machine
# ---------------------------------------------------------------------------

class NetworkMode(enum.Enum):
    NORMAL = "NORMAL"
    CRISIS = "CRISIS"


# Thresholds
LOSS_THRESHOLD   = 0.30    # enter CRISIS
RTT_THRESHOLD    = 500.0   # ms, enter CRISIS
RECOVERY_LOSS    = 0.15    # exit CRISIS (hysteresis)
RECOVERY_RTT     = 250.0   # ms, exit CRISIS

# Network
DEFAULT_PORT     = 12487
SOCK_TIMEOUT     = 2
DISCOVERY_WAIT   = 3
HEARTBEAT_BCAST  = 60      # seconds between broadcast re-discovery


# ---------------------------------------------------------------------------
# Incoming file state
# ---------------------------------------------------------------------------

class _IncomingFile:
    __slots__ = ("filename", "total_chunks", "file_size", "chunks")

    def __init__(self, filename: str, total_chunks: int, file_size: int):
        self.filename = filename
        self.total_chunks = total_chunks
        self.file_size = file_size
        self.chunks: dict[int, bytes] = {}


# ---------------------------------------------------------------------------
# ResilienceNode
# ---------------------------------------------------------------------------

class ResilienceNode:
    """Core node that ties all subsystems together."""

    def __init__(self, name: str, port: int = DEFAULT_PORT):
        self.name = name
        self.local_ip = self._get_local_ip()
        self.port = port
        self.mode = NetworkMode.NORMAL

        # Peer registry  {ip: display_name}
        self.peers: dict[str, str] = {}
        self.peers_lock = threading.Lock()

        # Subsystems
        self.simulator = SimulatorConfig()
        self.telemetry = TelemetryMonitor(
            send_func=self._send_udp_raw,
            interval=2.0,
            alpha=0.3,
            window_size=10,
        )
        self.telemetry.on_metrics_update = self._on_metrics_update

        # Per-peer reliable transport (lazily created)
        self._senders: dict[str, ReliableUDPSender] = {}
        self._receivers: dict[str, ReliableUDPReceiver] = {}

        # Incoming file buffers
        self._incoming_files: dict[str, _IncomingFile] = {}  # sender_ip -> state

        # Pre-allocated UDP socket (shared for all UDP ops)
        self._udp_sock: socket.socket = None
        self._udp_lock = threading.Lock()

        # Current chat target
        self.target_ip: str = None
        self.target_name: str = None

        # Output
        self._print_lock = threading.Lock()
        self._running = False

    # ================================================================
    #  Lifecycle
    # ================================================================

    def start(self) -> None:
        self._running = True

        # Pre-allocate UDP socket (zero-delay pivot)
        self._udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self._udp_sock.bind(("", self.port))
        except OSError as e:
            print(f"[ERROR] Cannot bind UDP port {self.port}: {e}")
            sys.exit(1)

        # Listeners
        threading.Thread(target=self._tcp_listener, daemon=True).start()
        threading.Thread(target=self._udp_listener, daemon=True).start()

        # Telemetry heartbeats
        self.telemetry.start()

        # Periodic broadcast for discovery
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()

        self._safe_print(f"[*] Listening on port {self.port} as '{self.name}'")
        self._safe_print(f"[*] Mode: {self.mode.value}")

    def stop(self) -> None:
        self._running = False
        self.telemetry.stop()
        for s in self._senders.values():
            s.stop()

    # ================================================================
    #  Helpers
    # ================================================================

    @staticmethod
    def _get_local_ip() -> str:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]

    def _safe_print(self, msg: str, prompt: bool = False) -> None:
        with self._print_lock:
            print(f"\n{msg}")
            if prompt:
                print("You: ", end="", flush=True)

    # ================================================================
    #  Low-level send
    # ================================================================

    def _send_udp_raw(self, target_ip: str, data: bytes) -> None:
        """UDP send with simulator injection (loss + delay)."""
        if self.simulator.enabled:
            if self.simulator.should_drop():
                return
            delay = self.simulator.get_delay()
            if delay > 0:
                time.sleep(delay)
        with self._udp_lock:
            try:
                self._udp_sock.sendto(data, (target_ip, self.port))
            except OSError:
                pass

    def _make_udp_sender(self, target_ip: str):
        """Returns a callable(data) bound to target_ip for ReliableUDPSender."""
        def _send(data: bytes) -> None:
            self._send_udp_raw(target_ip, data)
        return _send

    def _send_tcp(self, target_ip: str, data: bytes) -> bool:
        """TCP send with simulator injection."""
        if self.simulator.enabled:
            if self.simulator.should_drop():
                return True  # pretend success, packet silently lost
            delay = self.simulator.get_delay()
            if delay > 0:
                time.sleep(delay)
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(SOCK_TIMEOUT)
                s.connect((target_ip, self.port))
                s.sendall(data)
            return True
        except Exception:
            return False

    # ================================================================
    #  Discovery
    # ================================================================

    def broadcast_ask(self) -> None:
        """Send ASK via UDP broadcast."""
        data = pack_ask(self.local_ip)
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                s.bind((self.local_ip, 0))
                try:
                    s.sendto(data, ("<broadcast>", self.port))
                except OSError:
                    subnet = ".".join(self.local_ip.split(".")[:3]) + ".255"
                    s.sendto(data, (subnet, self.port))
        except OSError:
            pass

    def discover_peers(self) -> dict:
        self._safe_print(f"[*] Broadcasting discovery on port {self.port}...")
        self.broadcast_ask()
        self._safe_print(f"[*] Waiting {DISCOVERY_WAIT}s for replies...")
        time.sleep(DISCOVERY_WAIT)
        with self.peers_lock:
            return {ip: n for ip, n in self.peers.items()
                    if ip != self.local_ip}

    def _handle_ask(self, sender_ip: str) -> None:
        if sender_ip == self.local_ip:
            return
        # Send REPLY via UDP (fast, no handshake)
        reply = pack_reply(self.local_ip, self.name)
        self._send_udp_raw(sender_ip, reply)
        # Register peer
        is_new = False
        with self.peers_lock:
            if sender_ip not in self.peers:
                self.peers[sender_ip] = f"peer@{sender_ip}"
                is_new = True
        if is_new:
            self.telemetry.register_peer(sender_ip)
            self._safe_print(
                f"[*] New device detected at {sender_ip}", prompt=True
            )

    def _handle_reply(self, sender_ip: str, name: str) -> None:
        is_new = False
        with self.peers_lock:
            old = self.peers.get(sender_ip, "")
            if not old or old.startswith("peer@"):
                is_new = True
            self.peers[sender_ip] = name
        if is_new:
            self.telemetry.register_peer(sender_ip)
            self._safe_print(
                f"[*] {name} ({sender_ip}) is online.", prompt=True
            )

    # ================================================================
    #  Listeners
    # ================================================================

    def _tcp_listener(self) -> None:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            server.bind(("", self.port))
            server.listen()
        except OSError as e:
            print(f"[ERROR] Cannot bind TCP port {self.port}: {e}")
            sys.exit(1)

        while self._running:
            try:
                conn, addr = server.accept()
                threading.Thread(
                    target=self._handle_tcp_conn,
                    args=(conn, addr),
                    daemon=True,
                ).start()
            except OSError:
                time.sleep(0.5)

    def _handle_tcp_conn(self, conn: socket.socket, addr: tuple) -> None:
        try:
            chunks = []
            conn.settimeout(SOCK_TIMEOUT)
            while True:
                chunk = conn.recv(4096)
                if not chunk:
                    break
                chunks.append(chunk)
            raw = b"".join(chunks)
        except (OSError, socket.timeout):
            raw = b""
        finally:
            conn.close()

        if len(raw) < HEADER_SIZE:
            return

        try:
            pkt_type, _, flags, payload = unpack_header(raw)
        except ValueError:
            return

        sender_ip = addr[0]

        if pkt_type == PKT_ASK:
            self._handle_ask(unpack_ask(payload))
        elif pkt_type == PKT_REPLY:
            ip, name = unpack_reply(payload)
            self._handle_reply(ip, name)
        elif pkt_type == PKT_MESSAGE:
            text = unpack_message(payload)
            with self.peers_lock:
                name = self.peers.get(sender_ip, sender_ip)
            self._safe_print(f"[{name}]: {text}", prompt=True)
        elif pkt_type == PKT_SOS:
            text = unpack_sos(payload)
            with self.peers_lock:
                name = self.peers.get(sender_ip, sender_ip)
            self._display_sos(name, sender_ip, text)
        elif pkt_type == PKT_LOCATION:
            lat, lon = unpack_location(payload)
            with self.peers_lock:
                name = self.peers.get(sender_ip, sender_ip)
            self._display_location(name, sender_ip, lat, lon)

    def _udp_listener(self) -> None:
        while self._running:
            try:
                data, addr = self._udp_sock.recvfrom(65535)
                sender_ip = addr[0]
            except OSError:
                continue

            if len(data) < HEADER_SIZE:
                continue

            try:
                pkt_type, _, flags, payload = unpack_header(data)
            except ValueError:
                continue

            # ---- Dispatch by type ----

            if pkt_type == PKT_HEARTBEAT_REQ:
                ts_ns = unpack_heartbeat(payload)
                ack = self.telemetry.handle_heartbeat_req(sender_ip, ts_ns)
                self._send_udp_raw(sender_ip, ack)

            elif pkt_type == PKT_HEARTBEAT_ACK:
                ts_ns = unpack_heartbeat(payload)
                self.telemetry.handle_heartbeat_ack(sender_ip, ts_ns)

            elif pkt_type == PKT_ASK:
                self._handle_ask(unpack_ask(payload))

            elif pkt_type == PKT_REPLY:
                ip, name = unpack_reply(payload)
                self._handle_reply(ip, name)

            elif pkt_type == PKT_DATA:
                seq, chunk = unpack_data(payload)
                receiver = self._get_or_create_receiver(sender_ip)
                ack_pkt = receiver.handle_data(seq, chunk, flags)
                self._send_udp_raw(sender_ip, ack_pkt)

            elif pkt_type == PKT_DATA_ACK:
                acked_seq, rwnd, nacks = unpack_data_ack(payload)
                sender_obj = self._senders.get(sender_ip)
                if sender_obj:
                    sender_obj.handle_ack(acked_seq, rwnd, nacks)

            elif pkt_type == PKT_SOS:
                text = unpack_sos(payload)
                with self.peers_lock:
                    name = self.peers.get(sender_ip, sender_ip)
                self._display_sos(name, sender_ip, text)

            elif pkt_type == PKT_LOCATION:
                lat, lon = unpack_location(payload)
                with self.peers_lock:
                    name = self.peers.get(sender_ip, sender_ip)
                self._display_location(name, sender_ip, lat, lon)

            elif pkt_type == PKT_MESSAGE:
                text = unpack_message(payload)
                with self.peers_lock:
                    name = self.peers.get(sender_ip, sender_ip)
                self._safe_print(f"[{name}]: {text}", prompt=True)

    # ================================================================
    #  Per-peer reliable transport
    # ================================================================

    def _get_or_create_sender(self, ip: str) -> ReliableUDPSender:
        if ip not in self._senders:
            sender = ReliableUDPSender(
                send_func=self._make_udp_sender(ip),
                window_size=10,
                base_timeout=0.5,
                max_timeout=5.0,
                simulator=self.simulator,
            )
            sender.start()
            self._senders[ip] = sender
        return self._senders[ip]

    def _get_or_create_receiver(self, ip: str) -> ReliableUDPReceiver:
        if ip not in self._receivers:
            self._receivers[ip] = ReliableUDPReceiver(
                on_deliver=lambda seq, data, flags, _ip=ip:
                    self._on_data_deliver(seq, data, flags, _ip),
                max_rwnd=10,
            )
        return self._receivers[ip]

    def _on_data_deliver(self, seq: int, data: bytes, flags: int,
                         sender_ip: str) -> None:
        """Called when reliable UDP delivers a packet in-order.

        The 'data' is the inner packet (complete with its own 5-byte header)
        that was wrapped inside the DATA frame for reliable delivery.
        """
        if len(data) < HEADER_SIZE:
            return

        try:
            inner_type, _, inner_flags, inner_payload = unpack_header(data)
        except ValueError:
            return

        with self.peers_lock:
            name = self.peers.get(sender_ip, sender_ip)

        if inner_type == PKT_MESSAGE:
            text = unpack_message(inner_payload)
            self._safe_print(f"[{name}]: {text}", prompt=True)

        elif inner_type == PKT_SOS:
            text = unpack_sos(inner_payload)
            self._display_sos(name, sender_ip, text)

        elif inner_type == PKT_LOCATION:
            lat, lon = unpack_location(inner_payload)
            self._display_location(name, sender_ip, lat, lon)

        elif inner_type == PKT_FILE_META:
            fname, total, size = unpack_file_meta(inner_payload)
            self._incoming_files[sender_ip] = _IncomingFile(fname, total, size)
            self._safe_print(
                f"[*] Incoming file from {name}: '{fname}' "
                f"({size} bytes, {total} chunks)",
                prompt=True,
            )

        elif inner_type == PKT_FILE_CHUNK:
            chunk_seq, chunk_data = unpack_file_chunk(inner_payload)
            incoming = self._incoming_files.get(sender_ip)
            if incoming:
                incoming.chunks[chunk_seq] = chunk_data
                if inner_flags & FLAG_EOF:
                    self._save_incoming_file(incoming, name, sender_ip)

    def _save_incoming_file(self, incoming: _IncomingFile,
                            sender_name: str, sender_ip: str) -> None:
        """Reconstruct and save a fully received file."""
        save_name = f"received_{incoming.filename}"
        try:
            with open(save_name, "wb") as f:
                for i in range(1, incoming.total_chunks + 1):
                    chunk = incoming.chunks.get(i, b"")
                    f.write(chunk)
            self._safe_print(
                f"[*] File '{incoming.filename}' from {sender_name} "
                f"saved as '{save_name}'",
                prompt=True,
            )
        except Exception as e:
            self._safe_print(f"[!] Failed to save file: {e}", prompt=True)
        finally:
            self._incoming_files.pop(sender_ip, None)

    # ================================================================
    #  Display helpers
    # ================================================================

    def _display_sos(self, name: str, ip: str, text: str) -> None:
        self._safe_print(
            f"\n{'=' * 50}\n"
            f"  [!!!] SOS from {name} ({ip})\n"
            f"  {text}\n"
            f"{'=' * 50}",
            prompt=True,
        )

    def _display_location(self, name: str, ip: str,
                          lat: float, lon: float) -> None:
        self._safe_print(
            f"[LOCATION] {name} ({ip}): "
            f"lat={lat:.6f}, lon={lon:.6f}",
            prompt=True,
        )

    # ================================================================
    #  Message / SOS / Location sending
    # ================================================================

    def send_message(self, text: str, target_ip: str) -> bool:
        """Send a regular message. Route depends on current mode."""
        if self.mode == NetworkMode.NORMAL:
            pkt = pack_message(text)
            return self._send_tcp(target_ip, pkt)
        else:
            # CRISIS: wrap in reliable UDP
            inner = pack_message(text)
            sender = self._get_or_create_sender(target_ip)
            sender.send(inner)
            return True

    def send_sos(self, text: str, target_ip: str) -> None:
        """Send SOS message with highest priority."""
        sos_pkt = pack_sos(text)

        if self.mode == NetworkMode.CRISIS:
            # Priority bypass: inject directly into reliable sender
            sender = self._get_or_create_sender(target_ip)
            sender.send(sos_pkt, priority=True)
            self._safe_print(f"[SOS SENT] (CRISIS - priority bypass): {text}")
        else:
            # NORMAL: send via TCP
            if self._send_tcp(target_ip, sos_pkt):
                self._safe_print(f"[SOS SENT]: {text}")
            else:
                self._safe_print(f"[!] SOS delivery failed to {target_ip}")

    def send_location(self, lat: float, lon: float, target_ip: str) -> None:
        """Broadcast GPS coordinates."""
        loc_pkt = pack_location(lat, lon)

        if self.mode == NetworkMode.CRISIS:
            sender = self._get_or_create_sender(target_ip)
            sender.send(loc_pkt, priority=True)
            self._safe_print(
                f"[LOCATION SENT] (CRISIS - priority): "
                f"lat={lat:.6f}, lon={lon:.6f}"
            )
        else:
            if self._send_tcp(target_ip, loc_pkt):
                self._safe_print(
                    f"[LOCATION SENT]: lat={lat:.6f}, lon={lon:.6f}"
                )
            else:
                self._safe_print(
                    f"[!] Location delivery failed to {target_ip}"
                )

    def send_file(self, target_ip: str, filepath: str) -> None:
        """Send a file via reliable UDP (always UDP, regardless of mode)."""
        try:
            with open(filepath, "rb") as f:
                file_data = f.read()
        except FileNotFoundError:
            self._safe_print(f"[!] File '{filepath}' not found.")
            return
        except PermissionError:
            self._safe_print(f"[!] Permission denied: '{filepath}'")
            return

        filename = os.path.basename(filepath)
        CHUNK_SIZE = 1400  # below typical MTU
        chunks = [file_data[i:i + CHUNK_SIZE]
                  for i in range(0, len(file_data), CHUNK_SIZE)]
        if not chunks:
            chunks = [b""]

        total = len(chunks)
        sender = self._get_or_create_sender(target_ip)

        # Announce file transfer
        meta = pack_file_meta(filename, total, len(file_data))
        sender.send(meta)

        self._safe_print(
            f"[*] Sending '{filename}' ({len(file_data)} bytes, "
            f"{total} chunks) to {target_ip}..."
        )

        start = time.time()
        for i, chunk in enumerate(chunks):
            is_eof = (i == total - 1)
            inner = pack_file_chunk(i + 1, chunk, is_eof)
            sender.send(inner)

        # Wait for all ACKs
        sender.wait_complete(timeout=60.0)
        duration = time.time() - start
        if duration > 0:
            throughput = (len(file_data) * 8 / duration) / 1_000_000
        else:
            throughput = 0.0

        self._safe_print(
            f"[*] File '{filename}' sent! "
            f"Time: {duration:.2f}s | Throughput: {throughput:.2f} Mbps"
        )

    # ================================================================
    #  State machine (protocol pivot)
    # ================================================================

    def _on_metrics_update(self, ip: str, rtt: float, loss: float) -> None:
        """Called by telemetry after each heartbeat ACK.
        Only check pivot for the active target peer.
        """
        if ip != self.target_ip:
            return

        if self.mode == NetworkMode.NORMAL:
            if loss > LOSS_THRESHOLD or rtt > RTT_THRESHOLD:
                self._pivot_to_crisis(rtt, loss)
        elif self.mode == NetworkMode.CRISIS:
            if loss < RECOVERY_LOSS and rtt < RECOVERY_RTT:
                self._pivot_to_normal(rtt, loss)

    def _pivot_to_crisis(self, rtt: float, loss: float) -> None:
        self.mode = NetworkMode.CRISIS
        self._safe_print(
            f"\n{'=' * 50}\n"
            f"  [PIVOT] NORMAL --> CRISIS (Reliable UDP)\n"
            f"  RTT: {rtt:.0f}ms | Loss: {loss * 100:.1f}%\n"
            f"  All traffic now via Reliable UDP\n"
            f"{'=' * 50}",
            prompt=True,
        )

    def _pivot_to_normal(self, rtt: float, loss: float) -> None:
        self.mode = NetworkMode.NORMAL
        self._safe_print(
            f"\n{'=' * 50}\n"
            f"  [RECOVERY] CRISIS --> NORMAL (TCP)\n"
            f"  RTT: {rtt:.0f}ms | Loss: {loss * 100:.1f}%\n"
            f"  Traffic restored to TCP\n"
            f"{'=' * 50}",
            prompt=True,
        )

    # ================================================================
    #  Periodic broadcast
    # ================================================================

    def _heartbeat_loop(self) -> None:
        while self._running:
            time.sleep(HEARTBEAT_BCAST)
            self.broadcast_ask()

    # ================================================================
    #  Status
    # ================================================================

    def get_status(self) -> str:
        lines = [
            f"Mode    : {self.mode.value}",
            f"Local   : {self.name} ({self.local_ip}:{self.port})",
        ]
        if self.target_ip:
            lines.append(f"Target  : {self.target_name} ({self.target_ip})")
            rtt, loss = self.telemetry.get_metrics(self.target_ip)
            lines.append(f"  RTT={rtt:.1f}ms | Loss={loss * 100:.1f}%")

        all_metrics = self.telemetry.get_all_metrics()
        if all_metrics:
            lines.append("Peers:")
            for ip, (rtt, loss) in all_metrics.items():
                with self.peers_lock:
                    name = self.peers.get(ip, ip)
                marker = " <-- target" if ip == self.target_ip else ""
                lines.append(
                    f"  {name} ({ip}): "
                    f"RTT={rtt:.1f}ms Loss={loss * 100:.1f}%{marker}"
                )

        lines.append("")
        lines.append(self.simulator.get_status())
        return "\n".join(lines)
