# ResilienceFlow

**Adaptive Protocol Pivoting and Reliable UDP for Congested Emergency Networks**

CMPE 487 — Final Project

---

## Overview

ResilienceFlow is a peer-to-peer communication system designed for disaster scenarios where network congestion renders standard TCP unusable. It builds on Workshop 4's LAN chat by adding:

1. **Real-Time Network Telemetry** — Background heartbeat probes with EMA-based RTT and packet-loss tracking.
2. **Dynamic Protocol Pivot** — Automatic state-machine transition from TCP (NORMAL) to Reliable UDP (CRISIS) when loss > 30% or RTT > 500 ms, with hysteresis for recovery.
3. **Custom Reliable UDP** — Struct-packed 5-byte binary headers (no JSON), Selective-Repeat ARQ with per-packet NACKs, and adaptive retransmission timeouts.
4. **QoS & Prioritization** — `/sos` and `/location` commands that bypass normal queues in CRISIS mode for immediate, aggressive delivery.

---

## Prerequisites

- Python 3.10+
- No external dependencies — stdlib only

---

## Quick Start

On **each machine** (same LAN):

```bash
python3 main.py
```

1. Enter your name.
2. Peers are discovered via UDP broadcast on port 12487.
3. Select a peer and start chatting.
4. Use `/simulate loss 40` on one side to trigger the protocol pivot demo.

---

## Architecture

```
main.py          Entry point + CLI shell
node.py          ResilienceNode orchestrator, state machine (NORMAL/CRISIS)
protocol.py      Binary packet format (struct-packed, 5-byte header)
telemetry.py     Heartbeat probes, RTT/loss EMA calculation
transport.py     Reliable UDP: Selective-Repeat ARQ, sliding window
simulator.py     Application-layer packet loss & delay injection
chat.py          Original Workshop 4 code (kept for reference)
```

### State Machine

```
                loss > 30% OR rtt > 500ms
  NORMAL (TCP) ──────────────────────────> CRISIS (Reliable UDP)
       <──────────────────────────────────
                loss < 15% AND rtt < 250ms
```

### Binary Protocol Header (5 bytes)

```
Offset  Size  Field        Notes
0       1     version      Always 0x01
1       1     pkt_type     Packet type identifier
2       2     payload_len  Big-endian uint16
4       1     flags        Bitfield: PRIORITY|SOS|LOCATION|EOF
```

Compared to Workshop 4's JSON packets, this gives ~10x smaller headers.

---

## CLI Commands

| Command | Description |
|---|---|
| `/scan` | Re-discover peers on the network |
| `/list` | Show all known peers with RTT/loss metrics |
| `/switch` | Change chat target |
| `/status` | Show mode, metrics, simulator state |
| `/sos <message>` | Send emergency SOS (highest priority in CRISIS) |
| `/location <lat> <lon>` | Broadcast GPS coordinates |
| `/sendfile <path>` | Send a file via reliable UDP |
| `/simulate loss <0-100>` | Set simulated packet loss percentage |
| `/simulate delay <ms>` | Set simulated extra latency |
| `/simulate reset` | Disable all simulation |
| `/simulate status` | Show simulator statistics |
| `/help` | Show command help |
| `/quit` | Exit |

---

## Demo Scenario

Simulating the "Golden Hours" earthquake scenario from the proposal:

```
# Terminal 1 (Command Center)              # Terminal 2 (Medical Team)
python3 main.py                            python3 main.py
> Name: CommandCenter                      > Name: MedTeam
> Select peer: 1                           > Select peer: 1

# Start with normal chat (TCP)
Hello, status report?                      All stable for now.

# Simulate network congestion on Terminal 2
                                           /simulate loss 40

# Wait ~6 seconds for telemetry to detect degradation
# Both nodes will show:
#   [PIVOT] NORMAL --> CRISIS (Reliable UDP)

# Demonstrate SOS priority bypass under heavy traffic:
# Step 1 — create congestion with a large file transfer
                                           /sendfile test_1mb.bin

# Step 2 — while the file is in flight, send a regular message
                                           Sending supplies now.
# (This message does NOT appear on Terminal 1 yet —
#  it is queued behind the file chunks in the sliding window)

# Step 3 — send an SOS during the same transfer
                                           /sos Need tourniquets and hemostatic agents!
# (This appears on Terminal 1 IMMEDIATELY, before the file finishes —
#  SOS bypasses the window via the priority queue)

# Step 4 — once the file transfer completes, the earlier regular
#           message finally arrives on Terminal 1.

# Send coordinates
                                           /location 40.9869 29.0259

# Disable simulation to recover
                                           /simulate reset

# After ~6 seconds:
#   [RECOVERY] CRISIS --> NORMAL (TCP)
```

---

## Key Improvements over Workshop 4

| Feature | Workshop 4 | ResilienceFlow |
|---|---|---|
| Packet format | JSON (verbose) | struct-packed binary (5-byte header) |
| Reliability | Simple retransmit-all on timeout | Selective-Repeat ARQ with NACKs |
| Protocol | Fixed TCP chat + UDP file | Adaptive TCP/UDP pivot |
| Network monitoring | None | EMA-based RTT & loss telemetry |
| Emergency priority | None | /sos and /location with queue bypass |
| Simulation | None | Built-in loss/delay injection |

---

## Challenges We Overcame

**Compounding packet loss when simulating on both sides.**
Our initial approach was to run the loss simulator on both peers simultaneously, assuming each side would independently experience the configured drop rate. However, because every packet in our protocol (data chunks, ACKs, NACKs, heartbeats) must survive two independent probabilistic drop decisions — one on the sender and one on the receiver — the effective end-to-end loss was far higher than intended. For example, setting 50% loss on both sides resulted in an observed loss closer to 75%, because a packet had to get through both coin-flips to count as delivered. This caused `sendfile` to time out even on small files, since ACKs were being dropped just as aggressively as data packets. We resolved this by applying the simulator on one side only, which kept the observed loss in line with the configured value and made the demo reproducible.
