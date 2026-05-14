#!/usr/bin/env python3
"""
main.py - ResilienceFlow entry point and interactive CLI.

Adaptive Protocol Pivoting and Reliable UDP for Congested Emergency Networks.

Run:  python3 main.py
"""

import sys

from node import ResilienceNode, NetworkMode


# ---------------------------------------------------------------------------
# Peer selection (same UX as Workshop 4)
# ---------------------------------------------------------------------------

def select_peer(discovered: dict) -> tuple:
    """Interactive peer picker. Returns (ip, name)."""
    peer_list = list(discovered.items())
    print("\n[*] Discovered peers:")
    for i, (ip, name) in enumerate(peer_list):
        print(f"  {i + 1}. {name} ({ip})")

    while True:
        try:
            idx = int(input("\nSelect peer number: ").strip()) - 1
            if 0 <= idx < len(peer_list):
                return peer_list[idx]
            print(f"[!] Enter a number between 1 and {len(peer_list)}.")
        except ValueError:
            print("[!] Please enter a valid number.")
        except (EOFError, KeyboardInterrupt):
            print("\n[*] Exiting.")
            sys.exit(0)


# ---------------------------------------------------------------------------
# Help text
# ---------------------------------------------------------------------------

HELP_TEXT = """
Commands:
  /scan                  - Re-discover peers on the network
  /list                  - Show all known peers with metrics
  /switch                - Change chat target
  /status                - Show mode, RTT, loss, simulator state
  /sos <message>         - Send emergency SOS (highest priority)
  /location <lat> <lon>  - Broadcast GPS coordinates
  /sendfile <path>       - Send a file via reliable UDP
  /simulate loss <0-100> - Set simulated packet loss %
  /simulate delay <ms>   - Set simulated extra latency (ms)
  /simulate reset        - Disable simulation
  /simulate status       - Show simulator statistics
  /help                  - Show this help
  /quit                  - Exit
"""


# ---------------------------------------------------------------------------
# Command shell
# ---------------------------------------------------------------------------

def command_shell(node: ResilienceNode) -> None:
    """Interactive command loop."""

    # Initial discovery
    discovered = node.discover_peers()
    if discovered:
        node.target_ip, node.target_name = select_peer(discovered)
        print(f"\n[*] Now chatting with {node.target_name} ({node.target_ip})")
    else:
        print("\n[*] No peers found yet. Use /scan to discover, "
              "or wait for incoming connections.")

    print(HELP_TEXT)

    while True:
        try:
            user_input = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n[*] Exiting.")
            break

        if not user_input:
            continue

        # ---- Commands ----

        if user_input == "/quit":
            print("[*] Exiting.")
            break

        elif user_input == "/help":
            print(HELP_TEXT)

        elif user_input == "/scan":
            node.discover_peers()
            with node.peers_lock:
                all_peers = {ip: n for ip, n in node.peers.items()
                             if ip != node.local_ip}
            if all_peers:
                print("[*] Known peers after scan:")
                for ip, n in all_peers.items():
                    marker = "  <-- current" if ip == node.target_ip else ""
                    print(f"  {n} ({ip}){marker}")
            else:
                print("[!] No peers found.")

        elif user_input == "/list":
            with node.peers_lock:
                all_peers = {ip: n for ip, n in node.peers.items()
                             if ip != node.local_ip}
            if all_peers:
                metrics = node.telemetry.get_all_metrics()
                print("[*] Known peers:")
                for ip, n in all_peers.items():
                    rtt, loss = metrics.get(ip, (0.0, 0.0))
                    marker = "  <-- current" if ip == node.target_ip else ""
                    print(f"  {n} ({ip}) "
                          f"RTT={rtt:.1f}ms Loss={loss * 100:.1f}%{marker}")
            else:
                print("[!] No known peers yet. Try /scan.")

        elif user_input == "/switch":
            with node.peers_lock:
                all_peers = {ip: n for ip, n in node.peers.items()
                             if ip != node.local_ip}
            if not all_peers:
                print("[!] No peers known. Try /scan first.")
            else:
                node.target_ip, node.target_name = select_peer(all_peers)
                print(f"[*] Switched to {node.target_name} ({node.target_ip})")

        elif user_input == "/status":
            print(node.get_status())

        elif user_input.startswith("/sos "):
            text = user_input[5:].strip()
            if not text:
                print("[!] Usage: /sos <emergency message>")
                continue
            if node.target_ip is None:
                print("[!] No target selected. Use /switch first.")
                continue
            node.send_sos(text, node.target_ip)

        elif user_input.startswith("/location "):
            parts = user_input.split()
            if len(parts) != 3:
                print("[!] Usage: /location <latitude> <longitude>")
                continue
            try:
                lat = float(parts[1])
                lon = float(parts[2])
            except ValueError:
                print("[!] Invalid coordinates. Example: /location 40.9869 29.0259")
                continue
            if node.target_ip is None:
                print("[!] No target selected. Use /switch first.")
                continue
            node.send_location(lat, lon, node.target_ip)

        elif user_input.startswith("/sendfile "):
            filepath = user_input[10:].strip()
            if not filepath:
                print("[!] Usage: /sendfile <path_to_file>")
                continue
            if node.target_ip is None:
                print("[!] No target selected. Use /switch first.")
                continue
            node.send_file(node.target_ip, filepath)

        elif user_input.startswith("/simulate"):
            _handle_simulate(node, user_input)

        elif user_input.startswith("/"):
            print(f"[!] Unknown command: {user_input.split()[0]}")
            print("    Type /help for available commands.")

        else:
            # Regular message
            if node.target_ip is None:
                print("[!] No target selected. Use /switch to pick a peer.")
                continue

            encoded = user_input.encode("utf-8", errors="replace")
            if len(encoded) > 2048:
                print(f"[!] Message too long ({len(encoded)} bytes, max 2048).")
                continue

            mode_tag = f"[{node.mode.value}]"
            if not node.send_message(user_input, node.target_ip):
                print(f"[!] {mode_tag} Failed to reach "
                      f"{node.target_name} ({node.target_ip}).")


def _handle_simulate(node: ResilienceNode, cmd: str) -> None:
    """Parse and execute /simulate sub-commands."""
    parts = cmd.split()

    if len(parts) == 1:
        print("[!] Usage: /simulate loss|delay|reset|status [value]")
        return

    sub = parts[1]

    if sub == "loss":
        if len(parts) < 3:
            print("[!] Usage: /simulate loss <0-100>")
            return
        try:
            pct = float(parts[2])
            node.simulator.set_loss(pct / 100.0)
            print(f"[SIM] Packet loss set to {pct:.1f}%")
        except ValueError:
            print("[!] Invalid percentage.")

    elif sub == "delay":
        if len(parts) < 3:
            print("[!] Usage: /simulate delay <ms>")
            return
        try:
            ms = float(parts[2])
            node.simulator.set_delay(ms)
            print(f"[SIM] Extra delay set to {ms:.0f}ms")
        except ValueError:
            print("[!] Invalid delay value.")

    elif sub == "reset":
        node.simulator.reset()
        print("[SIM] Simulation disabled. All stats cleared.")

    elif sub == "status":
        print(node.simulator.get_status())

    else:
        print(f"[!] Unknown simulate command: {sub}")
        print("    Options: loss, delay, reset, status")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    print("=" * 55)
    print("  ResilienceFlow")
    print("  Adaptive Protocol Pivoting for Emergency Networks")
    print("=" * 55)

    node = ResilienceNode.__new__(ResilienceNode)

    # Get local IP first
    try:
        local_ip = ResilienceNode._get_local_ip()
    except Exception:
        print("[ERROR] Cannot determine local IP. Are you connected?")
        sys.exit(1)

    print(f"\n[*] Your IP: {local_ip}")

    # Get name
    while True:
        try:
            name = input("Enter your name: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n[*] Exiting.")
            sys.exit(0)
        if name:
            break
        print("[!] Name cannot be empty.")

    # Initialize and start node
    node = ResilienceNode(name)
    node.start()

    try:
        command_shell(node)
    finally:
        node.stop()
        print("[*] Goodbye.")


if __name__ == "__main__":
    main()
