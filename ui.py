#!/usr/bin/env python3
"""
ui.py - Simple Tkinter UI for ResilienceFlow.

This file is intentionally separate from main.py so the existing terminal
application keeps its current behavior unchanged.
"""

import queue
import threading
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from node import ResilienceNode


class ResilienceFlowUI:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("ResilienceFlow")
        self.root.geometry("980x680")
        self.root.minsize(820, 560)

        self.node: ResilienceNode | None = None
        self.log_queue: queue.Queue[str] = queue.Queue()
        self.peer_ips: list[str] = []

        self.name_var = tk.StringVar()
        self.port_var = tk.StringVar(value="12487")
        self.message_var = tk.StringVar()
        self.sos_var = tk.StringVar()
        self.lat_var = tk.StringVar()
        self.lon_var = tk.StringVar()
        self.file_var = tk.StringVar()
        self.loss_var = tk.StringVar(value="40")
        self.delay_var = tk.StringVar(value="200")
        self.status_var = tk.StringVar(value="Not started")

        self._build_ui()
        self._poll_logs()
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _build_ui(self) -> None:
        self.root.columnconfigure(0, weight=0)
        self.root.columnconfigure(1, weight=1)
        self.root.rowconfigure(1, weight=1)

        top = ttk.Frame(self.root, padding=10)
        top.grid(row=0, column=0, columnspan=2, sticky="ew")
        top.columnconfigure(7, weight=1)

        ttk.Label(top, text="Name").grid(row=0, column=0, padx=(0, 6))
        ttk.Entry(top, textvariable=self.name_var, width=18).grid(row=0, column=1)
        ttk.Label(top, text="Port").grid(row=0, column=2, padx=(12, 6))
        ttk.Entry(top, textvariable=self.port_var, width=8).grid(row=0, column=3)
        ttk.Button(top, text="Start", command=self._start_node).grid(
            row=0, column=4, padx=(12, 0)
        )
        ttk.Button(top, text="Quit", command=self._on_close).grid(
            row=0, column=5, padx=(6, 0)
        )
        ttk.Label(top, textvariable=self.status_var).grid(
            row=0, column=7, sticky="e"
        )

        left = ttk.Frame(self.root, padding=(10, 0, 6, 10))
        left.grid(row=1, column=0, sticky="ns")
        left.rowconfigure(1, weight=1)

        ttk.Label(left, text="Peers").grid(row=0, column=0, sticky="w")
        self.peer_list = tk.Listbox(left, width=34, height=18)
        self.peer_list.grid(row=1, column=0, sticky="ns")

        peer_buttons = ttk.Frame(left)
        peer_buttons.grid(row=2, column=0, sticky="ew", pady=(8, 0))
        peer_buttons.columnconfigure((0, 1), weight=1)
        ttk.Button(peer_buttons, text="Scan", command=self._scan_peers).grid(
            row=0, column=0, sticky="ew", padx=(0, 4)
        )
        ttk.Button(peer_buttons, text="Switch", command=self._switch_peer).grid(
            row=0, column=1, sticky="ew", padx=(4, 0)
        )

        utility = ttk.Frame(left)
        utility.grid(row=3, column=0, sticky="ew", pady=(8, 0))
        utility.columnconfigure((0, 1), weight=1)
        ttk.Button(utility, text="List", command=self._show_list).grid(
            row=0, column=0, sticky="ew", padx=(0, 4)
        )
        ttk.Button(utility, text="Status", command=self._show_status).grid(
            row=0, column=1, sticky="ew", padx=(4, 0)
        )

        right = ttk.Frame(self.root, padding=(6, 0, 10, 10))
        right.grid(row=1, column=1, sticky="nsew")
        right.columnconfigure(0, weight=1)
        right.rowconfigure(1, weight=1)

        commands = ttk.LabelFrame(right, text="Commands", padding=10)
        commands.grid(row=0, column=0, sticky="ew")
        commands.columnconfigure(1, weight=1)
        commands.columnconfigure(2, weight=1)
        commands.columnconfigure(3, weight=1)

        self._command_row(commands, 0, "Message", self._send_message,
                          [("Text", self.message_var)])
        self._command_row(commands, 1, "SOS", self._send_sos,
                          [("Message", self.sos_var)])
        self._command_row(commands, 2, "Location", self._send_location,
                          [("Latitude", self.lat_var), ("Longitude", self.lon_var)])
        self._command_row(commands, 3, "Send File", self._send_file,
                          [("Path", self.file_var)], browse=True)
        self._command_row(commands, 4, "Sim Loss", self._simulate_loss,
                          [("Percent", self.loss_var)])
        self._command_row(commands, 5, "Sim Delay", self._simulate_delay,
                          [("Milliseconds", self.delay_var)])

        sim_buttons = ttk.Frame(commands)
        sim_buttons.grid(row=6, column=0, columnspan=4, sticky="ew", pady=(8, 0))
        sim_buttons.columnconfigure((0, 1, 2), weight=1)
        ttk.Button(sim_buttons, text="Sim Reset", command=self._simulate_reset).grid(
            row=0, column=0, sticky="ew", padx=(0, 4)
        )
        ttk.Button(sim_buttons, text="Sim Status", command=self._simulate_status).grid(
            row=0, column=1, sticky="ew", padx=4
        )
        ttk.Button(sim_buttons, text="Help", command=self._show_help).grid(
            row=0, column=2, sticky="ew", padx=(4, 0)
        )

        log_frame = ttk.LabelFrame(right, text="Output", padding=8)
        log_frame.grid(row=1, column=0, sticky="nsew", pady=(10, 0))
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)

        self.output = tk.Text(log_frame, wrap="word", state="disabled", height=18)
        self.output.grid(row=0, column=0, sticky="nsew")
        scroll = ttk.Scrollbar(log_frame, orient="vertical", command=self.output.yview)
        scroll.grid(row=0, column=1, sticky="ns")
        self.output.configure(yscrollcommand=scroll.set)

    def _command_row(self, parent, row, label, command, fields, browse=False) -> None:
        ttk.Button(parent, text=label, command=command).grid(
            row=row, column=0, sticky="ew", pady=3, padx=(0, 8)
        )
        for idx, (placeholder, var) in enumerate(fields, start=1):
            field = ttk.Frame(parent)
            field.grid(row=row, column=idx, sticky="ew", pady=3, padx=(0, 8))
            field.columnconfigure(0, weight=1)
            ttk.Label(field, text=placeholder).grid(row=0, column=0, sticky="w")
            ttk.Entry(field, textvariable=var).grid(row=1, column=0, sticky="ew")
        if browse:
            ttk.Button(parent, text="Browse", command=self._browse_file).grid(
                row=row, column=3, sticky="ew", pady=3
            )

    # ------------------------------------------------------------------
    # Node setup and helpers
    # ------------------------------------------------------------------

    def _start_node(self) -> None:
        if self.node:
            self._log("[!] Node is already running.")
            return

        name = self.name_var.get().strip()
        if not name:
            messagebox.showerror("Missing name", "Enter a name before starting.")
            return

        try:
            port = int(self.port_var.get().strip())
        except ValueError:
            messagebox.showerror("Invalid port", "Port must be a number.")
            return

        def worker() -> None:
            try:
                self.node = ResilienceNode(name, port)
                original_print = self.node._safe_print

                def ui_print(msg: str, prompt: bool = False) -> None:
                    original_print(msg, prompt=False)
                    self._log(msg)

                self.node._safe_print = ui_print
                self.node.start()
                self.root.after(0, self.status_var.set, f"Running as {name}")
                self._scan_peers()
            except Exception as exc:
                self._log(f"[ERROR] Failed to start node: {exc}")
                self.node = None
                self.root.after(0, self.status_var.set, "Not started")

        threading.Thread(target=worker, daemon=True).start()

    def _require_node(self) -> ResilienceNode | None:
        if not self.node:
            self._log("[!] Start the node first.")
            return None
        return self.node

    def _require_target(self) -> str | None:
        node = self._require_node()
        if not node:
            return None
        if not node.target_ip:
            self._log("[!] Select a peer and click Switch first.")
            return None
        return node.target_ip

    def _run_async(self, func) -> None:
        threading.Thread(target=func, daemon=True).start()

    # ------------------------------------------------------------------
    # Command actions
    # ------------------------------------------------------------------

    def _scan_peers(self) -> None:
        node = self._require_node()
        if not node:
            return

        def worker() -> None:
            node.discover_peers()
            self._refresh_peers()

        self._run_async(worker)

    def _refresh_peers(self) -> None:
        node = self._require_node()
        if not node:
            return
        with node.peers_lock:
            peers = [(ip, name) for ip, name in node.peers.items()
                     if ip != node.local_ip]

        self.peer_ips = [ip for ip, _ in peers]
        self.root.after(0, self._replace_peer_list, peers)

    def _replace_peer_list(self, peers) -> None:
        self.peer_list.delete(0, tk.END)
        for ip, name in peers:
            marker = " *" if self.node and ip == self.node.target_ip else ""
            self.peer_list.insert(tk.END, f"{name} ({ip}){marker}")

    def _switch_peer(self) -> None:
        node = self._require_node()
        if not node:
            return
        selection = self.peer_list.curselection()
        if not selection:
            self._log("[!] Select a peer from the list first.")
            return
        ip = self.peer_ips[selection[0]]
        with node.peers_lock:
            name = node.peers.get(ip, ip)
        node.target_ip = ip
        node.target_name = name
        self._log(f"[*] Switched to {name} ({ip})")
        self._refresh_peers()

    def _show_list(self) -> None:
        node = self._require_node()
        if not node:
            return
        with node.peers_lock:
            peers = {ip: n for ip, n in node.peers.items()
                     if ip != node.local_ip}
        if not peers:
            self._log("[!] No known peers yet. Try Scan.")
            return
        metrics = node.telemetry.get_all_metrics()
        lines = ["[*] Known peers:"]
        for ip, name in peers.items():
            rtt, loss = metrics.get(ip, (0.0, 0.0))
            marker = "  <-- current" if ip == node.target_ip else ""
            lines.append(
                f"  {name} ({ip}) RTT={rtt:.1f}ms "
                f"Loss={loss * 100:.1f}%{marker}"
            )
        self._log("\n".join(lines))

    def _show_status(self) -> None:
        node = self._require_node()
        if node:
            self._log(node.get_status())

    def _send_message(self) -> None:
        target_ip = self._require_target()
        if not target_ip or not self.node:
            return
        text = self.message_var.get().strip()
        if not text:
            self._log("[!] Message text cannot be empty.")
            return
        if len(text.encode("utf-8", errors="replace")) > 2048:
            self._log("[!] Message too long, max 2048 bytes.")
            return

        def worker() -> None:
            if self.node and self.node.send_message(text, target_ip):
                self._log(f"[You]: {text}")
                self.root.after(0, self.message_var.set, "")
            elif self.node:
                self._log(f"[!] Failed to reach {self.node.target_name}.")

        self._run_async(worker)

    def _send_sos(self) -> None:
        target_ip = self._require_target()
        if not target_ip or not self.node:
            return
        text = self.sos_var.get().strip()
        if not text:
            self._log("[!] SOS message cannot be empty.")
            return
        self._run_async(lambda: self.node and self.node.send_sos(text, target_ip))

    def _send_location(self) -> None:
        target_ip = self._require_target()
        if not target_ip or not self.node:
            return
        try:
            lat = float(self.lat_var.get().strip())
            lon = float(self.lon_var.get().strip())
        except ValueError:
            self._log("[!] Invalid coordinates. Example: 40.9869 and 29.0259")
            return
        self._run_async(lambda: self.node and self.node.send_location(lat, lon, target_ip))

    def _send_file(self) -> None:
        target_ip = self._require_target()
        if not target_ip or not self.node:
            return
        path = self.file_var.get().strip()
        if not path:
            self._log("[!] File path cannot be empty.")
            return
        self._run_async(lambda: self.node and self.node.send_file(target_ip, path))

    def _simulate_loss(self) -> None:
        node = self._require_node()
        if not node:
            return
        try:
            pct = float(self.loss_var.get().strip())
        except ValueError:
            self._log("[!] Loss must be a number from 0 to 100.")
            return
        node.simulator.set_loss(pct / 100.0)
        self._log(f"[SIM] Packet loss set to {pct:.1f}%")

    def _simulate_delay(self) -> None:
        node = self._require_node()
        if not node:
            return
        try:
            ms = float(self.delay_var.get().strip())
        except ValueError:
            self._log("[!] Delay must be a number in milliseconds.")
            return
        node.simulator.set_delay(ms)
        self._log(f"[SIM] Extra delay set to {ms:.0f}ms")

    def _simulate_reset(self) -> None:
        node = self._require_node()
        if node:
            node.simulator.reset()
            self._log("[SIM] Simulation disabled. All stats cleared.")

    def _simulate_status(self) -> None:
        node = self._require_node()
        if node:
            self._log(node.simulator.get_status())

    def _show_help(self) -> None:
        self._log(
            "Commands are represented by buttons. Fill only the slots next "
            "to the command you want, then press that command button."
        )

    def _browse_file(self) -> None:
        path = filedialog.askopenfilename()
        if path:
            self.file_var.set(path)

    # ------------------------------------------------------------------
    # Logging and shutdown
    # ------------------------------------------------------------------

    def _log(self, text: str) -> None:
        self.log_queue.put(text)

    def _poll_logs(self) -> None:
        while True:
            try:
                text = self.log_queue.get_nowait()
            except queue.Empty:
                break
            self.output.configure(state="normal")
            self.output.insert(tk.END, text.rstrip() + "\n")
            self.output.see(tk.END)
            self.output.configure(state="disabled")
        self.root.after(100, self._poll_logs)

    def _on_close(self) -> None:
        if self.node:
            try:
                self.node.stop()
            except Exception:
                pass
        self.root.destroy()


def main() -> None:
    root = tk.Tk()
    style = ttk.Style(root)
    if "clam" in style.theme_names():
        style.theme_use("clam")
    ResilienceFlowUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
