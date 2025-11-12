# tk.py — Bosch UDF Converter (Light • Premium • Multi-file • Hardened)
# deps: pip install ttkbootstrap pandas pyarrow
# optional: pip install tkinterdnd2  (drag & drop enabled if present)

from __future__ import annotations
import os, sys, json, zipfile, threading, traceback, datetime
from pathlib import Path
from typing import List, Optional
import tkinter as tk
from tkinter import filedialog, messagebox
from tkinter.scrolledtext import ScrolledText

import ttkbootstrap as tb
from ttkbootstrap.constants import *
from ttkbootstrap.widgets import ToastNotification 

# Toast is optional across ttkbootstrap versions
try:
    from ttkbootstrap.widgets import ToastNotification as TBToast
except Exception:
    TBToast = None

# Optional drag-and-drop
try:
    from tkinterdnd2 import DND_FILES, TkinterDnD
    DND_AVAILABLE = True
except Exception:
    DND_AVAILABLE = False

APP_NAME = "Bosch UDF Converter"
SETTINGS_FILE = Path.home() / ".bosch_udf_gui.json"
DEFAULT_THEME = "flatly"   # light, clean

# ---------------- Decoder import (robust) ----------------
SCRIPT_DIR = Path(__file__).resolve().parent
_SYS_PATH_PROBED = False

def _import_decoder():
    """Import UDFDecoder; probe typical local layouts if not installed."""
    global _SYS_PATH_PROBED
    try:
        from bst_udf_decoder.udf.decoder import UDFDecoder  # type: ignore
        return UDFDecoder
    except Exception:
        if not _SYS_PATH_PROBED:
            _SYS_PATH_PROBED = True
            candidates: list[Path] = []
            p = SCRIPT_DIR
            for _ in range(5):
                candidates += [p, p / "working folder"]
                p = p.parent
            for c in candidates:
                if c.exists():
                    sp = str(c)
                    if sp not in sys.path:
                        sys.path.insert(0, sp)
        from bst_udf_decoder.udf.decoder import UDFDecoder  # type: ignore
        return UDFDecoder

def _check_deps() -> list[str]:
    missing = []
    try:
        _ = _import_decoder()
    except Exception:
        missing.append("bst_udf_decoder")
    try:
        import pyarrow as _pa  # noqa
        import pyarrow.parquet as _pq  # noqa
    except Exception:
        missing.append("pyarrow")
    try:
        import pandas as _pd  # noqa
    except Exception:
        missing.append("pandas")
    return missing

# ---------------- Settings helpers ----------------
def ts_now() -> str:
    return datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

def load_settings() -> dict:
    if SETTINGS_FILE.exists():
        try:
            return json.loads(SETTINGS_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_settings(data: dict) -> None:
    try:
        SETTINGS_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")
    except Exception:
        pass

# ---------------- App ----------------
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_NAME)
        self.geometry("1024x680")
        self.minsize(980, 620)

        # ttkbootstrap styling on plain Tk (works everywhere)
        self.style = tb.Style()                          # ← FIXED
        self.current_theme = load_settings().get("theme", DEFAULT_THEME)

        # guard against invalid theme names
        if self.current_theme not in self.style.theme_names():
            self.current_theme = DEFAULT_THEME

        try:
            self.style.theme_use(self.current_theme)
        except Exception:
            self.style.theme_use(DEFAULT_THEME)
            self.current_theme = DEFAULT_THEME

        # state
        self.files: List[Path] = []
        self.output_dir = tk.StringVar()
        self.scaling = tk.BooleanVar(value=True)
        self.write_parquet = tk.BooleanVar(value=True)
        self.write_csv = tk.BooleanVar(value=True)
        self.user_message = tk.StringVar()
        self.subfolder = tk.BooleanVar(value=False)
        self.timestamp_suffix = tk.BooleanVar(value=False)
        self.skip_existing = tk.BooleanVar(value=True)
        self.zip_outputs = tk.BooleanVar(value=False)
        self.is_running = False
        self.stop_flag = threading.Event()
        self.worker: Optional[threading.Thread] = None

        # restore settings
        self._restore_settings()

        # menu + ui
        self._build_menu()
        self._build_header()
        self._build_controls()
        self._build_queue()
        self._build_footer()
        self._refresh_buttons()

        # optional drag & drop
        if DND_AVAILABLE:
            try:
                # Re-parent onto a DnD-enabled toplevel
                self.drop_target_register = self.register  # satisfy attribute
                self.tree.drop_target_register(DND_FILES)
                self.tree.dnd_bind("<<Drop>>", self._on_drop_files)
            except Exception:
                pass  # ignore if tkinterdnd2 is present but not fully functional

    # ------------- settings -------------
    def _persist_settings(self):
        data = {
            "theme": self.current_theme,
            "output_dir": self.output_dir.get(),
            "scaling": self.scaling.get(),
            "write_parquet": self.write_parquet.get(),
            "write_csv": self.write_csv.get(),
            "user_message": self.user_message.get(),
            "subfolder": self.subfolder.get(),
            "timestamp_suffix": self.timestamp_suffix.get(),
            "skip_existing": self.skip_existing.get(),
            "zip_outputs": self.zip_outputs.get(),
        }
        save_settings(data)

    def _restore_settings(self):
        s = load_settings()
        self.output_dir.set(s.get("output_dir", ""))
        self.scaling.set(bool(s.get("scaling", True)))
        self.write_parquet.set(bool(s.get("write_parquet", True)))
        self.write_csv.set(bool(s.get("write_csv", True)))
        self.user_message.set(s.get("user_message", ""))
        self.subfolder.set(bool(s.get("subfolder", False)))
        self.timestamp_suffix.set(bool(s.get("timestamp_suffix", False)))
        self.skip_existing.set(bool(s.get("skip_existing", True)))
        self.zip_outputs.set(bool(s.get("zip_outputs", False)))

    # ------------- UI -------------
    def _build_menu(self):
        menubar = tk.Menu(self)
        m_file = tk.Menu(menubar, tearoff=0)
        m_file.add_command(label="Add files…", command=self._add_files)
        m_file.add_command(label="Choose output folder…", command=self._choose_output)
        m_file.add_separator()
        m_file.add_command(label="Save log…", command=self._save_log)
        m_file.add_separator()
        m_file.add_command(label="Exit", command=self._on_exit)
        menubar.add_cascade(label="File", menu=m_file)

        m_view = tk.Menu(menubar, tearoff=0)
        for th in ["flatly", "cosmo", "journal", "minty", "united", "lumen", "sandstone"]:
            m_view.add_command(label=th.capitalize(), command=lambda t=th: self._set_theme(t))
        menubar.add_cascade(label="View", menu=m_view)

        m_help = tk.Menu(menubar, tearoff=0)
        m_help.add_command(label="About", command=self._about)
        menubar.add_cascade(label="Help", menu=m_help)
        self.config(menu=menubar)

    def _set_theme(self, theme: str):
        try:
            self.style.theme_use(theme)
            self.current_theme = theme
            self._persist_settings()
        except Exception as e:
            messagebox.showerror("Theme error", str(e))

    def _about(self):
        message = (
            f"{APP_NAME}\n"
            "• Converts UDF/BIN → Parquet & CSV\n"
            "• Multi-file queue • Skip existing • Timestamp suffix\n"
            "• Optional ZIP bundle\n\n"
            "Drag & drop enabled if tkinterdnd2 is installed."
        )
        messagebox.showinfo("About", message)

    def _build_header(self):
        hdr = tb.Frame(self, padding=(16, 14, 16, 8))
        hdr.pack(fill=tk.X)
        left = tb.Frame(hdr); left.pack(side=tk.LEFT, fill=tk.X, expand=True)
        tb.Label(left, text=APP_NAME, font=("", 18, "bold")).pack(anchor=tk.W)
        tb.Label(left, text="Convert one or many .udf/.bin files to Parquet and CSV.",
                 bootstyle=SECONDARY).pack(anchor=tk.W)
        right = tb.Frame(hdr); right.pack(side=tk.RIGHT)
        tb.Label(right, text="BOSCH", bootstyle=(DANGER, INVERSE), padding=(12, 5)).pack()

    def _build_controls(self):
        box = tb.Labelframe(self, text="Source & Output", padding=10)
        box.pack(fill=tk.X, padx=16, pady=(0, 8))

        r1 = tb.Frame(box); r1.pack(fill=tk.X, pady=2)
        tb.Button(r1, text="Add files…", bootstyle=PRIMARY, command=self._add_files).pack(side=tk.LEFT, padx=(0, 6))
        tb.Button(r1, text="Remove selected", bootstyle=SECONDARY, command=self._remove_selected).pack(side=tk.LEFT)
        tb.Button(r1, text="Clear list", bootstyle=SECONDARY, command=self._clear_list).pack(side=tk.LEFT, padx=6)

        tb.Label(r1, text="Output folder:", padding=(16, 0, 6, 0)).pack(side=tk.LEFT)
        self.out_entry = tb.Entry(r1, textvariable=self.output_dir, width=44)
        self.out_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)
        tb.Button(r1, text="Choose…", bootstyle=PRIMARY, command=self._choose_output).pack(side=tk.LEFT, padx=(6, 0))

        r2 = tb.Frame(box); r2.pack(fill=tk.X, pady=(8, 0))
        tb.Checkbutton(r2, text="Parquet (.parquet)", variable=self.write_parquet).pack(side=tk.LEFT)
        tb.Checkbutton(r2, text="CSV (.csv)", variable=self.write_csv).pack(side=tk.LEFT, padx=12)
        tb.Checkbutton(r2, text="Apply scaling", variable=self.scaling).pack(side=tk.LEFT, padx=12)

        r3 = tb.Frame(box); r3.pack(fill=tk.X, pady=(8, 0))
        tb.Label(r3, text="User message (optional):").pack(side=tk.LEFT)
        tb.Entry(r3, textvariable=self.user_message).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(8, 0))

        r4 = tb.Frame(box); r4.pack(fill=tk.X, pady=(8, 0))
        tb.Checkbutton(r4, text="Create subfolder in output", variable=self.subfolder).pack(side=tk.LEFT)
        tb.Checkbutton(r4, text="Add timestamp suffix", variable=self.timestamp_suffix).pack(side=tk.LEFT, padx=12)
        tb.Checkbutton(r4, text="Skip existing files", variable=self.skip_existing).pack(side=tk.LEFT, padx=12)
        tb.Checkbutton(r4, text="ZIP all outputs", variable=self.zip_outputs).pack(side=tk.LEFT, padx=12)

        r5 = tb.Frame(box); r5.pack(fill=tk.X, pady=(10, 0))
        self.convert_btn = tb.Button(r5, text="Convert", bootstyle=SUCCESS, command=self._on_convert); self.convert_btn.pack(side=tk.LEFT)
        self.cancel_btn = tb.Button(r5, text="Cancel", bootstyle=(WARNING, OUTLINE), command=self._on_cancel, state=tk.DISABLED); self.cancel_btn.pack(side=tk.LEFT, padx=8)
        self.open_out_btn = tb.Button(r5, text="Open output folder", bootstyle=INFO, command=self._open_output, state=(tk.NORMAL if self.output_dir.get().strip() else tk.DISABLED)); self.open_out_btn.pack(side=tk.LEFT, padx=(8, 0))
        self.progress = tb.Progressbar(r5, mode="determinate", bootstyle=STRIPED, length=320); self.progress.pack(side=tk.RIGHT, padx=6)
        self.progress.configure(value=0, maximum=100)

    def _build_queue(self):
        box = tb.Labelframe(self, text="Conversion Queue", padding=10)
        box.pack(fill=tk.BOTH, expand=True, padx=16, pady=(0, 8))

        cols = ("#", "file", "status", "parquet", "csv")
        self.tree = tb.Treeview(box, columns=cols, show="headings", height=10, bootstyle="light")
        self.tree.heading("#", text="#");      self.tree.column("#", width=44, anchor=tk.CENTER, stretch=False)
        self.tree.heading("file", text="File"); self.tree.column("file", width=620, anchor=tk.W)
        self.tree.heading("status", text="Status"); self.tree.column("status", width=120, anchor=tk.W, stretch=False)
        self.tree.heading("parquet", text="Parquet"); self.tree.column("parquet", width=120, anchor=tk.W, stretch=False)
        self.tree.heading("csv", text="CSV");        self.tree.column("csv", width=120, anchor=tk.W, stretch=False)
        self.tree.pack(fill=tk.BOTH, expand=True)

        self.tree.tag_configure("ok",  foreground="#15803d")
        self.tree.tag_configure("run", foreground="#1d4ed8")
        self.tree.tag_configure("err", foreground="#b91c1c")
        self.tree.tag_configure("wait",foreground="#6b7280")

        self.tree.bind("<Double-1>", self._on_row_double_click)

    def _build_footer(self):
        foot = tb.Labelframe(self, text="Log", padding=10)
        foot.pack(fill=tk.BOTH, expand=False, padx=16, pady=(0, 12))
        self.log = ScrolledText(foot, height=8)
        self.log.pack(fill=tk.BOTH, expand=True)

    # ------------- actions -------------
    def _on_drop_files(self, event):
        raw = event.data
        for p in self._parse_dnd_list(raw):
            P = Path(p)
            if P.exists() and P.suffix.lower() in (".udf", ".bin") and P not in self.files:
                self.files.append(P)
                self._tree_insert(P, "Queued")
        self._renumber(); self._refresh_buttons()

    @staticmethod
    def _parse_dnd_list(data: str) -> List[str]:
        # Handles {C:\path with spaces\file.udf} {C:\file2.bin}
        out, cur, in_brace = [], "", False
        for ch in data:
            if ch == "{": in_brace, cur = True, ""
            elif ch == "}": in_brace, out = False, out + [cur] if isinstance(out, list) else out
            elif ch == " " and not in_brace: 
                if cur: out.append(cur); cur = ""
            else: cur += ch
        if cur: out.append(cur)
        return out

    def _add_files(self):
        paths = filedialog.askopenfilenames(
            title="Select UDF/BIN files",
            filetypes=[("UDF / BIN files", "*.udf *.bin"), ("All files", "*.*")],
        )
        if not paths: return
        for p in paths:
            P = Path(p)
            if P.exists() and P not in self.files:
                self.files.append(P)
                self._tree_insert(P, "Queued")
        self._renumber(); self._refresh_buttons()

    def _remove_selected(self):
        if self.is_running: return
        for iid in self.tree.selection():
            idx = int(self.tree.item(iid, "values")[0]) - 1
            if 0 <= idx < len(self.files): self.files.pop(idx)
            self.tree.delete(iid)
        self._renumber(); self._refresh_buttons()

    def _clear_list(self):
        if self.is_running: return
        self.files.clear()
        for iid in self.tree.get_children(): self.tree.delete(iid)
        self._refresh_buttons()

    def _choose_output(self):
        d = filedialog.askdirectory(title="Choose output folder")
        if d:
            self.output_dir.set(d)
            self.open_out_btn.configure(state=tk.NORMAL)
            self._persist_settings()

    def _open_output(self):
        d = self.output_dir.get().strip()
        if not d: return
        out = Path(d)
        try:
            if sys.platform.startswith("win"): os.startfile(str(out))  # type: ignore
            elif sys.platform == "darwin": os.system(f'open "{out}"')
            else: os.system(f'xdg-open "{out}"')
        except Exception as e:
            messagebox.showerror("Error", f"Could not open folder:\n{e}")

    def _save_log(self):
        path = filedialog.asksaveasfilename(
            title="Save log", defaultextension=".txt",
            filetypes=[("Text file", "*.txt")],
            initialfile=f"udf_conversion_log_{ts_now()}.txt",
        )
        if not path: return
        try:
            Path(path).write_text(self.log.get("1.0", tk.END), encoding="utf-8")
            self._notify("Log saved", "The log was saved successfully.", success=True)
        except Exception as e:
            messagebox.showerror("Error", f"Failed to save log:\n{e}")

    def _on_convert(self):
        if not self.files:
            messagebox.showwarning("No files", "Add at least one .udf/.bin file."); return
        if not (self.write_parquet.get() or self.write_csv.get()):
            messagebox.showwarning("No formats", "Enable Parquet and/or CSV."); return
        out = self.output_dir.get().strip()
        if not out:
            messagebox.showwarning("Output folder", "Choose an output folder."); return
        out_dir = Path(out)
        if not out_dir.exists():
            messagebox.showerror("Output folder", "Selected output folder does not exist."); return

        missing = _check_deps()
        if missing:
            messagebox.showerror("Missing dependencies", "Install first:\n- " + "\n- ".join(missing)); return

        if self.subfolder.get():
            out_dir = out_dir / "UDF_Exports"
            out_dir.mkdir(parents=True, exist_ok=True)

        if not self.skip_existing.get():
            collisions = self._find_collisions(out_dir, self._name_suffix())
            if collisions and not messagebox.askyesno("Overwrite files?",
                    "These files already exist and will be overwritten:\n\n" + "\n".join(collisions) + "\n\nContinue?"):
                return

        self._persist_settings()
        self._log_clear()
        self._log(f"Starting conversion of {len(self.files)} file(s)…")
        self.is_running = True
        self.stop_flag.clear()
        self.progress.configure(value=0, maximum=max(1, len(self.files)))
        self.convert_btn.configure(state=tk.DISABLED)
        self.cancel_btn.configure(state=tk.NORMAL)

        self.worker = threading.Thread(target=self._worker_run, args=(out_dir,), daemon=True)
        self.worker.start()

    def _on_cancel(self):
        if self.is_running:
            self.stop_flag.set()
            self._log("Cancel requested…")

    # ------------- worker -------------
    def _worker_run(self, out_dir: Path):
        produced: List[Path] = []
        try:
            UDFDecoder = _import_decoder()  # noqa: N806
            import pyarrow.parquet as pq
            import pandas as pd

            total = len(self.files)
            do_parq = self.write_parquet.get()
            do_csv = self.write_csv.get()
            apply_scaling = self.scaling.get()
            user_msg = self.user_message.get().strip() or None
            suffix = self._name_suffix()

            for idx, in_file in enumerate(self.files, start=1):
                if self.stop_flag.is_set():
                    self._set_status(in_file, "Cancelled", tag="err")
                    break

                self._set_status(in_file, "Running", tag="run")
                try:
                    base = in_file.stem + suffix
                    parq_path = out_dir / f"{base}.parquet" if do_parq else None
                    csv_path  = out_dir / f"{base}.csv" if do_csv else None

                    if self.skip_existing.get():
                        existed = False
                        if parq_path and parq_path.exists(): existed = True
                        if csv_path and csv_path.exists():  existed = True
                        if existed:
                            self._set_status(in_file, "Skipped", tag="wait",
                                             parquet=("✔ (existing)" if parq_path and parq_path.exists() else ""),
                                             csv=("✔ (existing)" if csv_path and csv_path.exists() else ""))
                            self._log(f"[{idx}/{total}] Skipped (already exists): {in_file.name}")
                            self._set_progress(idx)
                            continue

                    with UDFDecoder() as dec:
                        self._log(f"[{idx}/{total}] Reading: {in_file.name}")
                        dec.read_bin_file(file_path=str(in_file), scaling=apply_scaling, file_blob=None)

                        if user_msg:
                            dec.add_user_meta_data({"UserMessage": user_msg})

                        if parq_path:
                            table = dec.get_arrow_table()
                            pq.write_table(table, parq_path)
                            produced.append(parq_path)

                        if csv_path:
                            df = dec.get_pandas_dataframe()
                            df.to_csv(csv_path, index=False)
                            produced.append(csv_path)

                    self._set_status(in_file, "Done", tag="ok",
                                     parquet=("✔" if parq_path else ""),
                                     csv=("✔" if csv_path else ""))
                    self._log(f"[{idx}/{total}] Finished: {in_file.name}")

                except Exception as e:
                    self._set_status(in_file, "Error", tag="err")
                    self._log(f"ERROR for {in_file.name}: {e}")
                    self._log(traceback.format_exc(limit=12))

                self._set_progress(idx)

            if self.zip_outputs.get() and produced:
                zip_name = out_dir / f"udf_exports_{ts_now()}.zip"
                self._log(f"Bundling {len(produced)} file(s) → {zip_name.name}")
                with zipfile.ZipFile(zip_name, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                    for p in produced:
                        try:
                            zf.write(p, arcname=p.name)
                        except Exception as e:
                            self._log(f"Zip add failed for {p.name}: {e}")

            self.is_running = False
            self._end_run(stopped=self.stop_flag.is_set())

        except Exception as e:
            self.is_running = False
            self._end_run(stopped=True)
            messagebox.showerror("Fatal error", f"{e}\n\n{traceback.format_exc(limit=10)}")

    # ------------- helpers -------------
    def _name_suffix(self) -> str:
        return ("_" + ts_now()) if self.timestamp_suffix.get() else ""

    def _find_collisions(self, out_dir: Path, suffix: str) -> List[str]:
        hits = []
        for f in self.files:
            base = f.stem + suffix
            if self.write_parquet.get() and (out_dir / f"{base}.parquet").exists():
                hits.append(f"{base}.parquet")
            if self.write_csv.get() and (out_dir / f"{base}.csv").exists():
                hits.append(f"{base}.csv")
        return hits

    def _tree_insert(self, path: Path, status: str):
        self.tree.insert("", tk.END, values=(len(self.tree.get_children()) + 1, str(path), status, "", ""),
                         tags=("wait",))

    def _renumber(self):
        for i, iid in enumerate(self.tree.get_children(), start=1):
            vals = list(self.tree.item(iid, "values")); vals[0] = i
            self.tree.item(iid, values=vals)

    def _set_status(self, path: Path, status: str, tag: str = "wait",
                    parquet: str = "", csv: str = ""):
        for iid in self.tree.get_children():
            vals = list(self.tree.item(iid, "values"))
            if Path(vals[1]) == path:
                vals[2] = status
                if parquet: vals[3] = parquet
                if csv:     vals[4] = csv
                self.tree.item(iid, values=vals, tags=(tag,))
                break

    def _set_progress(self, done: int):
        self.progress.configure(value=done)

    def _end_run(self, stopped: bool):
        self.progress.stop()
        self.convert_btn.configure(state=tk.NORMAL)
        self.cancel_btn.configure(state=tk.DISABLED)
        self.open_out_btn.configure(state=(tk.NORMAL if self.output_dir.get().strip() else tk.DISABLED))
        self._refresh_buttons()
        self._notify("Conversion stopped" if stopped else "Conversion complete", "Check the output folder.", success=not stopped)

    def _refresh_buttons(self):
        self.convert_btn.configure(state=(tk.NORMAL if (not self.is_running and len(self.files) > 0) else tk.DISABLED))

    def _log(self, text: str):
        self.log.insert(tk.END, text + "\n")
        self.log.see(tk.END)

    def _log_clear(self):
        self.log.delete("1.0", tk.END)

    def _notify(self, title: str, message: str, success: bool):
        if TBToast:
            try:
                TBToast(title=title, message=message, duration=3000,
                        bootstyle=(SUCCESS if success else WARNING)).show_toast()
                return
            except Exception:
                pass
        (messagebox.showinfo if success else messagebox.showwarning)(title, message)

    def _on_row_double_click(self, _event):
        item = self.tree.focus()
        if not item: return
        vals = self.tree.item(item, "values")
        if not vals: return
        src = Path(vals[1])
        out = Path(self.output_dir.get().strip() or ".")
        sub = out / "UDF_Exports" if self.subfolder.get() else out
        suffix = self._name_suffix()
        for ext in (".parquet", ".csv"):
            p = sub / f"{src.stem}{suffix}{ext}"
            if p.exists():
                try:
                    if sys.platform.startswith("win"): os.startfile(str(p))  # type: ignore
                    elif sys.platform == "darwin": os.system(f'open "{p}"')
                    else: os.system(f'xdg-open "{p}"')
                    return
                except Exception:
                    pass
        self._open_output()

    def _on_exit(self):
        self._persist_settings()
        self.destroy()

# ---------------- main ----------------
if __name__ == "__main__":
    app = App()
    app.protocol("WM_DELETE_WINDOW", app._on_exit)
    app.mainloop()
