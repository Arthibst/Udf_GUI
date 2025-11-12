# tk.py — Bosch-grade UDF → Parquet/CSV (Light • Premium • Multi-file)
# deps: pip install ttkbootstrap pandas pyarrow

from __future__ import annotations
import os, sys, threading, traceback
from pathlib import Path
from typing import List, Optional
import tkinter as tk
from tkinter import filedialog, messagebox
from tkinter.scrolledtext import ScrolledText  # << use stdlib ScrolledText

import ttkbootstrap as tb
from ttkbootstrap.constants import *
from ttkbootstrap.widgets import ToastNotification  # keep Toast here

# -------- Robust decoder import (walk up & probe common dirs) --------
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
            for _ in range(4):
                candidates.append(p)
                candidates.append(p / "working folder")
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

# ------------------------------ App ------------------------------
class App(tb.Window):
    def __init__(self):
        super().__init__(themename="flatly")  # clean light theme
        self.title("Bosch UDF Converter")
        self.geometry("980x620")
        self.minsize(920, 560)

        # state
        self.files: List[Path] = []
        self.output_dir = tk.StringVar()
        self.scaling = tk.BooleanVar(value=True)
        self.write_parquet = tk.BooleanVar(value=True)
        self.write_csv = tk.BooleanVar(value=True)
        self.user_message = tk.StringVar()
        self.is_running = False
        self.stop_flag = threading.Event()
        self.worker: Optional[threading.Thread] = None

        self._build_header()
        self._build_controls()
        self._build_queue()
        self._build_footer()
        self._refresh_buttons()

    # ---------------- UI ----------------
    def _build_header(self):
        hdr = tb.Frame(self, padding=(16, 14, 16, 8))
        hdr.pack(fill=X)

        left = tb.Frame(hdr)
        left.pack(side=LEFT, fill=X, expand=True)
        tb.Label(left, text="Bosch UDF Converter", font=("", 16, "bold")).pack(anchor=W)
        tb.Label(left, text="Convert one or many .udf/.bin files to Parquet and CSV.", bootstyle=SECONDARY)\
          .pack(anchor=W)

        right = tb.Frame(hdr)
        right.pack(side=RIGHT)
        tb.Label(right, text="BOSCH", bootstyle=(DANGER, INVERSE), padding=(10, 4)).pack()

    def _build_controls(self):
        bar = tb.Labelframe(self, text="Source & Output", padding=10)
        bar.pack(fill=X, padx=16, pady=(0, 8))

        r1 = tb.Frame(bar); r1.pack(fill=X, pady=2)
        tb.Button(r1, text="Add files…", bootstyle=PRIMARY, command=self._add_files).pack(side=LEFT, padx=(0, 6))
        tb.Button(r1, text="Remove selected", bootstyle=SECONDARY, command=self._remove_selected).pack(side=LEFT)
        tb.Button(r1, text="Clear list", bootstyle=SECONDARY, command=self._clear_list).pack(side=LEFT, padx=6)

        tb.Label(r1, text="Output folder:", padding=(16, 0, 6, 0)).pack(side=LEFT)
        self.out_entry = tb.Entry(r1, textvariable=self.output_dir, width=40)
        self.out_entry.pack(side=LEFT, fill=X, expand=True)
        tb.Button(r1, text="Choose…", bootstyle=PRIMARY, command=self._choose_output).pack(side=LEFT, padx=(6, 0))

        r2 = tb.Frame(bar); r2.pack(fill=X, pady=(8, 0))
        tb.Checkbutton(r2, text="Write Parquet (.parquet)", variable=self.write_parquet).pack(side=LEFT)
        tb.Checkbutton(r2, text="Write CSV (.csv)", variable=self.write_csv).pack(side=LEFT, padx=12)
        tb.Checkbutton(r2, text="Apply scaling", variable=self.scaling).pack(side=LEFT, padx=12)

        r3 = tb.Frame(bar); r3.pack(fill=X, pady=(8, 0))
        tb.Label(r3, text="User message (optional):").pack(side=LEFT)
        tb.Entry(r3, textvariable=self.user_message).pack(side=LEFT, fill=X, expand=True, padx=(8, 0))

        r4 = tb.Frame(bar); r4.pack(fill=X, pady=(10, 0))
        self.convert_btn = tb.Button(r4, text="Convert", bootstyle=SUCCESS, command=self._on_convert); self.convert_btn.pack(side=LEFT)
        self.cancel_btn = tb.Button(r4, text="Cancel", bootstyle=(WARNING, OUTLINE), command=self._on_cancel, state=DISABLED); self.cancel_btn.pack(side=LEFT, padx=8)
        self.open_out_btn = tb.Button(r4, text="Open output folder", bootstyle=INFO, command=self._open_output, state=DISABLED); self.open_out_btn.pack(side=LEFT, padx=(8, 0))
        self.progress = tb.Progressbar(r4, mode="determinate", bootstyle=STRIPED, length=280); self.progress.pack(side=RIGHT, padx=6)
        self.progress.configure(value=0, maximum=100)

    def _build_queue(self):
        box = tb.Labelframe(self, text="Conversion Queue", padding=10)
        box.pack(fill=BOTH, expand=True, padx=16, pady=(0, 8))

        cols = ("#", "file", "status", "parquet", "csv")
        self.tree = tb.Treeview(box, columns=cols, show="headings", height=10, bootstyle="light")
        self.tree.heading("#", text="#");      self.tree.column("#", width=40, anchor=CENTER, stretch=False)
        self.tree.heading("file", text="File"); self.tree.column("file", width=520, anchor=W)
        self.tree.heading("status", text="Status"); self.tree.column("status", width=120, anchor=W, stretch=False)
        self.tree.heading("parquet", text="Parquet"); self.tree.column("parquet", width=120, anchor=W, stretch=False)
        self.tree.heading("csv", text="CSV");        self.tree.column("csv", width=120, anchor=W, stretch=False)
        self.tree.pack(fill=BOTH, expand=True)

        self.tree.tag_configure("ok",  foreground="#15803d")
        self.tree.tag_configure("run", foreground="#1d4ed8")
        self.tree.tag_configure("err", foreground="#b91c1c")
        self.tree.tag_configure("wait",foreground="#6b7280")

    def _build_footer(self):
        foot = tb.Labelframe(self, text="Log", padding=10)
        foot.pack(fill=BOTH, expand=False, padx=16, pady=(0, 12))
        self.log = ScrolledText(foot, height=8)  # << stdlib widget; no autohide
        self.log.pack(fill=BOTH, expand=True)

    # ---------------- Actions ----------------
    def _add_files(self):
        paths = filedialog.askopenfilenames(
            title="Select UDF/BIN files",
            filetypes=[("UDF / BIN files", "*.udf *.bin"), ("All files", "*.*")],
        )
        if not paths:
            return
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
            if 0 <= idx < len(self.files):
                self.files.pop(idx)
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
            self.open_out_btn.configure(state=NORMAL)

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

    def _on_convert(self):
        if not self.files:
            messagebox.showwarning("No files", "Add at least one .udf/.bin file."); return
        if not (self.write_parquet.get() or self.write_csv.get()):
            messagebox.showwarning("No formats", "Enable Parquet and/or CSV."); return
        out = self.output_dir.get().strip()
        if not out:
            messagebox.showwarning("Output folder", "Choose an output folder."); return
        if not Path(out).exists():
            messagebox.showerror("Output folder", "Selected output folder does not exist."); return

        missing = _check_deps()
        if missing:
            messagebox.showerror("Missing dependencies", "Install first:\n- " + "\n- ".join(missing)); return

        collisions = self._find_collisions(Path(out))
        if collisions:
            if not messagebox.askyesno("Overwrite files?",
                "These files already exist and will be overwritten:\n\n" + "\n".join(collisions) + "\n\nContinue?"):
                return

        # run
        self._log_clear()
        self._log(f"Starting conversion of {len(self.files)} file(s)…")
        self.is_running = True
        self.stop_flag.clear()
        self.progress.configure(value=0, maximum=max(1, len(self.files)))
        self.convert_btn.configure(state=DISABLED)
        self.cancel_btn.configure(state=NORMAL)

        self.worker = threading.Thread(target=self._worker_run, daemon=True)
        self.worker.start()

    def _on_cancel(self):
        if self.is_running:
            self.stop_flag.set()
            self._log("Cancel requested…")

    # ---------------- Worker ----------------
    def _worker_run(self):
        try:
            UDFDecoder = _import_decoder()  # noqa: N806
            import pyarrow.parquet as pq
            import pandas as pd

            total = len(self.files)
            out_dir = Path(self.output_dir.get().strip())
            do_parq = self.write_parquet.get()
            do_csv = self.write_csv.get()
            apply_scaling = self.scaling.get()
            user_msg = self.user_message.get().strip() or None

            for idx, in_file in enumerate(self.files, start=1):
                if self.stop_flag.is_set():
                    self._set_status(in_file, "Cancelled", tag="err")
                    break

                self._set_status(in_file, "Running", tag="run")
                try:
                    with UDFDecoder() as dec:
                        self._log(f"[{idx}/{total}] Reading: {in_file.name}")
                        dec.read_bin_file(file_path=str(in_file), scaling=apply_scaling, file_blob=None)

                        if user_msg:
                            dec.add_user_meta_data({"UserMessage": user_msg})

                        if do_parq:
                            table = dec.get_arrow_table()
                            pq.write_table(table, out_dir / f"{in_file.stem}.parquet")

                        if do_csv:
                            df = dec.get_pandas_dataframe()
                            df.to_csv(out_dir / f"{in_file.stem}.csv", index=False)

                    self._set_status(in_file, "Done", tag="ok",
                                     parquet="✔" if do_parq else "",
                                     csv="✔" if do_csv else "")
                    self._log(f"[{idx}/{total}] Finished: {in_file.name}")

                except Exception as e:
                    self._set_status(in_file, "Error", tag="err")
                    self._log(f"ERROR for {in_file.name}: {e}")
                    self._log(traceback.format_exc(limit=12))

                self._set_progress(idx)

            self.is_running = False
            self._end_run()

        except Exception as e:
            self.is_running = False
            self._end_run()
            messagebox.showerror("Fatal error", f"{e}\n\n{traceback.format_exc(limit=10)}")

    # ---------------- Helpers ----------------
    def _find_collisions(self, out_dir: Path) -> List[str]:
        hits = []
        for f in self.files:
            if self.write_parquet.get() and (out_dir / f"{f.stem}.parquet").exists():
                hits.append(f"{f.stem}.parquet")
            if self.write_csv.get() and (out_dir / f"{f.stem}.csv").exists():
                hits.append(f"{f.stem}.csv")
        return hits

    def _tree_insert(self, path: Path, status: str):
        self.tree.insert("", END, values=(len(self.tree.get_children()) + 1, str(path), status, "", ""),
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

    def _end_run(self):
        self.progress.stop()
        self.convert_btn.configure(state=NORMAL)
        self.cancel_btn.configure(state=DISABLED)
        self.open_out_btn.configure(state=NORMAL)
        self._refresh_buttons()
        ToastNotification(
            title="Conversion complete" if not self.stop_flag.is_set() else "Conversion stopped",
            message="Files processed. Check the output folder.",
            duration=3000,
            bootstyle=SUCCESS if not self.stop_flag.is_set() else WARNING
        ).show_toast()

    def _refresh_buttons(self):
        has_files = len(self.files) > 0
        if not self.is_running:
            self.convert_btn.configure(state=(NORMAL if has_files else DISABLED))
        self.open_out_btn.configure(state=(NORMAL if self.output_dir.get().strip() else DISABLED))

    def _log(self, text: str):
        self.log.insert(END, text + "\n")
        self.log.see(END)

    def _log_clear(self):
        self.log.delete("1.0", END)

# -------------------- main --------------------
if __name__ == "__main__":
    app = App()
    app.mainloop()
