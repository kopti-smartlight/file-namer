import os
import re
import csv
import time
import shutil
import queue
import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from typing import Dict, Optional, List, Tuple

# -------- 정규식 -------- #
IMG_NUM_RE = re.compile(r"(?:^|_)(\d{2})(?=\D*$)")
HM_RE = re.compile(r"(?i)(high|middle|\bh\b|\bm\b)")
A_NN_END_RE = re.compile(r"_(\d{2})(?=\.[^.]+$)", re.IGNORECASE)
A_NN_ANY_RE = re.compile(r"(\d{2})(?!\d)")

VALID_STOP_RE = re.compile(r"^\d{4}$")
VALID_SET_RE  = re.compile(r"^\d{4}$")

# -------- 옵션 -------- #
RETRY_MAX = 3
RETRY_BACKOFF_BASE = 0.15
VERIFY_SIZE = True
ATOMIC_COPY = True
FSYNC_AFTER_COPY = True

UI_TICK_MS = 50
UI_MSG_EVERY = 50

# -------- 유틸 -------- #
def is_date_folder(name: str) -> bool:
    return bool(re.fullmatch(r"\d{6}|\d{8}", name))

def to_yymmdd(date_folder_name: str) -> str:
    return date_folder_name[-6:]

def ensure_unique_path_fast(dest: Path) -> Path:
    if not dest.exists():
        return dest
    stem, suffix, parent = dest.stem, dest.suffix, dest.parent
    i = 1
    while True:
        cand = parent / f"{stem}_dup{i}{suffix}"
        if not cand.exists():
            return cand
        i += 1

def _find_child_dir_casefold(parent: Path, name_cf: str) -> Optional[Path]:
    try:
        with os.scandir(parent) as it:
            for d in it:
                if d.is_dir() and d.name.casefold() == name_cf:
                    return Path(d.path)
    except FileNotFoundError:
        return None
    return None

def list_all_b_files(b_date_path: Path):
    single_dir = _find_child_dir_casefold(b_date_path, "single")
    if single_dir and single_dir.is_dir():
        with os.scandir(single_dir) as it_sets:
            for d in it_sets:
                if d.is_dir() and VALID_SET_RE.match(d.name or ""):
                    with os.scandir(Path(d.path)) as it_files:
                        for f in it_files:
                            if f.is_file():
                                yield ("single", None, d.name, Path(f.path))
    with os.scandir(b_date_path) as it_stops:
        for s in it_stops:
            if not s.is_dir() or s.name.casefold() == "single":
                continue
            if not VALID_STOP_RE.match(s.name or ""):
                continue
            stop_dir = Path(s.path)
            with os.scandir(stop_dir) as it_sets2:
                for d in it_sets2:
                    if d.is_dir() and VALID_SET_RE.match(d.name or ""):
                        with os.scandir(Path(d.path)) as it_files:
                            for f in it_files:
                                if f.is_file():
                                    yield ("multi", s.name, d.name, Path(f.path))

def parse_b_file_num_and_base(b_file: Path):
    m = IMG_NUM_RE.search(b_file.stem)
    if not m:
        return None, None, None
    nn = m.group(1)
    stem = b_file.stem
    start = m.start(1)
    base = stem[: start - 1] if start > 0 and stem[start - 1] == "_" else stem[: start]
    base = base.rstrip("_")
    return nn, base, b_file.suffix

def build_hm_index_for_set(a_set_path: Path) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    if not a_set_path.is_dir():
        return mapping
    with os.scandir(a_set_path) as it:
        for f in it:
            if not f.is_file():
                continue
            name = f.name
            stem = Path(name).stem
            m = A_NN_END_RE.search(name)
            if m:
                nn = m.group(1)
            else:
                m2_all = list(A_NN_ANY_RE.finditer(stem))
                if not m2_all:
                    continue
                nn = m2_all[-1].group(1)
            hm_m = HM_RE.search(name)
            if not hm_m:
                continue
            token = hm_m.group(1).lower()
            val = "H" if token in ("high", "h") else ("M" if token in ("middle", "m") else None)
            if not val:
                continue
            if nn not in mapping or val == "H":
                mapping[nn] = val
    return mapping

def find_a_date_dir(a_root: Path, target_yymmdd: str) -> Optional[Path]:
    def digits(s: str) -> str:
        return "".join(ch for ch in s if ch.isdigit())
    cands: List[Path] = []
    try:
        with os.scandir(a_root) as it:
            for d in it:
                if not d.is_dir():
                    continue
                ds = digits(d.name)
                if len(ds) >= 6 and ds[-6:] == target_yymmdd:
                    cands.append(Path(d.path))
    except FileNotFoundError:
        return None
    if not cands:
        return None
    exact = [p for p in cands if p.name == target_yymmdd]
    if exact:
        return exact[0]
    yyyymmdd = []
    for p in cands:
        ds = "".join(ch for ch in p.name if ch.isdigit())
        if len(ds) == 8 and ds[-6:] == target_yymmdd:
            yyyymmdd.append(p)
    if yyyymmdd:
        return sorted(yyyymmdd)[0]
    return sorted(cands)[0]

# -------- 안전 복사 -------- #
def safe_copy2_atomic(src: Path, dst_final: Path) -> bool:
    dst_dir = dst_final.parent
    tmp = dst_final.with_name(dst_final.name + ".part")
    dir_fd = None
    if FSYNC_AFTER_COPY:
        try:
            dir_fd = os.open(str(dst_dir), os.O_RDONLY)
        except Exception:
            dir_fd = None
    for attempt in range(1, RETRY_MAX + 1):
        try:
            if tmp.exists():
                try: tmp.unlink()
                except Exception: pass
            shutil.copy2(src, tmp)
            if VERIFY_SIZE and (src.stat().st_size != tmp.stat().st_size):
                raise IOError("size mismatch")
            if FSYNC_AFTER_COPY:
                try:
                    with open(tmp, "rb") as f:
                        os.fsync(f.fileno())
                except Exception: pass
            os.replace(tmp, dst_final)
            if FSYNC_AFTER_COPY and dir_fd is not None:
                try: os.fsync(dir_fd)
                except Exception: pass
            return True
        except Exception:
            if attempt < RETRY_MAX:
                time.sleep(RETRY_BACKOFF_BASE * attempt)
            else:
                try:
                    if tmp.exists(): tmp.unlink()
                except Exception: pass
                return False
    return False

def _fallback_copy2(src: Path, dst: Path) -> bool:
    for attempt in range(1, RETRY_MAX + 1):
        try:
            shutil.copy2(src, dst)
            if VERIFY_SIZE and (src.stat().st_size != dst.stat().st_size):
                raise IOError("size mismatch")
            return True
        except Exception:
            if attempt < RETRY_MAX:
                time.sleep(RETRY_BACKOFF_BASE * attempt)
            else:
                return False

# -------- GUI -------- #
class App:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Namer (Safe-Fast)")

        self.var_b = tk.StringVar()
        self.var_a_root = tk.StringVar()
        self.var_out = tk.StringVar()
        self.status_var = tk.StringVar(value="대기 중…")

        tk.Label(root, text="컨버팅 폴더(B, 날짜)").grid(row=0, column=0, sticky="e", padx=5, pady=5)
        tk.Entry(root, textvariable=self.var_b, width=60).grid(row=0, column=1, padx=5)
        tk.Button(root, text="선택", command=self.select_b).grid(row=0, column=2, padx=5)

        tk.Label(root, text="원시 상위 폴더(A)").grid(row=1, column=0, sticky="e", padx=5, pady=5)
        tk.Entry(root, textvariable=self.var_a_root, width=60).grid(row=1, column=1, padx=5)
        tk.Button(root, text="선택", command=self.select_a_root).grid(row=1, column=2, padx=5)

        tk.Label(root, text="출력 폴더").grid(row=2, column=0, sticky="e", padx=5, pady=5)
        tk.Entry(root, textvariable=self.var_out, width=60).grid(row=2, column=1, padx=5)
        tk.Button(root, text="선택", command=self.select_out).grid(row=2, column=2, padx=5)

        # 모드 선택 (rename / copy) — 기본은 rename
        tk.Label(root, text="작업 모드").grid(row=3, column=0, sticky="e", padx=5, pady=5)
        self.mode_var = tk.StringVar(value="rename")
        mode_combo = ttk.Combobox(root, textvariable=self.mode_var, values=["rename", "copy"], state="readonly", width=10)
        mode_combo.grid(row=3, column=1, sticky="w", padx=5)

        self.run_btn = tk.Button(root, text="실행", bg="lightgreen", width=20, command=self.run)
        self.run_btn.grid(row=4, column=1, pady=10)

        self.progress = ttk.Progressbar(root, length=500)
        self.progress.grid(row=5, column=0, columnspan=3, pady=5)

        tk.Label(root, textvariable=self.status_var).grid(row=6, column=0, columnspan=3, pady=5)

        self._q = queue.Queue()
        self._stats = {}
        self._work_thread: Optional[threading.Thread] = None

    def select_b(self):
        folder = filedialog.askdirectory(title="컨버팅된 폴더(날짜) 선택")
        if folder: self.var_b.set(folder)

    def select_a_root(self):
        folder = filedialog.askdirectory(title="원시 상위 폴더 선택")
        if folder: self.var_a_root.set(folder)

    def select_out(self):
        folder = filedialog.askdirectory(title="출력 폴더 선택")
        if folder: self.var_out.set(folder)

    def _ui_tick(self):
        last_msg = None
        try:
            while True: last_msg = self._q.get_nowait()
        except queue.Empty: pass
        if last_msg: self.status_var.set(last_msg)
        s = self._stats
        if s.get("total",0) > 0:
            self.progress["maximum"] = s["total"]
            self.progress["value"] = s["processed"]
        if s.get("done",False):
            messagebox.showinfo("완료", f"완료: {s['copied_ok']} 성공 / {s['skipped']} 스킵")
            self.run_btn.config(state="normal"); return
        self.root.after(UI_TICK_MS, self._ui_tick)

    def run(self):
        if self._work_thread and self._work_thread.is_alive():
            messagebox.showwarning("진행 중", "이미 실행 중"); return  
        b_date = Path(self.var_b.get()); a_root = Path(self.var_a_root.get()); out_root = Path(self.var_out.get())
        if not (b_date.is_dir() and a_root.is_dir() and out_root.is_dir()):
            messagebox.showerror("오류", "경로 확인"); return
        yymmdd = to_yymmdd(b_date.name); a_date = find_a_date_dir(a_root, yymmdd)
        if not a_date: messagebox.showerror("오류", "원시 날짜 폴더 없음"); return
        files = list(list_all_b_files(b_date)); total = len(files)
        if total==0: messagebox.showinfo("정보","처리할 파일 없음"); return
        self._stats={"total":total,"processed":0,"copied_ok":0,"skipped":0,"done":False}
        def worker():
            for mode,stop,set_no,b_file in files:
                nn,base,ext = parse_b_file_num_and_base(b_file)
                if nn is None: self._stats["skipped"]+=1; self._stats["processed"]+=1; continue
                if mode=="single":
                    a_single=_find_child_dir_casefold(a_date,"single") or (a_date/"Single")
                    a_set=a_single/set_no
                else: a_set=a_date/(stop or "0000")/set_no
                mapping=build_hm_index_for_set(a_set); hm=mapping.get(nn)
                if hm is None: self._stats["skipped"]+=1; self._stats["processed"]+=1; continue
                if mode=="single": new=f"{yymmdd}-{set_no}-{base}_{hm}_{nn}{ext}"
                else: new=f"{yymmdd}-{stop}-{set_no}-{base}_{hm}_{nn}{ext}"
                out_dir=out_root/yymmdd/nn; out_dir.mkdir(parents=True,exist_ok=True); dest=ensure_unique_path_fast(out_dir/new)
                if self.mode_var.get()=="rename":
                    try: b_file.rename(dest); self._stats["copied_ok"]+=1
                    except: self._stats["skipped"]+=1
                else:
                    ok=safe_copy2_atomic(b_file,dest)
                    if ok: self._stats["copied_ok"]+=1
                    else: self._stats["skipped"]+=1
                self._stats["processed"]+=1
            self._stats["done"]=True
        self.run_btn.config(state="disabled")
        self._work_thread=threading.Thread(target=worker,daemon=True); self._work_thread.start(); self._ui_tick()

def main():
    root=tk.Tk(); App(root); root.mainloop()
if __name__=="__main__": main()
