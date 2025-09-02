import os
import re
import sys
import time
import shutil
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

"""
Namer (Updated)
- A = Raw folder (YYYYMMDD/Single/SET or YYYYMMDD/STOP/SET) with filenames containing High/Middle tokens
- B = Converted folder (same tree as A) with filenames like image_01.png, lumi_13.jpg, pf_01.pf
- OUTPUT = Destination folder; files are COPIED from B with a new standardized name based on A's High/Middle

Rules
-----
Single source → YYMMDD-SET-BASE_{H|M}_NN.ext
Multi  source → YYMMDD-STOP-SET-BASE_{H|M}_NN.ext
- YYMMDD is derived from the selected date folder name (supports 8-digit YYYYMMDD or 6-digit YYMMDD); we keep the last 6 digits
- STOP and SET are 4-digit, zero-padded
- NN is 2-digit image index parsed from B filename suffix (…_NN.ext)

Behavior
--------
- UI unchanged: user selects B date folder, A date folder, OUTPUT folder
- The program traverses B. For each file, it searches only the corresponding branch in A:
  * If file is under B/DATE/Single/SET → search in A/DATE/Single/SET
  * If file is under B/DATE/STOP/SET   → search in A/DATE/STOP/SET
- In A, determine H/M from filenames containing 'High' or 'Middle' (case-insensitive). If both exist for the NN, prefer High.
- Copy B file to OUTPUT with the new name. If a name already exists, append _dup1, _dup2, …
"""

# -------- Utilities -------- #
IMG_NUM_RE = re.compile(r"(?:^|_)(\d{2})(?=\D*$)")  # captures the last two digits token
HM_RE      = re.compile(r"(?i)\b(high|middle)\b")  # case-insensitive H/M token

VALID_STOP_RE = re.compile(r"^\d{4}$")
VALID_SET_RE  = re.compile(r"^\d{4}$")


def is_date_folder(name: str) -> bool:
    return bool(re.fullmatch(r"\d{6}|\d{8}", name))


def to_yymmdd(date_folder_name: str) -> str:
    # Accept 6 or 8 digits; keep last 6
    return date_folder_name[-6:]


def ensure_unique_path(dest: Path) -> Path:
    if not dest.exists():
        return dest
    stem = dest.stem
    suffix = dest.suffix
    parent = dest.parent
    i = 1
    while True:
        cand = parent / f"{stem}_dup{i}{suffix}"
        if not cand.exists():
            return cand
        i += 1


def list_all_b_files(b_date_path: Path):
    """Yield tuples (mode, stop, set_no, file_path) for every file under B date folder.
    mode: 'single' or 'multi'
    stop: 4-digit str or None for single
    set_no: 4-digit str
    file_path: Path to the file in B
    """
    # Single branch
    single_dir = b_date_path / "Single"
    if single_dir.is_dir():
        for set_dir in sorted(single_dir.iterdir()):
            if set_dir.is_dir() and VALID_SET_RE.match(set_dir.name):
                for f in sorted(set_dir.iterdir()):
                    if f.is_file():
                        yield ("single", None, set_dir.name, f)

    # Multi branches: any 4-digit folder at date root (excluding 'Single')
    for stop_dir in sorted(b_date_path.iterdir()):
        if stop_dir.is_dir() and stop_dir.name != "Single" and VALID_STOP_RE.match(stop_dir.name):
            for set_dir in sorted(stop_dir.iterdir()):
                if set_dir.is_dir() and VALID_SET_RE.match(set_dir.name):
                    for f in sorted(set_dir.iterdir()):
                        if f.is_file():
                            yield ("multi", stop_dir.name, set_dir.name, f)


def parse_b_file_num_and_base(b_file: Path):
    """Return (NN, base, ext) from B filename like 'lumi_13.jpg'. If not matched, return (None, None, None)."""
    m = IMG_NUM_RE.search(b_file.stem)
    if not m:
        return None, None, None
    nn = m.group(1)  # two digits as string
    # base is filename before the final '_NN'
    base = b_file.stem[: m.start(1) - 1] if m.start(1) > 0 else b_file.stem
    base = base.rstrip("_")
    return nn, base, b_file.suffix


def find_hm_from_a(a_set_path: Path, nn: str) -> str | None:
    """Look for files in A set folder that contain the image number NN, and detect High/Middle.
    Returns 'H', 'M', or None if not determinable.
    High has priority over Middle if both exist.
    """
    # strict match for NN as a token at the end (…_NN[.ext] or …_NN_something)
    pattern = re.compile(rf"(^|_){re.escape(nn)}(?!\d)")
    found_high = False
    found_middle = False

    try:
        for f in a_set_path.iterdir():
            if not f.is_file():
                continue
            name = f.name
            if pattern.search(name):
                hm = HM_RE.search(name)
                if hm:
                    token = hm.group(1).lower()
                    if token == "high":
                        found_high = True
                    elif token == "middle":
                        found_middle = True
                    # Early exit if both found
                    if found_high and found_middle:
                        break
    except FileNotFoundError:
        return None

    if found_high:
        return "H"
    if found_middle:
        return "M"
    return None


# -------- GUI Handlers -------- #
class App:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Namer (Single/Multi Auto)")

        self.var_b = tk.StringVar()
        self.var_a = tk.StringVar()
        self.var_out = tk.StringVar()
        self.status_var = tk.StringVar(value="대기 중…")

        # Row 0: B (Converted)
        tk.Label(root, text="컨버팅 폴더(B, 날짜)").grid(row=0, column=0, sticky="e", padx=5, pady=5)
        tk.Entry(root, textvariable=self.var_b, width=60).grid(row=0, column=1, padx=5)
        tk.Button(root, text="선택", command=self.select_b).grid(row=0, column=2, padx=5)

        # Row 1: A (Raw)
        tk.Label(root, text="원시 폴더(A, 날짜)").grid(row=1, column=0, sticky="e", padx=5, pady=5)
        tk.Entry(root, textvariable=self.var_a, width=60).grid(row=1, column=1, padx=5)
        tk.Button(root, text="선택", command=self.select_a).grid(row=1, column=2, padx=5)

        # Row 2: OUTPUT
        tk.Label(root, text="출력 폴더").grid(row=2, column=0, sticky="e", padx=5, pady=5)
        tk.Entry(root, textvariable=self.var_out, width=60).grid(row=2, column=1, padx=5)
        tk.Button(root, text="선택", command=self.select_out).grid(row=2, column=2, padx=5)

        # Row 3: Run
        tk.Button(root, text="실행", bg="lightgreen", width=20, command=self.run).grid(row=3, column=1, pady=10)

        # Row 4: Progress
        self.progress = ttk.Progressbar(root, length=500)
        self.progress.grid(row=4, column=0, columnspan=3, pady=5)

        # Row 5: Status
        tk.Label(root, textvariable=self.status_var).grid(row=5, column=0, columnspan=3, pady=5)

    def select_b(self):
        folder = filedialog.askdirectory(title="컨버팅된 폴더(날짜) 선택")
        if folder:
            name = os.path.basename(folder)
            if is_date_folder(name):
                self.var_b.set(folder)
            else:
                messagebox.showerror("오류", "날짜 폴더(YYMMDD 또는 YYYYMMDD)를 선택하세요.")

    def select_a(self):
        folder = filedialog.askdirectory(title="원시 폴더(날짜) 선택")
        if folder:
            name = os.path.basename(folder)
            if is_date_folder(name):
                self.var_a.set(folder)
            else:
                messagebox.showerror("오류", "날짜 폴더(YYMMDD 또는 YYYYMMDD)를 선택하세요.")

    def select_out(self):
        folder = filedialog.askdirectory(title="출력 폴더 선택")
        if folder:
            self.var_out.set(folder)

    def run(self):
        try:
            b_date = Path(self.var_b.get())
            a_date = Path(self.var_a.get())
            out_root = Path(self.var_out.get())

            if not b_date.is_dir() or not a_date.is_dir() or not out_root.is_dir():
                messagebox.showerror("오류", "폴더 경로를 올바르게 선택하세요.")
                return

            b_name = b_date.name
            a_name = a_date.name
            if not (is_date_folder(b_name) and is_date_folder(a_name)):
                messagebox.showerror("오류", "날짜 폴더(YYMMDD 또는 YYYYMMDD)만 선택할 수 있습니다.")
                return

            if to_yymmdd(b_name) != to_yymmdd(a_name):
                if not messagebox.askyesno("확인", "A/B 날짜 폴더가 다릅니다. 계속하시겠습니까?"):
                    return

            yymmdd = to_yymmdd(b_name)

            files = list(list_all_b_files(b_date))
            total = len(files)
            if total == 0:
                messagebox.showinfo("정보", "처리할 파일이 없습니다.")
                return

            self.progress["maximum"] = total
            self.progress["value"] = 0

            processed = 0
            skipped = 0
            t0 = time.time()

            for mode, stop, set_no, b_file in files:
                nn, base, ext = parse_b_file_num_and_base(b_file)
                if nn is None:
                    skipped += 1
                    processed += 1
                    self.progress["value"] = processed
                    self.status_var.set(f"스킵(번호X): {b_file.name} | {processed}/{total}")
                    self.root.update_idletasks()
                    continue

                # Determine the matching A set folder path based on mode
                if mode == "single":
                    a_set_path = a_date / "Single" / set_no
                else:  # multi
                    a_set_path = a_date / (stop or "0000") / set_no

                hm = find_hm_from_a(a_set_path, nn)
                if hm is None:
                    skipped += 1
                    processed += 1
                    self.progress["value"] = processed
                    self.status_var.set(f"스킵(H/M 미확정): {b_file.name} | {processed}/{total}")
                    self.root.update_idletasks()
                    continue

                # Build output filename
                if mode == "single":
                    new_name = f"{yymmdd}-{set_no}-{base}_{hm}_{nn}{ext}"
                else:
                    new_name = f"{yymmdd}-{stop}-{set_no}-{base}_{hm}_{nn}{ext}"

                # Output subfolder: keep legacy OUT/YYMMDD/NN layout
                out_dir = out_root / yymmdd / nn
                out_dir.mkdir(parents=True, exist_ok=True)
                dest = ensure_unique_path(out_dir / new_name)

                try:
                    shutil.copy2(b_file, dest)
                except Exception as e:
                    skipped += 1
                    processed += 1
                    self.progress["value"] = processed
                    self.status_var.set(f"오류({type(e).__name__}): {b_file.name} | {processed}/{total}")
                    self.root.update_idletasks()
                    continue

                processed += 1
                self.progress["value"] = processed

                # ETA
                elapsed = time.time() - t0
                rate = processed / elapsed if elapsed > 0 else 0
                remaining = (total - processed) / rate if rate > 0 else 0
                self.status_var.set(
                    f"처리: {processed}/{total} | 스킵: {skipped} | ETA ~{int(remaining)}s")
                self.root.update_idletasks()

            messagebox.showinfo(
                "완료",
                f"완료되었습니다.\n처리: {processed-skipped} / {total}\n스킵: {skipped}\n출력: {out_root / yymmdd}")

        except Exception as e:
            messagebox.showerror("오류", f"예상치 못한 오류: {type(e).__name__}: {e}")


def main():
    root = tk.Tk()
    App(root)
    root.mainloop()


if __name__ == "__main__":
    main()