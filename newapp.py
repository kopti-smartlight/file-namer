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

"""
Namer (Safe-Fast + Smooth UI) — 최종 통합판 ver3
- 규칙/출력:
  * Single  → YYMMDD-SET-BASE_{H|M}_NN.ext
  * Multi   → YYMMDD-STOP-SET-BASE_{H|M}_NN.ext
  * OUTPUT/YYMMDD/NN/ 에 복사, 중복 시 _dupN 부여
  * A/B 매칭: B의 Single/STOP/SET에 맞춰 A 동일 세트에서 NN의 H/M 판정(High 우선, H/M/High/Middle 허용)
- 안전:
  * copy2 + .part 임시파일 → os.replace() (원자적 완료)
  * 크기 검증 + fsync 시도 + 재시도 + 매니페스트 CSV + OK 사후 존재 검사
- 성능/UX:
  * os.scandir, 세트 단위 H/M 캐시, UI 메시지 스로틀, 디렉터리 생성 캐시
  * 워커 스레드 + Tk 메인스레드 50ms 갱신(프리즈 방지), 실행 중 버튼 잠금
- A 선택:
  * A는 “상위 폴더” 선택 → 그 바로 아래 날짜 폴더들(이름 숫자만 모은 뒤 끝 6자리) 중 B의 YYMMDD와 일치하는 폴더 자동 선택
  * Single 폴더는 대소문자 무시(single/Single)
"""

# -------- 정규식 -------- #
IMG_NUM_RE = re.compile(r"(?:^|_)(\d{2})(?=\D*$)")            # B 파일 끝 2자리 NN
# H/M 토큰: High/Middle 어디에 있어도, 한 글자 H/M은 단어경계에서만
HM_RE = re.compile(r"(?i)(high|middle|\bh\b|\bm\b)")
# A NN 추출: 1) 확장자 직전 '_NN' 우선  2) 없으면 오른쪽(끝) 기준 마지막 2자리
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
    """B 날짜 폴더 아래 모든 파일을 (mode, stop, set_no, file_path)로 생성."""
    # Single (대소문자 무시)
    single_dir = _find_child_dir_casefold(b_date_path, "single")
    if single_dir and single_dir.is_dir():
        with os.scandir(single_dir) as it_sets:
            for d in it_sets:
                if d.is_dir() and VALID_SET_RE.match(d.name or ""):
                    with os.scandir(Path(d.path)) as it_files:
                        for f in it_files:
                            if f.is_file():
                                yield ("single", None, d.name, Path(f.path))
    # Multi: 날짜 루트 바로 아래 4자리 폴더들 (Single 제외)
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

def parse_b_file_num_and_base(b_file: Path) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    'lumi_13.jpg' → ('13', 'lumi', '.jpg'); 실패 시 (None, None, None)
    NN 앞이 '_'이면 그것만 제거해서 base를 안전하게 자름.
    """
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
    """
    A 세트 폴더에서 {NN: 'H'|'M'} 캐시 생성.
    - NN은 1) 확장자 직전 '_NN' 우선
           2) 없으면 파일명 전체에서 '오른쪽(끝) 기준 마지막 2자리' 사용
    - 동일 NN에 H/M 둘 다 있으면 H 우선
    """
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
    """
    a_root 바로 아래 폴더들 중, 폴더명에서 숫자만 모은 문자열의 '끝 6자리'가 target_yymmdd와 같은 폴더 반환.
    우선순위: (1) 이름이 정확히 YYMMDD → (2) 숫자 8자리(YYYYMMDD) 끝 6자 일치 → (3) 그 외 일치
    """
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
                try:
                    tmp.unlink()
                except Exception:
                    pass

            shutil.copy2(src, tmp)

            if VERIFY_SIZE and (src.stat().st_size != tmp.stat().st_size):
                raise IOError("size mismatch")

            if FSYNC_AFTER_COPY:
                try:
                    with open(tmp, "rb") as f:
                        os.fsync(f.fileno())
                except Exception:
                    pass

            os.replace(tmp, dst_final)

            if FSYNC_AFTER_COPY and dir_fd is not None:
                try:
                    os.fsync(dir_fd)
                except Exception:
                    pass

            return True
        except Exception:
            if attempt < RETRY_MAX:
                time.sleep(RETRY_BACKOFF_BASE * attempt)
            else:
                try:
                    if tmp.exists():
                        tmp.unlink()
                except Exception:
                    pass
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

        tk.Label(root, text="원시 상위 폴더(A, 여러 날짜 포함)").grid(row=1, column=0, sticky="e", padx=5, pady=5)
        tk.Entry(root, textvariable=self.var_a_root, width=60).grid(row=1, column=1, padx=5)
        tk.Button(root, text="선택", command=self.select_a_root).grid(row=1, column=2, padx=5)

        tk.Label(root, text="출력 폴더").grid(row=2, column=0, sticky="e", padx=5, pady=5)
        tk.Entry(root, textvariable=self.var_out, width=60).grid(row=2, column=1, padx=5)
        tk.Button(root, text="선택", command=self.select_out).grid(row=2, column=2, padx=5)

        self.run_btn = tk.Button(root, text="실행", bg="lightgreen", width=20, command=self.run)
        self.run_btn.grid(row=3, column=1, pady=10)

        self.progress = ttk.Progressbar(root, length=500)
        self.progress.grid(row=4, column=0, columnspan=3, pady=5)

        tk.Label(root, textvariable=self.status_var).grid(row=5, column=0, columnspan=3, pady=5)

        self._q = queue.Queue()
        self._stats = {
            "total": 0,
            "processed": 0,
            "copied_ok": 0,
            "skipped": 0,
            "skip_no_num": 0,
            "skip_no_hm": 0,
            "skip_no_setdir": 0,
            "skip_empty_set": 0,
            "copy_errors": 0,
            "start_ts": 0.0,
            "done": False,
            "yymmdd": "",
            "manifest_path": "",
        }
        self._work_thread: Optional[threading.Thread] = None

    # --- 폴더 선택 --- #
    def select_b(self):
        folder = filedialog.askdirectory(title="컨버팅된 폴더(날짜) 선택")
        if folder:
            name = os.path.basename(folder)
            if is_date_folder(name):
                self.var_b.set(folder)
            else:
                messagebox.showerror("오류", "B는 날짜 폴더(YYMMDD 또는 YYYYMMDD)를 선택하세요.")

    def select_a_root(self):
        folder = filedialog.askdirectory(title="원시 상위 폴더(여러 날짜 포함) 선택")
        if folder:
            self.var_a_root.set(folder)

    def select_out(self):
        folder = filedialog.askdirectory(title="출력 폴더 선택")
        if folder:
            self.var_out.set(folder)

    # --- UI 주기 갱신 --- #
    def _ui_tick(self):
        last_msg = None
        try:
            while True:
                last_msg = self._q.get_nowait()
        except queue.Empty:
            pass
        if last_msg:
            self.status_var.set(last_msg)

        s = self._stats
        if s["total"] > 0:
            self.progress["maximum"] = s["total"]
            self.progress["value"] = s["processed"]
            elapsed = max(0.0, time.time() - s["start_ts"])
            rate = (s["processed"] / elapsed) if elapsed > 0 else 0.0
            remaining = int((s["total"] - s["processed"]) / rate) if rate > 0 else 0
            mm, ss = divmod(max(0, remaining), 60)
            if not last_msg:
                self.status_var.set(
                    f"처리: {s['processed']}/{s['total']} | 성공: {s['copied_ok']} | 스킵: {s['skipped']} | ETA {mm:02d}:{ss:02d}"
                )

        if s["done"]:
            manifest_info = f"\n매니페스트: {s['manifest_path']}" if s["manifest_path"] else ""
            messagebox.showinfo(
                "완료",
                (
                    f"완료되었습니다.\n"
                    f"성공: {s['copied_ok']} / {s['total']}\n"
                    f"스킵: {s['skipped']} "
                    f"(번호없음 {s['skip_no_num']}, H/M미확정 {s['skip_no_hm']}, "
                    f"세트폴더없음 {s['skip_no_setdir']}, 빈세트 {s['skip_empty_set']}, 복사오류 {s['copy_errors']})"
                    f"{manifest_info}\n"
                    f"출력: {Path(self.var_out.get()) / s['yymmdd']}"
                )
            )
            self.run_btn.config(state="normal")
            return

        self.root.after(UI_TICK_MS, self._ui_tick)

    # --- 실행 --- #
    def run(self):
        if self._work_thread and self._work_thread.is_alive():
            messagebox.showwarning("진행 중", "이미 실행 중입니다.")
            return

        try:
            b_date = Path(self.var_b.get())
            a_root = Path(self.var_a_root.get())
            out_root = Path(self.var_out.get())

            if not b_date.is_dir() or not a_root.is_dir() or not out_root.is_dir():
                messagebox.showerror("오류", "폴더 경로를 올바르게 선택하세요.")
                return

            b_name = b_date.name
            if not is_date_folder(b_name):
                messagebox.showerror("오류", "B는 날짜 폴더(YYMMDD 또는 YYYYMMDD)만 선택할 수 있습니다.")
                return

            yymmdd = to_yymmdd(b_name)
            a_date = find_a_date_dir(a_root, yymmdd)
            if not a_date:
                messagebox.showerror("오류", f"A 상위 폴더 아래에서 '{yymmdd}' 날짜 폴더를 찾지 못했습니다.")
                return

            files = list(list_all_b_files(b_date))
            total = len(files)
            if total == 0:
                messagebox.showinfo("정보", "처리할 파일이 없습니다.")
                return

            self._stats.update({
                "total": total,
                "processed": 0,
                "copied_ok": 0,
                "skipped": 0,
                "skip_no_num": 0,
                "skip_no_hm": 0,
                "skip_no_setdir": 0,
                "skip_empty_set": 0,
                "copy_errors": 0,
                "start_ts": time.time(),
                "done": False,
                "yymmdd": yymmdd,
                "manifest_path": "",
            })
            self.progress["maximum"] = total
            self.progress["value"] = 0

            def worker():
                hm_cache: Dict[tuple, Dict[str, str]] = {}
                created_dirs = set()
                manifest_rows: List[Tuple[str, str, str, str]] = []

                try:
                    for i, (mode, stop, set_no, b_file) in enumerate(files, 1):
                        nn, base, ext = parse_b_file_num_and_base(b_file)
                        if nn is None:
                            self._stats["skipped"] += 1
                            self._stats["skip_no_num"] += 1
                            self._stats["processed"] += 1
                            manifest_rows.append((str(b_file), "", "SKIP", "no_number"))
                            if (self._stats["processed"] % UI_MSG_EVERY) == 0:
                                self._q.put(f"스킵(번호X): {b_file.name} | {self._stats['processed']}/{total}")
                            continue

                        # A 세트 경로 (Single 대소문자 무시)
                        if mode == "single":
                            a_single_dir = _find_child_dir_casefold(a_date, "single") or (a_date / "Single")
                            a_set_path = a_single_dir / set_no
                            cache_key = ("single", None, set_no)
                        else:
                            a_set_path = a_date / (stop or "0000") / set_no
                            cache_key = ("multi", stop, set_no)

                        exists_set = a_set_path.is_dir()

                        mapping = hm_cache.get(cache_key)
                        if mapping is None:
                            mapping = build_hm_index_for_set(a_set_path)
                            hm_cache[cache_key] = mapping

                        hm = mapping.get(nn) if mapping else None
                        if hm is None:
                            self._stats["skipped"] += 1
                            # 세트 폴더 없음/빈세트/NN에 해당 H/M 없음 구분 + 빈세트는 샘플 기록
                            if not exists_set:
                                self._stats["skip_no_setdir"] += 1
                                reason = "no_set_dir"
                            elif not mapping:
                                self._stats["skip_empty_set"] += 1
                                # 세트 내 파일 샘플 3개를 reason 뒤에 붙여 기록
                                sample = []
                                try:
                                    with os.scandir(a_set_path) as it2:
                                        for j, ff in enumerate(it2):
                                            if j >= 3:
                                                break
                                            if ff.is_file():
                                                sample.append(ff.name)
                                except Exception:
                                    pass
                                reason = "empty_set" + (f":{';'.join(sample)}" if sample else "")
                            else:
                                self._stats["skip_no_hm"] += 1
                                reason = "no_HM"
                            self._stats["processed"] += 1
                            manifest_rows.append((str(b_file), "", "SKIP", reason))
                            if (self._stats["processed"] % UI_MSG_EVERY) == 0:
                                self._q.put(f"스킵({reason}): {b_file.name} | {self._stats['processed']}/{total}")
                            continue

                        # 출력 파일명
                        if mode == "single":
                            new_name = f"{yymmdd}-{set_no}-{base}_{hm}_{nn}{ext}"
                        else:
                            new_name = f"{yymmdd}-{stop}-{set_no}-{base}_{hm}_{nn}{ext}"

                        out_dir = out_root / yymmdd / nn
                        out_dir_s = str(out_dir)
                        if out_dir_s not in created_dirs:
                            out_dir.mkdir(parents=True, exist_ok=True)
                            created_dirs.add(out_dir_s)

                        dest = ensure_unique_path_fast(out_dir / new_name)

                        ok = safe_copy2_atomic(b_file, dest) if ATOMIC_COPY else _fallback_copy2(b_file, dest)
                        if not ok:
                            self._stats["skipped"] += 1
                            self._stats["copy_errors"] += 1
                            manifest_rows.append((str(b_file), str(dest), "ERROR", "copy_failed"))
                        else:
                            self._stats["copied_ok"] += 1
                            manifest_rows.append((str(b_file), str(dest), "OK", ""))

                        self._stats["processed"] += 1
                        if (self._stats["processed"] % UI_MSG_EVERY) == 0:
                            self._q.put(
                                f"처리: {self._stats['processed']}/{total} | 성공: {self._stats['copied_ok']} | 스킵: {self._stats['skipped']}"
                            )

                    # 매니페스트 저장
                    manifest_dir = out_root / yymmdd
                    manifest_dir.mkdir(parents=True, exist_ok=True)
                    manifest_path = manifest_dir / "_manifest.csv"
                    try:
                        with open(manifest_path, "w", newline="", encoding="utf-8") as f:
                            writer = csv.writer(f)
                            writer.writerow(["src", "dest", "status", "reason"])
                            writer.writerows(manifest_rows)
                        self._stats["manifest_path"] = str(manifest_path)
                    except Exception:
                        self._stats["manifest_path"] = ""

                    # OK 표기 파일 존재 검증
                    missing_after_ok = 0
                    for _, dest, status, _ in manifest_rows:
                        if status == "OK" and (not dest or not Path(dest).is_file()):
                            missing_after_ok += 1
                    if missing_after_ok > 0:
                        self._q.put(f"경고: OK 표시건 {missing_after_ok}개가 누락되었습니다. 매니페스트 확인 요망.")
                finally:
                    self._stats["done"] = True

            self.run_btn.config(state="disabled")
            self._work_thread = threading.Thread(target=worker, daemon=True)
            self._work_thread.start()
            self._ui_tick()

        except Exception as e:
            self.run_btn.config(state="normal")
            messagebox.showerror("오류", f"예상치 못한 오류: {type(e).__name__}: {e}")

def main():
    root = tk.Tk()
    App(root)
    root.mainloop()

if __name__ == "__main__":
    main()
