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
import errno

"""
Namer (Safe-Fast + Smooth UI) — 카메라별 분류 토글 지원 최종판

모드
- 카메라별분류(기본 ✓ 체크): 출력폴더에 YYMMDD/NN 구조로 정리하여 '잘라내기(이동/rename)' 후 규칙 이름으로 변경
- 카메라별분류안함(체크 해제): 컨버팅(B) 폴더 자리에서 '이름만 변경', 폴더 이동 없음

매칭/규칙(동일)
- Single  → YYMMDD-SET-BASE_{H|M}_NN.ext
- Multi   → YYMMDD-STOP-SET-BASE_{H|M}_NN.ext
- H/M 토큰: High/Middle/H/M (대소문자 무시)
- A의 NN: 1) 확장자 직전 '_NN' 우선, 2) 없으면 파일명 전체 오른쪽(끝) 기준 마지막 2자리
- B의 NN: 파일명 끝의 언더스코어+2자리 (…_NN.ext)

안전/성능
- 이름 충돌 시 _dupN 부여
- (카메라별분류 모드) 같은 디스크면 os.replace(초고속), 아니면 shutil.move(복사+삭제)
- 매니페스트 CSV + 진행/ETA 표시 + UI 스레드 분리
"""

# -------- 정규식 -------- #
IMG_NUM_RE   = re.compile(r"(?:^|_)(\d{2})(?=\D*$)")            # B 파일 끝 2자리 NN
HM_RE        = re.compile(r"(?i)(high|middle|\bh\b|\bm\b)")     # H/M 토큰
A_NN_END_RE  = re.compile(r"_(\d{2})(?=\.[^.]+$)", re.IGNORECASE)
A_NN_ANY_RE  = re.compile(r"(\d{2})(?!\d)")

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

# -------- 툴팁 위젯 -------- #
class ToolTip:
    def __init__(self, widget, text: str, delay_ms: int = 200):
        self.widget = widget
        self.text = text
        self.delay_ms = delay_ms
        self.tipwin: Optional[tk.Toplevel] = None
        self._after_id = None
        widget.bind("<Enter>", self._enter)
        widget.bind("<Leave>", self._leave)

    def _enter(self, _):
        self._schedule()

    def _leave(self, _):
        self._unschedule()
        self._hide()

    def _schedule(self):
        self._unschedule()
        self._after_id = self.widget.after(self.delay_ms, self._show)

    def _unschedule(self):
        if self._after_id:
            self.widget.after_cancel(self._after_id)
            self._after_id = None

    def _show(self):
        if self.tipwin or not self.text:
            return
        x = self.widget.winfo_rootx() + 20
        y = self.widget.winfo_rooty() + self.widget.winfo_height() + 8
        self.tipwin = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{x}+{y}")
        label = tk.Label(
            tw, text=self.text, justify="left",
            background="#ffffe0", relief="solid", borderwidth=1,
            font=("Segoe UI", 9), padx=6, pady=4
        )
        label.pack()

    def _hide(self):
        if self.tipwin:
            self.tipwin.destroy()
            self.tipwin = None

# -------- 유틸 -------- #
def is_date_folder(name: str) -> bool:
    return bool(re.fullmatch(r"\d{6}|\d{8}", name))

def to_yymmdd(date_folder_name: str) -> str:
    return date_folder_name[-6:]

def ensure_unique_path_fast(dest: Path) -> Path:
    """dest가 존재하면 _dupN을 부여하여 고유 경로 반환."""
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
    """parent 아래에서 이름 대소문자 무시로 하위 디렉터리 찾기"""
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
    # Single
    single_dir = _find_child_dir_casefold(b_date_path, "single")
    if single_dir and single_dir.is_dir():
        with os.scandir(single_dir) as it_sets:
            for d in it_sets:
                if d.is_dir() and VALID_SET_RE.match(d.name or ""):
                    with os.scandir(Path(d.path)) as it_files:
                        for f in it_files:
                            if f.is_file():
                                yield ("single", None, d.name, Path(f.path))
    # Multi
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
    """'lumi_13.jpg' → ('13', 'lumi', '.jpg'); 실패 시 (None, None, None)"""
    m = IMG_NUM_RE.search(b_file.stem)
    if not m:
        return None, None, None
    nn = m.group(1)
    stem = b_file.stem
    start = m.start(1)
    # NN 앞에 '_'이면 제외, 아니면 그대로
    base = stem[: start - 1] if start > 0 and stem[start - 1] == "_" else stem[: start]
    base = base.rstrip("_")
    return nn, base, b_file.suffix

def build_hm_index_for_set(a_set_path: Path) -> Dict[str, str]:
    """
    A 세트 폴더에서 {NN: 'H'|'M'} 캐시 생성.
    - NN: 1) 확장자 직전 '_NN' 우선  2) 없으면 파일명 오른쪽(끝) 기준 마지막 2자리
    - H/M: High/Middle/H/M (대소문자 무시, H/M은 단어경계)
    - 동일 NN에 H와 M이 있으면 H 우선
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
    a_root 바로 아래 폴더들 중, 폴더명에서 숫자만 모은 문자열의 끝 6자리가 target_yymmdd와 같은 폴더 반환.
    우선순위: 정확히 YYMMDD > 8자리(YYYYMMDD) 끝6자 일치 > 그 외 일치
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

# -------- 이동/복사 -------- #
def _same_device(path1: Path, path2: Path) -> bool:
    try:
        return os.stat(path1).st_dev == os.stat(path2).st_dev
    except Exception:
        return False

def fast_rename_or_move(src: Path, dest: Path) -> bool:
    """
    같은 디스크면 os.replace(초고속 rename), 다르면 shutil.move(복사+삭제).
    dest 부모 디렉터리는 호출부에서 이미 존재해야 함.
    """
    try:
        if _same_device(src, dest.parent):
            os.replace(src, dest)
            return True
        shutil.move(str(src), str(dest))
        return True
    except OSError as e:
        if e.errno in (errno.EBUSY, errno.EACCES):
            try:
                time.sleep(0.05)
                if _same_device(src, dest.parent):
                    os.replace(src, dest)
                else:
                    shutil.move(str(src), str(dest))
                return True
            except Exception:
                return False
        return False
    except Exception:
        return False

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

# -------- GUI -------- #
class App:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Namer (Safe-Fast)")

        self.var_b = tk.StringVar()
        self.var_a_root = tk.StringVar()
        self.var_out = tk.StringVar()
        self.status_var = tk.StringVar(value="대기 중…")

        # 입력 UI
        tk.Label(root, text="컨버팅 폴더(B, 날짜)").grid(row=0, column=0, sticky="e", padx=6, pady=6)
        tk.Entry(root, textvariable=self.var_b, width=60).grid(row=0, column=1, padx=6)
        tk.Button(root, text="선택", command=self.select_b).grid(row=0, column=2, padx=6)

        tk.Label(root, text="원시 상위 폴더(A)").grid(row=1, column=0, sticky="e", padx=6, pady=6)
        tk.Entry(root, textvariable=self.var_a_root, width=60).grid(row=1, column=1, padx=6)
        tk.Button(root, text="선택", command=self.select_a_root).grid(row=1, column=2, padx=6)

        tk.Label(root, text="출력 폴더").grid(row=2, column=0, sticky="e", padx=6, pady=6)
        self.out_entry = tk.Entry(root, textvariable=self.var_out, width=60)
        self.out_entry.grid(row=2, column=1, padx=6)
        self.out_btn = tk.Button(root, text="선택", command=self.select_out)
        self.out_btn.grid(row=2, column=2, padx=6)

        # 모드 토글 (체크: 카메라별분류 / 해제: 분류안함-제자리이름변경)
        self.camera_sort_var = tk.BooleanVar(value=True)  # 기본: 카메라별분류
        chk = tk.Checkbutton(root, text="카메라별분류", variable=self.camera_sort_var, command=self._on_mode_toggle)
        chk.grid(row=3, column=1, sticky="w", padx=6, pady=4)

        # ? 도움말 (툴팁)
        q_label = tk.Label(root, text="?", fg="#005bbb", cursor="question_arrow")
        q_label.grid(row=3, column=1, sticky="w", padx=120)  # 체크박스 오른쪽
        ToolTip(q_label,
                "모드 설명\n"
                "• 카메라별분류(체크): 출력폴더/YYMMDD/NN/ 아래로 ‘잘라내기(이동)’하여 이름을 규칙대로 변경합니다.\n"
                "• 카메라별분류안함(해제): 컨버팅 폴더 자리에 그대로 두고 파일명만 규칙대로 변경합니다.\n"
                "   (이 경우 출력폴더는 사용하지 않습니다.)")

        # 실행 버튼/진행바/상태
        self.run_btn = tk.Button(root, text="실행", bg="lightgreen", width=20, command=self.run)
        self.run_btn.grid(row=4, column=1, pady=10)

        self.progress = ttk.Progressbar(root, length=520)
        self.progress.grid(row=5, column=0, columnspan=3, pady=5)

        tk.Label(root, textvariable=self.status_var).grid(row=6, column=0, columnspan=3, pady=5)

        # 공유 상태
        self._q = queue.Queue()
        self._stats = {}
        self._work_thread: Optional[threading.Thread] = None

        # 초기 모드 반영
        self._on_mode_toggle()

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
        folder = filedialog.askdirectory(title="원시 상위 폴더 선택")
        if folder:
            self.var_a_root.set(folder)

    def select_out(self):
        folder = filedialog.askdirectory(title="출력 폴더 선택")
        if folder:
            self.var_out.set(folder)

    # --- 모드 토글 UI 반영 --- #
    def _on_mode_toggle(self):
        camera_sort = self.camera_sort_var.get()
        # 카메라별분류 모드가 아니면 출력폴더 비활성/미사용
        state = "normal" if camera_sort else "disabled"
        self.out_entry.config(state=state)
        self.out_btn.config(state=state)

    # --- UI 주기 갱신 --- #
    def _ui_tick(self):
        # 메시지 큐 압축 표시
        last_msg = None
        try:
            while True:
                last_msg = self._q.get_nowait()
        except queue.Empty:
            pass
        if last_msg:
            self.status_var.set(last_msg)

        # 진행도/ETA
        s = self._stats
        total = s.get("total", 0)
        if total > 0:
            self.progress["maximum"] = total
            self.progress["value"] = s.get("processed", 0)
            elapsed = max(0.0, time.time() - s.get("start_ts", time.time()))
            rate = (s.get("processed", 0) / elapsed) if elapsed > 0 else 0.0
            remaining = int((total - s.get("processed", 0)) / rate) if rate > 0 else 0
            mm, ss = divmod(max(0, remaining), 60)
            if not last_msg:
                self.status_var.set(
                    f"처리: {s.get('processed',0)}/{total} | 성공: {s.get('copied_ok',0)} | "
                    f"스킵: {s.get('skipped',0)} | ETA {mm:02d}:{ss:02d}"
                )

        # 완료 알림
        if s.get("done", False):
            manifest_info = f"\n매니페스트: {s.get('manifest_path','')}" if s.get("manifest_path") else ""
            messagebox.showinfo(
                "완료",
                (
                    f"완료되었습니다.\n"
                    f"성공: {s.get('copied_ok',0)} / {s.get('total',0)}\n"
                    f"스킵: {s.get('skipped',0)} "
                    f"(번호없음 {s.get('skip_no_num',0)}, H/M미확정 {s.get('skip_no_hm',0)}, "
                    f"세트폴더없음 {s.get('skip_no_setdir',0)}, 빈세트 {s.get('skip_empty_set',0)}, "
                    f"이동/복사오류 {s.get('copy_errors',0)})"
                    f"{manifest_info}"
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

        b_date = Path(self.var_b.get())
        a_root = Path(self.var_a_root.get())
        camera_sort = self.camera_sort_var.get()

        # 경로 검사
        if not (b_date.is_dir() and a_root.is_dir()):
            messagebox.showerror("오류", "컨버팅 폴더 또는 원시 상위 폴더 경로를 확인하세요.")
            return
        b_name = b_date.name
        if not is_date_folder(b_name):
            messagebox.showerror("오류", "B는 날짜 폴더(YYMMDD 또는 YYYYMMDD)만 선택할 수 있습니다.")
            return

        # 카메라별분류 모드일 때만 출력폴더 필수
        out_root = None
        if camera_sort:
            out_root = Path(self.var_out.get())
            if not out_root.is_dir():
                messagebox.showerror("오류", "출력 폴더 경로를 확인하세요.")
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

        # 통계 초기화
        self._stats = {
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
        }
        self.progress["maximum"] = total
        self.progress["value"] = 0

        # 워커
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

                    # A 세트 경로
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
                        if not exists_set:
                            self._stats["skip_no_setdir"] += 1
                            reason = "no_set_dir"
                        elif not mapping:
                            self._stats["skip_empty_set"] += 1
                            # 샘플 파일명 3개 첨부
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

                    # 출력 이름
                    if mode == "single":
                        new_name = f"{yymmdd}-{set_no}-{base}_{hm}_{nn}{ext}"
                    else:
                        new_name = f"{yymmdd}-{stop}-{set_no}-{base}_{hm}_{nn}{ext}"

                    if camera_sort:
                        # 카메라별분류: 출력폴더/YYMMDD/NN/ 로 이동(잘라내기)
                        out_dir = out_root / yymmdd / nn  # type: ignore
                        out_dir_s = str(out_dir)
                        if out_dir_s not in created_dirs:
                            out_dir.mkdir(parents=True, exist_ok=True)
                            created_dirs.add(out_dir_s)
                        dest = ensure_unique_path_fast(out_dir / new_name)
                        ok = fast_rename_or_move(b_file, dest)
                        if ok:
                            self._stats["copied_ok"] += 1
                            manifest_rows.append((str(b_file), str(dest), "OK", "moved"))
                        else:
                            self._stats["skipped"] += 1
                            self._stats["copy_errors"] += 1
                            manifest_rows.append((str(b_file), str(dest), "ERROR", "move_failed"))
                    else:
                        # 분류안함: 제자리 이름 변경(같은 디렉터리)
                        dest = ensure_unique_path_fast(b_file.parent / new_name)
                        try:
                            os.replace(b_file, dest)  # 같은 디렉터리여서 초고속
                            self._stats["copied_ok"] += 1
                            manifest_rows.append((str(b_file), str(dest), "OK", "renamed_inplace"))
                        except Exception:
                            self._stats["skipped"] += 1
                            self._stats["copy_errors"] += 1
                            manifest_rows.append((str(b_file), str(dest), "ERROR", "rename_inplace_failed"))

                    self._stats["processed"] += 1
                    if (self._stats["processed"] % UI_MSG_EVERY) == 0:
                        self._q.put(
                            f"처리: {self._stats['processed']}/{total} | 성공: {self._stats['copied_ok']} | 스킵: {self._stats['skipped']}"
                        )

                # 매니페스트 저장 (항상 생성)
                manifest_dir = (out_root / yymmdd) if camera_sort else b_date
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

                # OK 파일 존재 검증
                missing_after_ok = 0
                for _, dest, status, _ in manifest_rows:
                    if status == "OK" and (not dest or not Path(dest).is_file()):
                        missing_after_ok += 1
                if missing_after_ok > 0:
                    self._q.put(f"경고: OK 표시건 {missing_after_ok}개가 누락되었습니다. 매니페스트 확인 요망.")
            finally:
                self._stats["done"] = True

        # 실행
        self.run_btn.config(state="disabled")
        self._work_thread = threading.Thread(target=worker, daemon=True)
        self._work_thread.start()
        self._ui_tick()

def main():
    root = tk.Tk()
    App(root)
    root.mainloop()

if __name__ == "__main__":
    main()
