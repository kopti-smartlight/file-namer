import os
import shutil
import re
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import time
import subprocess
import sys

def select_b_folder():
    folder = filedialog.askdirectory(title="컨버트된 폴더 선택")
    if folder:
        folder_name = os.path.basename(folder)
        if re.fullmatch(r"\d{6}", folder_name):
            confirm = messagebox.askyesno("확인", f"컨버팅된 폴더 날짜가 {folder_name}로 맞습니까?")
            if confirm:
                b_folder_var.set(folder)
        else:
            messagebox.showerror("오류", "폴더 이름 형식이 틀립니다. 수정해주세요. 예시: 250527")

def select_a_folder():
    folder = filedialog.askdirectory(title="원시데이터 폴더 선택")
    if folder:
        a_folder_var.set(folder)

def select_output_folder():
    folder = filedialog.askdirectory(title="출력 폴더 선택")
    if folder:
        output_folder_var.set(folder)

def count_total_files(b_date_path):
    count = 0
    for b_number in os.listdir(b_date_path):
        b_dir_path = os.path.join(b_date_path, b_number)
        if os.path.isdir(b_dir_path):
            count += len([
                f for f in os.listdir(b_dir_path)
                if os.path.isfile(os.path.join(b_dir_path, f))
            ])
    return count


def seconds_to_min_sec(seconds):
    minutes = seconds // 60
    sec = seconds % 60
    return f"{int(minutes)}분 {int(sec)}초"

def open_folder(path):
    try:
        if sys.platform.startswith('win'):
            os.startfile(path)
        elif sys.platform.startswith('darwin'):
            subprocess.Popen(['open', path])
        else:
            subprocess.Popen(['xdg-open', path])
    except Exception as e:
        print(f"폴더 열기 실패: {e}")

def process_files():
    b_date_path = b_folder_var.get()  # 이제 사용자가 날짜 폴더 하나만 선택함
    a_root = a_folder_var.get()
    output_root = output_folder_var.get()

    if not all([b_date_path, a_root, output_root]):
        messagebox.showerror("오류", "모든 폴더를 선택하세요.")
        return

    total_files = count_total_files(b_date_path)
    if total_files == 0:
        messagebox.showerror("오류", "컨버트 폴더에 처리할 파일이 없습니다.")
        return

    progress_bar["maximum"] = total_files
    progress_bar["value"] = 0
    status_var.set("처리 시작...")

    start_time = time.time()
    processed_files = 0

    b_date = os.path.basename(b_date_path)  # 예: '250527'
    a_date = '20' + b_date  # '250527' → '20250527'

    for b_number in os.listdir(b_date_path):
        b_dir_path = os.path.join(b_date_path, b_number)
        if not os.path.isdir(b_dir_path):
            continue

        a_dir_path = os.path.join(a_root, a_date, "Single", b_number)
        if not os.path.exists(a_dir_path):
            print(f"a폴더에 {b_number} 폴더 없음, 건너뜀")
            continue

        a_files = os.listdir(a_dir_path)
        has_middle = any("middle" in f.lower() for f in a_files)
        has_high = any("high" in f.lower() for f in a_files)

        for b_file in os.listdir(b_dir_path):
            b_file_path = os.path.join(b_dir_path, b_file)
            if not os.path.isfile(b_file_path):
                print(f"[SKIP] 파일이 아님: {b_file_path}")
                continue

            #suffix = ""
            #if has_middle:
            #    suffix = "_M"
            #elif has_high:
            #    suffix = "_H"
            
            # b_file에서 _뒤 숫자 두 자리 추출
            name_only = os.path.splitext(b_file)[0]
            match = re.match(r"(.+)_([0-9]{2})$", name_only)
            if not match:
                print(f"[SKIP] 이름 형식 안 맞음: {b_file}")
                continue

            base_name, file_num = match.groups()

            # 해당 번호 포함된 a 파일 중 high/middle 여부 판단
            suffix = ""
            for a_file in a_files:
                if file_num in a_file:
                    lower_name = a_file.lower()
                    if "high" in lower_name:
                        suffix = "_H"
                    elif "middle" in lower_name:
                        suffix = "_M"
                    break  # 일치하는 파일 하나만 확인하면 됨
            
            name_only = os.path.splitext(b_file)[0]
            match = re.match(r"(.+)_([0-9]{2})$", name_only)
            if not match:
                print(f"[SKIP] 이름 형식 안 맞음: {b_file}")
                continue

            base_name, file_num = match.groups()
            new_file_name = f"{b_date}-{b_number}-{base_name}{suffix}_{file_num}.{b_file.split('.')[-1]}"

            output_subdir = os.path.join(output_root, b_date, file_num)
            os.makedirs(output_subdir, exist_ok=True)
            output_file_path = os.path.join(output_subdir, new_file_name)

            print(f"[COPY] {b_file_path} → {output_file_path}")
            shutil.copy2(b_file_path, output_file_path)

            processed_files += 1
            progress_bar["value"] = processed_files

            elapsed = time.time() - start_time
            avg_time = elapsed / processed_files if processed_files > 0 else 0
            remaining = total_files - processed_files
            est_remaining_time_sec = int(avg_time * remaining)
            est_time_str = seconds_to_min_sec(est_remaining_time_sec)

            status_var.set(f"진행: {processed_files}/{total_files} | 예상 남은 시간: {est_time_str}")
            root.update_idletasks()

    status_var.set("처리 완료!")
    messagebox.showinfo("완료", "파일 처리가 완료되었습니다.")
    open_folder(output_root)

# ------------------ GUI ------------------

root = tk.Tk()
root.title("빛공해이미지파일 분류기")

b_folder_var = tk.StringVar()
a_folder_var = tk.StringVar()
output_folder_var = tk.StringVar()
status_var = tk.StringVar()

tk.Label(root, text="컨버트된 폴더:").grid(row=0, column=0, sticky='e')
tk.Entry(root, textvariable=b_folder_var, width=60).grid(row=0, column=1)
tk.Button(root, text="선택", command=select_b_folder).grid(row=0, column=2)

tk.Label(root, text="원시데이터 폴더:").grid(row=1, column=0, sticky='e')
tk.Entry(root, textvariable=a_folder_var, width=60).grid(row=1, column=1)
tk.Button(root, text="선택", command=select_a_folder).grid(row=1, column=2)

tk.Label(root, text="출력 폴더:").grid(row=2, column=0, sticky='e')
tk.Entry(root, textvariable=output_folder_var, width=60).grid(row=2, column=1)
tk.Button(root, text="선택", command=select_output_folder).grid(row=2, column=2)

tk.Button(root, text="실행", command=process_files, bg="lightgreen", width=20).grid(row=3, column=1, pady=10)

progress_bar = ttk.Progressbar(root, length=400)
progress_bar.grid(row=4, column=0, columnspan=3, pady=5)

tk.Label(root, textvariable=status_var).grid(row=5, column=0, columnspan=3)

root.mainloop()
