import requests
import ttkbootstrap as ttk
import tkinter as tk
from tkinter import filedialog, messagebox, simpledialog
import tkinter.font as font
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urljoin, quote, unquote, urlparse
import json
from pathlib import Path
import threading
import concurrent.futures
import logging
from logging.handlers import QueueHandler
import queue
import configparser
from datetime import datetime
import re

CONFIG_FILE = 'config.ini'
CONFIG = {
    'BASE_URLS': [
        'https://rguk.ru/students/schedule/',
        'https://rguk.ru/upload/iblock/'
    ],
    'HEADERS': {'User-Agent': 'Mozilla/5.0'},
    'FIO_JSON': 'teachers.json',
    'MAX_WORKERS': 4,
    'OVERWRITE_CSV': False
}

log_queue = queue.Queue()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S',
                    handlers=[])
logger = logging.getLogger(__name__)
queue_handler = QueueHandler(log_queue)
logger.addHandler(queue_handler)


def show_vpn_warning():
    warning_file = Path('warning_shown.txt')
    if not warning_file.exists():
        messagebox.showwarning("Предупреждение",
                               "Эта программа работает без VPN. Использование VPN может привести к некорректной работе.")
        warning_file.touch()


def format_teacher_name(name):
    if is_already_formatted(name):
        return name
    parts = name.strip().split()
    if len(parts) < 2:
        return name
    last_name = parts[0]
    initials = [part[0] + '.' for part in parts[1:] if part]
    return f"{last_name} {''.join(initials)}"


def is_already_formatted(name):
    return bool(re.match(r'^[А-Яа-яЁё]+\s+[А-ЯЁ]\.[А-ЯЁ]\.$', name))


def load_config():
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    return config


def save_config(folder_path):
    config = configparser.ConfigParser()
    config['DEFAULT'] = {'LastFolder': str(folder_path)}
    with open(CONFIG_FILE, 'w') as configfile:
        config.write(configfile)


def validate_folder(folder_path):
    folder = Path(folder_path)
    try:
        if not folder.exists():
            folder.mkdir(parents=True, exist_ok=True)
        test_file = folder / '.test_write'
        test_file.touch()
        test_file.unlink()
        return True
    except (PermissionError, OSError) as e:
        logger.error(f"Ошибка доступа к папке {folder}: {e}")
        return False


def download_file(file_url: str, save_path: Path, log_func: callable, cancel_event: threading.Event) -> Path | None:
    try:
        filename = Path(file_url).name
        encoded_url = quote(file_url, safe='/:')
        full_path = save_path / filename
        log_func(f"Начинается загрузка: {filename} ({encoded_url})")

        head_response = requests.head(encoded_url, headers=CONFIG['HEADERS'], allow_redirects=False)
        head_response.raise_for_status()
        if head_response.status_code in (301, 302):
            log_func(f"[Ошибка] Редирект обнаружен для {filename}. URL: {encoded_url}")
            return None

        expected_size = int(head_response.headers.get('Content-Length', 0))
        content_type = head_response.headers.get('Content-Type', '').lower()
        if not content_type.startswith('application/vnd.openxmlformats') and not content_type.startswith(
                'application/vnd.ms-excel'):
            log_func(f"[Предупреждение] Несоответствие типа содержимого для {filename}: {content_type}")
            response = requests.get(encoded_url, headers=CONFIG['HEADERS'])
            if 'text/html' in content_type:
                soup = BeautifulSoup(response.text, 'html.parser')
                error_message = soup.find('title') or soup.find('h1')
                error_text = error_message.get_text() if error_message else "Неизвестная ошибка"
                log_func(f"[Детали ошибки] Сервер вернул HTML: {error_text}")
            return None

        if full_path.exists():
            local_size = full_path.stat().st_size
            if local_size == expected_size:
                log_func(f"[Пропущен] {filename} — уже загружен и размер совпадает")
                return None

        with requests.get(encoded_url, headers=CONFIG['HEADERS'], stream=True, allow_redirects=False) as r:
            r.raise_for_status()
            with open(full_path, 'wb') as f:
                total_size = 0
                for chunk in r.iter_content(chunk_size=8192):
                    if cancel_event.is_set():
                        log_func(f"[Отменено] Загрузка {filename}")
                        if full_path.exists():
                            full_path.unlink()
                        return None
                    if chunk:
                        f.write(chunk)
                        total_size += len(chunk)
                if total_size != expected_size and expected_size > 0:
                    log_func(
                        f"[Ошибка] Размер файла {filename} не совпадает: ожидалось {expected_size}, получено {total_size}")
                    full_path.unlink()
                    return None
        log_func(f"[Скачан] {filename} (размер: {total_size} байт)")
        return full_path
    except requests.exceptions.RequestException as e:
        log_func(f"[Ошибка сети] {filename}: {e}")
        return None
    except OSError as e:
        log_func(f"[Ошибка файла] {filename}: {e}")
        return None


def download_excel_files(save_path, log_func, progress_callback=None, cancel_event=None):
    save_path = Path(save_path)
    if not validate_folder(save_path):
        log_func("Ошибка: Нет доступа к папке для сохранения.")
        return []

    all_links = []
    for base_url in CONFIG['BASE_URLS']:
        try:
            response = requests.get(base_url, headers=CONFIG['HEADERS'])
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            links = [urljoin(base_url, link['href']) for link in soup.find_all('a', href=True)
                     if link['href'].lower().endswith(('.xls', '.xlsx'))]
            links = [link for link in links if 'view.officeapps.live.com' not in link]
            for link in soup.find_all('a', href=True):
                href = link['href']
                if 'view.officeapps.live.com' in href and 'src=' in href:
                    src_url = unquote(urlparse(href).query.split('src=')[1].split('&')[0])
                    if src_url.lower().endswith(('.xls', '.xlsx')):
                        links.append(src_url)
            all_links.extend(links)
            log_func(f"Найдено {len(links)} ссылок на Excel-файлы на странице {base_url}")
        except requests.exceptions.RequestException as e:
            log_func(f"Ошибка загрузки страницы {base_url}: {e}")
            continue

    if not all_links:
        log_func("⚠ Не найдено ссылок на Excel-файлы.")
        return []

    downloaded_files = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONFIG['MAX_WORKERS']) as executor:
        future_to_url = {executor.submit(download_file, url, save_path, log_func, cancel_event): url for url in
                         all_links}
        for future in concurrent.futures.as_completed(future_to_url):
            if cancel_event.is_set():
                log_func("[Отменено] Загрузка всех файлов")
                break
            result = future.result()
            if result:
                downloaded_files.append(result)
            if progress_callback:
                progress_callback(len(downloaded_files) / max(len(all_links), 1))
    return downloaded_files


def convert_to_csv(xl_file, log_func, cancel_event=None):
    xl_file = Path(xl_file)
    base_dir = xl_file.parent
    base_name = xl_file.stem
    csv_files = []
    try:
        log_func(f"Начало конвертации файла: {xl_file}")
        xls = pd.ExcelFile(xl_file)
        log_func(f"Найдено листов: {len(xls.sheet_names)}")
        for sheet in xls.sheet_names:
            if cancel_event and cancel_event.is_set():
                log_func(f"[Отменено] Конвертация {xl_file}")
                return csv_files
            csv_name = base_dir / f"{base_name}_{sheet}.csv"
            if csv_name.exists() and not CONFIG['OVERWRITE_CSV']:
                log_func(f"[Пропущено] CSV уже существует: {csv_name}")
                csv_files.append(csv_name)
                continue
            try:
                df = pd.read_excel(xl_file, sheet_name=sheet, engine='openpyxl')
                if df.empty:
                    log_func(f"[Пропущено] Лист '{sheet}' в {xl_file} пуст")
                    continue
                df.to_csv(csv_name, index=False, encoding='utf-8')
                csv_files.append(csv_name)
                log_func(f"[CSV создан] {csv_name} (строк: {len(df)})")
            except Exception as e:
                log_func(f"[Ошибка конвертации листа] {xl_file}, лист '{sheet}': {e}")
                continue
    except Exception as e:
        log_func(f"[Ошибка открытия файла] {xl_file}: {e}")
    return csv_files


def search_teachers_in_csv(csv_files, teacher_list, log_func, progress_callback=None, cancel_event=None):
    if not teacher_list:
        log_func("Ошибка: Список преподавателей пуст.")
        return []
    teacher_pattern = re.compile('|'.join(map(re.escape, teacher_list)), re.IGNORECASE)
    results = []
    for i, csv_file in enumerate(csv_files):
        if cancel_event and cancel_event.is_set():
            log_func("[Отменено] Поиск в CSV")
            break
        try:
            df = pd.read_csv(csv_file)
            log_func(f"Заголовки столбцов в {csv_file}: {list(df.columns)}")
            for _, row in df.iterrows():
                if cancel_event and cancel_event.is_set():
                    log_func("[Отменено] Поиск в CSV")
                    break
                row_dict = row.to_dict()
                for col, value in row_dict.items():
                    if isinstance(value, str) and teacher_pattern.search(value):
                        matching_teachers = [t for t in teacher_list if t.lower() in value.lower()]
                        if matching_teachers:
                            even_week = {
                                'День': row_dict.get('Unnamed: 1', ''),
                                'Время': row_dict.get('Unnamed: 3', ''),
                                'Аудитория': row_dict.get('Unnamed: 4', ''),
                                'Тип': row_dict.get('Unnamed: 5', ''),
                                'Преподаватель': row_dict.get('Unnamed: 6', ''),
                                'Предмет': row_dict.get('Unnamed: 7', '')
                            }
                            odd_week = {
                                'День': row_dict.get('Unnamed: 13', row_dict.get('Unnamed: 1', '')),
                                'Время': row_dict.get('Unnamed: 12', ''),
                                'Аудитория': row_dict.get('Unnamed: 11', ''),
                                'Тип': row_dict.get('Unnamed: 10', ''),
                                'Преподаватель': row_dict.get('Unnamed: 9', ''),
                                'Предмет': row_dict.get('Unnamed: 8', '')
                            }
                            results.append({
                                'Преподаватель': matching_teachers[0],
                                'Группа': Path(csv_file).stem.split('_')[0],
                                'Четная неделя': even_week,
                                'Нечетная неделя': odd_week
                            })
        except Exception as e:
            log_func(f"[Ошибка CSV] {csv_file}: {e}")
        if progress_callback:
            progress_callback(i + 1, len(csv_files))
    return results


def format_results(results):
    if not results:
        return "Нет результатов."
    output = [f"Найдено совпадений: {len(results)}\n"]
    for result in results:
        teacher = result['Преподаватель']
        group = result['Группа']
        output.append(f"Преподаватель: {teacher}\nГруппа: {group}\n")
        even_details = [f"{key}: {value}" for key, value in result['Четная неделя'].items() if
                        pd.notna(value) and value]
        output.append("Четная неделя:\n" + "; ".join(even_details) + "\n")
        odd_details = [f"{key}: {value}" for key, value in result['Нечетная неделя'].items() if
                       pd.notna(value) and value]
        output.append("Нечетная неделя:\n" + "; ".join(odd_details) + "\n")
    return "\n".join(output)


def save_results_to_csv(results, save_path):
    if not results:
        return None
    save_path = Path(save_path)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = save_path / f"teacher_schedule_{timestamp}.csv"
    data = []
    for result in results:
        row = {
            'Преподаватель': result['Преподаватель'],
            'Группа': result['Группа'],
            'День (Четная)': result['Четная неделя']['День'],
            'Время (Четная)': result['Четная неделя']['Время'],
            'Аудитория (Четная)': result['Четная неделя']['Аудитория'],
            'Тип (Четная)': result['Четная неделя']['Тип'],
            'Предмет (Четная)': result['Четная неделя']['Предмет'],
            'День (Нечетная)': result['Нечетная неделя']['День'],
            'Время (Нечетная)': result['Нечетная неделя']['Время'],
            'Аудитория (Нечетная)': result['Нечетная неделя']['Аудитория'],
            'Тип (Нечетная)': result['Нечетная неделя']['Тип'],
            'Предмет (Нечетная)': result['Нечетная неделя']['Предмет']
        }
        data.append(row)
    df = pd.DataFrame(data)
    df.to_csv(output_file, index=False, encoding='utf-8')
    return output_file


def load_teachers():
    try:
        if Path(CONFIG['FIO_JSON']).exists():
            with open(CONFIG['FIO_JSON'], 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"Ошибка загрузки учителей: {e}")
    return []


def save_teachers(teachers):
    with open(CONFIG['FIO_JSON'], 'w', encoding='utf-8') as f:
        json.dump(teachers, f, ensure_ascii=False, indent=2)


def log(message):
    logger.info(message)


class ScheduleApp:
    def __init__(self, root):
        self.listbox = None
        self.tree = None
        self.root = root
        self.root.title("РГУК: Расписание — Поиск преподавателей")
        self.folder_path = tk.StringVar()
        self.teachers = load_teachers()
        self.log_widget = None
        self.log_win = None
        self.results_win = None
        self.results = []
        self.progress_var = tk.DoubleVar()
        self.status_var = tk.StringVar(value="Готово")
        self.cancel_event = threading.Event()
        self.overwrite_var = tk.BooleanVar(value=CONFIG['OVERWRITE_CSV'])
        self.load_last_folder()
        show_vpn_warning()
        self.build_ui()
        self.process_log_queue()

    def load_last_folder(self):
        config = load_config()
        last_folder = config.get('DEFAULT', 'LastFolder', fallback='')
        if last_folder and validate_folder(last_folder):
            self.folder_path.set(last_folder)

    def build_ui(self):
        style = ttk.Style()
        style.configure('Treeview', font=('Arial', 8), rowheight=25)
        style.configure('Treeview.Heading', font=('Arial', 8, 'bold'))
        style.configure('TProgressbar', thickness=20)

        main_frame = ttk.Frame(self.root, padding=20)
        main_frame.grid(row=0, column=0, sticky="nsew")
        self.root.grid_rowconfigure(0, weight=1)
        self.root.grid_columnconfigure(0, weight=1)

        folder_frame = ttk.Frame(main_frame)
        folder_frame.grid(row=0, column=0, columnspan=3, sticky='ew', padx=10, pady=10)
        ttk.Label(folder_frame, text="📁 Папка для загрузки:", font=('Arial', 10)).grid(row=0, column=0, sticky='w',
                                                                                       padx=5, pady=5)
        path_entry = ttk.Entry(folder_frame, textvariable=self.folder_path, width=40, font=('Arial', 10))
        path_entry.grid(row=0, column=1, sticky='ew', padx=5, pady=5)
        browse_btn = ttk.Button(folder_frame, text="Обзор", command=self.select_folder, bootstyle="secondary")
        browse_btn.grid(row=0, column=2, sticky='e', padx=5, pady=5)
        folder_frame.grid_columnconfigure(1, weight=1)

        ttk.Checkbutton(main_frame, text="Перезаписывать существующие CSV", variable=self.overwrite_var,
                        command=self.update_overwrite_config).grid(row=1, column=0, columnspan=3, sticky='w', padx=10,
                                                                   pady=10)

        teacher_frame = ttk.Frame(main_frame)
        teacher_frame.grid(row=2, column=0, columnspan=3, sticky='nsew', padx=10, pady=10)
        ttk.Label(teacher_frame, text="👨‍🏫 Преподаватели:", font=('Arial', 10)).grid(row=0, column=0, sticky='w',
                                                                                     padx=5, pady=5)
        self.listbox = tk.Listbox(teacher_frame, height=6, font=('Arial', 10))
        self.listbox.grid(row=1, column=0, columnspan=2, sticky='nsew', padx=5, pady=5)
        for t in self.teachers:
            self.listbox.insert(tk.END, t)
        add_btn = ttk.Button(teacher_frame, text="Добавить", command=self.add_teacher, bootstyle="primary")
        add_btn.grid(row=1, column=2, sticky='ew', padx=5, pady=5)
        delete_btn = ttk.Button(teacher_frame, text="Удалить", command=self.delete_teacher, bootstyle="danger")
        delete_btn.grid(row=2, column=2, sticky='ew', padx=5, pady=5)
        teacher_frame.grid_columnconfigure(0, weight=1)
        teacher_frame.grid_columnconfigure(1, weight=1)
        teacher_frame.grid_rowconfigure(1, weight=1)

        action_frame = ttk.Frame(main_frame)
        action_frame.grid(row=3, column=0, columnspan=3, sticky='ew', padx=10, pady=10)
        self.download_btn = ttk.Button(action_frame, text="⬇ Скачать расписания", command=self.start_download_thread,
                                       bootstyle="success")
        self.download_btn.grid(row=0, column=0, sticky='ew', padx=5, pady=5)
        self.search_btn = ttk.Button(action_frame, text="🔍 Найти преподавателей", command=self.start_search_thread,
                                     bootstyle="info")
        self.search_btn.grid(row=1, column=0, sticky='ew', padx=5, pady=5)
        self.cancel_btn = ttk.Button(action_frame, text="⏹ Отмена", command=self.cancel_operation, state='disabled',
                                     bootstyle="warning")
        self.cancel_btn.grid(row=2, column=0, sticky='ew', padx=5, pady=5)
        results_btn = ttk.Button(action_frame, text="📋 Показать результаты", command=self.show_results,
                                 bootstyle="light")
        results_btn.grid(row=3, column=0, sticky='ew', padx=5, pady=5)
        log_btn = ttk.Button(action_frame, text="🪵 Открыть окно логов", command=self.show_logs, bootstyle="dark")
        log_btn.grid(row=4, column=0, sticky='ew', padx=5, pady=5)

        self.progress_bar = ttk.Progressbar(main_frame, variable=self.progress_var, maximum=100, bootstyle="striped")
        self.progress_bar.grid(row=4, column=0, columnspan=3, sticky='ew', padx=10, pady=10)

        ttk.Label(main_frame, textvariable=self.status_var, font=('Arial', 12, 'bold')).grid(row=5, column=0,
                                                                                             columnspan=3, sticky='ew',
                                                                                             padx=10, pady=10)

        main_frame.grid_rowconfigure(2, weight=1)
        main_frame.grid_columnconfigure(0, weight=1)

    def select_folder(self):
        initial_dir = self.folder_path.get() or str(Path.home())
        dialog_root = tk.Toplevel(self.root)
        dialog_root.withdraw()
        path = filedialog.askdirectory(
            parent=dialog_root,
            initialdir=initial_dir,
            title="Выберите папку для сохранения"
        )
        dialog_root.destroy()
        if path:
            if validate_folder(path):
                self.folder_path.set(path)
                save_config(path)
            else:
                messagebox.showerror("Ошибка", "Выбранная папка недоступна или не имеет прав на запись.")

    def add_teacher(self):
        name = simpledialog.askstring("ФИО преподавателя", "Введите ФИО:")
        if name:
            formatted_name = format_teacher_name(name)
            self.teachers.append(formatted_name)
            self.listbox.insert(tk.END, formatted_name)
            save_teachers(self.teachers)

    def delete_teacher(self):
        sel = self.listbox.curselection()
        if sel:
            self.teachers.pop(sel[0])
            self.listbox.delete(sel[0])
            save_teachers(self.teachers)

    def show_logs(self):
        if self.log_win and self.log_win.winfo_exists():
            self.log_win.lift()
            return
        self.log_win = tk.Toplevel(self.root)
        self.log_win.title("Журнал логов")
        self.log_widget = tk.Text(self.log_win, height=25, width=100, font=('Arial', 10))
        self.log_widget.pack(fill=tk.BOTH, expand=True)
        btn_frame = ttk.Frame(self.log_win)
        btn_frame.pack(pady=5)
        clear_btn = ttk.Button(btn_frame, text="Очистить лог", command=lambda: self.log_widget.delete(1.0, tk.END))
        clear_btn.pack(side=tk.LEFT, padx=5)
        save_btn = ttk.Button(btn_frame, text="Сохранить лог", command=self.save_log)
        save_btn.pack(side=tk.LEFT, padx=5)

    def save_log(self):
        initial_dir = self.folder_path.get() or str(Path.home())
        file_path = filedialog.asksaveasfilename(
            initialdir=initial_dir,
            title="Сохранить лог",
            defaultextension=".txt",
            filetypes=[("Текстовые файлы", "*.txt")]
        )
        if file_path:
            try:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(self.log_widget.get(1.0, tk.END))
                log(f"Лог сохранен в {file_path}")
            except Exception as e:
                log(f"Ошибка сохранения лога: {e}")
                messagebox.showerror("Ошибка", f"Не удалось сохранить лог: {e}")

    def sort_treeview(self, col, reverse):
        data = [(self.tree.set(item, col), item) for item in self.tree.get_children('')]
        data.sort(reverse=reverse)
        for index, (val, item) in enumerate(data):
            self.tree.move(item, '', index)
        self.tree.heading(col, command=lambda: self.sort_treeview(col, not reverse))

    def show_results(self):
        if not self.results:
            messagebox.showinfo("Результаты", "Нет результатов для отображения. Выполните поиск.")
            return
        if self.results_win and self.results_win.winfo_exists():
            self.results_win.lift()
            return
        self.results_win = tk.Toplevel(self.root)
        self.results_win.title("Результаты поиска")
        self.tree = ttk.Treeview(self.results_win,
                                 columns=("Преп.", "Гр.", "День (Ч)", "Вр. (Ч)", "Ауд. (Ч)", "Тип (Ч)", "Предм. (Ч)",
                                          "День (Н)", "Вр. (Н)", "Ауд. (Н)", "Тип (Н)", "Предм. (Н)"),
                                 show="headings")
        self.tree.heading("Преп.", text="Преп.")
        self.tree.heading("Гр.", text="Гр.")
        self.tree.heading("День (Ч)", text="День (Ч)")
        self.tree.heading("Вр. (Ч)", text="Вр. (Ч)")
        self.tree.heading("Ауд. (Ч)", text="Ауд. (Ч)")
        self.tree.heading("Тип (Ч)", text="Тип (Ч)")
        self.tree.heading("Предм. (Ч)", text="Предм. (Ч)")
        self.tree.heading("День (Н)", text="День (Н)")
        self.tree.heading("Вр. (Н)", text="Вр. (Н)")
        self.tree.heading("Ауд. (Н)", text="Ауд. (Н)")
        self.tree.heading("Тип (Н)", text="Тип (Н)")
        self.tree.heading("Предм. (Н)", text="Предм. (Н)")
        for col in self.tree["columns"]:
            self.tree.heading(col, command=lambda c=col: self.sort_treeview(c, False))
        vsb = ttk.Scrollbar(self.results_win, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(self.results_win, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        self.results_win.grid_rowconfigure(0, weight=1)
        self.results_win.grid_columnconfigure(0, weight=1)
        for result in self.results:
            teacher = result['Преподаватель']
            group = result['Группа']
            even_week = result['Четная неделя']
            odd_week = result['Нечетная неделя']
            self.tree.insert("", "end", values=(
                teacher,
                group,
                even_week['День'],
                even_week['Время'],
                even_week['Аудитория'],
                even_week['Тип'],
                even_week['Предмет'],
                odd_week['День'],
                odd_week['Время'],
                odd_week['Аудитория'],
                odd_week['Тип'],
                odd_week['Предмет']
            ))
        style = ttk.Style()
        font_name = style.lookup("Treeview", "font")
        tree_font = font.nametofont(font_name)
        columns = self.tree["columns"]
        for i, col in enumerate(columns):
            max_width = 50
            heading_text = self.tree.heading(col, "text")
            heading_width = tree_font.measure(heading_text)
            max_width = max(max_width, heading_width)
            for item in self.tree.get_children():
                values = self.tree.item(item, "values")
                if i < len(values):
                    text = str(values[i])
                    text_width = tree_font.measure(text)
                    max_width = max(max_width, text_width)
            self.tree.column(col, width=max_width + 20, minwidth=50, stretch=False)
        self.tree.tag_configure('wrapped', font=('Arial', 8))

    def process_log_queue(self):
        try:
            while True:
                record = log_queue.get_nowait()
                message = record.getMessage()
                if self.log_widget and self.log_widget.winfo_exists():
                    self.log_widget.insert(tk.END, f"{message}\n")
                    self.log_widget.see(tk.END)
        except queue.Empty:
            pass
        self.root.after(100, self.process_log_queue)

    def update_progress(self, progress):
        self.progress_var.set(progress * 100)
        self.root.update_idletasks()

    def cancel_operation(self):
        self.cancel_event.set()
        log("Операция отменена пользователем")
        self.status_var.set("Операция отменена")
        self.enable_buttons()
        self.progress_var.set(0)

    def start_download_thread(self):
        self.cancel_event.clear()
        threading.Thread(target=self.download_only, daemon=True).start()

    def start_search_thread(self):
        self.cancel_event.clear()
        threading.Thread(target=self.search_only, daemon=True).start()

    def download_only(self):
        self.disable_buttons()
        self.cancel_btn.config(state='normal')
        self.status_var.set("Загрузка файлов...")
        try:
            if not self.folder_path.get():
                messagebox.showwarning("Путь не выбран", "Выберите папку для сохранения.")
                return
            if not validate_folder(self.folder_path.get()):
                messagebox.showerror("Ошибка", "Папка недоступна или не имеет прав на запись.")
                return
            log("⬇ Начинается загрузка...")
            files = download_excel_files(self.folder_path.get(), log, self.update_progress, self.cancel_event)
            if not files:
                log("⚠ Нет новых файлов для загрузки.")
            if not self.cancel_event.is_set():
                log("✅ Загрузка завершена.")
        except Exception as e:
            log(f"❌ Ошибка при загрузке: {e}")
        finally:
            self.enable_buttons()
            self.cancel_btn.config(state='disabled')
            self.progress_var.set(0)
            if not self.cancel_event.is_set():
                self.status_var.set("Готово")

    def search_only(self):
        self.disable_buttons()
        self.cancel_btn.config(state='normal')
        self.status_var.set("Поиск преподавателей...")
        try:
            if not self.folder_path.get():
                messagebox.showwarning("Путь не выбран", "Выберите папку для сохранения.")
                return
            if not validate_folder(self.folder_path.get()):
                messagebox.showerror("Ошибка", "Папка недоступна или не имеет прав на запись.")
                return
            if not self.teachers:
                messagebox.showwarning("Ошибка", "Добавьте хотя бы одного преподавателя.")
                return
            log("🔍 Поиск преподавателей...")
            all_files = [f for f in Path(self.folder_path.get()).glob("*.xls*")]
            if not all_files:
                log("⚠ Нет Excel-файлов в выбранной папке.")
                return
            total_conversion = len(all_files)
            self.progress_var.set(0)
            all_csvs = []
            for i, file in enumerate(all_files):
                if self.cancel_event.is_set():
                    log("[Отменено] Конвертация Excel в CSV")
                    break
                converted = convert_to_csv(file, log, self.cancel_event)
                all_csvs.extend(converted)
                self.update_progress((i + 1) / total_conversion)
            total_search = len(all_csvs)
            self.progress_var.set(0)

            def search_progress(current, total):
                self.update_progress(current / total)

            self.results = search_teachers_in_csv(all_csvs, self.teachers, log, progress_callback=search_progress,
                                                  cancel_event=self.cancel_event)
            if not self.results:
                log("⚠ Преподаватели не найдены в расписании.")
            else:
                output_file = save_results_to_csv(self.results, self.folder_path.get())
                log(f"📋 Результаты сохранены в {output_file}")
                log(f"📊 Найдено совпадений: {len(self.results)}")
            if not self.cancel_event.is_set():
                log("✅ Поиск завершён.")
        except Exception as e:
            log(f"❌ Ошибка при поиске: {e}")
        finally:
            self.enable_buttons()
            self.cancel_btn.config(state='disabled')
            self.progress_var.set(0)
            if not self.cancel_event.is_set():
                self.status_var.set("Готово")

    def disable_buttons(self):
        self.download_btn.config(state='disabled')
        self.search_btn.config(state='disabled')

    def enable_buttons(self):
        self.download_btn.config(state='normal')
        self.search_btn.config(state='normal')

    def update_overwrite_config(self):
        CONFIG['OVERWRITE_CSV'] = self.overwrite_var.get()


if __name__ == '__main__':
    root = ttk.Window(themename="darkly")
    app = ScheduleApp(root)
    root.mainloop()
