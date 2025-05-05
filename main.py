import requests
import tkinter as tk
from tkinter import filedialog, messagebox, simpledialog, ttk
import tkinter.font as font
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urljoin
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

# Configuration
CONFIG_FILE = 'config.ini'
CONFIG = {
    'BASE_URL': 'https://rguk.ru/students/schedule/',
    'HEADERS': {'User-Agent': 'Mozilla/5.0'},
    'FIO_JSON': 'teachers.json',
    'MAX_WORKERS': 4
}

# Setup logging
log_queue = queue.Queue()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)
queue_handler = QueueHandler(log_queue)
logger.addHandler(queue_handler)


def load_config():
    """Load configuration from file."""
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    return config


def save_config(folder_path):
    """Save folder path to configuration file."""
    config = configparser.ConfigParser()
    config['DEFAULT'] = {'LastFolder': str(folder_path)}
    with open(CONFIG_FILE, 'w') as configfile:
        config.write(configfile)


def validate_folder(folder_path):
    """Validate if the folder is accessible and writable."""
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


def download_file(file_url: str, save_path: Path, log_func: callable) -> Path | None:
    """Download a file from a URL and save it to the specified path."""
    try:
        filename = Path(file_url).name
        full_path = save_path / filename
        if full_path.exists():
            local_size = full_path.stat().st_size
            headers = requests.head(file_url, headers=CONFIG['HEADERS']).headers
            remote_size = int(headers.get('Content-Length', 0))
            if local_size == remote_size:
                log_func(f"[Пропущен] {filename} — уже загружен")
                return None
        with requests.get(file_url, headers=CONFIG['HEADERS'], stream=True) as r:
            r.raise_for_status()
            with open(full_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        log_func(f"[Скачан] {filename}")
        return full_path
    except requests.exceptions.RequestException as e:
        log_func(f"[Ошибка сети] {filename}: {e}")
        return None
    except OSError as e:
        log_func(f"[Ошибка файла] {filename}: {e}")
        return None


def download_excel_files(save_path, log_func, progress_callback=None):
    """Download all Excel files concurrently from the base URL."""
    save_path = Path(save_path)
    if not validate_folder(save_path):
        log_func("Ошибка: Нет доступа к папке для сохранения.")
        return []
    try:
        response = requests.get(CONFIG['BASE_URL'], headers=CONFIG['HEADERS'])
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        links = [urljoin(CONFIG['BASE_URL'], link['href']) for link in soup.find_all('a', href=True)
                 if link['href'].lower().endswith(('.xls', '.xlsx'))]
    except requests.exceptions.RequestException as e:
        log_func(f"Ошибка загрузки страницы: {e}")
        return []
    downloaded_files = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONFIG['MAX_WORKERS']) as executor:
        future_to_url = {executor.submit(download_file, url, save_path, log_func): url for url in links}
        for future in concurrent.futures.as_completed(future_to_url):
            result = future.result()
            if result:
                downloaded_files.append(result)
            if progress_callback:
                progress_callback(len(downloaded_files) / max(len(links), 1))
    return downloaded_files


def convert_to_csv(xl_file, log_func):
    """Convert Excel file sheets to CSV."""
    xl_file = Path(xl_file)
    base_dir = xl_file.parent
    base_name = xl_file.stem
    csv_files = []
    try:
        xls = pd.ExcelFile(xl_file)
        for sheet in xls.sheet_names:
            csv_name = base_dir / f"{base_name}_{sheet}.csv"
            if csv_name.exists():
                log_func(f"[Пропущено] CSV уже есть: {csv_name}")
                csv_files.append(csv_name)
                continue
            df = pd.read_excel(xl_file, sheet_name=sheet)
            df.to_csv(csv_name, index=False)
            csv_files.append(csv_name)
            log_func(f"[CSV создан] {csv_name}")
    except Exception as e:
        log_func(f"[Ошибка конвертации] {xl_file}: {e}")
    return csv_files


def search_teachers_in_csv(csv_files, teacher_list, log_func, progress_callback=None):
    """Search for teachers in CSV files, separating even and odd week data."""
    if not teacher_list:
        log_func("Ошибка: Список преподавателей пуст.")
        return []
    teacher_pattern = re.compile('|'.join(map(re.escape, teacher_list)), re.IGNORECASE)
    results = []
    for i, csv_file in enumerate(csv_files):
        try:
            df = pd.read_csv(csv_file)
            log_func(f"Заголовки столбцов в {csv_file}: {list(df.columns)}")
            for _, row in df.iterrows():
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
    """Format search results into a readable string, including both weeks."""
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
    """Save search results to a CSV file, including both weeks."""
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
    """Load teacher list from JSON."""
    try:
        if Path(CONFIG['FIO_JSON']).exists():
            with open(CONFIG['FIO_JSON'], 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"Ошибка загрузки учителей: {e}")
    return []


def save_teachers(teachers):
    """Save teacher list to JSON."""
    with open(CONFIG['FIO_JSON'], 'w', encoding='utf-8') as f:
        json.dump(teachers, f, ensure_ascii=False, indent=2)


class Tooltip:
    """Create a tooltip for a widget."""

    def __init__(self, widget, text):
        self.widget = widget
        self.text = text
        self.tooltip = None
        self.widget.bind("<Enter>", self.show_tooltip)
        self.widget.bind("<Leave>", self.hide_tooltip)

    def show_tooltip(self, event=None):
        x, y, _, _ = self.widget.bbox("insert")
        x += self.widget.winfo_rootx() + 25
        y += self.widget.winfo_rooty() + 20
        self.tooltip = tk.Toplevel(self.widget)
        self.tooltip.wm_overrideredirect(True)
        self.tooltip.wm_geometry(f"+{x}+{y}")
        label = tk.Label(self.tooltip, text=self.text, background="#ffffe0", relief="solid", borderwidth=1)
        label.pack()

    def hide_tooltip(self, event=None):
        if self.tooltip:
            self.tooltip.destroy()
            self.tooltip = None


class ScheduleApp:
    def __init__(self, root):
        self.root = root
        self.root.title("РГУК: Расписание — Поиск преподавателей")
        self.root.configure(bg="#2e2e2e")
        self.folder_path = tk.StringVar()
        self.teachers = load_teachers()
        self.log_widget = None
        self.log_win = None
        self.results_win = None
        self.results = []
        self.progress_var = tk.DoubleVar()
        self.status_var = tk.StringVar(value="Готово")
        self.load_last_folder()
        self.build_ui()
        self.process_log_queue()

    def load_last_folder(self):
        """Load the last selected folder from config."""
        config = load_config()
        last_folder = config.get('DEFAULT', 'LastFolder', fallback='')
        if last_folder and validate_folder(last_folder):
            self.folder_path.set(last_folder)

    def create_folder_selection(self, frame):
        """Create folder selection UI components."""
        ttk.Label(frame, text="📁 Папка для загрузки:").pack(anchor='w')
        path_frame = ttk.Frame(frame)
        path_frame.pack(fill=tk.X, pady=2)
        ttk.Entry(path_frame, textvariable=self.folder_path, width=60).pack(side=tk.LEFT, fill=tk.X, expand=True)
        browse_btn = ttk.Button(path_frame, text="Обзор", command=self.select_folder)
        browse_btn.pack(side=tk.LEFT, padx=5)
        Tooltip(browse_btn, "Выбрать папку для сохранения файлов")

    def create_teacher_list(self, frame):
        """Create teacher list UI components."""
        ttk.Label(frame, text="👨‍🏫 Преподаватели:").pack(anchor='w')
        self.listbox = tk.Listbox(frame, height=6, bg="#444", fg="#ddd", selectbackground="#666")
        self.listbox.pack(fill=tk.X)
        for t in self.teachers:
            self.listbox.insert(tk.END, t)
        control_frame = ttk.Frame(frame)
        control_frame.pack(fill=tk.X, pady=5)
        add_btn = ttk.Button(control_frame, text="Добавить", command=self.add_teacher)
        add_btn.pack(side=tk.LEFT, padx=5)
        Tooltip(add_btn, "Добавить нового преподавателя")
        delete_btn = ttk.Button(control_frame, text="Удалить", command=self.delete_teacher)
        delete_btn.pack(side=tk.LEFT)
        Tooltip(delete_btn, "Удалить выбранного преподавателя")

    def build_ui(self):
        """Build the main UI."""
        style = ttk.Style()
        style.theme_use('default')
        style.configure('.', background="#2e2e2e", foreground="#ddd", fieldbackground="#444")
        style.map("TButton", background=[('active', '#555')], foreground=[('active', '#fff')])
        style.configure('TEntry', fieldbackground='#444', foreground='#ddd')
        style.configure('TLabel', background='#2e2e2e', foreground='#ddd')
        style.configure('Treeview', font=('Arial', 8), rowheight=25)  # Smaller font and row height
        style.configure('Treeview.Heading', font=('Arial', 8, 'bold'))
        frame = ttk.Frame(self.root, padding=10)
        frame.pack(fill=tk.BOTH, expand=True)
        self.create_folder_selection(frame)
        self.create_teacher_list(frame)
        self.download_btn = ttk.Button(frame, text="⬇ Скачать расписания", command=self.start_download_thread)
        self.download_btn.pack(pady=6)
        Tooltip(self.download_btn, "Скачать Excel-файлы с расписанием")
        self.search_btn = ttk.Button(frame, text="🔍 Найти преподавателей", command=self.start_search_thread)
        self.search_btn.pack(pady=4)
        Tooltip(self.search_btn, "Найти расписание указанных преподавателей")
        results_btn = ttk.Button(frame, text="📋 Показать результаты", command=self.show_results)
        results_btn.pack(pady=4)
        Tooltip(results_btn, "Показать результаты поиска")
        log_btn = ttk.Button(frame, text="🪵 Открыть окно логов", command=self.show_logs)
        log_btn.pack()
        Tooltip(log_btn, "Открыть журнал операций")
        self.progress_bar = ttk.Progressbar(frame, variable=self.progress_var, maximum=100)
        self.progress_bar.pack(fill=tk.X, pady=5)
        ttk.Label(frame, textvariable=self.status_var, background="#2e2e2e", foreground="#ddd").pack(side=tk.BOTTOM,
                                                                                                     fill=tk.X)

    def select_folder(self):
        """Select a folder for saving files."""
        initial_dir = self.folder_path.get() or str(Path.home())
        dialog_root = tk.Toplevel(self.root)
        dialog_root.withdraw()
        dialog_root.option_add('*Background', '#2e2e2e')
        dialog_root.option_add('*Foreground', '#ddd')
        dialog_root.option_add('*Listbox*Background', '#444')
        dialog_root.option_add('*Listbox*Foreground', '#ddd')
        dialog_root.option_add('*Entry*Background', '#444')
        dialog_root.option_add('*Entry*Foreground', '#ddd')
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
        """Add a new teacher to the list."""
        name = simpledialog.askstring("ФИО преподавателя", "Введите ФИО:")
        if name:
            self.teachers.append(name)
            self.listbox.insert(tk.END, name)
            save_teachers(self.teachers)

    def delete_teacher(self):
        """Delete the selected teacher from the list."""
        sel = self.listbox.curselection()
        if sel:
            self.teachers.pop(sel[0])
            self.listbox.delete(sel[0])
            save_teachers(self.teachers)

    def show_logs(self):
        """Show the log window."""
        if self.log_win and self.log_win.winfo_exists():
            self.log_win.lift()
            return
        self.log_win = tk.Toplevel(self.root)
        self.log_win.title("Журнал логов")
        self.log_win.configure(bg="#2e2e2e")
        self.log_widget = tk.Text(self.log_win, height=25, width=100, bg="#1f1f1f", fg="#ddd")
        self.log_widget.pack(fill=tk.BOTH, expand=True)
        clear_btn = ttk.Button(self.log_win, text="Очистить лог", command=lambda: self.log_widget.delete(1.0, tk.END))
        clear_btn.pack(pady=5)
        Tooltip(clear_btn, "Очистить журнал логов")

    def sort_treeview(self, col, reverse):
        """Sort the Treeview by the specified column."""
        data = [(self.tree.set(item, col), item) for item in self.tree.get_children('')]
        data.sort(reverse=reverse)
        for index, (val, item) in enumerate(data):
            self.tree.move(item, '', index)
        self.tree.heading(col, command=lambda: self.sort_treeview(col, not reverse))

    def show_results(self):
        """Show the search results in a compact Treeview with auto-width columns."""
        if not self.results:
            messagebox.showinfo("Результаты", "Нет результатов для отображения. Выполните поиск.")
            return
        if self.results_win and self.results_win.winfo_exists():
            self.results_win.lift()
            return
        self.results_win = tk.Toplevel(self.root)
        self.results_win.title("Результаты поиска")
        self.results_win.configure(bg="#2e2e2e")
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
            max_width = 50  # Minimum width
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

    def log(self, message):
        """Log a message."""
        logger.info(message)

    def process_log_queue(self):
        """Process the log queue to update the log window."""
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
        """Update the progress bar."""
        self.progress_var.set(progress * 100)
        self.root.update_idletasks()

    def start_download_thread(self):
        """Start the download process in a separate thread."""
        threading.Thread(target=self.download_only, daemon=True).start()

    def start_search_thread(self):
        """Start the search process in a separate thread."""
        threading.Thread(target=self.search_only, daemon=True).start()

    def download_only(self):
        """Execute the download process."""
        self.disable_buttons()
        self.status_var.set("Загрузка файлов...")
        try:
            if not self.folder_path.get():
                messagebox.showwarning("Путь не выбран", "Выберите папку(TARGET_DIR) для сохранения.")
                return
            if not validate_folder(self.folder_path.get()):
                messagebox.showerror("Ошибка", "Папка недоступна или не имеет прав на запись.")
                return
            self.log("⬇ Начинается загрузка...")
            files = download_excel_files(self.folder_path.get(), self.log, self.update_progress)
            if not files:
                self.log("⚠ Нет новых файлов для загрузки.")
            self.log("✅ Загрузка завершена.")
        finally:
            self.enable_buttons()
            self.progress_var.set(0)
            self.status_var.set("Готово")

    def search_only(self):
        """Execute the search process."""
        self.disable_buttons()
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
            self.log("🔍 Поиск преподавателей...")
            all_files = [f for f in Path(self.folder_path.get()).glob("*.xls*")]
            if not all_files:
                self.log("⚠ Нет Excel-файлов в выбранной папке.")
                return
            total_conversion = len(all_files)
            self.progress_var.set(0)
            all_csvs = []
            for i, file in enumerate(all_files):
                converted = convert_to_csv(file, self.log)
                all_csvs.extend(converted)
                self.update_progress((i + 1) / total_conversion)
            total_search = len(all_csvs)
            self.progress_var.set(0)

            def search_progress(current, total):
                self.update_progress(current / total)

            self.results = search_teachers_in_csv(all_csvs, self.teachers, self.log, progress_callback=search_progress)
            if not self.results:
                self.log("⚠ Преподаватели не найдены в расписании.")
            else:
                output_file = save_results_to_csv(self.results, self.folder_path.get())
                self.log(f"📋 Результаты сохранены в {output_file}")
                self.log(f"📊 Найдено совпадений: {len(self.results)}")
            self.log("✅ Поиск завершён.")
        finally:
            self.enable_buttons()
            self.progress_var.set(0)
            self.status_var.set("Готово")

    def disable_buttons(self):
        """Disable interactive buttons during operations."""
        self.download_btn.config(state='disabled')
        self.search_btn.config(state='disabled')

    def enable_buttons(self):
        """Enable interactive buttons after operations."""
        self.download_btn.config(state='normal')
        self.search_btn.config(state='normal')


if __name__ == '__main__':
    root = tk.Tk()
    root.option_add('*Background', '#2e2e2e')
    root.option_add('*Foreground', '#ddd')
    root.option_add('*Listbox*Background', '#444')
    root.option_add('*Listbox*Foreground', '#ddd')
    root.option_add('*Entry*Background', '#444')
    root.option_add('*Entry*Foreground', '#ddd')
    app = ScheduleApp(root)
    root.mainloop()
