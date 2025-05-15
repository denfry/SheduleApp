# Сборка приложения в .exe

Этот документ описывает, как собрать приложение **RGUK Schedule Scraper** в исполняемый файл (.exe) для Windows с использованием PyInstaller. Это позволяет распространять приложение без необходимости установки Python и зависимостей на целевой машине.

## Требования
- **Операционная система**: Windows (PyInstaller также поддерживает macOS и Linux, но инструкции ориентированы на Windows).
- **Python**: 3.7 или выше.
- **PyInstaller**: Установите с помощью `pip install pyinstaller`.
- **Зависимости проекта**: Убедитесь, что все зависимости (`requests`, `beautifulsoup4`, `pandas`, `openpyxl`, `ttkbootstrap`) установлены.

## Установка PyInstaller
1. Активируйте виртуальное окружение (если используется):
   ```bash
   source venv/bin/activate  # Linux/macOS
   venv\Scripts\activate     # Windows
   ```
2. Установите PyInstaller:
   ```bash
   pip install pyinstaller
   ```

## Сборка приложения
1. **Перейдите в папку проекта**:
   ```bash
   cd path/to/rguk-schedule-scraper
   ```

2. **Выполните команду PyInstaller**:
   Используйте следующую команду для создания .exe файла:
   ```bash
   pyinstaller --onefile --windowed --name RGUK_Schedule_Scraper main.py
   ```
   - `--onefile`: Создает один исполняемый файл (включает все зависимости).
   - `--windowed`: Скрывает консольное окно (подходит для GUI-приложений).
   - `--name RGUK_Schedule_Scraper`: Задает имя выходного файла.
   - `main.py`: Основной скрипт приложения.

3. **Результат сборки**:
   - После выполнения команды PyInstaller создаст папку `dist/` в корне проекта.
   - В папке `dist/` будет файл `RGUK_Schedule_Scraper.exe`.

4. **Тестирование**:
   - Запустите `dist/RGUK_Schedule_Scraper.exe`, чтобы убедиться, что приложение работает.
   - Убедитесь, что файлы `teachers.json` и `config.ini` создаются в той же папке, где находится .exe, если они используются.