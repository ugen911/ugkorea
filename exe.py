import sys
import tkinter as tk
from tkinter.scrolledtext import ScrolledText
import subprocess
import threading
import os

# Получаем путь к директории, где находятся скрипты
project_directory = os.path.dirname(os.path.abspath(__file__))
script_directory = os.path.join(project_directory, "reglament_task")

def run_command(command, output_widget):
    """Функция для выполнения команды и отображения вывода в текстовом поле."""
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, text=True)
    
    while True:
        output = process.stdout.readline()
        if output == "" and process.poll() is not None:
            break
        if output:
            output_widget.insert(tk.END, output)
            output_widget.yview(tk.END)
    
    err = process.stderr.read()
    if err:
        output_widget.insert(tk.END, err)
        output_widget.yview(tk.END)

def run_general_update(output_widget=None):
    """Функция для последовательного выполнения update_analitics_gl.py, nomenk_class.py и update_price_stock_old.py."""
    if output_widget:
        output_widget.insert(tk.END, "Запуск update_analitics_gl.py...\n")
    run_command(f"python {os.path.join(script_directory, 'update_analitics_gl.py')}", output_widget)
    
    if output_widget:
        output_widget.insert(tk.END, "Запуск nomenk_class.py...\n")
    run_command(f"python {os.path.join(script_directory, 'nomenk_class.py')}", output_widget)
    
    if output_widget:
        output_widget.insert(tk.END, "Запуск update_price_stock_old.py...\n")
    run_command(f"python {os.path.join(script_directory, 'update_price_stock_old.py')}", output_widget)

def run_update_price_stock_old(output_widget=None):
    """Функция для выполнения update_price_stock_old.py."""
    if output_widget:
        output_widget.insert(tk.END, "Запуск update_price_stock_old.py...\n")
    run_command(f"python {os.path.join(script_directory, 'update_price_stock_old.py')}", output_widget)

def start_thread(func, output_widget):
    """Запуск функции в отдельном потоке для того, чтобы интерфейс не замораживался."""
    thread = threading.Thread(target=func, args=(output_widget,))
    thread.start()

# Проверка параметров командной строки
if len(sys.argv) > 1:
    if sys.argv[1] == "general_update":
        run_general_update()
    elif sys.argv[1] == "update_price_stock":
        run_update_price_stock_old()
    sys.exit()

# Создание графического интерфейса
root = tk.Tk()
root.title("Task Runner")

# Создание текстового поля для вывода
output_text = ScrolledText(root, wrap=tk.WORD, width=100, height=30)
output_text.pack(pady=10)

# Создание кнопок
btn_run_general_update = tk.Button(root, text="Общее обновление", 
                                   command=lambda: start_thread(run_general_update, output_text))
btn_run_general_update.pack(pady=5)

btn_run_update_price_stock = tk.Button(root, text="Обновление остатков и цен", 
                                       command=lambda: start_thread(run_update_price_stock_old, output_text))
btn_run_update_price_stock.pack(pady=5)

# Запуск главного цикла интерфейса
root.mainloop()
