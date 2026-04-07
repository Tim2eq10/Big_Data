import os
import json
import matplotlib.pyplot as plt

output_dir = 'output'
os.makedirs(output_dir, exist_ok=True)

data = {}
for fname in ['results_1.json', 'results_3.json']:
    if os.path.exists(fname):
        with open(fname, 'r', encoding="utf-8") as fp:
            data.update(json.load(fp))
    else:
        print(f"Не найден файл: {fname}")

experiment_keys = sorted(data.keys())
print("Эксперименты:", experiment_keys)

ops_times = [data[key]["ops_time"] for key in experiment_keys]
total_times = [data[key]["total_time"] for key in experiment_keys]
final_mem_usages = [data[key]["final_memory_usage"] for key in experiment_keys]

def create_and_save_plot(x, y, xlabel, ylabel, title, filename, color):
    plt.figure(figsize=(8, 6))
    plt.bar(x, y, color=color)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.tight_layout()
    save_path = os.path.join(output_dir, filename)
    plt.savefig(save_path)
    plt.close()
    print(f"Saved plot: {save_path}")

create_and_save_plot(
    experiment_keys,
    ops_times,
    "Эксперимент",
    "Время операций (сек)",
    "Время выполнения операций с таблицей",
    "ops_time.png",
    'skyblue'
)

create_and_save_plot(
    experiment_keys,
    total_times,
    "Эксперимент",
    "Общее время (сек)",
    "Общее время выполнения (с чтением и оверхэдом)",
    "total_time.png",
    'salmon'
)

create_and_save_plot(
    experiment_keys,
    final_mem_usages,
    "Эксперимент",
    "Память (MB)",
    "Финальное использование памяти",
    "final_memory_usage.png",
    'lightgreen'
)

plt.figure(figsize=(10, 8))
for key in experiment_keys:
    series = data[key].get("memory_usage_over_time", [])
    if series:
        times, mems = zip(*series)
        plt.plot(times, mems, label=key)
    else:
        print(f"Нет данных по memory_usage_over_time для эксперимента {key}")

plt.xlabel("Время (сек)")
plt.ylabel("Использование памяти (MB)")
plt.title("Динамика использования памяти по времени")
plt.legend(title="Эксперимент")
plt.tight_layout()
mem_series_plot_path = os.path.join(output_dir, "memory_usage_series.png")
plt.savefig(mem_series_plot_path)
plt.close()
print(f"Saved plot: {mem_series_plot_path}")
