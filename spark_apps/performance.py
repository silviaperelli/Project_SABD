import statistics

def print_performance(execution_times, run, query):
    if execution_times:
        # Elimino il primo elemento dalla lista
        execution_times = execution_times[1:]

        avg_time = statistics.mean(execution_times)
        print(f"\n--- Statistiche Tempi Esecuzione Query {query} ({run - 1} runs) ---")
        print(f"Tempi individuali: {[round(t, 4) for t in execution_times]}")
        print(f"Tempo medio di esecuzione: {avg_time:.4f} secondi")
        if len(execution_times) > 1:  # La deviazione standard richiede almeno 2 campioni
            std_dev_time = statistics.stdev(execution_times)
            print(f"Deviazione standard dei tempi: {std_dev_time:.4f} secondi")
        print("----------------------------------------------------")
