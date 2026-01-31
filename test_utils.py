import json
from src.utils import json_to_markdown_table

# --- TEST 1: Caso "Metrics Engine" (Dati annidati per nodo) ---
# Questo simula l'input di: single_profile_evaluator_node
print("\n" + "="*50)
print("TEST 1: Dati Metrici Grezzi (Metrics Engine)")
print("="*50)

metrics_data = {
    "worker-1": {
        "cpu_usage_pct": 12.5,
        "ram_available_bytes": 8589934592,
        "status": "active"
    },
    "worker-2": {
        "cpu_usage_pct": 85.2,
        "ram_available_bytes": 102400, # Poco
        "status": "warning"
    }
}

# Simuliamo la chiamata
table_1 = json_to_markdown_table(metrics_data, key_label="Node")
print(table_1)


# --- TEST 2: Caso "Allocation Advisor" (Auto-Flattening) ---
# Questo simula l'input di: allocation_advisor_node
# NOTA: Qui c'è la chiave 'metrics' annidata che deve essere "esplosa"
print("\n" + "="*50)
print("TEST 2: Context Data con Flattening (Advisor)")
print("="*50)

advisor_data = [
    {
        "name": "worker-1",
        "score": 0.95,
        "risks": [], # Lista vuota (Stabile)
        "metrics": {
            "cpu_usage_pct": "12.5%",
            "ram_usage": "4GB"
        }
    },
    {
        "name": "worker-3",
        "score": 0.45,
        "risks": ["SPIKE", "CHAOTIC"],
        "metrics": {
            "cpu_usage_pct": "99.9%",
            "ram_usage": "16GB"
        }
    }
]

table_2 = json_to_markdown_table(advisor_data, key_label="Node")
print(table_2)


# --- TEST 3: Caso "Config/Constraint" (QoS Config) ---
# Questo simula l'input di: constraint_extractor_node
print("\n" + "="*50)
print("TEST 3: QoS Config Definitions")
print("="*50)

config_data = {
    "cpu_usage_pct": {
        "unit": "percentage_100",
        "description": "Utilizzo CPU totale"
    },
    "ram_available_bytes": {
        "unit": "bytes",
        "description": "RAM Libera assoluta"
    }
}

table_3 = json_to_markdown_table(config_data, key_label="Metric")
print(table_3)