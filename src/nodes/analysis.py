import json
import operator
import asyncio
from rich.panel import Panel
from rich.console import Console

# Import interni
from src.state import AgentState
from src.config import client
from src.utils import parse_prometheus_output, get_strictest_threshold_config, get_physical_threshold, classify_stability
from src.logger import log


console = Console()

# Mappa degli operatori per la valutazione sicura 
OPS = {
    "<": operator.lt,
    "<=": operator.le,
    ">": operator.gt,
    ">=": operator.ge,
    "==": operator.eq,
    "!=": operator.ne
}

async def single_profile_evaluator_node(state):
    """
    Valutatore di Profilo Singolo.
    Controlla i nodi contro i requisiti definiti in un profilo QoS.
    1. Itera sui nodi e verifica i requisiti matematici.
    2. Registra i risultati dettagliati.
    3. Prepara il payload di risultato.
    """
    profile = state["profile"]
    metrics_json = state["metrics"]
    target_filter = state.get("target_filter")

    try:
        metrics_data = json.loads(metrics_json)
    except:
        metrics_data = {}

    profile_name = profile.get("profile_name", "Unknown")
    requirements = profile.get("required_conditions", [])
    
    qualified_nodes = []
    analysis_log = {} 

    # 1. Iteriamo sui nodi
    for node, node_metrics in metrics_data.items():
        if target_filter and node != target_filter:
            continue

        is_qualified = True
        node_logs = []

        # 2. Verifica matematica dei requisiti
        for req in requirements:
            metric_key = req.get("metric")
            op_sym = req.get("operator")
            threshold = req.get("threshold")
            
            val = node_metrics.get(metric_key)
            
            if val is None:
                is_qualified = False
                node_logs.append(f"{metric_key}: N/A (FAIL)")
                break

            op_func = OPS.get(op_sym)
            if op_func:
                try:
                    val_float = float(val)
                    thresh_float = float(threshold)
                    
                    if op_func(val_float, thresh_float):
                        node_logs.append(f"{metric_key}: {val_float} {op_sym} {thresh_float} (PASS)")
                    else:
                        is_qualified = False
                        node_logs.append(f"{metric_key}: {val_float} not {op_sym} {thresh_float} (FAIL)")
                except ValueError:
                     is_qualified = False
                     node_logs.append(f"{metric_key}: Type Error (FAIL)")

        if is_qualified:
            qualified_nodes.append(node)
        
        analysis_log[node] = node_logs

    # 3. Preparazione Risultato
    result_payload = {
        "profile_name": profile_name,
        "qualified_nodes": qualified_nodes,
        "analysis_lines": analysis_log
    }

    # --- VISUALIZZAZIONE ---
    count = len(qualified_nodes)
    if count > 0:
        # Visuale per l'utente (con colori)
        console.print(f"⚡ Profilo di carico valutato ({profile_name}): [bold green]{count}[/bold green] nodi idonei.")
        # Log di sistema (testo pulito)
        log.info(f"Profilo di carico valutato ({profile_name}): {count} nodi idonei.")
    else:
        console.print(f"⚡ Profilo di carico valutato ({profile_name}): Nessun nodo soddisfa i requisiti.", style="yellow")
        log.info(f"Profilo di carico valutato ({profile_name}): Nessun nodo soddisfa i requisiti.")

    return {"profile_results": [json.dumps(result_payload)]}


async def stability_analyzer_node(state: AgentState):
    """
    Nodo di Analisi di Stabilità.
    Esegue analisi storiche parallele per identificare anomalie nei nodi candidati
    rispetto ai profili di carico individuati.
    1. Recupera i nodi candidati e i profili di carico individuati.
    2. Costruisce ed esegue query storiche in parallelo.
    3. Analizza i risultati per classificare la stabilità.
    4. Prepara il report di stabilità.

    """
    candidates = state.get("final_candidates", [])
    target_profiles = state.get("target_profiles", [])
    config = state.get("qos_config", {})
    metrics_def = config.get("metrics", {})
    profiles_def = config.get("profiles", {})
    metrics_json = state.get("metrics_report", "{}")
    
    if not candidates or not target_profiles:
        return {"stability_report": {}}

    tools = await client.get_tools()
    query_tool = next((t for t in tools if t.name == "execute_query"), None)
    if not query_tool: 
        log.error("Tool 'execute_query' mancante. Salto analisi stabilità.")
        return {"stability_report": {}}


    console.print(Panel("📉 Avvio Analisi Stabilità (Parallel Async)", style="blue"))
    log.info("Avvio Analisi Stabilità.")

    # Trova le soglie più restrittive dai profili target (Principio di Cautela)
    active_thresholds_map = get_strictest_threshold_config(target_profiles, profiles_def)
    metrics_to_analyze = set()
    for p in target_profiles:
        # Per ogni profilo target recupero i nomi delle metriche a cui sono associati pesi di scoring
        metrics_to_analyze.update(profiles_def.get(p, {}).get("scoring_weights", {}).keys())

    time_window = "24h"
    resolution = "5m"
    
    tasks = []
    task_metadata = [] 

    # Costruzione ed esecuzione query storiche in parallelo
    for metric_name in metrics_to_analyze:
        this_metric_def = metrics_def.get(metric_name, {})
        base_query = this_metric_def.get("query")
        if not base_query: continue

        q_avg = f"avg_over_time(({base_query})[{time_window}:{resolution}])"
        q_std = f"stddev_over_time(({base_query})[{time_window}:{resolution}])"

        tasks.append(query_tool.ainvoke({"query": q_avg}))
        task_metadata.append({"metric": metric_name, "type": "avg"})

        tasks.append(query_tool.ainvoke({"query": q_std}))
        task_metadata.append({"metric": metric_name, "type": "std"})

    if not tasks:
        return {"stability_report": {}}

    # Visuale
    console.print(f"🚀 Lancio [bold]{len(tasks)}[/bold] query storiche simultanee...")
    # Log
    log.info(f"Lancio {len(tasks)} query storiche simultanee...")

    results_raw = await asyncio.gather(*tasks, return_exceptions=True)

    temp_results = {} 
    for i, res in enumerate(results_raw):
        meta = task_metadata[i]
        m_name = meta["metric"]
        q_type = meta["type"]

        if isinstance(res, Exception):
            log.warning(f"⚠️ Errore query storica {m_name} ({q_type}): {res}")
            continue
        
        parsed_data = parse_prometheus_output(res, m_name)
        if m_name not in temp_results: 
            temp_results[m_name] = {}
        temp_results[m_name][q_type] = parsed_data

    stability_report = {}
    try:
        current_data_snapshot = json.loads(metrics_json)
    except:
        current_data_snapshot = {}
    
    spikes_found = 0

    for metric_name, data_pair in temp_results.items():
        parsed_avg = data_pair.get("avg", {})
        parsed_std = data_pair.get("std", {})
        
        this_metric_def = metrics_def.get(metric_name, {})
        phys_threshold = get_physical_threshold(metric_name, this_metric_def, active_thresholds_map)

        for node in candidates:
            curr_val = current_data_snapshot.get(node, {}).get(metric_name)
            avg_val = parsed_avg.get(node)
            std_val = parsed_std.get(node)

            if curr_val is not None and avg_val is not None:
                result = classify_stability(float(curr_val), avg_val, std_val, phys_threshold)
                
                if node not in stability_report: stability_report[node] = {}
                stability_report[node][metric_name] = {
                    "status": result["status"],
                    "reason": result["reason"],
                    "stats": result["metrics"]
                }

                if result["status"] in ["SPIKE", "CHAOTIC"]:
                    # Qui warning va bene sia per console che log perché è importante
                    console.print(f"⚠️ Instabilità rilevata su {node} [{metric_name}]: {result['status']}", style="bold red")
                    log.warning(f"Instabilità rilevata su {node} [{metric_name}]: {result['status']}")
                    spikes_found += 1
    
    if spikes_found == 0:
        console.print("✅ Analisi storica completata: Nessuna anomalia critica.", style="green")
        log.info("Analisi storica completata: Nessuna anomalia critica.")
    else:
        console.print(f"ℹ️ Analisi storica completata: {spikes_found} possibili anomalie tracciate.", style="yellow")
        log.info(f"Analisi storica completata: {spikes_found} possibili anomalie tracciate.")

    return {"stability_report": stability_report}