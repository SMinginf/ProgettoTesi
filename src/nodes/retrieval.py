import asyncio
from langchain.messages import SystemMessage
from src.state import AgentState
from src.config import client, console
from rich.panel import Panel
from rich.markdown import Markdown
import time
from src.utils import parse_prometheus_output, json_to_markdown_table
import json



# --- NODO 2: MOTORE DATI (Solo Esecuzione) ---
async def metrics_engine_node(state: AgentState):
    """
    Esegue le query definite nella configurazione QoS in PARALLELO (Async Scatter-Gather).
    OTTIMIZZAZIONE: Applica il filtro target direttamente alla fonte (Push-Down Predicate).
    """
    start_time = time.perf_counter()
    
    # 1. Recupero Tool
    tools = await client.get_tools()
    query_tool = next((t for t in tools if t.name == "execute_query"), None)
    
    if not query_tool:
        return {
            "metrics_report": "Error: Tool 'execute_query' not found",
            "messages": [SystemMessage(content="Error: Prometheus tool missing.")]
        }

    # 2. Setup Configurazione
    config = state.get("qos_config", {})
    metrics_def = config.get("metrics", {})
    
    if not metrics_def:
        return {"metrics_report": "Error: Nessuna metrica definita."}

    # --- RECUPERO FILTRO TARGET (OTTIMIZZAZIONE STEP 2) ---
    target_filter = state.get("target_filter")
    
    if target_filter:
        console.print(f"[magenta]   🎯 Focus Mode Attivo: Estraggo solo dati per '{target_filter}'[/magenta]")
    
    # --- PREPARAZIONE TASKS ---
    tasks = []
    metric_names = [] # Teniamo traccia dell'ordine
    
    print(f"   🚀 Avvio retrieval parallelo per {len(metrics_def)} metriche...")

    for metric_name, definition in metrics_def.items():
        query = definition.get("query")
        if query:
            # Creiamo i task asincroni
            tasks.append(query_tool.ainvoke({"query": query}))
            metric_names.append(metric_name)

    # --- ESECUZIONE PARALLELA (FIRE ALL) ---
    raw_results = await asyncio.gather(*tasks, return_exceptions=True)

    # --- AGGREGAZIONE RISULTATI CON FILTRO ---
    nodes_snapshot = {} # { "nome_nodo": { "cpu": 10, "ram": 50 } }
    errors_count = 0

    for metric_name, result in zip(metric_names, raw_results):
        if isinstance(result, Exception):
            console.print(f"[dim red]⚠️ Errore query {metric_name}: {result}[/dim red]")
            errors_count += 1
            continue

        try:
            parsed_series = parse_prometheus_output(result, metric_name)
            
            # Pivot dei dati: Da {Metrica -> {Nodo -> Val}} a {Nodo -> {Metrica -> Val}}
            for node, value in parsed_series.items():
                
                # --- IL FILTRO (Push-Down Logic) ---
                # Se c'è un filtro attivo e il nodo non corrisponde, lo scartiamo SUBITO.
                # Risparmiamo memoria e token successivi.
                if target_filter and node != target_filter:
                    continue
                # -----------------------------------

                if node not in nodes_snapshot:
                    nodes_snapshot[node] = {}
                nodes_snapshot[node][metric_name] = value
                
        except Exception as e:
            console.print(f"[red]❌ Errore parsing {metric_name}: {e}[/red]")
            errors_count += 1

    # --- STATISTICHE E LOGGING ---
    elapsed_time = time.perf_counter() - start_time
    node_count = len(nodes_snapshot)
    
    # Feedback visivo differenziato per Focus Mode
    title_suffix = f"(Focus: {target_filter})" if target_filter else "(Full Cluster)"
    msg_color = "green" if errors_count == 0 else "yellow"
    
    stats_msg = (
        f"✅ Completato in [bold white]{elapsed_time:.3f}s[/bold white]\n"
        f"📊 Metriche: {len(metrics_def)} | Nodi Catturati: {node_count} | Errori: {errors_count}"
    )
    
    console.print(Panel(
        stats_msg,
        title=f"Metrics Engine {title_suffix}",
        border_style=msg_color
    ))

    # Serializzazione
    snapshot_json = json.dumps(nodes_snapshot, indent=2)
    
    # --- VISUALIZZAZIONE TABELLARE (Miglioria Visuale Precedente) ---
    # Usiamo la tabella Markdown per il log invece del JSON grezzo
    if node_count > 0:
        preview_table = json_to_markdown_table(nodes_snapshot, key_label="Node")
        console.print(Panel(
            Markdown(f"{preview_table}"), 
            title="📊 Live Data Snapshot",
            border_style="cyan"
        ))
    else:
        console.print("[dim yellow]   ⚠️ Nessun dato trovato per il target richiesto.[/dim yellow]")

    return {
        "metrics_report": snapshot_json,
        "messages": [SystemMessage(content=f"Metrics updated in {elapsed_time:.2f}s.")]
    }