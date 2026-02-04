import json
import asyncio
import time
from rich.panel import Panel
from rich.markdown import Markdown
from rich.console import Console # <--- 1. Import Console
from langchain_core.messages import SystemMessage

# Import interni
from src.state import AgentState
from src.config import client
from src.utils import parse_prometheus_output, json_to_markdown_table
from src.logger import log

# Inizializziamo la console
console = Console()

async def metrics_engine_node(state: AgentState):
    """
    Esegue le query definite nella configurazione QoS in PARALLELO (Async Scatter-Gather).
    OTTIMIZZAZIONE: Applica il filtro target direttamente alla fonte (Push-Down Predicate).
    """
    start_time = time.perf_counter()
    
    # Header Visuale
    console.print(Panel("📊 Metrics Engine (Real-time Fetching)", style="blue"))
    log.info("Avvio Metrics Engine.")

    # 1. Recupero Tool
    tools = await client.get_tools()
    query_tool = next((t for t in tools if t.name == "execute_query"), None)
    
    if not query_tool:
        msg = "❌ Errore critico: Tool 'execute_query' non trovato su MCP Server."
        console.print(msg, style="bold red")
        log.error("Tool 'execute_query' non trovato.")
        return {
            "metrics_report": "Error: Tool 'execute_query' not found",
            "messages": [SystemMessage(content="Error: Prometheus tool missing.")]
        }

    # 2. Setup Configurazione
    config = state.get("qos_config", {})
    metrics_def = config.get("metrics", {})
    
    if not metrics_def:
        log.error("Nessuna metrica definita nella configurazione QoS.")
        return {"metrics_report": "Error: Nessuna metrica definita."}

    # --- RECUPERO FILTRO TARGET ---
    target_filter = state.get("target_filter")
    
    if target_filter:
        console.print(f"🎯 Focus Mode Attivo: Estraggo solo dati per [bold magenta]{target_filter}[/bold magenta]")
        log.info(f"Focus Mode Attivo per: {target_filter}")
    
    # --- PREPARAZIONE TASKS ---
    tasks = []
    metric_names = [] # Teniamo traccia dell'ordine
    
    console.print(f"🚀 Avvio retrieval parallelo per [bold]{len(metrics_def)}[/bold] metriche...", style="dim")
    log.info(f"Lancio {len(metrics_def)} query Prometheus in parallelo.")

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
            log.warning(f"Errore query {metric_name}: {result}")
            errors_count += 1
            continue

        try:
            parsed_series = parse_prometheus_output(result, metric_name)
            
            # Pivot dei dati: Da {Metrica -> {Nodo -> Val}} a {Nodo -> {Metrica -> Val}}
            for node, value in parsed_series.items():
                
                # --- IL FILTRO (Push-Down Logic) ---
                if target_filter and node != target_filter:
                    continue
                # -----------------------------------

                if node not in nodes_snapshot:
                    nodes_snapshot[node] = {}
                nodes_snapshot[node][metric_name] = value
                
        except Exception as e:
            log.error(f"Errore parsing {metric_name}: {e}")
            errors_count += 1

    # --- STATISTICHE E LOGGING ---
    elapsed_time = time.perf_counter() - start_time
    node_count = len(nodes_snapshot)
    
    # Feedback visivo
    title_suffix = f"(Focus: {target_filter})" if target_filter else "(Full Cluster)"
    
    # 1. Output Visuale (Console)
    if errors_count > 0:
        console.print(f"⚠️ Completato con {errors_count} errori.", style="yellow")

    # 2. Log di Sistema
    log.info(f"Metrics Engine Report {title_suffix} | Tempo: {elapsed_time:.3f}s | Nodi: {node_count} | Errori: {errors_count}")

    # Serializzazione
    snapshot_json = json.dumps(nodes_snapshot, indent=2)
    
    # --- VISUALIZZAZIONE TABELLARE ---
    if node_count > 0:
        # Generiamo la tabella Markdown per visualizzarla nel pannello
        preview_table = json_to_markdown_table(nodes_snapshot, key_label="Node")
        
        # Stampa visiva del Pannello con Markdown renderizzato
        console.print(Panel(
            Markdown(preview_table), 
            title=f"📊 Live Data Snapshot ({elapsed_time:.2f}s)", 
            border_style="dim cyan"
        ))
        
        log.info(f"Snapshot dati salvato in memoria ({len(snapshot_json)} bytes)")
        
    else:
        console.print("⚠️ Nessun dato trovato per il target richiesto.", style="bold red")
        log.warning("Nessun dato trovato per il target richiesto.")

    return {
        "metrics_report": snapshot_json,
        "active_targets": list(nodes_snapshot.keys()),
        "messages": [SystemMessage(content=f"Metrics updated in {elapsed_time:.2f}s.")]
    }