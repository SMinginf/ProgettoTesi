import asyncio
import time
import json
from langchain.messages import SystemMessage
from rich.panel import Panel
from rich.markdown import Markdown

# Import interni
from src.state import AgentState
from src.config import client  # Rimosso 'console' perché usiamo il logger
from src.utils import parse_prometheus_output, json_to_markdown_table
from src.logger import log     # NUOVO: Importiamo il logger centralizzato

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
        log.error("Tool 'execute_query' non trovato su MCP Server.")
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
        log.info(f"🎯 Focus Mode Attivo: Estraggo solo dati per '[bold magenta]{target_filter}[/bold magenta]'")
    
    # --- PREPARAZIONE TASKS ---
    tasks = []
    metric_names = [] # Teniamo traccia dell'ordine
    
    log.info(f"🚀 Avvio retrieval parallelo per {len(metrics_def)} metriche...")

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
            log.warning(f"⚠️ Errore query [bold]{metric_name}[/bold]: {result}")
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
            log.error(f"❌ Errore parsing {metric_name}: {e}")
            errors_count += 1

    # --- STATISTICHE E LOGGING ---
    elapsed_time = time.perf_counter() - start_time
    node_count = len(nodes_snapshot)
    
    # Feedback visivo differenziato
    title_suffix = f"(Focus: {target_filter})" if target_filter else "(Full Cluster)"
   
 
    # Usiamo i colori di markup di Rich dentro la stringa
    log.info(f"📊 [bold]Metrics Engine Report {title_suffix}[/bold]")
    log.info(f"   ✅ Tempo: {elapsed_time:.3f}s | Metriche: {len(metrics_def)} | Nodi: {node_count} | Errori: {errors_count}")

    # Serializzazione
    snapshot_json = json.dumps(nodes_snapshot, indent=2)
    
    # --- VISUALIZZAZIONE TABELLARE ---
    if node_count > 0:
        preview_table = json_to_markdown_table(nodes_snapshot, key_label="Node")
        # Invece di stampare un Pannello grafico che rompe il logger, 
        # logghiamo che i dati sono pronti o mostriamo una preview testuale se necessario.
        log.info(f"💾 Snapshot dati salvato in memoria ({len(snapshot_json)} bytes)")
        
        # Se vuoi vedere la tabella nel terminale per debug, usa console.print DIRETTO
        # (Questo bypassa il logger e stampa la grafica carina)
        from src.logger import console
        console.print(Panel(Markdown(preview_table), title="📊 Live Data Debug", border_style="dim cyan"))
        
    else:
        log.warning("⚠️ Nessun dato trovato per il target richiesto.")

    return {
        "metrics_report": snapshot_json,
        "messages": [SystemMessage(content=f"Metrics updated in {elapsed_time:.2f}s.")]
    }