from src.state import AgentState
from src.config import llm
from langchain.messages import HumanMessage
import json
from src.utils import json_to_markdown_table
from src.logger import log
from rich.console import Console
from rich.table import Table    
from rich.panel import Panel

console = Console()

async def report_synthesizer_node(state: AgentState):
    """
    Aggrega i risultati delle valutazioni dei profili e genera il Capability Report finale.
    OTTIMIZZAZIONE: Adaptive View (Scheda Singola vs Matrice Cluster) + Audit Logs.
    """

    results = state["profile_results"] # Lista di stringhe JSON dai worker
    target_filter = state.get("target_filter") # Recuperiamo il filtro (es. "worker-1")
    
    # Header Visuale
    console.print(Panel("📑 Generating Capability Report", style="grey50"))
    log.info(f"Avvio Report Synthesizer. Target filter: {target_filter}")

    # 1. Parsing dei risultati parziali
    summary_data = []
    audit_logs = [] 

    # Creiamo anche una tabella Rich per la visualizzazione immediata
    rich_table = Table(title="📊 Matrice Idoneità Preliminare", show_header=True)
    rich_table.add_column("Profilo", style="bold magenta")
    rich_table.add_column("Nodi Qualificati", style="green")

    for r_str in results:
        try:
            r_dict = json.loads(r_str)
            p_name = r_dict.get("profile_name", "Unknown")
            q_nodes = r_dict.get("qualified_nodes", [])
            analysis = r_dict.get("analysis_lines", {}) 

            q_nodes_str = ", ".join(q_nodes) if q_nodes else "NESSUNO"

            # A. Dati per la Tabella Sintetica (Prompt)
            summary_data.append({
                "Profile": p_name,
                "Qualified Nodes": q_nodes_str
            })

            # Aggiunta riga alla tabella visiva
            rich_table.add_row(p_name, q_nodes_str)

            # B. Dati per i Log di Audit
            if analysis:
                audit_section = f"--- Dettagli Profilo: {p_name} ---\n"
                has_relevant_logs = False
                
                for node, checks in analysis.items():
                    if target_filter and node != target_filter:
                        continue
                        
                    checks_str = "; ".join(checks)
                    audit_section += f"- {node}: {checks_str}\n"
                    has_relevant_logs = True
                
                if has_relevant_logs:
                    audit_logs.append(audit_section)

        except Exception as e:
            log.error(f"Errore parsing risultato profilo: {e}")
            continue
    
    # MOSTRA TABELLA ALL'UTENTE
    console.print(rich_table)
    
    # 2. Creazione Viste (Data Presentation per LLM)
    table_view = json_to_markdown_table(summary_data, key_label="Profile")
    audit_view = "\n".join(audit_logs)
    
    # --- 3. LOGICA ADATTIVA ---
    if target_filter:
        # MODO SCHEDA SINGOLA (Focus Report)
        prompt_style = f"""
        MODALITÀ: FOCUS REPORT (Singolo Nodo: {target_filter}).
        
        NON generare tabelle comparative generali. L'utente vuole sapere solo di questo nodo.
        
        Struttura richiesta (Markdown):
        # Stato di Salute: {target_filter}
        
        ## Idoneità Profili
        - Elenca i profili per cui il nodo è risultato QUALIFIED (vedi Matrice).
        
        ## Dettagli Tecnici (Audit)
        - Usa i LOG DETTAGLIATI per spiegare brevemente i criteri passati o falliti specifici per questo nodo.
        - Sii specifico coi numeri (es. "Ha fallito CPU perché 85% > 80%").
        """
    else:
        # MODO CLUSTER (Overview Report)
        prompt_style = """
        MODALITÀ: CLUSTER OVERVIEW.
        
        1. Genera una Matrice di Idoneità (Tabella) per confrontare i nodi.
        2. Aggiungi una sezione "Technical Audit" dove spieghi brevemente i criteri soddisfatti o falliti per CIASCUN nodo, citando i dati matematici forniti nei log.
        """

    # 4. Costruzione Prompt Finale
    prompt = f"""
    SEI UN REPORTER TECNICO (SRE).
    
    {prompt_style}
    
    DATI SINTESI (Matrice):
    {table_view}
    
    LOG DETTAGLIATI (Audit Trail):
    {audit_view}
    
    Genera il Capability Report finale (Markdown).
    Usa icone (✅, ❌, ⚠️) per la massima leggibilità.
    """
    
    response = await llm.ainvoke([HumanMessage(content=prompt)])
    
    log.info("Report finale generato.")
    return {"messages": [response]}