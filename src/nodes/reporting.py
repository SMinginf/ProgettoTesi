from src.state import AgentState
from src.config import llm
from langchain.messages import HumanMessage
import json
from src.utils import json_to_markdown_table


# async def report_synthesizer_node(state: AgentState):
#     """
#     Aggrega i risultati delle valutazioni dei profili e genera il Capability Report finale in Markdown.
#     """

#     results = state["profile_results"] # Lista di stringhe JSON dai worker
#     # 1. Parsing dei risultati parziali
#     summary_data = []
#     for r_str in results:
#         try:
#             r_dict = json.loads(r_str)
#             summary_data.append({
#                 "Profile": r_dict.get("profile_name", "Unknown"),
#                 "Qualified Nodes": ", ".join(r_dict.get("qualified_nodes", []))
#             })
#         except:
#             continue
    
#     # --- INTEGRAZIONE: Tabella riassuntiva ---
#     table_view = json_to_markdown_table(summary_data, key_label="Profile")
    
#     prompt = f"""
#     SEI UN REPORTER TECNICO.
    
#     Hai ricevuto {len(results)} valutazioni tecniche dai validatori.
#     Il tuo compito è SOLO aggregarle in un report Markdown leggibile.
    
#     NON ricalcolare i numeri. Fidati delle valutazioni ricevute.
    
#     INPUT DATI:
#     {table_view}
    
#     Genera il Capability Report finale (Markdown).
#     """
    
#     # Qui chiamo l'LLM standard (non strutturato) perché devo solo scrivere testo
#     response = await llm.ainvoke([HumanMessage(content=prompt)])
    
#     return {"messages": [response]}



async def report_synthesizer_node(state: AgentState):
    """
    Aggrega i risultati delle valutazioni dei profili e genera il Capability Report finale.
    OTTIMIZZAZIONE: Adaptive View (Scheda Singola vs Matrice Cluster) + Audit Logs.
    """

    results = state["profile_results"] # Lista di stringhe JSON dai worker
    target_filter = state.get("target_filter") # Recuperiamo il filtro (es. "worker-1")
    
    # 1. Parsing dei risultati parziali
    summary_data = []
    audit_logs = [] 

    for r_str in results:
        try:
            r_dict = json.loads(r_str)
            p_name = r_dict.get("profile_name", "Unknown")
            q_nodes = r_dict.get("qualified_nodes", [])
            analysis = r_dict.get("analysis_lines", {}) # {node: ["cpu < 80 (PASS)", ...]}

            # A. Dati per la Tabella Sintetica
            summary_data.append({
                "Profile": p_name,
                "Qualified Nodes": ", ".join(q_nodes) if q_nodes else "NESSUNO"
            })

            # B. Dati per i Log di Audit
            # Filtriamo i log se siamo in focus mode per non sporcare il prompt
            if analysis:
                audit_section = f"--- Dettagli Profilo: {p_name} ---\n"
                has_relevant_logs = False
                
                for node, checks in analysis.items():
                    # Se c'è un filtro attivo, includiamo SOLO i log di quel nodo
                    if target_filter and node != target_filter:
                        continue
                        
                    checks_str = "; ".join(checks)
                    audit_section += f"- {node}: {checks_str}\n"
                    has_relevant_logs = True
                
                if has_relevant_logs:
                    audit_logs.append(audit_section)

        except Exception as e:
            continue
    
    # 2. Creazione Viste (Data Presentation)
    table_view = json_to_markdown_table(summary_data, key_label="Profile")
    audit_view = "\n".join(audit_logs)
    
    # --- 3. LOGICA ADATTIVA (IL CUORE DELL'AGGIORNAMENTO) ---
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
    
    return {"messages": [response]}
    