import json
import operator
from rich.table import Table
from rich.panel import Panel
from src.state import AgentState
from src.config import llm, console
from src.schemas import (UserRequestClassification,
                              TaskProfileIntent,
                                RequirementExtraction)
from langchain.messages import HumanMessage, AIMessage
from src.utils import humanize_metrics_with_config, json_to_markdown_table, get_last_user_message
from src.logger import log


# --- NODO 3: CLASSIFIER ---
async def classify_intent_node(state: AgentState):
    """
    Analizza l'input utente e determina l'intento: "allocation" o "status".
    Se l'utente specifica un nodo particolare, lo estrae e lo assegna a "target_filter".
    """
    user_input = get_last_user_message(state["messages"])

    # Recupera i nodi attivi dallo stato e formatta per il prompt
    targets_raw = state.get("active_targets", [])
    
    if isinstance(targets_raw, list) and targets_raw:
        formatted_targets = "\n- ".join(targets_raw) # Crea elenco puntato
    else:
        formatted_targets = "Nessun nodo rilevato."
    
    prompt = f"""
    Analizza la seguente richiesta e classificala: "{user_input}"
    
    Restituisci l'intento e inserisci in "target_filter" il nome del nodo specifico se menzionato e se esiste tra i nodi validi, altrimenti non inserire nulla.

    Nodi validi:
    {formatted_targets} 
    """
    structured_llm = llm.with_structured_output(UserRequestClassification)
    
    try:
        response = await structured_llm.ainvoke(prompt)
        
        intent = response.intent
        target = response.target_filter
                
        if target and target.lower() in ["nessuno", "none", "null", "n/a", "tutti", "all"]:
            target = None

        # 1. Visualizzazione per l'utente
        console.print(f"🧠 Classificazione intento: [bold magenta]{intent}[/bold magenta]")
        if target:
            console.print(f"🎯 Target: [bold cyan]{target}[/bold cyan]")

        # 2. Log di sistema
        log.info(f"Classificazione intento: {intent} | Target: {target}")

    except Exception as e:
        log.error(f"Errore classificazione intento: {e}")
        # Fallback prudente
        return {"intent": "status", "target_filter": None}
    
    return {"intent": intent, "target_filter": target}

async def classify_task_node(state: AgentState):
    """
    Analizza la descrizione del task utente e identifica i profili di carico più adatti.
    1. Usa la configurazione QoS per recuperare i profili disponibili.
    2. Costruisce un prompt che elenca i profili con le loro descrizioni.
    3. Chiede all'LLM di selezionare i profili più rilevanti per il task descritto.
    4. Registra la selezione e la motivazione nello stato.

    """
    user_input = get_last_user_message(state["messages"])

    config = state.get("qos_config", {})
    profiles = config.get("profiles", {})
    
    # Passo SOLO "description". La key_label sarà il nome del profilo.
    # Escludmo "required_conditions" e "scoring_weights" per evitare di confondere l'LLM.
    profiles_table = json_to_markdown_table(
        profiles, 
        key_label="Profile Name", 
        columns=["description"] 
    )
    
    prompt = f"""
    ANALIZZA LA NATURA DEL TASK.
    
    Profili Disponibili:
    {profiles_table}
    
    Richiesta Utente: "{user_input}"
    
    Compito:
    Identifica quali profili di carico si adattano meglio a questa richiesta.
    Se l'utente specifica requisiti tecnici (es. "voglio tanta RAM"), seleziona il profilo corrispondente (memory-bound).
    """
    
    model = llm.with_structured_output(TaskProfileIntent)
    result = await model.ainvoke(prompt)
    
    # STAMPA MIGLIORATA
    sel_profiles = result.selected_profiles
    reason = result.reasoning
    
    # 1. Visualizzazione per l'utente
    console.print(Panel(
        f"Task mappato su: [bold magenta]{sel_profiles}[/bold magenta]\n[italic dim]\"{reason}\"[/italic dim]",
        title="🧠 Technical Profiler",
        border_style="magenta"
    ))
    
    # 2. Log di sistema
    log.info(f"Task profile classification: {sel_profiles} | Reason: {reason}")
    
    return {
        "target_profiles": sel_profiles,
        "classification_reason": reason
    }

async def constraint_extractor_node(state: AgentState):
    """
    Estrae i vincoli numerici espliciti dalla richiesta utente.
    1. Usa le metriche disponibili nella configurazione QoS per guidare l'estrazione.
    2. Restituisce una lista di vincoli strutturati nello stato.

    """
    # --- Recupero ultimo messaggio utente ---
    user_input = get_last_user_message(state["messages"])

    config = state.get("qos_config", {})
    metrics = config.get("metrics", {})
    
    # Estraggo SOLO le colonne utili per capire il significato della metrica.
    # La colonna 'query' verrà ignorata automaticamente.
    metrics_table = json_to_markdown_table(
        metrics, 
        key_label="Metric", 
        columns=["unit", "description"] 
    )
    
    prompt = f"""
    SEI UN ESTRATTORE DI VINCOLI TECNICI.
    
    Il tuo unico obiettivo è trovare numeri e requisiti nella richiesta e convertirli in filtri per metriche.
    Se non sono presenti numeri espliciti, restituisci una lista vuota.
    
    METRICHE DISPONIBILI:
    {metrics_table}
    
    RICHIESTA UTENTE: "{user_input}"
    
    REGOLE DI CONVERSIONE:
    1. RAM/DISK (Bytes):
        - 1KB = 1024, 1MB = 1024^2, 1GB = 1024^3.
        - Es: "4GB RAM libera" -> metrica: `ram_available_bytes`, val: 4294967296, op: `>=`
    2. PERCENTUALI (0-100):
        - Es: "CPU sotto il 20%" -> metrica: `cpu_usage_pct`, val: 20, op: `<`
    3. Se non ci sono numeri espliciti, restituisci una lista vuota.
    """
    
    model = llm.with_structured_output(RequirementExtraction)
    try:
        result = await model.ainvoke(prompt)
        
        # Serializziamo per salvare nello stato (Pydantic -> Dict)
        constraints_list = [c.model_dump() for c in result.constraints]
    
        if constraints_list:
            # 1. Visualizzazione per l'utente
            c_text = "\n".join([f"- [bold]{c['metric_name']}[/bold] {c['operator']} {c['value']} ({c['original_text']})" for c in constraints_list])
            console.print(Panel(c_text, title="📏 Vincoli Estratti", border_style="yellow"))
            
            # 2. Log di sistema 
            # Loggo la lista grezza, utile per debuggare i valori esatti
            log.info(f"Vincoli numerici estratti: {constraints_list}")
        else:
            # 1. Utente
            console.print("Nessun vincolo numerico esplicito trovato.", style="dim")
            # 2. Log
            log.info("Nessun vincolo numerico esplicito trovato.")
        
        return {"explicit_constraints": constraints_list}
        
    except Exception as e:
        log.error(f"Errore durante l'estrazione dei vincoli: {e}")
        return {"explicit_constraints": []}

async def candidate_filter_node(state: AgentState):
    """
    Filtra i nodi candidati basandosi su:
    1. Profili di carico tecnici (intersezione dei nodi qualificati).
    2. Vincoli espliciti dell'utente (es. RAM minima).
    Restituisce la lista finale dei nodi che soddisfano tutti i criteri.

    """
    
    # --- RECUPERO DATI DALLO STATO ---
    target_profiles = state.get("target_profiles", [])
    raw_results = state.get("profile_results", [])
    user_constraints = state.get("explicit_constraints", [])
    metrics_json = state.get("metrics_report", "{}")
    
    try:
        metrics_data = json.loads(metrics_json) # Dict: {"worker-1": {"cpu": 10...}, ...}
    except:
        metrics_data = {}

    console.print(Panel("🌪️ Filtering Candidates", style="grey50"))
    log.info("Avvio filtro candidati (Candidate Filter Node).")

    # --- FASE 1: FILTRO PER PROFILO  ---
    
    # Parsing dei risultati del Map-Reduce
    profile_qualification_map = {} # profile_name -> set(nodi qualificati)
    for r_str in raw_results:
        try:
            r_dict = json.loads(r_str)
            p_name = r_dict.get("profile_name")
            q_nodes = set(r_dict.get("qualified_nodes", [])) 
            profile_qualification_map[p_name] = q_nodes
        except:
            continue

    # Calcolo dei candidati iniziali
    initial_candidates = set() # uso un set così da evitare duplicati
    
    if not target_profiles:
        # Caso: Nessun profilo specifico
        msg = "⚠️ Nessun profilo target specifico. Considero tutti i nodi tecnicamente validi."
        console.print(msg, style="yellow")
        log.warning(msg)
        
        # Considero tutti i nodi qualificati da ogni profilo
        for nodes_set in profile_qualification_map.values():
            initial_candidates.update(nodes_set)
    else:
        # Caso: Intersezione profili richiesti
        first_prof = target_profiles[0]
        if first_prof in profile_qualification_map:
            initial_candidates = set(profile_qualification_map[first_prof])
            
            msg = f"Candidati per il profilo ({first_prof}): {len(initial_candidates)} nodi."
            console.print(msg)
            log.info(msg)
        else:
            msg = f"❌ Errore: Nessun risultato tecnico per il profilo {first_prof}"
            console.print(msg, style="bold red")
            log.error(msg)
            initial_candidates = set()

        # Intersezione con gli altri profili
        for p_name in target_profiles[1:]:
            p_nodes = profile_qualification_map.get(p_name, set())
            prev_count = len(initial_candidates)
            initial_candidates.intersection_update(p_nodes)
            
            msg = f"Intersezione con {p_name}: {prev_count} -> {len(initial_candidates)} nodi."
            console.print(msg)
            log.info(msg)

    # --- FASE 2: FILTRO PER VINCOLI UTENTE ---
    final_candidates = list(initial_candidates)
    
    if user_constraints and final_candidates:
        msg = f"Applicazione di {len(user_constraints)} vincoli esplicitati dall'utente..."
        console.print(msg)
        log.info(msg)

        # Mappa degli operatori
        ops = {
            ">": operator.gt, "<": operator.lt,
            ">=": operator.ge, "<=": operator.le,
            "==": operator.eq, "!=": operator.ne
        }
        
        # Ciclo sui nodi candidati
        for node in list(final_candidates):

            # Per ogni nodo prendo le sue metriche...
            node_metrics = metrics_data.get(node, {})
            
            # ... e ciclo sui vincoli utente
            for constr in user_constraints:
                metric_key = constr["metric_name"]
                target_val = constr["value"]
                op_sym = constr["operator"]
                op_func = ops.get(op_sym)
                
                real_val = node_metrics.get(metric_key)
                
                # Check 1: verifica esistenza della metrica per il nodo
                if real_val is None:
                    console.print(f"[dim red]   - {node} scartato: Manca dato {metric_key}[/dim red]")
                    log.info(f"Node {node} scartato: Manca dato {metric_key}")
                    if node in final_candidates: final_candidates.remove(node)
                    break 
                
                # Check 2: verifica soglia. Se fallisce, scarta il nodo
                if op_func and not op_func(real_val, target_val):
                    console.print(f"[dim red]   - {node} scartato: {metric_key}={real_val} non è {op_sym} {target_val}[/dim red]")
                    log.info(f"Node {node} scartato: {metric_key}={real_val} failed constraint {op_sym} {target_val}")
                    if node in final_candidates: final_candidates.remove(node)
                    break 

    # --- OUPUT FINALE ---
    if final_candidates:
        # Visuale
        console.print(f"[green]   ✅ Finalisti:[/green] [bold]{', '.join(final_candidates)}[/bold]")
        # Log
        log.info(f"Finalisti identificati: {final_candidates}")
    else:
        # Visuale
        console.print("   ⛔ Nessun candidato sopravvissuto ai filtri.", style="bold red")
        # Log
        log.warning("Nessun candidato sopravvissuto ai filtri.")

    return {"final_candidates": final_candidates}

async def allocation_advisor_node(state: AgentState):
    """
    Nodo principale per consigliare l'allocazione sul nodo migliore.
    1. Recupera i candidati finali e le metriche dallo stato.
    2. Calcola uno score di performance per ogni nodo basato sui pesi dei profili target.
    3. Valuta il rischio di instabilità basato sui dati di stabilità.
    4. Classifica i nodi e identifica il vincitore, il runner-up e un "porto sicuro" se disponibile.
    5. Costruisce un prompt dinamico basato sulla strategia di selezione
         (es. clear winner, consider runner-up, propose safe haven, all risky).
    6. Invoca l'LLM per generare la raccomandazione finale per l'utente.

    """
    
    
    # --- 0. RECUPERO CONTESTO DALLO STATO ---
    candidates = state.get("final_candidates", [])
    target_profiles = state.get("target_profiles", [])
    metrics_json = state.get("metrics_report", "{}")
    stability_data = state.get("stability_report", {}) 
    
    config = state.get("qos_config", {})
    profiles_def = config.get("profiles", {})
    
    try:
        metrics_data = json.loads(metrics_json)
    except:
        metrics_data = {}


    console.print(Panel("🚀 Allocation Advisor (Deep Scan)", style="grey50"))
    log.info("Avvio Allocation Advisor.")

    if not candidates:
        msg = "❌ Nessun nodo idoneo trovato."
        console.print(msg, style="bold red")
        return {"messages": [AIMessage(content=msg)]}
    
    # --- FASE 1: PREPARAZIONE PESI (WEIGHT MIXING) ---
    active_weights = {}

    # Caso: Nessun profilo specifico, usiamo peso di default su CPU
    if not target_profiles:
        active_weights = {"cpu_usage_pct": {"weight": 1.0, "direction": "minimize"}}
    else:

        # Per ogni profilo target prendo i pesi delle sue metriche 
        for p_name in target_profiles:
            p_weights = profiles_def.get(p_name, {}).get("scoring_weights", {})

            # Mixaggio pesi
            for metric, info in p_weights.items():

                # Se la metrica non esiste, la aggiungo
                if metric not in active_weights:
                    active_weights[metric] = info
                else:
                    # Se esiste (ovvero più profili target la usano), prendo il peso più alto
                    if info["weight"] > active_weights[metric]["weight"]:
                        active_weights[metric] = info
    
    # Normalizzazione pesi
    total_weight_sum = sum(info["weight"] for info in active_weights.values())
    normalized_weights_map = {} # Dict {nome_metrica -> {"weight": float, "direction": str, "stability_threshold": float}, ...}
    if total_weight_sum > 0:
        # Caso in cui ci sono più profili target
        for metric, info in active_weights.items():
            new_info = info.copy()
            new_info["weight"] = info["weight"] / total_weight_sum
            normalized_weights_map[metric] = new_info
    else:
        normalized_weights_map = active_weights

    # --- FASE 2: CALCOLO SCORE & RISK ASSESSMENT ---
    node_perf_scores = {n: 0.0 for n in candidates}
    node_risks = {n: [] for n in candidates} 

    for metric_name, info in normalized_weights_map.items(): 
        # Inizio il calcolo degli score per questa metrica dei vari nodi candidati

        weight = info.get("weight", 0)
        direction = info.get("direction", "minimize")
        
        values = [] # conterrà i valori di quella metrica per ogni nodo candidato
        valid_nodes = []
        for node in candidates:
            # Raccolgo per ogni nodo candidato il valore della metrica di interesse
            val = metrics_data.get(node, {}).get(metric_name)
            if val is not None:
                values.append(float(val))
                valid_nodes.append(node)
        
        if not values: continue

        # Normalizzazione valori MIN-MAX 
        # Questo mi permette di confrontare metriche con scale e unità di misura diverse
        min_v, max_v = min(values), max(values)
        spread = max_v - min_v 

        # Calcolo score -> 
        # - metric_score = (MAX - val) / spread  (se minimize)
        # - metric_score = (val - MIN) / spread  (se maximize)
        
        for node in valid_nodes:
            raw_val = float(metrics_data.get(node, {}).get(metric_name))
            
            metric_score = 0.0
            if spread == 0: 
                metric_score = 1.0 
            else:
                if direction == "minimize":
                    metric_score = (max_v - raw_val) / spread
                else:
                    metric_score = (raw_val - min_v) / spread
            
            # Raccolgo qui gli score parziali una metrica alla volta per ogni nodo candidato
            node_perf_scores[node] += (metric_score * weight)

            # Recupero le info sulla stabilità della metrica per questo nodo
            stab_info = stability_data.get(node, {}).get(metric_name, {})
            status = stab_info.get("status", "UNKNOWN")
            reason = stab_info.get("reason", "")
            
            if status in ["SPIKE", "CHAOTIC"]:
                node_risks[node].append(f"{metric_name} -> {reason}")

    # --- FASE 3: RANKING & RESCUE SCAN ---
    ranked_nodes = sorted(node_perf_scores.items(), key=lambda x: x[1], reverse=True)
    
    winner = ranked_nodes[0][0]
    runner_up = ranked_nodes[1][0] if len(ranked_nodes) > 1 else None
    
    safe_haven_node = None
    for node, score in ranked_nodes:
        if not node_risks[node]: 
            # Primo nodo che non ha rischi di instabilità su nessuna metrica in ordine di classifica
            safe_haven_node = node
            break 
            
    # --- FASE 4: DEFINIZIONE STRATEGIA ---
    strategy = "STANDARD"
    candidates_to_show = [winner]
    if runner_up:
        candidates_to_show.append(runner_up) 

    # Controllo se il nodo vincitore ha metriche non stabili
    winner_is_safe = (len(node_risks[winner]) == 0)
    
    if winner_is_safe:
        # Se il winner non ha nessuna metrica instabile, strategia CLEAR_WINNER
        strategy = "CLEAR_WINNER"
    
    elif safe_haven_node:
        # Se il winner non è safe, ma abbiamo un porto sicuro
        if safe_haven_node == runner_up:
            strategy = "CONSIDER_RUNNER_UP"
        else:
            strategy = "PROPOSE_SAFE_HAVEN"
            # Unica eccezione: qui dobbiamo aggiungere un terzo nodo
            candidates_to_show.append(safe_haven_node)

    else:
        # Nessun nodo è safe
        strategy = "ALL_RISKY"

    # --- LOGGING VISIVO (CONSOLE) ---
    table = Table(title="🏆 Ranking & Rescue Scan", show_header=True, header_style="bold magenta")
    table.add_column("Rank", style="dim", width=4)
    table.add_column("Nodo", style="bold")
    table.add_column("Score", justify="right")
    table.add_column("Status", style="red")
    
    for i, (node, score) in enumerate(ranked_nodes, 1):
        medal = "🥇" if i==1 else "🥈" if i==2 else ""
        if node == safe_haven_node and node != winner: medal += "🛡️" 
        
        issues = ", ".join(node_risks[node]) if node_risks[node] else "[green]✅ Stable[/green]"
        table.add_row(f"{i} {medal}", node, f"{score:.4f}", issues)
    
    # Visuale
    console.print(table)
    
    # Log Sistema
    log_ranking = [f"{n}: {s:.2f} ({'Risk' if node_risks[n] else 'Safe'})" for n, s in ranked_nodes]
    log.info(f"Ranking calcolato: {log_ranking} | Strategy: {strategy}")

    # --- FASE 5: PREPARAZIONE DATI PER LLM ---
    metrics_keys = list(normalized_weights_map.keys())
    
    def get_node_context(n_name):
        if not n_name: 
            return "N/A"
        raw = {k: metrics_data.get(n_name, {}).get(k) for k in metrics_keys}
        fmt = humanize_metrics_with_config(raw, config)
        risks = node_risks[n_name]
        return {
            "name": n_name,
            "score": f"{node_perf_scores[n_name]:.2f}",
            "risks": risks if risks else "STABLE", 
            "metrics": fmt 
        }

    context_data = [get_node_context(n) for n in candidates_to_show]
    compressed_table = json_to_markdown_table(context_data, key_label="Node")

    # Definizione delle istruzioni specifiche per ogni strategia
    strategies_map = {
        "CLEAR_WINNER": f"""
        - FOCUS: Conferma immediata.
        - SITUAZIONE: Il nodo '{winner}' è la scelta dominante (sia per potenza che stabilità).
        - AZIONE: Raccomandalo decisamente. Cita le metriche specifiche che lo rendono superiore.
        """,

        "CONSIDER_RUNNER_UP": f"""
        - FOCUS: Trade-off tra Potenza e Sicurezza.
        - SITUAZIONE: Il nodo '{winner}' è potente ma presenta rischi (vedi metriche 'risks'). Il nodo '{runner_up}' è l'alternativa stabile.
        - AZIONE: Evidenzia i rischi del vincitore e proponi '{runner_up}' come alternativa solida per carichi di lavoro che non tollerano fallimenti.
        """,

        "PROPOSE_SAFE_HAVEN": f"""
        - FOCUS: Mitigazione del Rischio (Critical Warning).
        - SITUAZIONE: I primi due classificati ({winner} e il secondo) sono INSTABILI. Spiega perchè.
        - AZIONE: Devi spostare l'attenzione sul 'Porto Sicuro': '{safe_haven_node}'.
        - ARGOMENTAZIONE: "Sebbene {winner} abbia metriche di performance migliori, per carichi critici consiglio vivamente {safe_haven_node} poiché è l'unico con stabilità operativa garantita."
        """,

        "ALL_RISKY": f"""
        - FOCUS: Gestione dell'incertezza.
        - SITUAZIONE: Nessun nodo offre garanzie di stabilità completa. Per ogni nodo spiega il perchè.
        - AZIONE: Consiglia '{winner}' come "il male minore" o la scelta tecnicamente migliore, ma allega un DISCLAIMER OBBLIGATORIO.
    
        """
    }

    # Selezione delle istruzioni in base alla strategia attuale
    # Se la strategia non è in lista, usa un fallback generico
    current_instructions = strategies_map.get(strategy, "Analizza i dati e raccomanda il nodo migliore bilanciando risorse e rischi.")

    # --- FASE 6: PROMPT DINAMICO ---
    prompt = f"""
    SEI UN ALLOCATION ADVISOR SRE AVANZATO.
    
    STRATEGIA RILEVATA DALL'ALGORITMO: {strategy}
    
    Ecco i dati dei candidati rilevanti:
    {compressed_table}
      
    COMPITO:
    Scrivi una raccomandazione professionale per l'utente.
    Indica i punti di forza di {winner} rispetto a {runner_up}, ma anche i punti di debolezza, se presenti, in tal caso NON inventare giustificazioni positive per il {winner} ma ammetti le sue debolezze.
    Basati sui dati reali (es. "Ha 5 GB di RAM in più). 
    Cita ognuna delle metriche date.
    
    ISTRUZIONI DI GENERAZIONE:
    {current_instructions}
   
    """
    
    response = await llm.ainvoke(state["messages"] + [HumanMessage(content=prompt)])
    
    return {"messages": [response]}

async def allocation_advisor_node_llm(state: AgentState):
    """
    VARIANTE AUTONOMA: L'LLM riceve i dati grezzi (metriche + stabilità) di tutti i candidati
    e decide autonomamente la classifica, pesando pro e contro.
    """
    
    # --- 1. RECUPERO DATI ---
    candidates = state.get("final_candidates", [])
    target_profiles = state.get("target_profiles", [])
    metrics_json = state.get("metrics_report", "{}")
    stability_data = state.get("stability_report", {})
    config = state.get("qos_config", {})
    
    # Header Visuale
    console.print(Panel("🧠 Allocation Advisor (LLM Reasoning Mode)", style="grey50"))
    log.info("Avvio Allocation Advisor (Modalità LLM autonoma).")

    # Se non ci sono candidati, usciamo subito
    if not candidates:
        msg = "❌ Nessun nodo idoneo trovato in base ai filtri applicati."
        console.print(msg, style="bold red")
        return {"messages": [AIMessage(content=msg)]}

    try:
        metrics_data = json.loads(metrics_json)
    except:
        metrics_data = {}

    # --- 2. PREPARAZIONE DEL CONTESTO (DATA PREP) ---
    # Invece di calcolare uno score, preparo una "Scheda Tecnica" per ogni nodo.
    
    candidates_context = []
    
    # Recupero le metriche rilevanti
    relevant_metrics = set()
    if target_profiles:
        for p in target_profiles:
            weights = config.get("profiles", {}).get(p, {}).get("scoring_weights", {})
            relevant_metrics.update(weights.keys())
    else:
        if candidates:
            relevant_metrics = metrics_data.get(candidates[0], {}).keys()

    # Creo anche una tabella visiva per l'utente per capire cosa sto mandando all'LLM
    table = Table(title="📊 Dati inviati all'LLM", show_header=True)
    table.add_column("Nodo", style="bold cyan")
    table.add_column("Stabilità", style="bold")
    table.add_column("Metriche Rilevanti")

    for node in candidates:
        # A. Dati Metrici (Performance)
        node_raw_metrics = {k: metrics_data.get(node, {}).get(k) for k in relevant_metrics}
        node_human_metrics = humanize_metrics_with_config(node_raw_metrics, config)
        
        # B. Dati Stabilità (Rischio)
        risk_flags = []
        node_stability = stability_data.get(node, {})
        for metric, info in node_stability.items():
            status = info.get("status", "UNKNOWN")
            if status in ["SPIKE", "CHAOTIC"]:
                risk_flags.append(f"{metric} is {status} ({info.get('reason')})")
        
        status_summary = "STABLE" if not risk_flags else f"UNSTABLE: {', '.join(risk_flags)}"
        
        # Coloriamo lo status per la tabella visiva
        status_visual = "[green]STABLE[/green]" if not risk_flags else f"[red]⚠️ {len(risk_flags)} Issues[/red]"

        # C. Creazione Scheda Nodo
        candidates_context.append({
            "node_name": node,
            "performance_metrics": node_human_metrics,
            "stability_status": status_summary,
            "_debug_raw_values": node_raw_metrics 
        })

        # Aggiungiamo riga alla tabella visiva
        table.add_row(node, status_visual, str(node_human_metrics))

    # --- MOSTRA DATI UTENTE ---
    console.print(table)
    log.info(f"Contesto preparato per {len(candidates)} nodi. Invio all'LLM...")

    # --- 3. COSTRUZIONE DEL PROMPT ---
    compressed_table = json_to_markdown_table(candidates_context, key_label="Node")
    
    prompt = f"""
    SEI UN SENIOR CAPACITY PLANNER (SRE).
    
    OBIETTIVO: Selezionare il nodo migliore per un task di tipo: {target_profiles}.
    
    Hai a disposizione le schede tecniche dei nodi candidati (che hanno già superato i requisiti minimi).
    Il tuo compito è stilare una CLASSIFICA (Ranking) basata su:
    1. **Performance**: Chi ha più risorse libere (es. RAM, CPU bassa).
    2. **Affidabilità**: Penalizza pesantemente i nodi segnati come "UNSTABLE" (SPIKE o CHAOTIC), a meno che il vantaggio di performance non sia enorme.
    
    DATI CANDIDATI:
    {compressed_table}
    
    OUTPUT RICHIESTO:
    Scrivi una raccomandazione professionale per l'utente e fornisci:
    1. **Classifica**: Ordina i nodi dal migliore al peggiore con una breve motivazione per ciascuno.
    2. **Il Vincitore**: Il nodo raccomandato.
    3. **Il Runner-up**: La migliore alternativa.
    4. **Ragionamento**: Illustra esattamente il processo logico usato per stilare la classifica dei nodi e le metriche che hanno pesato maggiormente. Spiega i motivi per cui hai scelto il vincitore rispetto agli altri (cita le metriche e i numeri esatti).
    5. **Warning**: Se il vincitore ha problemi di stabilità, evidenzialo chiaramente.
    
    """

    response = await llm.ainvoke(state["messages"] + [HumanMessage(content=prompt)])
    
    log.info("Risposta LLM generata.")
    return {"messages": [response]}

# async def conversational_node(state: AgentState):
#     """
#     Gestisce la conversazione 'umana'. 
#     Non usa tool, legge solo la history dei messaggi nello 'state' e risponde.
#     """
#     console.print("[italic dim]💬 Generazione risposta conversazionale...[/italic dim]")
    
#     # Passiamo tutta la storia dei messaggi all'LLM. 
#     # L'LLM vedrà le tabelle e i report generati nei turni precedenti.
#     response = await llm.ainvoke(state["messages"])

#     console.print(Panel(
#         response.content, 
#         title="💬 Assistant", 
#         border_style="white",
#         expand=False # Evita che il pannello occupi tutta la larghezza se il testo è breve
#     ))
    
#     return {"messages": [response]}