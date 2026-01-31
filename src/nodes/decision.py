import json
import operator
from rich.table import Table
from rich.panel import Panel
from src.state import AgentState
from src.config import llm, console
from src.schemas import (UserRequestClassification,
                              TaskProfileIntent,
                                RequirementExtraction)
from langchain.messages import HumanMessage
from src.utils import humanize_metrics_with_config, json_to_markdown_table


# --- NODO 3: CLASSIFIER ---
async def classify_intent_node(state: AgentState):
    """
    Analizza l'input utente e determina l'intento: "allocation" o "status".
    Inoltre, estrae un filtro target se specificato (es. nome server).  
    """
    user_input = state["messages"][0].content

    
    prompt = f"""
    Analizza la seguente richiesta e classificala: "{user_input}"
    
    Restituisci l'intento e inserisci in "target_filter" il nome del server specifico se menzionato, altrimenti non inserire nulla. 
    """
    structured_llm = llm.with_structured_output(UserRequestClassification)
    response = await structured_llm.ainvoke(prompt)
    

    intent = response['intent']
    target = response['target_filter']
    
    # STAMPA MIGLIORATA
    color = "green" if intent == "allocation" else "blue"
    icon = "🚀" if intent == "allocation" else "📋"
    
    msg = f"Intent rilevato: [bold {color}]{intent.upper()}[/bold {color}]\n"
    if target:
        msg += f"Target specifico: [bold]{target}[/bold]"
    else:
        msg += "Ambito: [italic]Intero Cluster[/italic]"
        
    console.print(Panel(msg, title=f"{icon} Intent Classifier", border_style=color))

    return {"intent": intent, "target_filter": target}


async def intent_classifier_node(state: AgentState):
    """
    Step 1: Capire la natura del task (CPU vs RAM vs Disk).
    Non si preoccupa dei numeri specifici.
    """
    # --- FIX: NON usare state["messages"][-1] alla cieca ---
    # Scansioniamo la storia all'indietro per trovare l'ultimo messaggio UMANO
    user_input = "N/A"
    for msg in reversed(state["messages"]):
        if isinstance(msg, HumanMessage):
            user_input = msg.content
            break
            
    # Debug opzionale per vedere cosa sta leggendo
    # print(f"DEBUG: Analyzing User Input: {user_input}")

    config = state.get("qos_config", {})
    profiles = config.get("profiles", {})
    
    # Passiamo SOLO "description". La key_label sarà il nome del profilo.
    # Escludiamo "required_conditions" e "scoring_weights" che confondono l'LLM.
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
    
    console.print(Panel(
        f"Task mappato su: [bold magenta]{sel_profiles}[/bold magenta]\n[italic dim]\"{reason}\"[/italic dim]",
        title="🧠 Technical Profiler",
        border_style="magenta"
    ))
    
    
    return {
        "target_profiles": sel_profiles,
        "classification_reason": reason
    }


async def constraint_extractor_node(state: AgentState):
    """
    Step 2: Estrarre numeri e convertirli in vincoli Prometheus validi.
    """
    # --- FIX: Recupero ultimo messaggio umano ---
    user_input = "N/A"
    for msg in reversed(state["messages"]):
        if isinstance(msg, HumanMessage):
            user_input = msg.content
            break

    config = state.get("qos_config", {})
    metrics = config.get("metrics", {})
    

    # Chiediamo SOLO le colonne utili per capire il significato della metrica.
    # La colonna 'query' verrà ignorata automaticamente.
    metrics_table = json_to_markdown_table(
        metrics, 
        key_label="Metric", 
        columns=["unit", "description"] # <--- ECCO LA MAGIA
    )

    
    prompt = f"""
    SEI UN ESTRATTORE DI VINCOLI TECNICI.
    
    Il tuo unico obiettivo è trovare numeri e requisiti nella richiesta e convertirli in filtri per metriche.
    
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
        
        # STAMPA MIGLIORATA

        # Serializziamo per salvare nello stato (Pydantic -> Dict)
        constraints_list = [c.model_dump() for c in result.constraints]
    
        if constraints_list:
            c_text = "\n".join([f"- [bold]{c['metric_name']}[/bold] {c['operator']} {c['value']} ({c['original_text']})" for c in constraints_list])
            console.print(Panel(c_text, title="📏 Vincoli Estratti", border_style="yellow"))
        else:
            console.print("[dim yellow]   ℹ️  Nessun vincolo numerico esplicito trovato.[/dim yellow]")
        
        return {"explicit_constraints": constraints_list}
        
    except Exception:
        return {"explicit_constraints": []}


async def candidate_filter_node(state: AgentState):
    """
    FILTRO CANDIDATI (The Funnel).
    
    Questo nodo riceve:
    1. I risultati tecnici di TUTTI i profili (dal Map-Reduce 'single_profile_evaluator').
    2. L'intenzione dell'utente (da 'intent_classifier').
    3. I vincoli espliciti (da 'constraint_extractor').
    
    Obiettivo: Trovare l'intersezione dei nodi che soddisfano TUTTO.
    """
    
    # --- 1. RECUPERO DATI DALLO STATO ---
    
    # A. I profili che l'utente vuole (es. ["cpu-bound", "memory-bound"])
    target_profiles = state.get("target_profiles", [])
    
    # B. I report tecnici generati dai worker paralleli
    # Lista di stringhe JSON: [{"profile_name": "cpu-bound", "qualified_nodes": ["w1", "w2"]}, ...]
    raw_results = state.get("profile_results", [])
    
    # C. Vincoli espliciti (es. RAM > 8GB)
    user_constraints = state.get("explicit_constraints", [])
    
    # D. Dati metrici grezzi (necessari per controllare i vincoli numerici)
    metrics_json = state.get("metrics_report", "{}")
    try:
        metrics_data = json.loads(metrics_json) # Dict: {"worker-1": {"cpu": 10...}, ...}
    except:
        metrics_data = {}

    console.print("\n[bold grey50]--- 🌪️ Filtering Candidates ---[/bold grey50]")

    # --- FASE 1: FILTRO PER PROFILO (Capability Intersection) ---
    # Logica: Se l'utente vuole CPU e RAM, il nodo deve essere idoneo per ENTRAMBI.
    
    # Parsiamo i risultati del Map-Reduce in un dizionario per accesso rapido
    # Structure: {"cpu-bound": {"w1", "w2"}, "disk-bound": {"w1"}}
    profile_qualification_map = {}
    for r_str in raw_results:
        try:
            r_dict = json.loads(r_str)
            p_name = r_dict.get("profile_name")
            q_nodes = set(r_dict.get("qualified_nodes", [])) # Usiamo set per intersezioni
            profile_qualification_map[p_name] = q_nodes
        except:
            continue

    # Calcolo dei candidati iniziali
    initial_candidates = set()
    
    if not target_profiles:
        # Caso: L'utente non ha specificato un profilo (richiesta generica)
        # O il classificatore ha fallito. Consideriamo TUTTI i nodi che hanno superato almeno un test.
        console.print("[yellow]⚠️ Nessun profilo target specifico. Considero tutti i nodi tecnicamente validi.[/yellow]")
        for nodes_set in profile_qualification_map.values():
            initial_candidates.update(nodes_set)
    else:
        # Caso: L'utente vuole specifici profili (Intersezione)
        # Inizializziamo con i nodi del primo profilo target
        first_prof = target_profiles[0]
        if first_prof in profile_qualification_map:
            initial_candidates = set(profile_qualification_map[first_prof])
            console.print(f"Base candidati ({first_prof}): {len(initial_candidates)} nodi.")
        else:
            console.print(f"[red]❌ Errore: Nessun risultato tecnico per il profilo {first_prof}[/red]")
            # Se manca il primo profilo, l'intersezione sarà vuota
            initial_candidates = set()

        # Intersezione con gli altri profili target (se ce ne sono altri)
        for p_name in target_profiles[1:]:
            p_nodes = profile_qualification_map.get(p_name, set())
            prev_count = len(initial_candidates)
            initial_candidates.intersection_update(p_nodes)
            console.print(f"Intersezione con {p_name}: {prev_count} -> {len(initial_candidates)} nodi.")

    # --- FASE 2: FILTRO PER VINCOLI UTENTE (Explicit Constraints) ---
    # Logica: Applicare le regole matematiche (es. ram > 8GB)
    
    final_candidates = list(initial_candidates)
    
    if user_constraints and final_candidates:
        console.print(f"Applicazione {len(user_constraints)} vincoli esplicitati dall'utente...")
        
        # Mappa stringa -> funzione operatore
        ops = {
            ">": operator.gt, "<": operator.lt,
            ">=": operator.ge, "<=": operator.le,
            "==": operator.eq, "!=": operator.ne
        }
        
        # Iteriamo su una COPIA della lista per poter rimuovere elementi in sicurezza
        for node in list(final_candidates):
            node_metrics = metrics_data.get(node, {})
            
            for constr in user_constraints:
                metric_key = constr["metric_name"]
                target_val = constr["value"]
                op_sym = constr["operator"]
                op_func = ops.get(op_sym)
                
                # Recuperiamo il valore reale dal nodo
                real_val = node_metrics.get(metric_key)
                
                # Check 1: La metrica esiste?
                if real_val is None:
                    console.print(f"[dim red]   - {node} scartato: Manca dato {metric_key}[/dim red]")
                    if node in final_candidates: final_candidates.remove(node)
                    break 
                
                # Check 2: Il valore rispetta la soglia?
                if op_func and not op_func(real_val, target_val):
                    console.print(f"[dim red]   - {node} scartato: {metric_key}={real_val} non è {op_sym} {target_val}[/dim red]")
                    if node in final_candidates: final_candidates.remove(node)
                    break # Nodo scartato, inutile controllare altri vincoli

    # --- OUPUT FINALE ---
    
    # STAMPA FINALE DEL NODO
    if final_candidates:
        console.print(f"[green]   ✅ Finalisti:[/green] [bold]{', '.join(final_candidates)}[/bold]")
    else:
        console.print("[bold red]   ⛔ Nessun candidato sopravvissuto ai filtri.[/bold red]")

    # Salviamo nello stato per l'Advisor successivo
    return {"final_candidates": final_candidates}

async def allocation_advisor_node(state: AgentState):
    """
    DECISION ENGINE (LOGICA SRE AVANZATA).
    
    Questo è il "Cervello" decisionale dell'agente.
    Non si limita a guardare chi ha più RAM libera. Incrocia due dimensioni:
    1. PERFORMANCE (Score 0-1): Chi è più potente ADESSO.
    2. STABILITÀ (Risk Flags): Chi è stato affidabile nelle ULTIME 24H.
    
    Novità principale: "RESCUE SCAN".
    Se i nodi più potenti sono instabili, l'algoritmo scorre la classifica 
    verso il basso per trovare un "Porto Sicuro" (Safe Haven).
    """
    
    # --- 0. RECUPERO CONTESTO DALLO STATO ---
    candidates = state.get("final_candidates", [])
    target_profiles = state.get("target_profiles", [])
    
    # Report grezzo delle metriche attuali (snapshot istantaneo)
    metrics_json = state.get("metrics_report", "{}")
    
    # Report di stabilità generato dal nodo precedente (contiene le etichette SPIKE, CHAOTIC)
    stability_data = state.get("stability_report", {}) 
    
    # Configurazioni generali
    config = state.get("qos_config", {})
    profiles_def = config.get("profiles", {})
    
    try:
        metrics_data = json.loads(metrics_json)
    except:
        metrics_data = {}

    console.print("\n[bold grey50]--- 🚀 Allocation Advisor (Deep Scan) ---[/bold grey50]")

    if not candidates:
        return {"messages": [HumanMessage(content="❌ Nessun nodo idoneo trovato.")]}
    
    # --- FASE 1: PREPARAZIONE PESI (WEIGHT MIXING) ---
    # Determiniamo quali metriche contano per questa decisione.
    # Se l'utente non ha specificato profili, usiamo la CPU come default.
    
    active_weights = {}
    if not target_profiles:
        active_weights = {"cpu_usage_pct": {"weight": 1.0, "direction": "minimize"}}
    else:
        # Se ci sono più profili (es. CPU + DISK), fondiamo i pesi.
        # Logica: "Max Weight Wins". Se per un profilo la CPU pesa 0.9 e per l'altro 0.1,
        # nel computo totale peserà 0.9 (è critica).
        for p_name in target_profiles:
            p_weights = profiles_def.get(p_name, {}).get("scoring_weights", {})
            for metric, info in p_weights.items():
                if metric not in active_weights:
                    active_weights[metric] = info
                else:
                    if info["weight"] > active_weights[metric]["weight"]:
                        active_weights[metric] = info
    
    # Normalizzazione: La somma dei pesi deve fare 1.0 per avere uno score coerente (0-100%)
    total_weight_sum = sum(info["weight"] for info in active_weights.values())
    normalized_weights_map = {}
    if total_weight_sum > 0:
        for metric, info in active_weights.items():
            new_info = info.copy()
            new_info["weight"] = info["weight"] / total_weight_sum
            normalized_weights_map[metric] = new_info
    else:
        normalized_weights_map = active_weights

    # --- FASE 2: CALCOLO SCORE & RISK ASSESSMENT ---
    # Qui costruiamo la classifica.
    
    node_perf_scores = {n: 0.0 for n in candidates}
    node_risks = {n: [] for n in candidates} # Lista dei problemi rilevati per ogni nodo

    for metric_name, info in normalized_weights_map.items():
        weight = info.get("weight", 0)
        direction = info.get("direction", "minimize") # minimize (CPU) o maximize (RAM Free)
        
        # A. Raccogliamo tutti i valori per capire il range (Min-Max)
        values = []
        valid_nodes = []
        for node in candidates:
            val = metrics_data.get(node, {}).get(metric_name)
            if val is not None:
                values.append(float(val))
                valid_nodes.append(node)
        
        if not values: continue

        min_v, max_v = min(values), max(values)
        spread = max_v - min_v # Ampiezza del range
        
        # B. Calcoliamo il punteggio per ogni nodo su questa specifica metrica
        for node in valid_nodes:
            raw_val = float(metrics_data.get(node, {}).get(metric_name))
            
            # Normalizzazione Min-Max (Scala 0.0 - 1.0)
            # Ci permette di sommare pere (GB) con mele (%)
            perf_score = 0.0
            if spread == 0: 
                perf_score = 1.0 # Tutti i nodi sono uguali
            else:
                if direction == "minimize":
                    # Migliore se più basso (es. CPU Usage): (Max - Val) / Spread
                    perf_score = (max_v - raw_val) / spread
                else:
                    # Migliore se più alto (es. RAM Available): (Val - Min) / Spread
                    perf_score = (raw_val - min_v) / spread
            
            # Aggiungiamo al punteggio totale pesato del nodo
            node_perf_scores[node] += (perf_score * weight)

            # C. Risk Check (Controllo Etichette di Stabilità)
            # Non ricalcoliamo nulla. Leggiamo solo se il nodo precedente ha messo un flag "SPIKE" o "CHAOTIC".
            stab_info = stability_data.get(node, {}).get(metric_name, {})
            status = stab_info.get("status", "UNKNOWN")
            
            if status in ["SPIKE", "CHAOTIC", "DRIFT"]:
                reason = stab_info.get("reason", "")
                # Loggiamo il rischio: "cpu_usage: CHAOTIC"
                node_risks[node].append(f"{metric_name}: {status}")

    # --- FASE 3: RANKING & RESCUE SCAN (IL CUORE SRE) ---
    
    # 1. Ordinamento puramente meritocratico (per performance)
    ranked_nodes = sorted(node_perf_scores.items(), key=lambda x: x[1], reverse=True)
    
    winner = ranked_nodes[0][0]
    # Gestione caso lista con 1 solo elemento
    runner_up = ranked_nodes[1][0] if len(ranked_nodes) > 1 else None
    
    # 2. "Rescue Scan": Cerchiamo il Safe Haven (Porto Sicuro)
    # Scorriamo la classifica dal primo all'ultimo. Il primo che NON ha rischi è il nostro salvagente.
    safe_haven_node = None
    for node, score in ranked_nodes:
        if not node_risks[node]: # Se la lista rischi è vuota (STABILE)
            safe_haven_node = node
            break # Trovato! Non serve scendere oltre (è il migliore tra i sicuri)
            
    # --- FASE 4: DEFINIZIONE STRATEGIA (COSA DIRE ALL'LLM?) ---
    # L'LLM non deve decidere, deve solo spiegare la nostra decisione.
    
    strategy = "STANDARD"
    candidates_to_show = [winner] # Di base mostriamo sempre il vincitore

    winner_is_safe = (len(node_risks[winner]) == 0)
    
    if winner_is_safe:
        # SCENARIO A: Tutto perfetto. Il più potente è anche stabile.
        strategy = "CLEAR_WINNER"
        if runner_up: candidates_to_show.append(runner_up)
    
    else:
        # Il vincitore ha dei problemi. Dobbiamo valutare le alternative.
        if safe_haven_node:
            if safe_haven_node == runner_up:
                # SCENARIO B: Il secondo classificato è il porto sicuro.
                # Conflitto classico: "Vuoi potenza (Winner) o sicurezza (Runner-up)?"
                strategy = "CONSIDER_RUNNER_UP"
                candidates_to_show.append(runner_up)
            else:
                # SCENARIO C (CRITICO): Il porto sicuro è nascosto in basso (es. 3° o 4° posto).
                # Dobbiamo forzare la mano e mostrare questo terzo candidato.
                strategy = "PROPOSE_SAFE_HAVEN"
                if runner_up: candidates_to_show.append(runner_up)
                candidates_to_show.append(safe_haven_node) # Aggiungiamo esplicitamente il Safe Haven
        else:
            # SCENARIO D: Nessun nodo è stabile. Siamo nei guai.
            # Consigliamo il meno peggio (Winner) ma con forti warning.
            strategy = "ALL_RISKY"
            if runner_up: candidates_to_show.append(runner_up)

    # --- LOGGING VISIVO (CONSOLE) ---
    table = Table(title="🏆 Ranking & Rescue Scan", show_header=True, header_style="bold magenta")
    table.add_column("Rank", style="dim", width=4)
    table.add_column("Nodo", style="bold")
    table.add_column("Score", justify="right")
    table.add_column("Status", style="red")
    
    for i, (node, score) in enumerate(ranked_nodes, 1):
        medal = "🥇" if i==1 else "🥈" if i==2 else ""
        # Mettiamo uno scudo visivo se è il Safe Haven ma non ha vinto la medaglia d'oro
        if node == safe_haven_node and node != winner: medal += "🛡️" 
        
        issues = ", ".join(node_risks[node]) if node_risks[node] else "[green]✅ Stable[/green]"
        table.add_row(f"{i} {medal}", node, f"{score:.4f}", issues)
    console.print(table)

    # --- FASE 5: PREPARAZIONE DATI PER LLM ---
    metrics_keys = list(normalized_weights_map.keys())
    
    # Funzione helper per formattare i dati di un singolo nodo
    def get_node_context(n_name):
        if not n_name: return "N/A"
        # Estraiamo i dati grezzi
        raw = {k: metrics_data.get(n_name, {}).get(k) for k in metrics_keys}
        # Li convertiamo in stringhe leggibili (es. "10 GB")
        fmt = humanize_metrics_with_config(raw, config)
        risks = node_risks[n_name]
        return {
            "name": n_name,
            "score": f"{node_perf_scores[n_name]:.2f}", # Score tecnico
            "risks": risks if risks else "STABILE",      # Etichette di rischio
            "metrics": fmt                               # Dati leggibili
        }

    # Creiamo il JSON di contesto SOLO per i candidati scelti dalla strategia
    context_data = [get_node_context(n) for n in candidates_to_show]

    compressed_table = json_to_markdown_table(context_data, key_label="Node")

    # --- FASE 6: PROMPT DINAMICO ---
    # Istruiamo l'LLM su come comportarsi in base alla Strategy calcolata in Fase 4.
    
    prompt = f"""
    SEI UN ALLOCATION ADVISOR SRE AVANZATO.
    
    STRATEGIA RILEVATA DALL'ALGORITMO: {strategy}
    
    Ecco i dati dei candidati rilevanti:
    {compressed_table}
      
    COMPITO:
    Scrivi una raccomandazione professionale per l'utente.
    Spiega PERCHÉ {winner} è la scelta migliore basandoti sui dati reali (es. "Ha 5GB di RAM in più"). Cita ognuna delle metriche dateti.
    Spiega cosa lo differenzia dal secondo classificato.
    
    ISTRUZIONI DI GENERAZIONE (Segui rigorosamente la strategia):
    
    1. **CLEAR_WINNER**: 
       - Il nodo '{winner}' è la scelta perfetta (Potente e Stabile).
       - Raccomandalo senza esitazioni citando le sue metriche.
    
    2. **CONSIDER_RUNNER_UP**: 
       - Il Winner '{winner}' è potente ma ha rischi di stabilità (vedi 'risks').
       - Il Runner-up '{runner_up}' è stabile.
       - Devi proporre il Runner-up come alternativa solida.
       
    3. **PROPOSE_SAFE_HAVEN** (Attenzione qui):
       - I primi due classificati sono INSTABILI.
       - L'algoritmo ha trovato un "Porto Sicuro": '{safe_haven_node}'.
       - Il tuo compito è dire: "Sebbene {winner} sia più potente, consiglio vivamente {safe_haven_node} per carichi critici perché è l'unico con stabilità garantita."
       
    4. **ALL_RISKY**: 
       - Tutti i nodi mostrano instabilità.
       - Consiglia il Winner '{winner}' ma aggiungi un disclaimer: "Monitoraggio richiesto: nessun nodo garantisce stabilità perfetta al momento."
    
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
    
    # Se non ci sono candidati, usciamo subito
    if not candidates:
        return {"messages": [HumanMessage(content="❌ Nessun nodo idoneo trovato in base ai filtri applicati.")]}

    try:
        metrics_data = json.loads(metrics_json)
    except:
        metrics_data = {}

    console.print("\n[bold grey50]--- 🧠 Allocation Advisor (LLM Reasoning Mode) ---[/bold grey50]")

    # --- 2. PREPARAZIONE DEL CONTESTO (DATA PREP) ---
    # Invece di calcolare uno score, prepariamo una "Scheda Tecnica" per ogni nodo.
    # L'LLM deve ricevere dati puliti (es. "8 GB" e non "8589934592").
    
    candidates_context = []
    
    # Recuperiamo le metriche rilevanti per i profili richiesti
    # (Per evitare di passare all'LLM 50 metriche inutili se ne servono solo 2)
    relevant_metrics = set()
    if target_profiles:
        for p in target_profiles:
            # Prendiamo le metriche definite nei pesi del profilo
            weights = config.get("profiles", {}).get(p, {}).get("scoring_weights", {})
            relevant_metrics.update(weights.keys())
    else:
        # Fallback: se non ci sono profili, prendiamo tutto quello che abbiamo
        if candidates:
            relevant_metrics = metrics_data.get(candidates[0], {}).keys()

    for node in candidates:
        # A. Dati Metrici (Performance)
        node_raw_metrics = {k: metrics_data.get(node, {}).get(k) for k in relevant_metrics}
        node_human_metrics = humanize_metrics_with_config(node_raw_metrics, config)
        
        # B. Dati Stabilità (Rischio)
        # Cerchiamo se ci sono flag rossi nel report di stabilità
        risk_flags = []
        node_stability = stability_data.get(node, {})
        for metric, info in node_stability.items():
            status = info.get("status", "UNKNOWN")
            if status in ["SPIKE", "CHAOTIC", "DRIFT"]:
                risk_flags.append(f"{metric} is {status} ({info.get('reason')})")
        
        status_summary = "STABLE" if not risk_flags else f"UNSTABLE: {', '.join(risk_flags)}"

        # C. Creazione Scheda Nodo
        candidates_context.append({
            "node_name": node,
            "performance_metrics": node_human_metrics,
            "stability_status": status_summary,
            # Aggiungiamo un campo raw per aiutare l'LLM a ordinare se le unità sono confuse
            "_debug_raw_values": node_raw_metrics 
        })

    # --- 3. COSTRUZIONE DEL PROMPT ---
    # Qui diamo all'LLM il ruolo di Decision Maker.

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

    # --- 4. INVOCAZIONE ---
    response = await llm.ainvoke(state["messages"] + [HumanMessage(content=prompt)])
    
    return {"messages": [response]}
