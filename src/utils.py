import json
from src.schemas import CapabilityReport
from langchain.messages import HumanMessage

def clean_tool_output(result) -> str:
    """
    Estrae il payload JSON puro dalla struttura complessa di LangChain/MCP.
    Input: [{'type': 'text', 'text': '{"resultType":...}', 'id': ...}]
    Output: '{"resultType":...}'
    """
    # Caso specifico del tuo log: Lista contenente dizionari con chiave 'text'
    if isinstance(result, list) and len(result) > 0:
        first_item = result[0]
        if isinstance(first_item, dict) and 'text' in first_item:
            return first_item['text']
            
    # Fallback: Se è già stringa o altro formato oggetto
    if hasattr(result, 'content'):
        return result.content
    
    return str(result)

def parse_prometheus_output(raw_output: str, metric_name: str) -> dict:
    parsed_data = {}

    raw_output = clean_tool_output(raw_output)
    
    try:
        # 1. Tentiamo di parsare la stringa come JSON
        data = json.loads(raw_output)
        
        # Verifica se è la struttura standard di Prometheus API
        if isinstance(data, dict) and "result" in data:
            results_list = data["result"]
            
            for item in results_list:
                # 2. Estrazione Identificativo Nodo
                metric_labels = item.get("metric", {})
                
                # Nel tuo esempio la chiave è "name", ma spesso è "instance". 
                # Le cerchiamo entrambe in ordine di priorità.
                node_name = metric_labels.get("name") or metric_labels.get("instance") or "unknown"
                
                # 3. Estrazione Valore
                # Il formato standard è "value": [timestamp, "valore_stringa"]
                value_list = item.get("value", [])
                if len(value_list) >= 2:
                    try:
                        val = float(value_list[1])
                        parsed_data[node_name] = round(val, 3)
                    except ValueError:
                        print(f"⚠️ Valore non numerico per {node_name}: {value_list[1]}")
                        
        else:
            print(f"⚠️ Struttura JSON imprevista per {metric_name}")

    except json.JSONDecodeError:
        # Se fallisce il JSON, significa che l'output era testo grezzo (fallback o errore)
        print(f"⚠️ Errore decoding JSON per {metric_name}. Output grezzo: {raw_output[:50]}...")
        
    return parsed_data

# --- FUNZIONE DI FORMATTAZIONE MARKDOWN ---
def format_capability_report_markdown(report: CapabilityReport) -> str:
    md_output = []
    
    # 1. REPORT DETTAGLIATO PER PROFILO
    for prof in report.profiles:
        # Intestazione Profilo
        md_output.append(f"### 🏷️ Profilo: {prof.profile_name}")
        
        # Sezione Requisiti
        md_output.append("**Requisiti:**")
        for req in prof.requirements:
            # Controllo difensivo: se è un dizionario (come ci aspettiamo ora)
            if isinstance(req, dict):
                m = req.get('metric', 'N/A')
                o = req.get('operator', '')
                t = req.get('threshold', '')
                r = req.get('reason', '')
                # Formattiamo: "- cpu_usage < 80 (Reason: ...)"
                line = f"- `{m}` {o} {t}"
                if r:
                    line += f" _({r})_"
                md_output.append(line)
            else:
                # Fallback se l'LLM decide comunque di mandare una stringa
                md_output.append(f"- {str(req)}")
            
        # Sezione Analisi (Metriche)
        md_output.append("\n**Analisi:**")
        for point in prof.metric_analysis:
            md_output.append(f"- {point}")
            
        # Sezione Risultato
        md_output.append(f"\n**Risultato:** {prof.conclusion}")
        md_output.append("---")

    # 2. TABELLA RIASSUNTIVA (Pivot Data)
    # Dobbiamo incrociare i dati: Righe=Nodi, Colonne=Profili
    if report.all_nodes_scanned and report.profiles:
        md_output.append("## 📊 Tabella Riassuntiva Idoneità")
        
        # Header Colonne (i profili)
        profile_names = [p.profile_name for p in report.profiles]
        header = "| Nodo | " + " | ".join(profile_names) + " |"
        separator = "| :--- | " + " | ".join([":---:" for _ in profile_names]) + " |"
        
        md_output.append(header)
        md_output.append(separator)
        
        # Righe (i nodi)
        for node in report.all_nodes_scanned:
            row = f"| **{node}** |"
            
            for prof in report.profiles:
                # Controlliamo se questo nodo è nella lista dei 'buoni' di quel profilo
                is_suitable = node in prof.suitable_nodes
                icon = "✅" if is_suitable else "❌"
                row += f" {icon} |"
            md_output.append(row)

    # 3. SINTESI
    md_output.append(f"\n### 📝 Sintesi Finale\n{report.final_synthesis}")
    
    return "\n".join(md_output)


def humanize_metrics_with_config(metrics_dict: dict, qos_config: dict) -> dict:
    """
    Formatta i valori usando l'unità di misura definita nel qos_config.json.
    """
    humanized = {}
    metrics_def = qos_config.get("metrics", {}) # Recuperiamo le definizioni

    for key, value in metrics_dict.items():
        if value is None:
            humanized[key] = "N/A"
            continue
        
        # Recuperiamo l'unità specifica per questa metrica dal config
        # Se non trovata, default a 'raw'
        unit_type = metrics_def.get(key, {}).get("unit", "raw")
        
        try:
            val_float = float(value)
            
            if unit_type == "percentage_100":
                # Il valore è già 0-100 o 0-1? Nel tuo config le query hanno spesso "* 100"
                # Quindi assumiamo sia già in scala 0-100.
                humanized[key] = f"{val_float:.2f}%"
                
            elif unit_type == "bytes":
                # Conversione dinamica GB/MB
                if val_float > 1024**3:
                    humanized[key] = f"{val_float / (1024**3):.2f} GB"
                elif val_float > 1024**2:
                    humanized[key] = f"{val_float / (1024**2):.2f} MB"
                else:
                    humanized[key] = f"{val_float:.0f} bytes"
                    
            elif unit_type == "rate":
                humanized[key] = f"{val_float:.2f} ops/s"
                
            else: # unit == "raw" o sconosciuto
                humanized[key] = f"{val_float:.2f}"
                
        except (ValueError, TypeError):
            humanized[key] = str(value)
            
    return humanized
    
# --- HELPER FUNCTIONS: STABILITY CORE ---

def get_strictest_threshold_config(target_profiles: list, all_profiles_def: dict) -> dict:
    """
    Estrae le soglie di stabilità dai profili target.
    Se c'è conflitto su una metrica, vince la soglia MINORE (Principio di Cautela).
    """
    threshold_map = {} # { "cpu_usage": 5.0, "disk_usage": 2.0 }

    for p_name in target_profiles:
        p_weights = all_profiles_def.get(p_name, {}).get("scoring_weights", {})

        for metric_name, conf in p_weights.items():
            # Cerchiamo se c'è un override esplicito nel profilo
            t_new = conf.get("stability_threshold")
            
            # Se questo profilo non definisce una soglia, non impone restrizioni extra
            if t_new is None:
                continue

            # Se è la prima volta che vediamo una soglia per questa metrica
            if metric_name not in threshold_map:
                threshold_map[metric_name] = float(t_new)
            else:
                # Conflitto: teniamo la soglia più bassa (più severa)
                threshold_map[metric_name] = min(threshold_map[metric_name], float(t_new))
    
    return threshold_map

def get_physical_threshold(metric_name: str, metric_def: dict, strict_thresholds: dict) -> float:
    """
    Cascata a 3 livelli per trovare la soglia fisica (Delta):
    1. Profilo (Strict Map calcolata sopra) -> VINCE SEMPRE
    2. Metrica (Json definition)
    3. Unità (Default)
    """
    # 1. LIVELLO PROFILO (SLA attivo)
    if metric_name in strict_thresholds:
        return strict_thresholds[metric_name]

    # 2. LIVELLO METRICA (Hardware specific definition)
    if "stability_threshold" in metric_def:
        return float(metric_def["stability_threshold"])

    # 3. LIVELLO UNITÀ (Fallback generico)
    unit_type = metric_def.get("unit", "raw")
    if unit_type == "percentage_100": return 5.0      
    elif unit_type == "bytes": return 200 * 1024**2   
    elif unit_type == "rate": return 5.0              
    else: return 1.0

def classify_stability(current, avg, std, delta_threshold):
    """
    Calcola Z-Score, Coefficiente di Variazione (CV) e classifica lo stato.
    Restituisce un dizionario con "status", "reason" e metriche calcolate.
    """
    if std is None or avg is None or current is None:
        return {"status": "UNKNOWN", "reason": "No Data", "metrics": {}}
    
    delta = abs(current - avg)
    
    if std > 0:
        z_score = delta / std
    else:
        z_score = 0.0 if delta == 0 else 999.9
    
    # Coefficiente di Variazione (CV)
    # Se la media storica è inferiore alla soglia di rilevanza fisica (es. Load < 0.2),
    # allora le fluttuazioni relative (CV) sono matematicamente enormi ma fisicamente inutili.
    # In questo caso, sopprimiamo il CV.
    if avg < delta_threshold:
        cv = 0.0
    else:
        cv = (std / avg) if avg > 0 else 0.0

    # Costanti di sistema
    Z_THRESHOLD = 2.0
    CV_CHAOS_THRESHOLD = 0.3

    # LOGICA DI CLASSIFICAZIONE
    if cv > CV_CHAOS_THRESHOLD:
        return {"status": "CHAOTIC", "reason": f"Alta variabilità (CV={cv:.2f})", "metrics": {"z": z_score, "cv": cv}}

    if z_score > Z_THRESHOLD:
        if delta > delta_threshold:
            return {"status": "SPIKE", "reason": f"Picco anomalo (+{delta:.2f} > soglia {delta_threshold})", "metrics": {"z": z_score, "cv": cv}}
        else:
            return {"status": "FALSE_ALARM", "reason": "Variazione statistica trascurabile", "metrics": {"z": z_score, "cv": cv}}
            
    return {"status": "STABLE", "reason": "Nella norma", "metrics": {"z": z_score, "cv": cv}}



def json_to_markdown_table(data, key_label="Node", columns=None) -> str:

    """
    Converte strutture dati in Markdown.
    Args:
        data: List[Dict] o Dict[Dict]
        key_label: Nome della prima colonna (chiave primaria)
        columns: (Opzionale) Lista di stringhe. Se presente, include SOLO queste colonne nell'ordine dato.
    """
    if not data:
        return "Nessun dato disponibile."

    # --- 1. NORMALIZZAZIONE (Invariata) ---
    rows = []
    if isinstance(data, dict):
        for key, attributes in data.items():
            if isinstance(attributes, dict):
                row = attributes.copy()
                row[key_label] = key 
                rows.append(row)
    elif isinstance(data, list):
        import copy
        rows = copy.deepcopy(data)
    else:
        return str(data)

    if not rows: return "Tabella vuota."

    # --- 2. FLATTENING (Invariato) ---
    final_rows = []
    for r in rows:
        flat_row = r.copy()
        if "metrics" in flat_row and isinstance(flat_row["metrics"], dict):
            nested_metrics = flat_row.pop("metrics")
            flat_row.update(nested_metrics)
        final_rows.append(flat_row)

    # --- 3. GESTIONE COLONNE (MODIFICATO) ---
    
    if columns:
        # SE L'UTENTE SPECIFICA LE COLONNE: Usiamo rigorosamente quelle + la key_label
        # (A meno che la key_label non sia già inclusa in columns)
        sorted_headers = []
        if key_label not in columns:
            sorted_headers.append(key_label)
        sorted_headers.extend(columns)
    else:
        # ALTRIMENTI: Auto-discovery (Logica precedente)
        headers = set()
        for r in final_rows:
            headers.update(r.keys())
        
        priority_order = [key_label, "name", "score", "risks", "status", "stability_status"]
        sorted_headers = []
        for p in priority_order:
            if p in headers:
                sorted_headers.append(p)
                headers.remove(p)
        sorted_headers += sorted(list(headers))

    # --- 4. RENDERING MARKDOWN (Invariato) ---
    lines = []
    lines.append(" | ".join(sorted_headers))
    lines.append(" | ".join(["---"] * len(sorted_headers)))
    
    for row in final_rows:
        values = []
        for h in sorted_headers:
            val = row.get(h, " - ") 
            if isinstance(val, float): val = f"{val:.2f}"
            if isinstance(val, list): val = ", ".join(str(x) for x in val)
            values.append(str(val))
        lines.append(" | ".join(values))

    return "\n".join(lines)

def get_last_user_message(messages):
    """
    Scorre la lista dei messaggi al contrario per trovare 
    l'ultimo input fornito dall'utente (HumanMessage).
    """
    # reversed(messages) ci permette di partire dall'ultimo messaggio aggiunto
    for message in reversed(messages):
        if isinstance(message, HumanMessage):
            return message.content
            
    # Fallback di sicurezza se non si trova nessun messaggio umano
    return ""