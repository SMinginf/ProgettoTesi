import json
from langchain_core.messages import SystemMessage
from src.state import AgentState
from src.config import client
# IMPORTA IL LOGGER CENTRALIZZATO
from src.logger import log

# --- NODO 1: SETUP E CONTESTO ---
async def context_manager_node(state: AgentState):
    ''' 
    1. Verifica salute Prometheus.
    2. Recupera i target.
    3. SCARICA la configurazione QoS (Resource) dal server.
    '''    
    # 1. Recuperiamo tutti i tool disponibili
    tools = await client.get_tools()
    
    # 1. Recupero dei tool necessari
    health_tool = next((t for t in tools if t.name == "health_check"), None)
    target_tool = next((t for t in tools if t.name == "get_targets"), None)
    
    # --- FASE 1: HEALTH CHECK ---
    if not health_tool:
        log.critical("Tool 'health_check' non trovato su MCP Server.")
        return {
            "messages": [SystemMessage(content="ERRORE CRITICO: Tool 'health_check' non trovato.")],
            "sanity_check_ok": False
        }
        
    try:
        # Eseguiamo health_check
        health_result = await health_tool.ainvoke({})
        
        health_str = str(health_result).lower()
        if "error" in health_str or "unhealthy" in health_str or "down" in health_str:
             log.error(f"Health Check Fallito: {health_result}")
             raise Exception(f"Health Check Fallito: {health_result}")
        else:
            log.info("✅ Prometheus Health Check: OK")
             
    except Exception as e:
        log.error(f"⛔ ERRORE HEALTH CHECK: {e}")
        return {
            "messages": [SystemMessage(content=f"⛔ ERRORE HEALTH CHECK: Il server Prometheus non risponde o è irraggiungibile.\nDettagli: {str(e)}")],
            "sanity_check_ok": False
        }

    # --- FASE 2: GET TARGETS (Solo se health passata) ---
    if not target_tool:
        log.critical("Tool 'get_targets' non trovato.")
        return {
            "messages": [SystemMessage(content="ERRORE CRITICO: Tool 'get_targets' non trovato.")],
            "sanity_check_ok": False
        }

    unique_names = set() # Usiamo un set per rimuovere automaticamente i duplicati

    try:
        targets_result = await target_tool.ainvoke({})
        
        # 1. Estrazione della stringa JSON grezza dal risultato del tool
        raw_json_str = ""
        
        # Gestione vari formati di output (ad es. lista di dizionari o oggetti)
        if isinstance(targets_result, list) and len(targets_result) > 0:
            first_item = targets_result[0]
            # Se è un oggetto TextContent o un dizionario
            if hasattr(first_item, "text"):
                raw_json_str = first_item.text
            elif isinstance(first_item, dict) and "text" in first_item:
                raw_json_str = first_item["text"]
            else:
                raw_json_str = str(first_item) # Fallback
        elif hasattr(targets_result, "text"):
            raw_json_str = targets_result.text
        else:
            raw_json_str = str(targets_result)

        # 2. Parsing del JSON
        try:
            data = json.loads(raw_json_str)
            
            # La stringa inizia con '{"activeTargets": ...}'
            active_targets_raw = data.get("activeTargets", [])
            
            for t in active_targets_raw:
                labels = t.get("labels", {})
                # Cerco il nome "umano" (es. worker-1)
                name = labels.get("name")
                
                # Se non c'è "name", provo "instance" (es. IP:Port)
                if not name:
                    name = labels.get("instance")
                
                if name:
                    unique_names.add(name) # Il set rimuove i doppi (mysql, node, ecc.)

        except json.JSONDecodeError:
            log.error(f"Errore parsing JSON interno targets: {raw_json_str[:50]}...")

        # Convertiamo in lista ordinata per coerenza
        targets_list = sorted(list(unique_names))
        log.info(f"✅ Nodi identificati: {targets_list}")
             
    except Exception as e:
        log.error(f"⛔ ERRORE GET TARGETS: {e}")
        return {
            "messages": [SystemMessage(content=f"⛔ ERRORE GET TARGETS: Impossibile recuperare i target attivi.\nDettagli: {str(e)}")],
            "sanity_check_ok": False,
            "active_targets": []
        }

    # 3. CARICAMENTO RISORSA 
    TARGET_URI = "prometheus://qos/config"
    qos_config = {}
    
    try:
        log.info(f"⬇️  Richiesta risorsa specifica: [bold]{TARGET_URI}[/bold]")
        
        resources = await client.get_resources(uris=TARGET_URI)
        
        if resources and len(resources) > 0:
            config_blob = resources[0]
            text_content = config_blob.as_string()
            qos_config = json.loads(text_content)
            
            num_metrics = len(qos_config.get('metrics', {}))
            num_profiles = len(qos_config.get('profiles', {}))
            log.info(f"✅ Configurazione QoS caricata: [bold]{num_metrics}[/bold] metriche, [bold]{num_profiles}[/bold] profili.")
        else:
            log.warning(f"⚠️ Il server ha restituito una lista vuota per l'URI: {TARGET_URI}")
            qos_config = {"metrics": {}, "profiles": {}}

    except Exception as e:
        log.error(f"❌ Errore caricamento Risorse: {e}", exc_info=True)
        qos_config = {"metrics": {}, "profiles": {}}
        return {
            "qos_config": qos_config,
            "messages": [SystemMessage(content=f"ATTENZIONE: Errore caricamento risorse ({e}).")],
            "sanity_check_ok": False 
        }

    # Safety Check
    if not qos_config.get("profiles"):
        log.warning("ATTENZIONE: Configurazione QoS vuota o profili mancanti.")
        msg = "ATTENZIONE: Configurazione QoS vuota. L'agente non funzionerà correttamente."
    else:
        msg = "Sistema pronto. Configurazione caricata tramite resource."

    return {
        "active_targets": targets_list,
        "qos_config": qos_config,
        "sanity_check_ok": True,
        "messages": [SystemMessage(content=msg)]
    }