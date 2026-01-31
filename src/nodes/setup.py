import json
from langchain.messages import SystemMessage
from src.state import AgentState
from src.config import client

# --- NODO 1 ---
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
        return {
            "messages": [SystemMessage(content="ERRORE CRITICO: Tool 'health_check' non trovato.")],
            "sanity_check_ok": False
        }
        
    try:
        # Eseguiamo health_check
        health_result = await health_tool.ainvoke({})
        
        # Logica di validazione: controlliamo se il risultato contiene errori o è vuoto
        # Adattare in base a cosa restituisce esattamente il tuo server MCP
        health_str = str(health_result).lower()
        if "error" in health_str or "unhealthy" in health_str or "down" in health_str:
             raise Exception(f"Health Check Fallito: {health_result}")
             
    except Exception as e:
        return {
            "messages": [SystemMessage(content=f"⛔ ERRORE HEALTH CHECK: Il server Prometheus non risponde o è irraggiungibile.\nDettagli: {str(e)}")],
            "sanity_check_ok": False
        }

    # --- FASE 2: GET TARGETS (Solo se health passata) ---
    if not target_tool:
        return {
            "messages": [SystemMessage(content="ERRORE CRITICO: Tool 'get_targets' non trovato.")],
            "sanity_check_ok": False
        }

    try:
        targets_result = await target_tool.ainvoke({})
        
        if isinstance(targets_result, list):
             targets_clean = "\n".join([str(block) for block in targets_result])
        else:
             targets_clean = str(targets_result)
             
    except Exception as e:
        return {
            "messages": [SystemMessage(content=f"⛔ ERRORE GET TARGETS: Impossibile recuperare i target attivi.\nDettagli: {str(e)}")],
            "sanity_check_ok": False
        }

    # 3. CARICAMENTO RISORSA 
    # URI definito nel server.py
    TARGET_URI = "prometheus://qos/config"
    qos_config = {}
    
    try:
        print(f"   ⬇️  Richiesta risorsa specifica: {TARGET_URI}")
        
        # USIAMO LA SPECIFICA CHE HAI TROVATO:
        # Passiamo 'uris' per chiedere solo quella specifica risorsa.
        resources = await client.get_resources(uris=TARGET_URI)
        
        if resources and len(resources) > 0:
            # Se la lista non è vuota, il primo elemento è sicuramente quello richiesto.
            # Non ci importa se resource.source è None, perché abbiamo filtrato a monte.
            config_blob = resources[0]
            
            text_content = config_blob.as_string()
            qos_config = json.loads(text_content)
            print(f"   ✅ Knowledge Base caricata: {len(qos_config.get('metrics', {}))} metriche.")
        else:
            print(f"   ⚠️ Il server ha restituito una lista vuota per l'URI: {TARGET_URI}")
            qos_config = {"metrics": {}, "profiles": {}}

    except Exception as e:
        print(f"   ❌ Errore caricamento Risorse: {e}")
        qos_config = {"metrics": {}, "profiles": {}}
        return {
            "qos_config": qos_config,
            "messages": [SystemMessage(content=f"ATTENZIONE: Errore caricamento risorse ({e}).")],
            "sanity_check_ok": False 
        }

    # Safety Check
    if not qos_config.get("profiles"):
        msg = "ATTENZIONE: Configurazione QoS vuota. L'analisi capacità non funzionerà."
    else:
        msg = "System Ready. Knowledge Loaded via Resources."

    return {
        "active_targets": targets_clean,
        "qos_config": qos_config,
        "sanity_check_ok": True,
        "messages": [SystemMessage(content=msg)]
    }
