import json
from langchain_core.messages import SystemMessage
from src.state import AgentState
from src.config import client
# IMPORTA IL LOGGER CENTRALIZZATO
from src.logger import log

# --- NODO 1: SETUP E CONTESTO ---
import json
import asyncio
from rich.panel import Panel
from rich.console import Console # <--- 1. Import
from langchain_core.messages import SystemMessage

# Import interni
from src.state import AgentState
from src.config import client
from src.logger import log

# Inizializziamo la console
console = Console()

async def context_manager_node(state: AgentState):
    """ 
    1. Verifica salute Prometheus.
    2. Recupera i target.
    3. SCARICA la configurazione QoS (Resource) dal server.
    """    
    
    # Header Visuale
    console.print(Panel("🔌 System Context Setup", style="blue"))
    log.info("Avvio Context Manager: Health Check, Targets, Resources.")

    # 1. Recuperiamo tutti i tool disponibili
    try:
        tools = await client.get_tools()
    except Exception as e:
        log.critical(f"Errore connessione MCP Tools: {e}")
        return {
            "messages": [SystemMessage(content=f"Errore critico MCP: {e}")],
            "sanity_check_ok": False
        }

    # Recupero tool specifici
    health_tool = next((t for t in tools if t.name == "health_check"), None)
    target_tool = next((t for t in tools if t.name == "get_targets"), None)
    
    # --- FASE 1: HEALTH CHECK ---
    if not health_tool:
        msg = "Tool 'health_check' non trovato su MCP Server."
        console.print(f"❌ {msg}", style="bold red")
        log.critical(msg)
        return {
            "messages": [SystemMessage(content=f"ERRORE CRITICO: {msg}")],
            "sanity_check_ok": False
        }
        
    try:
        # Eseguiamo health_check
        console.print("Diagnostica: Controllo stato Prometheus...", style="dim")
        health_result = await health_tool.ainvoke({})
        
        health_str = str(health_result).lower()
        if "error" in health_str or "unhealthy" in health_str or "down" in health_str:
             console.print(f"❌ Health Check Fallito: {health_result}", style="bold red")
             log.error(f"Health Check Fallito: {health_result}")
             raise Exception(f"Health Check Fallito: {health_result}")
        else:
             console.print("✅ Prometheus Health Check: OK", style="green")
             log.info("Prometheus Health Check: OK")
             
    except Exception as e:
        log.error(f"ERRORE HEALTH CHECK: {e}")
        return {
            "messages": [SystemMessage(content=f"⛔ ERRORE HEALTH CHECK: {str(e)}")],
            "sanity_check_ok": False
        }

    # --- FASE 2: GET TARGETS (Solo se health passata) ---
    if not target_tool:
        msg = "Tool 'get_targets' non trovato."
        console.print(f"❌ {msg}", style="bold red")
        log.critical(msg)
        return {
            "messages": [SystemMessage(content=f"ERRORE CRITICO: {msg}")],
            "sanity_check_ok": False
        }

    unique_names = set() 

    try:
        console.print("Diagnostica: Scansione nodi attivi...", style="dim")
        targets_result = await target_tool.ainvoke({})
        
        # 1. Estrazione JSON (Logica robusta per vari formati MCP)
        raw_json_str = ""
        
        if isinstance(targets_result, list) and len(targets_result) > 0:
            first_item = targets_result[0]
            if hasattr(first_item, "text"):
                raw_json_str = first_item.text
            elif isinstance(first_item, dict) and "text" in first_item:
                raw_json_str = first_item["text"]
            else:
                raw_json_str = str(first_item)
        elif hasattr(targets_result, "text"):
            raw_json_str = targets_result.text
        else:
            raw_json_str = str(targets_result)

        # 2. Parsing
        try:
            data = json.loads(raw_json_str)
            active_targets_raw = data.get("activeTargets", [])
            
            for t in active_targets_raw:
                labels = t.get("labels", {})
                name = labels.get("name")
                if not name:
                    name = labels.get("instance")
                if name:
                    unique_names.add(name) 

        except json.JSONDecodeError:
            log.error(f"Errore parsing JSON targets: {raw_json_str[:50]}...")

        targets_list = sorted(list(unique_names))
        
        if targets_list:
            console.print(f"✅ Nodi identificati: [bold cyan]{targets_list}[/bold cyan]")
            log.info(f"Nodi identificati: {targets_list}")
        else:
            console.print("⚠️ Nessun nodo attivo trovato.", style="yellow")
            log.warning("Lista nodi vuota.")
             
    except Exception as e:
        log.error(f"ERRORE GET TARGETS: {e}")
        return {
            "messages": [SystemMessage(content=f"⛔ ERRORE GET TARGETS: {str(e)}")],
            "sanity_check_ok": False,
            "active_targets": []
        }

    # --- FASE 3: CARICAMENTO RISORSA ---
    TARGET_URI = "prometheus://qos/config"
    qos_config = {}
    
    try:
        console.print(f"Diagnostica: Download Config QoS ([dim]{TARGET_URI}[/dim])...", style="dim")
        log.info(f"Richiesta resource: {TARGET_URI}")
        
        resources = await client.get_resources(uris=TARGET_URI)
        
        if resources and len(resources) > 0:
            config_blob = resources[0]
            
            # MCP Python SDK: resource.text or resource.blob depending on implementation
            # Assumiamo text per file di configurazione
            if hasattr(config_blob, "text") and config_blob.text:
                text_content = config_blob.text
            elif hasattr(config_blob, "content") and config_blob.content:
                 # Se fosse base64 o bytes, servirebbe decode. Assumiamo stringa.
                 text_content = config_blob.content
            else:
                 # Fallback: prova a chiamare il metodo del tuo SDK se diverso
                 # Nel tuo codice originale usavi .as_string(), mantengo quello se è del tuo SDK custom
                 try:
                    text_content = config_blob.as_string()
                 except:
                    text_content = str(config_blob)

            qos_config = json.loads(text_content)
            
            num_metrics = len(qos_config.get('metrics', {}))
            num_profiles = len(qos_config.get('profiles', {}))
            
            console.print(f"✅ Configurazione QoS caricata: [bold]{num_metrics}[/bold] metriche, [bold]{num_profiles}[/bold] profili.", style="green")
            log.info(f"Config QoS caricata: {num_metrics} metriche, {num_profiles} profili.")
        else:
            console.print(f"⚠️ Resource vuota per URI: {TARGET_URI}", style="yellow")
            log.warning(f"Resource list vuota per URI: {TARGET_URI}")
            qos_config = {"metrics": {}, "profiles": {}}

    except Exception as e:
        console.print(f"❌ Errore caricamento Risorse: {e}", style="bold red")
        log.error(f"Errore caricamento Risorse: {e}", exc_info=True)
        return {
            "qos_config": {"metrics": {}, "profiles": {}},
            "messages": [SystemMessage(content=f"ATTENZIONE: Errore caricamento risorse ({e}).")],
            "sanity_check_ok": False 
        }

    # Safety Check
    if not qos_config.get("profiles"):
        console.print("⚠️ ATTENZIONE: Configurazione QoS incompleta.", style="bold yellow")
        log.warning("Configurazione QoS vuota o profili mancanti.")
        msg = "ATTENZIONE: Configurazione QoS vuota. L'agente non funzionerà correttamente."
    else:
        msg = "Sistema pronto. Configurazione caricata tramite resource."

    return {
        "active_targets": targets_list,
        "qos_config": qos_config,
        "sanity_check_ok": True,
        "messages": [SystemMessage(content=msg)]
    }