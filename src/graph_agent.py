from langgraph.graph import StateGraph, END
from langgraph.types import Send

from .state import AgentState
from .nodes import setup, retrieval, analysis, decision, reporting


def map_profiles(state: AgentState):
    """
    Mappa i profili QoS in nodi di valutazione.
    OTTIMIZZAZIONE (Early Binding): 
    Se abbiamo già identificato i profili target (es. cpu-bound), valutiamo SOLO quelli.
    Se l'intento è generico ("status") o non chiaro, valutiamo TUTTO (Fallback).
    """

    config = state.get("qos_config", {})
    raw_profiles = config.get("profiles", [])
    metrics = state.get("metrics_report")
    
    # 1. Recupero contesto decisionale (già popolato dall'intent_classifier a monte)
    target_profiles = state.get("target_profiles", [])
    intent = state.get("intent", "status") 

    # 2. Normalizzazione Profili
    all_profiles_list = []
    if isinstance(raw_profiles, dict):
        for name, data in raw_profiles.items():
            if isinstance(data, dict):
                enriched_profile = data.copy()
                if "profile_name" not in enriched_profile:
                    enriched_profile["profile_name"] = name
                all_profiles_list.append(enriched_profile)
    elif isinstance(raw_profiles, list):
        all_profiles_list = raw_profiles
    
    # --- FILTRO ARCHITETTURALE ---
    profiles_to_scan = []
    
    # Se siamo in allocazione e abbiamo capito cosa vuole l'utente, filtriamo.
    if intent == "allocation" and target_profiles:
        profiles_to_scan = [p for p in all_profiles_list if p['profile_name'] in target_profiles]
        # Fallback di sicurezza: se il filtro svuota tutto (es. nome profilo errato), scansiona tutto
        if not profiles_to_scan:
            profiles_to_scan = all_profiles_list
    else:
        # Status Report o Intento generico: Scansione completa.
        profiles_to_scan = all_profiles_list

    # Debug
    # print(f"DEBUG: Dispatching evaluation for {len(profiles_to_scan)} profiles (Target: {target_profiles})")

    return [
        Send("single_profile_evaluator", {
            "profile": p, 
            "metrics": metrics,
            "target_filter": state.get("target_filter")
        }) 
        for p in profiles_to_scan
    ]

# 1. Definiamo la funzione di routing iniziale
def route_initial_intent(state):
    """
    Decide se avviare il motore tecnico o rimanere in chat.
    """
    intent = state.get("intent", "chat")
    
    if intent == "chat":
        return "conversational" # Bypassiamo tutto il setup tecnico
    else:
        return "context"   # Avviamo il setup (health check, config load)


def route_after_metrics(state: AgentState):
    """
    Decide il percorso dopo aver scaricato le metriche.
    - Se l'intento è 'allocation', passiamo all'Intent Classifier per affinare il target.
    - Se l'intento è 'status', saltiamo il classifier e lanciamo subito il Map-Reduce su tutto.
    """
    intent = state.get("intent", "status")
    
    if intent == "allocation":
        # Andiamo al nodo di classificazione tecnica
        return "intent_classifier"
    else:
        # SHORTCUT: Saltiamo il classifier e avviamo direttamente i worker (Map-Reduce)
        # Nota: Qui chiamiamo direttamente la funzione map_profiles per generare i Send()
        return map_profiles(state)


# --- ROUTING ---
def route_after_evaluation(state):
        match state["intent"]:
            case "allocation":
                return ["constraint_extractor"]
            case "status":
                return ["synthesizer"]
            case _:
                return END

        


# --- GRAFO ---
async def build_graph():
    workflow = StateGraph(AgentState)
    
    # --- 1. REGISTRAZIONE NODI ---
    workflow.add_node("context", setup.context_manager_node)
    workflow.add_node("classifier", decision.classify_intent_node) 
    workflow.add_node("metrics_engine", retrieval.metrics_engine_node)
    workflow.add_node("intent_classifier", decision.intent_classifier_node) 

    # NUOVO: Registriamo il nodo chat
    #workflow.add_node("conversational", decision.conversational_node)
    
    # Nodi Ramo Status
    workflow.add_node("single_profile_evaluator", analysis.single_profile_evaluator_node)
    workflow.add_node("synthesizer", reporting.report_synthesizer_node)
    
    # Nodi Ramo Allocation
    workflow.add_node("constraint_extractor", decision.constraint_extractor_node)
    workflow.add_node("candidate_filter", decision.candidate_filter_node)
    workflow.add_node("stability_analyzer", analysis.stability_analyzer_node)
    workflow.add_node("allocation_advisor", decision.allocation_advisor_node)

    # --- 2. DEFINIZIONE ARCHI ---
    
    # Setup Iniziale

    workflow.set_entry_point("context")
    workflow.add_edge("context", "classifier")
    workflow.add_edge("classifier", "metrics_engine")

    #workflow.set_entry_point("classifier")
    # workflow.add_conditional_edges(
    #     "classifier",
    #     route_initial_intent,
    #     {
    #         "conversational": "conversational", # Via veloce
    #         "context": "context"      # Via tecnica
    #     }
    # )

    #workflow.add_edge("context", "metrics_engine")

    # Chiudi il ramo chat
    #workflow.add_edge("conversational", END)
    
    # --- BIVIO STRATEGICO (La correzione) ---
    # Dopo le metriche, controlliamo l'intento.
    # Possiamo andare al nodo "intent_classifier" OPPURE direttamente ai nodi "single_profile_evaluator" (via Send)
    workflow.add_conditional_edges(
        "metrics_engine",
        route_after_metrics, 
        ["intent_classifier", "single_profile_evaluator"]
    )
    
    # --- RAMO ALLOCATION (Step intermedio) ---
    # Se siamo passati dall'intent_classifier, ORA lanciamo il Map-Reduce (filtrato)
    workflow.add_conditional_edges(
        "intent_classifier",
        map_profiles, 
        ["single_profile_evaluator"]
    )
    
    # --- POST-VALUTAZIONE (Convergenza) ---
    workflow.add_conditional_edges(
        "single_profile_evaluator",
        route_after_evaluation,
        {
            "constraint_extractor": "constraint_extractor",
            "synthesizer": "synthesizer"
        }
    )
    
    # --- PIPELINE ALLOCATION SEQUENZIALE ---
    workflow.add_edge("constraint_extractor", "candidate_filter")
    workflow.add_edge("candidate_filter", "stability_analyzer")
    workflow.add_edge("stability_analyzer", "allocation_advisor")

    # --- CHIUSURA ---
    workflow.add_edge("synthesizer", END)
    workflow.add_edge("allocation_advisor", END)
    
    return workflow.compile()    
