from typing import List, Any, Literal, TypedDict
from typing_extensions import Annotated
import operator
from langchain.messages import AnyMessage



# --- DEFINIZIONE DELLO STATO DELL'AGENTE ---

class AgentState(TypedDict):
    # The Annotated type with operator.add ensures that new messages are appended to the existing list rather than replacing it.
    messages: Annotated[list[AnyMessage], operator.add] # Storia della chat

    #active_targets: str        # Lista dei target attivi (es. "node-exporter, mysql")
    #retry_count: int           # Contatore per i tentativi di correzione
    #last_error: str            # Ultimo errore riscontrato (per il Refiner)
    sanity_check_ok: bool

    # Dati strutturati raccolti
    metrics_report: str                                # Il risultato delle query in formato JSON strutturato
    intent: Literal["allocation", "status"]            

    qos_config: dict            # <--- Qui salviamo il JSON scaricato dal Server MCP
    target_filter: None | str        # None (tutti) oppure "server-lpha" (singolo server)

    # NUOVO: Accumulatore per i risultati dei singoli profili
    # Annotated[list, operator.add] significa che ogni worker aggiunge il suo risultato alla lista
    profile_results: Annotated[List[str], operator.add]

    # NUOVO: Lista dei profili target identificati per il task descritto dall'utente (es. ["cpu-bound", "memory-bound"])
    target_profiles: List[str] 
    explicit_constraints: List[dict]   # Lista dei vincoli espliciti estratti dall'input utente
    
    # NUOVO: Spiegazione del perché sono stati scelti quei profili di carico (utile per il debug o per l'utente)
    classification_reason: str

    # NUOVO: I nodi che hanno superato TUTTI i filtri (Profili + Utente)
    final_candidates: List[str]

    # NUOVO: Report statistico sulla stabilità dei nodi candidati
    stability_report: dict
