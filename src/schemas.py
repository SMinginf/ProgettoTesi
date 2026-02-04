from typing import List, Dict, Literal, Optional, TypedDict
from pydantic import BaseModel, Field

# --- SCHEMI DI OUTPUT STRUTTURATO ---

class ProfileEvaluation(BaseModel):
    """Valutazione di un singolo profilo QoS su tutto il cluster."""
    profile_name: str = Field(description="Nome del profilo (es. cpu-bound)")
    requirements: List[dict] = Field(description="Lista dei requisiti/soglie dal JSON")

    metric_analysis: List[str] = Field(description="Lista di report dettagliati. Per ogni metrica del profilo, crea una stringa che elenca i valori esatti misurati su tutti i nodi analizzati.")

    conclusion: str = Field(description="Il blocco 'Risultato': chi è idoneo e perché.")
    
    # Ci serve una lista esplicita per costruire la tabella riassuntiva finale
    suitable_nodes: List[str] = Field(description="Lista esatta dei nomi dei nodi analizzati risultati IDONEI")

class CapabilityReport(BaseModel):
    """Il report finale."""
    # Lista di tutti i nodi analizzati (serve per disegnare le righe della tabella)
    all_nodes_scanned: List[str] = Field(description="Lista di tutti i nodi analizzati")
    profiles: List[ProfileEvaluation] = Field(description="Lista delle valutazioni per profilo")
    final_synthesis: str = Field(description="Sintesi finale. Quali nodi sono idonei per quali profili, raccomandazioni generali.")


class UserRequestClassification(BaseModel):
    intent: Literal["allocation", "status"]            
    target_filter: Optional[str] = Field(
        default=None, 
        description="Il nome del server specifico se menzionato, altrimenti None."
    )


# class UserRequestClassification(TypedDict):
#     intent: Literal["allocation", "status"]            # "allocation" oppure "status"
#     target_filter: None | str     # None (tutti) oppure "server-lpha" (singolo server)


class SingleProfileCheck(BaseModel):
    """Output atomico per un singolo profilo."""
    profile_name: str
    #analysis_lines: List[str] = Field(description="Es: 'worker-1: nome_metrica = 0.044 < 5.0 (PASS)'")
    analysis_lines: Dict[str, List[str]] = Field(description="La chiave del dizionario è il nome del nodo, il valore è una lista di stringhe nel formato 'nome_metrica = valore < soglia (PASS/FAIL)'")
    qualified_nodes: List[str]

class UserConstraint(BaseModel):
    """Un vincolo esplicito espresso dall'utente."""
    metric_name: str = Field(description="Il nome della metrica tecnica (es. ram_available_bytes)")
    operator: str = Field(description="Operatore matematico: >, <, >=, <=")
    value: float = Field(description="Il valore numerico target (convertito nell'unità della metrica)")
    original_text: str = Field(description="Il testo originale dell'utente (es. 'almeno 8GB')")

class RequirementExtraction(BaseModel):
    """Output del Nodo 2: Estrazione vincoli."""
    constraints: List[Optional[UserConstraint]] = Field(default=[], description="Lista dei vincoli espliciti estratti dall'input utente.")

class TaskProfileIntent(BaseModel):
    """
    Classificazione del task utente basata sui profili QoS disponibili.
    Supporta multi-label (es. un task può essere sia cpu-bound che memory-bound).
    """
    selected_profiles: List[str] = Field(
        description="Lista dei nomi dei profili (es. ['cpu-bound', 'disk-bound']). Seleziona TUTTI quelli rilevanti."
    )

    reasoning: str = Field(
        description="Breve spiegazione tecnica del perché questi profili si applicano al task descritto."
    )
