import logging
from rich.logging import RichHandler
from rich.console import Console
from rich.theme import Theme

# 1. Configurazione della Console (Interfaccia Utente)
# Definiamo un tema per mantenere i colori coerenti in tutto il progetto
custom_theme = Theme({
    "info": "cyan",
    "warning": "yellow",
    "error": "bold red",
    "success": "bold green",
    "agent": "bold magenta",
    "user": "bold green"
})

# Questa console verrà usata ESCLUSIVAMENTE nel main.py per parlare con l'utente
console = Console(theme=custom_theme)

# 2. Configurazione del Logger (Backend/Nodi)
def setup_logger(level="INFO"):
    """
    Configura il logger di sistema per i nodi.
    Usa RichHandler per avere log formattati e colorati automaticamente.
    """
    # Rimuove handler precedenti se esistono (per evitare log doppi in ambienti come notebook)
    logging.getLogger().handlers = []
    
    logging.basicConfig(
        level=level,
        format="%(message)s", # Rich aggiunge già il timestamp, qui mettiamo solo il messaggio
        datefmt="[%X]",
        handlers=[RichHandler(
            console=console, 
            rich_tracebacks=True, # Mostra gli errori in modo espanso e colorato
            markup=True,          # Abilita i tag [bold] ecc. nei messaggi di log
            show_path=False       # Metti True se vuoi vedere in quale file è stato generato il log
        )]
    )
    
    # Crea un logger specifico per la tua tesi
    logger = logging.getLogger("thesis_agent")
    return logger

# Istanza globale del logger da importare nei nodi
log = setup_logger()