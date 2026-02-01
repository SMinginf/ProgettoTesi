from dotenv import load_dotenv
load_dotenv("api_key.env")

import asyncio
from rich.panel import Panel
from rich.markdown import Markdown
from langchain_core.messages import HumanMessage


# Import interni
from src.graph_agent import build_graph
# Setup del logger e Console UI
from src.logger import console, setup_logger

# Configura il logger globale (Backend)
log = setup_logger()

async def main():
    # 1. Header UI
    console.print(Panel.fit(
        "[bold cyan]🤖 SRE Agent: QoS & Capability Planner[/bold cyan]\n[italic]Neuro-Symbolic Architecture[/italic]",
        border_style="cyan"
    ))

    # 2. Costruzione del grafo
    try:
        app = await build_graph()
        log.info("✅ Grafo LangGraph inizializzato correttamente.")
    except Exception as e:
        log.critical(f"❌ Impossibile avviare il grafo: {e}")
        return
    
    # 3. Loop Principale
    while True:
        try:
            # Input Utente (UI)
            # Usiamo console.input per mantenere lo stile
            query = await asyncio.to_thread(console.input, "\n[bold green]👤 Richiesta (q per uscire): [/bold green]")
            
            if query.lower() in ['q', 'quit', 'exit', 'esci']: 
                console.print("[bold blue]👋 Terminazione sessione. A presto![/bold blue]")
                break
            
            # Stato iniziale
            initial_state = {
                "messages": [HumanMessage(content=query)],
                "sanity_check_ok": True,
                "profile_results": [] 
            }
            
            # Separatore visivo: Da qui iniziano i log tecnici
            console.rule("[bold yellow]Elaborazione Agente[/bold yellow]")
            
            # Esecuzione Grafo (Streaming)     
            async for output in app.astream(initial_state):
                for node_name, state_update in output.items():
                    
                    # Intercettiamo la risposta finale per visualizzarla in un bel pannello UI
                    # (Solitamente arriva dal nodo 'allocation_advisor' o 'synthesizer')
                    if node_name in ["allocation_advisor", "synthesizer"]:
                        
                        # Recuperiamo l'ultimo messaggio generato
                        if "messages" in state_update and state_update["messages"]:
                            final_msg = state_update["messages"][-1].content
                            
                            # Titolo e Colore in base al nodo
                            if node_name == "allocation_advisor":
                                title = "🚀 Allocation Advice"
                                color = "green"
                            else:
                                title = "📋 Capability & Status Report"
                                color = "blue"
                            
                            # Separatore visivo
                            console.rule(f"[bold {color}]Risposta Finale[/bold {color}]")
                            console.print("\n")
                            
                            # Stampa formattata Markdown
                            console.print(Panel(
                                Markdown(final_msg), 
                                title=title, 
                                border_style=color
                            ))
                            
                    # Se in futuro avrai altri nodi finali (es. conversational), gestiscili qui
                    elif node_name == "conversational":
                         # logica simile...
                         pass

            console.print("\n[dim]--- Turno completato ---[/dim]")

        except Exception as e:
            # Gestione errori robusta con stack trace nel logger
            console.rule("[bold red]ERRORE[/bold red]")
            log.error(f"Errore durante l'elaborazione: {e}", exc_info=True)
            console.print("[red]Si è verificato un errore imprevisto. Controlla i log sopra.[/red]")

if __name__ == "__main__":
    asyncio.run(main())