from dotenv import load_dotenv
load_dotenv("api_key.env")

import asyncio, json

from src.config import console
from src.graph_agent import build_graph

from langchain.messages import HumanMessage

from rich.panel import Panel
from rich.markdown import Markdown



async def main():
    console.print(Panel.fit(
        "[bold cyan]🤖 SRE Agent: QoS & Capability Planner[/bold cyan]\n[italic]Dynamic Knowledge Base Architecture[/italic]",
        border_style="cyan"
    ))
    
    app = await build_graph()
    
    while True:
        query = await asyncio.to_thread(input, "\n👤 Richiesta (q per uscire): ")
        if query.lower() in ['q', 'quit']: break
        
        initial_state = {
            "messages": [HumanMessage(content=query)],
            "sanity_check_ok": True,
            "profile_results": [] # Inizializziamo la lista per i risultati parziali
        }
        
        console.print("\n[bold grey50]--- Inizio Elaborazione ---[/bold grey50]")
        
        async for output in app.astream(initial_state):
            for node_name, state_update in output.items():
                
                # --- 1. SETUP & CONTEXT ---
                if node_name == "context":
                    if not state_update.get("sanity_check_ok", True):
                        console.print("[bold red]❌ Errore Critico![/bold red]")
                        break
                    if "qos_config" in state_update:
                        console.print("[green]✅ Knowledge Base Caricata.[/green]")

                # --- 2. PIANIFICAZIONE ---
                elif node_name == "planner":
                    intent = state_update.get("intent", "N/A")
                    target = state_update.get("target_filter")
                    tgt_str = f"Target: [bold]{target}[/bold]" if target else "Target: [bold]ALL[/bold]"
                    console.print(Panel(f"Intent: [bold]{intent.upper()}[/bold]\n{tgt_str}", title="🧠 Planner Decision", border_style="yellow", width=40))

                # --- 3. RACCOLTA DATI ---
                elif node_name == "metrics_engine":
                    # Tentiamo di contare i nodi dal report JSON
                    report_json = state_update.get("metrics_report", "{}")
                    try:
                        data_dict = json.loads(report_json)
                        count = len(data_dict)
                    except:
                        count = "?"
                    console.print(f"[cyan]📊 Metriche raccolte per {count} nodi.[/cyan]")

                # --- 4. ESECUZIONE PARALLELA (Map Node) ---
                elif node_name == "single_profile_evaluator":
                    # Qui riceviamo l'update di UNO dei worker
                    # Estraiamo il nome del profilo appena analizzato per dare feedback
                    partial_results = state_update.get("profile_results", [])
                    if partial_results:
                        try:
                            # L'ultimo elemento aggiunto alla lista
                            last_res_json = partial_results[-1] 
                            last_res = json.loads(last_res_json)
                            p_name = last_res.get("profile_name", "Unknown")
                            console.print(f"[dim]   ⚙️  Validato profilo: [bold]{p_name}[/bold][/dim]")
                        except:
                            console.print("[dim]   ⚙️  Validato un profilo...[/dim]")

                # --- 5. RISULTATO FINALE (Reduce Node o Advisor) ---
                elif node_name in ["synthesizer", "allocation_advisor"]:
                    final_msg = state_update["messages"][-1].content
                    
                    if node_name == "synthesizer":
                        title = "📋 Capability Report (Final)"
                        color = "blue"
                    else:
                        title = "🚀 Allocation Advice"
                        color = "green"
                    
                    console.print("\n")
                    console.print(Panel(Markdown(final_msg), title=title, border_style=color))
                    console.print("[bold grey50]--- Fine Elaborazione ---[/bold grey50]")

if __name__ == "__main__":
    asyncio.run(main())