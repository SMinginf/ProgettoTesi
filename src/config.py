import os
from langchain_groq import ChatGroq
from langchain_mcp_adapters.client import MultiServerMCPClient
from rich.console import Console


console = Console()


llm = ChatGroq(model="llama-3.3-70b-versatile",temperature=0.1, max_tokens=4096, api_key=os.getenv("GROQ_API_KEY"))

MCP_SERVER_PATH = "C:\\Users\\signo\\Desktop\\Università\\Tesi\\prometheus-mcp-server-main\\src\\prometheus_mcp_server\\main.py"
client = MultiServerMCPClient(
    {
        "mcp-prometheus": {
            "command": "python",
            "args": [MCP_SERVER_PATH],
            "transport": "stdio",
        }
    }
)

