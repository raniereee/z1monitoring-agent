"""
Definição base de ferramentas (tools) que o agente pode usar.

Cada tool é uma função que o agente pode chamar para executar ações
ou buscar informações.
"""

from dataclasses import dataclass
from typing import Callable, Any
import structlog

log = structlog.get_logger()


@dataclass
class Tool:
    """
    Uma ferramenta que o agente pode usar.

    Attributes:
        name: Nome único da ferramenta (usado pelo LLM para chamar)
        description: Descrição do que a ferramenta faz (usado no prompt)
        parameters: Schema dos parâmetros esperados
        function: A função Python que executa a ferramenta
    """

    name: str
    description: str
    parameters: dict
    function: Callable[..., Any]

    def run(self, **kwargs) -> Any:
        """Executa a ferramenta com os parâmetros fornecidos."""
        log.info(f"🔧 Tool [{self.name}] executando", params=kwargs)
        try:
            result = self.function(**kwargs)
            log.info(f"🔧 Tool [{self.name}] resultado", result=result)
            return result
        except Exception as e:
            log.error(f"🔧 Tool [{self.name}] erro", error=str(e))
            return {"error": str(e)}

    def to_openai_schema(self) -> dict:
        """Retorna o schema no formato OpenAI/Anthropic function calling."""
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.parameters,
            },
        }
