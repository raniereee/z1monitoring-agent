"""
Core do Agente - O loop principal de raciocínio.

O agente recebe uma mensagem, decide se precisa usar ferramentas
ou se pode responder diretamente, e executa até resolver.
"""

import json
import anthropic
import structlog
from typing import List, Optional
from .tools import Tool

log = structlog.get_logger()

# Cliente Anthropic
client = anthropic.Anthropic()

# Configurações
MAX_ITERATIONS = 7  # Evita loops infinitos e gasto desnecessário de tokens
MODEL_FAST = "claude-haiku-4-5-20251001"
MODEL_DEEP = "claude-sonnet-4-20250514"


class Agent:
    """
    Agente conversacional com capacidade de usar ferramentas.

    O agente segue o loop:
    1. Recebe mensagem do usuário
    2. Envia para LLM com contexto e ferramentas disponíveis
    3. Se LLM pedir tool: executa e volta pro passo 2
    4. Se LLM responder: retorna a resposta
    """

    def __init__(
        self,
        tools: List[Tool],
        system_prompt: str,
        context: Optional[dict] = None,
        use_deep_model: bool = False,
        message_history: Optional[List[dict]] = None,
    ):
        """
        Args:
            tools: Lista de ferramentas disponíveis para o agente
            system_prompt: Prompt de sistema que define o comportamento
            context: Contexto adicional (dados do usuário, granja, etc)
            use_deep_model: Se True usa Sonnet (análise profunda), senão Haiku (rápido)
            message_history: Histórico de mensagens anteriores [{"role": "user"|"assistant", "content": "..."}]
        """
        self.tools = {tool.name: tool for tool in tools}
        self.system_prompt = system_prompt
        self.context = context or {}
        self.model = MODEL_DEEP if use_deep_model else MODEL_FAST
        self.messages = list(message_history) if message_history else []
        self.total_input_tokens = 0
        self.total_output_tokens = 0
        self.api_calls = 0

    def _build_system_prompt(self) -> str:
        """Monta o prompt de sistema com contexto."""
        prompt = self.system_prompt

        if self.context:
            prompt += "\n\n## Contexto do usuário:\n"
            for key, value in self.context.items():
                prompt += f"- {key}: {value}\n"

        return prompt

    def _get_tools_schema(self) -> List[dict]:
        """Retorna schema das tools no formato Anthropic."""
        return [
            {
                "name": tool.name,
                "description": tool.description,
                "input_schema": tool.parameters,
            }
            for tool in self.tools.values()
        ]

    def run(self, user_message: str, image_base64: Optional[str] = None, image_media_type: str = "image/jpeg") -> str:
        """
        Executa o agente com a mensagem do usuário.

        Args:
            user_message: Mensagem enviada pelo usuário
            image_base64: Imagem em base64 (opcional, para análise visual)
            image_media_type: MIME type da imagem

        Returns:
            Resposta final do agente (texto)
        """
        # Monta content da mensagem (texto ou multimodal)
        if image_base64:
            content = [
                {
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": image_media_type,
                        "data": image_base64,
                    },
                },
                {
                    "type": "text",
                    "text": user_message or "Analise esta imagem.",
                },
            ]
        else:
            content = user_message

        self.messages.append(
            {
                "role": "user",
                "content": content,
            }
        )

        log.info("🤖 Agent iniciando", mensagem=(user_message or "")[:100], has_image=bool(image_base64))

        for iteration in range(MAX_ITERATIONS):
            log.info(f"🤖 Agent iteração {iteration + 1}/{MAX_ITERATIONS}")

            # Chama o LLM
            response = client.messages.create(
                model=self.model,
                max_tokens=1024,
                system=self._build_system_prompt(),
                tools=self._get_tools_schema(),
                messages=self.messages,
            )

            # Acumula tokens
            if hasattr(response, 'usage') and response.usage:
                self.total_input_tokens += response.usage.input_tokens
                self.total_output_tokens += response.usage.output_tokens
            self.api_calls += 1

            log.info("🤖 Agent resposta", stop_reason=response.stop_reason)

            # Processa a resposta
            if response.stop_reason == "end_turn":
                # LLM decidiu responder diretamente
                text_response = self._extract_text(response)
                self.messages.append(
                    {
                        "role": "assistant",
                        "content": response.content,
                    }
                )
                log.info("🤖 Agent finalizou", resposta=text_response[:100])
                return text_response

            elif response.stop_reason == "tool_use":
                # LLM quer usar uma ferramenta
                self.messages.append(
                    {
                        "role": "assistant",
                        "content": response.content,
                    }
                )

                # Executa todas as tools solicitadas
                tool_results = []
                for block in response.content:
                    if block.type == "tool_use":
                        tool_name = block.name
                        tool_input = block.input
                        tool_use_id = block.id

                        log.info("🤖 Agent chamando tool", tool=tool_name, input=tool_input)

                        # Executa a tool
                        if tool_name in self.tools:
                            result = self.tools[tool_name].run(**tool_input)
                        else:
                            result = {"error": f"Tool '{tool_name}' não encontrada"}

                        tool_results.append(
                            {
                                "type": "tool_result",
                                "tool_use_id": tool_use_id,
                                "content": json.dumps(result, ensure_ascii=False),
                            }
                        )

                # Adiciona resultados ao histórico
                self.messages.append(
                    {
                        "role": "user",
                        "content": tool_results,
                    }
                )

            else:
                # Resposta inesperada
                log.warning(f"🤖 Agent stop_reason inesperado: {response.stop_reason}")
                return "Desculpe, ocorreu um erro. Tente novamente."

        # Atingiu limite de iterações
        log.warning("🤖 Agent atingiu limite de iterações")
        return "Desculpe, não consegui processar sua solicitação. Tente ser mais específico."

    def _extract_text(self, response) -> str:
        """Extrai texto da resposta do LLM."""
        for block in response.content:
            if hasattr(block, "text"):
                return block.text
        return ""

    def add_context(self, key: str, value: any):
        """Adiciona informação ao contexto do agente."""
        self.context[key] = value

    def clear_history(self):
        """Limpa o histórico de mensagens."""
        self.messages = []
