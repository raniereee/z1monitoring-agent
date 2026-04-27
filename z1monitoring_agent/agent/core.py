"""
Core do Agente - O loop principal de raciocínio.

O agente recebe uma mensagem, decide se precisa usar ferramentas
ou se pode responder diretamente, e executa até resolver.
"""

import json
import time
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
MODEL_DEEP = "claude-sonnet-4-6"


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
        tool_cache: Optional[dict] = None,
        tool_cache_ttl: int = 1800,
    ):
        """
        Args:
            tools: Lista de ferramentas disponíveis para o agente
            system_prompt: Prompt de sistema que define o comportamento
            context: Contexto adicional (dados do usuário, granja, etc)
            use_deep_model: Se True usa Sonnet (análise profunda), senão Haiku (rápido)
            message_history: Histórico de mensagens anteriores. Cada item é um
                dict com {role, content}. content pode ser string (texto puro)
                OU lista de blocos Anthropic (tool_use/tool_result/text).
            tool_cache: dict pré-existente {key: {"result": ..., "ts": float}}
                pra reutilizar resultados de tool entre turnos. Caller passa
                e salva de volta. Se None, cria vazio (sem persistência).
            tool_cache_ttl: segundos de validade dos entries do cache (default
                1800 = 30 min).
        """
        self.tools = {tool.name: tool for tool in tools}
        self.system_prompt = system_prompt
        self.context = context or {}
        self.model = MODEL_DEEP if use_deep_model else MODEL_FAST
        self.messages = list(message_history) if message_history else []
        self.total_input_tokens = 0
        self.total_output_tokens = 0
        self.api_calls = 0
        self.tool_cache = tool_cache if tool_cache is not None else {}
        self.tool_cache_ttl = tool_cache_ttl
        self.tool_cache_hits = 0
        # Marca onde começa a contribuição deste turno (msgs adicionadas
        # daqui em diante). Caller usa pra saber o que persistir.
        self._messages_start_idx = len(self.messages)

    def _build_system_prompt(self) -> list:
        """
        Monta o prompt de sistema como array de content blocks.
        O prompt base é cacheável (estático), o contexto é dinâmico.
        """
        blocks = [
            {
                "type": "text",
                "text": self.system_prompt,
                "cache_control": {"type": "ephemeral"},
            }
        ]

        if self.context:
            context_text = "\n\n## Contexto do usuário:\n"
            for key, value in self.context.items():
                context_text += f"- {key}: {value}\n"
            blocks.append({"type": "text", "text": context_text})

        return blocks

    def _get_tools_schema(self) -> List[dict]:
        """
        Retorna schema das tools no formato Anthropic.
        A última tool recebe cache_control para cachear todo o bloco de tools.
        """
        tools_list = list(self.tools.values())
        schemas = []
        for i, tool in enumerate(tools_list):
            schema = {
                "name": tool.name,
                "description": tool.description,
                "input_schema": tool.parameters,
            }
            if i == len(tools_list) - 1:
                schema["cache_control"] = {"type": "ephemeral"}
            schemas.append(schema)
        return schemas

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
                max_tokens=2048,
                system=self._build_system_prompt(),
                tools=self._get_tools_schema(),
                messages=self.messages,
            )

            # Acumula tokens
            if hasattr(response, 'usage') and response.usage:
                self.total_input_tokens += response.usage.input_tokens
                self.total_output_tokens += response.usage.output_tokens
                cache_read = getattr(response.usage, 'cache_read_input_tokens', 0) or 0
                cache_creation = getattr(response.usage, 'cache_creation_input_tokens', 0) or 0
                if cache_read > 0 or cache_creation > 0:
                    log.info("💾 Cache", read=cache_read, creation=cache_creation)
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

                        # Cache lookup: chave = nome + inputs canônicos.
                        # Bypass pra tools com efeito colateral (executar
                        # ajuste, registrar visita, enviar mensagem).
                        cache_key = self._tool_cache_key(tool_name, tool_input)
                        cached = self._tool_cache_get(cache_key)
                        if cached is not None:
                            log.info("🧠 Tool [%s] cache hit", tool_name)
                            result = cached
                            self.tool_cache_hits += 1
                        elif tool_name in self.tools:
                            result = self.tools[tool_name].run(**tool_input)
                            self._tool_cache_put(cache_key, result, tool_name)
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

            elif response.stop_reason == "max_tokens":
                # Resposta cortada por limite de tokens — enviar o que tem
                text_response = self._extract_text(response)
                if text_response:
                    log.info("🤖 Agent resposta truncada (max_tokens)", resposta=text_response[:100])
                    return text_response
                log.warning("🤖 Agent max_tokens sem texto")
                return "Desculpe, a resposta ficou muito longa. Tente ser mais específico."

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

    # -----------------------------------------------------------------------
    # Tool result cache
    # -----------------------------------------------------------------------
    # Tools que NÃO devem ser cacheadas (efeito colateral em execução).
    _TOOL_CACHE_BYPASS = frozenset({
        "confirmar_ajuste_parametro",
        "enviar_botoes_confirmacao",
        "notificar_usuario",
        "registrar_visita",
    })

    @staticmethod
    def _tool_cache_key(tool_name: str, tool_input: dict) -> str:
        try:
            args = json.dumps(tool_input or {}, sort_keys=True, ensure_ascii=False)
        except Exception:
            args = str(tool_input)
        return f"{tool_name}::{args}"

    def _tool_cache_get(self, key: str):
        entry = self.tool_cache.get(key)
        if not entry:
            return None
        ts = entry.get("ts", 0)
        if (time.time() - ts) > self.tool_cache_ttl:
            self.tool_cache.pop(key, None)
            return None
        return entry.get("result")

    def _tool_cache_put(self, key: str, result, tool_name: str):
        if tool_name in self._TOOL_CACHE_BYPASS:
            return
        self.tool_cache[key] = {"result": result, "ts": time.time()}

    def get_new_messages(self) -> list:
        """Mensagens adicionadas neste turno (incluindo tool_use/tool_result).
        Útil pra caller persistir histórico estruturado."""
        return self.messages[self._messages_start_idx:]
