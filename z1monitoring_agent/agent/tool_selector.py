"""
Seleção dinâmica de tools baseada na intent do usuário.

Classifica a mensagem + histórico e retorna só as tools relevantes,
reduzindo tokens de input em ~70%.
"""

import os
import structlog
from anthropic import Anthropic

log = structlog.get_logger()

client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

# Tools que sempre são enviadas (core set)
CORE_TOOLS = [
    "buscar_granja",
    "mostrar_menu_principal",
    "mostrar_ajuda",
    "suporte",
    "resetar_conversa",
]

# Mapeamento categoria → tools específicas
TOOL_SETS = {
    "consulta_status": [
        "consultar_status",
        "status_equipamento",
        "consultar_quadros_com_problema",
        "ranking_offline",
        "consultar_periodos_offline",
        "saude_empresa",
        "validar_flx_vs_ccd",
        "descrever_eta",
    ],
    "tempo_real": [
        "tempo_real",
        "analise",
        "panorama_24h",
        "descrever_eta",
    ],
    "ajuste_parametros": [
        "ajustar_faixa",
        "controlar_dosadora",
        "controlar_abs",
        "definir_limite_24h",
        "ajustar_oz1",
        "enviar_botoes_confirmacao",
        "confirmar_ajuste_parametro",
    ],
    "controle_equipamento": [
        "controlar_saida",
        "controlar_alarme_galpao",
        "controlar_lote",
    ],
    "relatorios": [
        "consumo",
        "analise_consumo_detalhada",
        "descrever_eta",
        "validar_flx_vs_ccd",
        "relatorio_gas",
        "ranking_granjas",
        "panorama_24h",
        "saude_empresa",
    ],
    "granjas_clientes": [
        "listar_granjas_usuario",
        "listar_clientes_primarios",
        "buscar_cliente_primario",
        "listar_granjas_cliente_primario",
        "consultar_falta_gas_cliente_primario",
        "panorama_24h",
        "saude_empresa",
    ],
    "outros": [
        "registrar_visita",
        "dimensionar_eta",
        "notificar_usuario",
    ],
}

# Categorias que exigem raciocínio analítico (usam Sonnet)
DEEP_CATEGORIES = {"relatorios"}

CLASSIFY_PROMPT = """Classifique a intenção do usuário nas categorias abaixo.
O usuário pode pedir MAIS DE UMA coisa na mesma mensagem. Identifique TODAS as categorias necessárias.
Use o histórico da conversa para entender o contexto.

CATEGORIAS:
- consulta_status: alarmes, equipamentos offline/online, falta de insumo, falta de gás, sensor fora da faixa, problemas
- tempo_real: ver dados em tempo real, status de uma granja, análise de água, panorama 24h
- ajuste_parametros: alterar pH, ORP, dosadora, ABS, limite 24h, ozônio, modo cíclico/automático
- controle_equipamento: ligar/desligar bomba, válvula, alarme de galpão, controle de lote
- relatorios: gráfico de consumo, relatório de gás, ranking de granjas, apanhado/panorama/análise geral de várias granjas
- granjas_clientes: listar granjas, buscar cliente, informações de clientes primários, panorama de TODAS as granjas de um cliente primário (ex: "BIOTER", "Copacol", "Avioeste")
- outros: registrar visita, dimensionar ETA, suporte, ajuda, menu

Responda APENAS com os nomes das categorias separados por vírgula, sem explicação.
Exemplos: "relatorios" ou "relatorios,consulta_status" ou "tempo_real,ajuste_parametros"."""


def classify_intent(message: str, history: list = None) -> str:
    """
    Classifica a intent do usuário usando histórico.

    Args:
        message: Mensagem atual do usuário
        history: Lista de mensagens recentes [{"role": "user/assistant", "content": "..."}]

    Returns:
        Nome da categoria
    """
    try:
        messages_ctx = ""
        if history:
            for msg in history[-5:]:
                role = "Usuário" if msg.get("role") == "user" else "Bot"
                messages_ctx += f"{role}: {msg.get('content', '')}\n"

        user_msg = f"""Histórico recente:
{messages_ctx if messages_ctx else '(sem histórico)'}

Mensagem atual do usuário: {message}"""

        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=20,
            system=CLASSIFY_PROMPT,
            messages=[{"role": "user", "content": user_msg}],
        )

        raw = response.content[0].text.strip().lower()
        categories = [c.strip() for c in raw.split(",") if c.strip()]

        # Custo da classificação
        input_tokens = response.usage.input_tokens if response.usage else 0
        output_tokens = response.usage.output_tokens if response.usage else 0

        # Filtra apenas categorias válidas
        valid = [c for c in categories if c in TOOL_SETS]

        log.info(
            "🏷️ Intent classificada",
            categories=valid,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
        )

        if not valid:
            log.warning(f"Categorias desconhecidas: {categories}, usando todas")
            return None

        return valid

    except Exception as e:
        log.error("Erro ao classificar intent", error=str(e))
        return None


def select_tools(all_tools: list, categories: list = None) -> list:
    """
    Filtra tools baseado nas categorias.

    Args:
        all_tools: Lista completa de tools
        categories: Lista de categorias classificadas (None = todas)

    Returns:
        Lista filtrada de tools
    """
    if not categories:
        return all_tools

    allowed_names = set(CORE_TOOLS)
    for cat in categories:
        allowed_names.update(TOOL_SETS.get(cat, []))

    selected = [t for t in all_tools if t.name in allowed_names]

    log.info(
        "🔧 Tools selecionadas",
        categories=categories,
        total=len(all_tools),
        selected=len(selected),
        names=[t.name for t in selected],
    )

    return selected


def needs_deep_model(categories: list = None) -> bool:
    """Retorna True se alguma categoria exige modelo analítico (Sonnet)."""
    if not categories:
        return True  # Sem classificação = fallback seguro com Sonnet
    return bool(set(categories) & DEEP_CATEGORIES)
