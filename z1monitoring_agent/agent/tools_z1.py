"""
Ferramentas do Agente Z1 Monitoramento.

Conjunto completo de ferramentas que replicam todas as funcionalidades
do sistema de steps atual, organizadas por categoria:

1. STATUS E CONSULTAS - Equipamentos online/offline, falta de insumos
2. TEMPO REAL - Leituras de sensores em tempo real
3. ANÁLISES - Análise de água, gás, ozônio, ETA
4. RELATÓRIOS - Consumo, abastecimento, ranking
5. GRÁFICOS - Geração de gráficos de consumo
6. CONTROLE - Ajuste de parâmetros, ligar/desligar
7. ALARMES - Habilitar/desabilitar alarmes de galpão
8. SUPORTE - Guias e solicitação de suporte
9. NAVEGAÇÃO - Menu, ajuda, panorama
"""

import os
import structlog
from datetime import datetime, timedelta
from typing import Optional
from .tools import Tool

# Models
from z1monitoring_models.models.urgent_alarm import UrgentAlarm
from z1monitoring_models.models.farm import Farm
from z1monitoring_models.models.plates import Plate
from z1monitoring_models.models.changes_requests import ChangesRequests
from z1monitoring_models.models.plates_state import PlateState

from z1monitoring_agent.utils.eta_dimensioning import calculate_eta, generate_pdf
from z1monitoring_agent.agent.eta_timeline import condense_eta_timeline


def _normalize_circuito_keys(circuito):
    """Traduz `recircula_para` legado pra `fluxo_segue_para_posicao`.
    Nome novo e literal: "a saida deste node segue pra posicao N". O node
    em si NAO esta em recirculacao — so entrega agua pra um node destino
    que recircula internamente. Nome antigo induzia interpretacao errada
    pelo LLM.
    """
    if not isinstance(circuito, list):
        return circuito
    for node in circuito:
        if isinstance(node, dict) and "recircula_para" in node:
            node.setdefault(
                "fluxo_segue_para_posicao", node["recircula_para"]
            )
            del node["recircula_para"]
    return circuito


def _get_farm_topology(farm) -> dict:
    """Extrai topologia semântica da farm para contexto do LLM."""
    if not farm or not hasattr(farm, 'topology') or not farm.topology:
        return None
    topology = farm.topology
    if not isinstance(topology, dict):
        return None
    circuito = topology.get("circuito")
    if not circuito:
        return None
    result = {"circuito": _normalize_circuito_keys(list(circuito))}
    relacoes = topology.get("relacoes")
    if relacoes:
        result["relacoes"] = relacoes
    return result


def _inject_eta_timeline(result: dict, farm, window_hours: int = 24):
    """Anexa timeline 24h condensada (IOX + CCD/periféricos) ao result.
    Usado em tools analíticas que já enriquecem com topologia_eta.
    """
    if not farm or not getattr(farm, "id", None):
        return
    try:
        timeline = condense_eta_timeline(farm.id, window_hours=window_hours)
    except Exception as e:
        import structlog as _sl
        _sl.get_logger().warning(
            "Falha ao montar timeline ETA",
            farm=getattr(farm, "name", None),
            error=str(e),
        )
        return
    if timeline.get("timeline_24h"):
        result["timeline_24h"] = timeline["timeline_24h"]
    if timeline.get("iox_estado_atual"):
        result["iox_estado_atual"] = timeline["iox_estado_atual"]


# spaces_upload é injetado pelo app que usa o pacote
spaces_upload = None


def set_spaces_upload(fn):
    """Permite que o app injete a função de upload."""
    global spaces_upload
    spaces_upload = fn

log = structlog.get_logger()


# =============================================================================
# CONTEXTO DO USUÁRIO
# =============================================================================


class UserContext:
    """Contexto do usuário para as ferramentas."""

    def __init__(self, user, conversation=None):
        self.user = user
        self.conversation = conversation
        self.permission_name = user.permissions.get("name", "SECONDARY") if user else "SECONDARY"
        self.associated = user.associated if user else None
        self.is_admin = self.permission_name == "ADMIN"
        self.is_primary = self.permission_name in ["ETA_REPRESENTANTES", "ETEA_REPRESENTANTES_ADMIN", "ETA_REPRESENTANTES_TEC", "ETA_VENDEDOR", "URBANO_REPRESENTANTES"]
        self.is_urban = self.permission_name == "URBANO_REPRESENTANTES"
        self.pending_messages = []  # Mensagens extras (imagens, docs) para enviar junto com a resposta
        self.msisdn = None  # Telefone do usuário (WhatsApp)
        self.channel = None  # Canal do WhatsApp
        self.send_immediate_fn = None  # Função para envio imediato (injetada pelo handler)
        self.reset_requested = False  # Sinaliza ao handler que o histórico deve ser zerado após o turno


# Contexto global (será setado pelo handler)
_current_context: Optional[UserContext] = None


def set_user_context(user, conversation=None):
    """Define o contexto do usuário para as ferramentas."""
    global _current_context
    _current_context = UserContext(user, conversation)


def get_user_context() -> Optional[UserContext]:
    """Obtém o contexto do usuário atual."""
    return _current_context


_PRIMARY_PERM_NAMES = {
    "PRIMARY",
    "ETA_REPRESENTANTES",
    "ETA_REPRESENTANTES_ADMIN",
    "ETA_REPRESENTANTES_TEC",
    "URBANO_REPRESENTANTES",
    "ETA_VENDEDOR",
    "ETA_READONLY",
}

_READONLY_PERM_NAMES = {"ETA_READONLY"}


def _get_allowed_farm_names(ctx):
    """
    Retorna a lista de nomes de farms que o usuário tem acesso.
    Retorna None quando é ADMIN (sem filtro).
    """
    if not ctx or ctx.is_admin:
        return None
    if ctx.permission_name in _PRIMARY_PERM_NAMES:
        farms = Farm.get_all_that_associated_allowed_permitted(ctx.associated)
    else:
        farms = Farm.get_all_farms_objs_filtereds({"owner": ctx.associated})
    return [f.name for f in farms]


def _enforce_farm_access(farm):
    """Retorna a farm se o user tem acesso, None caso contrario."""
    if not farm:
        return None
    ctx = get_user_context()
    if not ctx or ctx.is_admin:
        return farm
    allowed = _get_allowed_farm_names(ctx) or []
    if farm.name in allowed:
        return farm
    log.warning(
        "Tentativa de acesso a granja sem permissao (bloqueada)",
        farm=farm.name,
        user=ctx.user.name if ctx.user else None,
        associated=ctx.associated,
        permission=ctx.permission_name,
    )
    return None


def _can_write(ctx) -> bool:
    """Retorna True se o usuário pode executar ações de escrita."""
    if not ctx:
        return False
    return ctx.permission_name not in _READONLY_PERM_NAMES


def _readonly_response(farm_name: str = None) -> dict:
    """Resposta padronizada quando usuário read-only tenta acao."""
    local_txt = f" em {farm_name}" if farm_name else ""
    return {
        "erro": "acesso_somente_leitura",
        "mensagem": (
            f"👁️ Sua conta é somente leitura. Ações de escrita{local_txt} "
            "devem ser executadas pelo responsável técnico."
        ),
    }


def _require_write(fn):
    """Decorator que bloqueia execução de tool de escrita para read-only."""
    from functools import wraps

    @wraps(fn)
    def wrapper(*args, **kwargs):
        ctx = get_user_context()
        if not _can_write(ctx):
            log.warning(
                "Tool de escrita bloqueada para usuario read-only",
                tool=fn.__name__,
                user=ctx.user.name if ctx and ctx.user else None,
                permission=ctx.permission_name if ctx else None,
            )
            return _readonly_response()
        return fn(*args, **kwargs)

    return wrapper


def _resolve_farm_acl(nome):
    """Wrapper de Farm.get_farm_like_sensibility que aplica ACL por escopo do user."""
    farm = Farm.get_farm_like_sensibility(nome)
    return _enforce_farm_access(farm)


_PLATE_TYPE_CATEGORIA = {
    "IOX": "ambiencia e quadros de comandos",
    "IOC": "ambiencia",
    "Z1": "agua",
    "PHI": "agua",
    "ORP": "agua",
    "FLX": "agua",
    "NVL": "agua",
    "CCD": "agua",
    "OZ1": "agua",
    "WGT": "insumos",
    "CLPCG": "ambiencia",
    "QP4": "quadro",
    "QP7": "quadro",
    "QBT": "quadro",
    "QBT_CIS": "quadro",
}


def _plates_by_serials(serials):
    """Busca placas por lista de seriais. Retorna dict {serial: plate}."""
    result = {}
    for s in set(s for s in serials if s):
        try:
            matches = Plate.get_all({"serial": s}) or []
            if matches:
                result[s] = matches[0]
        except Exception:
            pass
    return result


def _enrich_alarm(alarme, plate_map: dict = None) -> dict:
    """Enriquece um UrgentAlarm com plate_type, categoria e descrição da placa.

    A categoria existe para o LLM NÃO confundir domínios — um alarme de IOX
    (cortinas, ventilação, galpão) não tem relação com ABS/ácido/cloro/dosagem.
    """
    plate_type = None
    plate_descr = None
    serial = getattr(alarme, "serial", None)
    if serial and plate_map is not None:
        plate = plate_map.get(serial)
        if plate:
            plate_type = plate.plate_type
            plate_descr = plate.description
    categoria = _PLATE_TYPE_CATEGORIA.get(plate_type, "outro") if plate_type else "outro"
    return {
        "granja": alarme.farm,
        "serial": serial,
        "plate_type": plate_type,
        "plate_descricao": plate_descr,
        "categoria": categoria,
        "sensor": alarme.sensor,
        "status": alarme.status,
        "atendido": alarme.attended,
        "data": alarme.created_at.strftime("%d/%m %H:%M") if alarme.created_at else None,
    }


# =============================================================================
# 1. STATUS E CONSULTAS
# =============================================================================


def consultar_status(tipo: str, granja: str = None, filtro: str = None, dias: int = 1) -> dict:
    """
    Consulta status geral do sistema. Unifica alarmes, equipamentos offline/online,
    falta de insumos, falta de gás e sensores fora da faixa.

    Args:
        tipo: alarmes, offline, online, falta_insumo, falta_gas, fora_faixa
        granja: Nome da granja para filtrar (opcional)
        filtro: Filtro adicional - para falta_insumo: acido/cloro/todos; para fora_faixa: ph/orp/todos
        dias: Dias para buscar alarmes (default: 1)

    Returns:
        Resultado da consulta
    """
    if tipo == "alarmes":
        return consultar_alarmes(granja=granja, dias=dias)
    elif tipo == "offline":
        return consultar_equipamentos(status="offline")
    elif tipo == "online":
        return consultar_equipamentos(status="online")
    elif tipo == "falta_insumo":
        return consultar_falta_insumo(insumo=filtro or "todos")
    elif tipo == "falta_gas":
        return consultar_falta_gas()
    elif tipo == "fora_faixa":
        return consultar_sensor_fora_faixa(sensor=filtro or "todos")
    else:
        return {"erro": f"Tipo '{tipo}' não reconhecido. Use: alarmes, offline, online, falta_insumo, falta_gas, fora_faixa"}


def consultar_alarmes(granja: str = None, dias: int = 1) -> dict:
    """
    Consulta alarmes urgentes recentes.

    Args:
        granja: Nome da granja para filtrar (opcional)
        dias: Quantos dias para trás buscar (default: 1)

    Returns:
        Lista de alarmes encontrados
    """
    try:
        ctx = get_user_context()
        data_inicio = datetime.now() - timedelta(days=dias)
        allowed_farm_names = _get_allowed_farm_names(ctx)

        if granja:
            if allowed_farm_names is not None and granja not in allowed_farm_names:
                return {
                    "encontrados": 0,
                    "mensagem": "Você não tem acesso a esta granja.",
                    "alarmes": [],
                }
            alarmes = UrgentAlarm.get_by_farm_and_date(granja, data_inicio)
        else:
            alarmes = UrgentAlarm.get_recent(data_inicio)
            if allowed_farm_names is not None:
                allowed_set = set(allowed_farm_names)
                alarmes = [a for a in alarmes if a.farm in allowed_set]

        if not alarmes:
            return {
                "encontrados": 0,
                "mensagem": f"Nenhum alarme nos últimos {dias} dia(s)",
                "alarmes": [],
            }

        top = alarmes[:10]
        plate_map = _plates_by_serials([getattr(a, "serial", None) for a in top])
        alarmes_formatados = [_enrich_alarm(a, plate_map) for a in top]

        return {
            "encontrados": len(alarmes),
            "mostrando": len(alarmes_formatados),
            "alarmes": alarmes_formatados,
        }

    except Exception as e:
        log.error("Erro ao consultar alarmes", error=str(e))
        return {"erro": str(e)}


def consultar_equipamentos(status: str = "offline") -> dict:
    """
    Lista equipamentos por status de comunicação.

    Args:
        status: offline ou online

    Returns:
        Lista de equipamentos com o status solicitado
    """
    ctx = get_user_context()
    try:
        is_offline = status == "offline"
        filters = {"have_communication": not is_offline}
        if ctx and not ctx.is_admin:
            filters["associateds_allowed"] = ctx.associated

        plates = Plate.get_all(filters)

        if not plates:
            return {"total": 0, "mensagem": f"Nenhum equipamento {status}"}

        equipamentos = []

        if is_offline:
            from z1monitoring_models.models.events_last import LastEvent
            from datetime import datetime
            now = datetime.now()

            for plate in plates:
                last = LastEvent.get_last_register(plate.owner, plate.serial)
                ultimo_contato = last.get("created_at") if isinstance(last, dict) else getattr(last, "created_at", None) if last else None
                dias_offline = (now - ultimo_contato).days if ultimo_contato else 999

                equipamentos.append({
                    "serial": plate.serial,
                    "tipo": plate.plate_type,
                    "granja": plate.farm_associated,
                    "ultimo_contato": ultimo_contato.strftime("%d/%m/%Y %H:%M") if ultimo_contato else "desconhecido",
                    "dias_offline": dias_offline,
                })

            equipamentos.sort(key=lambda x: x["dias_offline"], reverse=True)
        else:
            for plate in plates[:30]:
                equipamentos.append({
                    "serial": plate.serial,
                    "tipo": plate.plate_type,
                    "granja": plate.farm_associated,
                })

        return {
            "total": len(plates),
            "mostrando": min(len(equipamentos), 30),
            "equipamentos": equipamentos[:30],
        }

    except Exception as e:
        log.error("Erro ao consultar equipamentos online", error=str(e))
        return {"erro": str(e)}


def consultar_falta_insumo(insumo: str = "todos") -> dict:
    """
    Lista equipamentos com falta de insumo (ácido, cloro ou ambos).

    Args:
        insumo: acido, cloro, ou todos

    Returns:
        Lista de equipamentos sem o insumo especificado
    """
    ctx = get_user_context()
    try:
        resultados = []

        if insumo in ("acido", "todos"):
            filters = {"plate_type": ["Z1"], "have_acid": False}
            if ctx and not ctx.is_admin:
                filters["associateds_allowed"] = ctx.associated
            plates = Plate.get_all(filters)
            if plates:
                locais = list(set([p.farm_associated for p in plates if p.farm_associated]))
                resultados.append({"insumo": "acido", "total": len(plates), "locais_afetados": locais})

        if insumo in ("cloro", "todos"):
            filters = {"plate_type": ["Z1"], "have_chlorine": False}
            if ctx and not ctx.is_admin:
                filters["associateds_allowed"] = ctx.associated
            plates = Plate.get_all(filters)
            if plates:
                locais = list(set([p.farm_associated for p in plates if p.farm_associated]))
                resultados.append({"insumo": "cloro", "total": len(plates), "locais_afetados": locais})

        if not resultados:
            return {"total": 0, "mensagem": f"Nenhum equipamento com falta de {'insumo' if insumo == 'todos' else insumo}"}

        return {"resultados": resultados}

    except Exception as e:
        log.error("Erro ao consultar falta de insumo", error=str(e))
        return {"erro": str(e)}


def consultar_falta_gas() -> dict:
    """
    Lista equipamentos com falta de gás (nível baixo).

    Returns:
        Lista de equipamentos com gás baixo
    """
    ctx = get_user_context()
    try:
        from z1monitoring_agent.utils import commons_actions

        # Busca todas as placas (o handler filtra por gás)
        filters = {}
        if ctx and not ctx.is_admin:
            filters["associateds_allowed"] = ctx.associated

        plates = Plate.get_all(filters)

        if not plates:
            return {"total": 0, "mensagem": "Nenhum equipamento encontrado"}

        # Usa handler existente
        resultado = commons_actions.handler_placas_falta_gas(plates)

        return {"mensagem": resultado}

    except Exception as e:
        log.error("Erro ao consultar falta de gás", error=str(e))
        return {"erro": str(e)}


def consultar_sensor_fora_faixa(sensor: str = "todos") -> dict:
    """
    Lista equipamentos com sensor fora da faixa configurada.

    Args:
        sensor: ph, orp, ou todos

    Returns:
        Lista de equipamentos com o sensor fora da faixa
    """
    ctx = get_user_context()
    try:
        resultados = []

        if sensor in ("ph", "todos"):
            filters = {"plate_type": ["Z1", "PHI"], "out_ph": "true"}
            if ctx and not ctx.is_admin:
                filters["associateds_allowed"] = ctx.associated
            plates = Plate.get_all(filters)
            for p in plates:
                sv = p.sensors_value
                resultados.append({"sensor": "ph", "granja": p.farm_associated, "valor": sv.get("ph", 0) if sv else 0, "serial": p.serial})

            ccd_filters = {"plate_type": ["CCD"]}
            if ctx and not ctx.is_admin:
                ccd_filters["associateds_allowed"] = ctx.associated
            for p in Plate.get_all(ccd_filters):
                sv = p.sensors_value
                if sv and sv.get("Falha: PH fora da faixa") == 1:
                    resultados.append({"sensor": "ph", "granja": p.farm_associated, "valor": sv.get("PH", 0), "serial": p.serial})

        if sensor in ("orp", "todos"):
            filters = {"plate_type": ["Z1", "ORP"], "out_orp": "true"}
            if ctx and not ctx.is_admin:
                filters["associateds_allowed"] = ctx.associated
            plates = Plate.get_all(filters)
            for p in plates:
                sv = p.sensors_value
                resultados.append({"sensor": "orp", "granja": p.farm_associated, "valor": sv.get("orp", 0) if sv else 0, "serial": p.serial})

            ccd_filters = {"plate_type": ["CCD"]}
            if ctx and not ctx.is_admin:
                ccd_filters["associateds_allowed"] = ctx.associated
            for p in Plate.get_all(ccd_filters):
                sv = p.sensors_value
                if sv and sv.get("Falha: ORP fora da faixa") == 1:
                    resultados.append({"sensor": "orp", "granja": p.farm_associated, "valor": sv.get("ORP", 0), "serial": p.serial})

        if not resultados:
            return {"total": 0, "mensagem": f"Nenhum equipamento com {'sensor' if sensor == 'todos' else sensor} fora da faixa"}

        return {"total": len(resultados), "locais": resultados}

    except Exception as e:
        log.error("Erro ao consultar sensor fora da faixa", error=str(e))
        return {"erro": str(e)}


def status_equipamento(serial: str) -> dict:
    """
    Consulta status detalhado de um equipamento pelo serial.

    Args:
        serial: Número serial do equipamento

    Returns:
        Status completo do equipamento
    """
    try:
        plate = Plate.load(serial)

        if not plate:
            return {"encontrado": False, "mensagem": f"Equipamento '{serial}' não encontrado"}

        # Monta informações básicas
        info = {
            "encontrado": True,
            "serial": plate.serial,
            "tipo": plate.plate_type,
            "granja": plate.farm_associated,
            "descricao": plate.description,
            "comunicando": plate.have_communication,
            "ultimo_contato": plate.updated_at.strftime("%d/%m %H:%M") if plate.updated_at else None,
        }

        # Adiciona sensores se disponíveis
        if plate.sensors_value:
            info["sensores"] = plate.sensors_value

        return info

    except Exception as e:
        log.error("Erro ao consultar equipamento", error=str(e))
        return {"erro": str(e)}


# =============================================================================
# 2. TEMPO REAL
# =============================================================================


def tempo_real(granja: str, sensor: str = "geral") -> dict:
    """
    Obtém leitura em tempo real de sensores de uma granja.

    Args:
        granja: Nome da granja
        sensor: geral, ph, orp, temperatura, gas, nivel_agua, fluxo_agua, ozonio, dosadora

    Returns:
        Leituras do sensor solicitado
    """
    try:
        farm = _resolve_farm_acl(granja)
        if not farm:
            return {"erro": f"Granja '{granja}' não encontrada"}

        if sensor == "geral":
            from z1monitoring_agent.utils import commons_actions
            plates = Plate.get_all({"farm_associated": farm.name})
            if not plates:
                return {"erro": f"Nenhum equipamento encontrado em '{farm.name}'"}
            resultado = commons_actions.handler_tempo_real_geral(farm, plates)
            return {"granja": farm.name, "mensagem": resultado}

        if sensor == "gas":
            from z1monitoring_agent.utils import commons_actions
            plates = Plate.get_all({"farm_associated": farm.name})
            if not plates:
                return {"erro": f"Nenhum equipamento encontrado em '{farm.name}'"}
            resultado = commons_actions.handler_tempo_real_gas(farm, plates)
            return {"granja": farm.name, "mensagem": resultado}

        # Mapeamento sensor → tipo de placa e campos
        sensor_config = {
            "ph": {"plate_types": ["Z1", "PHI", "CCD"], "fields": lambda sv: {"ph": sv.get("ph") or sv.get("PH"), "ph_min": sv.get("ph_min"), "ph_max": sv.get("ph_max")}},
            "orp": {"plate_types": ["Z1", "ORP", "CCD"], "fields": lambda sv: {"orp": sv.get("orp") or sv.get("ORP"), "orp_min": sv.get("orp_min"), "orp_max": sv.get("orp_max")}},
            "temperatura": {"plate_types": ["Z1", "AZ1", "CCD"], "fields": lambda sv: {"temperatura": sv.get("temperature") or sv.get("Temperatura da Água")}},
            "nivel_agua": {"plate_types": ["NVL"], "fields": lambda sv: {"nivel_percentual": sv.get("level_percentage"), "volume_litros": sv.get("volume")}},
            "fluxo_agua": {"plate_types": ["FLX"], "fields": lambda sv: {"fluxo_lpm": sv.get("flow"), "total_litros": sv.get("total_volume")}},
            "dosadora": {"plate_types": ["CCD"], "fields": lambda sv: {
                "acido_kg": sv.get("Ácido"), "cloro_kg": sv.get("Cloro"),
                "modo_acido": sv.get("Modo Dosadora Ácido"), "modo_cloro": sv.get("Modo Dosadora Cloro"),
                "dosadora_acido_ligada": sv.get("Comando Dosadora Ácido"), "dosadora_cloro_ligada": sv.get("Comando Dosadora Cloro"),
            }},
            "ozonio": {"plate_types": ["OZ1"], "fields": lambda sv: {"orp": sv.get("orp"), "horas_ligado": sv.get("hours_on")}},
        }

        config = sensor_config.get(sensor)
        if not config:
            return {"erro": f"Sensor '{sensor}' não reconhecido. Use: geral, ph, orp, temperatura, gas, nivel_agua, fluxo_agua, ozonio, dosadora"}

        plates = Plate.get_all({"farm_associated": farm.name, "plate_type": config["plate_types"]})
        if not plates:
            return {"erro": f"Nenhum sensor de {sensor} encontrado em '{farm.name}'"}

        leituras = []
        for plate in plates:
            sv = plate.sensors_value
            if not sv:
                continue
            info = {"serial": plate.serial, "comunicando": plate.have_communication}
            info.update(config["fields"](sv))

            # Dados extras pra ozônio
            if sensor == "ozonio":
                params = plate.params or {}
                if params.get("cell_en") is not None:
                    info["celula_ligada"] = params["cell_en"] in (1, True, "1")
                if params.get("dryer_en") is not None:
                    info["secador_ligado"] = params["dryer_en"] in (1, True, "1")

            leituras.append(info)

        return {"granja": farm.name, "sensor": sensor, "leituras": leituras}

    except Exception as e:
        log.error("Erro ao consultar tempo real", error=str(e))
        return {"erro": str(e)}


# =============================================================================
# 3. ANÁLISES
# =============================================================================


def analise(granja: str, tipo: str = "agua", horas: int = 24) -> dict:
    """
    Faz análise de uma granja.

    Args:
        granja: Nome da granja
        tipo: agua (pH, ORP, temperatura) ou gas (nível, consumo, autonomia)
        horas: janela da timeline de eventos (default 24, max 168 = 7 dias)

    Returns:
        Análise do tipo solicitado. timeline_eta cobre as últimas `horas`.
    """
    try:
        farm = _resolve_farm_acl(granja)
        if not farm:
            return {"erro": f"Granja '{granja}' não encontrada"}

        # Clamp de janela pra evitar abuso
        if horas < 1:
            horas = 1
        if horas > 168:
            horas = 168

        if tipo == "gas":
            from z1monitoring_agent.utils import commons_actions
            plates = Plate.get_all({"farm_associated": farm.name})
            if not plates:
                return {"erro": f"Nenhum equipamento encontrado em '{farm.name}'"}
            resultado = commons_actions.handler_tempo_real_gas(farm, plates)
            return {"granja": farm.name, "mensagem": resultado}

        # tipo == "agua"
        plates = Plate.get_all({"farm_associated": farm.name, "plate_type": ["Z1", "PHI", "ORP", "CCD"]})
        if not plates:
            return {"erro": f"Nenhum sensor de água encontrado em '{farm.name}'"}

        result = {"granja": farm.name, "ph": None, "orp": None, "temperatura": None, "status_geral": "ok", "alertas": []}

        for plate in plates:
            sv = plate.sensors_value
            if not sv:
                continue

            if result["ph"] is None:
                ph = sv.get("ph") or sv.get("PH")
                if ph is not None:
                    ph_min = sv.get("ph_min") or sv.get("pH Alvo Inferior", 6.5)
                    ph_max = sv.get("ph_max") or sv.get("pH Alvo Superior", 7.5)
                    na_faixa = float(ph_min) <= float(ph) <= float(ph_max)
                    result["ph"] = {"valor": ph, "minimo": ph_min, "maximo": ph_max, "na_faixa": na_faixa}
                    if not na_faixa:
                        result["alertas"].append(f"pH fora da faixa: {ph}")
                        result["status_geral"] = "alerta"

            if result["orp"] is None:
                orp = sv.get("orp") or sv.get("ORP")
                if orp is not None:
                    orp_min = sv.get("orp_min") or sv.get("ORP Alvo Inferior", 650)
                    orp_max = sv.get("orp_max") or sv.get("ORP Alvo Superior", 750)
                    na_faixa = float(orp_min) <= float(orp) <= float(orp_max)
                    result["orp"] = {"valor": orp, "minimo": orp_min, "maximo": orp_max, "na_faixa": na_faixa}
                    if not na_faixa:
                        result["alertas"].append(f"ORP fora da faixa: {orp}")
                        result["status_geral"] = "alerta"

            if result["temperatura"] is None:
                temp = sv.get("temperature") or sv.get("Temperatura da Água")
                if temp is not None:
                    result["temperatura"] = temp

        topologia = _get_farm_topology(farm)
        if topologia:
            result["topologia_eta"] = topologia
        _inject_eta_timeline(result, farm, window_hours=horas)
        result["periodo_horas"] = horas

        return result

    except Exception as e:
        log.error("Erro na análise", error=str(e))
        return {"erro": str(e)}


# =============================================================================
# 4. GRANJAS E CLIENTES
# =============================================================================


def listar_clientes_primarios(nome: str = None) -> dict:
    """
    Lista clientes primários do sistema.

    Args:
        nome: Nome ou parte do nome para filtrar (opcional)

    Returns:
        Lista de clientes primários
    """
    ctx = get_user_context()
    try:
        # Apenas admin pode ver clientes primários
        if ctx and not ctx.is_admin:
            return {"erro": "Apenas administradores podem listar clientes primários"}

        from z1monitoring_models.models.clients_primary import ClientPrimary

        if nome:
            clientes = ClientPrimary.get_all_associated_by_name(nome)
        else:
            # get_all retorna dicts, precisamos converter
            clientes_dicts = ClientPrimary.get_all()
            return {
                "total": len(clientes_dicts),
                "mostrando": min(len(clientes_dicts), 30),
                "clientes": [
                    {"nome": c.get("fantasy_name"), "cnpj": c.get("cnpj"), "tipo": c.get("client_type")}
                    for c in clientes_dicts[:30]
                ],
            }

        if not clientes:
            return {"total": 0, "clientes": [], "mensagem": f"Nenhum cliente encontrado com nome '{nome}'"}

        return {
            "total": len(clientes),
            "mostrando": min(len(clientes), 30),
            "clientes": [
                {"nome": c.fantasy_name, "cnpj": c.cnpj, "tipo": c.client_type}
                for c in clientes[:30]
            ],
        }

    except Exception as e:
        log.error("Erro ao listar clientes primários", error=str(e))
        return {"erro": str(e)}


def buscar_cliente_primario(nome: str) -> dict:
    """
    Busca um cliente primário pelo nome.

    Args:
        nome: Nome ou parte do nome do cliente primário

    Returns:
        Informações do cliente primário encontrado
    """
    ctx = get_user_context()
    try:
        if ctx and not ctx.is_admin:
            return {"erro": "Apenas administradores podem buscar clientes primários"}

        from z1monitoring_models.models.clients_primary import ClientPrimary

        clientes = ClientPrimary.get_all_associated_by_name(nome)

        if not clientes:
            return {"encontrado": False, "mensagem": f"Cliente primário '{nome}' não encontrado"}

        cliente = clientes[0]  # Pega o primeiro resultado

        return {
            "encontrado": True,
            "nome": cliente.fantasy_name,
            "razao_social": cliente.social_name,
            "cnpj": cliente.cnpj,
            "tipo": cliente.client_type,
            "email": cliente.email,
            "telefone": cliente.phone,
        }

    except Exception as e:
        log.error("Erro ao buscar cliente primário", error=str(e))
        return {"erro": str(e)}


def listar_granjas_cliente_primario(nome_cliente: str, tipo_equipamento: str = None) -> dict:
    """
    Lista granjas que pertencem a um cliente primário.

    A hierarquia é: ClientePrimário -> ClienteSecundário -> Granja -> Placa
    Esta função busca o cliente primário pelo nome, encontra todos os clientes
    secundários associados a ele, e retorna as granjas desses clientes.

    Args:
        nome_cliente: Nome do cliente primário (ex: "Ultragas", "BRF")
        tipo_equipamento: Filtrar por tipo de equipamento ("gas", "agua", "dosagem", etc) - opcional

    Returns:
        Lista de granjas do cliente primário
    """
    ctx = get_user_context()
    try:
        if ctx and not ctx.is_admin:
            return {"erro": "Apenas administradores podem consultar por cliente primário"}

        from z1monitoring_models.models.clients_primary import ClientPrimary
        from z1monitoring_models.models.clients_secondary import ClientSecondary

        # 1. Busca o cliente primário pelo nome
        clientes_primarios = ClientPrimary.get_all_associated_by_name(nome_cliente)

        if not clientes_primarios:
            return {"encontrado": False, "mensagem": f"Cliente primário '{nome_cliente}' não encontrado"}

        cliente_primario = clientes_primarios[0]
        cnpj = cliente_primario.cnpj

        # 2. Busca clientes secundários que tem esse CNPJ no associateds_allowed
        clientes_secundarios = ClientSecondary.get_all({"associateds_allowed": cnpj})

        if not clientes_secundarios:
            return {
                "cliente": cliente_primario.fantasy_name,
                "total_granjas": 0,
                "granjas": [],
                "mensagem": "Nenhum cliente secundário associado a este cliente primário",
            }

        # 3. Pega os identifications dos clientes secundários
        identificacoes = [cs.identification for cs in clientes_secundarios]

        # 4. Busca granjas de todos esses clientes secundários
        granjas_encontradas = []
        for identificacao in identificacoes:
            farms = Farm.get_all_farms_objs_filtereds({"owner": identificacao})
            for farm in farms:
                # Filtra por tipo de equipamento se especificado
                if tipo_equipamento:
                    plates = Plate.get_all({"farm_associated": farm.name})
                    tem_tipo = False
                    for plate in plates:
                        if tipo_equipamento.lower() == "gas" and plate.plate_type == "WGT":
                            tem_tipo = True
                            break
                        elif tipo_equipamento.lower() == "agua" and plate.plate_type in ["Z1", "PHI", "ORP", "NVL", "FLX"]:
                            tem_tipo = True
                            break
                        elif tipo_equipamento.lower() == "dosagem" and plate.plate_type == "CCD":
                            tem_tipo = True
                            break
                    if not tem_tipo:
                        continue

                granjas_encontradas.append({
                    "nome": farm.name,
                    "cliente_secundario": identificacao,
                })

        return {
            "cliente_primario": cliente_primario.fantasy_name,
            "total_granjas": len(granjas_encontradas),
            "mostrando": min(len(granjas_encontradas), 50),
            "granjas": granjas_encontradas[:50],
        }

    except Exception as e:
        log.error("Erro ao listar granjas do cliente primário", error=str(e))
        return {"erro": str(e)}


def consultar_falta_gas_cliente_primario(nome_cliente: str) -> dict:
    """
    Lista locais com falta de gás de um cliente primário específico.

    Útil para distribuidoras de gás (como Ultragas) que precisam ver
    apenas os locais que pertencem a elas.

    Args:
        nome_cliente: Nome do cliente primário (ex: "Ultragas")

    Returns:
        Lista de locais com falta de gás do cliente primário
    """
    ctx = get_user_context()
    try:
        if ctx and not ctx.is_admin:
            return {"erro": "Apenas administradores podem consultar por cliente primário"}

        from z1monitoring_models.models.clients_primary import ClientPrimary
        from z1monitoring_models.models.clients_secondary import ClientSecondary
        from z1monitoring_agent.utils import commons_actions

        # 1. Busca o cliente primário
        clientes_primarios = ClientPrimary.get_all_associated_by_name(nome_cliente)

        if not clientes_primarios:
            return {"encontrado": False, "mensagem": f"Cliente primário '{nome_cliente}' não encontrado"}

        cliente_primario = clientes_primarios[0]
        cnpj = cliente_primario.cnpj

        # 2. Busca clientes secundários associados
        clientes_secundarios = ClientSecondary.get_all({"associateds_allowed": cnpj})

        if not clientes_secundarios:
            return {
                "cliente": cliente_primario.fantasy_name,
                "total": 0,
                "mensagem": "Nenhum cliente secundário associado",
            }

        # 3. Pega todas as placas dos clientes secundários
        identificacoes = [cs.identification for cs in clientes_secundarios]
        todas_placas = []

        for identificacao in identificacoes:
            farms = Farm.get_all_farms_objs_filtereds({"owner": identificacao})
            for farm in farms:
                plates = Plate.get_all({"farm_associated": farm.name})
                todas_placas.extend(plates)

        if not todas_placas:
            return {
                "cliente": cliente_primario.fantasy_name,
                "total": 0,
                "mensagem": "Nenhum equipamento encontrado",
            }

        # 4. Usa o handler existente para filtrar placas com falta de gás
        resultado = commons_actions.handler_placas_falta_gas(todas_placas)

        return {
            "cliente": cliente_primario.fantasy_name,
            "mensagem": resultado,
        }

    except Exception as e:
        log.error("Erro ao consultar falta de gás do cliente primário", error=str(e))
        return {"erro": str(e)}


def _normalize_text(text: str) -> str:
    """Remove acentos e converte para lowercase."""
    import unicodedata

    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower().strip()


def _get_farm_candidates(nome: str) -> list:
    """Retorna todas as granjas que batem por substring com o nome dado,
    respeitando o escopo de acesso do usuário."""
    try:
        all_farms = Farm.get_all_farms_objs_filtereds({})
        ctx = get_user_context()
        if ctx and not ctx.is_admin:
            allowed = set(_get_allowed_farm_names(ctx) or [])
            all_farms = [f for f in all_farms if f.name in allowed]

        nome_norm = _normalize_text(nome)
        candidates = []
        for f in all_farms:
            farm_norm = _normalize_text(f.name)
            if nome_norm in farm_norm or farm_norm in nome_norm:
                candidates.append(f.name)
        return candidates
    except Exception:
        return []


def buscar_granja(nome: str) -> dict:
    """
    Busca informações de uma granja pelo nome.
    Detecta ambiguidade quando múltiplas granjas batem com o nome.

    Args:
        nome: Nome ou parte do nome da granja

    Returns:
        Informações da granja, ou lista de candidatas se ambíguo
    """
    try:
        # Primeiro verifica se há múltiplos matches (ambiguidade)
        candidates = _get_farm_candidates(nome)

        if len(candidates) == 0:
            # Tenta fuzzy match
            farm = _resolve_farm_acl(nome)
            if not farm:
                return {"encontrada": False, "mensagem": f"Granja '{nome}' não encontrada"}
            candidates = [farm.name]

        if len(candidates) > 1:
            return {
                "encontrada": False,
                "ambiguo": True,
                "candidatas": candidates,
                "mensagem": f"Encontrei {len(candidates)} granjas com nome parecido. Qual delas?",
            }

        farm_name = candidates[0]
        farm = _resolve_farm_acl(farm_name)
        if not farm:
            return {"encontrada": False, "mensagem": f"Granja '{nome}' não encontrada"}

        # Conta equipamentos
        plates = Plate.get_all({"farm_associated": farm.name})

        return {
            "encontrada": True,
            "nome": farm.name,
            "cliente": farm.owner,
            "total_equipamentos": len(plates) if plates else 0,
        }

    except Exception as e:
        log.error("Erro ao buscar granja", error=str(e))
        return {"erro": str(e)}


def listar_granjas_usuario() -> dict:
    """
    Lista todas as granjas do usuário atual.

    Returns:
        Lista de granjas do usuário
    """
    ctx = get_user_context()
    try:
        if not ctx:
            return {"erro": "Contexto de usuário não disponível"}
        if ctx.is_admin:
            farms = Farm.get_all_farms_objs_filtereds({})
        elif ctx.permission_name in _PRIMARY_PERM_NAMES:
            farms = Farm.get_all_that_associated_allowed_permitted(ctx.associated)
        else:
            farms = Farm.get_all_farms_objs_filtereds({"owner": ctx.associated})

        if not farms:
            return {"total": 0, "granjas": [], "mensagem": "Nenhuma granja encontrada"}

        granjas = [{"nome": f.name, "cliente": f.owner} for f in farms[:30]]

        return {
            "total": len(farms),
            "mostrando": len(granjas),
            "granjas": granjas,
        }

    except Exception as e:
        log.error("Erro ao listar granjas", error=str(e))
        return {"erro": str(e)}


# =============================================================================
# 5. CONTROLE DE PARÂMETROS
# =============================================================================


@_require_write
def ajustar_faixa(granja: str, sensor: str, valor_min: float, valor_max: float) -> dict:
    """
    Ajusta a faixa de um sensor (pH ou ORP) de uma granja.

    Args:
        granja: Nome da granja
        sensor: ph ou orp
        valor_min: Valor mínimo (ex: 6.5 para pH, 650 para ORP)
        valor_max: Valor máximo (ex: 7.5 para pH, 750 para ORP)

    Returns:
        Confirmação do ajuste solicitado
    """
    try:
        if valor_min >= valor_max:
            return {"erro": f"{sensor} mínimo deve ser menor que o máximo"}

        if sensor == "ph" and (valor_min < 0 or valor_max > 14):
            return {"erro": "pH deve estar entre 0 e 14"}
        if sensor == "orp" and (valor_min < 0 or valor_max > 1500):
            return {"erro": "ORP deve estar entre 0 e 1500 mV"}

        farm = _resolve_farm_acl(granja)
        if not farm:
            return {"erro": f"Granja '{granja}' não encontrada"}

        unidade = " mV" if sensor == "orp" else ""
        return {
            "acao": f"ajuste_{sensor}",
            "granja": farm.name,
            f"{sensor}_min": valor_min,
            f"{sensor}_max": valor_max,
            "requer_confirmacao": True,
            "mensagem": f"Confirma ajuste de {sensor.upper()} para {valor_min} - {valor_max}{unidade} em {farm.name}?",
        }

    except Exception as e:
        log.error("Erro ao ajustar faixa", error=str(e))
        return {"erro": str(e)}


@_require_write
def controlar_dosadora(granja: str, dosadora: str, acao: str) -> dict:
    """
    Controla uma dosadora (ligar/desligar ou mudar modo).

    Args:
        granja: Nome da granja
        dosadora: Tipo da dosadora ("acido" ou "cloro")
        acao: Ação a executar ("ligar", "desligar", "automatico", "ciclico")

    Returns:
        Confirmação da ação solicitada
    """
    try:
        if dosadora not in ["acido", "cloro"]:
            return {"erro": "Dosadora deve ser 'acido' ou 'cloro'"}

        if acao not in ["ligar", "desligar", "automatico", "ciclico"]:
            return {"erro": "Ação deve ser 'ligar', 'desligar', 'automatico' ou 'ciclico'"}

        farm = _resolve_farm_acl(granja)
        if not farm:
            return {"erro": f"Granja '{granja}' não encontrada"}

        return {
            "acao": f"dosadora_{acao}",
            "granja": farm.name,
            "dosadora": dosadora,
            "requer_confirmacao": True,
            "mensagem": f"Confirma {acao} dosadora de {dosadora} em {farm.name}?",
        }

    except Exception as e:
        log.error("Erro ao controlar dosadora", error=str(e))
        return {"erro": str(e)}


@_require_write
def controlar_abs(granja: str, dosadora: str, acao: str) -> dict:
    """
    Controla o ABS (freio automático de limite 24h) de ácido ou cloro.

    Args:
        granja: Nome da granja
        dosadora: acido ou cloro
        acao: liberar (desativa freio, permite injeção) ou rearmar (reativa freio automático)

    Returns:
        Confirmação da ação solicitada
    """
    try:
        if dosadora not in ["acido", "cloro"]:
            return {"erro": "Dosadora deve ser 'acido' ou 'cloro'"}

        farm = _resolve_farm_acl(granja)
        if not farm:
            return {"erro": f"Granja '{granja}' não encontrada"}

        if acao == "liberar":
            return {
                "acao": f"liberar_abs_{dosadora}",
                "granja": farm.name,
                "dosadora": dosadora,
                "requer_confirmacao": True,
                "mensagem": f"Confirma liberar injeção de {dosadora} em {farm.name}? (override do limite 24h)",
            }
        elif acao == "rearmar":
            param_name = "abs_acid" if dosadora == "acido" else "abs_chlorine"
            return {
                "acao": f"rearmar_abs_{dosadora}",
                "granja": farm.name,
                "dosadora": dosadora,
                "parametro": param_name,
                "valor": 1,
                "requer_confirmacao": True,
                "mensagem": f"Confirma rearmar ABS de {dosadora} em {farm.name}? (reativa controle automático)",
            }
        else:
            return {"erro": "Ação deve ser 'liberar' ou 'rearmar'"}

    except Exception as e:
        log.error("Erro ao controlar ABS", error=str(e))
        return {"erro": str(e)}


@_require_write
def definir_limite_24h(granja: str, dosadora: str, limite_kg: float) -> dict:
    """
    Define o limite de consumo em 24h para uma dosadora (ABS).

    Quando o consumo atinge este limite, a injeção é bloqueada automaticamente
    até ser liberada manualmente ou passar 24h.

    Args:
        granja: Nome da granja
        dosadora: Tipo da dosadora ("acido" ou "cloro")
        limite_kg: Limite em kg para 24h

    Returns:
        Confirmação do ajuste solicitado
    """
    try:
        if dosadora not in ["acido", "cloro"]:
            return {"erro": "Dosadora deve ser 'acido' ou 'cloro'"}

        if limite_kg <= 0:
            return {"erro": "Limite deve ser maior que zero"}

        if limite_kg > 100:
            return {"erro": "Limite máximo é 100 kg"}

        farm = _resolve_farm_acl(granja)
        if not farm:
            return {"erro": f"Granja '{granja}' não encontrada"}

        return {
            "acao": f"definir_limite_24h_{dosadora}",
            "granja": farm.name,
            "dosadora": dosadora,
            "limite_kg": limite_kg,
            "requer_confirmacao": True,
            "mensagem": f"Confirma limite de {limite_kg}kg/24h para {dosadora} em {farm.name}?",
        }

    except Exception as e:
        log.error("Erro ao definir limite 24h", error=str(e))
        return {"erro": str(e)}


DRYER_TEMP_MIN = 20
DRYER_TEMP_MAX = 80


@_require_write
def ajustar_oz1(
    granja: str,
    celula_ligada: bool = None,
    secador_ligado: bool = None,
    temperatura_secador: int = None,
    tempo_celula_ligada_min: int = None,
    tempo_celula_desligada_min: int = None,
) -> dict:
    """
    Controla a máquina de ozônio (OZ1): ligar/desligar célula, secador, temperatura, tempos.

    Args:
        granja: Nome da granja
        celula_ligada: True para ligar, False para desligar a célula de ozônio
        secador_ligado: True para ligar, False para desligar o secador
        temperatura_secador: Temperatura do secador em °C (20-80)
        tempo_celula_ligada_min: Tempo que a célula fica ligada em minutos
        tempo_celula_desligada_min: Tempo que a célula fica desligada em minutos

    Returns:
        Confirmação do ajuste solicitado
    """
    try:
        farm = _resolve_farm_acl(granja)
        if not farm:
            return {"erro": f"Granja '{granja}' não encontrada"}

        plates = Plate.get_all(
            {
                "farm_associated": farm.name,
                "plate_type": ["OZ1"],
            }
        )

        if not plates:
            return {"erro": f"'{farm.name}' não possui máquina de ozônio (OZ1) cadastrada."}

        # Verifica se pelo menos um parâmetro foi informado
        parametros = [celula_ligada, secador_ligado, temperatura_secador, tempo_celula_ligada_min, tempo_celula_desligada_min]
        if not any(x is not None for x in parametros):
            return {
                "mensagem": "O que deseja ajustar na máquina de ozônio?",
                "opcoes": [
                    "Ligar/desligar célula de ozônio",
                    "Ligar/desligar secador",
                    "Ajustar temperatura do secador (20-80°C)",
                    "Tempo da célula ligada/desligada (minutos)",
                ],
            }

        # Validações
        if temperatura_secador is not None:
            if temperatura_secador < DRYER_TEMP_MIN or temperatura_secador > DRYER_TEMP_MAX:
                return {"erro": f"Temperatura do secador deve estar entre {DRYER_TEMP_MIN}°C e {DRYER_TEMP_MAX}°C. Valor informado: {temperatura_secador}°C"}

        if tempo_celula_ligada_min is not None and tempo_celula_ligada_min < 1:
            return {"erro": "O tempo da célula ligada deve ser pelo menos 1 minuto."}

        if tempo_celula_desligada_min is not None and tempo_celula_desligada_min < 1:
            return {"erro": "O tempo da célula desligada deve ser pelo menos 1 minuto."}

        # Monta detalhes do ajuste
        plate = plates[0]
        params = plate.params or {}
        ajustes = []

        if celula_ligada is not None:
            estado_novo = "Ligar" if celula_ligada else "Desligar"
            cell_en = params.get("cell_en")
            estado_atual = "ligada" if cell_en in (1, True, "1") else "desligada" if cell_en is not None else None
            ajuste = f"{estado_novo} célula de ozônio"
            if estado_atual:
                ajuste += f" (atualmente {estado_atual})"
            ajustes.append(ajuste)

        if secador_ligado is not None:
            estado_novo = "Ligar" if secador_ligado else "Desligar"
            dryer_en = params.get("dryer_en")
            estado_atual = "ligado" if dryer_en in (1, True, "1") else "desligado" if dryer_en is not None else None
            ajuste = f"{estado_novo} secador"
            if estado_atual:
                ajuste += f" (atualmente {estado_atual})"
            ajustes.append(ajuste)

        if temperatura_secador is not None:
            ajuste = f"Temperatura do secador: {temperatura_secador}°C"
            if params.get("dryer_temp") is not None:
                ajuste += f" (atualmente {params['dryer_temp']}°C)"
            ajustes.append(ajuste)

        if tempo_celula_ligada_min is not None:
            ajuste = f"Tempo célula ligada: {tempo_celula_ligada_min} min"
            if params.get("cell_horas_on") is not None:
                ajuste += f" (atualmente {params['cell_horas_on']} min)"
            ajustes.append(ajuste)

        if tempo_celula_desligada_min is not None:
            ajuste = f"Tempo célula desligada: {tempo_celula_desligada_min} min"
            if params.get("cell_min_off") is not None:
                ajuste += f" (atualmente {params['cell_min_off']} min)"
            ajustes.append(ajuste)

        return {
            "acao": "ajustar_oz1",
            "granja": farm.name,
            "celula_ligada": celula_ligada,
            "secador_ligado": secador_ligado,
            "temperatura_secador": temperatura_secador,
            "tempo_celula_ligada_min": tempo_celula_ligada_min,
            "tempo_celula_desligada_min": tempo_celula_desligada_min,
            "ajustes": ajustes,
            "requer_confirmacao": True,
            "mensagem": f"Confirma ajuste na máquina de ozônio em {farm.name}?\n" + "\n".join(f"- {a}" for a in ajustes),
        }

    except Exception as e:
        log.error("Erro ao ajustar OZ1", error=str(e))
        return {"erro": str(e)}


# =============================================================================
# 6. ALARMES
# =============================================================================


@_require_write
def controlar_alarme_galpao(granja: str, acao: str, galpao: str = None) -> dict:
    """
    Habilita ou desabilita alarmes de um galpão.

    Args:
        granja: Nome da granja
        acao: habilitar ou desabilitar
        galpao: Nome do galpão (opcional, se não informado aplica a todos)

    Returns:
        Confirmação da ação
    """
    try:
        farm = _resolve_farm_acl(granja)
        if not farm:
            return {"erro": f"Granja '{granja}' não encontrada"}

        return {
            "acao": f"{acao}_alarme",
            "granja": farm.name,
            "galpao": galpao or "todos",
            "requer_confirmacao": True,
            "mensagem": f"Confirma {acao} alarmes em {farm.name}?",
        }

    except Exception as e:
        log.error("Erro ao controlar alarme", error=str(e))
        return {"erro": str(e)}


# =============================================================================
# 7. NAVEGAÇÃO E AJUDA
# =============================================================================


def mostrar_menu_principal() -> dict:
    """
    Mostra o menu principal de opções ao usuário.

    Returns:
        Sinalização para mostrar menu
    """
    return {
        "acao": "mostrar_menu",
        "mensagem": "Mostrando menu principal",
    }


def resetar_conversa() -> dict:
    """
    Recomeça a conversa do zero: descarta o histórico do chat e o cache de
    ferramentas. Use quando o usuário pedir explicitamente pra recomeçar,
    voltar ao início, esquecer tudo, limpar a conversa, começar de novo, etc.

    Após chamar esta tool, responda APENAS com a mensagem retornada (curta,
    acolhedora). NÃO chame nenhuma outra tool no mesmo turno.

    Returns:
        Mensagem pronta pro usuário.
    """
    ctx = get_user_context()
    if ctx is not None:
        ctx.reset_requested = True
    return {
        "acao": "reset_conversa",
        "mensagem": "Beleza, então vamos do começo. Me diz o que você precisa.",
    }


def mostrar_ajuda() -> dict:
    """
    Mostra guia de ajuda com todas as funcionalidades disponíveis.

    Returns:
        Texto de ajuda
    """
    ctx = get_user_context()
    is_urban = ctx and ctx.is_urban

    if is_urban:
        return {
            "acao": "mostrar_ajuda",
            "tipo": "urbano",
            "funcionalidades": [
                "Consulta de nível de gás",
                "Relatório de consumo de gás",
                "Relatório de abastecimento",
                "Gráfico de consumo",
                "Alertas de gás baixo",
            ],
        }
    else:
        return {
            "acao": "mostrar_ajuda",
            "tipo": "completo",
            "funcionalidades": [
                "Tempo real (pH, ORP, temperatura, gás, nível, fluxo)",
                "Análise de água",
                "Equipamentos online/offline",
                "Falta de insumos (ácido, cloro, gás)",
                "Parâmetros fora da faixa",
                "Ajuste de parâmetros",
                "Controle de dosadoras",
                "Alarmes de galpão",
                "Gráficos de consumo",
                "Relatórios",
                "Suporte técnico",
            ],
        }


def suporte(acao: str = "solicitar", tipo_equipamento: str = None, topico: str = None, problema: str = None) -> dict:
    """
    Suporte técnico: solicitar atendimento, obter guia ou listar tópicos disponíveis.

    Args:
        acao: solicitar, guia, ou listar_topicos
        tipo_equipamento: Tipo do equipamento (Z1, CCD, PHI, ORP, WGT, FLX, NVL, OZ1)
        topico: Tópico do suporte (calibracao, offline, leitura, dosagem, config)
        problema: Descrição do problema (para acao=solicitar)

    Returns:
        Resultado da ação de suporte
    """
    if acao == "solicitar":
        return {
            "acao": "iniciar_suporte",
            "equipamento": tipo_equipamento,
            "problema": problema,
            "mensagem": "Iniciando atendimento de suporte técnico",
        }

    if acao == "listar_topicos":
        try:
            from monitoring.whatsapp_steps_z1.support_guides_config import (
                SUPPORT_TOPICS,
                PLATE_TYPE_NAMES,
            )
            if not tipo_equipamento:
                return {"tipos_disponiveis": list(PLATE_TYPE_NAMES.keys())}

            tipo_upper = tipo_equipamento.upper()
            topicos = SUPPORT_TOPICS.get(tipo_upper, [])
            if not topicos:
                return {"encontrado": False, "tipos_disponiveis": list(PLATE_TYPE_NAMES.keys())}

            return {
                "encontrado": True,
                "equipamento": PLATE_TYPE_NAMES.get(tipo_upper, tipo_equipamento),
                "topicos": [{"id": t["id"], "nome": t["label"]} for t in topicos],
            }
        except Exception as e:
            return {"erro": str(e)}

    if acao == "guia":
        try:
            from monitoring.whatsapp_steps_z1.support_guides_config import (
                SUPPORT_GUIDES,
                PLATE_TYPE_NAMES,
                GUIDES_PUBLIC_URL,
            )
            if not tipo_equipamento or not topico:
                return {"erro": "tipo_equipamento e topico são obrigatórios para acao=guia"}

            tipo_lower = tipo_equipamento.lower()
            guide_id = f"{tipo_lower}_{topico}"
            guide = SUPPORT_GUIDES.get(guide_id)
            if not guide:
                for alt in [f"{tipo_lower}_outros", f"{tipo_lower}_offline"]:
                    if alt in SUPPORT_GUIDES:
                        guide = SUPPORT_GUIDES[alt]
                        break

            if not guide:
                return {"encontrado": False, "topicos_disponiveis": ["calibracao", "offline", "leitura", "dosagem", "config"]}

            return {
                "encontrado": True,
                "equipamento": PLATE_TYPE_NAMES.get(tipo_equipamento.upper(), tipo_equipamento),
                "topico": topico,
                "texto": guide.get("text", ""),
                "imagem_url": f"{GUIDES_PUBLIC_URL}{guide.get('image', '')}" if guide.get("image") else None,
            }
        except Exception as e:
            return {"erro": str(e)}

    return {"erro": f"Ação '{acao}' não reconhecida. Use: solicitar, guia, listar_topicos"}


# =============================================================================
# 8. GRÁFICOS E RELATÓRIOS
# =============================================================================


def consumo(granja: str, dias: int = 7, formato: str = "dados") -> dict:
    """
    Consulta consumo de ácido, cloro e água de uma granja.

    Args:
        granja: Nome da granja
        dias: Período em dias (default: 7, máximo: 90)
        formato: dados (retorna números) ou grafico (gera imagem e envia ao usuário)

    Returns:
        Dados de consumo ou confirmação de gráfico gerado
    """
    if formato == "grafico":
        try:
            from z1monitoring_agent.utils import commons_actions

            farm = _resolve_farm_acl(granja)
            if not farm:
                return {"erro": f"Granja '{granja}' não encontrada"}

            if dias < 1 or dias > 90:
                return {"erro": "Período deve ser entre 1 e 90 dias"}

            plates = Plate.get_all({"farm_associated": farm.name})
            if not plates:
                return {"erro": f"Nenhum equipamento encontrado em '{farm.name}'"}

            result = commons_actions.handler_graphic_request(farm, plates, dias)
            if not result:
                return {"erro": f"Não foi possível gerar gráficos para '{farm.name}'"}

            ctx = get_user_context()
            if ctx:
                for msg in result:
                    if msg.get("type") in ["image", "image_upload"]:
                        ctx.pending_messages.append(msg)

            qtd = len([m for m in result if m.get("type") in ["image", "image_upload"]])
            return {
                "granja": farm.name,
                "dias": dias,
                "graficos_gerados": qtd,
                "mensagem": f"{qtd} gráfico(s) de consumo gerado(s) para {farm.name} ({dias} dias). As imagens serão enviadas ao usuário.",
            }
        except Exception as e:
            log.error("Erro ao gerar gráfico", error=str(e))
            return {"erro": str(e)}
    try:
        import datetime
        from datetime import timedelta
        from z1monitoring_models.models.choose_event_model import get_events_model

        farm = _resolve_farm_acl(granja)
        if not farm:
            return {"erro": f"Granja '{granja}' não encontrada"}

        if dias < 1 or dias > 90:
            return {"erro": "Período deve ser entre 1 e 90 dias"}

        plates = Plate.get_all({"farm_associated": farm.name})
        if not plates:
            return {"erro": f"Nenhum equipamento encontrado em '{farm.name}'"}

        now = datetime.datetime.now()
        date_upper = now.strftime("%Y-%m-%d %H:%M:%S")
        date_lower = (now - timedelta(days=dias)).strftime("%Y-%m-%d 00:00:00")

        consumption_data = {}

        has_ccd = any(p.plate_type == "CCD" for p in plates)
        has_z1 = any(p.plate_type == "Z1" for p in plates)

        if has_ccd:
            _Event = get_events_model("CCD")
            events = _Event.get_insumes_consumed_last_days(farm.name, date_lower, date_upper)
            for ev in events:
                day_key = ev.created_at.strftime("%Y-%m-%d")
                consumption_data[day_key] = {
                    "acido_kg": float(getattr(ev, "acid_consumed_acc", 0) or 0),
                    "cloro_kg": float(getattr(ev, "chlorine_consumed_acc", 0) or 0),
                    "agua_litros": float(getattr(ev, "water_consumed_acc", 0) or 0),
                }
        elif has_z1:
            _Event = get_events_model("Z1")
            events = _Event.get_insumes_consumed_last_days(farm.name, date_lower, date_upper)
            for ev in events:
                day_key = ev.created_at.strftime("%Y-%m-%d")
                consumption_data[day_key] = {
                    "acido_kg": float(ev.acid_consumed_acc if ev.acid_consumed_acc is not None else 0),
                    "cloro_kg": float(ev.chlorine_consumed_acc if ev.chlorine_consumed_acc is not None else 0),
                }

        # Adiciona água de FLX se não veio do CCD
        flx_serials_associated_with_ccd = set()
        if has_ccd:
            for p in plates:
                if p.plate_type == "CCD":
                    associateds = p.to_dict().get("params", {}).get("associateds_plates", [])
                    for ass in associateds:
                        if ass.startswith("FLX"):
                            flx_serials_associated_with_ccd.add(ass)

        days_with_ccd_water = set()
        for day_key, data in consumption_data.items():
            if data.get("agua_litros", 0) > 0:
                days_with_ccd_water.add(day_key)

        for p in plates:
            if p.plate_type != "FLX":
                continue
            if p.serial in flx_serials_associated_with_ccd:
                continue
            _Event = get_events_model("FLX")
            events = _Event.get_water_consumed_last_days(farm.name, p.serial, date_lower, date_upper)
            for ev in events:
                day_key = ev.created_at.strftime("%Y-%m-%d")
                if day_key in days_with_ccd_water:
                    continue
                if day_key not in consumption_data:
                    consumption_data[day_key] = {}
                current = consumption_data[day_key].get("agua_litros", 0)
                consumption_data[day_key]["agua_litros"] = current + float(ev.water_consumed or 0)

        if not consumption_data:
            return {"erro": f"Sem dados de consumo para '{farm.name}' nos últimos {dias} dias"}

        # Calcula totais
        total_acido = sum(d.get("acido_kg", 0) for d in consumption_data.values())
        total_cloro = sum(d.get("cloro_kg", 0) for d in consumption_data.values())
        total_agua = sum(d.get("agua_litros", 0) for d in consumption_data.values())
        dias_com_dados = len([d for d in consumption_data.values() if any(v > 0 for v in d.values())])

        # Ordena por data (mais recente primeiro), limita a 15 dias no retorno
        sorted_days = sorted(consumption_data.keys(), reverse=True)[:15]
        dados_por_dia = {day: consumption_data[day] for day in sorted_days}

        result = {
            "granja": farm.name,
            "periodo_dias": dias,
            "total_acido_kg": round(total_acido, 2),
            "total_cloro_kg": round(total_cloro, 2),
            "total_agua_litros": round(total_agua, 1),
            "media_diaria_acido_kg": round(total_acido / max(dias_com_dados, 1), 3),
            "media_diaria_cloro_kg": round(total_cloro / max(dias_com_dados, 1), 3),
            "media_diaria_agua_litros": round(total_agua / max(dias_com_dados, 1), 1),
            "dias_com_dados": dias_com_dados,
            "dados_por_dia": dados_por_dia,
        }

        topologia = _get_farm_topology(farm)
        if topologia:
            result["topologia_eta"] = topologia
        _inject_eta_timeline(result, farm)

        return result

    except Exception as e:
        log.error("Erro ao consultar histórico de consumo", error=str(e))
        return {"erro": str(e)}


def analise_consumo_detalhada(granja: str, dias: int = 10, data_inicio: str = None) -> dict:
    """
    Análise detalhada de consumo de uma granja: consumo diário, perfil horário,
    períodos offline do FLX e variações significativas.

    Args:
        granja: Nome da granja
        dias: Período em dias (default: 10, máximo: 30)
        data_inicio: Data de início no formato YYYY-MM-DD (opcional, se informado calcula dias até hoje)

    Returns:
        Dados analíticos completos para interpretação
    """
    try:
        from z1monitoring_models.dbms import Session
        from sqlalchemy import text
        from datetime import date

        farm = _resolve_farm_acl(granja)
        if not farm:
            return {"erro": f"Granja '{granja}' não encontrada"}

        if data_inicio:
            try:
                dt_inicio = date.fromisoformat(data_inicio)
                dias = (date.today() - dt_inicio).days
                if dias < 1:
                    dias = 1
            except ValueError:
                pass

        if dias < 1 or dias > 30:
            dias = min(max(dias, 1), 30)

        result = {"granja": farm.name, "periodo_dias": dias}

        topologia = _get_farm_topology(farm)
        if topologia:
            result["topologia_eta"] = topologia
        _inject_eta_timeline(result, farm)

        with Session() as session:
            # 1. Consumo diário de água (FLX)
            daily_water = session.execute(text("""
                SELECT
                    DATE(created_at) AS dia,
                    COUNT(*) AS total_leituras,
                    ROUND(SUM((readings->>'water_flow')::numeric)) AS consumo_litros,
                    ROUND(AVG((readings->>'water_flow')::numeric), 1) AS media_fluxo_lmin
                FROM events_flx
                WHERE farm ILIKE :farm
                  AND created_at >= NOW() - INTERVAL :dias
                GROUP BY DATE(created_at)
                ORDER BY dia
            """), {"farm": f"%{farm.name}%", "dias": f"{dias} days"}).fetchall()

            result["consumo_agua_diario"] = [
                {"dia": str(r[0]), "leituras": r[1], "litros": float(r[2] or 0), "media_fluxo_lmin": float(r[3] or 0)}
                for r in daily_water
            ]

            # 2. Consumo diário de ácido e cloro (CCD ou Z1)
            plates = Plate.get_all({"farm_associated": farm.name})
            has_ccd = any(p.plate_type == "CCD" for p in plates)
            has_z1 = any(p.plate_type == "Z1" for p in plates)

            if has_ccd:
                # Campos "Consumo X Acumulado" são monotonicamente crescentes
                # (acumulado desde o início da operação). Consumo do dia é
                # delta = MAX(dia) - MIN(dia). SUM() somava todas as leituras
                # do dia, explodindo o valor em ~1000x.
                daily_insumos = session.execute(text("""
                    SELECT
                        DATE(created_at) AS dia,
                        ROUND(GREATEST(
                            MAX((readings->>'Consumo Ácido Acumulado')::numeric)
                            - MIN((readings->>'Consumo Ácido Acumulado')::numeric),
                            0
                        ), 1) AS acido_kg,
                        ROUND(GREATEST(
                            MAX((readings->>'Consumo Cloro Acumulado')::numeric)
                            - MIN((readings->>'Consumo Cloro Acumulado')::numeric),
                            0
                        ), 1) AS cloro_kg
                    FROM events_ccd
                    WHERE farm ILIKE :farm
                      AND created_at >= NOW() - INTERVAL :dias
                    GROUP BY DATE(created_at)
                    ORDER BY dia
                """), {"farm": f"%{farm.name}%", "dias": f"{dias} days"}).fetchall()

                result["consumo_insumos_diario"] = [
                    {"dia": str(r[0]), "acido_kg": float(r[1] or 0), "cloro_kg": float(r[2] or 0)}
                    for r in daily_insumos
                ]
            elif has_z1:
                daily_insumos = session.execute(text("""
                    SELECT
                        DATE(created_at) AS dia,
                        ROUND(GREATEST(
                            MAX((readings->>'acid_consumed_acc')::numeric)
                            - MIN((readings->>'acid_consumed_acc')::numeric),
                            0
                        ), 1) AS acido_kg,
                        ROUND(GREATEST(
                            MAX((readings->>'chlorine_consumed_acc')::numeric)
                            - MIN((readings->>'chlorine_consumed_acc')::numeric),
                            0
                        ), 1) AS cloro_kg
                    FROM events_z1
                    WHERE farm ILIKE :farm
                      AND created_at >= NOW() - INTERVAL :dias
                    GROUP BY DATE(created_at)
                    ORDER BY dia
                """), {"farm": f"%{farm.name}%", "dias": f"{dias} days"}).fetchall()

                result["consumo_insumos_diario"] = [
                    {"dia": str(r[0]), "acido_kg": float(r[1] or 0), "cloro_kg": float(r[2] or 0)}
                    for r in daily_insumos
                ]

            # 3. Perfil horário (divide período ao meio para comparação)
            metade = dias // 2
            hourly = session.execute(text("""
                SELECT
                    CASE WHEN DATE(created_at) < (NOW() - INTERVAL :metade)::date THEN 'primeira_metade' ELSE 'segunda_metade' END AS periodo,
                    EXTRACT(HOUR FROM created_at)::int AS hora,
                    ROUND(AVG((readings->>'water_flow')::numeric), 1) AS media_fluxo
                FROM events_flx
                WHERE farm ILIKE :farm
                  AND created_at >= NOW() - INTERVAL :dias
                GROUP BY periodo, hora
                ORDER BY periodo, hora
            """), {"farm": f"%{farm.name}%", "dias": f"{dias} days", "metade": f"{metade} days"}).fetchall()

            perfil_primeira = {}
            perfil_segunda = {}
            for r in hourly:
                target = perfil_primeira if r[0] == "primeira_metade" else perfil_segunda
                target[int(r[1])] = float(r[2] or 0)

            result["perfil_horario"] = {
                "primeira_metade_periodo": perfil_primeira,
                "segunda_metade_periodo": perfil_segunda,
            }

            # 4. Gaps de comunicação FLX > 15 min
            gaps = session.execute(text("""
                WITH ordered AS (
                    SELECT
                        created_at,
                        LAG(created_at) OVER (ORDER BY created_at) AS prev
                    FROM events_flx
                    WHERE farm ILIKE :farm
                      AND created_at >= NOW() - INTERVAL :dias
                )
                SELECT
                    prev AS offline_inicio,
                    created_at AS online_retorno,
                    ROUND(EXTRACT(EPOCH FROM (created_at - prev)) / 60) AS minutos_offline
                FROM ordered
                WHERE prev IS NOT NULL
                  AND EXTRACT(EPOCH FROM (created_at - prev)) / 60 > 15
                ORDER BY prev DESC
                LIMIT 20
            """), {"farm": f"%{farm.name}%", "dias": f"{dias} days"}).fetchall()

            result["periodos_offline_flx"] = [
                {"inicio": str(r[0]), "retorno": str(r[1]), "minutos": int(r[2])}
                for r in gaps
            ]
            result["total_periodos_offline"] = len(gaps)

            # 5. Detectar variações significativas (queda ou aumento > 40% entre dias consecutivos)
            if result["consumo_agua_diario"]:
                variacoes = []
                dados = result["consumo_agua_diario"]
                for i in range(1, len(dados)):
                    prev_litros = dados[i - 1]["litros"]
                    curr_litros = dados[i]["litros"]
                    if prev_litros > 0:
                        variacao_pct = ((curr_litros - prev_litros) / prev_litros) * 100
                        if abs(variacao_pct) > 40:
                            variacoes.append({
                                "de": dados[i - 1]["dia"],
                                "para": dados[i]["dia"],
                                "litros_antes": prev_litros,
                                "litros_depois": curr_litros,
                                "variacao_pct": round(variacao_pct, 1),
                            })
                result["variacoes_significativas"] = variacoes

        return result

    except Exception as e:
        log.error("Erro na análise detalhada de consumo", error=str(e))
        return {"erro": str(e)}


def relatorio_gas(tipo: str = "consumo", granja: str = None) -> dict:
    """
    Relatório de gás: consumo (nível, consumo médio, autonomia) ou abastecimento (últimos 30 dias).

    Args:
        tipo: consumo ou abastecimento
        granja: Nome da granja (opcional, se não informado mostra todos)

    Returns:
        Dados do relatório
    """
    if tipo == "abastecimento":
        ctx = get_user_context()
        try:
            from monitoring.services.reports import get_relatorio_abastecimento_gas

            farm_name = "TODOS"
            if granja:
                farm = _resolve_farm_acl(granja)
                if not farm:
                    return {"erro": f"Granja '{granja}' não encontrada"}
                farm_name = farm.name

            allowed_farms = None
            if farm_name == "TODOS" and ctx:
                if ctx.is_admin:
                    allowed_farms = None
                else:
                    filters = {"associated": ctx.associated}
                    allowed_farms = Farm.get_all_farm_name(filters)

            result = get_relatorio_abastecimento_gas(farm_name, allowed_farms=allowed_farms)
            return {"mensagem": result}

        except Exception as e:
            log.error("Erro relatório abastecimento gás", error=str(e))
            return {"erro": str(e)}
    ctx = get_user_context()
    try:
        from z1monitoring_models.models.events import get_events_model

        # Busca farms que o usuário tem acesso
        if ctx and ctx.is_admin:
            farms = Farm.get_all_that_associated_allowed_permitted(ctx.associated)
            if not farms:
                farms = Farm.get_all_farms_objs_filtereds({})
        elif ctx:
            farms = Farm.get_all_that_associated_allowed_permitted(ctx.associated)
        else:
            return {"erro": "Contexto de usuário não disponível"}

        if not farms:
            return {"erro": "Nenhum local com monitoramento de gás encontrado."}

        gas_data = []
        for farm in farms:
            filters = {"farm_associated": farm.name, "plate_type": ["WGT"]}
            if ctx and not ctx.is_admin:
                filters["associateds_allowed"] = ctx.associated
            plates = Plate.get_all(filters)
            if not plates:
                continue

            for plate in plates:
                try:
                    _Event = get_events_model(plate.plate_type)
                    gas_restante, consumo_medio, dias_autonomia = _Event.get_autonomy_gas(plate.owner, plate.serial)

                    if gas_restante is not None:
                        capacidade = 0
                        try:
                            iomap = plate.params.get("iomap", {})
                            load1 = iomap.get("load1", {})
                            capacidade = load1.get("capacity", 0)
                        except Exception:
                            pass

                        percentual = 0
                        if capacidade > 0 and gas_restante:
                            percentual = round((float(gas_restante) / capacidade) * 100, 0)

                        gas_data.append({
                            "local": farm.name,
                            "gas_restante_kg": round(float(gas_restante), 1) if gas_restante else 0,
                            "nivel_percentual": int(percentual),
                            "consumo_medio_kg_dia": round(float(consumo_medio), 2) if consumo_medio else 0,
                            "autonomia_dias": int(dias_autonomia) if dias_autonomia else 0,
                        })
                except Exception as e:
                    log.error("Erro dados gás", farm=farm.name, error=str(e))

        if not gas_data:
            return {"erro": "Nenhum dado de consumo de gás encontrado."}

        gas_data.sort(key=lambda x: x["autonomia_dias"])

        alertas = [d for d in gas_data if 0 < d["autonomia_dias"] <= 7]

        return {
            "total_locais": len(gas_data),
            "dados": gas_data,
            "alertas_autonomia_baixa": alertas,
        }

    except Exception as e:
        log.error("Erro relatório consumo gás", error=str(e))
        return {"erro": str(e)}


def ranking_granjas(dias: int = 7) -> dict:
    """
    Obtém ranking de desempenho das granjas.

    Args:
        dias: Período para análise (default: 7)

    Returns:
        Ranking das granjas
    """
    return {
        "acao": "ranking",
        "dias": dias,
        "mensagem": f"Gerando ranking de {dias} dias...",
    }


def panorama_24h(granja: str = None, cliente_primario: str = None) -> dict:
    """
    Obtém panorama das últimas 24 horas: placas online/offline, alarmes,
    consumo de ácido/cloro e leituras atuais de pH/ORP por granja.

    Args:
        granja: Nome da granja (opcional).
        cliente_primario: Nome do cliente primário/empresa (opcional). Se
                informado, cobre todas as granjas vinculadas a esse cliente
                via secondaries.
        Se ambos omitidos, cobre todas as granjas que o usuário tem acesso.
        Em qualquer caso o resultado é limitado a 15 granjas (com observação).

    Returns:
        Dados reais extraídos do banco. NÃO inferir nem inventar valores fora
        deste retorno — se um campo não está presente, é porque não há dado.
    """
    try:
        from z1monitoring_models.models.choose_event_model import get_events_model
        from z1monitoring_models.models.clients_secondary import ClientSecondary
        from z1monitoring_models.models.clients_primary import ClientPrimary

        ctx = get_user_context()
        now = datetime.now()
        inicio = now - timedelta(hours=24)
        truncated = False
        truncated_motivo = None
        cliente_resolvido = None
        ambiguidade = None

        if granja:
            farm = _resolve_farm_acl(granja)
            if not farm:
                return {"erro": f"Granja '{granja}' não encontrada"}
            target_farm_names = [farm.name]
        elif cliente_primario:
            matches = ClientPrimary.get_all_associated_by_name(cliente_primario)
            if not matches:
                return {"erro": f"Cliente primário '{cliente_primario}' não encontrado"}
            pc = matches[0]
            cliente_resolvido = pc.fantasy_name
            if len(matches) > 1:
                ambiguidade = [m.fantasy_name for m in matches[1:6]]
            secondaries = ClientSecondary.get_all({"associateds_allowed": pc.cnpj}) or []
            cliente_farms = []
            for sc in secondaries:
                cliente_farms.extend(
                    Farm.get_all_farms_objs_filtereds({"owner": sc.identification}) or []
                )
            allowed = _get_allowed_farm_names(ctx)
            if allowed is not None:
                allowed_set = set(allowed)
                cliente_farms = [f for f in cliente_farms if f.name in allowed_set]
            if not cliente_farms:
                return {
                    "erro": f"Nenhuma granja acessível para o cliente '{pc.fantasy_name}'",
                    "cliente_primario": pc.fantasy_name,
                }
            if len(cliente_farms) > 15:
                truncated = True
                truncated_motivo = (
                    f"Cliente '{pc.fantasy_name}' tem {len(cliente_farms)} "
                    f"granjas: resultado limitado às 15 primeiras."
                )
                cliente_farms = cliente_farms[:15]
            target_farm_names = [f.name for f in cliente_farms]
        else:
            allowed = _get_allowed_farm_names(ctx)
            if allowed is None:
                farms_all = Farm.get_all_farms_objs_filtereds({})
                target_farm_names = [f.name for f in farms_all[:15]]
                if len(farms_all) > 15:
                    truncated = True
                    truncated_motivo = (
                        "Admin com >15 granjas: resultado limitado às 15 primeiras."
                    )
            else:
                target_farm_names = allowed[:15]
                if len(allowed) > 15:
                    truncated = True
                    truncated_motivo = (
                        f"Usuário tem {len(allowed)} granjas: resultado limitado às 15 primeiras."
                    )

        if not target_farm_names:
            return {"erro": "Nenhuma granja acessível no escopo do usuário"}

        alarmes_recentes = UrgentAlarm.get_recent(inicio) or []
        alarmes_do_escopo = [a for a in alarmes_recentes if a.farm in target_farm_names]
        plate_map = _plates_by_serials([getattr(a, "serial", None) for a in alarmes_do_escopo])
        alarmes_por_farm = {}
        for a in alarmes_do_escopo:
            enriched = _enrich_alarm(a, plate_map)
            alarmes_por_farm.setdefault(a.farm, []).append({
                k: v for k, v in enriched.items() if k != "granja"
            })

        date_upper = now.strftime("%Y-%m-%d %H:%M:%S")
        date_lower = inicio.strftime("%Y-%m-%d %H:%M:%S")

        granjas_resumo = []
        for farm_name in target_farm_names:
            plates = Plate.get_all({"farm_associated": farm_name})
            if not plates:
                continue

            online = sum(1 for p in plates if p.have_communication)
            offline = len(plates) - online
            tipos = sorted({p.plate_type for p in plates})

            farm_data = {
                "granja": farm_name,
                "placas_total": len(plates),
                "placas_online": online,
                "placas_offline": offline,
                "tipos_equipamento": tipos,
                "alarmes_24h": len(alarmes_por_farm.get(farm_name, [])),
            }

            offline_list = [
                {"serial": p.serial, "tipo": p.plate_type}
                for p in plates if not p.have_communication
            ]
            if offline_list:
                farm_data["placas_offline_detalhe"] = offline_list[:10]

            for p in plates:
                if p.plate_type not in ["Z1", "PHI", "ORP", "CCD"]:
                    continue
                sv = p.sensors_value or {}
                if "ph" not in farm_data:
                    ph = sv.get("ph") or sv.get("PH")
                    if ph is not None:
                        farm_data["ph"] = ph
                if "orp" not in farm_data:
                    orp = sv.get("orp") or sv.get("ORP")
                    if orp is not None:
                        farm_data["orp"] = orp
                if "temperatura" not in farm_data:
                    temp = sv.get("temperature") or sv.get("Temperatura da Água")
                    if temp is not None:
                        farm_data["temperatura"] = temp

            has_ccd = any(p.plate_type == "CCD" for p in plates)
            has_z1 = any(p.plate_type == "Z1" for p in plates)
            if has_ccd or has_z1:
                try:
                    _Event = get_events_model("CCD" if has_ccd else "Z1")
                    events = _Event.get_insumes_consumed_last_days(farm_name, date_lower, date_upper) or []
                    acido = sum(float(getattr(e, "acid_consumed_acc", 0) or 0) for e in events)
                    cloro = sum(float(getattr(e, "chlorine_consumed_acc", 0) or 0) for e in events)
                    if acido > 0:
                        farm_data["acido_kg_24h"] = round(acido, 2)
                    if cloro > 0:
                        farm_data["cloro_kg_24h"] = round(cloro, 2)
                except Exception as e:
                    log.warning("Falha ao calcular consumo 24h", farm=farm_name, error=str(e))

            if alarmes_por_farm.get(farm_name):
                farm_data["alarmes_detalhe"] = alarmes_por_farm[farm_name][:5]

            granjas_resumo.append(farm_data)

        if not granjas_resumo:
            return {
                "periodo": "ultimas_24h",
                "granjas_analisadas": 0,
                "mensagem": "Nenhuma placa encontrada nas granjas do escopo do usuário.",
            }

        total_placas = sum(f["placas_total"] for f in granjas_resumo)
        total_online = sum(f["placas_online"] for f in granjas_resumo)
        total_offline = sum(f["placas_offline"] for f in granjas_resumo)
        total_alarmes = sum(f["alarmes_24h"] for f in granjas_resumo)

        result = {
            "periodo": "ultimas_24h",
            "gerado_em": now.strftime("%d/%m/%Y %H:%M"),
            "granjas_analisadas": len(granjas_resumo),
            "placas_total": total_placas,
            "placas_online": total_online,
            "placas_offline": total_offline,
            "alarmes_24h_total": total_alarmes,
            "granjas": granjas_resumo,
        }
        if granja:
            result["granja_filtrada"] = target_farm_names[0]
        if cliente_resolvido:
            result["cliente_primario"] = cliente_resolvido
        if ambiguidade:
            result["outros_clientes_compativeis"] = ambiguidade
        if truncated and truncated_motivo:
            result["observacao"] = truncated_motivo
        return result

    except Exception as e:
        log.error("Erro ao gerar panorama", error=str(e))
        return {"erro": str(e)}


# =============================================================================
# 9. CONTROLE DE SAÍDAS
# =============================================================================


@_require_write
def controlar_saida(granja: str, saida: str, acao: str) -> dict:
    """
    Liga ou desliga uma saída (bomba, válvula, motor, etc).

    Args:
        granja: Nome da granja
        saida: Nome da saída (bomba, valvula, motor, ventilador, etc)
        acao: ligar ou desligar

    Returns:
        Confirmação da ação solicitada
    """
    try:
        farm = _resolve_farm_acl(granja)
        if not farm:
            return {"erro": f"Granja '{granja}' não encontrada"}

        return {
            "acao": f"{acao}_saida",
            "granja": farm.name,
            "saida": saida,
            "requer_confirmacao": True,
            "mensagem": f"Confirma {acao.upper()} {saida} em {farm.name}?",
        }

    except Exception as e:
        log.error("Erro ao controlar saída", error=str(e))
        return {"erro": str(e)}


def consultar_quadros_com_problema() -> dict:
    """
    Lista quadros de comando com problemas (alarmes ativos).

    Returns:
        Lista de quadros com problema
    """
    ctx = get_user_context()
    try:
        filters = {
            "plate_type": ["QP7", "QP4", "QBT", "QBT_CIS", "IOX"],
            "have_problem": "true",
        }
        if ctx and not ctx.is_admin:
            filters["associateds_allowed"] = ctx.associated

        plates = Plate.get_all(filters)

        if not plates:
            return {"total": 0, "mensagem": "Nenhum quadro com problema no momento"}

        quadros = []
        for plate in plates[:20]:
            quadros.append(
                {
                    "serial": plate.serial,
                    "tipo": plate.plate_type,
                    "granja": plate.farm_associated,
                    "problema": plate.sensors_value.get("problem_description") if plate.sensors_value else None,
                }
            )

        return {
            "total": len(plates),
            "mostrando": len(quadros),
            "quadros": quadros,
        }

    except Exception as e:
        log.error("Erro ao consultar quadros", error=str(e))
        return {"erro": str(e)}


# =============================================================================
# 10. LOTES
# =============================================================================


@_require_write
def controlar_lote(granja: str, acao: str, galpao: str = None) -> dict:
    """
    Inicia ou finaliza um lote em uma granja/galpão.

    Args:
        granja: Nome da granja
        acao: iniciar ou finalizar
        galpao: Nome do galpão (opcional)

    Returns:
        Solicitação de controle de lote
    """
    try:
        farm = _resolve_farm_acl(granja)
        if not farm:
            return {"erro": f"Granja '{granja}' não encontrada"}

        label = "início" if acao == "iniciar" else "fim"
        return {
            "acao": f"{acao}_lote",
            "granja": farm.name,
            "galpao": galpao,
            "requer_confirmacao": True,
            "mensagem": f"Confirma {label} de lote em {farm.name}" + (f" - {galpao}" if galpao else "") + "?",
        }

    except Exception as e:
        log.error("Erro ao controlar lote", error=str(e))
        return {"erro": str(e)}


# =============================================================================
# 11. REGISTRO DE VISITA
# =============================================================================


@_require_write
def registrar_visita(granja: str, motivo: str = None, observacoes: str = None) -> dict:
    """
    Registra uma visita técnica em uma granja.

    Args:
        granja: Nome da granja visitada
        motivo: Motivo da visita (opcional)
        observacoes: Observações da visita (opcional)

    Returns:
        Solicitação de registro de visita
    """
    try:
        farm = _resolve_farm_acl(granja)
        if not farm:
            return {"erro": f"Granja '{granja}' não encontrada"}

        ctx = get_user_context()
        visitante = ctx.user.name if ctx and ctx.user else "Não identificado"

        return {
            "acao": "registrar_visita",
            "granja": farm.name,
            "visitante": visitante,
            "motivo": motivo,
            "observacoes": observacoes,
            "requer_confirmacao": True,
            "mensagem": f"Registrar visita em {farm.name}?",
        }

    except Exception as e:
        log.error("Erro ao registrar visita", error=str(e))
        return {"erro": str(e)}


# =============================================================================
# 10. DIMENSIONAMENTO ETA
# =============================================================================


def dimensionar_eta(
    consumo_diario_litros: float,
    ferro: float = 0,
    manganes: float = 0,
    ph: float = 7.0,
    turbidez: float = 0,
    cor: float = 0,
    dqo: float = 0,
    sulfeto: float = 0,
    dureza: float = 0,
    alcalinidade: float = 0,
    solidos_totais: float = 0,
    coliformes_totais: float = 0,
    e_coli: float = 0,
    cliente: str = "",
    local: str = "",
) -> dict:
    """Dimensiona ETA com ozônio e gera PDF."""
    ctx = get_user_context()

    try:
        params = {
            "consumo_diario_litros": consumo_diario_litros,
            "ferro": ferro,
            "manganes": manganes,
            "ph": ph,
            "turbidez": turbidez,
            "cor": cor,
            "dqo": dqo,
            "sulfeto": sulfeto,
            "dureza": dureza or None,
            "alcalinidade": alcalinidade or None,
            "solidos_totais": solidos_totais or None,
            "coliformes_totais": coliformes_totais or None,
            "e_coli": e_coli or None,
            "cliente": cliente,
            "local": local,
        }

        result = calculate_eta(params)
        pdf_path = generate_pdf(result)

        # Upload para DigitalOcean Spaces
        fname = os.path.basename(pdf_path)
        spaces_upload(fname, pdf_path)
        url = f"https://p4audiopublic.nyc3.digitaloceanspaces.com/app/{fname}"

        # Enviar PDF via WhatsApp
        if ctx:
            ctx.pending_messages.append({
                "type": "document",
                "msg": "Dimensionamento de ETA",
                "url": url,
            })

        # Limpar arquivo temporário
        try:
            os.remove(pdf_path)
        except Exception:
            pass

        log.info("dimensionar_eta: PDF gerado e enviado", url=url, cliente=cliente)

        return {
            "acao": "dimensionamento_gerado",
            "ozonio_gh": result["ozonio_gh"],
            "ph_min": result["ph_min"],
            "ph_max": result["ph_max"],
            "orp_min": result["orp_min"],
            "orp_max": result["orp_max"],
            "volume_tanque_l": result["volume_tanque_contato_l"],
            "url_pdf": url,
        }

    except Exception as e:
        log.error("Erro no dimensionamento ETA", error=str(e))
        return {"erro": str(e)}


# =============================================================================
# 11. NOTIFICAÇÃO INTERMEDIÁRIA
# =============================================================================


def notificar_usuario(mensagem: str) -> dict:
    """
    Envia uma mensagem intermediária ao usuário antes de processamento pesado.
    Use para avisar que uma análise vai demorar.
    A mensagem é enviada imediatamente (não espera o fim do processamento).
    """
    ctx = get_user_context()
    if ctx and ctx.send_immediate_fn:
        try:
            ctx.send_immediate_fn(mensagem)
            log.info("notificar_usuario: mensagem imediata enviada", mensagem=mensagem[:50])
        except Exception as e:
            log.warning("notificar_usuario: erro ao enviar", error=str(e))
    return {"acao": "notificacao_enviada", "mensagem": mensagem}


def enviar_botoes_confirmacao(mensagem: str, botoes: list) -> dict:
    """
    Envia botões interativos ao usuário via WhatsApp para confirmação.
    Use SEMPRE que precisar de uma resposta sim/não ou escolha do usuário.
    Máximo 3 botões, cada título com no máximo 20 caracteres.
    """
    ctx = get_user_context()
    if not ctx:
        return {"erro": "Contexto do usuário não disponível"}

    buttons_formatted = []
    for i, botao in enumerate(botoes):
        if isinstance(botao, str):
            buttons_formatted.append({"id": botao.lower().replace(" ", "_"), "title": botao[:20]})
        elif isinstance(botao, dict):
            buttons_formatted.append({
                "id": botao.get("id", f"btn_{i}"),
                "title": botao.get("title", "")[:20],
            })

    ctx.pending_messages.append({
        "type": "buttons",
        "msg": mensagem,
        "buttons": buttons_formatted,
    })

    log.info("enviar_botoes_confirmacao: botões enfileirados", mensagem=mensagem[:50], botoes=buttons_formatted)
    return {"acao": "botoes_enviados", "mensagem": mensagem, "botoes": buttons_formatted}


# =============================================================================
# 11b. EXECUÇÃO DE AJUSTES (após confirmação do usuário)
# =============================================================================


@_require_write
def confirmar_ajuste_parametro(
    granja: str,
    ph_min: float = None,
    ph_max: float = None,
    orp_min: float = None,
    orp_max: float = None,
    modo_acido: str = None,
    modo_cloro: str = None,
    habilitar_acido: bool = None,
    habilitar_cloro: bool = None,
    limite_acido_24h: float = None,
    limite_cloro_24h: float = None,
    liberar_abs_acido: bool = None,
    liberar_abs_cloro: bool = None,
    ativar_abs_acido: bool = None,
    ativar_abs_cloro: bool = None,
) -> dict:
    """
    Executa o ajuste de parâmetros na placa CCD após confirmação do usuário.
    Só chame esta função DEPOIS que o usuário confirmar a ação.
    """
    ctx = get_user_context()
    if not ctx:
        return {"erro": "Contexto do usuário não disponível"}

    # ETA_READONLY: não executa ações de escrita
    if ctx.permission_name == "ETA_READONLY":
        return {
            "bloqueado": True,
            "mensagem": "Sua permissão não permite realizar alterações. Todo o fluxo foi executado para que você possa acompanhar o funcionamento real do sistema.",
        }

    try:
        farm = _resolve_farm_acl(granja)
        if not farm:
            return {"erro": f"Granja '{granja}' não encontrada"}

        # Busca placa CCD da granja
        plates = Plate.get_all({"farm_id": farm.id, "plate_type": ["CCD"]})
        if not plates:
            return {"erro": f"{farm.name} não possui central de dosagem (CCD)"}

        ccd_plate = plates[0] if isinstance(plates, list) else plates
        is_online = PlateState.is_online(ccd_plate.serial)

        params_to_merge = {}
        extra_params_to_merge = {
            "action_pending": "settings_update",
        }
        if ctx.msisdn:
            extra_params_to_merge["notification_msisdn"] = ctx.msisdn
        if ctx.channel:
            extra_params_to_merge["notification_channel"] = ctx.channel

        change_base = {
            "local": farm.name,
            "serial": ccd_plate.serial,
            "user": ctx.user.name if ctx.user else "agent",
            "plate_type": "CCD",
            "channel": "whatsapp",
            "old_value": "-",
            "result": "success",
        }

        alteracoes = []

        # pH
        if ph_min is not None:
            params_to_merge["ph_inf"] = ph_min
            ChangesRequests({**change_base, "parameter": "ph_inf", "value": str(ph_min)})
        if ph_max is not None:
            params_to_merge["ph_sup"] = ph_max
            ChangesRequests({**change_base, "parameter": "ph_sup", "value": str(ph_max)})
        if ph_min is not None and ph_max is not None:
            alteracoes.append(f"pH: {ph_min} - {ph_max}")

        # ORP
        if orp_min is not None:
            params_to_merge["orp_inf"] = orp_min
            ChangesRequests({**change_base, "parameter": "orp_inf", "value": str(orp_min)})
        if orp_max is not None:
            params_to_merge["orp_sup"] = orp_max
            ChangesRequests({**change_base, "parameter": "orp_sup", "value": str(orp_max)})
        if orp_min is not None and orp_max is not None:
            alteracoes.append(f"ORP: {orp_min} - {orp_max} mV")

        # Modo ácido
        if modo_acido is not None:
            params_to_merge["acid_mode"] = modo_acido
            ChangesRequests({**change_base, "parameter": "acid_mode", "value": str(modo_acido)})
            modo_leg = "automático" if modo_acido == "auto" else "cíclico"
            alteracoes.append(f"Modo ácido: {modo_leg}")

        # Modo cloro
        if modo_cloro is not None:
            params_to_merge["cloro_mode"] = modo_cloro
            ChangesRequests({**change_base, "parameter": "cloro_mode", "value": str(modo_cloro)})
            modo_leg = "automático" if modo_cloro == "auto" else "cíclico"
            alteracoes.append(f"Modo cloro: {modo_leg}")

        # Habilitar/desabilitar ácido
        if habilitar_acido is not None:
            acid_en = "enable" if habilitar_acido else "disable"
            params_to_merge["acid_en"] = acid_en
            ChangesRequests({**change_base, "parameter": "acid_en", "value": "habilita" if habilitar_acido else "desabilita"})
            alteracoes.append(f"Dosadora ácido: {'Ligada' if habilitar_acido else 'Desligada'}")

        # Habilitar/desabilitar cloro
        if habilitar_cloro is not None:
            cloro_en = "enable" if habilitar_cloro else "disable"
            params_to_merge["cloro_en"] = cloro_en
            ChangesRequests({**change_base, "parameter": "cloro_en", "value": "habilita" if habilitar_cloro else "desabilita"})
            alteracoes.append(f"Dosadora cloro: {'Ligada' if habilitar_cloro else 'Desligada'}")

        # Limites 24h
        if limite_acido_24h is not None:
            params_to_merge["acid_max_24h"] = limite_acido_24h
            ChangesRequests({**change_base, "parameter": "acid_max_24h", "value": str(limite_acido_24h)})
            alteracoes.append(f"Limite ácido 24h: {limite_acido_24h} kg")

        if limite_cloro_24h is not None:
            params_to_merge["chlorine_max_24h"] = limite_cloro_24h
            ChangesRequests({**change_base, "parameter": "chlorine_max_24h", "value": str(limite_cloro_24h)})
            alteracoes.append(f"Limite cloro 24h: {limite_cloro_24h} kg")

        # ABS liberação
        if liberar_abs_acido:
            params_to_merge["abs_acid"] = 0
            ChangesRequests({**change_base, "parameter": "abs_acid", "value": "liberado"})
            alteracoes.append("ABS ácido: liberado")

        if liberar_abs_cloro:
            params_to_merge["abs_chlorine"] = 0
            ChangesRequests({**change_base, "parameter": "abs_chlorine", "value": "liberado"})
            alteracoes.append("ABS cloro: liberado")

        # ABS ativação
        if ativar_abs_acido:
            params_to_merge["abs_acid"] = 1
            ChangesRequests({**change_base, "parameter": "abs_acid", "value": "ativado"})
            alteracoes.append("ABS ácido: ativado")

        if ativar_abs_cloro:
            params_to_merge["abs_chlorine"] = 1
            ChangesRequests({**change_base, "parameter": "abs_chlorine", "value": "ativado"})
            alteracoes.append("ABS cloro: ativado")

        # Atualiza placas associadas (PHI e ORP)
        associated_plates = ccd_plate.params.get("associateds_plates", []) if ccd_plate.params else []
        for associated_serial in associated_plates:
            if associated_serial.startswith("PHI") and ph_min is not None and ph_max is not None:
                Plate.update_params_merge(
                    associated_serial, {"sensors_ranges": {"max_ph": float(ph_max), "min_ph": float(ph_min)}}
                )
                log.info("Atualizado sensor PHI", serial=associated_serial, min_ph=ph_min, max_ph=ph_max)

            elif associated_serial.startswith("ORP") and orp_min is not None and orp_max is not None:
                Plate.update_params_merge(
                    associated_serial, {"sensors_ranges": {"max_orp": float(orp_max), "min_orp": float(orp_min)}}
                )
                log.info("Atualizado sensor ORP", serial=associated_serial, min_orp=orp_min, max_orp=orp_max)

        # Grava na placa CCD
        Plate.update_fields_atomic(
            ccd_plate.serial, params_merge=params_to_merge, extra_params_merge=extra_params_to_merge
        )

        log.info("confirmar_ajuste_parametro: sucesso", serial=ccd_plate.serial, farm=farm.name, alteracoes=alteracoes)

        if is_online:
            status_msg = "Solicitação recebida. Aguardando confirmação do equipamento..."
        else:
            status_msg = "Solicitação recebida, porém o equipamento está OFFLINE. Os parâmetros serão aplicados automaticamente quando o equipamento voltar a se comunicar."

        return {
            "sucesso": True,
            "granja": farm.name,
            "serial": ccd_plate.serial,
            "status_equipamento": status_msg,
            "alteracoes": alteracoes,
            "instrucao_resposta": "IMPORTANTE: NÃO diga que a alteração foi aplicada. Informe que a SOLICITAÇÃO foi recebida e está aguardando confirmação do equipamento.",
        }

    except Exception as e:
        log.error("confirmar_ajuste_parametro: erro", error=str(e))
        return {"erro": str(e)}


# =============================================================================
# 12. ANÁLISE DE PERÍODOS OFFLINE E RANKING
# =============================================================================


def ranking_offline(dias: int = 30, gap_minutos: int = 15) -> dict:
    """
    Ranking das granjas/placas que ficam mais tempo offline.
    """
    ctx = get_user_context()
    try:
        from z1monitoring_models.models.choose_event_model import get_offline_ranking

        # Buscar granjas do usuário
        if ctx and ctx.is_admin:
            farms = Farm.get_all_farms_obj()
        elif ctx:
            farms = Farm.get_all_farms_objs_filtereds({"owner": ctx.associated})
        else:
            return {"erro": "Contexto de usuario nao disponivel"}

        if not farms:
            return {"erro": "Nenhuma granja encontrada"}

        farm_ids = [f.id for f in farms if f.id]

        rows = get_offline_ranking(farm_ids, dias=dias, gap_minutos=gap_minutos, limit=20)

        if not rows:
            return {
                "dias": dias,
                "mensagem": f"Nenhum periodo offline > {gap_minutos}min encontrado nos ultimos {dias} dias",
            }

        ranking = []
        for r in rows:
            total = r["total_offline_min"]
            if total >= 60:
                duracao = f"{int(total // 60)}h{int(total % 60)}min"
            else:
                duracao = f"{int(total)}min"
            ranking.append({
                "granja": r["farm"],
                "serial": r["serial"],
                "total_gaps": r["total_gaps"],
                "total_offline": duracao,
            })

        return {
            "dias": dias,
            "gap_minimo_min": gap_minutos,
            "total_resultados": len(ranking),
            "ranking": ranking,
        }

    except Exception as e:
        log.error("Erro no ranking offline", error=str(e))
        return {"erro": str(e)}


def saude_empresa(empresa: str, problema: str = "todos", dias_minimo: int = 0) -> dict:
    """
    Verifica a saúde das granjas de uma empresa.
    Identifica: placas offline, sem ácido, sem cloro, pH/ORP fora da faixa, ABS desativados.
    Mostra há quantos dias cada problema está ativo.

    Args:
        empresa: Nome da empresa/cliente primário
        problema: offline, sem_acido, sem_cloro, ph_fora, orp_fora, abs_manual, todos
        dias_minimo: Filtrar apenas problemas com mais de X dias

    Returns:
        Lista de problemas ativos por granja
    """
    ctx = get_user_context()
    try:
        from z1monitoring_models.models.events_last import LastEvent
        from z1monitoring_models.models.critical_history import CriticalHistory
        from z1monitoring_models.models.plates_state import PlateState
        from z1monitoring_models.models.clients_secondary import ClientSecondary
        from z1monitoring_models.models.clients_primary import ClientPrimary
        from datetime import datetime

        now = datetime.now()

        # Buscar granjas da empresa
        matches = ClientPrimary.get_all_associated_by_name(empresa)
        if not matches:
            return {"erro": f"Empresa '{empresa}' nao encontrada"}
        pc = matches[0]
        ambiguidade = (
            [m.fantasy_name for m in matches[1:6]] if len(matches) > 1 else None
        )

        secondaries = ClientSecondary.get_all({"associateds_allowed": pc.cnpj})
        farms = []
        for sc in secondaries:
            sc_farms = Farm.get_all_farms_objs_filtereds({"owner": sc.identification})
            farms.extend(sc_farms)

        if not farms:
            return {"erro": "Nenhuma granja encontrada para esta empresa"}

        resultados = []

        for farm in farms:
            plates = Plate.get_all_plates_filtered(farm_id=farm.id)
            if not plates:
                continue

            for plate in plates:
                problemas_placa = []

                # OFFLINE
                if problema in ("offline", "todos"):
                    state = PlateState.load(plate.serial)
                    if state and not state.have_communication:
                        last = LastEvent.get_last_register(plate.owner, plate.serial)
                        ultimo = (
                            last.get("created_at") if isinstance(last, dict)
                            else getattr(last, "created_at", None) if last
                            else None
                        )
                        dias_off = (now - ultimo).days if ultimo else 999
                        if dias_off >= dias_minimo:
                            problemas_placa.append({
                                "tipo": "offline",
                                "desde": ultimo.strftime("%d/%m/%Y") if ultimo else "desconhecido",
                                "dias": dias_off,
                            })

                # SEM ÁCIDO / SEM CLORO / PH FORA / ORP FORA via CriticalHistory
                if problema in ("sem_acido", "sem_cloro", "ph_fora", "orp_fora", "todos"):
                    sensor_map = {
                        "sem_acido": "Ácido",
                        "sem_cloro": "Cloro",
                        "ph_fora": "pH",
                        "orp_fora": "ORP",
                    }
                    sensors_to_check = [problema] if problema != "todos" else list(sensor_map.keys())

                    for prob in sensors_to_check:
                        sensor_name = sensor_map[prob]
                        active = CriticalHistory.get_active_by_serial(plate.serial, sensor_name)
                        if not active:
                            active = CriticalHistory.get_active_by_serial(plate.serial, f"Falha: {sensor_name} fora da faixa")
                        if not active:
                            active = CriticalHistory.get_active_by_serial(plate.serial, f"Falha: PH fora da faixa" if prob == "ph_fora" else "")
                        if active:
                            dias_prob = (now - active.created_at).days
                            if dias_prob >= dias_minimo:
                                problemas_placa.append({
                                    "tipo": prob,
                                    "desde": active.created_at.strftime("%d/%m/%Y"),
                                    "dias": dias_prob,
                                })

                # ABS MANUAL (último reading da CCD)
                if problema in ("abs_manual", "todos") and plate.plate_type == "CCD":
                    sv = plate.sensors_value
                    if sv:
                        abs_items = []
                        if sv.get("ABS Ácido Desarmado Manualmente") == 0:
                            abs_items.append("Ácido")
                        if sv.get("ABS Cloro Desarmado Manualmente") == 0:
                            abs_items.append("Cloro")
                        if abs_items:
                            problemas_placa.append({
                                "tipo": "abs_manual",
                                "descricao": f"ABS liberado: {', '.join(abs_items)}",
                            })

                if problemas_placa:
                    resultados.append({
                        "granja": farm.name,
                        "serial": plate.serial,
                        "tipo_placa": plate.plate_type,
                        "problemas": problemas_placa,
                    })

        if not resultados:
            msg = "Nenhum problema encontrado"
            if dias_minimo:
                msg += f" com mais de {dias_minimo} dias"
            out = {"empresa": pc.fantasy_name, "mensagem": msg}
            if ambiguidade:
                out["outras_empresas_compativeis"] = ambiguidade
            return out

        resultados.sort(key=lambda x: max((p.get("dias", 0) for p in x["problemas"]), default=0), reverse=True)

        out = {
            "empresa": pc.fantasy_name,
            "total_com_problema": len(resultados),
            "filtro": problema,
            "dias_minimo": dias_minimo,
            "resultados": resultados[:30],
        }
        if ambiguidade:
            out["outras_empresas_compativeis"] = ambiguidade
        return out

    except Exception as e:
        log.error("Erro em saude_empresa", error=str(e))
        return {"erro": str(e)}


def consultar_periodos_offline(granja: str, tipo_placa: str = None, dias: int = 30, gap_minutos: int = 15) -> dict:
    """
    Analisa gaps na tabela de eventos para identificar períodos em que a placa ficou offline.

    Args:
        granja: Nome da granja
        tipo_placa: Tipo da placa (FLX, Z1, CCD, etc.). Se não informado, analisa todas.
        dias: Quantos dias para trás analisar (default: 30)
        gap_minutos: Intervalo mínimo sem dados para considerar offline (default: 15 min)

    Returns:
        Lista de períodos offline com duração
    """
    try:
        from z1monitoring_models.models.choose_event_model import get_offline_gaps

        # Buscar granja
        candidates = _get_farm_candidates(granja)
        if not candidates:
            return {"erro": f"Granja '{granja}' nao encontrada"}
        farm = Farm.load(candidates[0])
        if not farm:
            return {"erro": f"Granja '{granja}' nao encontrada"}

        # Buscar placas da granja
        plates = Plate.get_all({"farm_id": farm.id})
        if not plates:
            return {"erro": "Nenhum equipamento encontrado nesta granja"}

        if tipo_placa:
            plates = [p for p in plates if p.plate_type.upper() == tipo_placa.upper()]
            if not plates:
                return {"erro": f"Nenhuma placa tipo {tipo_placa} nesta granja"}

        resultados = []

        for plate in plates:
            try:
                gaps = get_offline_gaps(plate.serial, plate.plate_type, dias=dias, gap_minutos=gap_minutos)
                if not gaps:
                    continue

                periodos = []
                total_offline_min = 0
                for g in gaps:
                    gap = g["gap_minutos"]
                    total_offline_min += gap
                    if gap >= 60:
                        duracao = f"{int(gap // 60)}h{int(gap % 60)}min"
                    else:
                        duracao = f"{int(gap)}min"
                    periodos.append({
                        "inicio": g["offline_inicio"].strftime("%d/%m %H:%M") if g["offline_inicio"] else "",
                        "retorno": g["online_retorno"].strftime("%d/%m %H:%M") if g["online_retorno"] else "",
                        "duracao": duracao,
                    })

                if total_offline_min >= 60:
                    total_str = f"{int(total_offline_min // 60)}h{int(total_offline_min % 60)}min"
                else:
                    total_str = f"{int(total_offline_min)}min"

                resultados.append({
                    "serial": plate.serial,
                    "tipo": plate.plate_type,
                    "total_periodos": len(periodos),
                    "total_offline": total_str,
                    "periodos": periodos[:10],
                })

            except Exception as e:
                log.warning(f"Erro ao analisar offline {plate.serial}: {e}")
                continue

        if not resultados:
            return {
                "granja": farm.name,
                "dias": dias,
                "mensagem": f"Nenhum periodo offline > {gap_minutos}min encontrado nos ultimos {dias} dias",
            }

        return {
            "granja": farm.name,
            "dias": dias,
            "gap_minimo_min": gap_minutos,
            "equipamentos_com_gaps": len(resultados),
            "detalhes": resultados,
        }

    except Exception as e:
        log.error("Erro ao consultar periodos offline", error=str(e))
        return {"erro": str(e)}


def descrever_eta(granja: str) -> dict:
    """
    Retorna a topologia da ETA de uma granja para o agente descrever
    como a estação de tratamento está montada.

    Args:
        granja: Nome da granja
    """
    try:
        farm = _resolve_farm_acl(granja)
        if not farm:
            return {"erro": f"Granja '{granja}' não encontrada"}

        plates = Plate.get_all({"farm_associated": farm.name})
        equipamentos = [
            {"serial": p.serial, "tipo": p.plate_type, "descricao": p.description or ""}
            for p in plates
        ]

        topologia = _get_farm_topology(farm)
        if not topologia:
            return {
                "granja": farm.name,
                "equipamentos": equipamentos,
                "topologia": None,
                "mensagem": f"A granja {farm.name} possui {len(equipamentos)} equipamentos mas não tem topologia da ETA cadastrada. Não é possível descrever o circuito hidráulico.",
            }

        return {
            "granja": farm.name,
            "equipamentos": equipamentos,
            "topologia_eta": topologia,
            "instrucao": (
                "Descreva o caminho da água na ETA em linguagem simples, "
                "seguindo a ordem do circuito. Explique onde cada equipamento está "
                "posicionado e qual sua função. Mencione a recirculação se houver. "
                "Se houver ozônio externo, explique que é uma máquina não gerenciada "
                "pelo sistema Z1. Relate as conexões de insumo (WGT → dosadoras) "
                "e controle (CCD → dosadoras)."
            ),
        }

    except Exception as e:
        log.error("Erro em descrever_eta", error=str(e))
        return {"erro": str(e)}


def validar_flx_vs_ccd(granja: str, dias: int = 14, data_inicio: str = None) -> dict:
    """
    Diagnóstico cruzado de sensores: compara dados de FLX vs CCD para detectar
    problemas de sensor, anomalias de consumo e inconsistências.

    Args:
        granja: Nome da granja
        dias: Período em dias (default: 14, máximo: 30)
        data_inicio: Data de início YYYY-MM-DD (opcional, calcula dias até hoje)
    """
    try:
        from z1monitoring_models.dbms import Session
        from sqlalchemy import text
        from datetime import date
        from statistics import median

        farm = _resolve_farm_acl(granja)
        if not farm:
            return {"erro": f"Granja '{granja}' não encontrada"}

        if data_inicio:
            try:
                dt_inicio = date.fromisoformat(data_inicio)
                dias = (date.today() - dt_inicio).days
                if dias < 1:
                    dias = 1
            except ValueError:
                pass

        if dias < 1 or dias > 30:
            dias = min(max(dias, 1), 30)

        plates = Plate.get_all({"farm_associated": farm.name})
        has_flx = any(p.plate_type == "FLX" for p in plates)
        has_ccd = any(p.plate_type == "CCD" for p in plates)
        has_z1 = any(p.plate_type == "Z1" for p in plates)

        if not has_flx:
            return {"erro": f"Granja '{farm.name}' não possui sensor FLX para diagnóstico"}

        result = {
            "granja": farm.name,
            "periodo_dias": dias,
            "equipamentos": [p.plate_type for p in plates],
            "diagnosticos": [],
            "recomendacoes": [],
        }

        topologia = _get_farm_topology(farm)
        if topologia:
            result["topologia_eta"] = topologia
        _inject_eta_timeline(result, farm)

        with Session() as session:
            # 1. Consumo diário FLX
            flx_diario = session.execute(text("""
                SELECT DATE(created_at) AS dia,
                    ROUND(SUM((readings->>'water_flow')::numeric)) AS litros,
                    ROUND(AVG((readings->>'water_flow')::numeric), 1) AS media_fluxo,
                    ROUND(MAX((readings->>'water_flow')::numeric), 1) AS max_fluxo,
                    COUNT(*) AS leituras
                FROM events_flx
                WHERE farm ILIKE :farm AND created_at >= NOW() - INTERVAL :dias
                GROUP BY DATE(created_at)
                ORDER BY dia
            """), {"farm": f"%{farm.name}%", "dias": f"{dias} days"}).fetchall()

            result["flx_diario"] = [
                {"dia": str(r[0]), "litros": float(r[1] or 0), "media": float(r[2] or 0),
                 "max": float(r[3] or 0), "leituras": r[4]}
                for r in flx_diario
            ]

            # 2. Consumo diário CCD (se existir)
            if has_ccd:
                ccd_diario = session.execute(text("""
                    WITH ultimo_do_dia AS (
                        SELECT DATE(created_at) AS dia,
                            (readings->>'Fluxo de Água')::numeric AS fluxo,
                            (readings->>'Consumo Ácido 24h')::numeric AS acido_24h,
                            (readings->>'Consumo Cloro 24h')::numeric AS cloro_24h,
                            (readings->>'PH')::numeric AS ph,
                            (readings->>'ORP')::numeric AS orp,
                            ROW_NUMBER() OVER (PARTITION BY DATE(created_at) ORDER BY created_at DESC) AS rn
                        FROM events_ccd
                        WHERE farm ILIKE :farm AND created_at >= NOW() - INTERVAL :dias
                    )
                    SELECT dia, ROUND(acido_24h, 2) AS acido_24h, ROUND(cloro_24h, 2) AS cloro_24h,
                           ROUND(ph, 2) AS ph, ROUND(orp, 0) AS orp
                    FROM ultimo_do_dia WHERE rn = 1 ORDER BY dia
                """), {"farm": f"%{farm.name}%", "dias": f"{dias} days"}).fetchall()

                result["ccd_diario"] = [
                    {"dia": str(r[0]), "acido_24h": float(r[1] or 0), "cloro_24h": float(r[2] or 0),
                     "ph": float(r[3] or 0), "orp": float(r[4] or 0)}
                    for r in ccd_diario
                ]

                # Água via CCD (soma diária)
                ccd_agua = session.execute(text("""
                    SELECT DATE(created_at) AS dia,
                        ROUND(SUM(COALESCE((readings->>'Fluxo de Água')::numeric, 0))) AS litros
                    FROM events_ccd
                    WHERE farm ILIKE :farm AND created_at >= NOW() - INTERVAL :dias
                    GROUP BY DATE(created_at)
                    ORDER BY dia
                """), {"farm": f"%{farm.name}%", "dias": f"{dias} days"}).fetchall()

                ccd_agua_map = {str(r[0]): float(r[1] or 0) for r in ccd_agua}

            # 3. Análise: detecção de anomalias
            if len(flx_diario) >= 3:
                litros_list = [float(r[1] or 0) for r in flx_diario]
                max_fluxo_list = [float(r[3] or 0) for r in flx_diario]
                mediana_litros = median(litros_list)

                # Detectar queda brusca
                for i in range(1, len(litros_list)):
                    if litros_list[i-1] > 0:
                        variacao = (litros_list[i] - litros_list[i-1]) / litros_list[i-1] * 100
                        if variacao < -40:
                            result["diagnosticos"].append({
                                "tipo": "queda_brusca_flx",
                                "de": str(flx_diario[i-1][0]),
                                "para": str(flx_diario[i][0]),
                                "litros_antes": litros_list[i-1],
                                "litros_depois": litros_list[i],
                                "variacao_pct": round(variacao, 1),
                            })

                # Detectar teto de sensor (max estável em valor baixo)
                if len(max_fluxo_list) >= 4:
                    primeira_metade = max_fluxo_list[:len(max_fluxo_list)//2]
                    segunda_metade = max_fluxo_list[len(max_fluxo_list)//2:]
                    max_antes = max(primeira_metade) if primeira_metade else 0
                    max_depois = max(segunda_metade) if segunda_metade else 0

                    if max_antes > 0 and max_depois > 0 and max_depois < max_antes * 0.5:
                        result["diagnosticos"].append({
                            "tipo": "teto_sensor_reduzido",
                            "max_antes": max_antes,
                            "max_depois": max_depois,
                            "reducao_pct": round((1 - max_depois/max_antes) * 100, 1),
                            "detalhe": f"Fluxo máximo caiu de {max_antes} para {max_depois} L/min. Possível obstrução ou problema no sensor FLX.",
                        })

            # 4. Cross-validation FLX vs CCD
            if has_ccd and result.get("ccd_diario"):
                flx_map = {d["dia"]: d["litros"] for d in result["flx_diario"]}
                ccd_insumos = {d["dia"]: d for d in result["ccd_diario"]}

                # Pegar dias em comum
                dias_comuns = sorted(set(flx_map.keys()) & set(ccd_insumos.keys()))
                if len(dias_comuns) >= 4:
                    metade = len(dias_comuns) // 2
                    dias_antes = dias_comuns[:metade]
                    dias_depois = dias_comuns[metade:]

                    # Médias antes e depois
                    agua_antes = sum(flx_map[d] for d in dias_antes) / len(dias_antes)
                    agua_depois = sum(flx_map[d] for d in dias_depois) / len(dias_depois)
                    acido_antes = sum(ccd_insumos[d]["acido_24h"] for d in dias_antes) / len(dias_antes)
                    acido_depois = sum(ccd_insumos[d]["acido_24h"] for d in dias_depois) / len(dias_depois)
                    cloro_antes = sum(ccd_insumos[d]["cloro_24h"] for d in dias_antes) / len(dias_antes)
                    cloro_depois = sum(ccd_insumos[d]["cloro_24h"] for d in dias_depois) / len(dias_depois)
                    ph_antes = sum(ccd_insumos[d]["ph"] for d in dias_antes) / len(dias_antes)
                    ph_depois = sum(ccd_insumos[d]["ph"] for d in dias_depois) / len(dias_depois)

                    var_agua = ((agua_depois - agua_antes) / agua_antes * 100) if agua_antes > 0 else 0
                    var_acido = ((acido_depois - acido_antes) / acido_antes * 100) if acido_antes > 0 else 0
                    var_cloro = ((cloro_depois - cloro_antes) / cloro_antes * 100) if cloro_antes > 0 else 0

                    result["comparativo"] = {
                        "periodo_antes": f"{dias_antes[0]} a {dias_antes[-1]}",
                        "periodo_depois": f"{dias_depois[0]} a {dias_depois[-1]}",
                        "agua_media_antes": round(agua_antes, 0),
                        "agua_media_depois": round(agua_depois, 0),
                        "variacao_agua_pct": round(var_agua, 1),
                        "acido_medio_antes": round(acido_antes, 2),
                        "acido_medio_depois": round(acido_depois, 2),
                        "variacao_acido_pct": round(var_acido, 1),
                        "cloro_medio_antes": round(cloro_antes, 2),
                        "cloro_medio_depois": round(cloro_depois, 2),
                        "variacao_cloro_pct": round(var_cloro, 1),
                        "ph_medio_antes": round(ph_antes, 2),
                        "ph_medio_depois": round(ph_depois, 2),
                    }

                    # Diagnóstico de inconsistência
                    agua_caiu_muito = var_agua < -30
                    insumos_estavel = abs(var_acido) < 20 and abs(var_cloro) < 20
                    ph_estavel = abs(ph_depois - ph_antes) < 0.5

                    if agua_caiu_muito and insumos_estavel and ph_estavel:
                        result["diagnosticos"].append({
                            "tipo": "inconsistencia_flx_vs_ccd",
                            "detalhe": (
                                f"Água (FLX) caiu {abs(round(var_agua, 1))}% mas ácido/cloro e pH se mantiveram estáveis. "
                                f"Isso indica problema no sensor FLX, não redução real de consumo. "
                                f"A CCD continua dosando normalmente, confirmando que o fluxo real não mudou."
                            ),
                        })
                        result["recomendacoes"].append(
                            "Visita técnica para verificar sensor FLX: possível obstrução, pá do rotor presa ou sensor com defeito."
                        )
                    elif agua_caiu_muito and not insumos_estavel:
                        result["diagnosticos"].append({
                            "tipo": "reducao_real_confirmada",
                            "detalhe": (
                                f"Água caiu {abs(round(var_agua, 1))}% e insumos também reduziram "
                                f"(ácido {round(var_acido, 1)}%, cloro {round(var_cloro, 1)}%). "
                                f"Indica mudança real no consumo (possível saída de lote, vazio sanitário)."
                            ),
                        })

                    # Cross-validation litros FLX vs CCD
                    if ccd_agua_map:
                        diferencas = []
                        for d in dias_comuns:
                            if flx_map[d] > 0 and d in ccd_agua_map and ccd_agua_map[d] > 0:
                                diff_pct = abs(flx_map[d] - ccd_agua_map[d]) / max(flx_map[d], ccd_agua_map[d]) * 100
                                diferencas.append({"dia": d, "flx": flx_map[d], "ccd": ccd_agua_map[d], "diff_pct": round(diff_pct, 1)})

                        anomalias_cross = [d for d in diferencas if d["diff_pct"] > 30]
                        if anomalias_cross:
                            result["diagnosticos"].append({
                                "tipo": "divergencia_flx_ccd",
                                "dias_divergentes": anomalias_cross[:5],
                                "detalhe": f"{len(anomalias_cross)} dia(s) com divergência >30% entre FLX e CCD.",
                            })

            if not result["diagnosticos"]:
                result["diagnosticos"].append({
                    "tipo": "sem_anomalias",
                    "detalhe": "Nenhuma inconsistência detectada entre os sensores no período analisado."
                })

        return result

    except Exception as e:
        log.error("Erro em validar_flx_vs_ccd", error=str(e))
        return {"erro": str(e)}


# =============================================================================
# DEFINIÇÃO DAS FERRAMENTAS PARA O AGENTE
# =============================================================================


TOOLS_Z1 = [
    # ===== STATUS E CONSULTAS =====
    Tool(
        name="consultar_status",
        description="Consulta status do sistema: alarmes, equipamentos offline/online, falta de insumos, falta de gás, sensores fora da faixa.",
        parameters={
            "type": "object",
            "properties": {
                "tipo": {"type": "string", "enum": ["alarmes", "offline", "online", "falta_insumo", "falta_gas", "fora_faixa"], "description": "Tipo da consulta"},
                "granja": {"type": "string", "description": "Filtrar por granja (opcional)"},
                "filtro": {"type": "string", "description": "Filtro: acido/cloro/todos para insumo, ph/orp/todos para fora_faixa"},
                "dias": {"type": "integer", "description": "Dias para alarmes (default: 1)", "default": 1},
            },
            "required": ["tipo"],
        },
        function=consultar_status,
    ),
    Tool(
        name="status_equipamento",
        description="Consulta status detalhado de um equipamento pelo número serial.",
        parameters={
            "type": "object",
            "properties": {
                "serial": {"type": "string", "description": "Número serial do equipamento"},
            },
            "required": ["serial"],
        },
        function=status_equipamento,
    ),
    # ===== TEMPO REAL =====
    Tool(
        name="tempo_real",
        description="Obtém leitura em tempo real de sensores de uma granja. "
                    "Pode consultar: geral (todos), ph, orp, temperatura, gas, nivel_agua, fluxo_agua, ozonio, dosadora.",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja"},
                "sensor": {
                    "type": "string",
                    "description": "Sensor a consultar: geral, ph, orp, temperatura, gas, nivel_agua, fluxo_agua, ozonio, dosadora (default: geral)",
                    "default": "geral",
                },
            },
            "required": ["granja"],
        },
        function=tempo_real,
    ),
    # ===== ANÁLISES =====
    Tool(
        name="analise",
        description="Faz análise de uma granja. Tipo 'agua': pH, ORP, temperatura com alertas + timeline de eventos. Tipo 'gas': nível, consumo e autonomia. Use 'horas' para ajustar a janela da timeline (default 24, max 168=7 dias).",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja"},
                "tipo": {"type": "string", "description": "agua ou gas (default: agua)", "default": "agua"},
                "horas": {"type": "integer", "description": "Janela da timeline em horas (default 24, max 168). Use 36, 48, 72, etc conforme o pedido.", "default": 24},
            },
            "required": ["granja"],
        },
        function=analise,
    ),
    # ===== GRANJAS =====
    Tool(
        name="buscar_granja",
        description="Busca informações de uma granja pelo nome.",
        parameters={
            "type": "object",
            "properties": {
                "nome": {"type": "string", "description": "Nome ou parte do nome da granja"},
            },
            "required": ["nome"],
        },
        function=buscar_granja,
    ),
    Tool(
        name="listar_granjas_usuario",
        description="Lista todas as granjas do usuário atual.",
        parameters={"type": "object", "properties": {}, "required": []},
        function=listar_granjas_usuario,
    ),
    # ===== CLIENTES PRIMÁRIOS (apenas admin) =====
    Tool(
        name="listar_clientes_primarios",
        description="Lista clientes primários (distribuidoras, integradoras). Pode filtrar por nome. APENAS ADMIN.",
        parameters={
            "type": "object",
            "properties": {
                "nome": {"type": "string", "description": "Nome ou parte do nome para filtrar (opcional)"},
            },
            "required": [],
        },
        function=listar_clientes_primarios,
    ),
    Tool(
        name="buscar_cliente_primario",
        description="Busca informações de um cliente primário pelo nome. APENAS ADMIN.",
        parameters={
            "type": "object",
            "properties": {
                "nome": {"type": "string", "description": "Nome ou parte do nome do cliente primário"},
            },
            "required": ["nome"],
        },
        function=buscar_cliente_primario,
    ),
    Tool(
        name="listar_granjas_cliente_primario",
        description="Lista granjas que pertencem a um cliente primário (ex: 'granjas da Ultragas', 'locais da BRF'). Hierarquia: ClientePrimário -> ClienteSecundário -> Granja. APENAS ADMIN.",
        parameters={
            "type": "object",
            "properties": {
                "nome_cliente": {"type": "string", "description": "Nome do cliente primário (ex: Ultragas, BRF)"},
                "tipo_equipamento": {
                    "type": "string",
                    "enum": ["gas", "agua", "dosagem"],
                    "description": "Filtrar por tipo: gas (balanças), agua (pH/ORP/nível), dosagem (CCD)",
                },
            },
            "required": ["nome_cliente"],
        },
        function=listar_granjas_cliente_primario,
    ),
    Tool(
        name="consultar_falta_gas_cliente_primario",
        description="Lista locais com falta de gás de um cliente primário específico (ex: 'falta de gás da Ultragas'). APENAS ADMIN.",
        parameters={
            "type": "object",
            "properties": {
                "nome_cliente": {"type": "string", "description": "Nome do cliente primário (ex: Ultragas)"},
            },
            "required": ["nome_cliente"],
        },
        function=consultar_falta_gas_cliente_primario,
    ),
    # ===== CONTROLE =====
    Tool(
        name="ajustar_faixa",
        description="Ajusta a faixa de pH ou ORP de uma granja. Requer confirmação do usuário.",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja"},
                "sensor": {"type": "string", "description": "ph ou orp"},
                "valor_min": {"type": "number", "description": "Valor mínimo (ex: 6.5 para pH, 650 para ORP)"},
                "valor_max": {"type": "number", "description": "Valor máximo (ex: 7.5 para pH, 750 para ORP)"},
            },
            "required": ["granja", "sensor", "valor_min", "valor_max"],
        },
        function=ajustar_faixa,
    ),
    Tool(
        name="controlar_dosadora",
        description="Controla dosadora de ácido ou cloro (ligar, desligar, modo automático ou cíclico).",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja"},
                "dosadora": {"type": "string", "enum": ["acido", "cloro"], "description": "Tipo da dosadora"},
                "acao": {
                    "type": "string",
                    "enum": ["ligar", "desligar", "automatico", "ciclico"],
                    "description": "Ação",
                },
            },
            "required": ["granja", "dosadora", "acao"],
        },
        function=controlar_dosadora,
    ),
    Tool(
        name="controlar_abs",
        description="Controla o ABS (freio automático de limite 24h). Liberar = destravar/desbloquear injeção. Rearmar = reativar freio automático.",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja"},
                "dosadora": {"type": "string", "enum": ["acido", "cloro"], "description": "Tipo da dosadora"},
                "acao": {"type": "string", "description": "liberar ou rearmar"},
            },
            "required": ["granja", "dosadora", "acao"],
        },
        function=controlar_abs,
    ),
    Tool(
        name="definir_limite_24h",
        description="Define o limite de consumo em 24h para uma dosadora (ABS).",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja"},
                "dosadora": {"type": "string", "enum": ["acido", "cloro"], "description": "Tipo da dosadora"},
                "limite_kg": {"type": "number", "description": "Limite em kg para 24h (ex: 5.0)"},
            },
            "required": ["granja", "dosadora", "limite_kg"],
        },
        function=definir_limite_24h,
    ),
    Tool(
        name="ajustar_oz1",
        description="Controla a máquina de ozônio (OZ1): ligar/desligar célula de ozônio, ligar/desligar secador, ajustar temperatura do secador, definir tempos de ciclo da célula. Se o usuário informar tempo em horas, converta para minutos antes de chamar. Requer confirmação.",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja"},
                "celula_ligada": {"type": "boolean", "description": "True para ligar, False para desligar a célula de ozônio"},
                "secador_ligado": {"type": "boolean", "description": "True para ligar, False para desligar o secador"},
                "temperatura_secador": {"type": "integer", "description": "Temperatura do secador em °C (20-80)"},
                "tempo_celula_ligada_min": {
                    "type": "integer",
                    "description": "Tempo que a célula fica ligada em MINUTOS. Se o usuário disser horas, converta (ex: 2h = 120min)",
                },
                "tempo_celula_desligada_min": {
                    "type": "integer",
                    "description": "Tempo que a célula fica desligada em MINUTOS. Se o usuário disser horas, converta (ex: 30min = 30)",
                },
            },
            "required": ["granja"],
        },
        function=ajustar_oz1,
    ),
    # ===== ALARMES =====
    Tool(
        name="controlar_alarme_galpao",
        description="Habilita ou desabilita alarmes de um galpão ou de toda a granja.",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja"},
                "acao": {"type": "string", "description": "habilitar ou desabilitar"},
                "galpao": {"type": "string", "description": "Nome do galpão (opcional)"},
            },
            "required": ["granja", "acao"],
        },
        function=controlar_alarme_galpao,
    ),
    # ===== NAVEGAÇÃO =====
    Tool(
        name="mostrar_menu_principal",
        description="Mostra o menu principal de opções. Use quando o usuário pedir ajuda ou não souber o que fazer.",
        parameters={"type": "object", "properties": {}, "required": []},
        function=mostrar_menu_principal,
    ),
    Tool(
        name="resetar_conversa",
        description=(
            "Recomeça a conversa do zero: limpa o histórico do chat e o cache "
            "de ferramentas. Use quando o usuário pedir explicitamente pra "
            "recomeçar/voltar ao início/esquecer tudo/limpar a conversa/começar "
            "de novo/reset. Depois de chamar, responda APENAS com a mensagem "
            "da tool e NÃO chame nenhuma outra tool no mesmo turno."
        ),
        parameters={"type": "object", "properties": {}, "required": []},
        function=resetar_conversa,
    ),
    Tool(
        name="mostrar_ajuda",
        description="Mostra guia completo de todas as funcionalidades disponíveis.",
        parameters={"type": "object", "properties": {}, "required": []},
        function=mostrar_ajuda,
    ),
    Tool(
        name="suporte",
        description="Suporte técnico: solicitar atendimento, obter guia com instruções, ou listar tópicos disponíveis.",
        parameters={
            "type": "object",
            "properties": {
                "acao": {
                    "type": "string",
                    "description": "solicitar, guia, ou listar_topicos (default: solicitar)",
                    "default": "solicitar",
                },
                "tipo_equipamento": {"type": "string", "description": "Tipo do equipamento (Z1, CCD, PHI, ORP, WGT, FLX, NVL, OZ1)"},
                "topico": {"type": "string", "description": "Tópico (calibracao, offline, leitura, dosagem, config)"},
                "problema": {"type": "string", "description": "Descrição do problema (para acao=solicitar)"},
            },
            "required": [],
        },
        function=suporte,
    ),
    # ===== DADOS E RELATÓRIOS =====
    Tool(
        name="consumo",
        description="Consulta consumo de ácido, cloro e água de uma granja. Formato 'dados' retorna números por dia. Formato 'grafico' gera imagem e envia ao usuário.",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja"},
                "dias": {"type": "integer", "description": "Período em dias (default: 7, máximo: 90)", "default": 7},
                "formato": {"type": "string", "description": "dados ou grafico (default: dados)", "default": "dados"},
            },
            "required": ["granja"],
        },
        function=consumo,
    ),
    Tool(
        name="analise_consumo_detalhada",
        description="Análise profunda de consumo: consumo diário de água/ácido/cloro, perfil horário comparativo, períodos offline do FLX, e detecção de variações significativas. Use quando o usuário pedir análise completa, investigação de queda/aumento de consumo, ou diagnóstico de problemas.",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja"},
                "dias": {"type": "integer", "description": "Período em dias (default: 10, máximo: 30)", "default": 10},
                "data_inicio": {"type": "string", "description": "Data de início no formato YYYY-MM-DD. Se informado, calcula os dias até hoje automaticamente. Use quando o usuário especificar uma data de início."},
            },
            "required": ["granja"],
        },
        function=analise_consumo_detalhada,
    ),
    Tool(
        name="descrever_eta",
        description="Descreve como a ETA (estação de tratamento de água) de uma granja está montada: o caminho da água, posição dos sensores, dosadoras, recirculação, ozônio. Use quando o usuário perguntar como a ETA está montada, qual o circuito, ou pedir descrição da instalação.",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja"},
            },
            "required": ["granja"],
        },
        function=descrever_eta,
    ),
    Tool(
        name="validar_flx_vs_ccd",
        description="Valida o sensor de fluxo (FLX) cruzando com dados da central de dosagem (CCD). Compara consumo de água do FLX com dosagem de ácido/cloro, pH e ORP da CCD. Use quando: consumo de água caiu mas dosagem se manteve, suspeita de problema no sensor de fluxo, ou o usuário pede verificação do hidrometro/FLX. Detecta: FLX com obstrução ou teto (max travado), queda de leitura do FLX sem correspondência na CCD, e diferencia problema real de consumo vs defeito no sensor de fluxo.",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja"},
                "dias": {"type": "integer", "description": "Período em dias (default: 14, máximo: 30)", "default": 14},
                "data_inicio": {"type": "string", "description": "Data de início YYYY-MM-DD (opcional, calcula dias até hoje)"},
            },
            "required": ["granja"],
        },
        function=validar_flx_vs_ccd,
    ),
    Tool(
        name="relatorio_gas",
        description="Relatório de gás. Tipo 'consumo': nível, consumo médio e autonomia. Tipo 'abastecimento': abastecimentos dos últimos 30 dias.",
        parameters={
            "type": "object",
            "properties": {
                "tipo": {"type": "string", "description": "consumo ou abastecimento (default: consumo)", "default": "consumo"},
                "granja": {"type": "string", "description": "Nome da granja (opcional)"},
            },
            "required": [],
        },
        function=relatorio_gas,
    ),
    Tool(
        name="ranking_granjas",
        description="Obtém ranking de desempenho das granjas em um período.",
        parameters={
            "type": "object",
            "properties": {
                "dias": {"type": "integer", "description": "Período em dias (default: 7)", "default": 7},
            },
            "required": [],
        },
        function=ranking_granjas,
    ),
    Tool(
        name="panorama_24h",
        description=(
            "Panorama das últimas 24h: placas online/offline, alarmes, consumo de "
            "ácido/cloro e leituras atuais de pH/ORP/temperatura, por granja. "
            "Use os dados EXATOS que vierem no retorno — NÃO invente granjas, "
            "sensores, valores ou alertas que não estejam no payload."
        ),
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja (opcional, filtra a apenas uma)"},
            },
            "required": [],
        },
        function=panorama_24h,
    ),
    # ===== CONTROLE DE SAÍDAS =====
    Tool(
        name="controlar_saida",
        description="Liga ou desliga uma saída física (bomba, válvula, motor, ventilador). Requer confirmação.",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja"},
                "saida": {"type": "string", "description": "Nome da saída (bomba, valvula, motor, etc)"},
                "acao": {"type": "string", "description": "ligar ou desligar"},
            },
            "required": ["granja", "saida", "acao"],
        },
        function=controlar_saida,
    ),
    Tool(
        name="consultar_quadros_com_problema",
        description="Lista quadros de comando com problemas ou alarmes ativos.",
        parameters={"type": "object", "properties": {}, "required": []},
        function=consultar_quadros_com_problema,
    ),
    # ===== LOTES =====
    Tool(
        name="controlar_lote",
        description="Inicia ou finaliza um lote em uma granja/galpão.",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja"},
                "acao": {"type": "string", "description": "iniciar ou finalizar"},
                "galpao": {"type": "string", "description": "Nome do galpão (opcional)"},
            },
            "required": ["granja", "acao"],
        },
        function=controlar_lote,
    ),
    # ===== REGISTRO DE VISITA =====
    Tool(
        name="registrar_visita",
        description="Registra uma visita técnica em uma granja com motivo e observações.",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja visitada"},
                "motivo": {"type": "string", "description": "Motivo da visita (opcional)"},
                "observacoes": {"type": "string", "description": "Observações da visita (opcional)"},
            },
            "required": ["granja"],
        },
        function=registrar_visita,
    ),

    # ===== DIMENSIONAMENTO ETA =====
    Tool(
        name="dimensionar_eta",
        description="Dimensiona uma ETA (Estação de Tratamento de Água) com pré-tratamento por ozônio. "
                    "Use quando o usuário enviar uma análise de água (imagem ou texto) e consumo diário. "
                    "Extrai os parâmetros da análise e calcula: gerador de ozônio (g/h), faixa de pH, faixa de ORP, "
                    "tanque de contato e filtração. Gera e envia um PDF com o dimensionamento.",
        parameters={
            "type": "object",
            "properties": {
                "consumo_diario_litros": {"type": "number", "description": "Consumo diário de água em litros"},
                "ferro": {"type": "number", "description": "Ferro (Fe) em mg/L"},
                "manganes": {"type": "number", "description": "Manganês (Mn) em mg/L"},
                "ph": {"type": "number", "description": "pH da água"},
                "turbidez": {"type": "number", "description": "Turbidez em NTU"},
                "cor": {"type": "number", "description": "Cor aparente em uH"},
                "dqo": {"type": "number", "description": "DQO em mg/L (opcional)"},
                "sulfeto": {"type": "number", "description": "Sulfeto (H2S) em mg/L (opcional)"},
                "dureza": {"type": "number", "description": "Dureza total em mg/L (opcional)"},
                "alcalinidade": {"type": "number", "description": "Alcalinidade em mg/L (opcional)"},
                "solidos_totais": {"type": "number", "description": "Sólidos totais em mg/L (opcional)"},
                "coliformes_totais": {"type": "number", "description": "Coliformes totais NMP/100mL (opcional)"},
                "e_coli": {"type": "number", "description": "E. coli NMP/100mL (opcional)"},
                "cliente": {"type": "string", "description": "Nome do cliente"},
                "local": {"type": "string", "description": "Local da instalação"},
            },
            "required": ["consumo_diario_litros", "ferro", "manganes", "ph"],
        },
        function=dimensionar_eta,
    ),
    # ===== NOTIFICAÇÃO INTERMEDIÁRIA =====
    Tool(
        name="notificar_usuario",
        description="Envia uma mensagem intermediária ao usuário ANTES de executar uma operação pesada. "
                    "Use SEMPRE antes de chamar ranking_offline, consultar_periodos_offline ou ranking_granjas.",
        parameters={
            "type": "object",
            "properties": {
                "mensagem": {"type": "string", "description": "Mensagem para o usuário (ex: 'Analisando dados, isso pode levar um momento...')"},
            },
            "required": ["mensagem"],
        },
        function=notificar_usuario,
    ),
    # ===== RANKING DE OFFLINE =====
    Tool(
        name="ranking_offline",
        description="Ranking das granjas/placas que ficam mais tempo offline. "
                    "Útil para identificar problemas recorrentes de comunicação.",
        parameters={
            "type": "object",
            "properties": {
                "dias": {"type": "integer", "description": "Quantos dias para trás analisar (default: 30)", "default": 30},
                "gap_minutos": {"type": "integer", "description": "Intervalo mínimo sem dados para considerar offline (default: 15)", "default": 15},
            },
            "required": [],
        },
        function=ranking_offline,
    ),
    # ===== SAÚDE DA EMPRESA =====
    Tool(
        name="saude_empresa",
        description="Verifica a saúde das granjas de uma empresa. "
                    "Identifica placas offline, sem ácido, sem cloro, pH/ORP fora da faixa, ABS desativados. "
                    "Mostra há quantos dias cada problema está ativo. "
                    "Use quando perguntarem sobre problemas de uma empresa, quantas granjas com problema, etc.",
        parameters={
            "type": "object",
            "properties": {
                "empresa": {"type": "string", "description": "Nome da empresa/cliente primário"},
                "problema": {
                    "type": "string",
                    "description": "Tipo de problema: offline, sem_acido, sem_cloro, ph_fora, orp_fora, abs_manual, todos",
                    "default": "todos",
                },
                "dias_minimo": {
                    "type": "integer",
                    "description": "Filtrar problemas com mais de X dias (default: 0 = todos)",
                    "default": 0,
                },
            },
            "required": ["empresa"],
        },
        function=saude_empresa,
    ),
    # ===== ANÁLISE DE PERÍODOS OFFLINE =====
    Tool(
        name="consultar_periodos_offline",
        description="Analisa gaps nos registros de eventos para identificar períodos em que uma placa ficou offline (sem comunicar). "
                    "Útil para entender impacto no consumo acumulado e identificar problemas de comunicação.",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja"},
                "tipo_placa": {"type": "string", "description": "Tipo da placa (FLX, Z1, CCD, NVL, etc.). Se não informado, analisa todas."},
                "dias": {"type": "integer", "description": "Quantos dias para trás analisar (default: 30)", "default": 30},
                "gap_minutos": {"type": "integer", "description": "Intervalo mínimo sem dados para considerar offline em minutos (default: 15)", "default": 15},
            },
            "required": ["granja"],
        },
        function=consultar_periodos_offline,
    ),
    # ===== BOTÕES INTERATIVOS =====
    Tool(
        name="enviar_botoes_confirmacao",
        description="Envia botões interativos ao usuário via WhatsApp. Use SEMPRE que precisar de confirmação (sim/não) ou que o usuário escolha entre opções. Máximo 3 botões, título máximo 20 caracteres cada.",
        parameters={
            "type": "object",
            "properties": {
                "mensagem": {"type": "string", "description": "Texto da mensagem que acompanha os botões"},
                "botoes": {
                    "type": "array",
                    "description": "Lista de botões (máx 3). Cada botão pode ser uma string ou objeto {id, title}",
                    "items": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "string", "description": "ID do botão (ex: 'sim', 'nao', 'cancelar')"},
                            "title": {"type": "string", "description": "Texto do botão (máx 20 chars)"},
                        },
                        "required": ["id", "title"],
                    },
                },
            },
            "required": ["mensagem", "botoes"],
        },
        function=enviar_botoes_confirmacao,
    ),
    # ===== EXECUÇÃO DE AJUSTES (após confirmação) =====
    Tool(
        name="confirmar_ajuste_parametro",
        description="Executa o ajuste de parâmetros na placa CCD APÓS o usuário confirmar. "
                    "Só chame quando o usuário responder 'Confirmar' ou 'Sim' aos botões de confirmação. "
                    "Passe os mesmos parâmetros que foram apresentados na confirmação.",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja"},
                "ph_min": {"type": "number", "description": "pH mínimo"},
                "ph_max": {"type": "number", "description": "pH máximo"},
                "orp_min": {"type": "number", "description": "ORP mínimo (mV)"},
                "orp_max": {"type": "number", "description": "ORP máximo (mV)"},
                "modo_acido": {"type": "string", "enum": ["auto", "cy"], "description": "Modo dosadora ácido"},
                "modo_cloro": {"type": "string", "enum": ["auto", "cy"], "description": "Modo dosadora cloro"},
                "habilitar_acido": {"type": "boolean", "description": "Ligar (true) ou desligar (false) dosadora ácido"},
                "habilitar_cloro": {"type": "boolean", "description": "Ligar (true) ou desligar (false) dosadora cloro"},
                "limite_acido_24h": {"type": "number", "description": "Limite consumo ácido 24h (kg)"},
                "limite_cloro_24h": {"type": "number", "description": "Limite consumo cloro 24h (kg)"},
                "liberar_abs_acido": {"type": "boolean", "description": "Liberar ABS ácido"},
                "liberar_abs_cloro": {"type": "boolean", "description": "Liberar ABS cloro"},
                "ativar_abs_acido": {"type": "boolean", "description": "Ativar ABS ácido"},
                "ativar_abs_cloro": {"type": "boolean", "description": "Ativar ABS cloro"},
            },
            "required": ["granja"],
        },
        function=confirmar_ajuste_parametro,
    ),
]
