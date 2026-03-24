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

from z1monitoring_agent.utils.eta_dimensioning import calculate_eta, generate_pdf

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
        self.is_primary = self.permission_name in ["ETA_REPRESENTANTES", "ETA_VENDEDOR", "URBANO_REPRESENTANTES"]
        self.is_urban = self.permission_name == "URBANO_REPRESENTANTES"
        self.pending_messages = []  # Mensagens extras (imagens, docs) para enviar junto com a resposta


# Contexto global (será setado pelo handler)
_current_context: Optional[UserContext] = None


def set_user_context(user, conversation=None):
    """Define o contexto do usuário para as ferramentas."""
    global _current_context
    _current_context = UserContext(user, conversation)


def get_user_context() -> Optional[UserContext]:
    """Obtém o contexto do usuário atual."""
    return _current_context


# =============================================================================
# 1. STATUS E CONSULTAS
# =============================================================================


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
        data_inicio = datetime.now() - timedelta(days=dias)

        if granja:
            alarmes = UrgentAlarm.get_by_farm_and_date(granja, data_inicio)
        else:
            alarmes = UrgentAlarm.get_recent(data_inicio)

        if not alarmes:
            return {
                "encontrados": 0,
                "mensagem": f"Nenhum alarme nos últimos {dias} dia(s)",
                "alarmes": [],
            }

        alarmes_formatados = []
        for alarme in alarmes[:10]:
            alarmes_formatados.append(
                {
                    "granja": alarme.farm,
                    "sensor": alarme.sensor,
                    "status": alarme.status,
                    "atendido": alarme.attended,
                    "data": alarme.created_at.strftime("%d/%m %H:%M") if alarme.created_at else None,
                }
            )

        return {
            "encontrados": len(alarmes),
            "mostrando": len(alarmes_formatados),
            "alarmes": alarmes_formatados,
        }

    except Exception as e:
        log.error("Erro ao consultar alarmes", error=str(e))
        return {"erro": str(e)}


def consultar_equipamentos_offline() -> dict:
    """
    Lista todos os equipamentos que estão offline (sem comunicação).

    Returns:
        Lista de equipamentos offline
    """
    ctx = get_user_context()
    try:
        filters = {"have_communication": False}
        if ctx and not ctx.is_admin:
            filters["associateds_allowed"] = ctx.associated

        plates = Plate.get_all(filters)

        if not plates:
            return {"total": 0, "mensagem": "Nenhum equipamento offline no momento"}

        equipamentos = []
        for plate in plates[:20]:
            equipamentos.append(
                {
                    "serial": plate.serial,
                    "tipo": plate.plate_type,
                    "granja": plate.farm_associated,
                    "ultimo_contato": plate.updated_at.strftime("%d/%m %H:%M") if plate.updated_at else None,
                }
            )

        return {
            "total": len(plates),
            "mostrando": len(equipamentos),
            "equipamentos": equipamentos,
        }

    except Exception as e:
        log.error("Erro ao consultar equipamentos offline", error=str(e))
        return {"erro": str(e)}


def consultar_equipamentos_online() -> dict:
    """
    Lista todos os equipamentos que estão online (comunicando).

    Returns:
        Lista de equipamentos online
    """
    ctx = get_user_context()
    try:
        filters = {"have_communication": True}
        if ctx and not ctx.is_admin:
            filters["associateds_allowed"] = ctx.associated

        plates = Plate.get_all(filters)

        if not plates:
            return {"total": 0, "mensagem": "Nenhum equipamento online"}

        equipamentos = []
        for plate in plates[:20]:
            equipamentos.append(
                {
                    "serial": plate.serial,
                    "tipo": plate.plate_type,
                    "granja": plate.farm_associated,
                }
            )

        return {
            "total": len(plates),
            "mostrando": len(equipamentos),
            "equipamentos": equipamentos,
        }

    except Exception as e:
        log.error("Erro ao consultar equipamentos online", error=str(e))
        return {"erro": str(e)}


def consultar_falta_acido() -> dict:
    """
    Lista equipamentos com falta de ácido.

    Returns:
        Lista de equipamentos sem ácido
    """
    ctx = get_user_context()
    try:
        filters = {"plate_type": ["Z1"], "have_acid": False}
        if ctx and not ctx.is_admin:
            filters["associateds_allowed"] = ctx.associated

        plates = Plate.get_all(filters)

        if not plates:
            return {"total": 0, "mensagem": "Nenhum equipamento com falta de ácido"}

        locais = list(set([p.farm_associated for p in plates if p.farm_associated]))

        return {
            "total": len(plates),
            "locais_afetados": locais,
        }

    except Exception as e:
        log.error("Erro ao consultar falta de ácido", error=str(e))
        return {"erro": str(e)}


def consultar_falta_cloro() -> dict:
    """
    Lista equipamentos com falta de cloro.

    Returns:
        Lista de equipamentos sem cloro
    """
    ctx = get_user_context()
    try:
        filters = {"plate_type": ["Z1"], "have_chlorine": False}
        if ctx and not ctx.is_admin:
            filters["associateds_allowed"] = ctx.associated

        plates = Plate.get_all(filters)

        if not plates:
            return {"total": 0, "mensagem": "Nenhum equipamento com falta de cloro"}

        locais = list(set([p.farm_associated for p in plates if p.farm_associated]))

        return {
            "total": len(plates),
            "locais_afetados": locais,
        }

    except Exception as e:
        log.error("Erro ao consultar falta de cloro", error=str(e))
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


def consultar_ph_fora_faixa() -> dict:
    """
    Lista equipamentos com pH fora da faixa configurada.

    Returns:
        Lista de equipamentos com pH fora da faixa
    """
    ctx = get_user_context()
    try:
        filters = {"plate_type": ["Z1", "PHI"], "out_ph": "true"}
        if ctx and not ctx.is_admin:
            filters["associateds_allowed"] = ctx.associated

        plates = Plate.get_all(filters)

        if not plates:
            return {"total": 0, "mensagem": "Nenhum equipamento com pH fora da faixa"}

        locais = []
        for p in plates:
            ph_atual = p.sensors_value.get("ph", 0) if p.sensors_value else 0
            locais.append(
                {
                    "granja": p.farm_associated,
                    "ph_atual": ph_atual,
                    "serial": p.serial,
                }
            )

        return {
            "total": len(plates),
            "locais": locais,
        }

    except Exception as e:
        log.error("Erro ao consultar pH fora da faixa", error=str(e))
        return {"erro": str(e)}


def consultar_orp_fora_faixa() -> dict:
    """
    Lista equipamentos com ORP fora da faixa configurada.

    Returns:
        Lista de equipamentos com ORP fora da faixa
    """
    ctx = get_user_context()
    try:
        filters = {"plate_type": ["Z1", "ORP"], "out_orp": "true"}
        if ctx and not ctx.is_admin:
            filters["associateds_allowed"] = ctx.associated

        plates = Plate.get_all(filters)

        if not plates:
            return {"total": 0, "mensagem": "Nenhum equipamento com ORP fora da faixa"}

        locais = []
        for p in plates:
            orp_atual = p.sensors_value.get("orp", 0) if p.sensors_value else 0
            locais.append(
                {
                    "granja": p.farm_associated,
                    "orp_atual": orp_atual,
                    "serial": p.serial,
                }
            )

        return {
            "total": len(plates),
            "locais": locais,
        }

    except Exception as e:
        log.error("Erro ao consultar ORP fora da faixa", error=str(e))
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


def tempo_real_geral(granja: str) -> dict:
    """
    Obtém leitura geral em tempo real de todos os sensores de uma granja.

    Args:
        granja: Nome da granja

    Returns:
        Leituras de todos os sensores
    """
    try:
        from z1monitoring_agent.utils import commons_actions

        farm = Farm.get_farm_like_sensibility(granja)
        if not farm:
            return {"erro": f"Granja '{granja}' não encontrada"}

        plates = Plate.get_all({"farm_associated": farm.name})

        if not plates:
            return {"erro": f"Nenhum equipamento encontrado na granja '{farm.name}'"}

        # Usa a função existente que monta a mensagem de tempo real
        resultado = commons_actions.handler_tempo_real_geral(farm, plates)

        return {"granja": farm.name, "mensagem": resultado}

    except Exception as e:
        log.error("Erro ao consultar tempo real geral", error=str(e))
        return {"erro": str(e)}


def tempo_real_ph(granja: str) -> dict:
    """
    Obtém leitura de pH em tempo real de uma granja.

    Args:
        granja: Nome da granja

    Returns:
        Leitura de pH
    """
    try:
        farm = Farm.get_farm_like_sensibility(granja)
        if not farm:
            return {"erro": f"Granja '{granja}' não encontrada"}

        plates = Plate.get_all(
            {
                "farm_associated": farm.name,
                "plate_type": ["Z1", "PHI"],
            }
        )

        if not plates:
            return {"erro": f"Nenhum sensor de pH encontrado em '{farm.name}'"}

        leituras = []
        for plate in plates:
            if plate.sensors_value and "ph" in plate.sensors_value:
                leituras.append(
                    {
                        "serial": plate.serial,
                        "ph": plate.sensors_value.get("ph"),
                        "ph_min": plate.sensors_value.get("ph_min"),
                        "ph_max": plate.sensors_value.get("ph_max"),
                        "comunicando": plate.have_communication,
                    }
                )

        return {
            "granja": farm.name,
            "leituras": leituras,
        }

    except Exception as e:
        log.error("Erro ao consultar pH", error=str(e))
        return {"erro": str(e)}


def tempo_real_orp(granja: str) -> dict:
    """
    Obtém leitura de ORP em tempo real de uma granja.

    Args:
        granja: Nome da granja

    Returns:
        Leitura de ORP
    """
    try:
        farm = Farm.get_farm_like_sensibility(granja)
        if not farm:
            return {"erro": f"Granja '{granja}' não encontrada"}

        plates = Plate.get_all(
            {
                "farm_associated": farm.name,
                "plate_type": ["Z1", "ORP"],
            }
        )

        if not plates:
            return {"erro": f"Nenhum sensor de ORP encontrado em '{farm.name}'"}

        leituras = []
        for plate in plates:
            if plate.sensors_value and "orp" in plate.sensors_value:
                leituras.append(
                    {
                        "serial": plate.serial,
                        "orp": plate.sensors_value.get("orp"),
                        "orp_min": plate.sensors_value.get("orp_min"),
                        "orp_max": plate.sensors_value.get("orp_max"),
                        "comunicando": plate.have_communication,
                    }
                )

        return {
            "granja": farm.name,
            "leituras": leituras,
        }

    except Exception as e:
        log.error("Erro ao consultar ORP", error=str(e))
        return {"erro": str(e)}


def tempo_real_temperatura(granja: str) -> dict:
    """
    Obtém leitura de temperatura em tempo real de uma granja.

    Args:
        granja: Nome da granja

    Returns:
        Leitura de temperatura
    """
    try:
        farm = Farm.get_farm_like_sensibility(granja)
        if not farm:
            return {"erro": f"Granja '{granja}' não encontrada"}

        plates = Plate.get_all(
            {
                "farm_associated": farm.name,
                "plate_type": ["Z1", "AZ1"],
            }
        )

        if not plates:
            return {"erro": f"Nenhum sensor de temperatura em '{farm.name}'"}

        leituras = []
        for plate in plates:
            if plate.sensors_value and "temperature" in plate.sensors_value:
                leituras.append(
                    {
                        "serial": plate.serial,
                        "temperatura": plate.sensors_value.get("temperature"),
                        "comunicando": plate.have_communication,
                    }
                )

        return {
            "granja": farm.name,
            "leituras": leituras,
        }

    except Exception as e:
        log.error("Erro ao consultar temperatura", error=str(e))
        return {"erro": str(e)}


def tempo_real_gas(granja: str) -> dict:
    """
    Obtém nível de gás em tempo real de uma granja.

    Args:
        granja: Nome da granja

    Returns:
        Nível de gás
    """
    try:
        from z1monitoring_agent.utils import commons_actions

        farm = Farm.get_farm_like_sensibility(granja)
        if not farm:
            return {"erro": f"Granja '{granja}' não encontrada"}

        plates = Plate.get_all({"farm_associated": farm.name})

        if not plates:
            return {"erro": f"Nenhum equipamento encontrado em '{farm.name}'"}

        # Usa handler existente
        resultado = commons_actions.handler_tempo_real_gas(farm, plates)

        return {"granja": farm.name, "mensagem": resultado}

    except Exception as e:
        log.error("Erro ao consultar gás", error=str(e))
        return {"erro": str(e)}


def tempo_real_nivel_agua(granja: str) -> dict:
    """
    Obtém nível de água em tempo real de uma granja.

    Args:
        granja: Nome da granja

    Returns:
        Nível de água
    """
    try:
        farm = Farm.get_farm_like_sensibility(granja)
        if not farm:
            return {"erro": f"Granja '{granja}' não encontrada"}

        plates = Plate.get_all(
            {
                "farm_associated": farm.name,
                "plate_type": ["NVL"],
            }
        )

        if not plates:
            return {"erro": f"Nenhum sensor de nível encontrado em '{farm.name}'"}

        leituras = []
        for plate in plates:
            if plate.sensors_value:
                leituras.append(
                    {
                        "serial": plate.serial,
                        "nivel_percentual": plate.sensors_value.get("level_percentage"),
                        "volume_litros": plate.sensors_value.get("volume"),
                        "comunicando": plate.have_communication,
                    }
                )

        return {
            "granja": farm.name,
            "leituras": leituras,
        }

    except Exception as e:
        log.error("Erro ao consultar nível de água", error=str(e))
        return {"erro": str(e)}


def tempo_real_fluxo_agua(granja: str) -> dict:
    """
    Obtém fluxo de água em tempo real de uma granja.

    Args:
        granja: Nome da granja

    Returns:
        Fluxo de água
    """
    try:
        farm = Farm.get_farm_like_sensibility(granja)
        if not farm:
            return {"erro": f"Granja '{granja}' não encontrada"}

        plates = Plate.get_all(
            {
                "farm_associated": farm.name,
                "plate_type": ["FLX"],
            }
        )

        if not plates:
            return {"erro": f"Nenhum sensor de fluxo encontrado em '{farm.name}'"}

        leituras = []
        for plate in plates:
            if plate.sensors_value:
                leituras.append(
                    {
                        "serial": plate.serial,
                        "fluxo_lpm": plate.sensors_value.get("flow"),
                        "total_litros": plate.sensors_value.get("total_volume"),
                        "comunicando": plate.have_communication,
                    }
                )

        return {
            "granja": farm.name,
            "leituras": leituras,
        }

    except Exception as e:
        log.error("Erro ao consultar fluxo de água", error=str(e))
        return {"erro": str(e)}


def tempo_real_ozonio(granja: str) -> dict:
    """
    Obtém leitura de ozônio em tempo real de uma granja.

    Args:
        granja: Nome da granja

    Returns:
        Leitura de ozônio com sensores e configuração atual
    """
    try:
        farm = Farm.get_farm_like_sensibility(granja)
        if not farm:
            return {"erro": f"Granja '{granja}' não encontrada"}

        plates = Plate.get_all(
            {
                "farm_associated": farm.name,
                "plate_type": ["OZ1"],
            }
        )

        if not plates:
            return {"erro": f"Nenhum gerador de ozônio encontrado em '{farm.name}'"}

        leituras = []
        for plate in plates:
            info = {
                "serial": plate.serial,
                "comunicando": plate.have_communication,
            }
            if plate.sensors_value:
                info["orp"] = plate.sensors_value.get("orp")
                info["horas_ligado"] = plate.sensors_value.get("hours_on")
            # Inclui configuração atual
            params = plate.params or {}
            cell_en = params.get("cell_en")
            dryer_en = params.get("dryer_en")
            if cell_en is not None:
                info["celula_ligada"] = cell_en in (1, True, "1")
            if dryer_en is not None:
                info["secador_ligado"] = dryer_en in (1, True, "1")
            if params.get("dryer_temp") is not None:
                info["temperatura_secador"] = params["dryer_temp"]
            if params.get("cell_horas_on") is not None:
                info["tempo_celula_ligada_min"] = params["cell_horas_on"]
            if params.get("cell_min_off") is not None:
                info["tempo_celula_desligada_min"] = params["cell_min_off"]
            leituras.append(info)

        return {
            "granja": farm.name,
            "leituras": leituras,
        }

    except Exception as e:
        log.error("Erro ao consultar ozônio", error=str(e))
        return {"erro": str(e)}


def tempo_real_dosadora(granja: str) -> dict:
    """
    Obtém status da central de dosagem (CCD) em tempo real.

    Args:
        granja: Nome da granja

    Returns:
        Status das dosadoras
    """
    try:
        farm = Farm.get_farm_like_sensibility(granja)
        if not farm:
            return {"erro": f"Granja '{granja}' não encontrada"}

        plates = Plate.get_all(
            {
                "farm_associated": farm.name,
                "plate_type": ["CCD"],
            }
        )

        if not plates:
            return {"erro": f"Nenhuma central de dosagem encontrada em '{farm.name}'"}

        leituras = []
        for plate in plates:
            if plate.sensors_value:
                leituras.append(
                    {
                        "serial": plate.serial,
                        "acido_kg": plate.sensors_value.get("acid_weight"),
                        "cloro_kg": plate.sensors_value.get("chlorine_weight"),
                        "modo_acido": plate.sensors_value.get("acid_mode"),
                        "modo_cloro": plate.sensors_value.get("chlorine_mode"),
                        "dosadora_acido_ligada": plate.sensors_value.get("acid_pump_on"),
                        "dosadora_cloro_ligada": plate.sensors_value.get("chlorine_pump_on"),
                        "comunicando": plate.have_communication,
                    }
                )

        return {
            "granja": farm.name,
            "leituras": leituras,
        }

    except Exception as e:
        log.error("Erro ao consultar dosadora", error=str(e))
        return {"erro": str(e)}


# =============================================================================
# 3. ANÁLISES
# =============================================================================


def analise_agua(granja: str) -> dict:
    """
    Faz análise completa da qualidade da água de uma granja.

    Args:
        granja: Nome da granja

    Returns:
        Análise da água com pH, ORP, temperatura
    """
    try:
        farm = Farm.get_farm_like_sensibility(granja)
        if not farm:
            return {"erro": f"Granja '{granja}' não encontrada"}

        plates = Plate.get_all(
            {
                "farm_associated": farm.name,
                "plate_type": ["Z1", "PHI", "ORP"],
            }
        )

        if not plates:
            return {"erro": f"Nenhum sensor de água encontrado em '{farm.name}'"}

        analise = {
            "granja": farm.name,
            "ph": None,
            "orp": None,
            "temperatura": None,
            "status_geral": "ok",
            "alertas": [],
        }

        for plate in plates:
            if not plate.sensors_value:
                continue

            # pH
            if "ph" in plate.sensors_value and analise["ph"] is None:
                ph = plate.sensors_value.get("ph")
                ph_min = plate.sensors_value.get("ph_min", 6.5)
                ph_max = plate.sensors_value.get("ph_max", 7.5)
                analise["ph"] = {
                    "valor": ph,
                    "minimo": ph_min,
                    "maximo": ph_max,
                    "na_faixa": ph_min <= ph <= ph_max if ph else False,
                }
                if not analise["ph"]["na_faixa"]:
                    analise["alertas"].append(f"pH fora da faixa: {ph}")
                    analise["status_geral"] = "alerta"

            # ORP
            if "orp" in plate.sensors_value and analise["orp"] is None:
                orp = plate.sensors_value.get("orp")
                orp_min = plate.sensors_value.get("orp_min", 650)
                orp_max = plate.sensors_value.get("orp_max", 750)
                analise["orp"] = {
                    "valor": orp,
                    "minimo": orp_min,
                    "maximo": orp_max,
                    "na_faixa": orp_min <= orp <= orp_max if orp else False,
                }
                if not analise["orp"]["na_faixa"]:
                    analise["alertas"].append(f"ORP fora da faixa: {orp}")
                    analise["status_geral"] = "alerta"

            # Temperatura
            if "temperature" in plate.sensors_value and analise["temperatura"] is None:
                analise["temperatura"] = plate.sensors_value.get("temperature")

        return analise

    except Exception as e:
        log.error("Erro na análise de água", error=str(e))
        return {"erro": str(e)}


def analise_gas(granja: str) -> dict:
    """
    Faz análise do consumo e nível de gás de uma granja.

    Args:
        granja: Nome da granja

    Returns:
        Análise do gás com nível, consumo e autonomia
    """
    try:
        from z1monitoring_agent.utils import commons_actions

        farm = Farm.get_farm_like_sensibility(granja)
        if not farm:
            return {"erro": f"Granja '{granja}' não encontrada"}

        plates = Plate.get_all({"farm_associated": farm.name})

        if not plates:
            return {"erro": f"Nenhum equipamento encontrado em '{farm.name}'"}

        # Usa handler existente (mesmo que tempo_real_gas)
        resultado = commons_actions.handler_tempo_real_gas(farm, plates)

        return {"granja": farm.name, "mensagem": resultado}

    except Exception as e:
        log.error("Erro na análise de gás", error=str(e))
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
    """Retorna todas as granjas que batem por substring com o nome dado."""
    try:
        all_farms = Farm.get_all_farms_objs_filtereds({})
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
            farm = Farm.get_farm_like_sensibility(nome)
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
        farm = Farm.get_farm_like_sensibility(farm_name)
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
        if ctx and ctx.is_admin:
            # Admin vê todas - usa método que retorna objetos
            farms = Farm.get_all_farms_objs_filtereds({})
        elif ctx:
            farms = Farm.get_all_farms_objs_filtereds({"owner": ctx.associated})
        else:
            return {"erro": "Contexto de usuário não disponível"}

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


def ajustar_ph(granja: str, ph_min: float, ph_max: float) -> dict:
    """
    Ajusta a faixa de pH de uma granja.

    Args:
        granja: Nome da granja
        ph_min: Valor mínimo de pH (ex: 6.5)
        ph_max: Valor máximo de pH (ex: 7.5)

    Returns:
        Confirmação do ajuste solicitado
    """
    try:
        # Validações
        if ph_min >= ph_max:
            return {"erro": "pH mínimo deve ser menor que o máximo"}

        if ph_min < 0 or ph_max > 14:
            return {"erro": "pH deve estar entre 0 e 14"}

        farm = Farm.get_farm_like_sensibility(granja)
        if not farm:
            return {"erro": f"Granja '{granja}' não encontrada"}

        # Retorna confirmação para o agente solicitar ao usuário
        return {
            "acao": "ajuste_ph",
            "granja": farm.name,
            "ph_min": ph_min,
            "ph_max": ph_max,
            "requer_confirmacao": True,
            "mensagem": f"Confirma ajuste de pH para {ph_min} - {ph_max} em {farm.name}?",
        }

    except Exception as e:
        log.error("Erro ao ajustar pH", error=str(e))
        return {"erro": str(e)}


def ajustar_orp(granja: str, orp_min: int, orp_max: int) -> dict:
    """
    Ajusta a faixa de ORP de uma granja.

    Args:
        granja: Nome da granja
        orp_min: Valor mínimo de ORP em mV (ex: 650)
        orp_max: Valor máximo de ORP em mV (ex: 750)

    Returns:
        Confirmação do ajuste solicitado
    """
    try:
        # Validações
        if orp_min >= orp_max:
            return {"erro": "ORP mínimo deve ser menor que o máximo"}

        if orp_min < 0 or orp_max > 1000:
            return {"erro": "ORP deve estar entre 0 e 1000 mV"}

        farm = Farm.get_farm_like_sensibility(granja)
        if not farm:
            return {"erro": f"Granja '{granja}' não encontrada"}

        return {
            "acao": "ajuste_orp",
            "granja": farm.name,
            "orp_min": orp_min,
            "orp_max": orp_max,
            "requer_confirmacao": True,
            "mensagem": f"Confirma ajuste de ORP para {orp_min} - {orp_max} mV em {farm.name}?",
        }

    except Exception as e:
        log.error("Erro ao ajustar ORP", error=str(e))
        return {"erro": str(e)}


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

        farm = Farm.get_farm_like_sensibility(granja)
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


def liberar_injecao(granja: str, dosadora: str) -> dict:
    """
    Libera injeção de ácido ou cloro quando o limite 24h foi atingido (ABS).

    O sistema possui um freio automático (ABS) que bloqueia a injeção
    quando o consumo de 24h é ultrapassado. Esta função libera manualmente.

    Args:
        granja: Nome da granja
        dosadora: Tipo da dosadora ("acido" ou "cloro")

    Returns:
        Confirmação da liberação solicitada
    """
    try:
        if dosadora not in ["acido", "cloro"]:
            return {"erro": "Dosadora deve ser 'acido' ou 'cloro'"}

        farm = Farm.get_farm_like_sensibility(granja)
        if not farm:
            return {"erro": f"Granja '{granja}' não encontrada"}

        return {
            "acao": f"liberar_abs_{dosadora}",
            "granja": farm.name,
            "dosadora": dosadora,
            "requer_confirmacao": True,
            "mensagem": f"Confirma liberar injeção de {dosadora} em {farm.name}? (override do limite 24h)",
        }

    except Exception as e:
        log.error("Erro ao liberar injeção", error=str(e))
        return {"erro": str(e)}


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

        farm = Farm.get_farm_like_sensibility(granja)
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


def rearmar_abs(granja: str, dosadora: str) -> dict:
    """
    Rearma o ABS (freio automático) para atuar automaticamente após liberação manual.

    Após usar liberar_injecao, o ABS fica desativado. Esta função reativa
    o controle automático enviando abs_acid=1 ou abs_chlorine=1 para a placa.

    Args:
        granja: Nome da granja
        dosadora: Tipo da dosadora ("acido" ou "cloro")

    Returns:
        Confirmação do rearmamento solicitado
    """
    try:
        if dosadora not in ["acido", "cloro"]:
            return {"erro": "Dosadora deve ser 'acido' ou 'cloro'"}

        farm = Farm.get_farm_like_sensibility(granja)
        if not farm:
            return {"erro": f"Granja '{granja}' não encontrada"}

        # Parâmetro a ser enviado para a placa
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

    except Exception as e:
        log.error("Erro ao rearmar ABS", error=str(e))
        return {"erro": str(e)}


DRYER_TEMP_MIN = 20
DRYER_TEMP_MAX = 80


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
        farm = Farm.get_farm_like_sensibility(granja)
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


def habilitar_alarme_galpao(granja: str, galpao: str = None) -> dict:
    """
    Habilita alarmes de um galpão.

    Args:
        granja: Nome da granja
        galpao: Nome do galpão (opcional, se não informado habilita todos)

    Returns:
        Confirmação da ação
    """
    try:
        farm = Farm.get_farm_like_sensibility(granja)
        if not farm:
            return {"erro": f"Granja '{granja}' não encontrada"}

        return {
            "acao": "habilitar_alarme",
            "granja": farm.name,
            "galpao": galpao or "todos",
            "requer_confirmacao": True,
            "mensagem": f"Confirma habilitar alarmes em {farm.name}?",
        }

    except Exception as e:
        log.error("Erro ao habilitar alarme", error=str(e))
        return {"erro": str(e)}


def desabilitar_alarme_galpao(granja: str, galpao: str = None) -> dict:
    """
    Desabilita alarmes de um galpão.

    Args:
        granja: Nome da granja
        galpao: Nome do galpão (opcional, se não informado desabilita todos)

    Returns:
        Confirmação da ação
    """
    try:
        farm = Farm.get_farm_like_sensibility(granja)
        if not farm:
            return {"erro": f"Granja '{granja}' não encontrada"}

        return {
            "acao": "desabilitar_alarme",
            "granja": farm.name,
            "galpao": galpao or "todos",
            "requer_confirmacao": True,
            "mensagem": f"Confirma desabilitar alarmes em {farm.name}?",
        }

    except Exception as e:
        log.error("Erro ao desabilitar alarme", error=str(e))
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


def solicitar_suporte(equipamento: str = None, problema: str = None) -> dict:
    """
    Inicia solicitação de suporte técnico.

    Args:
        equipamento: Tipo de equipamento com problema (opcional)
        problema: Descrição do problema (opcional)

    Returns:
        Início do fluxo de suporte
    """
    return {
        "acao": "iniciar_suporte",
        "equipamento": equipamento,
        "problema": problema,
        "mensagem": "Iniciando atendimento de suporte técnico",
    }


def obter_guia_suporte(tipo_equipamento: str, topico: str) -> dict:
    """
    Obtém guia de suporte com instruções e imagem para um tópico específico.

    Args:
        tipo_equipamento: Tipo do equipamento (Z1, CCD, PHI, ORP, WGT, FLX, NVL, OZ1, etc)
        topico: Tópico do suporte (calibracao, offline, leitura, dosagem, etc)

    Returns:
        Guia com texto explicativo e URL da imagem
    """
    try:
        from monitoring.whatsapp_steps_z1.support_guides_config import (
            SUPPORT_GUIDES,
            PLATE_TYPE_NAMES,
            GUIDES_PUBLIC_URL,
        )

        # Monta o ID do guia
        tipo_lower = tipo_equipamento.lower()
        guide_id = f"{tipo_lower}_{topico}"

        guide = SUPPORT_GUIDES.get(guide_id)
        if not guide:
            # Tenta variações comuns
            alternativas = [
                f"{tipo_lower}_outros",
                f"{tipo_lower}_offline",
            ]
            for alt in alternativas:
                if alt in SUPPORT_GUIDES:
                    guide = SUPPORT_GUIDES[alt]
                    guide_id = alt
                    break

        if not guide:
            return {
                "encontrado": False,
                "mensagem": f"Guia não encontrado para {tipo_equipamento} - {topico}",
                "topicos_disponiveis": ["calibracao", "offline", "leitura", "dosagem", "config"],
            }

        nome_equipamento = PLATE_TYPE_NAMES.get(tipo_equipamento.upper(), tipo_equipamento)

        return {
            "encontrado": True,
            "equipamento": nome_equipamento,
            "topico": topico,
            "texto": guide.get("text", ""),
            "imagem_url": f"{GUIDES_PUBLIC_URL}{guide.get('image', '')}" if guide.get("image") else None,
        }

    except Exception as e:
        log.error("Erro ao obter guia de suporte", error=str(e))
        return {"erro": str(e)}


def listar_topicos_suporte(tipo_equipamento: str) -> dict:
    """
    Lista os tópicos de suporte disponíveis para um tipo de equipamento.

    Args:
        tipo_equipamento: Tipo do equipamento (Z1, CCD, PHI, ORP, WGT, FLX, NVL, etc)

    Returns:
        Lista de tópicos disponíveis
    """
    try:
        from monitoring.whatsapp_steps_z1.support_guides_config import (
            SUPPORT_TOPICS,
            PLATE_TYPE_NAMES,
        )

        tipo_upper = tipo_equipamento.upper()
        topicos = SUPPORT_TOPICS.get(tipo_upper, [])

        if not topicos:
            return {
                "encontrado": False,
                "mensagem": f"Tipo de equipamento '{tipo_equipamento}' não encontrado",
                "tipos_disponiveis": list(PLATE_TYPE_NAMES.keys()),
            }

        nome_equipamento = PLATE_TYPE_NAMES.get(tipo_upper, tipo_equipamento)

        return {
            "encontrado": True,
            "equipamento": nome_equipamento,
            "topicos": [{"id": t["id"], "nome": t["label"]} for t in topicos],
        }

    except Exception as e:
        log.error("Erro ao listar tópicos de suporte", error=str(e))
        return {"erro": str(e)}


# =============================================================================
# 8. GRÁFICOS E RELATÓRIOS
# =============================================================================


def consultar_historico_consumo(granja: str, dias: int = 7) -> dict:
    """
    Consulta histórico de consumo diário de ácido, cloro e água de uma granja.
    Retorna os dados numéricos por dia para análise (sem gerar gráfico).

    Args:
        granja: Nome da granja
        dias: Período em dias (default: 7, máximo: 90)

    Returns:
        Dados de consumo por dia com totais
    """
    try:
        import datetime
        from datetime import timedelta
        from z1monitoring_models.models.choose_event_model import get_events_model

        farm = Farm.get_farm_like_sensibility(granja)
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

        return {
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

    except Exception as e:
        log.error("Erro ao consultar histórico de consumo", error=str(e))
        return {"erro": str(e)}


def gerar_grafico_consumo(granja: str, dias: int = 7) -> dict:
    """
    Gera gráfico de consumo para uma granja.
    Os gráficos (imagens) são enfileirados para envio direto ao usuário.

    Args:
        granja: Nome da granja
        dias: Período em dias (default: 7)

    Returns:
        Confirmação de que os gráficos foram gerados
    """
    try:
        from z1monitoring_agent.utils import commons_actions

        farm = Farm.get_farm_like_sensibility(granja)
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

        # Enfileira as imagens para envio direto (não passam pelo texto do agente)
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


def relatorio_consumo_gas() -> dict:
    """
    Gera relatório consolidado de consumo de gás de todos os locais.
    Retorna tabela com nível, consumo médio e autonomia de cada local.

    Returns:
        Dados do relatório de gás
    """
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


def relatorio_abastecimento_gas(granja: str = None) -> dict:
    """
    Gera relatório de abastecimentos de gás dos últimos 30 dias.

    Args:
        granja: Nome da granja (opcional, se não informado mostra todos)

    Returns:
        Relatório de abastecimentos
    """
    ctx = get_user_context()
    try:
        from monitoring.services.reports import get_relatorio_abastecimento_gas

        farm_name = "TODOS"
        if granja:
            farm = Farm.get_farm_like_sensibility(granja)
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


def panorama_24h(granja: str = None) -> dict:
    """
    Obtém panorama das últimas 24 horas de uma granja ou todas.

    Args:
        granja: Nome da granja (opcional, se não informado mostra todas)

    Returns:
        Panorama 24h com status de todos os sensores
    """
    try:
        if granja:
            farm = Farm.get_farm_like_sensibility(granja)
            if not farm:
                return {"erro": f"Granja '{granja}' não encontrada"}
            farm_name = farm.name
        else:
            farm_name = None

        return {
            "acao": "panorama_24h",
            "granja": farm_name,
            "mensagem": f"Gerando panorama 24h{' de ' + farm_name if farm_name else ''}...",
        }

    except Exception as e:
        log.error("Erro ao gerar panorama", error=str(e))
        return {"erro": str(e)}


# =============================================================================
# 9. CONTROLE DE SAÍDAS
# =============================================================================


def ligar_saida(granja: str, saida: str) -> dict:
    """
    Liga uma saída (bomba, válvula, motor, etc).

    Args:
        granja: Nome da granja
        saida: Nome da saída (bomba, valvula, motor, ventilador, etc)

    Returns:
        Confirmação da ação solicitada
    """
    try:
        farm = Farm.get_farm_like_sensibility(granja)
        if not farm:
            return {"erro": f"Granja '{granja}' não encontrada"}

        return {
            "acao": "ligar_saida",
            "granja": farm.name,
            "saida": saida,
            "requer_confirmacao": True,
            "mensagem": f"Confirma LIGAR {saida} em {farm.name}?",
        }

    except Exception as e:
        log.error("Erro ao ligar saída", error=str(e))
        return {"erro": str(e)}


def desligar_saida(granja: str, saida: str) -> dict:
    """
    Desliga uma saída (bomba, válvula, motor, etc).

    Args:
        granja: Nome da granja
        saida: Nome da saída (bomba, valvula, motor, ventilador, etc)

    Returns:
        Confirmação da ação solicitada
    """
    try:
        farm = Farm.get_farm_like_sensibility(granja)
        if not farm:
            return {"erro": f"Granja '{granja}' não encontrada"}

        return {
            "acao": "desligar_saida",
            "granja": farm.name,
            "saida": saida,
            "requer_confirmacao": True,
            "mensagem": f"Confirma DESLIGAR {saida} em {farm.name}?",
        }

    except Exception as e:
        log.error("Erro ao desligar saída", error=str(e))
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


def iniciar_lote(granja: str, galpao: str = None) -> dict:
    """
    Inicia um novo lote em uma granja/galpão.

    Args:
        granja: Nome da granja
        galpao: Nome do galpão (opcional)

    Returns:
        Solicitação de início de lote
    """
    try:
        farm = Farm.get_farm_like_sensibility(granja)
        if not farm:
            return {"erro": f"Granja '{granja}' não encontrada"}

        return {
            "acao": "iniciar_lote",
            "granja": farm.name,
            "galpao": galpao,
            "requer_confirmacao": True,
            "mensagem": f"Confirma início de lote em {farm.name}" + (f" - {galpao}" if galpao else "") + "?",
        }

    except Exception as e:
        log.error("Erro ao iniciar lote", error=str(e))
        return {"erro": str(e)}


def finalizar_lote(granja: str, galpao: str = None) -> dict:
    """
    Finaliza um lote em uma granja/galpão.

    Args:
        granja: Nome da granja
        galpao: Nome do galpão (opcional)

    Returns:
        Solicitação de finalização de lote
    """
    try:
        farm = Farm.get_farm_like_sensibility(granja)
        if not farm:
            return {"erro": f"Granja '{granja}' não encontrada"}

        return {
            "acao": "finalizar_lote",
            "granja": farm.name,
            "galpao": galpao,
            "requer_confirmacao": True,
            "mensagem": f"Confirma fim de lote em {farm.name}" + (f" - {galpao}" if galpao else "") + "?",
        }

    except Exception as e:
        log.error("Erro ao finalizar lote", error=str(e))
        return {"erro": str(e)}


# =============================================================================
# 11. REGISTRO DE VISITA
# =============================================================================


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
        farm = Farm.get_farm_like_sensibility(granja)
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
# 12. PRÉ-CADASTRO
# =============================================================================


def iniciar_pre_cadastro() -> dict:
    """
    Inicia o processo de pré-cadastro de um novo cliente/equipamento.

    Returns:
        Início do fluxo de pré-cadastro
    """
    return {
        "acao": "iniciar_pre_cadastro",
        "mensagem": "Iniciando pré-cadastro. Vou precisar de algumas informações.",
        "campos_necessarios": [
            "Nome completo",
            "CPF ou CNPJ",
            "Telefone",
            "CEP",
            "Serial do equipamento",
        ],
    }


def adicionar_equipamento_pre_cadastro(serial: str) -> dict:
    """
    Adiciona um equipamento ao pré-cadastro pelo número serial.

    Args:
        serial: Número serial do equipamento

    Returns:
        Confirmação do equipamento adicionado
    """
    try:
        # Verifica se o serial tem formato válido
        if not serial or len(serial) < 5:
            return {"erro": "Serial inválido. Informe o número completo."}

        # Tenta identificar o tipo pelo prefixo
        tipo = None
        serial_upper = serial.upper()
        prefixos = {
            "Z1": "SmartPH",
            "CCD": "Central de Dosagem",
            "PHI": "Sensor pH",
            "ORP": "Sensor ORP",
            "WGT": "Balança",
            "FLX": "Fluxômetro",
            "NVL": "Sensor de Nível",
            "IOX": "SmartSync",
            "AZ1": "Ambiência",
            "OZ1": "Gerador de Ozônio",
        }

        for prefixo, nome in prefixos.items():
            if serial_upper.startswith(prefixo):
                tipo = nome
                break

        return {
            "acao": "adicionar_equipamento_pre_cadastro",
            "serial": serial,
            "tipo_identificado": tipo,
            "mensagem": f"Equipamento {serial} adicionado" + (f" ({tipo})" if tipo else ""),
        }

    except Exception as e:
        log.error("Erro ao adicionar equipamento", error=str(e))
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
# 11. ANÁLISE DE PERÍODOS OFFLINE E RANKING
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
            farms = Farm.get_all({})
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
        farm = candidates[0]

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


# =============================================================================
# DEFINIÇÃO DAS FERRAMENTAS PARA O AGENTE
# =============================================================================


TOOLS_Z1 = [
    # ===== STATUS E CONSULTAS =====
    Tool(
        name="consultar_alarmes",
        description="Consulta alarmes urgentes recentes. Pode filtrar por granja e período em dias.",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja (opcional)"},
                "dias": {"type": "integer", "description": "Dias para buscar (default: 1)", "default": 1},
            },
            "required": [],
        },
        function=consultar_alarmes,
    ),
    Tool(
        name="consultar_equipamentos_offline",
        description="Lista todos os equipamentos que estão offline/sem comunicação.",
        parameters={"type": "object", "properties": {}, "required": []},
        function=consultar_equipamentos_offline,
    ),
    Tool(
        name="consultar_equipamentos_online",
        description="Lista todos os equipamentos que estão online/comunicando.",
        parameters={"type": "object", "properties": {}, "required": []},
        function=consultar_equipamentos_online,
    ),
    Tool(
        name="consultar_falta_acido",
        description="Lista equipamentos/locais com falta de ácido.",
        parameters={"type": "object", "properties": {}, "required": []},
        function=consultar_falta_acido,
    ),
    Tool(
        name="consultar_falta_cloro",
        description="Lista equipamentos/locais com falta de cloro.",
        parameters={"type": "object", "properties": {}, "required": []},
        function=consultar_falta_cloro,
    ),
    Tool(
        name="consultar_falta_gas",
        description="Lista equipamentos/locais com nível de gás baixo.",
        parameters={"type": "object", "properties": {}, "required": []},
        function=consultar_falta_gas,
    ),
    Tool(
        name="consultar_ph_fora_faixa",
        description="Lista equipamentos com pH fora da faixa configurada.",
        parameters={"type": "object", "properties": {}, "required": []},
        function=consultar_ph_fora_faixa,
    ),
    Tool(
        name="consultar_orp_fora_faixa",
        description="Lista equipamentos com ORP fora da faixa configurada.",
        parameters={"type": "object", "properties": {}, "required": []},
        function=consultar_orp_fora_faixa,
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
        name="tempo_real_geral",
        description="Obtém leitura geral de todos os sensores de uma granja em tempo real.",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja"},
            },
            "required": ["granja"],
        },
        function=tempo_real_geral,
    ),
    Tool(
        name="tempo_real_ph",
        description="Obtém leitura de pH em tempo real de uma granja.",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja"},
            },
            "required": ["granja"],
        },
        function=tempo_real_ph,
    ),
    Tool(
        name="tempo_real_orp",
        description="Obtém leitura de ORP em tempo real de uma granja.",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja"},
            },
            "required": ["granja"],
        },
        function=tempo_real_orp,
    ),
    Tool(
        name="tempo_real_temperatura",
        description="Obtém leitura de temperatura em tempo real de uma granja.",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja"},
            },
            "required": ["granja"],
        },
        function=tempo_real_temperatura,
    ),
    Tool(
        name="tempo_real_gas",
        description="Obtém nível de gás em tempo real de uma granja. Mostra peso, percentual e autonomia.",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja"},
            },
            "required": ["granja"],
        },
        function=tempo_real_gas,
    ),
    Tool(
        name="tempo_real_nivel_agua",
        description="Obtém nível de água do reservatório em tempo real.",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja"},
            },
            "required": ["granja"],
        },
        function=tempo_real_nivel_agua,
    ),
    Tool(
        name="tempo_real_fluxo_agua",
        description="Obtém fluxo de água em tempo real (litros por minuto).",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja"},
            },
            "required": ["granja"],
        },
        function=tempo_real_fluxo_agua,
    ),
    Tool(
        name="tempo_real_ozonio",
        description="Obtém leitura do gerador de ozônio em tempo real.",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja"},
            },
            "required": ["granja"],
        },
        function=tempo_real_ozonio,
    ),
    Tool(
        name="tempo_real_dosadora",
        description="Obtém status da central de dosagem (CCD) - quantidade de ácido/cloro e modo de operação.",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja"},
            },
            "required": ["granja"],
        },
        function=tempo_real_dosadora,
    ),
    # ===== ANÁLISES =====
    Tool(
        name="analise_agua",
        description="Faz análise completa da qualidade da água (pH, ORP, temperatura) com alertas.",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja"},
            },
            "required": ["granja"],
        },
        function=analise_agua,
    ),
    Tool(
        name="analise_gas",
        description="Faz análise do gás - nível, consumo diário e autonomia restante.",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja"},
            },
            "required": ["granja"],
        },
        function=analise_gas,
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
        name="ajustar_ph",
        description="Ajusta a faixa de pH de uma granja. Requer confirmação do usuário.",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja"},
                "ph_min": {"type": "number", "description": "Valor mínimo de pH (ex: 6.5)"},
                "ph_max": {"type": "number", "description": "Valor máximo de pH (ex: 7.5)"},
            },
            "required": ["granja", "ph_min", "ph_max"],
        },
        function=ajustar_ph,
    ),
    Tool(
        name="ajustar_orp",
        description="Ajusta a faixa de ORP de uma granja. Requer confirmação do usuário.",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja"},
                "orp_min": {"type": "integer", "description": "Valor mínimo de ORP em mV (ex: 650)"},
                "orp_max": {"type": "integer", "description": "Valor máximo de ORP em mV (ex: 750)"},
            },
            "required": ["granja", "orp_min", "orp_max"],
        },
        function=ajustar_orp,
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
        name="liberar_injecao",
        description="DESTRAVAR/LIBERAR o ABS - desativa temporariamente o freio automático para permitir injeção mesmo após atingir limite 24h. Use quando precisa DESTRAVAR, LIBERAR, ou DESBLOQUEAR a dosagem.",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja"},
                "dosadora": {
                    "type": "string",
                    "enum": ["acido", "cloro"],
                    "description": "Tipo da dosadora",
                },
            },
            "required": ["granja", "dosadora"],
        },
        function=liberar_injecao,
    ),
    Tool(
        name="definir_limite_24h",
        description="Define o limite de consumo em 24h para uma dosadora (ABS). Quando atingido, bloqueia injeção automaticamente.",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja"},
                "dosadora": {
                    "type": "string",
                    "enum": ["acido", "cloro"],
                    "description": "Tipo da dosadora",
                },
                "limite_kg": {
                    "type": "number",
                    "description": "Limite em kg para 24h (ex: 5.0)",
                },
            },
            "required": ["granja", "dosadora", "limite_kg"],
        },
        function=definir_limite_24h,
    ),
    Tool(
        name="rearmar_abs",
        description="TRAVAR/ARMAR o ABS - ativa o freio automático de limite 24h. Use quando o usuário pedir para TRAVAR, ARMAR, ATIVAR, ou HABILITAR o ABS/freio automático de ácido ou cloro.",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja"},
                "dosadora": {
                    "type": "string",
                    "enum": ["acido", "cloro"],
                    "description": "Tipo da dosadora",
                },
            },
            "required": ["granja", "dosadora"],
        },
        function=rearmar_abs,
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
        name="habilitar_alarme_galpao",
        description="Habilita os alarmes de um galpão ou de toda a granja.",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja"},
                "galpao": {"type": "string", "description": "Nome do galpão (opcional)"},
            },
            "required": ["granja"],
        },
        function=habilitar_alarme_galpao,
    ),
    Tool(
        name="desabilitar_alarme_galpao",
        description="Desabilita os alarmes de um galpão ou de toda a granja.",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja"},
                "galpao": {"type": "string", "description": "Nome do galpão (opcional)"},
            },
            "required": ["granja"],
        },
        function=desabilitar_alarme_galpao,
    ),
    # ===== NAVEGAÇÃO =====
    Tool(
        name="mostrar_menu_principal",
        description="Mostra o menu principal de opções. Use quando o usuário pedir ajuda ou não souber o que fazer.",
        parameters={"type": "object", "properties": {}, "required": []},
        function=mostrar_menu_principal,
    ),
    Tool(
        name="mostrar_ajuda",
        description="Mostra guia completo de todas as funcionalidades disponíveis.",
        parameters={"type": "object", "properties": {}, "required": []},
        function=mostrar_ajuda,
    ),
    Tool(
        name="solicitar_suporte",
        description="Inicia solicitação de suporte técnico para um equipamento ou problema.",
        parameters={
            "type": "object",
            "properties": {
                "equipamento": {"type": "string", "description": "Tipo de equipamento (opcional)"},
                "problema": {"type": "string", "description": "Descrição do problema (opcional)"},
            },
            "required": [],
        },
        function=solicitar_suporte,
    ),
    Tool(
        name="obter_guia_suporte",
        description="Obtém guia de suporte com instruções detalhadas e imagem para um tópico específico de equipamento.",
        parameters={
            "type": "object",
            "properties": {
                "tipo_equipamento": {
                    "type": "string",
                    "description": "Tipo do equipamento (Z1, CCD, PHI, ORP, WGT, FLX, NVL, OZ1)",
                },
                "topico": {
                    "type": "string",
                    "description": "Tópico (calibracao, offline, leitura, dosagem, config)",
                },
            },
            "required": ["tipo_equipamento", "topico"],
        },
        function=obter_guia_suporte,
    ),
    Tool(
        name="listar_topicos_suporte",
        description="Lista os tópicos de suporte disponíveis para um tipo de equipamento.",
        parameters={
            "type": "object",
            "properties": {
                "tipo_equipamento": {
                    "type": "string",
                    "description": "Tipo do equipamento (Z1, CCD, PHI, ORP, WGT, FLX, NVL, OZ1)",
                },
            },
            "required": ["tipo_equipamento"],
        },
        function=listar_topicos_suporte,
    ),
    # ===== DADOS E RELATÓRIOS =====
    Tool(
        name="consultar_historico_consumo",
        description="Consulta histórico de consumo diário de ácido, cloro e água de uma granja. Retorna dados numéricos por dia com totais e médias para análise. Use esta tool quando o usuário pedir análise de consumo, comparações ou verificar se o consumo faz sentido.",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja"},
                "dias": {"type": "integer", "description": "Período em dias (default: 7, máximo: 90)", "default": 7},
            },
            "required": ["granja"],
        },
        function=consultar_historico_consumo,
    ),
    Tool(
        name="gerar_grafico_consumo",
        description="Gera gráfico de consumo de uma granja para um período específico.",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja"},
                "dias": {"type": "integer", "description": "Período em dias (default: 7)", "default": 7},
            },
            "required": ["granja"],
        },
        function=gerar_grafico_consumo,
    ),
    Tool(
        name="relatorio_consumo_gas",
        description="Gera relatório consolidado de consumo de gás com nível, consumo médio e autonomia de cada local.",
        parameters={"type": "object", "properties": {}, "required": []},
        function=relatorio_consumo_gas,
    ),
    Tool(
        name="relatorio_abastecimento_gas",
        description="Gera relatório de abastecimentos de gás dos últimos 30 dias. Pode filtrar por granja.",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja (opcional, se não informado mostra todos)"},
            },
            "required": [],
        },
        function=relatorio_abastecimento_gas,
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
        description="Obtém panorama das últimas 24 horas com status de todos os sensores.",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja (opcional)"},
            },
            "required": [],
        },
        function=panorama_24h,
    ),
    # ===== CONTROLE DE SAÍDAS =====
    Tool(
        name="ligar_saida",
        description="Liga uma saída física (bomba, válvula, motor, ventilador). Requer confirmação.",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja"},
                "saida": {"type": "string", "description": "Nome da saída (bomba, valvula, motor, etc)"},
            },
            "required": ["granja", "saida"],
        },
        function=ligar_saida,
    ),
    Tool(
        name="desligar_saida",
        description="Desliga uma saída física (bomba, válvula, motor, ventilador). Requer confirmação.",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja"},
                "saida": {"type": "string", "description": "Nome da saída (bomba, valvula, motor, etc)"},
            },
            "required": ["granja", "saida"],
        },
        function=desligar_saida,
    ),
    Tool(
        name="consultar_quadros_com_problema",
        description="Lista quadros de comando com problemas ou alarmes ativos.",
        parameters={"type": "object", "properties": {}, "required": []},
        function=consultar_quadros_com_problema,
    ),
    # ===== LOTES =====
    Tool(
        name="iniciar_lote",
        description="Inicia um novo lote em uma granja/galpão. Usado no início de cada ciclo de produção.",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja"},
                "galpao": {"type": "string", "description": "Nome do galpão (opcional)"},
            },
            "required": ["granja"],
        },
        function=iniciar_lote,
    ),
    Tool(
        name="finalizar_lote",
        description="Finaliza um lote em uma granja/galpão. Usado no fim de cada ciclo de produção.",
        parameters={
            "type": "object",
            "properties": {
                "granja": {"type": "string", "description": "Nome da granja"},
                "galpao": {"type": "string", "description": "Nome do galpão (opcional)"},
            },
            "required": ["granja"],
        },
        function=finalizar_lote,
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
    # ===== PRÉ-CADASTRO =====
    Tool(
        name="iniciar_pre_cadastro",
        description="Inicia o processo de pré-cadastro de novo cliente/equipamento.",
        parameters={"type": "object", "properties": {}, "required": []},
        function=iniciar_pre_cadastro,
    ),
    Tool(
        name="adicionar_equipamento_pre_cadastro",
        description="Adiciona um equipamento ao pré-cadastro pelo número serial.",
        parameters={
            "type": "object",
            "properties": {
                "serial": {"type": "string", "description": "Número serial do equipamento"},
            },
            "required": ["serial"],
        },
        function=adicionar_equipamento_pre_cadastro,
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
]
