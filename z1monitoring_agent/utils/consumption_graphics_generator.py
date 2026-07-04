"""
Módulo centralizado para geração de gráficos de consumo.
Evita duplicação de código entre handlers de WhatsApp.
"""

import re
import unicodedata
from datetime import datetime

import structlog

from z1monitoring_agent.utils import graphics
from z1monitoring_models.models.choose_event_model import get_events_model
from z1monitoring_models.models.events_flx import EventFLX
from z1monitoring_models.models.plates import Plate

log = structlog.get_logger()


def _compute_water_data_quality(plates, date_lower, date_upper, consumption_data):
    """Estima litros possivelmente perdidos por amostras que não chegaram.

    Firmware mede water_flow no ciclo e descarta se a transmissão falha
    (não retransmite). Cobertura abaixo da esperada (1 amostra/min) =
    consumo real maior que o registrado. Retorna None se cobertura ≥ 95%
    ou dados insuficientes pra estimar.
    """
    flx_plates = [p for p in plates if p.plate_type == "FLX"]
    if not flx_plates:
        return None

    try:
        dl = datetime.strptime(date_lower, "%Y-%m-%d %H:%M:%S")
        du = datetime.strptime(date_upper, "%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError):
        return None

    period_minutes = max(int((du - dl).total_seconds() / 60), 1)
    expected = period_minutes * len(flx_plates)

    received = sum(EventFLX.count_samples_in_period(p.serial, date_lower, date_upper) for p in flx_plates)

    if received <= 0 or expected <= 0:
        return None

    coverage = received / expected
    if coverage >= 0.95:
        return None

    consumo_medido = sum(float(d.get("water_consumed", 0) or 0) for d in consumption_data.values())
    if consumo_medido <= 0:
        return None

    consumo_estimado = consumo_medido * expected / received
    perdido = consumo_estimado - consumo_medido

    return {
        "coverage": coverage,
        "missing_samples": expected - received,
        "consumo_perdido_estimado": perdido,
    }


def _format_br_number(value):
    return f"{int(round(value)):,}".replace(",", ".")


def _has_gas_signal(wgt_data, gas_level_data):
    """True se a granja tem gás de fato no período: algum consumo de gás > 0
    ou algum nível de tanque > 0.

    O gráfico WGT (`graphics.consume_wgt`) é dedicado a gás ("Consumo de Gás e
    Nível do Tanque"). Ácido/cloro medidos pela WGT vão pro gráfico principal
    (ramo has_wgt em generate_consumption_graphics), então uma WGT sem gás não
    deve gerar um segundo gráfico zerado.
    """
    for fields in (wgt_data or {}).values():
        for k, v in fields.items():
            if "gas" in k.lower():
                try:
                    if float(v or 0) > 0:
                        return True
                except (TypeError, ValueError):
                    continue
    for v in (gas_level_data or {}).values():
        try:
            if float(v or 0) > 0:
                return True
        except (TypeError, ValueError):
            continue
    return False


def generate_consumption_graphics(farm, date_lower, date_upper):
    """
    Gera gráficos de consumo para uma fazenda em um período.

    Args:
        farm: Objeto Farm
        date_lower: Data inicial (formato: 'YYYY-MM-DD HH:MM:SS')
        date_upper: Data final (formato: 'YYYY-MM-DD HH:MM:SS')

    Returns:
        Lista de dicionários com os gráficos gerados:
        [
            {"type": "image", "msg": "header", "url": "https://..."},
            ...
        ]
    """
    response_list = []
    plates = Plate.get_all({"farm_id": farm.id})

    # ========================================
    # 1. Gráfico de consumo Z1/CCD + FLX
    # ========================================
    consumption_data = {}
    temperature_sensors = []

    # Verifica se tem CCD ou Z1
    has_ccd = any(p.plate_type == "CCD" for p in plates)
    has_z1 = any(p.plate_type == "Z1" for p in plates)
    has_wgt = any(p.plate_type == "WGT" for p in plates)

    log.info(f"DEBUG consumption_graphics - has_ccd: {has_ccd}, has_z1: {has_z1}, has_wgt: {has_wgt}")
    log.info(f"DEBUG consumption_graphics - date_lower: {date_lower}, date_upper: {date_upper}")

    if has_ccd:
        # Usa CCD para consumo de ácido, cloro e água
        _Event = get_events_model("CCD")
        events = _Event.get_insumes_consumed_last_days(farm.name, date_lower, date_upper)
        log.info(f"CCD consumption_graphics - Total events returned: {len(events)}")
        for ev in events:
            day_key = ev.created_at.strftime("%Y-%m-%d")
            log.info(
                f"CCD consumption_graphics Event - day: {day_key}, "
                f"cloro: {getattr(ev, 'chlorine_consumed_acc', None)}, "
                f"acido: {getattr(ev, 'acid_consumed_acc', None)}, "
                f"agua: {getattr(ev, 'water_consumed_acc', None)}"
            )
            if day_key not in consumption_data:
                consumption_data[day_key] = {}

            consumption_data[day_key].update(
                {
                    "chlorine_consumed_acc": getattr(ev, "chlorine_consumed_acc", 0) or 0,
                    "acid_consumed_acc": getattr(ev, "acid_consumed_acc", 0) or 0,
                    "water_consumed": getattr(ev, "water_consumed_acc", 0) or 0,
                }
            )
        log.info(f"consumption_data após CCD consumption_graphics: {consumption_data}")

    elif has_z1:
        # Usa Z1 (legacy) para consumo de ácido e cloro
        _Event = get_events_model("Z1")
        events = _Event.get_insumes_consumed_last_days(farm.name, date_lower, date_upper)
        for ev in events:
            day_key = ev.created_at.strftime("%Y-%m-%d")
            if day_key not in consumption_data:
                consumption_data[day_key] = {}

            consumption_data[day_key].update(
                {
                    "chlorine_consumed_acc": ev.chlorine_consumed_acc if ev.chlorine_consumed_acc is not None else 0,
                    "acid_consumed_acc": ev.acid_consumed_acc if ev.acid_consumed_acc is not None else 0,
                }
            )

    elif has_wgt:
        # WGT standalone (sem CCD/Z1): é a própria fonte de ácido/cloro. A WGT
        # mede insumo por peso; quando há CCD, o CCD já espelha esses readings
        # (ramo has_ccd acima). get_insumes_consumed_last_days retorna tuplas
        # (created_at, field_name, value) com field_name normalizado
        # (acido/cloro/gas). Ácido/cloro entram no gráfico principal; o gás
        # segue no gráfico WGT dedicado (bloco 2).
        _Event = get_events_model("WGT")
        events = _Event.get_insumes_consumed_last_days(farm.name, date_lower, date_upper)
        field_to_key = {"acido": "acid_consumed_acc", "cloro": "chlorine_consumed_acc"}
        for created_at, field_name, value in events:
            insume_key = field_to_key.get(field_name)
            if not insume_key:
                continue
            day_key = created_at.strftime("%Y-%m-%d")
            if day_key not in consumption_data:
                consumption_data[day_key] = {}
            consumption_data[day_key][insume_key] = float(value or 0)

    # Identifica FLXs associadas ao CCD
    flx_serials_associated_with_ccd = set()
    if has_ccd:
        for ccd_plate_ in plates:
            if ccd_plate_.plate_type != "CCD":
                continue
            ccd_plate = ccd_plate_.to_dict()
            associateds = ccd_plate.get("params", {}).get("associateds_plates", [])
            for ass in associateds:
                if ass.startswith("FLX"):
                    flx_serials_associated_with_ccd.add(ass)

    # Guarda dias que já têm dados de água do CCD (para não duplicar)
    days_with_ccd_water = set()
    for day_key, data in consumption_data.items():
        if "water_consumed" in data and data["water_consumed"] > 0:
            days_with_ccd_water.add(day_key)

    # Adiciona consumo de água (FLX) para dias sem dados do CCD
    for plate_ in plates:
        plate = plate_.to_dict()
        if plate["plate_type"] != "FLX":
            continue

        _Event = get_events_model("FLX")
        events = _Event.get_water_consumed_last_days(farm.name, plate["serial"], date_lower, date_upper)
        for ev in events:
            day_key = ev.created_at.strftime("%Y-%m-%d")
            if day_key not in consumption_data:
                consumption_data[day_key] = {}

            # FLX e CCD associados medem a mesma água via rotas diferentes
            # (uplink direto da FLX vs ESP-NOW → CCD). Firmware não retransmite
            # amostras perdidas, então o MAIOR recupera o que cada rota capturou.
            # Sem associação, somar (pontos de medição distintos).
            try:
                flx_value = float(ev.water_consumed)
            except Exception as e:
                log.error(f"Erro ao processar water_consumed: {e}")
                flx_value = 0.0

            current_water = consumption_data[day_key].get("water_consumed", 0)
            if plate["serial"] in flx_serials_associated_with_ccd and day_key in days_with_ccd_water:
                consumption_data[day_key]["water_consumed"] = max(current_water, flx_value)
            else:
                consumption_data[day_key]["water_consumed"] = current_water + flx_value

    # Adiciona temperatura (FLX)
    _Event = get_events_model("FLX")
    temp_events = _Event.get_mean_temperature_water_consumed_last_days(farm.name, date_lower, date_upper)
    for ev in temp_events:
        day_key = ev.created_at.strftime("%Y-%m-%d")
        if day_key not in consumption_data:
            consumption_data[day_key] = {}

        sensor_name = ev.sensor
        consumption_data[day_key][sensor_name] = round(ev.temperature, 1)

        if sensor_name not in temperature_sensors:
            temperature_sensors.append(sensor_name)

    # Gera gráfico se houver dados de consumo (ácido/cloro/água) ou temperatura
    if consumption_data:
        fname, fname_with_dir = graphics.consume(consumption_data, temperature_sensors)
        image_url = f"https://img.monitora.pro/space/{fname}"
        # Usa upload direto para Meta (mais confiável)
        response_list.append({"type": "image_upload", "url": image_url, "file_path": fname_with_dir})

        quality = _compute_water_data_quality(plates, date_lower, date_upper, consumption_data)
        if quality:
            cobertura_pct = int(round(quality["coverage"] * 100))
            faltando = _format_br_number(quality["missing_samples"])
            perdido = _format_br_number(quality["consumo_perdido_estimado"])
            msg = (
                f"ℹ️ Qualidade do dado: as placas de fluxo enviaram "
                f"{cobertura_pct}% das amostras esperadas no período "
                f"({faltando} faltando). O consumo real pode ter sido "
                f"~{perdido} L maior que o registrado no gráfico."
            )
            response_list.append({"type": "text", "msg": msg})

    # ========================================
    # 2. Gráfico de consumo WGT (dinâmico)
    # ========================================
    wgt_data = {}
    wgt_plate = None
    gas_level_data = {}

    # Se tem CCD, verifica quais WGT estão associadas
    wgt_serials_associated_with_ccd = set()
    if has_ccd:
        for ccd_plate_ in plates:
            if ccd_plate_.plate_type != "CCD":
                continue
            ccd_plate = ccd_plate_.to_dict()
            associateds = ccd_plate.get("params", {}).get("associateds_plates", [])
            for ass in associateds:
                if ass.startswith("WGT"):
                    wgt_serials_associated_with_ccd.add(ass)

    for plate_ in plates:
        if plate_.plate_type != "WGT":
            continue

        wgt_plate = plate_.to_dict()
        plate_serial = wgt_plate.get("serial")

        # Se esta WGT está associada ao CCD, pula
        if plate_serial in wgt_serials_associated_with_ccd:
            continue

        iomap = wgt_plate.get("params", {}).get("iomap", {})

        # Normaliza descrições habilitadas no iomap
        enabled_fields = set()
        for load_key, load_config in iomap.items():
            if load_config.get("status") != "disable":
                description = load_config.get("description", "")

                # Normaliza seguindo a lógica do banco
                normalized = unicodedata.normalize("NFKD", description)
                normalized = "".join([c for c in normalized if not unicodedata.combining(c)])
                normalized = normalized.lower()
                normalized = re.sub(r"[^a-z0-9]+", "_", normalized)
                normalized = normalized.strip("_")

                enabled_fields.add(normalized)

        # Busca eventos de consumo WGT
        _Event = get_events_model("WGT")
        events = _Event.get_insumes_consumed_last_days(farm.name, date_lower, date_upper)

        for ev in events:
            created_at = ev[0]
            field_name = ev[1]
            value = ev[2]

            # Filtra apenas campos habilitados
            if field_name not in enabled_fields:
                continue

            day_key = created_at.strftime("%Y-%m-%d")
            if day_key not in wgt_data:
                wgt_data[day_key] = {}

            wgt_data[day_key][field_name] = value or 0.0

        # Busca capacidade e multiplier do iomap para cálculo de percentual
        load1_config = iomap.get("load1", {})
        capacidade = load1_config.get("capacity", 0)
        multiplier = load1_config.get("multiplier", 1)

        log.info(f"WGT iomap load1 - capacidade: {capacidade}, multiplier: {multiplier}")

        # Busca valores de gás disponível por dia (load1)
        events_gas_level = _Event.get_gas_level_by_day(farm.name, date_lower, date_upper)
        gas_level_data = {}
        for ev in events_gas_level:
            day_key = ev[0].strftime("%Y-%m-%d")
            # O valor de gas já vem em kg do banco (não precisa multiplicar)
            gas_disponivel = float(ev[1] or 0)
            # Calcula percentual
            if capacidade > 0:
                percentual = round((gas_disponivel / capacidade) * 100, 1)
            else:
                percentual = 0
            gas_level_data[day_key] = percentual
            log.info(f"WGT gas level - dia: {day_key}, gas_kg: {gas_disponivel}, percentual: {percentual}%")

    # Gera gráfico WGT só se houver gás de fato (consumo ou nível > 0). Ácido/
    # cloro da WGT já foram pro gráfico principal; sem gás não há 2º gráfico.
    if wgt_data and wgt_plate and _has_gas_signal(wgt_data, gas_level_data):
        fname, fname_with_dir = graphics.consume_wgt(wgt_plate, wgt_data, temperature_sensors, gas_level_data)
        image_url = f"https://img.monitora.pro/space/{fname}"
        # Usa upload direto para Meta (mais confiável)
        response_list.append({"type": "image_upload", "url": image_url, "file_path": fname_with_dir})

    return response_list
