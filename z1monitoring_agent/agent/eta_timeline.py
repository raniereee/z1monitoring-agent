"""Condensação 24h de eventos ETA pra análise do LLM.

Monta uma timeline unificada com transições IOX (bombas, bóias, alarmes
hidráulicos) + marcos das leituras da CCD (ou periféricos, se não houver CCD)
+ estado atual. Reduz ~2000 eventos brutos/dia em ~10-30 linhas de narrativa
que o modelo consegue correlacionar.

Regras importantes:
- CCD é hub ESP-NOW: quando a farm tem CCD, as leituras de pH/ORP/fluxo/
  ácido/cloro vêm do evento CCD. Eventos PHI/ORP/FLX/WGT periféricos são
  redundantes — ignorados pra não duplicar tokens.
- FLX (e o campo "Fluxo de Água" da CCD) pode ser entrada da ETA, saída ou
  recirculação. Quem define é a posição na topologia, não o tipo da placa.
- IOX é filtrada por heurística de description/type pra pegar só os IOs
  ligados ao circuito hidráulico.
"""

from datetime import datetime, timedelta
from typing import Optional

import structlog
from sqlalchemy.sql import text

from z1monitoring_models.dbms import Session
from z1monitoring_models.models.farm import Farm

log = structlog.get_logger()


# Heuristicas de "evento relevante" pra medidas
PH_DELTA = 1.0  # pH mudou >=1.0 entre leituras consecutivas = marco
ORP_DELTA = 100  # ORP mudou >=100 mV = marco
DEBOUNCE_MIN = 5  # gap minimo entre marcos consecutivos do mesmo tipo


def _should_emit(prev_marco_ts, cur_ts, delta, threshold,
                 prev_marco_val=None, cur_val=None):
    """Debouncing com deteccao de cascata.

    Emite marco se:
    - e o primeiro marco da serie
    - passou >=DEBOUNCE_MIN do marco anterior
    - delta entre leituras consecutivas e excepcional (>=2x threshold)
    - OU delta acumulado desde o ultimo marco e grande (>=2x threshold) —
      captura queda livre continua (ex: pH caindo 6.1->4.4->3.1->1.45)
      mesmo dentro da janela de debounce

    Mantem oscilacao filtrada (sensor oscila em torno de um valor) mas
    nao perde informacao quando o sensor esta em trajetoria monotonica.
    """
    if prev_marco_ts is None:
        return True
    gap_min = (cur_ts - prev_marco_ts).total_seconds() / 60
    if gap_min >= DEBOUNCE_MIN:
        return True
    if abs(delta) >= 2 * threshold:
        return True
    if prev_marco_val is not None and cur_val is not None:
        if abs(cur_val - prev_marco_val) >= 2 * threshold:
            return True
    return False


def _cluster_iox_by_minute(events):
    """Agrupa transicoes IOX do mesmo minuto em um unico marco.
    Evita 4-6 linhas de timeline quando varios IOs mudam juntos."""
    out = []
    buckets = {}  # ts -> dict de deltas
    for e in events:
        if e.get("marco") == "iox_transicao":
            ts = e["ts"]
            buckets.setdefault(ts, {}).update(e.get("iox_delta", {}))
        else:
            out.append(e)
    for ts, deltas in buckets.items():
        marco = "iox_cluster" if len(deltas) > 1 else "iox_transicao"
        out.append({"ts": ts, "marco": marco, "iox_delta": deltas})
    return out


IOX_HYDRAULIC_KEYWORDS_SENSOR = (
    "boia",
    "automático",
    "automatico",
    "recalque",
    "tratamento",
    "pac",
    "pré limpeza",
    "pre limpeza",
    "tempo",
)
IOX_HYDRAULIC_KEYWORDS_FAIL = (
    "água",
    "agua",
    "bomba",
    "ozônio",
    "ozonio",
    "recirculação",
    "recirculacao",
    "recalque",
    "dosadora",
)


def _is_hydraulic_io(description: str, io_type: str) -> bool:
    """Classifica se um IO do iomap é relevante pro circuito hidráulico."""
    if not description:
        return False
    d = description.lower()
    if io_type == "device":
        # bombas e saídas de comando sempre entram
        return True
    if io_type == "sensor":
        return any(k in d for k in IOX_HYDRAULIC_KEYWORDS_SENSOR)
    if io_type == "fail":
        return any(k in d for k in IOX_HYDRAULIC_KEYWORDS_FAIL)
    return False


def _farm_has_ccd(farm_id: int) -> bool:
    with Session() as session:
        row = session.execute(
            text(
                "SELECT 1 FROM plates WHERE farm_id = :fid "
                "AND plate_type = 'CCD' LIMIT 1"
            ),
            {"fid": farm_id},
        ).first()
    return row is not None


def _get_plates_by_type(farm_id: int, plate_type: str):
    """Retorna [(serial, owner, iomap_or_params)] das placas da farm."""
    with Session() as session:
        rows = session.execute(
            text(
                "SELECT serial, owner, params FROM plates "
                "WHERE farm_id = :fid AND plate_type = :pt"
            ),
            {"fid": farm_id, "pt": plate_type},
        ).all()
    return [(r[0], r[1], r[2] or {}) for r in rows]


def _fmt_ts(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%d %H:%M")


def _collect_iox_events(farm_id: int, start: datetime, end: datetime):
    """Retorna (timeline_events, estado_atual, agregado_24h) dos IOs hidráulicos.

    timeline_events: lista de marcos de transição 0↔1
    estado_atual: dict description -> 0/1 (último valor conhecido no fim da janela)
    agregado_24h: lista de dicts por IO com tempo ligado total e contagem de ligações
    """
    iox_plates = _get_plates_by_type(farm_id, "IOX")
    timeline: list = []
    estado_atual: dict = {}
    agregado: dict = {}  # descricao -> {transicoes, tempo_on_min, ...}

    for serial, owner, params in iox_plates:
        iomap = (params or {}).get("iomap") or {}
        relevant_descs = set()
        for io_key, io_data in iomap.items():
            if not isinstance(io_data, dict):
                continue
            desc = io_data.get("description", "")
            io_type = io_data.get("type", "")
            if _is_hydraulic_io(desc, io_type):
                relevant_descs.add(desc)

        if not relevant_descs:
            continue

        with Session() as session:
            rows = session.execute(
                text(
                    "SELECT created_at, readings FROM events_iox "
                    "WHERE serial_plate = :serial "
                    "AND created_at >= :start AND created_at < :end "
                    "ORDER BY created_at"
                ),
                {"serial": serial, "start": start, "end": end},
            ).all()

        prev_values: dict = {}
        on_since: dict = {}  # descricao -> ts quando ligou (pra medir duração)

        for ts, readings in rows:
            if not isinstance(readings, dict):
                continue
            for desc in relevant_descs:
                if desc not in readings:
                    continue
                raw = readings.get(desc)
                try:
                    current = int(raw)
                except (TypeError, ValueError):
                    continue

                prev = prev_values.get(desc)
                if prev is None:
                    prev_values[desc] = current
                    if current == 1:
                        on_since[desc] = ts
                    continue

                if current != prev:
                    timeline.append(
                        {
                            "ts": _fmt_ts(ts),
                            "marco": "iox_transicao",
                            "iox_delta": {desc: f"{prev}→{current}"},
                        }
                    )
                    agg = agregado.setdefault(
                        desc,
                        {"transicoes": 0, "tempo_on_min": 0, "ligou_count": 0},
                    )
                    agg["transicoes"] += 1
                    if current == 1:
                        # ligou agora
                        agg["ligou_count"] += 1
                        on_since[desc] = ts
                    else:
                        # desligou — soma duração
                        started = on_since.pop(desc, None)
                        if started is not None:
                            agg["tempo_on_min"] += int(
                                (ts - started).total_seconds() // 60
                            )
                    prev_values[desc] = current

        # fecha IOs que terminaram a janela ainda ligados
        for desc, started in on_since.items():
            agg = agregado.setdefault(
                desc, {"transicoes": 0, "tempo_on_min": 0, "ligou_count": 0}
            )
            agg["tempo_on_min"] += int((end - started).total_seconds() // 60)

        # estado_atual = valores do último evento da janela
        if rows:
            _, last_readings = rows[-1]
            if isinstance(last_readings, dict):
                for desc in relevant_descs:
                    if desc in last_readings:
                        try:
                            estado_atual[desc] = int(last_readings[desc])
                        except (TypeError, ValueError):
                            pass

    return timeline, estado_atual, agregado


def _collect_ccd_events(farm_id: int, start: datetime, end: datetime):
    """Eventos relevantes do CCD: pH cruzando delta, ABS, modo dosadora,
    fluxo cruzando zero, perda ESP-NOW.
    """
    ccd_plates = _get_plates_by_type(farm_id, "CCD")
    events: list = []

    for serial, owner, _params in ccd_plates:
        with Session() as session:
            rows = session.execute(
                text(
                    "SELECT created_at, readings FROM events_ccd "
                    "WHERE serial_plate = :serial "
                    "AND created_at >= :start AND created_at < :end "
                    "ORDER BY created_at"
                ),
                {"serial": serial, "start": start, "end": end},
            ).all()

        prev_ph = prev_orp = None
        last_ph_marco_ts = last_orp_marco_ts = None
        last_ph_marco_val = last_orp_marco_val = None
        prev_flags = {}  # chave -> 0/1
        prev_fluxo = None
        prev_modo_acido = prev_modo_cloro = None

        watched_flags = (
            "Falha: Acionado ABS de Consumo Máximo Ácido",
            "Falha: Acionado ABS de Consumo Máximo Cloro",
            "Falha: Falta de recirculação",
            "Falha: Sensor de pH",
            "Falha: Sensor de ORP",
            "Falha: Sensor de Fluxo",
            "Falha: Sensor de Peso",
            "Falha: Dosagem de Ácido Ineficiente",
            "Falha: Dosagem de Cloro Ineficiente",
            "ABS Ácido Desarmado Manualmente",
            "ABS Cloro Desarmado Manualmente",
        )

        for ts, readings in rows:
            if not isinstance(readings, dict):
                continue

            # --- pH delta (com debouncing) ---
            try:
                ph = float(readings.get("PH"))
            except (TypeError, ValueError):
                ph = None
            if ph is not None:
                if prev_ph is not None:
                    delta_consec = ph - prev_ph
                    # referencia pra acumulado: ultimo marco se existe, senao 1a leitura
                    ref = last_ph_marco_val if last_ph_marco_val is not None else prev_ph
                    delta_ref = ph - ref
                    hit_consec = abs(delta_consec) >= PH_DELTA
                    hit_cumulative = abs(delta_ref) >= PH_DELTA
                    if (hit_consec or hit_cumulative) and _should_emit(
                        last_ph_marco_ts, ts, delta_consec, PH_DELTA,
                        last_ph_marco_val, ph,
                    ):
                        events.append(
                            {
                                "ts": _fmt_ts(ts),
                                "marco": "ph_delta",
                                "medidas": {
                                    "ph": round(ph, 2),
                                    "delta": round(delta_ref, 2),
                                },
                                "fonte": "CCD",
                            }
                        )
                        last_ph_marco_ts = ts
                        last_ph_marco_val = ph
                prev_ph = ph

            # --- ORP delta (com debouncing) ---
            try:
                orp = float(readings.get("ORP"))
            except (TypeError, ValueError):
                orp = None
            if orp is not None:
                if prev_orp is not None:
                    delta_consec = orp - prev_orp
                    ref = last_orp_marco_val if last_orp_marco_val is not None else prev_orp
                    delta_ref = orp - ref
                    hit_consec = abs(delta_consec) >= ORP_DELTA
                    hit_cumulative = abs(delta_ref) >= ORP_DELTA
                    if (hit_consec or hit_cumulative) and _should_emit(
                        last_orp_marco_ts, ts, delta_consec, ORP_DELTA,
                        last_orp_marco_val, orp,
                    ):
                        events.append(
                            {
                                "ts": _fmt_ts(ts),
                                "marco": "orp_delta",
                                "medidas": {
                                    "orp": round(orp, 0),
                                    "delta": round(delta_ref, 0),
                                },
                                "fonte": "CCD",
                            }
                        )
                        last_orp_marco_ts = ts
                        last_orp_marco_val = orp
                prev_orp = orp

            # --- Fluxo cruzando zero ---
            try:
                fluxo = float(readings.get("Fluxo de Água"))
            except (TypeError, ValueError):
                fluxo = None
            if fluxo is not None and prev_fluxo is not None:
                if (prev_fluxo == 0 and fluxo > 0) or (
                    prev_fluxo > 0 and fluxo == 0
                ):
                    events.append(
                        {
                            "ts": _fmt_ts(ts),
                            "marco": "fluxo_cruzou_zero",
                            "medidas": {"fluxo": fluxo, "anterior": prev_fluxo},
                            "fonte": "CCD",
                        }
                    )
            prev_fluxo = fluxo

            # --- Flags (ABS, Falhas, etc) 0↔1 ---
            for flag in watched_flags:
                if flag not in readings:
                    continue
                try:
                    val = int(readings.get(flag))
                except (TypeError, ValueError):
                    continue
                prev = prev_flags.get(flag)
                if prev is not None and val != prev:
                    events.append(
                        {
                            "ts": _fmt_ts(ts),
                            "marco": "flag_mudou",
                            "ccd": {flag: f"{prev}→{val}"},
                        }
                    )
                prev_flags[flag] = val

            # --- Modo dosadora ---
            modo_acido = readings.get("Modo Dosadora Ácido")
            if (
                prev_modo_acido is not None
                and modo_acido is not None
                and modo_acido != prev_modo_acido
            ):
                events.append(
                    {
                        "ts": _fmt_ts(ts),
                        "marco": "modo_dosadora_mudou",
                        "ccd": {
                            "Modo Dosadora Ácido": f"{prev_modo_acido}→{modo_acido}"
                        },
                    }
                )
            prev_modo_acido = modo_acido

            modo_cloro = readings.get("Modo Dosadora Cloro")
            if (
                prev_modo_cloro is not None
                and modo_cloro is not None
                and modo_cloro != prev_modo_cloro
            ):
                events.append(
                    {
                        "ts": _fmt_ts(ts),
                        "marco": "modo_dosadora_mudou",
                        "ccd": {
                            "Modo Dosadora Cloro": f"{prev_modo_cloro}→{modo_cloro}"
                        },
                    }
                )
            prev_modo_cloro = modo_cloro

    return events


def _collect_peripheral_events(farm_id: int, start: datetime, end: datetime):
    """Fallback pra farms sem CCD: lê pH do PHI, ORP do ORP, fluxo do FLX.
    Aplica mesma heurística de delta.
    """
    events: list = []

    # PHI -> pH
    for serial, owner, _ in _get_plates_by_type(farm_id, "PHI"):
        with Session() as session:
            rows = session.execute(
                text(
                    "SELECT created_at, readings FROM events_phi "
                    "WHERE serial_plate = :s "
                    "AND created_at >= :start AND created_at < :end "
                    "ORDER BY created_at"
                ),
                {"s": serial, "start": start, "end": end},
            ).all()
        prev_ph = None
        last_ph_marco_ts = None
        last_ph_marco_val = None
        for ts, readings in rows:
            if not isinstance(readings, dict):
                continue
            try:
                ph = float(readings.get("ph"))
            except (TypeError, ValueError):
                continue
            if prev_ph is not None:
                delta_consec = ph - prev_ph
                ref = last_ph_marco_val if last_ph_marco_val is not None else prev_ph
                delta_ref = ph - ref
                hit_consec = abs(delta_consec) >= PH_DELTA
                hit_cumulative = abs(delta_ref) >= PH_DELTA
                if (hit_consec or hit_cumulative) and _should_emit(
                    last_ph_marco_ts, ts, delta_consec, PH_DELTA,
                    last_ph_marco_val, ph,
                ):
                    events.append(
                        {
                            "ts": _fmt_ts(ts),
                            "marco": "ph_delta",
                            "medidas": {
                                "ph": round(ph, 2),
                                "delta": round(delta_ref, 2),
                            },
                            "fonte": f"PHI:{serial}",
                        }
                    )
                    last_ph_marco_ts = ts
                    last_ph_marco_val = ph
            prev_ph = ph

    # ORP -> ORP
    for serial, owner, _ in _get_plates_by_type(farm_id, "ORP"):
        with Session() as session:
            rows = session.execute(
                text(
                    "SELECT created_at, readings FROM events_orp "
                    "WHERE serial_plate = :s "
                    "AND created_at >= :start AND created_at < :end "
                    "ORDER BY created_at"
                ),
                {"s": serial, "start": start, "end": end},
            ).all()
        prev_orp = None
        last_orp_marco_ts = None
        last_orp_marco_val = None
        for ts, readings in rows:
            if not isinstance(readings, dict):
                continue
            try:
                orp = float(readings.get("orp"))
            except (TypeError, ValueError):
                continue
            if prev_orp is not None:
                delta_consec = orp - prev_orp
                ref = last_orp_marco_val if last_orp_marco_val is not None else prev_orp
                delta_ref = orp - ref
                hit_consec = abs(delta_consec) >= ORP_DELTA
                hit_cumulative = abs(delta_ref) >= ORP_DELTA
                if (hit_consec or hit_cumulative) and _should_emit(
                    last_orp_marco_ts, ts, delta_consec, ORP_DELTA,
                    last_orp_marco_val, orp,
                ):
                    events.append(
                        {
                            "ts": _fmt_ts(ts),
                            "marco": "orp_delta",
                            "medidas": {
                                "orp": round(orp, 0),
                                "delta": round(delta_ref, 0),
                            },
                            "fonte": f"ORP:{serial}",
                        }
                    )
                    last_orp_marco_ts = ts
                    last_orp_marco_val = orp
            prev_orp = orp

    # FLX -> fluxo cruzando zero
    for serial, owner, _ in _get_plates_by_type(farm_id, "FLX"):
        with Session() as session:
            rows = session.execute(
                text(
                    "SELECT created_at, readings FROM events_flx "
                    "WHERE serial_plate = :s "
                    "AND created_at >= :start AND created_at < :end "
                    "ORDER BY created_at"
                ),
                {"s": serial, "start": start, "end": end},
            ).all()
        prev_fluxo = None
        for ts, readings in rows:
            if not isinstance(readings, dict):
                continue
            try:
                fluxo = float(readings.get("water_flow", readings.get("fluxo")))
            except (TypeError, ValueError):
                continue
            if prev_fluxo is not None and (
                (prev_fluxo == 0 and fluxo > 0)
                or (prev_fluxo > 0 and fluxo == 0)
            ):
                events.append(
                    {
                        "ts": _fmt_ts(ts),
                        "marco": "fluxo_cruzou_zero",
                        "medidas": {"fluxo": fluxo, "anterior": prev_fluxo},
                        "fonte": f"FLX:{serial}",
                    }
                )
            prev_fluxo = fluxo

    return events


def condense_eta_timeline(
    farm_id: int,
    window_hours: int = 24,
    end: Optional[datetime] = None,
) -> dict:
    """Gera timeline 24h condensada da ETA pra análise do LLM.

    Args:
        farm_id: id da farm (tabela farms)
        window_hours: tamanho da janela em horas (default 24)
        end: fim da janela (default now)

    Returns:
        dict com:
        - timeline_24h: lista ordenada de marcos (IOX transições + CCD/periféricos)
        - iox_estado_atual: dict description -> 0/1 (valores no fim da janela)
        - iox_24h_agregado: lista por IO com tempo ligado, ligações, transições
        - meta: farm_name, topologia, fonte_medidas ("CCD" ou "periféricos")

        Retorna {} se farm não existe.
    """
    farm = Farm.load_by_id(farm_id)
    if not farm:
        return {}

    end_dt = end or datetime.now()
    start_dt = end_dt - timedelta(hours=window_hours)

    has_ccd = _farm_has_ccd(farm_id)
    fonte_medidas = "CCD" if has_ccd else "periféricos"

    iox_timeline, iox_state, iox_agg = _collect_iox_events(
        farm_id, start_dt, end_dt
    )

    if has_ccd:
        measure_events = _collect_ccd_events(farm_id, start_dt, end_dt)
    else:
        measure_events = _collect_peripheral_events(farm_id, start_dt, end_dt)

    # Agrupa transicoes IOX do mesmo minuto num unico marco (reduz ruido)
    iox_timeline = _cluster_iox_by_minute(iox_timeline)

    timeline = sorted(
        iox_timeline + measure_events, key=lambda e: e["ts"]
    )

    iox_agregado_list = [
        {
            "descricao": desc,
            "transicoes": a["transicoes"],
            "ligou_count": a["ligou_count"],
            "tempo_on_min": a["tempo_on_min"],
            "estado_final": iox_state.get(desc),
        }
        for desc, a in sorted(iox_agg.items())
    ]

    topology = farm.topology if isinstance(farm.topology, dict) else None

    return {
        "meta": {
            "farm_id": farm_id,
            "farm_name": farm.name,
            "janela": {
                "inicio": _fmt_ts(start_dt),
                "fim": _fmt_ts(end_dt),
                "horas": window_hours,
            },
            "fonte_medidas": fonte_medidas,
            "topologia": topology,
        },
        "timeline_24h": timeline,
        "iox_estado_atual": iox_state,
        "iox_24h_agregado": iox_agregado_list,
    }
