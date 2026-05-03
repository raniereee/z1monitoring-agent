from z1monitoring_models.models.active_chats import ActiveChats
from z1monitoring_models.models.plates import Plate
from z1monitoring_models.models.choose_event_model import get_events_model
from z1monitoring_models.dbms import Session
import datetime
import structlog
from z1monitoring_models.constants import PLATES_TYPES

log = structlog.get_logger()


class Conversation:
    chat = None
    user = None
    principal_client = None


def get_weather():
    now = datetime.datetime.now()
    weather = "Olá"
    if now.hour > 0 and now.hour < 12:
        weather = "Bom dia"
    if now.hour >= 12 and now.hour < 18:
        weather = "Boa tarde"
    if now.hour >= 18 and now.hour <= 23:
        weather = "Boa noite"

    return weather


def send_not_plate_found(message):
    pass


def send_dont_got_message(message):
    pass


def set_status(chat, status):
    chat.status = status
    ActiveChats.update(chat)


def mount_header_farm(farm):
    msg = f"""*{farm.name}*\n\n"""
    return msg


def mount_only_ph(plate, last_ev):
    msg = ""
    if not last_ev:
        msg += """Essa placa """
        msg += f"""({plate.get("description", "pH")})"""

        msg += """ ainda não possui leituras. ⏳ Aguardando...!\n\n"""
        return msg

    msg += f"""📆 {last_ev["created_at"].strftime("%H:%M:%S %d/%m/%Y")}\n\n"""
    if plate["have_communication"]:
        msg += """📡 *Online*\n"""
    else:
        msg += """❌ *Offline* \n"""

    if "max_ph" in plate["params"]["sensors_ranges"]:
        if (
            float(last_ev["ph"]) > plate["params"]["sensors_ranges"]["max_ph"]
            or float(last_ev["ph"]) < plate["params"]["sensors_ranges"]["min_ph"]
        ):
            msg += f"""❌ *pH*          : {last_ev["ph"]}\n\n"""
        else:
            msg += f"""✅ *pH*          : {last_ev["ph"]}\n\n"""

    return msg


def mount_only_audio_ph(plate, last_ev):
    msg = ""

    log.info(plate)
    if not last_ev:
        msg += """Essa placa """
        msg += f"""({plate.get("description", "pH")})"""
        msg += """ ainda não possui leituras. ⏳ Aguardando...!\n\n"""
        return msg

    if "max_ph" in plate["params"]["sensors_ranges"]:
        if (
            float(last_ev["ph"]) > plate["params"]["sensors_ranges"]["max_ph"]
            or float(last_ev["ph"]) < plate["params"]["sensors_ranges"]["min_ph"]
        ):
            msg += f"""pH : {last_ev["ph"]}\n"""
        else:
            msg += f"""pH  : {last_ev["ph"]}\n"""

    msg += f"""as {last_ev["created_at"].strftime("%H:%M %d/%m/%Y")}\n\n"""

    return msg


def mount_only_orp(plate, last_ev):
    msg = ""
    if not last_ev:
        msg += """Essa placa """
        msg += f"""({plate.get("description", "ORP")})"""
        msg += """ ainda não possui leituras. ⏳ Aguardando...!\n\n"""
        return msg

    msg += f"""📆 {last_ev["created_at"].strftime("%H:%M:%S %d/%m/%Y")}\n\n"""
    if plate["have_communication"]:
        msg += """📡 *Online*\n"""
    else:
        msg += """❌ *Offline* \n"""

    if "max_orp" in plate["params"]["sensors_ranges"]:
        if (
            float(last_ev["orp"]) > plate["params"]["sensors_ranges"]["max_orp"]
            or float(last_ev["orp"]) < plate["params"]["sensors_ranges"]["min_orp"]
        ):
            msg += f"""❌ *ORP*          : {last_ev["orp"]}\n\n"""
        else:
            msg += f"""✅ *ORP*          : {last_ev["orp"]}\n\n"""

    return msg


def mount_only_temperature(plate, last_ev):
    msg = ""
    if not last_ev:
        msg += """Essa placa """
        msg += f"""({plate.get("description", "Temperatura")})"""

        msg += """ ainda não possui leituras. ⏳ Aguardando...!\n\n"""
        return msg

    msg += f"""📆 {last_ev["created_at"].strftime("%H:%M:%S %d/%m/%Y")}\n\n"""
    if plate["have_communication"]:
        msg += """📡 *Online*\n"""
    else:
        msg += """❌ *Offline* \n"""

    if "max_temperature" in plate["params"]["sensors_ranges"]:
        if float(last_ev["temperature"]) > float(plate["params"]["sensors_ranges"]["max_temperature"]) or float(
            last_ev["temperature"]
        ) < float(plate["params"]["sensors_ranges"]["min_temperature"]):
            msg += f"""❌ *Temperatura* : {last_ev["temperature"]} °C\n"""
        else:
            msg += f"""✅ *Temperatura* : {last_ev["temperature"]} °C\n"""

    return msg


def mount_realtime_sensors(plate, last_ev):
    msg = ""
    if not last_ev:
        msg += """Essa placa """
        msg += f"""({plate.get("description", "Z1 Smart")})"""
        msg += """ ainda não possui leituras. ⏳ Aguardando...!\n\n"""
        return msg

    if plate["have_communication"]:
        msg += f"""📆 {last_ev["created_at"].strftime("%H:%M:%S %d/%m/%Y")}\n\n"""
        msg += """📡 *Online*\n"""
    else:
        msg += """❌  Seu sistema *SMARTPH* está *SEM INTERNET*!\n\n"""
        msg += """*Por favor, verifique sua internet ou reinicie seu equipamento*\n\n"""
        return msg

    if "max_ph" in plate["params"]["sensors_ranges"]:
        if (
            float(last_ev["ph"]) > plate["params"]["sensors_ranges"]["max_ph"]
            or float(last_ev["ph"]) < plate["params"]["sensors_ranges"]["min_ph"]
        ):
            msg += f"""❌ *pH*          : {last_ev["ph"]}\n"""
        else:
            msg += f"""✅ *pH*          : {last_ev["ph"]}\n"""

    if "max_orp" in plate["params"]["sensors_ranges"]:
        if (
            float(last_ev["orp"]) > plate["params"]["sensors_ranges"]["max_orp"]
            or float(last_ev["orp"]) < plate["params"]["sensors_ranges"]["min_orp"]
        ):
            msg += f"""❌ *ORP*          : {last_ev["orp"]}\n"""
        else:
            msg += f"""✅ *ORP*          : {last_ev["orp"]}\n"""

    if "max_temperature" in plate["params"]["sensors_ranges"]:
        if float(last_ev["temperature"]) > float(plate["params"]["sensors_ranges"]["max_temperature"]) or float(
            last_ev["temperature"]
        ) < float(plate["params"]["sensors_ranges"]["min_temperature"]):
            msg += f"""❌ *Temperatura* : {last_ev["temperature"]} °C\n"""
        else:
            msg += f"""✅ *Temperatura* : {last_ev["temperature"]} °C\n"""

    if "max_acid" in plate["params"]["sensors_ranges"]:
        if (
            float(last_ev["acid"]) > plate["params"]["sensors_ranges"]["max_acid"]
            or float(last_ev["acid"]) < plate["params"]["sensors_ranges"]["min_acid"]
        ):
            msg += f"""❌ *Qtd. Ácido*       : {last_ev["acid"]} Kg\n"""
        else:
            msg += f"""✅ *Qtd. Ácido*       : {last_ev["acid"]} Kg\n"""

    if "max_chlorine" in plate["params"]["sensors_ranges"]:
        if (
            float(last_ev["chlorine"]) > plate["params"]["sensors_ranges"]["max_chlorine"]
            or float(last_ev["chlorine"]) < plate["params"]["sensors_ranges"]["min_chlorine"]
        ):
            msg += f"""❌ *Qtd. Cloro*       : {last_ev["chlorine"]} Kg\n"""
        else:
            msg += f"""✅ *Qtd. Cloro*       : {last_ev["chlorine"]} Kg\n"""

    msg += "\n📊 Consumo Diário\n\n"
    msg += f"""    - *Ácido*  : {round(last_ev.get("acid_consumed_acc", "0.0"), 1)} Kg\n"""
    msg += f"""    - *Cloro*  : {round(last_ev.get("chlorine_consumed_acc", "0.0"), 1)} Kg\n\n\n"""

    return msg


def mount_realtime_orp(plate, last_ev):
    msg = ""
    if not last_ev:
        msg += """Essa placa """
        msg += f"""({plate.get("description", "ORP")})"""
        msg += """ ainda não possui leituras. ⏳ Aguardando...!\n\n"""
        return msg

    try:
        msg += f"""📆 {last_ev["created_at"].strftime("%H:%M:%S %d/%m/%Y")}\n\n"""
    except Exception as e:
        e
        pass

    if plate["have_communication"]:
        msg += """📡 *Online*\n"""
    else:
        msg += """❌ *Offline* \n"""

    if "max_orp" in plate["params"]["sensors_ranges"]:
        if (
            float(last_ev["orp"]) > plate["params"]["sensors_ranges"]["max_orp"]
            or float(last_ev["orp"]) < plate["params"]["sensors_ranges"]["min_orp"]
        ):
            msg += f"""❌ *ORP* {plate.get("description", " ")}     : {last_ev["orp"]}\n"""
        else:
            msg += f"""✅ *ORP* {plate.get("description", " ")}     : {last_ev["orp"]}\n"""

    msg += f"""*Temperatura*          : {last_ev["temperature"]} ºC\n\n"""

    return msg


def mount_realtime_phi(plate, last_ev):
    msg = ""
    if not last_ev:
        msg += """Essa placa """
        msg += f"""({plate.get("description", "PHI")})"""
        msg += """ ainda não possui leituras. ⏳ Aguardando...!\n\n"""
        return msg

    msg += f"""📆 {last_ev["created_at"].strftime("%H:%M:%S %d/%m/%Y")}\n\n"""
    if plate["have_communication"]:
        msg += """📡 *Online*\n"""
    else:
        msg += """❌ *Offline* \n"""

    if "max_ph" in plate["params"]["sensors_ranges"]:
        if (
            float(last_ev["ph"]) > plate["params"]["sensors_ranges"]["max_ph"]
            or float(last_ev["ph"]) < plate["params"]["sensors_ranges"]["min_ph"]
        ):
            msg += f"""❌ *pH* {plate.get("description", " ")}     : {last_ev["ph"]}\n"""
        else:
            msg += f"""✅ *pH* {plate.get("description", " ")}     : {last_ev["ph"]}\n"""

    msg += f"""*Temperatura*          : {last_ev["temperature"]} ºC\n\n"""

    return msg


def mount_realtime_water_flow(plate, last_ev):
    msg = ""
    msg += f"""🚰 *Hidrômetro*    : *{plate.get("description", " ")}*\n"""
    if last_ev is None:
        msg += "⚠️ Sem dados no momento!\n\n"
        return msg

    if plate["have_communication"]:
        msg += """📡 *Online*\n"""
        msg += f"""📆 {last_ev["created_at"].strftime("%H:%M:%S %d/%m/%Y")}\n\n"""
    else:
        msg += """❌  Seu sistema *SMARTFLX* está *SEM INTERNET*!\n\n"""
        msg += """*Por favor, verifique sua internet ou reinicie seu equipamento*\n\n"""
        return msg

    if last_ev["water_flow"] > 1000:
        flow = round(last_ev["water_flow"] / 1000, 1)
        msg += f"""*Fluxo*                      : {flow} mil L/min\n"""
    else:
        msg += f"""*Fluxo*                      : {round(last_ev["water_flow"])} L/min\n"""

    # msg += f"""*Temperatura*          : {last_ev["temperature"]} ºC\n"""

    if "water_consumed" in last_ev:
        # if round(last_ev["water_consumed"]) > 1000000:
        #    water_consumed = int(round(last_ev["water_consumed"])/1000)
        #    msg += f"""*Consumo do dia*   : {water_consumed} mil L\n"""
        # else:
        water_consumed = round(last_ev["water_consumed"])
        msg += f"""*Consumo do dia*   : {water_consumed} L\n"""

    msg += "\n\n"
    return msg


def mount_realtime_chlorine(plate, last_ev):
    msg = ""
    msg += f"""*Consumo de Cloro*    : *{plate["description"] if plate["description"] else ""}*\n"""
    if last_ev is None:
        msg += "⚠️ Sem dados no momento!\n\n"
        return msg

    if plate["have_communication"]:
        msg += """📡 *Online*\n"""
        msg += f"""📆 {last_ev["created_at"].strftime("%H:%M:%S %d/%m/%Y")}\n\n"""
    else:
        msg += """❌  Seu sistema *SMARTFLX* está *SEM INTERNET*!\n\n"""
        msg += """*Por favor, verifique sua internet ou reinicie seu equipamento*\n\n"""
        return msg

    if "chlorine_consumed_acc" in last_ev:
        chlorine_consumed_acc = round(last_ev["chlorine_consumed_acc"])
        msg += f"""*Consumo do dia*   : {chlorine_consumed_acc} Kg\n"""
    else:
        msg += """*Consumo do dia*   : 0.0 Kg\n"""

    msg += "\n\n"
    return msg


def mount_realtime_acid(plate, last_ev):
    msg = ""
    msg += f"""*Consumo de Acido*    : *{plate["description"] if plate["description"] else ""}*\n"""
    if last_ev is None:
        msg += "⚠️ Sem dados no momento!\n\n"
        return msg

    if plate["have_communication"]:
        msg += """📡 *Online*\n"""
        msg += f"""📆 {last_ev["created_at"].strftime("%H:%M:%S %d/%m/%Y")}\n\n"""
    else:
        msg += """❌  Seu sistema *SMARTFLX* está *SEM INTERNET*!\n\n"""
        msg += """*Por favor, verifique sua internet ou reinicie seu equipamento*\n\n"""
        return msg

    if "chlorine_consumed_acc" in last_ev:
        chlorine_consumed_acc = round(last_ev["chlorine_consumed_acc"])
        msg += f"""*Consumo do dia*   : {chlorine_consumed_acc} Kg\n"""
    else:
        msg += """*Consumo do dia*   : 0.0 Kg\n"""

    msg += "\n\n"
    return msg


def mount_realtime_water_level(plate, last_ev):
    msg = ""
    if not last_ev:
        msg += """Essa placa """
        msg += f"""({plate.get("description", "NVL")})"""
        msg += """ ainda não possui leituras. ⏳ Aguardando...!\n\n"""
        return msg

    msg += f"""💧 *Reservatório*  : *{plate["description"]}*\n"""
    msg += f"""📆 {last_ev["created_at"].strftime("%H:%M:%S %d/%m/%Y")}\n"""
    msg += f""" *Volume*                : acima de {last_ev["water_level"]} %\n"""
    msg += """*ATENÇÃO*: seu sistema possui 3 estágios de volume. *A marcação indica apenas a faixa de volume*.\n\n"""

    msg += "\n\n"
    return msg


def mount_realtime_clpcg(plate, last_ev, iomap):
    msg = ""
    if not last_ev:
        msg += """Essa placa """
        msg += f"""({plate.get("description", "CLP")})"""

        msg += """ ainda não possui leituras. ⏳ Aguardando...!\n\n"""
        return msg

    msg += f"""📟 *Ambiência*  : *{plate["description"]}*\n"""
    msg += f"""📆 {last_ev["created_at"].strftime("%H:%M:%S %d/%m/%Y")}\n"""

    if plate["have_communication"]:
        msg += """📡 *Online*\n"""
    else:
        msg += """❌  Sua *Ambiêcia* está *SEM INTERNET*!\n\n"""
        msg += """*Por favor, verifique sua internet ou reinicie seu equipamento*\n\n"""
        return msg

    # ordered_last_ev = dict( sorted(last_ev.items(), key=lambda x: x[0].lower()))
    msg += f"""
    ----------------------------
    *Idade do Lote*: 📝{last_ev.get("Idade do Lote")} Dias\n
    *Temperatura Desejada*🌡️:\n
                    {last_ev.get("Temperatura Desejada")} °C

    ----------------------------
    *SENSORES:*

    🌡️ Temperatura : {last_ev.get("Temperatura")} °C
    🧭 Pressão     : {last_ev.get("Pressão")} Pa
    🍃 CO₂         : {last_ev.get("CO2")} ppm
    🔊 Pânico      : {last_ev.get("Pânico")} IA

    """

    if last_ev.get("Habilita Modo Curva") == 1:
        msg += f"""
    ----------------------------
    *CURVAS TEMPERATURA*:📝
    IDADE🐤 |  TEMPERATURA🌡️
    ----------------------------
    {last_ev.get("Idade do Lote Curva 1")} DIAS   |      {last_ev.get("Temperatura Desejada Curva Valor 1")} °C
    {last_ev.get("Idade do Lote Curva 2")} DIAS   |      {last_ev.get("Temperatura Desejada Curva Valor 2")} °C
    {last_ev.get("Idade do Lote Curva 3")} DIAS   |      {last_ev.get("Temperatura Desejada Curva Valor 3")} °C
    {last_ev.get("Idade do Lote Curva 4")} DIAS   |      {last_ev.get("Temperatura Desejada Curva Valor 4")} °C
    {last_ev.get("Idade do Lote Curva 5")} DIAS   |      {last_ev.get("Temperatura Desejada Curva Valor 5")} °C
    {last_ev.get("Idade do Lote Curva 6")} DIAS   |      {last_ev.get("Temperatura Desejada Curva Valor 6")} °C
    ----------------------------\n\n"""

    msg += f"""*Parâmetros Temperatura*:🌡️ \n
    📝Temp Desejada:   {last_ev.get("Temperatura Desejada Alterar")} °C
    📝Temp Alta Alarme:   {last_ev.get("Valor Temperatura Alarme")} °C
    📝Temp Liga Exaustor  {last_ev.get("Valor Temperatura Liga Exaustor")} °C
    📝Temp Baixa Alarme:  {last_ev.get("Valor Temperatura Alarme Baixa")} °C
    ----------------------------

    *Parâmetros Pressão*: 🧭
    📝Pressão minima: {last_ev.get("Valor Alarme Pressão Mínima")} Pa
    ----------------------------

    *Parâmetros Pânico*: 🔊
    📝 Panico valor liga : \n                  {last_ev.get("Valor Índice Atividade Ligar Pânico")} IA
    🕒 Hora Liga Panico : \n                  {last_ev.get("Valor Horário Liga Pânico")}
    🕒 Hora Desliga Panico : \n                  {last_ev.get("Valor Horário Desliga Pânico")}
    ----------------------------

    *Tempo Restante Sem Vent.⏳:*
        {last_ev.get("Tempo Restante Sem Ventilação Mínima")}
    ----------------------------
    Modo:
        {"* Curva 📈" if last_ev.get("Habilita Modo Curva") == 1 else "* Desejada 🌡️"}

    ----------------------------------
    💨 *Exaustor Ligado por:*
    🌡️ Temp        : {"✅" if last_ev.get("Exaustor Ligado por Temperatura") == 1 else "⚪"}
    🍃 CO2          : {"✅" if last_ev.get("Exaustor Ligado por Co2") == 1 else "⚪"}
    🕒 Sem Vent. : {"✅" if last_ev.get("Exaustor Ligado por Tempo Limite sem Ventilação Mínima") == 1 else "⚪"}

    """

    msg_sensors = ""
    for key, value in iomap.items():
        if value.get("type") != "fail":
            continue

        if last_ev.get(value.get("description")):
            msg_sensors += f""" *{value.get("description").capitalize()}*       : ❌\n"""

    if msg_sensors:
        msg += "----------------------------\n"
        msg += "⚠️ *Alarmes* ⚠️\n\n"
        msg += msg_sensors
    else:
        msg += "✅ *Operando normalmente*\n\n"

    msg += "\n\n"
    return msg


def mount_realtime_hda(plate, last_ev):
    msg = ""
    if not last_ev:
        msg += """Essa placa """
        msg += f"""({plate.get("description", "HDA")})"""
        msg += """ ainda não possui leituras. ⏳ Aguardando...!\n\n"""
        return msg

    msg += f"""💧 *Presenca de agua*  : *{plate["description"]}*\n"""
    msg += f"""📆 {last_ev["created_at"].strftime("%H:%M:%S %d/%m/%Y")}\n"""
    msg += f""" *Situacão*                : {"✅" if last_ev["water_presence"]else "🚨 - *Sem agua*"}\n\n"""
    msg += "\n\n"
    return msg


def mount_realtime_iox(plate, last_ev):
    msg = ""
    msg += f"""🎛⚡ *Smart Sync*  : *{plate.get("description")}*\n"""
    if not last_ev:
        msg += "Sem dados no momento.\n\n"
        return msg

    if plate["have_communication"]:
        msg += """📡 *Online*\n"""
        msg += f"""📆 {last_ev["created_at"].strftime("%H:%M:%S %d/%m/%Y")}\n\n"""
    else:
        msg += """❌  Sua *Smart Sync* está *SEM INTERNET*!\n\n"""
        msg += """*Por favor, verifique sua internet ou reinicie seu equipamento*\n\n"""
        return msg

    fail_msg = ""
    ordered_last_ev = dict(sorted(last_ev.items(), key=lambda x: x[0].lower()))
    for key, value in ordered_last_ev.items():
        if key == "created_at":
            continue

        if (
            "falha" in key.lower()
            or "alarme" in key.lower()
            or "falta" in key.lower()
            or "desarme" in key.lower()
            or "emergência" in key.lower()
        ):
            if value:
                fail_msg += f""" *{key}*       : {"❌" if value else "⚪️"}\n"""

    # ETA - Chiqueiro
    if not fail_msg:
        description = plate.get("description") if plate.get("description") else ""
        msg += f"🟢 Quadro de comandos *{description}* sem alarmes"
    else:
        msg += fail_msg

    msg += "\n\n"
    return msg


def mount_realtime_wgt(plate, last_ev):
    msg = ""
    msg += f"""⏲️ *SMART PLATAFORMA* • *{plate.get("description")}*\n"""
    if not last_ev:
        msg += "Sem dados no momento.\n\n"
        return msg

    if plate["have_communication"]:
        msg += f"""📡 *Online*  • 🕐 {last_ev["created_at"].strftime("%H:%M")}  • 📅 {last_ev["created_at"].strftime("%d/%m/%Y")}\n\n"""
    else:
        msg += """❌  Sua *Plataforma de Carga* está *SEM INTERNET*!\n\n"""
        msg += """*Por favor, verifique sua internet ou reinicie seu equipamento*\n\n"""
        return msg

    un_ordered_last_ev = {}
    iomap = plate.get("params", {}).get("iomap", {})
    for load, struct_load in iomap.items():
        if struct_load.get("status") == "disable":
            continue

        position = load.replace("load", "")
        description = struct_load.get("description")
        capacity = struct_load.get("capacity")
        un_ordered_last_ev.update({position: {"description": description, "capacity": capacity}})

    stock = ""
    consume = ""
    ordered_last_ev = dict(sorted(un_ordered_last_ev.items(), key=lambda x: x[0].lower()))
    for position, info in ordered_last_ev.items():
        description = info["description"]
        capacity = info["capacity"]

        value_consume = last_ev.get(f"Consumo {description} Acumulado")
        stock_value = last_ev.get(description)

        try:
            stock_float = float(stock_value) if stock_value is not None else None
        except (ValueError, TypeError):
            stock_float = None
        try:
            cap_float = float(capacity) if capacity else None
        except (ValueError, TypeError):
            cap_float = None

        if stock_float is not None and cap_float:
            pct = (stock_float / cap_float) * 100
            stock += f"""*{description}*  → {stock_float:.1f} kg — {pct:.0f}%\n"""
        else:
            stock += f"""*{description}*  → {stock_value} kg\n"""
        consume += f"""    - *{description}*  : {round(value_consume, 1)} Kg\n"""

    msg += """📦 ESTOQUE GERAL\n\n"""
    msg += stock
    msg += "\n"
    msg += """📊 Consumo do dia\n\n"""
    msg += consume

    msg += "\n\n"
    return msg


def mount_realtime_status_iox(plate, last_ev):
    msg = ""
    msg += f"""🎛⚡ *Smart Sync*  : *{plate.get("description")}*\n"""
    if not last_ev:
        msg += "Sem dados no momento.\n\n"
        return msg

    if plate["have_communication"]:
        msg += f"""📡 *Online*  • 🕐 {last_ev["created_at"].strftime("%H:%M")}  • 📅 {last_ev["created_at"].strftime("%d/%m/%Y")}\n\n"""
    else:
        msg += """❌  Sua *Smart Sync* está *SEM INTERNET*!\n\n"""
        msg += """*Por favor, verifique sua internet ou reinicie seu equipamento*\n\n"""
        return msg

    ordered_last_ev = dict(sorted(last_ev.items(), key=lambda x: x[0].lower()))
    msg_sensors = ""
    for key, value in ordered_last_ev.items():
        if key == "created_at":
            continue

        if (
            "falha" in key.lower()
            or "alarme" in key.lower()
            or "falta" in key.lower()
            or "desarme" in key.lower()
            or "smaai" in key.lower()
        ):
            if value:
                msg_sensors += f""" *{key}*       : ❌\n"""

    if msg_sensors:
        msg += "⚠️ *Alarmes* ⚠️\n\n"
        msg += msg_sensors
    else:
        msg += "✅ *Operando normalmente*\n\n"
    msg += "\n\n"

    return msg


def mount_realtime_oz1(plate, last_ev):
    msg = ""
    msg += f"""🎛⚡ *Máquina de Ozônio*  : *{plate.get("description")}*\n"""
    if not last_ev:
        msg += "Sem dados no momento.\n\n"
        return msg

    if plate["have_communication"]:
        msg += f"""📡 *Online*  • 🕐 {last_ev["created_at"].strftime("%H:%M")}  • 📅 {last_ev["created_at"].strftime("%d/%m/%Y")}\n\n"""
    else:
        msg += """❌  Sua *Máquina de Ozonio* está *SEM INTERNET*!\n\n"""
        msg += """*Por favor, verifique sua internet ou reinicie seu equipamento*\n\n"""
        return msg

    ordered_last_ev = dict(sorted(last_ev.items(), key=lambda x: x[0].lower()))

    is_not_generating = ordered_last_ev.get("Falha: Geração de Ozônio")
    if is_not_generating:
        msg += "⚠️ ATENÇÃO: geração de ozônio interrompida\n\n"
    else:
        msg += "✅ Máquina gerando de ozônio normalmente\n\n"
        msg += f"""*Fluxo de Ar*: {ordered_last_ev.get("Fluxo de Ar")} l/m\n\n"""

    for key, value in ordered_last_ev.items():
        if key == "created_at":
            continue

        if "falha" in key.lower():
            if value:
                msg += f""" *{key}* : {"❌"}\n"""

    msg += "\n\n"
    return msg


def mount_realtime_ccd(plate, last_ev):
    msg = ""
    msg += f"""🧪 *Estação de Dosagem*  : *{plate.get("description") or ""}*\n"""
    if not last_ev:
        msg += "Sem dados no momento.\n\n"
        return msg

    if plate["have_communication"]:
        msg += f"""📡 *Online*  • 🕐 {last_ev["created_at"].strftime("%H:%M")}  • 📅 {last_ev["created_at"].strftime("%d/%m/%Y")}\n\n"""
        msg += "━━━━━━━━━━━━━\n\n"
    else:
        msg += """❌  Sua *Estação de Dosagem* está *SEM INTERNET*!\n\n"""
        msg += """*Por favor, verifique sua internet ou reinicie seu equipamento*\n\n"""
        # return msg

    power_status = {0: "Desligada", 1: "Ligada"}

    msg += "*⚙️ MODO OPERAÇÃO*\n\n"
    msg += """💧 Dosadora Ácido\n"""
    msg += f"""   └ {last_ev.get("Modo Dosadora Ácido")} • {power_status.get(last_ev.get("Comando Dosadora Ácido"))}\n\n"""

    msg += """🧴 Dosadora Cloro\n"""
    msg += f"""   └ {last_ev.get("Modo Dosadora Cloro")} • {power_status.get(last_ev.get("Comando Dosadora Cloro"))}\n\n"""
    msg += "━━━━━━━━━━━━━\n\n"

    associateds_plates_ = plate.get("params", {}).get("associateds_plates", [])
    associateds_plates = []
    for ass in associateds_plates_:
        if ass.startswith("ORP"):
            associateds_plates.append("ORP")
        elif ass.startswith("PHI"):
            associateds_plates.append("PHI")
        elif ass.startswith("WGT"):
            associateds_plates.append("WGT")
        elif ass.startswith("FLX"):
            associateds_plates.append("FLX")

    if "PHI" in associateds_plates or "ORP" in associateds_plates:
        msg += "📊 PARÂMETROS\n\n"

    if "PHI" in associateds_plates:
        ph_inf = last_ev.get("pH Alvo Inferior")
        ph_sup = last_ev.get("pH Alvo Superior")
        ph = last_ev.get("PH", None)

        msg += f"✅ pH → {ph}\n" if not last_ev.get("Falha: PH fora da faixa") else f"❌  pH → {ph}\n"
        msg += "" if not last_ev.get("Falha: PH fora da faixa") else "⚠️ Fora da faixa\n"
        msg += f"✓ Ideal: : {ph_inf} – {ph_sup}\n"
        msg += "\n\n"

    if "ORP" in associateds_plates:
        orp_inf = last_ev.get("ORP Alvo Inferior")
        orp_sup = last_ev.get("ORP Alvo Superior")
        orp = last_ev.get("ORP", None)

        msg += f"✅ ORP → {orp}mV\n" if not last_ev.get("Falha: ORP fora da faixa") else f"❌  ORP → {orp}mV\n"
        msg += "" if not last_ev.get("Falha: ORP fora da faixa") else "⚠️ Fora da faixa\n"
        msg += f"✓ Ideal: : {orp_inf} – {orp_sup} mV\n"
        msg += "\n\n"

    if "PHI" in associateds_plates or "ORP" in associateds_plates:
        msg += "━━━━━━━━━━━━━\n\n"

    if "WGT" in associateds_plates:
        msg += "🔵 INSUMOS\n\n"
        acid = last_ev.get("Ácido", None)
        if acid is not None:
            msg += f"""💧 Ácido → {last_ev.get("Ácido")} kg\n"""
            msg += f"""├ Nível: {last_ev.get("Porcentagem Ácido")}%\n"""
            msg += f"""├ Consumo: {last_ev.get("Consumo Ácido Acumulado")}kg\n"""

            if last_ev.get("Porcentagem Ácido") <= 10.0:
                msg += """└ 🔴 Reposição urgente\n\n"""
            else:
                msg += "\n"

        cloro = last_ev.get("Cloro", None)
        if cloro is not None:
            msg += f"""🧴 Cloro → {last_ev.get("Cloro")} kg\n"""
            msg += f"""├ Nível: {last_ev.get("Porcentagem Cloro")}%\n"""
            msg += f"""├ Consumo: {last_ev.get("Consumo Cloro Acumulado")}kg\n"""

            if last_ev.get("Porcentagem Cloro") <= 10.0:
                msg += """└ 🔴 Reposição urgente\n\n"""
            else:
                msg += "\n"

        pac = last_ev.get("Pac", None)
        if pac is not None:
            msg += f"""🧴 Pac → {last_ev.get("Pac")} kg\n"""
            # msg += f"""├ Nível: {last_ev.get("Porcentagem Pac")}%\n"""
            msg += f"""├ Consumo: {last_ev.get("Consumo Pac Acumulado")}kg\n"""

            if last_ev.get("Porcentagem Pac") <= 10.0:
                msg += """└ 🔴 Reposição urgente\n\n"""
            else:
                msg += "\n"

        msg += "━━━━━━━━━━━━━\n\n"

    if "FLX" in associateds_plates:
        msg += "🔵 HIDROMETROS\n\n"
        flow = last_ev.get("Fluxo de Água")
        consumed = last_ev.get("Consumo Água Acumulado")

        msg += f"Fluxo: {flow}L/min\n"
        msg += f"Acumulado: {consumed}L\n\n"

    falhas = ""
    if last_ev.get("Falha: Dosagem de Ácido Ineficiente") and "PHI" in associateds_plates:
        falhas += "    🔴 *Dificuldade em ajustar o pH*\n"

    if last_ev.get("Falha: Dosagem de Cloro Ineficiente") and "ORP" in associateds_plates:
        falhas += "    🔴 *Dificuldade em ajustar ORP*\n"

    if last_ev.get("Falha: Falta de recirculação\n"):
        falhas += "    🔴 *Falta de recirculação*\n"

    if last_ev.get("Falha: Sensor de Fluxo") and "FLX" in associateds_plates:
        falhas += "    🔴 *Falha: Sensor de Fluxo*\n"

    if falhas:
        msg += "🚨 ALERTAS ATIVOS\n\n"
        msg += falhas

    msg += "\n\n"
    return msg


def mount_realtime_az1(plate, last_ev):
    msg = ""
    msg += f"""🎛⚡ *Ambiência*  : *{plate.get("description")}*\n"""
    if not last_ev:
        msg += "Sem dados no momento.\n\n"
        return msg

    if plate["have_communication"]:
        msg += """📡 *Online*\n"""
        msg += f"""📆 {last_ev["created_at"].strftime("%H:%M:%S %d/%m/%Y")}\n\n"""
    else:
        msg += """❌  Sua *Ambiêcia* está *SEM INTERNET*!\n\n"""
        msg += """*Por favor, verifique sua internet ou reinicie seu equipamento*\n\n"""
        return msg

    for key, value in last_ev.items():
        if key == "created_at":
            continue

        if key == "CO2":
            msg += f""" *{key}*       : {value} ppm\n"""
            continue

        if key == "Umidade":
            msg += f""" *{key}*       : {value} %\n"""
            continue

        if key == "Pressão":
            msg += f""" *{key}*       : {value} Pa\n"""
            continue

        if key == "Temperatura":
            msg += f""" *{key}*       : {round(value / 10, 1)} ºC\n"""
            continue

        if "falha" in key.lower() or "alarme" in key.lower() or "falta" in key.lower():
            msg += f""" *{key}*       : {"❌" if value else "⚪️"}\n"""
        else:
            msg += f""" *{key}*       : {"🟢" if value else "⚪️"}\n"""

    msg += "\n\n"
    return msg


def mount_realtime_qp4(plate, last_ev):
    msg = ""
    msg += f"""🎛⚡ *Quadro de Comandos*  : *{plate.get("description", " ")}*\n"""
    if not last_ev:
        msg += "Sem dados no momento.\n\n"
        return msg

    if plate["have_communication"]:
        msg += f"""📡 *Online*  • 🕐 {last_ev["created_at"].strftime("%H:%M")}  • 📅 {last_ev["created_at"].strftime("%d/%m/%Y")}\n\n"""
    else:
        msg += """❌  Seu *Quadro de Comandos* está *SEM INTERNET*!\n\n"""
        msg += """*Por favor, verifique sua internet ou reinicie seu equipamento*\n\n"""
        return msg

    ordered_last_ev = dict(sorted(last_ev.items(), key=lambda x: x[0].lower()))
    for key, value in ordered_last_ev.items():
        if key == "Pre Limpeza":
            pre_limpeza = value
        elif key == "Retrolavagem":
            retrolavagem = value
        elif key == "Req Manual":
            recalque_manual = value
        elif key == "Tratamento Automatico":
            tratamento_automatico = value
        elif key == "Erro seletora pre limpeza":
            erro_seletora_pre_limpeza = value
        elif key == "Erro seletora tratamento":
            erro_seletora_tratamento = value
        elif key == "Emergencia":
            emergencia = value
        elif key == "Falha Bomba 1":
            falha_bomba1 = value
        elif key == "Falha Bomba 2":
            falha_bomba2 = value
        elif key == "Falha Bomba 3":
            falha_bomba3 = value
        elif key == "Falta de água no pré tratamento":
            falta_agua_pre_trat = value
        elif key == "Falta de água no tratamento":
            falta_agua_trat = value

    msg += "*⚙️ OPERAÇÃO*\n\n"
    log.info(
        f"{erro_seletora_pre_limpeza} or {erro_seletora_tratamento} or {emergencia} or {falha_bomba1} or {falha_bomba2} or {falha_bomba3}"
    )
    if (
        erro_seletora_pre_limpeza
        or erro_seletora_tratamento
        or emergencia
        or falha_bomba1
        or falha_bomba2
        or falha_bomba3
        or falta_agua_pre_trat
        or falta_agua_trat
    ):
        msg += "🚨 *Estação em Falha* 🚨\n\n"
    else:
        msg += "🟢 *Estação em Funcionamento*\n\n"

    if pre_limpeza == 0 and retrolavagem == 0:
        msg += "✅ *Pré-limpeza em Espera*\n"
    elif pre_limpeza == 1 and retrolavagem == 0:
        msg += "✅ *Pré-limpeza em Modo Automático*\n"
    elif pre_limpeza == 0 and retrolavagem == 1:
        msg += "✅ *Pré-limpeza em Modo Manual*\n"

    if recalque_manual == 0 and tratamento_automatico == 0:
        msg += "✅ *Tratamento em Espera*\n"
    elif recalque_manual == 0 and tratamento_automatico == 1:
        msg += "✅ *Tratamento em Modo Automático*\n"
    elif recalque_manual == 1 and tratamento_automatico == 0:
        msg += "✅ *Tratamento em Modo Manual*\n"

    msg += "\n\n"
    return msg


def mount_realtime_qbt(plate, last_ev):
    msg = ""
    msg += f"""🎛⚡ *Quadro de Comandos*  : *{plate.get("description", " ")}*\n"""
    if not last_ev:
        msg += "Sem dados no momento.\n\n"
        return msg

    msg += f"""📆 {last_ev["created_at"].strftime("%H:%M:%S %d/%m/%Y")}\n\n"""

    ordered_last_ev = dict(sorted(last_ev.items(), key=lambda x: x[0].lower()))
    for key, value in ordered_last_ev.items():
        if key == "Modo Pré Limpeza Automática":
            pre_limpeza = value
        elif key == "Modo Retrolavagem do Filtro da Pré Limpeza":
            retrolavagem_pre_limpeza = value
        elif key == "Modo Tratamento Automático":
            modo_tratamento_automatico = value
        elif key == "Modo Retrolavagem do Filtro do Tratamento":
            modo_retrolavagem_tratamento = value
        elif key == "Modo Cisterna Automático":
            modo_cisterna_automatico = value

        elif key == "Comando Interrompido":
            falha = value
        elif key == "Falha ozonio":
            falha = value
        elif key == "Falha na Seletora da Pré Limpeza":
            falha = value
        elif key == "Falha no Acionamento da Bomba de Recalque da Cisterna":
            falha = value
        elif key == "Falha no Acionamento da Bomba da Pré Limpeza":
            falha = value
        elif key == "Falha no Acionamento da Bomba de Recalque Tratamento":
            falha = value
        elif key == "Falha no Acionamento da Bomba do Tratamento":
            falha = value
        elif key == "Falha no Sistema de Boias da Caixa da Pré Limpeza":
            falha = value
        elif key == "Falta de água na Cisterna":
            falha = value
        elif key == "Falta de água na Caixa da Pré Limpeza":
            falha = value
        elif key == "Falta de água na Caixa do Tratamento":
            falha = value

    msg += "*Sua estação de tratamento encontra-se assim:*\n\n"

    if falha:
        msg += "🚨 *Estação em Falha* 🚨\n\n"
    else:
        msg += "🟢 *Estação em Funcionamento*\n\n"

    # Pré-tratamento
    if pre_limpeza == 0 and retrolavagem_pre_limpeza == 0:
        msg += "✅ *Pré-limpeza em Espera*\n"

    elif pre_limpeza == 1 and retrolavagem_pre_limpeza == 0:
        msg += "✅ *Pré-limpeza em Modo Automático*\n"

    elif pre_limpeza == 0 and retrolavagem_pre_limpeza == 1:
        msg += "✅ *Modo Retrolavagem no Filtro da Pré Limpeza*\n"

    # Tratamento
    if modo_tratamento_automatico == 0:
        msg += "✅ *Tratamento em Espera*\n"
    elif modo_tratamento_automatico == 1:
        msg += "✅ *Tratamento em Modo Automático*\n"

    # Retrolavagem Tratamento
    if modo_retrolavagem_tratamento == 0:
        msg += "✅ *Modo Retrolavagem do Filtro do Tratamento em Espera*\n"
    elif modo_retrolavagem_tratamento == 1:
        msg += "✅ *Modo Retrolavagem do Filtro do Tratamento*\n"

    # Cisterna
    if modo_cisterna_automatico == 0:
        msg += "✅ *Modo Cisterna em Espera*\n"
    elif modo_cisterna_automatico == 1:
        msg += "✅ *Cisterna em Modo Automático*\n"

    msg += "\n\n"
    return msg


def mount_tempo_real_elevatoria(plate, last_ev):
    msg = ""
    # msg += f"""🎛⚡ *Estação Elevatória*  : *{plate.get("description", " ")}*\n"""
    msg += f"""🎛⚡ *{plate.get("description", " ")}*\n"""

    if not last_ev:
        msg += "Sem dados no momento.\n\n"
        return msg

    msg += f"""📆 {last_ev["created_at"].strftime("%H:%M:%S %d/%m/%Y")}\n\n"""
    falha_list = ""

    ordered_last_ev = dict(sorted(last_ev.items(), key=lambda x: x[0].lower()))
    for key, value in ordered_last_ev.items():
        falha = 0
        if key == "Botão ON/OFF":
            botao_onoff = value
        elif key == "Gerador":
            gerador = value
        elif key == "Falha: Bomba 1":
            falha = value
        elif key == "Falha: Bomba 2":
            falha = value
        elif key == "Falha: Fonte":
            falha = value
        elif key == "Falha: Comando":
            falha = value
        elif key == "Falha: Desarme Disjuntor Geral ou DR Bombas":
            falha = value
        elif key == "Falha: Falta de Fase":
            falha = value
        elif key == "Falha: Erro nas boias":
            falha = value
        elif key == "Falha: Fluxo Insuficiente Bomba 1":
            falha = value
        elif key == "Falha: Fluxo Insuficiente Bomba 2":
            falha = value
        elif key == "Falha: Sensor de Fluxo OFFLINE":
            falha = value

        if falha:
            falha_list += f" - *{key}*\n"

    msg += "**⚙️ OPERAÇÃO*\n\n"

    if falha_list:
        msg += "🚨 *Estação em Falha* 🚨\n\n"
        msg += falha_list
        msg += "\n\n"
    else:
        msg += "🟢 *Estação em Funcionamento*\n\n"

    msg += "✅ *Habilitado*\n" if botao_onoff else "🚨 *Desabilitado*"
    msg += "✅ *Gerador ligado*\n" if gerador else "⬜ *Gerador desligado*"

    # Nível da água — busca sensores de nível no iomap
    iomap = plate.get("params", {}).get("iomap", {})
    niveis = {}
    for sensor_key, sensor_cfg in iomap.items():
        desc = sensor_cfg.get("description", "")
        desc_lower = desc.lower()
        if "nível" in desc_lower or "nivel" in desc_lower:
            valor = last_ev.get(desc)
            if "inferior" in desc_lower:
                niveis[1] = {"description": desc, "active": bool(valor)}
            elif "intermedi" in desc_lower:
                niveis[2] = {"description": desc, "active": bool(valor)}
            elif "superior" in desc_lower:
                niveis[3] = {"description": desc, "active": bool(valor)}

    if niveis:
        nivel_superior_ativo = niveis.get(3, {}).get("active", False)
        if nivel_superior_ativo:
            msg += "\n\n🚨 *NÍVEL DA ÁGUA* 🚨\n"
        else:
            msg += "\n\n💧 *NÍVEL DA ÁGUA*\n"

        for idx in [3, 2, 1]:
            nivel = niveis.get(idx)
            if not nivel:
                continue
            nome = nivel["description"]
            if nivel["active"]:
                icone = "🟥" if idx == 3 else "🟦"
            else:
                icone = "⬜"
            msg += f"{icone} {nome}\n"

    msg += "\n━━━━━━━━━━━━━\n\n"
    return msg


def mount_realtime_message(filters):
    msg = ""

    # Primeiro verifica se tem CCD na farm
    ccd_filters = filters.copy()
    ccd_filters.update({"plate_type": ["CCD"]})
    ccd_plates = Plate.get_all(ccd_filters)
    has_ccd = len(ccd_plates) > 0

    # Se tem CCD, pega as placas associadas
    associated_with_ccd = []
    if has_ccd:
        for ccd_plate_ in ccd_plates:
            ccd_plate = ccd_plate_.to_dict()
            associateds = ccd_plate.get("params", {}).get("associateds_plates", [])
            for ass in associateds:
                if ass.startswith("ORP") or ass.startswith("PHI") or ass.startswith("WGT") or ass.startswith("FLX"):
                    associated_with_ccd.append(ass)

    for plate_type in PLATES_TYPES:
        filters.update({"plate_type": [plate_type]})
        plates = Plate.get_all(filters)
        msg_tmp = ""

        # Se tem CCD e esse tipo de placa está associado, pula
        if has_ccd and plate_type in ["PHI", "ORP", "WGT", "FLX"]:
            # Verifica se alguma placa desse tipo está associada ao CCD
            skip_type = False
            for plate_ in plates:
                plate = plate_.to_dict()
                plate_serial = plate.get("serial")
                if any(plate_serial in ass for ass in associated_with_ccd):
                    skip_type = True
                    break

            if skip_type:
                continue

        # Tratamento especial para FLX (hidrômetros) - formato tabela
        if plate_type == "FLX" and len(plates) > 1:
            flx_data = []
            offline_flx = []
            total_consumption = 0
            latest_time = None

            for plate_ in plates:
                plate = plate_.to_dict()
                _Event = get_events_model(plate["plate_type"])
                last_ev = _Event.get_last_event(plate["owner"], plate["serial"])

                if last_ev and plate["have_communication"]:
                    flow = (
                        round(last_ev["water_flow"])
                        if last_ev["water_flow"] <= 1000
                        else round(last_ev["water_flow"] / 1000, 1)
                    )
                    flow_unit = "L/min" if last_ev["water_flow"] <= 1000 else "mil L/min"
                    consumption = round(last_ev.get("water_consumed", 0))
                    total_consumption += consumption

                    if not latest_time or last_ev["created_at"] > latest_time:
                        latest_time = last_ev["created_at"]

                    flx_data.append(
                        {
                            "description": plate.get("description", "Sem nome"),
                            "flow": f"{flow} {flow_unit}",
                            "consumption": f"{consumption} L",
                        }
                    )
                else:
                    last_reading = last_ev["created_at"].strftime("%H:%M") if last_ev else "N/A"
                    offline_flx.append(
                        {
                            "description": plate.get("description", "Sem nome"),
                            "last_reading": last_reading,
                        }
                    )

            if flx_data:
                time_str = latest_time.strftime("%H:%M %d/%m/%Y") if latest_time else ""
                msg_tmp += f"🚰 *HIDRÔMETROS* - {time_str}\n\n"
                msg_tmp += "📍 LOCAL     💧 FLUXO    📊 CONSUMO\n"
                msg_tmp += "-----------------------\n"

                for item in flx_data:
                    desc = item["description"][:9].ljust(9)
                    flow = item["flow"].rjust(10)
                    consumption = item["consumption"].rjust(9)
                    msg_tmp += f"{desc}  {flow}  {consumption}\n"

                online_count = len(flx_data)
                total_formatted = f"{total_consumption:,}".replace(",", ".")
                msg_tmp += f"\n📡 {online_count} Online | 💧 Total: {total_formatted} L\n"

                if offline_flx:
                    msg_tmp += "\n❌ *OFFLINE*\n"
                    for item in offline_flx:
                        msg_tmp += f"{item['description']} - Última leitura: {item['last_reading']}\n"

                msg_tmp += "\n━━━━━━━━━━━━━\n\n"

        else:
            for plate_ in plates:
                plate = plate_.to_dict()
                iomap = plate.get("params", {}).get("iomap", {})

                # """1️⃣ Sensores em tempo real;"""
                _Event = get_events_model(plate["plate_type"])
                last_ev = _Event.get_last_event(plate["owner"], plate["serial"])

                if plate["plate_type"] == "Z1":
                    msg_tmp += mount_realtime_sensors(plate, last_ev)
                    msg_tmp += "━━━━━━━━━━━━━\n\n"

                elif plate["plate_type"] == "FLX":
                    msg_tmp += mount_realtime_water_flow(plate, last_ev)
                    msg_tmp += "━━━━━━━━━━━━━\n\n"

                elif plate["plate_type"] == "NVL":
                    msg_tmp += mount_realtime_water_level(plate, last_ev)
                    msg_tmp += "━━━━━━━━━━━━━\n\n"

                elif plate["plate_type"] == "CLPCG":
                    msg_tmp += mount_realtime_clpcg(plate, last_ev, iomap)
                    msg_tmp += "━━━━━━━━━━━━━\n\n"

                elif plate["plate_type"] == "HDA":
                    msg_tmp += mount_realtime_hda(plate, last_ev)
                    msg_tmp += "━━━━━━━━━━━━━\n\n"

                elif plate["plate_type"] == "IOX":
                    msg_tmp += mount_realtime_iox(plate, last_ev)
                    msg_tmp += "━━━━━━━━━━━━━\n\n"

                elif plate["plate_type"] == "WGT":
                    msg_tmp += mount_realtime_wgt(plate, last_ev)
                    msg_tmp += "━━━━━━━━━━━━━\n\n"

                elif plate["plate_type"] == "QP4":
                    msg_tmp += mount_realtime_qp4(plate, last_ev)
                    msg_tmp += "━━━━━━━━━━━━━\n\n"

                elif plate["plate_type"] == "PHI":
                    msg_tmp += mount_realtime_phi(plate, last_ev)
                    msg_tmp += "━━━━━━━━━━━━━\n\n"

                elif plate["plate_type"] == "ORP":
                    msg_tmp += mount_realtime_orp(plate, last_ev)
                    msg_tmp += "━━━━━━━━━━━━━\n\n"

                elif plate["plate_type"] == "AZ1":
                    msg_tmp += mount_realtime_az1(plate, last_ev)
                    msg_tmp += "━━━━━━━━━━━━━\n\n"

                elif plate["plate_type"] == "OZ1":
                    msg_tmp += mount_realtime_oz1(plate, last_ev)
                    msg_tmp += "━━━━━━━━━━━━━\n\n"

                elif plate["plate_type"] == "QP7":
                    msg_tmp += mount_tempo_real_elevatoria(plate, last_ev)
                    msg_tmp += "━━━━━━━━━━━━━\n\n"

                elif plate["plate_type"] == "CCD":
                    msg_tmp += mount_realtime_ccd(plate, last_ev)
                    msg_tmp += "━━━━━━━━━━━━━\n\n"

        msg += msg_tmp

    return msg


def check_recent_unanswered_alarms(user_phone):
    """
    Verifica se há alarmes urgentes recentes (últimas 24h) não atendidos.

    Lógica simplificada: Se há alarmes não atendidos nas últimas 24h,
    considera que é uma resposta atrasada e retorna mensagem educativa.

    Retorna tupla: (tem_alarmes_recentes: bool, mensagem_educativa: str)
    """
    from z1monitoring_models.models.urgent_alarm import UrgentAlarm
    from datetime import timedelta

    log.info("🔍 Verificando alarmes recentes")

    # Busca alarmes das últimas 24 horas não atendidos
    now = datetime.datetime.now()
    time_limit = now - timedelta(hours=24)

    try:
        session = Session()
        recent_alarms_count = (
            session.query(UrgentAlarm)
            .filter(
                UrgentAlarm.created_at >= time_limit,
                UrgentAlarm.attended.is_(False),
            )
            .count()
        )
        session.close()

        log.info(f"🔍 Total de alarmes não atendidos nas últimas 24h: {recent_alarms_count}")

        # Se há alarmes recentes não atendidos, assume que é resposta atrasada
        if recent_alarms_count > 0:
            log.info(f"✅ Há {recent_alarms_count} alarmes não atendidos - enviando mensagem educativa")
            msg = (
                "Entendi que você não pode atender ao alarme urgente. "
                "No entanto, é muito importante responder *imediatamente* após receber a mensagem de alarme, "
                "pois isso ajuda a equipe a tomar ações mais rápidas.\n\n"
                "⏰ Respostas rápidas = Problemas resolvidos mais rápido!"
            )
            return (True, msg)
        else:
            # Não há alarmes recentes
            log.info("❌ Não há alarmes recentes não atendidos")
            return (False, None)

    except Exception as e:
        log.error(f"❌ Erro ao verificar alarmes recentes: {e}")
        import traceback

        traceback.print_exc()
        return (False, None)
