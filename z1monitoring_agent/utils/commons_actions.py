from z1monitoring_agent.utils.whatsapp_utils import (
    mount_header_farm,
    mount_only_ph,
    mount_realtime_phi,
    mount_realtime_acid,
    mount_realtime_chlorine,
    mount_realtime_oz1,
    mount_only_orp,
    mount_realtime_orp,
    mount_realtime_water_flow,
    mount_tempo_real_elevatoria,
    mount_realtime_message,
    mount_realtime_ccd,
    mount_only_temperature,
)
from z1monitoring_models.models.choose_event_model import get_events_model
import datetime
from datetime import timedelta
from z1monitoring_agent.utils.consumption_graphics_generator import generate_consumption_graphics
from z1monitoring_models.models.farm import Farm
from z1monitoring_models.models.clients_secondary import ClientSecondary
from unidecode import unidecode
from string import punctuation
import structlog


# Stubs para funções não essenciais
def audio_generate(text):
    return None


def try_select_farm_from_fonetic_analisis(*args, **kwargs):
    return None


def normalize_local_name(name):
    return name


log = structlog.get_logger()


def verify_requested_local_secundary_client(user, local_name):

    farm_list = Farm.get_all_farms_objs_filtereds({"owner": user.associated})
    if len(farm_list) == 0:
        msg = "Voce nao tem locais para visualição. " "Se acha que isso está errado entre em contato com seu fornecedor"
        return None, {"type": "text", "msg": msg}

    if len(farm_list) == 1:
        return farm_list[0], []

    if not local_name:
        msg = (
            """Desculpe, não entendi o local solicitado.\n\n"""
            """Solicite algo como: 🎤 *Me envia o tempo real do pH do comércio de pedras*"""
        )
        return None, [{"type": "text", "msg": msg}]

    # Tenta primeiro com o nome original
    local = Farm.get_farm_like_sensibility(local_name)

    # Se não encontrar, tenta com nome normalizado (sem prefixos como "Granja")
    if not local:
        normalized_name = normalize_local_name(local_name)
        if normalized_name != local_name:
            log.info(f"Tentando com nome normalizado: '{local_name}' -> '{normalized_name}'")
            local = Farm.get_farm_like_sensibility(normalized_name)

    # Se ainda não encontrar, usa LLM para análise fonética
    if not local:
        filter = {}
        if user:
            filter = {"associated": user.associated}
        _all_farm_name = Farm.get_all_farm_name(filter)
        fixed_local_name = try_select_farm_from_fonetic_analisis(_all_farm_name, local_name)

        if fixed_local_name:
            local = Farm.get_farm_like_sensibility(fixed_local_name)

        # 4. Se ainda não encontrou, tenta buscar por nome de ClientSecondary
        if not local:
            local, error_msgs = _try_find_farm_by_client_secondary_name(user, local_name)
            if error_msgs:
                return None, error_msgs

        if not local:
            log.info(f"Farm nao encontrada: {local_name}")
            msg = f"❗ Desculpe, talvez eu nao entendi direito o nome do local ou ele nao exista. Local solicitado: {local_name.capitalize()}"
            return None, [{"type": "text", "msg": msg}]

    return local, []


def verify_local_requested(user, local_name):

    if not local_name:
        msg = (
            """Desculpe, não entendi o local solicitado.\n\n"""
            """Solicite algo como: 🎤 *Me envia o tempo real do pH do comércio de pedras*"""
        )
        return None, [{"type": "text", "msg": msg, "status": False}]

    # Tenta primeiro com o nome original
    local = Farm.get_farm_like_sensibility(local_name)

    # Se não encontrar, tenta com nome normalizado (sem prefixos como "Granja")
    if not local:
        normalized_name = normalize_local_name(local_name)
        if normalized_name != local_name:
            log.info(f"Tentando com nome normalizado: '{local_name}' -> '{normalized_name}'")
            local = Farm.get_farm_like_sensibility(normalized_name)

    # Se ainda não encontrar, usa LLM para análise fonética
    if not local:
        filter = {}
        if user:
            filter = {"associated": user.associated}
        _all_farm_name = Farm.get_all_farm_name(filter)
        fixed_local_name = try_select_farm_from_fonetic_analisis(_all_farm_name, local_name)

        if fixed_local_name:
            local = Farm.get_farm_like_sensibility(fixed_local_name)

        # 4. Se ainda não encontrou, tenta buscar por nome de ClientSecondary
        if not local:
            local, error_msgs = _try_find_farm_by_client_secondary_name(user, local_name)
            if error_msgs:
                return None, error_msgs

        if not local:
            log.info(f"Farm nao encontrada: {local_name}")
            msg = f"❗ Desculpe, talvez eu nao entendi direito o nome do local ou ele nao exista. Local solicitado: {local_name.capitalize()}"
            return None, [{"type": "text", "msg": msg, "status": False}]

    return local, []


def _try_find_farm_by_client_secondary_name(user, local_name):
    """
    Tenta encontrar uma farm pelo nome do ClientSecondary (dono).

    Quando o usuário não lembra o nome da granja mas lembra do cliente secundário,
    ex: "me envia o tempo real da granja do Jaime Kolling"

    Returns:
        tuple: (farm, error_msgs) - farm se encontrada, ou error_msgs se houver erro
    """
    try:
        # Obtém filtro baseado no usuário (ADMIN não tem filtro)
        filter_cs = {}
        if user and user.permissions.get("name") != "ADMIN" and user.associated:
            filter_cs = {"associateds_allowed": user.associated}

        # Busca todos os clientes secundários
        all_clients = ClientSecondary.get_all(filter_cs)
        if not all_clients:
            return None, None

        # Cria lista de nomes de clientes secundários para análise fonética
        client_names = []
        client_map = {}  # nome -> ClientSecondary
        for client in all_clients:
            if client.name:
                client_names.append(client.name)
                client_map[client.name] = client
            if client.fantasy_name:
                client_names.append(client.fantasy_name)
                client_map[client.fantasy_name] = client

        if not client_names:
            return None, None

        # Usa análise fonética para encontrar o cliente secundário
        matched_name = try_select_farm_from_fonetic_analisis(client_names, local_name)
        if not matched_name:
            return None, None

        matched_client = client_map.get(matched_name)
        if not matched_client:
            return None, None

        log.info(f"ClientSecondary encontrado por análise fonética: '{local_name}' -> '{matched_name}'")

        # Busca farms onde esse ClientSecondary é o dono
        farms = Farm.get_all_farms_objs_filtereds({"owner": matched_client.identification})

        if not farms:
            log.info(f"ClientSecondary '{matched_name}' não possui farms associadas")
            return None, None

        if len(farms) == 1:
            log.info(f"Farm única encontrada para ClientSecondary '{matched_name}': {farms[0].name}")
            return farms[0], None

        # Múltiplas farms - pede para usuário especificar
        farms_list = "\n".join([f"• {farm.name}" for farm in farms])
        msg = (
            f"O cliente *{matched_name}* possui {len(farms)} locais:\n\n"
            f"{farms_list}\n\n"
            f"Por favor, especifique qual local você deseja."
        )
        log.info(f"ClientSecondary '{matched_name}' possui múltiplas farms: {[f.name for f in farms]}")
        return None, [{"type": "text", "msg": msg, "status": False}]

    except Exception as e:
        log.error(f"Erro ao buscar ClientSecondary por nome: {e}")
        return None, None


def handler_placas_online(plate_list):
    if len(plate_list) == 0:
        msg = "Não há placas online"
        audio_msg = audio_generate(msg)
        return [{"type": "text", "msg": msg}, audio_msg]
    else:
        already_used_list = []
        farms_list = ""
        for plate in plate_list:
            if plate.farm_associated in already_used_list:
                continue

            already_used_list.append(plate.farm_associated)
            farms_list += f"""{plate.farm_associated};\n"""

        msg = f"Nesse momento existe *{len(already_used_list)}* online. São: \n\n{farms_list}"
        msg2audio = f"Nesse momento, existe {len(already_used_list)} online. São: \n\n{farms_list}"
        audio_msg = audio_generate(msg2audio)
        return [{"type": "text", "msg": msg}, audio_msg]


def handler_placas_offline(plate_list):
    if len(plate_list) == 0:
        msg = "Não há equipamentos offline"
        audio_msg = audio_generate(msg)
        return [{"type": "text", "msg": msg}, audio_msg]
    else:
        already_used_list = []
        farms_list = ""
        for plate in plate_list:
            if plate.farm_associated in already_used_list:
                continue

            already_used_list.append(plate.farm_associated)
            farms_list += f"""{plate.farm_associated};\n"""

        msg = f"Nesse momento existe *{len(already_used_list)}* local(is) offline. São eles:\n\n{farms_list}"
        msg2audio = f"Nesse momento, existe {len(already_used_list)} local offline. São eles:\n\n{farms_list}"

        audio_msg = audio_generate(msg2audio)
        return [{"type": "text", "msg": msg}, audio_msg]


def handler_placas_falta_acido(plate_list):
    if len(plate_list) == 0:
        msg = "Não há locais com falta de ácido"
        audio_msg = audio_generate(msg)
        return [{"type": "text", "msg": msg}, audio_msg]
    else:
        already_used_list = []
        farms_list = ""
        for plate in plate_list:
            if plate.farm_associated in already_used_list:
                continue

            already_used_list.append(plate.farm_associated)
            farms_list += f"""{plate.farm_associated};\n"""

        msg = f"Nesse momento existe *{len(already_used_list)}* sem ácido. São:\n\n{farms_list}"
        msg2audio = f"Nesse momento, existe {len(already_used_list)} sem ácido. São:\n\n{farms_list}"
        audio_msg = audio_generate(msg2audio)
        return [{"type": "text", "msg": msg}, audio_msg]


def handler_placas_falta_cloro(plate_list):

    if len(plate_list) == 0:
        msg = "Não há locais com falta de cloro"
        audio_msg = audio_generate(msg)
        return [{"type": "text", "msg": msg}, audio_msg]
    else:
        already_used_list = []
        farms_list = ""
        for plate in plate_list:
            if plate.farm_associated in already_used_list:
                continue

            already_used_list.append(plate.farm_associated)
            farms_list += f"""{plate.farm_associated};\n"""

        msg = f"Nesse momento existe *{len(already_used_list)}* local(is) sem cloro. São eles: \n\n{farms_list}"
        msg2audio = f"Nesse momento, existe {len(already_used_list)} local sem cloro. São eles: \n\n{farms_list}"
        audio_msg = audio_generate(msg2audio)
        return [{"type": "text", "msg": msg}, audio_msg]


def handler_placas_falta_gas(plate_list):
    if len(plate_list) == 0:
        msg = "Não há locais com monitoramento de gás."
        return [{"type": "text", "msg": msg}]

    already_used_list = []
    farms_com_falta = []
    todos_niveis = []  # Para ranking quando não há falta

    for plate in plate_list:
        if plate.farm_associated in already_used_list:
            continue

        if plate.params.get("status", {}).get("have_problem", False) is True:
            continue

        # Verifica se tem sensor de gás no iomap
        iomap = plate.params.get("iomap", {})
        for key, value in iomap.items():
            if value.get("status", "enable") == "disable":
                continue
            desc = value.get("description", "")
            if "gas" not in unidecode(desc).strip(punctuation).lower():
                continue

            # Tem sensor de gás - verifica nível atual
            _Event = get_events_model(plate.plate_type)
            last_ev = _Event.get_last_event(plate.owner, plate.serial)
            if not last_ev:
                continue

            gas_raw = last_ev.get(desc)
            try:
                gas_val = float(gas_raw) if gas_raw is not None else None
            except (ValueError, TypeError):
                gas_val = None

            if gas_val is None:
                continue

            # Pega capacidade e mínimo
            capacity = value.get("capacity", 0)
            try:
                capacity = float(capacity) if capacity else 0
            except (ValueError, TypeError):
                capacity = 0

            min_weight = value.get("min_weight", 0)
            try:
                min_weight = float(min_weight) if min_weight else 0
            except (ValueError, TypeError):
                min_weight = 0

            # Calcula percentual
            pct = (gas_val / capacity * 100) if capacity > 0 else 0

            # Guarda para ranking
            todos_niveis.append({"farm": plate.farm_associated, "gas_kg": gas_val, "capacity": capacity, "pct": pct})

            # Se está abaixo do mínimo, adiciona à lista de falta
            if gas_val <= min_weight:
                already_used_list.append(plate.farm_associated)
                farms_com_falta.append(f"{plate.farm_associated} ({gas_val:.0f} kg)")
            else:
                already_used_list.append(plate.farm_associated)
            break

    if len(farms_com_falta) > 0:
        farms_list = "\n".join(farms_com_falta)
        msg = f"*{len(farms_com_falta)}* locais com falta de gás:\n\n{farms_list}"
        return [{"type": "text", "msg": msg}]

    # Sem falta - mostra TOP 10 menores percentuais
    if len(todos_niveis) == 0:
        msg = "Nenhum local com sensor de gás encontrado."
        return [{"type": "text", "msg": msg}]

    todos_niveis.sort(key=lambda x: x["pct"])
    top10 = todos_niveis[:10]

    msg = "Nenhum local está com falta de gás no momento.\n\n"
    msg += "*TOP 10 - Menores níveis de gás:*\n\n"
    msg += "```\n"
    msg += f"{'Local':<25} {'Peso':>8} {'%':>6}\n"
    msg += "-" * 41 + "\n"
    for item in top10:
        nome = item["farm"][:24]
        msg += f"{nome:<25} {item['gas_kg']:>7.0f}kg {item['pct']:>5.0f}%\n"
    msg += "```"

    return [{"type": "text", "msg": msg}]


def handler_ph_fora_faixa(plate_list):
    if len(plate_list) == 0:
        msg = "Não há locais com pH fora da faixa de operação."
        msg2audio = "Não há locais com pH fora da faixa de operação."
        audio_msg = audio_generate(msg2audio)
        return [{"type": "text", "msg": msg}, audio_msg]
    else:
        already_used_list = []
        farms_list = ""
        for plate in plate_list:
            if plate.farm_associated in already_used_list:
                continue

            already_used_list.append(plate.farm_associated)
            farms_list += f"""{plate.farm_associated};\n"""

        msg = f"Nesse momento existe *{len(already_used_list)}* com pH fora da faixa. São:\n\n{farms_list}"
        msg2audio = f"Nesse momento, existe {len(already_used_list)} com pH fora da faixa. São:\n\n{farms_list}"
        audio_msg = audio_generate(msg2audio)
        return [{"type": "text", "msg": msg}, audio_msg]


def handler_orp_fora_faixa(plate_list):
    if len(plate_list) == 0:
        msg = "Não há locais com ORP fora da faixa de operação."
        msg2audio = "Não há locais com ORP fora da faixa de operação."
        audio_msg = audio_generate(msg2audio)
        return [{"type": "text", "msg": msg}, audio_msg]
    else:
        already_used_list = []
        farms_list = ""
        for plate in plate_list:
            if plate.farm_associated in already_used_list:
                continue

            already_used_list.append(plate.farm_associated)
            farms_list += f"""{plate.farm_associated};\n"""

        msg = f"Nesse momento existe *{len(already_used_list)}* com ORP fora da faixa. São:\n\n{farms_list}"
        msg2audio = f"Nesse momento, existe {len(already_used_list)} com ORP fora da faixa. São:\n\n{farms_list}"
        audio_msg = audio_generate(msg2audio)
        return [{"type": "text", "msg": msg}, audio_msg]


def handler_alteracao_ccd(items):
    if len(items) == 0:
        msg = "Não há alteracões a ser listadas."
        # audio_msg = audio_generate(msg)
        return [{"type": "text", "msg": msg}]

    msg = "*Últimas alterações:* \n\n"
    for item in items.get("data", []):
        msg += f"""{item.get("created_at")}\n"""
        msg += f"""{item.get("user")}\n"""
        msg += f"""{item.get("local")}\n"""
        msg += f"""{item.get("parameter")} : {item.get("value")}\n\n"""
        msg += "\n\n"

    msg += "*Para mais informações consulte na plataforma!*"

    return [{"type": "text", "msg": msg}]


def handler_orp_ozonio_fora_faixa(plate_list):
    if len(plate_list) == 0:
        msg = "Não há placas com ORP do ozônio fora da faixa de operação."
        audio_msg = audio_generate(msg)
        return [{"type": "text", "msg": msg}, audio_msg]
    else:
        already_used_list = []
        farms_list = ""
        for plate in plate_list:
            if plate.farm_associated in already_used_list:
                continue

            already_used_list.append(plate.farm_associated)
            farms_list += f"""{plate.farm_associated};\n"""

        msg = f"Nesse momento existe *{len(already_used_list)}* local(is) com ORP do Ozônio fora da faixa. São eles:\n\n{farms_list}"
        msg2audio = f"Nesse momento, existe {len(already_used_list)} local com ORP do Ozônio fora da faixa. São eles:\n\n{farms_list}"
        audio_msg = audio_generate(msg2audio)
        return [{"type": "text", "msg": msg}, audio_msg]


def handler_quadro_com_problemas(plate_list):
    if len(plate_list) == 0:
        msg = "Não há quadros com falha nesse momento."
        audio_msg = audio_generate(msg)
        return [{"type": "text", "msg": msg}, audio_msg]

    already_used_list = []
    farms_list = ""
    for plate in plate_list:
        if plate.farm_associated in already_used_list:
            continue

        already_used_list.append(plate.farm_associated)
        farms_list += f"""{plate.farm_associated};\n"""

    if len(already_used_list) == 0:
        msg = "Nesse momento existe"
    else:
        msg = "Nesse momento existem"

    msg += f" *{len(already_used_list)}* local(is) com ORP do Ozônio fora da faixa. São eles:\n\n{farms_list}"
    msg2audio = f"Nesse momento, existe {len(already_used_list)} local com ORP do Ozônio fora da faixa. São eles:\n\n{farms_list}"
    audio_msg = audio_generate(msg2audio)
    return [{"type": "text", "msg": msg}, audio_msg]


def handler_tempo_real_ph(farm, plates):
    msg_text = mount_header_farm(farm)

    if len(plates) == 0:
        msg = f"Não encontrado placas para esse local {farm.name} "
        return [{"type": "text", "msg": msg}]
    for plate_ in plates:
        plate = plate_.to_dict()
        if plate["plate_type"] not in ["Z1", "PHI"]:
            continue

        # """1️⃣ Sensores em tempo real;"""
        _Event = get_events_model(plate["plate_type"])
        last_ev = _Event.get_last_event(plate["owner"], plate["serial"])
        if plate["plate_type"] == "Z1":
            msg_text += mount_only_ph(plate, last_ev)
        elif plate["plate_type"] == "PHI":
            msg_text += mount_realtime_phi(plate, last_ev)

    return [{"type": "text", "msg": msg_text}]


def handler_quantidade_acido(farm, plates):
    msg_text = mount_header_farm(farm)

    if len(plates) == 0:
        msg = f"Não encontrado placas para o local {farm.name} "
        return [{"type": "text", "msg": msg}]
    for plate_ in plates:
        plate = plate_.to_dict()
        if plate["plate_type"] not in ["Z1", "PHI"]:
            continue

        # """1️⃣ Sensores em tempo real;"""
        _Event = get_events_model(plate["plate_type"])
        last_ev = _Event.get_last_event(plate["owner"], plate["serial"])
        msg_text += mount_realtime_acid(plate, last_ev)

    return [{"type": "text", "msg": msg_text}]


def handler_quantidade_cloro(farm, plates):
    msg_text = mount_header_farm(farm)

    if len(plates) == 0:
        msg = f"Não encontrado placas para o local {farm.name} "
        return [{"type": "text", "msg": msg}]
    for plate_ in plates:
        plate = plate_.to_dict()
        if plate["plate_type"] not in ["Z1", "PHI"]:
            continue

        # """1️⃣ Sensores em tempo real;"""
        _Event = get_events_model(plate["plate_type"])
        last_ev = _Event.get_last_event(plate["owner"], plate["serial"])
        msg_text += mount_realtime_chlorine(plate, last_ev)

    return [{"type": "text", "msg": msg_text}]


def handler_quantidade_acido_cloro(farm, plates):
    msg_text = mount_header_farm(farm)

    if len(plates) == 0:
        msg = f"Não encontrado placas para o local {farm.name} "
        return [{"type": "text", "msg": msg}]

    for plate_ in plates:
        plate = plate_.to_dict()
        if plate["plate_type"] not in ["Z1", "PHI"]:
            continue

        # """1️⃣ Sensores em tempo real;"""
        _Event = get_events_model(plate["plate_type"])
        last_ev = _Event.get_last_event(plate["owner"], plate["serial"])
        msg_text += mount_realtime_acid(plate, last_ev)
        msg_text += "\n\n"
        msg_text += mount_realtime_chlorine(plate, last_ev)

    return [{"type": "text", "msg": msg_text}]


def handler_tempo_real_ozonio(farm, plates):

    msg_text = mount_header_farm(farm)

    if len(plates) == 0:
        msg = f"Não encontrado placas para: {farm.name} "
        return [{"type": "text", "msg": msg}]

    for plate_ in plates:
        plate = plate_.to_dict()

        _Event = get_events_model(plate["plate_type"])
        last_ev = _Event.get_last_event(plate["owner"], plate["serial"])
        msg_text += mount_realtime_oz1(plate, last_ev)

    return [{"type": "text", "msg": msg_text}]


def handler_tempo_real_central_dosagem(farm, plates):

    msg_text = mount_header_farm(farm)

    if len(plates) == 0:
        msg = f"Não encontrado placas para: {farm.name} "
        return [{"type": "text", "msg": msg}]

    for plate_ in plates:
        plate = plate_.to_dict()

        _Event = get_events_model(plate["plate_type"])
        last_ev = _Event.get_last_event(plate["owner"], plate["serial"])
        msg_text += mount_realtime_ccd(plate, last_ev)

    return [{"type": "text", "msg": msg_text}]


def handler_tempo_real_geral(farm, plates, msisdn=None):

    if len(plates) == 0:
        msg = f"Não encontrado placas para: {farm.name} "
        return [{"type": "text", "msg": msg}]

    filters = {"farm_associated": farm.name}
    msg = mount_header_farm(farm)
    msg += mount_realtime_message(filters)

    messages = [{"type": "text", "msg": msg}]

    # Adiciona botão de análise ETA se usuário tiver placas relevantes
    messages = add_eta_analysis_button(messages, plates)

    # Salva a farm no state para análise ETA posterior
    if msisdn:
        from z1monitoring_models.models.active_chats import ActiveChats
        from sqlalchemy.orm.attributes import flag_modified

        active_chat = ActiveChats.load(msisdn)
        if active_chat:
            if not active_chat.context:
                active_chat.context = {}
            active_chat.context["last_farm_requested"] = farm.name
            flag_modified(active_chat, "context")
            ActiveChats.save(active_chat)

    return messages


def handler_tempo_real_orp(farm, plates):
    msg_text = mount_header_farm(farm)

    if len(plates) == 0:
        msg = f"Não encontrado placas para o local {farm.name} "
        return [{"type": "text", "msg": msg}]

    for plate_ in plates:
        plate = plate_.to_dict()
        if plate["plate_type"] not in ["Z1", "ORP"]:
            continue

        # """1️⃣ Sensores em tempo real;"""
        _Event = get_events_model(plate["plate_type"])
        last_ev = _Event.get_last_event(plate["owner"], plate["serial"])
        if plate["plate_type"] == "Z1":
            msg_text += mount_only_orp(plate, last_ev)
        elif plate["plate_type"] == "ORP":
            msg_text += mount_realtime_orp(plate, last_ev)

    return [{"type": "text", "msg": msg_text}]


def handler_tempo_real_fluxo(response, farm, plates):

    msg_text = mount_header_farm(farm)

    if len(plates) == 0:
        msg = f"Não encontrado placas para o local {farm.name} "
        return [{"type": "text", "msg": msg}]

    data_solicited = response.get("data", None)
    for plate_ in plates:
        plate = plate_.to_dict()
        if plate["plate_type"] not in ["FLX"]:
            continue

        # """1️⃣ Sensores em tempo real;"""
        if not data_solicited:
            _Event = get_events_model(plate["plate_type"])
            last_ev = _Event.get_last_event(plate["owner"], plate["serial"])
            if plate["plate_type"] == "FLX":
                msg_text += mount_realtime_water_flow(plate, last_ev)
        else:
            if plate["plate_type"] == "FLX":
                _Event = get_events_model(plate["plate_type"])
                last_ev = _Event.get_water_consumed_at_date(plate["farm_associated"], plate["serial"], data_solicited)
                msg_text += mount_realtime_water_flow(plate, last_ev)

    return [{"type": "text", "msg": msg_text}]


def handler_tempo_real_temperatura(farm, plates):
    """
    Handler para tempo real de temperatura da água.
    Placas compatíveis: Z1, CCD (possuem sensor de temperatura)
    """
    msg_text = mount_header_farm(farm)

    if len(plates) == 0:
        msg = f"Não encontrado placas para o local {farm.name}"
        return [{"type": "text", "msg": msg}]

    found_temperature = False
    for plate_ in plates:
        plate = plate_.to_dict()
        # Placas que têm sensor de temperatura
        if plate["plate_type"] not in ["Z1", "CCD"]:
            continue

        _Event = get_events_model(plate["plate_type"])
        last_ev = _Event.get_last_event(plate["owner"], plate["serial"])
        msg_text += mount_only_temperature(plate, last_ev)
        found_temperature = True

    if not found_temperature:
        msg = f"O local {farm.name} não possui sensores de temperatura de água."
        return [{"type": "text", "msg": msg}]

    return [{"type": "text", "msg": msg_text}]


def handler_tempo_real_elevatoria(farm, plates):
    msg_text = mount_header_farm(farm)

    if len(plates) == 0:
        msg = f"Não encontrado placas para {farm.name} "
        return [{"type": "text", "msg": msg}]

    for plate_ in plates:
        plate = plate_.to_dict()
        if plate["plate_type"] not in ["QP7"]:
            continue

        # """1️⃣ Sensores em tempo real;"""
        _Event = get_events_model(plate["plate_type"])
        last_ev = _Event.get_last_event(plate["owner"], plate["serial"])
        msg_text += mount_tempo_real_elevatoria(plate, last_ev)

    return [{"type": "text", "msg": msg_text}]


def handler_tempo_real_gas(farm, plates):
    if len(plates) == 0:
        msg = f"Não encontrado equipamentos para: {farm.name} que possam medir gás."
        return [{"type": "text", "msg": msg}]

    msg_header = mount_header_farm(farm)
    msg_header += "*Medição de gás*\n\n"
    msg_text = ""
    for plate_ in plates:
        iomap = plate_.params.get("iomap", {})

        for key, value in iomap.items():
            if value.get("status", "enable") == "disable":
                continue

            if "gas" not in unidecode(value.get("description")).strip(punctuation).lower():
                continue

            _Event = get_events_model(plate_.plate_type)
            last_ev = _Event.get_last_event(plate_.owner, plate_.serial)
            if not last_ev:
                continue
            gas_raw = last_ev.get(value.get("description"))
            try:
                gas_val = float(gas_raw) if gas_raw is not None else None
            except (ValueError, TypeError):
                gas_val = None
            capacity = value.get("capacity")
            try:
                capacity = float(capacity) if capacity else None
            except (ValueError, TypeError):
                capacity = None
            if gas_val is not None and capacity:
                pct = (gas_val / capacity) * 100
                msg_text += f"""*{value.get("description")}*: {gas_val:.1f} kg — {pct:.0f}%\n"""
            else:
                msg_text += f"""*{value.get("description")}*: {gas_raw} kg\n"""

    if msg_text:
        msg_text = msg_header + msg_text
    else:
        msg_text = msg_header + "Não foi encontrado medições."

    return [{"type": "text", "msg": msg_text}]


def handler_graphic_request(farm, plates, number_of_days_analisys):
    """
    Gera gráficos de consumo para diferentes tipos de placas.

    Suporta:
    - Z1/CCD: Consumo de ácido e cloro
    - FLX: Consumo de água e temperatura
    - WGT: Consumo dinâmico baseado no iomap
    """
    if len(plates) == 0:
        msg = f"Não encontrado placas para o local {farm.name}"
        return [{"type": "text", "msg": msg}]

    msg_text = mount_header_farm(farm)
    now = datetime.datetime.now()
    date_upper = now.strftime("%Y-%m-%d %H:%M:%S")
    date_lower = (now - timedelta(days=number_of_days_analisys)).strftime("%Y-%m-%d 00:00:00")

    # Usa função centralizada para gerar gráficos
    graphics_list = generate_consumption_graphics(farm, date_lower, date_upper)

    # Adiciona header msg aos gráficos
    for graphic in graphics_list:
        graphic["msg"] = msg_text

    return graphics_list


# ------------------------
# Análise de ETA (Estação de Tratamento de Água)
# ------------------------


def user_has_eta_plates(plates):
    """
    Verifica se o usuário possui placas do tipo ETA (tratamento de água).

    Args:
        plates: Lista de placas do usuário

    Returns:
        bool: True se possui placas ETA (CCD, Z1)
    """
    ETA_PLATE_TYPES = ["CCD", "Z1"]

    for plate in plates:
        if plate.plate_type in ETA_PLATE_TYPES:
            return True
    return False


def add_eta_analysis_button(messages, plates):
    """
    Adiciona botão 'Analisar minha ETA' se usuário possui placas de tratamento.

    Args:
        messages: Lista de mensagens a ser retornada
        plates: Lista de placas do usuário

    Returns:
        Lista de mensagens com botão adicionado (se aplicável)
    """
    if not user_has_eta_plates(plates):
        return messages

    # Adiciona botão após a última mensagem
    messages.append(
        {
            "type": "buttons",
            "msg": "💡 Quer entender melhor?",
            "buttons": [{"id": "analisar", "title": "🔬 Analisar ETA"}],
        }
    )

    return messages
