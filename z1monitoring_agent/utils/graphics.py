import matplotlib.pyplot as plt
import pandas as pd
import random
import structlog

log = structlog.get_logger()


def consume(evs2graphic, lista_controle_temp):

    panda_ob = {"data": [], "acido": [], "cloro": [], "agua": []}
    for sensor in lista_controle_temp:
        if sensor not in panda_ob:
            panda_ob.update({sensor: []})

    list_days = sorted(evs2graphic.keys())
    # for dateday, obj in evs2graphic.items():
    for day in list_days:
        panda_ob["data"].append(day)

        panda_ob["acido"].append(float(evs2graphic[day].get("acid_consumed_acc") or 0.0))
        panda_ob["cloro"].append(float(evs2graphic[day].get("chlorine_consumed_acc") or 0.0))
        panda_ob["agua"].append(float(evs2graphic[day].get("water_consumed") or 0.0))

        for sensor in lista_controle_temp:
            panda_ob[sensor].append(float(evs2graphic[day].get(sensor) or 0.0))

    # Converter para DataFrame do Pandas
    df = pd.DataFrame(panda_ob)

    # Converter a coluna 'data' para tipo datetime
    df["data"] = pd.to_datetime(df["data"])

    # Ordenar DataFrame pela coluna 'data'
    # df = df.sort_values(by='data')

    # Normalizar os valores da água
    for v in df["agua"].values:
        if v > 10000:
            df["agua"] = df["agua"] / 10000
            # log.info("dividiu por 10mil")
            MULTIPLICADOR = 10
            break
        elif v > 1000:
            df["agua"] = df["agua"] / 1000
            # log.info(f"Aqui dividiu por 1mil. {df['agua'].values[0]}")
            MULTIPLICADOR = 1
            break
        else:
            # log.info("Nao dividiu")
            # log.info(f"{df['agua'].values[0]} - {df['agua']}")
            MULTIPLICADOR = 1

    # Definir a largura das barras
    bar_width = 0.2

    # Calcular altura mínima visível (3% do valor máximo)
    max_value = max(df["acido"].max(), df["cloro"].max(), df["agua"].max())
    min_bar_height = max_value * 0.03 if max_value > 0 else 0.1

    log.info(f"Graphics - max_value: {max_value}, min_bar_height: {min_bar_height}")
    log.info(f"Graphics - acido values: {df['acido'].tolist()}")
    log.info(f"Graphics - cloro values: {df['cloro'].tolist()}")
    log.info(f"Graphics - agua values: {df['agua'].tolist()}")

    # Criar cópias dos dados com altura mínima para visualização
    acido_visual = df["acido"].copy()
    cloro_visual = df["cloro"].copy()

    # Aplicar altura mínima apenas para valores não-zero muito pequenos
    for i in range(len(acido_visual)):
        if 0 < acido_visual.iloc[i] < min_bar_height:
            log.info(f"Ajustando acido[{i}] de {acido_visual.iloc[i]} para {min_bar_height}")
            acido_visual.iloc[i] = min_bar_height
        if 0 < cloro_visual.iloc[i] < min_bar_height:
            log.info(f"Ajustando cloro[{i}] de {cloro_visual.iloc[i]} para {min_bar_height}")
            cloro_visual.iloc[i] = min_bar_height

    # Plotar gráfico de barras e linha
    fig, ax1 = plt.subplots(figsize=(15, 9))

    # Plotar barras para Ácido, Cloro e Água (usando valores ajustados para visualização)
    ax1.bar(
        df.index - bar_width,
        acido_visual,
        color="b",
        width=bar_width,
        align="center",
        label="Ácido (Kg)",
    )
    ax1.bar(
        df.index,
        cloro_visual,
        color="g",
        width=bar_width,
        align="center",
        label="Cloro (Kg)",
    )
    ax1.bar(
        df.index + bar_width,
        df["agua"],
        color="r",
        width=bar_width,
        align="center",
        label="Água / 1000L",
    )

    # Ajustar escala do eixo Y para garantir que valores pequenos sejam visíveis
    max_acido = df["acido"].max()
    max_cloro = df["cloro"].max()
    max_agua = df["agua"].max()
    max_all = max(max_acido, max_cloro, max_agua)

    # Se água é muito maior que ácido/cloro, ajustar ylim para mostrar valores pequenos
    if max_all > 0:
        # Garantir margem de 20% acima do máximo
        ax1.set_ylim(0, max_all * 1.2)

        # Se há valores muito pequenos (< 5% do máximo), ajustar a visibilidade
        min_visible = max_all * 0.02  # 2% do máximo como altura mínima visível

        # Ajustar posição dos textos para valores pequenos
        for i, row in df.iterrows():
            # Ácido
            text_y_acido = max(row["acido"] + 0.05, min_visible) if row["acido"] > 0 else row["acido"]
            ax1.text(
                i - bar_width,
                text_y_acido,
                f"{row['acido']:.1f}",
                ha="center",
            )
            # Cloro
            text_y_cloro = max(row["cloro"] + 0.05, min_visible) if row["cloro"] > 0 else row["cloro"]
            ax1.text(i, text_y_cloro, f"{row['cloro']:.1f}", ha="center")
            # Água
            ax1.text(
                i + bar_width,
                row["agua"] + 0.05,
                f"{round(MULTIPLICADOR * row['agua'], 1)}",
                ha="center",
            )
    else:
        # Fallback se não houver dados
        for i, row in df.iterrows():
            ax1.text(
                i - bar_width,
                row["acido"] + 0.05,
                f"{row['acido']:.1f}",
                ha="center",
            )
            ax1.text(i, row["cloro"] + 0.05, f"{row['cloro']:.1f}", ha="center")
            ax1.text(
                i + bar_width,
                row["agua"] + 0.05,
                f"{round(MULTIPLICADOR * row['agua'], 1)}",
                ha="center",
            )

    # Configurações do eixo x
    ax1.set_xlabel("Data")
    ax1.set_ylabel("Valores")
    ax1.set_title("Consumo de Ácido, Cloro e Água ao longo do tempo")
    ax1.set_xticks(range(len(df)))
    ax1.set_xticklabels(df["data"].dt.strftime("%Y-%m-%d"), rotation=45)
    ax1.legend(loc="upper left")

    # Criar o segundo eixo y para a temperatura
    ax2 = ax1.twinx()
    ax2.set_ylim(10, 30)  # Definindo os limites do eixo y para a temperatura
    colors = ["orange", "pink", "blue"]
    idx = 0
    for sensor in lista_controle_temp:
        ax2.plot(df.index, df[sensor], color=colors[idx], marker="o", label=sensor)
        idx = idx + 1

    # Adicionar valor de temperatura em cada ponto
    for sensor in lista_controle_temp:
        for i, temp in enumerate(df[sensor]):
            ax2.text(
                i, temp + 0.5, f"{temp}°C", ha="center", va="bottom"
            )  # Corrigindo a posição do texto da temperatura

    # Configurações do eixo y para a temperatura
    ax2.set_ylabel("Temperatura (°C)")
    ax2.legend(loc="upper right")

    plt.tight_layout()

    hashid = str(random.getrandbits(128))
    fname = f"{hashid}.png"

    import os

    space_dir = "/home/ubuntu/space"
    if not os.path.exists(space_dir):
        log.warning(f"Diretório {space_dir} não existe, usando /tmp")
        space_dir = "/tmp"

    fname_with_dir = f"{space_dir}/{fname}"

    try:
        plt.savefig(fname_with_dir)
        # Força sync para evitar race condition com META
        with open(fname_with_dir, "rb") as f:
            os.fsync(f.fileno())
        log.info(f"Gráfico consumo salvo em: {fname_with_dir}")
    except Exception as e:
        log.error(f"Erro ao salvar gráfico consumo: {e}")

    plt.close()

    return fname, fname_with_dir


def consume_wgt(plate, evs2graphic, lista_controle_temp, gas_level_data=None):

    log.info(f"consume_wgt - evs2graphic: {evs2graphic}")
    log.info(f"consume_wgt - lista_controle_temp: {lista_controle_temp}")
    log.info(f"consume_wgt - gas_level_data: {gas_level_data}")

    panda_ob = {"data": [], "gas": [], "percentual": []}

    list_days = sorted(evs2graphic.keys())
    for day in list_days:
        panda_ob["data"].append(day)
        # Busca valor de gás por qualquer chave que contenha "gas"
        day_data = evs2graphic[day]
        gas_val = day_data.get("Consumo Gas Acumulado") or day_data.get("gas")
        if gas_val is None:
            gas_val = next((v for k, v in day_data.items() if "gas" in k.lower()), 0.0)
        panda_ob["gas"].append(float(gas_val or 0.0))

        # Busca percentual de gás disponível para o dia
        percentual = gas_level_data.get(day, 0) if gas_level_data else 0
        panda_ob["percentual"].append(float(percentual))

    # Converter para DataFrame do Pandas
    df = pd.DataFrame(panda_ob)

    # Converter a coluna 'data' para tipo datetime
    df["data"] = pd.to_datetime(df["data"])

    # Definir a largura das barras
    bar_width = 0.4

    # Plotar gráfico de barras e linha
    fig, ax1 = plt.subplots(figsize=(15, 9))

    # Plotar barras para consumo de gás
    ax1.bar(
        df.index,
        df["gas"],
        color="b",
        width=bar_width,
        align="center",
        label="Consumo (Kg)",
    )

    # Adicionar valor sobre cada barra
    for i, row in df.iterrows():
        ax1.text(i, row["gas"] + 0.05, f"{row['gas']:.1f}", ha="center")

    # Configurações do eixo x
    ax1.set_xlabel("Data")
    ax1.set_ylabel("Consumo (Kg)")
    ax1.set_title("Consumo de Gás e Nível do Tanque")
    ax1.set_xticks(range(len(df)))
    ax1.set_xticklabels(df["data"].dt.strftime("%Y-%m-%d"), rotation=45)
    ax1.legend(loc="upper left")

    # Criar segundo eixo Y para percentual (se houver dados)
    if gas_level_data and any(df["percentual"] > 0):
        ax2 = ax1.twinx()

        # Limite dinâmico: max(110, maior_valor + 10%)
        max_perc = df["percentual"].max()
        y_max = max(110, max_perc * 1.1)
        ax2.set_ylim(0, y_max)

        ax2.plot(df.index, df["percentual"], color="green", marker="o", linewidth=2, label="Nível (%)")

        # Adicionar valor de percentual em cada ponto
        offset_y = y_max * 0.02  # 2% do range para offset do texto
        for i, perc in enumerate(df["percentual"]):
            if perc > 0:
                ax2.text(i, perc + offset_y, f"{perc:.0f}%", ha="center", va="bottom", color="green", fontweight="bold")

        ax2.set_ylabel("Nível do Tanque (%)", color="green")
        ax2.tick_params(axis="y", labelcolor="green")
        ax2.legend(loc="upper right")

    plt.tight_layout()

    hashid = str(random.getrandbits(128))
    fname = f"{hashid}.png"

    import os

    space_dir = "/home/ubuntu/space"
    if not os.path.exists(space_dir):
        log.warning(f"Diretório {space_dir} não existe, usando /tmp")
        space_dir = "/tmp"

    fname_with_dir = f"{space_dir}/{fname}"

    try:
        plt.savefig(fname_with_dir)
        # Força sync para evitar race condition com META
        with open(fname_with_dir, "rb") as f:
            os.fsync(f.fileno())
        log.info(f"Gráfico WGT salvo em: {fname_with_dir}")
    except Exception as e:
        log.error(f"Erro ao salvar gráfico WGT: {e}")

    plt.close()

    return fname, fname_with_dir
