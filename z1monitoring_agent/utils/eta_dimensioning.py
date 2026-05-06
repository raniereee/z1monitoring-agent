"""
Dimensionamento de ETA com pré-tratamento por ozônio.

Calcula:
- Dosagem e capacidade do gerador de ozônio (g/h)
- Faixa de pH ideal para operação
- Faixa de ORP ideal para operação
- Volume do tanque de contato
- Recomendações de filtração e pós-tratamento
"""

import os
import uuid
import tempfile
from datetime import datetime
from fpdf import FPDF
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import structlog

log = structlog.get_logger()

# Fatores estequiométricos de demanda de ozônio (mg O₃ por mg do contaminante)
O3_DEMAND = {
    "ferro": 0.43,
    "manganes": 0.88,
    "cor": 0.10,       # por unidade de cor aparente
    "dqo": 0.50,
    "sulfeto": 3.0,
    "turbidez": 0.05,  # contribuição menor, por NTU
}

# Ozônio residual base (mg/L) para garantir desinfecção
O3_RESIDUAL_BASE = 0.5

# Fator de segurança
SAFETY_FACTOR = 1.8

# Tempo de contato padrão (minutos)
CONTACT_TIME_MIN = 10


def calculate_eta(params: dict) -> dict:
    """
    Calcula o dimensionamento da ETA.

    Args:
        params: {
            "consumo_diario_litros": float,
            "ferro": float (mg/L),
            "manganes": float (mg/L),
            "ph": float,
            "turbidez": float (NTU),
            "cor": float (uH),
            "dqo": float (mg/L, opcional),
            "sulfeto": float (mg/L, opcional),
            "dureza": float (mg/L, opcional),
            "coliformes_totais": float (opcional),
            "e_coli": float (opcional),
            "alcalinidade": float (mg/L, opcional),
            "solidos_totais": float (mg/L, opcional),
            "cliente": str (nome do cliente),
            "local": str (local da instalação),
        }

    Returns:
        dict com dimensionamento completo
    """
    consumo_l = params.get("consumo_diario_litros", 0)
    ferro = params.get("ferro", 0) or 0
    manganes = params.get("manganes", 0) or 0
    ph = params.get("ph", 7.0) or 7.0
    turbidez = params.get("turbidez", 0) or 0
    cor = params.get("cor", 0) or 0
    dqo = params.get("dqo", 0) or 0
    sulfeto = params.get("sulfeto", 0) or 0

    # Demanda de ozônio (mg/L)
    demanda = (
        ferro * O3_DEMAND["ferro"]
        + manganes * O3_DEMAND["manganes"]
        + cor * O3_DEMAND["cor"]
        + turbidez * O3_DEMAND["turbidez"]
        + dqo * O3_DEMAND["dqo"]
        + sulfeto * O3_DEMAND["sulfeto"]
        + O3_RESIDUAL_BASE
    )

    # Aplicar fator de segurança
    demanda_total = demanda * SAFETY_FACTOR

    # Vazão em L/h
    vazao_lh = consumo_l / 24.0

    # Capacidade do gerador de ozônio (g/h)
    ozonio_gh = (demanda_total * vazao_lh) / 1000.0

    # Volume do tanque de contato (litros)
    vazao_lmin = vazao_lh / 60.0
    volume_tanque = vazao_lmin * CONTACT_TIME_MIN

    # Faixa de pH ideal
    if manganes > 0.5:
        ph_min, ph_max = 7.0, 7.8
    elif ferro > 1.0:
        ph_min, ph_max = 6.5, 7.5
    else:
        ph_min, ph_max = 6.5, 7.5

    # Faixa de ORP ideal (mV)
    if manganes > 0.5:
        orp_min, orp_max = 700, 800
    elif ferro > 1.0:
        orp_min, orp_max = 650, 750
    else:
        orp_min, orp_max = 600, 700

    # Recomendações de filtração
    filtros = []
    if ferro > 0.3 or manganes > 0.1:
        filtros.append("Filtro de areia/birm para retenção de ferro e manganês oxidados")
    if cor > 15 or dqo > 5:
        filtros.append("Filtro de carvão ativado para remoção de cor e matéria orgânica")
    if turbidez > 5:
        filtros.append("Filtro multimídia para redução de turbidez")
    if not filtros:
        filtros.append("Filtro de areia para polimento final")

    # Correção de pH
    correcao_ph = None
    if ph < 6.0:
        correcao_ph = "Dosagem de cal hidratada ou soda cáustica para elevação do pH"
    elif ph > 8.5:
        correcao_ph = "Dosagem de ácido para redução do pH"

    return {
        "consumo_diario_litros": consumo_l,
        "vazao_lh": round(vazao_lh, 1),
        "demanda_o3_mg_l": round(demanda_total, 2),
        "ozonio_gh": round(ozonio_gh, 2),
        "volume_tanque_contato_l": round(volume_tanque, 1),
        "tempo_contato_min": CONTACT_TIME_MIN,
        "ph_min": ph_min,
        "ph_max": ph_max,
        "orp_min": orp_min,
        "orp_max": orp_max,
        "filtros_recomendados": filtros,
        "correcao_ph": correcao_ph,
        "parametros_entrada": {
            "ferro": ferro,
            "manganes": manganes,
            "ph": ph,
            "turbidez": turbidez,
            "cor": cor,
            "dqo": dqo,
            "sulfeto": sulfeto,
            "dureza": params.get("dureza", None),
            "coliformes_totais": params.get("coliformes_totais", None),
            "e_coli": params.get("e_coli", None),
            "alcalinidade": params.get("alcalinidade", None),
            "solidos_totais": params.get("solidos_totais", None),
        },
        "cliente": params.get("cliente", ""),
        "local": params.get("local", ""),
        "com_ozonio": params.get("com_ozonio", True),
        "com_pac": params.get("com_pac", False),
        "fonte_origem": params.get("fonte_origem", ""),
    }


def generate_pdf(dimensionamento: dict) -> str:
    """
    Gera PDF profissional com o dimensionamento da ETA.

    Returns:
        Caminho do arquivo PDF gerado.
    """
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=20)
    pdf.add_page()

    # Header
    pdf.set_fill_color(0, 71, 133)
    pdf.rect(0, 0, 210, 40, "F")
    pdf.set_text_color(255, 255, 255)
    pdf.set_font("Helvetica", "B", 22)
    pdf.set_y(8)
    pdf.cell(0, 12, "Dimensionamento de ETA", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 11)
    pdf.cell(0, 8, "Pre-tratamento com Ozonio", align="C", new_x="LMARGIN", new_y="NEXT")

    # Info do cliente
    pdf.set_text_color(0, 0, 0)
    pdf.set_y(48)
    pdf.set_font("Helvetica", "", 10)

    cliente = dimensionamento.get("cliente", "")
    local = dimensionamento.get("local", "")
    data = datetime.now().strftime("%d/%m/%Y")

    if cliente:
        pdf.cell(0, 6, f"Cliente: {cliente}", new_x="LMARGIN", new_y="NEXT")
    if local:
        pdf.cell(0, 6, f"Local: {local}", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 6, f"Data: {data}", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 6, f"Consumo diario: {dimensionamento['consumo_diario_litros']:,.0f} litros/dia", new_x="LMARGIN", new_y="NEXT")

    pdf.ln(6)

    # Seção: Parâmetros da Análise de Água
    _section_title(pdf, "Parametros da Analise de Agua")
    params = dimensionamento["parametros_entrada"]

    param_labels = {
        "ferro": ("Ferro (Fe)", "mg/L"),
        "manganes": ("Manganes (Mn)", "mg/L"),
        "ph": ("pH", ""),
        "turbidez": ("Turbidez", "NTU"),
        "cor": ("Cor Aparente", "uH"),
        "dqo": ("DQO", "mg/L"),
        "sulfeto": ("Sulfeto (H2S)", "mg/L"),
        "dureza": ("Dureza Total", "mg/L"),
        "alcalinidade": ("Alcalinidade", "mg/L"),
        "solidos_totais": ("Solidos Totais", "mg/L"),
        "coliformes_totais": ("Coliformes Totais", "NMP/100mL"),
        "e_coli": ("E. coli", "NMP/100mL"),
    }

    for key, (label, unit) in param_labels.items():
        val = params.get(key)
        if val is not None and val != 0:
            val_str = f"{val}" if unit == "" else f"{val} {unit}"
            _param_row(pdf, label, val_str)

    pdf.ln(6)

    # Seção: Dimensionamento
    _section_title(pdf, "Dimensionamento")

    _result_row(pdf, "Gerador de Ozonio", f"{dimensionamento['ozonio_gh']} g/h")
    _result_row(pdf, "Demanda de O3", f"{dimensionamento['demanda_o3_mg_l']} mg/L")
    _result_row(pdf, "Vazao", f"{dimensionamento['vazao_lh']:,.1f} L/h")
    _result_row(pdf, "Faixa de pH", f"{dimensionamento['ph_min']} - {dimensionamento['ph_max']}")
    _result_row(pdf, "Faixa de ORP", f"{dimensionamento['orp_min']} - {dimensionamento['orp_max']} mV")
    _result_row(pdf, "Tanque de Contato", f"{dimensionamento['volume_tanque_contato_l']:,.1f} litros")
    _result_row(pdf, "Tempo de Contato", f"{dimensionamento['tempo_contato_min']} minutos")

    pdf.ln(6)

    # Seção: Recomendações
    _section_title(pdf, "Recomendacoes")

    pdf.set_font("Helvetica", "", 10)
    for filtro in dimensionamento.get("filtros_recomendados", []):
        pdf.cell(6, 6, "-")
        pdf.cell(0, 6, f" {filtro}", new_x="LMARGIN", new_y="NEXT")

    if dimensionamento.get("correcao_ph"):
        pdf.cell(6, 6, "-")
        pdf.cell(0, 6, f" {dimensionamento['correcao_ph']}", new_x="LMARGIN", new_y="NEXT")

    pdf.cell(6, 6, "-")
    pdf.cell(0, 6, " Dosagem de cloro residual: 0.2 - 0.5 mg/L (pos-tratamento)", new_x="LMARGIN", new_y="NEXT")

    # Diagrama de blocos
    com_ozonio = dimensionamento.get("com_ozonio", True)
    com_pac = dimensionamento.get("com_pac", False)
    diagram_path = _generate_diagram(com_ozonio, com_pac)
    if diagram_path:
        pdf.add_page()
        _section_title(pdf, "Fluxograma do Tratamento")
        pdf.ln(2)
        pdf.image(diagram_path, x=10, w=190)
        try:
            os.remove(diagram_path)
        except Exception:
            pass

    # Footer
    pdf.set_y(-30)
    pdf.set_font("Helvetica", "I", 8)
    pdf.set_text_color(128, 128, 128)
    pdf.cell(0, 5, "Documento gerado automaticamente pelo sistema Z1 Monitoramento.", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 5, "Os valores apresentados sao estimativas tecnicas e devem ser validados por um profissional.", align="C")

    # Salvar
    fname = f"dimensionamento_eta_{uuid.uuid4().hex[:8]}.pdf"
    output_dir = os.environ.get("PATH_REPORTS", "/tmp/")
    fpath = os.path.join(output_dir, fname)
    pdf.output(fpath)

    log.info("PDF de dimensionamento gerado", path=fpath)
    return fpath


def _generate_diagram(com_ozonio: bool, com_pac: bool) -> str:
    """Gera diagrama de blocos do fluxo de tratamento e retorna o caminho da imagem."""
    try:
        # Definir etapas conforme tipo de tratamento
        if com_ozonio and not com_pac:
            steps = [
                ("Agua\nBruta", "#4A90D9"),
                ("Acidificacao", "#E74C3C"),
                ("Gerador\nde Ozonio", "#E67E22"),
                ("Tanque de\nContato", "#F39C12"),
                ("Filtracao", "#27AE60"),
                ("Cloracao", "#8E44AD"),
                ("Agua\nTratada", "#2ECC71"),
            ]
        elif com_ozonio and com_pac:
            steps = [
                ("Agua\nBruta", "#4A90D9"),
                ("Acidificacao", "#E74C3C"),
                ("PAC /\nFloculacao", "#3498DB"),
                ("Decantacao", "#2980B9"),
                ("Gerador\nde Ozonio", "#E67E22"),
                ("Tanque de\nContato", "#F39C12"),
                ("Filtracao", "#27AE60"),
                ("Cloracao", "#8E44AD"),
                ("Agua\nTratada", "#2ECC71"),
            ]
        elif not com_ozonio and not com_pac:
            steps = [
                ("Agua\nBruta", "#4A90D9"),
                ("Acidificacao", "#E74C3C"),
                ("Filtracao", "#27AE60"),
                ("Cloracao", "#8E44AD"),
                ("Agua\nTratada", "#2ECC71"),
            ]
        else:  # sem ozonio, com pac
            steps = [
                ("Agua\nBruta", "#4A90D9"),
                ("Acidificacao", "#E74C3C"),
                ("PAC /\nFloculacao", "#3498DB"),
                ("Decantacao", "#2980B9"),
                ("Filtracao", "#27AE60"),
                ("Cloracao", "#8E44AD"),
                ("Agua\nTratada", "#2ECC71"),
            ]

        n = len(steps)
        fig_width = max(n * 2.2, 10)
        fig, ax = plt.subplots(1, 1, figsize=(fig_width, 2.5))
        ax.set_xlim(-0.5, n * 2.2)
        ax.set_ylim(-0.5, 2)
        ax.axis("off")

        box_w = 1.6
        box_h = 1.2
        gap = 2.2
        y = 0.4

        for i, (label, color) in enumerate(steps):
            x = i * gap
            rect = mpatches.FancyBboxPatch(
                (x, y), box_w, box_h,
                boxstyle="round,pad=0.1",
                facecolor=color, edgecolor="white", linewidth=2,
            )
            ax.add_patch(rect)
            ax.text(
                x + box_w / 2, y + box_h / 2, label,
                ha="center", va="center", fontsize=8, fontweight="bold",
                color="white",
            )

            # Seta
            if i < n - 1:
                ax.annotate(
                    "", xy=(x + gap, y + box_h / 2), xytext=(x + box_w, y + box_h / 2),
                    arrowprops=dict(arrowstyle="->", color="#555555", lw=2),
                )

        plt.tight_layout(pad=0.2)
        tmp = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
        fig.savefig(tmp.name, dpi=150, bbox_inches="tight", facecolor="white")
        plt.close(fig)
        return tmp.name

    except Exception as e:
        log.error("Erro ao gerar diagrama", error=str(e))
        return ""


def _section_title(pdf, title):
    pdf.set_font("Helvetica", "B", 13)
    pdf.set_fill_color(230, 236, 245)
    pdf.cell(0, 10, f"  {title}", fill=True, new_x="LMARGIN", new_y="NEXT")
    pdf.ln(3)


def _param_row(pdf, label, value):
    pdf.set_font("Helvetica", "", 10)
    pdf.cell(80, 6, f"  {label}", border=0)
    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(0, 6, value, new_x="LMARGIN", new_y="NEXT")


def _result_row(pdf, label, value):
    pdf.set_font("Helvetica", "", 11)
    pdf.cell(80, 8, f"  {label}", border=0)
    pdf.set_font("Helvetica", "B", 12)
    pdf.set_text_color(0, 71, 133)
    pdf.cell(0, 8, value, new_x="LMARGIN", new_y="NEXT")
    pdf.set_text_color(0, 0, 0)


# =============================================================================
# Memorial Hídrico Agro
# =============================================================================

CLASSIFICACAO_LABELS = {
    "agua_boa": ("Agua boa", (39, 174, 96)),
    "agua_intermediaria": ("Agua intermediaria", (243, 156, 18)),
    "agua_carregada": ("Agua carregada", (211, 84, 0)),
    "atendimento_presencial": ("Atendimento presencial", (192, 57, 43)),
}

PARAM_LABELS_MEMORIAL = {
    "ph": ("pH", ""),
    "turbidez": ("Turbidez", "NTU"),
    "cor": ("Cor aparente", "uH"),
    "ferro": ("Ferro (Fe)", "mg/L"),
    "manganes": ("Manganes (Mn)", "mg/L"),
    "alcalinidade": ("Alcalinidade", "mg/L"),
    "dureza": ("Dureza total", "mg/L"),
    "dqo": ("DQO", "mg/L"),
    "dbo": ("DBO", "mg/L"),
    "amonia": ("Amonia", "mg/L"),
    "nitritos": ("Nitritos", "mg/L"),
    "nitratos": ("Nitratos", "mg/L"),
    "sulfeto": ("Sulfeto (H2S)", "mg/L"),
    "cloro_residual": ("Cloro residual", "mg/L"),
    "solidos_totais": ("Solidos totais", "mg/L"),
    "solidos_suspensos": ("Solidos suspensos", "mg/L"),
    "solidos_dissolvidos": ("Solidos dissolvidos", "mg/L"),
    "bacterias_totais": ("Bacterias totais", "NMP/100mL"),
    "coliformes_fecais": ("Coliformes fecais", "NMP/100mL"),
    "e_coli": ("E. coli", "NMP/100mL"),
}


def generate_memorial_pdf(memorial: dict, basics: dict) -> str:
    """
    Gera PDF do Memorial Hidrico Agro.

    Args:
        memorial: dict retornado por generate_memorial_hidrico (claude.py)
        basics: {
            "cliente": str,
            "local": str,
            "fonte_origem": str,
            "volume_diario_litros": float,
            "volume_projeto_litros": float,
            "vazao_min_lh": float,
            "fator_seguranca": float,
            "reservacao_planejada_litros": float | None,
            "parametros_laudo": dict,
        }

    Returns:
        Caminho do arquivo PDF gerado.
    """
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=20)
    pdf.add_page()

    # Header
    pdf.set_fill_color(0, 71, 133)
    pdf.rect(0, 0, 210, 40, "F")
    pdf.set_text_color(255, 255, 255)
    pdf.set_font("Helvetica", "B", 22)
    pdf.set_y(8)
    pdf.cell(
        0, 12, "Memorial Hidrico Agro",
        align="C", new_x="LMARGIN", new_y="NEXT",
    )
    pdf.set_font("Helvetica", "", 11)
    pdf.cell(
        0, 8, "Pre-dimensionamento de ETA para nova granja",
        align="C", new_x="LMARGIN", new_y="NEXT",
    )

    # Resumo do projeto
    pdf.set_text_color(0, 0, 0)
    pdf.set_y(48)
    pdf.set_font("Helvetica", "", 10)

    cliente = basics.get("cliente", "")
    local = basics.get("local", "")
    fonte = basics.get("fonte_origem", "")
    data = datetime.now().strftime("%d/%m/%Y")
    vol_dia = basics.get("volume_diario_litros") or 0
    vol_proj = basics.get("volume_projeto_litros") or 0
    vazao = basics.get("vazao_min_lh") or 0
    fator = basics.get("fator_seguranca") or 0
    reservacao = basics.get("reservacao_planejada_litros")

    if cliente:
        pdf.cell(0, 6, f"Cliente: {cliente}", new_x="LMARGIN", new_y="NEXT")
    if local:
        pdf.cell(0, 6, f"Local: {local}", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 6, f"Data: {data}", new_x="LMARGIN", new_y="NEXT")
    if fonte:
        pdf.cell(
            0, 6, f"Fonte de origem: {fonte}",
            new_x="LMARGIN", new_y="NEXT",
        )
    pdf.cell(
        0, 6, f"Volume diario estimado: {vol_dia:,.0f} L/dia",
        new_x="LMARGIN", new_y="NEXT",
    )
    pdf.cell(
        0, 6,
        f"Volume de projeto: {vol_proj:,.0f} L/dia "
        f"(fator de seguranca {fator:.2f})",
        new_x="LMARGIN", new_y="NEXT",
    )
    pdf.cell(
        0, 6, f"Vazao minima da ETA: {vazao:,.1f} L/h",
        new_x="LMARGIN", new_y="NEXT",
    )
    if reservacao:
        pdf.cell(
            0, 6, f"Reservacao planejada: {reservacao:,.0f} L",
            new_x="LMARGIN", new_y="NEXT",
        )

    pdf.ln(4)

    # Classificação da água
    classif = memorial.get("classificacao_agua", "")
    label, color = CLASSIFICACAO_LABELS.get(
        classif, (classif or "Sem classificacao", (100, 100, 100))
    )
    pdf.set_fill_color(*color)
    pdf.set_text_color(255, 255, 255)
    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(
        0, 9, f"  Classificacao da agua: {label}",
        fill=True, new_x="LMARGIN", new_y="NEXT",
    )
    pdf.set_text_color(0, 0, 0)
    justif = memorial.get("justificativa_classificacao", "")
    if justif:
        pdf.set_font("Helvetica", "I", 10)
        pdf.multi_cell(0, 5, justif)
    pdf.ln(4)

    # Análise laboratorial
    parametros = basics.get("parametros_laudo") or {}
    if any(parametros.get(k) is not None for k in PARAM_LABELS_MEMORIAL):
        _section_title(pdf, "Parametros da analise de agua")
        for key, (label_p, unit) in PARAM_LABELS_MEMORIAL.items():
            val = parametros.get(key)
            if val is not None and val != 0:
                val_str = f"{val}" if not unit else f"{val} {unit}"
                _param_row(pdf, label_p, val_str)
        pdf.ln(4)

    # Atendimento presencial (quando aplicável)
    if memorial.get("atendimento_presencial_recomendado"):
        msg = memorial.get("mensagem_atendimento_presencial") or (
            "A analise apresenta parametros que exigem avaliacao tecnica "
            "presencial antes da definicao da ETA."
        )
        _section_title(pdf, "Atendimento presencial recomendado")
        pdf.set_font("Helvetica", "", 10)
        pdf.multi_cell(0, 5, msg)
        pdf.ln(4)

    # Rotas viáveis
    rotas = memorial.get("rotas_viaveis") or []
    if rotas:
        _section_title(pdf, "Rotas de tratamento sugeridas para cotacao")
        for rota in rotas:
            codigo = rota.get("codigo", "")
            nome = rota.get("nome", "")
            comp = rota.get("composicao", "")
            por_que = rota.get("indicada_porque", "")
            atencao = rota.get("atencao")

            pdf.set_font("Helvetica", "B", 11)
            pdf.set_text_color(0, 71, 133)
            pdf.cell(
                0, 7, f"  Rota {codigo} - {nome}",
                new_x="LMARGIN", new_y="NEXT",
            )
            pdf.set_text_color(0, 0, 0)
            pdf.set_font("Helvetica", "", 10)
            if comp:
                pdf.multi_cell(0, 5, f"  Composicao: {comp}")
            if por_que:
                pdf.multi_cell(0, 5, f"  Indicada porque: {por_que}")
            if atencao:
                pdf.set_text_color(192, 57, 43)
                pdf.multi_cell(0, 5, f"  Atencao: {atencao}")
                pdf.set_text_color(0, 0, 0)
            pdf.ln(2)
        pdf.ln(2)

    # Rotas bloqueadas
    bloqueadas = memorial.get("rotas_bloqueadas") or []
    if bloqueadas:
        _section_title(pdf, "Rotas bloqueadas")
        pdf.set_font("Helvetica", "", 10)
        for rota in bloqueadas:
            codigo = rota.get("codigo", "")
            motivo = rota.get("motivo", "")
            pdf.cell(6, 6, "-")
            pdf.multi_cell(0, 6, f" Rota {codigo}: {motivo}")
        pdf.ln(2)

    # Alertas técnicos
    alertas = memorial.get("alertas_tecnicos") or []
    if alertas:
        _section_title(pdf, "Alertas tecnicos")
        pdf.set_font("Helvetica", "", 10)
        for alerta in alertas:
            par = alerta.get("parametro", "")
            val = alerta.get("valor_observado", "")
            impl = alerta.get("implicacao", "")
            pdf.set_font("Helvetica", "B", 10)
            pdf.cell(0, 5, f"  {par}: {val}", new_x="LMARGIN", new_y="NEXT")
            pdf.set_font("Helvetica", "", 10)
            pdf.multi_cell(0, 5, f"  {impl}")
            pdf.ln(1)
        pdf.ln(2)

    # Escopo mínimo de cotação
    escopo = memorial.get("escopo_minimo_cotacao") or {}
    if escopo:
        if pdf.get_y() > 220:
            pdf.add_page()
        _section_title(pdf, "Escopo minimo para cotacao")
        _result_row(
            pdf, "Vazao minima da ETA",
            f"{escopo.get('vazao_minima_lh', vazao):,.1f} L/h",
        )
        _result_row(
            pdf, "Volume diario de projeto",
            f"{escopo.get('volume_diario_projeto_litros', vol_proj):,.0f} L/dia",
        )
        reserv_min = escopo.get("reservacao_minima_litros")
        if reserv_min:
            _result_row(
                pdf, "Reservacao minima recomendada", f"{reserv_min:,.0f} L"
            )
        etapas = escopo.get("etapas_a_cotar") or []
        if etapas:
            pdf.set_font("Helvetica", "B", 10)
            pdf.cell(
                0, 6, "  Etapas a cotar:",
                new_x="LMARGIN", new_y="NEXT",
            )
            pdf.set_font("Helvetica", "", 10)
            for etapa in etapas:
                pdf.cell(6, 5, "  -")
                pdf.cell(0, 5, f" {etapa}", new_x="LMARGIN", new_y="NEXT")
            pdf.ln(1)

        for key, label_e in [
            ("pontos_de_coleta_e_monitoramento", "Pontos de coleta e monitoramento"),
            ("plano_manutencao_e_insumos", "Plano de manutencao e insumos"),
            ("manual_e_responsabilidade_tecnica", "Manual e responsabilidade tecnica"),
        ]:
            txt = escopo.get(key)
            if txt:
                pdf.set_font("Helvetica", "B", 10)
                pdf.cell(0, 6, f"  {label_e}:", new_x="LMARGIN", new_y="NEXT")
                pdf.set_font("Helvetica", "", 10)
                pdf.multi_cell(0, 5, f"  {txt}")
                pdf.ln(1)

    # Resumo para o produtor
    resumo = memorial.get("resumo_para_produtor")
    if resumo:
        if pdf.get_y() > 230:
            pdf.add_page()
        pdf.ln(2)
        _section_title(pdf, "Resumo para o produtor")
        pdf.set_font("Helvetica", "", 10)
        pdf.multi_cell(0, 5, resumo)
        pdf.ln(3)

    # Footer
    pdf.set_y(-30)
    pdf.set_font("Helvetica", "I", 8)
    pdf.set_text_color(128, 128, 128)
    obs = memorial.get("observacao_final") or (
        "A definicao final depende de parceiro tecnico responsavel pela ETA."
    )
    pdf.multi_cell(0, 5, obs, align="C")
    pdf.cell(
        0, 5,
        "Memorial gerado pelo sistema Z1 Monitoramento. "
        "Nao substitui responsabilidade tecnica.",
        align="C",
    )

    # Salvar
    fname = f"memorial_hidrico_{uuid.uuid4().hex[:8]}.pdf"
    output_dir = os.environ.get("PATH_REPORTS", "/tmp/")
    fpath = os.path.join(output_dir, fname)
    pdf.output(fpath)

    log.info("Memorial Hidrico PDF gerado", path=fpath)
    return fpath
