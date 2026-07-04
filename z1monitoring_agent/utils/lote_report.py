"""Geração do PDF de relatório de lote avícola (entregue pelo WhatsApp).

O produtor pede o relatório de um lote (aberto ou fechado) e recebe o PDF pra
transcrever na ficha física da JBS/Seara. Conteúdo: cabeçalho do lote +
mortalidade agregada por semana de vida (como a JBS contabiliza) + registros
de PPM (cloro) do período + snapshot de água da ETA.

Portado do backend_whatsapp (`utils/lote_report.py`), que por sua vez porta a
agregação (`resumo_mortalidade`/`_semana_de_vida`) do backend_z1
(`services/lotes.py`) — mesma lógica do painel web, pra o PDF bater com a tela.

FPDF (core font Helvetica, encoding latin-1): evitamos emojis e caracteres
fora de latin-1 (—, …) no conteúdo do PDF; usamos hífen e reticências ASCII.
"""

import os
import tempfile
from datetime import date

import structlog
from fpdf import FPDF

log = structlog.get_logger()

TIPOS_MORTE = ("natural", "locomotor", "refugo", "outros")
TIPOS_RACAO = ("RAPI", "RAI", "RAC1", "RAC2", "RAF")
_TIPO_LABEL = {
    "natural": "Natural",
    "locomotor": "Locomotor",
    "refugo": "Refugo",
    "outros": "Outros",
}
_GENERO_LABEL = {"masculino": "Machos", "feminino": "Femeas", "misto": "Misto"}


# ---------------------------------------------------------------------------
# Agregação (portada de backend_z1/services/lotes.py)
# ---------------------------------------------------------------------------
def _semana_de_vida(inicio, dia_iso):
    """Semana 1-based a partir do início do lote (blocos de 7 dias)."""
    try:
        d0 = inicio if isinstance(inicio, date) else date.fromisoformat(str(inicio)[:10])
        d1 = date.fromisoformat(str(dia_iso)[:10])
        diff = (d1 - d0).days
        return max(0, diff) // 7 + 1
    except (TypeError, ValueError):
        return 1


def resumo_mortalidade(lote):
    """{por_dia, por_semana, totais} — agrega a mortalidade por dia e semana."""
    registros = (lote.data or {}).get("mortalidade", []) or []

    por_dia = {}
    for r in registros:
        d = r.get("data")
        if not d:
            continue
        dia = por_dia.setdefault(
            d,
            {"data": d, "natural": 0, "locomotor": 0, "refugo": 0, "outros": 0, "total": 0},
        )
        q = int(r.get("quantidade") or 0)
        tipo = r.get("tipo")
        if tipo in dia:
            dia[tipo] += q
        dia["total"] += q
    dias = sorted(por_dia.values(), key=lambda x: x["data"])

    por_semana = {}
    for dia in dias:
        s = _semana_de_vida(lote.inicio, dia["data"])
        bloco = por_semana.setdefault(
            s,
            {
                "semana": s,
                "inicio": dia["data"],
                "fim": dia["data"],
                "dias": [],
                "natural": 0,
                "locomotor": 0,
                "refugo": 0,
                "outros": 0,
                "total": 0,
            },
        )
        bloco["dias"].append(dia)
        for t in TIPOS_MORTE:
            bloco[t] += dia[t]
        bloco["total"] += dia["total"]
        if dia["data"] < bloco["inicio"]:
            bloco["inicio"] = dia["data"]
        if dia["data"] > bloco["fim"]:
            bloco["fim"] = dia["data"]
    semanas = sorted(por_semana.values(), key=lambda x: x["semana"])

    totais = {t: sum(d[t] for d in dias) for t in TIPOS_MORTE}
    totais["total"] = sum(d["total"] for d in dias)
    return {"por_dia": dias, "por_semana": semanas, "totais": totais}


def resumo_racao(lote):
    """Entregas ordenadas por data + total por fase e geral (kg)."""
    registros = (lote.data or {}).get("racao", []) or []
    entregas = []
    por_tipo = {t: 0.0 for t in TIPOS_RACAO}
    total = 0.0
    for r in registros:
        try:
            peso = float(r.get("peso_kg") or 0)
        except (TypeError, ValueError):
            peso = 0.0
        tipo = r.get("tipo")
        if tipo in por_tipo:
            por_tipo[tipo] += peso
        total += peso
        entregas.append(r)
    entregas.sort(key=lambda x: (x.get("data") or ""))
    return {"entregas": entregas, "por_tipo": por_tipo, "total_kg": total}


# ---------------------------------------------------------------------------
# Helpers de formatação (latin-1 safe)
# ---------------------------------------------------------------------------
def _safe(s):
    """Garante latin-1 (core font do FPDF). Char fora da tabela vira '?' em vez
    de derrubar a geração inteira (ex.: emoji no nome da granja)."""
    return str(s if s is not None else "-").encode("latin-1", "replace").decode("latin-1")


def _br(value, casas=2):
    if value is None:
        return "-"
    try:
        return f"{float(value):.{casas}f}".replace(".", ",")
    except (TypeError, ValueError):
        return str(value)


def _data_br(iso):
    try:
        return date.fromisoformat(str(iso)[:10]).strftime("%d/%m/%Y")
    except (TypeError, ValueError):
        return str(iso or "-")


def _ppm_fields(reading):
    """Lê ppm/ph/orp/temp tolerando o formato aninhado (bot) e o plano (web)."""
    r = reading.readings or {}
    eta = r.get("eta") or {}
    ph = eta.get("ph", r.get("ph"))
    orp = eta.get("orp", r.get("orp"))
    temp = eta.get("temperature", r.get("temperature"))
    return r.get("ppm"), ph, orp, temp


# ---------------------------------------------------------------------------
# PDF
# ---------------------------------------------------------------------------
def _header_pdf(pdf, titulo):
    pdf.set_font("Helvetica", "B", 15)
    pdf.cell(0, 9, titulo, ln=1)
    pdf.set_draw_color(180, 180, 180)
    pdf.line(pdf.l_margin, pdf.get_y(), pdf.w - pdf.r_margin, pdf.get_y())
    pdf.ln(3)


def _kv(pdf, label, valor):
    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(45, 6, f"{label}:", ln=0)
    pdf.set_font("Helvetica", "", 10)
    pdf.cell(0, 6, str(valor), ln=1)


def _section(pdf, titulo):
    pdf.ln(6)
    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(0, 7, titulo, ln=1)
    pdf.ln(1.5)


def _idade_dias(lote):
    try:
        fim = lote.fim or date.today()
        return max(0, (fim - lote.inicio).days)
    except (TypeError, ValueError, AttributeError):
        return "-"


def gerar_pdf_lote(lote, farm, galpao, ppms, snapshot=None, racao_habilitada=False):
    """Monta o PDF do lote e retorna o caminho do arquivo (temp).

    ppms: lista de PpmReading do período. snapshot: dict água da ETA
    {orp, ph, temperature} ou None. racao_habilitada: inclui a seção de
    ração recebida (só integradora habilitada — ficha JBS/Seara).
    """
    resumo = resumo_mortalidade(lote)
    data = lote.data or {}
    alojados = data.get("animais_alojados")
    genero = _GENERO_LABEL.get((data.get("genero") or "").lower(), data.get("genero") or "-")

    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()

    _header_pdf(pdf, "Relatorio de Lote")

    # --- Cabeçalho ---
    _kv(pdf, "Granja", _safe(getattr(farm, "name", "-")))
    _kv(pdf, "Galpao", _safe(getattr(galpao, "nome", "-")))
    _kv(pdf, "Lote", f"n {lote.numero}  ({'aberto' if lote.fim is None else 'fechado'})")
    _kv(pdf, "Genero", _safe(genero))
    _kv(pdf, "Animais alojados", alojados if alojados is not None else "-")
    _kv(pdf, "Inicio", _data_br(lote.inicio))
    if lote.fim is not None:
        _kv(pdf, "Encerramento", _data_br(lote.fim))
    _kv(pdf, "Idade do lote", f"{_idade_dias(lote)} dias")

    # --- Mortalidade: dia a dia + fechamento semanal + total (como na web) ---
    _section(pdf, "Mortalidade")
    mcols = [("Data", 56), ("Natural", 24), ("Locomotor", 26), ("Refugo", 22), ("Outros", 22), ("Total", 24)]

    def _mrow(valores, fill=None, bold=False):
        if fill is not None:
            pdf.set_fill_color(*fill)
        pdf.set_font("Helvetica", "B" if bold else "", 9)
        for (nome, w), val in zip(mcols, valores):
            align = "L" if nome == "Data" else "R"
            pdf.cell(w, 6.5, _safe(val), border=1, ln=0, align=align, fill=fill is not None)
        pdf.ln()

    _mrow([c[0] for c in mcols], fill=(235, 235, 235), bold=True)
    if not resumo["por_semana"]:
        pdf.set_font("Helvetica", "I", 9)
        pdf.cell(0, 6, "Sem mortalidade lancada neste lote.", ln=1)
    else:
        for bloco in resumo["por_semana"]:
            for dia in bloco["dias"]:
                _mrow(
                    [
                        _data_br(dia["data"]),
                        dia["natural"],
                        dia["locomotor"],
                        dia["refugo"],
                        dia["outros"],
                        dia["total"],
                    ]
                )
            rotulo = f"Semana {bloco['semana']} ({_data_br(bloco['inicio'])}-{_data_br(bloco['fim'])})"
            _mrow(
                [rotulo, bloco["natural"], bloco["locomotor"], bloco["refugo"], bloco["outros"], bloco["total"]],
                fill=(255, 237, 213),
                bold=True,
            )
        t = resumo["totais"]
        _mrow(
            ["TOTAL", t["natural"], t["locomotor"], t["refugo"], t["outros"], t["total"]],
            fill=(221, 221, 221),
            bold=True,
        )
        if alojados:
            try:
                pct = _br(100 * t["total"] / int(alojados), 2)
                pdf.ln(1.5)
                pdf.set_font("Helvetica", "I", 8)
                pdf.set_text_color(90, 90, 90)
                pdf.cell(0, 5, f"Mortalidade acumulada: {t['total']} de {alojados} alojados ({pct}%)", ln=1)
                pdf.set_text_color(0, 0, 0)
            except (TypeError, ValueError, ZeroDivisionError):
                pass

    # --- Racao recebida (so integradora habilitada) ---
    if racao_habilitada:
        rr = resumo_racao(lote)
        _section(pdf, "Racao recebida")
        rcols = [("Data", 34), ("Nota fiscal", 46), ("Fase", 28), ("Peso (kg)", 40)]
        pdf.set_font("Helvetica", "B", 9)
        for nome, w in rcols:
            pdf.cell(w, 6, nome, border=1, ln=0, align="C", fill=True)
        pdf.ln()
        if not rr["entregas"]:
            pdf.set_font("Helvetica", "I", 9)
            pdf.cell(0, 6, "Nenhuma entrega de racao registrada neste lote.", ln=1)
        else:
            pdf.set_font("Helvetica", "", 9)
            for e in rr["entregas"]:
                valores = [
                    _data_br(e.get("data")),
                    _safe(e.get("nota_fiscal") or "-")[:24],
                    _safe(e.get("tipo") or "-"),
                    _br(e.get("peso_kg"), 0),
                ]
                for (nome, w), val in zip(rcols, valores):
                    align = "R" if nome == "Peso (kg)" else ("C" if nome == "Fase" else "L")
                    pdf.cell(w, 6, val, border=1, ln=0, align=align)
                pdf.ln()
            pdf.set_font("Helvetica", "B", 9)
            pdf.set_fill_color(221, 221, 221)
            pdf.cell(rcols[0][1] + rcols[1][1] + rcols[2][1], 6.5, "TOTAL", border=1, ln=0, align="L", fill=True)
            pdf.cell(rcols[3][1], 6.5, _br(rr["total_kg"], 0), border=1, ln=1, align="R", fill=True)
            partes = [f"{t}: {_br(rr['por_tipo'][t], 0)} kg" for t in TIPOS_RACAO if rr["por_tipo"][t] > 0]
            if partes:
                pdf.ln(1.5)
                pdf.set_font("Helvetica", "I", 8)
                pdf.set_text_color(90, 90, 90)
                pdf.cell(0, 5, "Por fase - " + "  |  ".join(partes), ln=1)
                pdf.set_text_color(0, 0, 0)

    # --- PPM (cloro) no periodo ---
    _section(pdf, "Cloro (PPM) registrado no periodo")
    pcols = [("Data", 34), ("Ponto", 46), ("PPM", 22), ("pH", 22), ("ORP (mV)", 30), ("Temp.", 24)]
    pdf.set_font("Helvetica", "B", 9)
    for nome, w in pcols:
        pdf.cell(w, 6, nome, border=1, ln=0, align="C", fill=True)
    pdf.ln()
    pdf.set_font("Helvetica", "", 9)
    for r in ppms:
        ppm, ph, orp, temp = _ppm_fields(r)
        quando = r.created_at.strftime("%d/%m/%Y") if getattr(r, "created_at", None) else "-"
        valores = [
            quando,
            _safe(r.local or "-")[:24],
            _br(ppm),
            _br(ph, 2),
            _br(orp, 0),
            f"{_br(temp, 1)} C" if temp is not None else "-",
        ]
        for (nome, w), val in zip(pcols, valores):
            pdf.cell(w, 6, val, border=1, ln=0, align="C" if nome != "Ponto" else "L")
        pdf.ln()
    if not ppms:
        pdf.set_font("Helvetica", "I", 9)
        pdf.cell(0, 6, "Nenhum PPM registrado no periodo deste lote.", ln=1)

    # --- Snapshot de água da ETA ---
    if snapshot:
        _section(pdf, "Agua da ETA (ultima leitura)")
        pdf.set_font("Helvetica", "", 10)
        partes = []
        if snapshot.get("ph") is not None:
            partes.append(f"pH {_br(snapshot['ph'])}")
        if snapshot.get("orp") is not None:
            partes.append(f"ORP {_br(snapshot['orp'], 0)} mV")
        if snapshot.get("temperature") is not None:
            partes.append(f"Temp. {_br(snapshot['temperature'], 1)} C")
        pdf.cell(0, 6, "  |  ".join(partes) if partes else "Sem leitura recente.", ln=1)

    # Desliga o auto page-break: set_y(-15) cai bem no limite e jogaria o
    # rodapé pra uma 2ª página em branco.
    pdf.set_auto_page_break(False)
    pdf.set_y(-15)
    pdf.set_font("Helvetica", "I", 7)
    pdf.set_text_color(130, 130, 130)
    pdf.cell(0, 5, "Gerado pelo Z1 Monitoramento via WhatsApp.", align="C")

    fname = f"relatorio_lote_{lote.id}.pdf"
    path = os.path.join(tempfile.gettempdir(), fname)
    pdf.output(path)
    log.info("pdf de lote gerado", lote_id=lote.id, path=path, ppms=len(ppms))
    return path
