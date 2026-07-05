"""Contract test do tool cache — escrita e envio NUNCA são cacheados.

Bug real (2026-07-05): consumo(formato='grafico') veio do cache → nenhuma
imagem enfileirada → usuário recebeu o menu em vez do gráfico. Pior ainda
seria escrita: informar_mortalidade servida do cache = o agente confirma
uma gravação que NÃO aconteceu.

Regras travadas aqui:
1. Toda tool de ESCRITA (@_require_write) tem cacheable=False.
2. Toda tool que ENVIA algo (pending_messages/send_immediate) tem
   cacheable=False.
3. O core respeita cacheable no GET e no PUT (entry antigo persistido no
   chat.context não pode ressuscitar envio/gravação).

Rodar: pytest tests/test_tool_cache.py -v
"""

import inspect
import os

os.environ.setdefault("SQLALCHEMY_DATABASE_URI", "postgresql://x:x@localhost:1/x")
os.environ.setdefault("ANTHROPIC_API_KEY", "test-dummy")

from z1monitoring_agent.agent import core  # noqa: E402
from z1monitoring_agent.agent import tools_z1 as tz  # noqa: E402
from z1monitoring_agent.agent.tools_z1 import TOOLS_Z1  # noqa: E402


def _tools_por_nome():
    return {t.name: t for t in TOOLS_Z1}


def test_toda_tool_de_escrita_nao_cacheia():
    """@_require_write ⇒ cacheable=False (cache = 'gravou' sem gravar)."""
    src_mod = inspect.getsource(tz)
    errados = []
    for t in TOOLS_Z1:
        # decorator aparece na linha imediatamente acima do def da função
        marcador = f"@_require_write\ndef {t.function.__name__}("
        if marcador in src_mod and t.cacheable:
            errados.append(t.name)
    assert not errados, f"tools de ESCRITA cacheáveis (perda de gravação): {errados}"


def test_toda_tool_que_envia_nao_cacheia():
    """pending_messages/send_immediate ⇒ cacheable=False (cache não reenvia)."""
    errados = []
    for t in TOOLS_Z1:
        src = inspect.getsource(t.function)
        envia = "pending_messages" in src or "send_immediate_fn" in src
        if envia and t.cacheable:
            # exceção: tool que sinaliza _no_cache no retorno que envia
            if '"_no_cache": True' in src:
                continue
            errados.append(t.name)
    assert not errados, f"tools que ENVIAM e são cacheáveis (mídia some no hit): {errados}"


def test_consumo_grafico_sinaliza_no_cache():
    src = inspect.getsource(tz.consumo)
    assert '"_no_cache": True' in src, (
        "consumo(formato='grafico') perdeu o _no_cache — gráfico servido do "
        "cache não reenvia as imagens"
    )


def test_core_respeita_cacheable_no_get_e_no_put():
    src = inspect.getsource(core.Agent)
    assert "if can_cache else None" in src, "core perdeu o gate de cacheable no GET"
    assert 'result.get("_no_cache")' in src, "core perdeu o gate de _no_cache no PUT"


def test_leituras_principais_continuam_cacheaveis():
    """Anti-overcorrection: as leituras que motivaram o cache seguem ligadas."""
    por_nome = _tools_por_nome()
    for name in ("buscar_granja", "consumo", "descrever_eta", "panorama_24h", "consultar_status"):
        assert por_nome[name].cacheable, f"{name} deveria continuar cacheável"
