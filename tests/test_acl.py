"""Contract test de ACL das tools do agente Prisma.

Trava as regras de permissão pra não regredirem (3 vazamentos foram
encontrados em auditoria em 2026-07-04 — este teste teria pego os três):

1. FARM-only: as tools de produtor (lote/mortalidade/ração/ficha/PPM/fonte)
   negam ADMIN, cliente primário e qualquer outro perfil — paridade com
   _FARM_ONLY_INTENTS do backend_whatsapp.
2. ADMIN-only: consultas por cliente primário negam não-admin.
3. Escopo por granja: toda tool registrada com parâmetro `granja` passa por
   um helper de ACL (ou está na lista de exceções JUSTIFICADAS).
4. saude_empresa: representante primário só consulta a própria empresa.

Se este teste quebrar, ou você adicionou uma tool sem guard, ou mudou uma
regra de permissão sem atualizar o contrato aqui — nos dois casos, pare e
confira antes de "consertar o teste".

Rodar: pytest tests/test_acl.py -v
(sem banco: os checks funcionais usam contexto fake e os guards retornam
antes de qualquer query)
"""

import inspect
import os

os.environ.setdefault("SQLALCHEMY_DATABASE_URI", "postgresql://x:x@localhost:1/x")
os.environ.setdefault("ANTHROPIC_API_KEY", "test-dummy")

from z1monitoring_agent.agent import tools_z1 as tz  # noqa: E402
from z1monitoring_agent.agent.tools_z1 import TOOLS_Z1, set_user_context  # noqa: E402

# ============================================================================
# O CONTRATO
# ============================================================================

# Paridade com _FARM_ONLY_INTENTS do backend_whatsapp (bot_conversation.py).
FARM_ONLY = {
    "abrir_lote",
    "fechar_lote",
    "informar_mortalidade",
    "registrar_racao",
    "relatorio_lote",
    "registrar_ppm",
    "mudar_fonte_agua",
}

# Consultas por cliente primário: só ADMIN (mesma regra do guiado).
ADMIN_ONLY = {
    "listar_granjas_cliente_primario",
    "consultar_falta_gas_cliente_primario",
}

# Helpers aceitos como mecanismo de escopo por granja.
ACL_HELPERS = (
    "_resolve_farm_acl",
    "_enforce_farm_access",
    "_lote_resolver_farm",
    "_get_allowed_farm_names",
)

# Tools com parâmetro `granja` SEM helper — cada uma precisa de justificativa.
GRANJA_EXEMPT = {
    # dispatcher puro: delega pra funções com ACL própria (testadas abaixo)
    "consultar_status",
}

# Delegadas do consultar_status — precisam de ACL própria (helper ou inline).
STATUS_DELEGATES = (
    "consultar_alarmes",
    "consultar_equipamentos",
    "consultar_falta_insumo",
    "consultar_falta_gas",
    "consultar_sensor_fora_faixa",
)


class _FakeUser:
    def __init__(self, perm, associated="11222333000144"):
        self.permissions = {"name": perm}
        self.associated = associated
        self.name = "Teste"


def _set_ctx(perm):
    set_user_context(_FakeUser(perm), None)


def _tool(name):
    return next(t for t in TOOLS_Z1 if t.name == name)


# Kwargs mínimos válidos por tool FARM-only (pra passar das validações de
# input que vêm ANTES do guard, sem nunca chegar ao banco).
_FARM_ONLY_KWARGS = {
    "abrir_lote": {},
    "fechar_lote": {},
    "informar_mortalidade": {"natural": 1},
    "registrar_racao": {"peso_kg": 100, "tipo_racao": "RAI", "nota_fiscal": "123"},
    "relatorio_lote": {},
    "registrar_ppm": {"ppm": 1.0},
    "mudar_fonte_agua": {"fonte": "rio"},
}


# ============================================================================
# 1. FARM-only — funcional: ADMIN, primário e SECONDARY são NEGADOS
# ============================================================================


def test_farm_only_nega_admin_primario_e_secundario():
    for name in FARM_ONLY:
        fn = _tool(name).function
        for perm in ("ADMIN", "ETA_REPRESENTANTES", "SECONDARY"):
            _set_ctx(perm)
            result = fn(**_FARM_ONLY_KWARGS[name])
            assert isinstance(result, dict) and "erro" in result, (
                f"{name} deveria NEGAR perfil {perm}, retornou: {result}"
            )
            assert "produtor" in result["erro"], (name, perm, result)


def test_farm_only_guard_presente_no_fonte():
    for name in FARM_ONLY:
        src = inspect.getsource(_tool(name).function)
        assert "_lote_farm_denied" in src, f"{name} perdeu o guard _lote_farm_denied"


def test_farm_only_lista_fechada():
    """Bidirecional: toda tool com o guard está no contrato (e vice-versa)."""
    com_guard = {
        t.name
        for t in TOOLS_Z1
        if "_lote_farm_denied" in inspect.getsource(t.function)
    }
    assert com_guard == FARM_ONLY, (
        f"contrato desatualizado — com guard: {sorted(com_guard)}, "
        f"contrato: {sorted(FARM_ONLY)}"
    )


def test_lote_farm_denied_so_farm_passa():
    class Ctx:
        def __init__(self, perm):
            self.permission_name = perm
            self.is_admin = perm == "ADMIN"

    assert tz._lote_farm_denied(Ctx("FARM")) is None
    for perm in ("ADMIN", "ETA_REPRESENTANTES", "SECONDARY", "ETA_READONLY"):
        assert tz._lote_farm_denied(Ctx(perm)) is not None, perm
    assert tz._lote_farm_denied(None) is not None


# ============================================================================
# 2. ADMIN-only — funcional: não-admin é NEGADO antes de tocar o banco
# ============================================================================


def test_admin_only_nega_nao_admin():
    for name in ADMIN_ONLY:
        fn = _tool(name).function
        for perm in ("FARM", "ETA_REPRESENTANTES", "SECONDARY"):
            _set_ctx(perm)
            result = fn(nome_cliente="Ultragas")
            assert isinstance(result, dict) and "erro" in result, (
                f"{name} deveria NEGAR perfil {perm}, retornou: {result}"
            )
            assert "administrador" in result["erro"].lower(), (name, perm, result)


# ============================================================================
# 3. Escopo por granja — toda tool com `granja` usa helper de ACL
# ============================================================================


def test_toda_tool_com_granja_tem_acl():
    sem_acl = []
    for t in TOOLS_Z1:
        props = (t.parameters or {}).get("properties", {})
        if "granja" not in props or t.name in GRANJA_EXEMPT:
            continue
        src = inspect.getsource(t.function)
        if not any(h in src for h in ACL_HELPERS):
            sem_acl.append(t.name)
    assert not sem_acl, (
        f"tools com parâmetro granja SEM helper de ACL: {sem_acl} — "
        "adicione o helper ou justifique em GRANJA_EXEMPT"
    )


def test_delegadas_do_consultar_status_tem_acl():
    for name in STATUS_DELEGATES:
        src = inspect.getsource(getattr(tz, name))
        tem = any(h in src for h in ACL_HELPERS) or (
            "is_admin" in src and "associateds_allowed" in src
        )
        assert tem, f"delegada {name} do consultar_status sem ACL"


# ============================================================================
# 4. Regressões específicas da auditoria de 2026-07-04
# ============================================================================


def test_saude_empresa_escopa_primario_na_propria_empresa():
    src = inspect.getsource(tz.saude_empresa)
    assert "ctx.associated" in src and "_PRIMARY_PERM_NAMES" in src, (
        "saude_empresa perdeu o escopo por empresa do representante primário"
    )


def test_ranking_offline_usa_associateds_allowed_pro_primario():
    src = inspect.getsource(tz.ranking_offline)
    assert "get_all_that_associated_allowed_permitted" in src, (
        "ranking_offline voltou a filtrar só por owner (vazio pro primário)"
    )


def test_consultar_periodos_offline_tem_enforce():
    src = inspect.getsource(tz.consultar_periodos_offline)
    assert "_enforce_farm_access" in src, (
        "consultar_periodos_offline perdeu o enforce de granja"
    )
