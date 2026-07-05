"""Farm Resolver — matching léxico de nomes de granja (port do backend_whatsapp).

Espelha as funções puras do `llm/farm_resolver.py` do guiado (normalização
sem prefixo + SequenceMatcher). A etapa de LLM fonético do guiado NÃO é
portada: no agente, quem decide foneticamente é o próprio LLM do loop —
`buscar_granja` devolve os `similares` deste módulo e o agente escolhe
(Wassmuth≈Vasmute, Kolling≈Colin), sem chamada extra.

Funções puras (recebem listas de nomes; escopo/ACL é responsabilidade do
caller em tools_z1).
"""

from difflib import SequenceMatcher

from unidecode import unidecode

FARM_PREFIXES = (
    "granja ",
    "fazenda ",
    "sitio ",
    "chacara ",
    "aviario ",
    "propriedade ",
    "empresa ",
)

# Auto-resolve só com match forte e sem vice-campeão próximo — o resto vai
# pro fluxo de desambiguação (buscar_granja/similares).
AUTO_RESOLVE_MIN_RATIO = 0.75
AUTO_RESOLVE_MIN_GAP = 0.1


def normalize_text(text: str) -> str:
    """Normaliza para comparação: lowercase, sem acentos, strip."""
    if not text:
        return ""
    return unidecode(text.lower().strip())


def normalize_no_prefix(text: str) -> str:
    """Normaliza + remove prefixos como 'Granja '/'Fazenda '."""
    t = normalize_text(text)
    for p in FARM_PREFIXES:
        if t.startswith(p):
            t = t[len(p):].strip()
    return t


def calculate_similarity(a: str, b: str) -> float:
    """SequenceMatcher ratio sobre versão normalizada sem prefixo.

    O SIMILARITY (pg_trgm) do banco compara o pedido com o nome COM prefixo
    ('bak' vs 'Granja Back' fica abaixo do threshold) — aqui os dois lados
    são normalizados sem prefixo ('bak' vs 'back' = 0.86).

    Além do nome completo, compara com cada TOKEN (>=3 chars) do candidato:
    o produtor fala o sobrenome/núcleo ('losso' → 'MARCOS LOSS' via token
    'loss' = 0.89), e o nome completo dilui o match (0.5)."""
    na = normalize_no_prefix(a)
    nb = normalize_no_prefix(b)
    best = SequenceMatcher(None, na, nb).ratio()
    for tok in nb.split():
        if len(tok) >= 3:
            best = max(best, SequenceMatcher(None, na, tok).ratio())
    return best


# Iniciais foneticamente equivalentes na grafia alemã/polonesa local
# (Wassmuth≈Vasmute, Kolling≈Colin). Fora dessas classes, consoante inicial
# DIFERENTE = nome diferente (Basso não é Losso), por mais alto que seja o
# ratio do resto.
_INITIAL_EQUIV = {"w": "v", "k": "c", "q": "c", "z": "s"}


def _initial_class(s: str) -> str:
    c = (s or "")[:1]
    return _INITIAL_EQUIV.get(c, c)


def _initial_compatible(raw_norm: str, candidate: str) -> bool:
    """True se a inicial do pedido bate com a do nome ou de algum token dele."""
    alvo = _initial_class(raw_norm)
    nb = normalize_no_prefix(candidate)
    if _initial_class(nb) == alvo:
        return True
    return any(_initial_class(tok) == alvo for tok in nb.split())


def top_similares(raw: str, names: list, top_n: int = 15) -> list:
    """Top-N nomes por similaridade com `raw`. Retorna [(score, name)] desc."""
    if not raw:
        return []
    scored = sorted(((calculate_similarity(raw, n), n) for n in names if n), reverse=True)
    return scored[:top_n]


def best_match(raw: str, names: list) -> str:
    """Nome único quando o match é forte, inequívoco E com inicial
    compatível; senão None (vai pro fluxo de desambiguação/LLM)."""
    scored = top_similares(raw, names, top_n=2)
    if not scored:
        return None
    best_score, best_name = scored[0]
    second_score = scored[1][0] if len(scored) > 1 else 0.0
    if (
        best_score >= AUTO_RESOLVE_MIN_RATIO
        and (best_score - second_score) >= AUTO_RESOLVE_MIN_GAP
        and _initial_compatible(normalize_no_prefix(raw), best_name)
    ):
        return best_name
    return None
