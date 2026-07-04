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
    são normalizados sem prefixo ('bak' vs 'back' = 0.86)."""
    return SequenceMatcher(None, normalize_no_prefix(a), normalize_no_prefix(b)).ratio()


def top_similares(raw: str, names: list, top_n: int = 15) -> list:
    """Top-N nomes por similaridade com `raw`. Retorna [(score, name)] desc."""
    if not raw:
        return []
    scored = sorted(((calculate_similarity(raw, n), n) for n in names if n), reverse=True)
    return scored[:top_n]


def best_match(raw: str, names: list) -> str:
    """Nome único quando o match é forte e inequívoco; senão None."""
    scored = top_similares(raw, names, top_n=2)
    if not scored:
        return None
    best_score, best_name = scored[0]
    second_score = scored[1][0] if len(scored) > 1 else 0.0
    if best_score >= AUTO_RESOLVE_MIN_RATIO and (best_score - second_score) >= AUTO_RESOLVE_MIN_GAP:
        return best_name
    return None
