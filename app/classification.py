from __future__ import annotations

import re
from pathlib import Path


TOKEN_RE = re.compile(r"[a-z0-9]{2,}")


def clean_tokens(*values: object) -> list[str]:
    raw = " ".join(str(value or "").lower() for value in values)
    normalized = re.sub(r"[^a-z0-9]+", " ", raw)
    return TOKEN_RE.findall(normalized)


def _token_set(*values: object) -> set[str]:
    return set(clean_tokens(*values))


def _has_any(tokens: set[str], values: set[str]) -> bool:
    return bool(tokens & values)


def _single_match(tokens: set[str], mapping: list[tuple[str, set[str]]]) -> str | None:
    for label, aliases in mapping:
        if _has_any(tokens, aliases):
            return label
    return None


def canonical_category_pair(category: str | None, subcategory: str | None = None) -> tuple[str | None, str | None]:
    main = " ".join(str(category or "").strip().split())[:80] or None
    sub = " ".join(str(subcategory or "").strip().split())[:80] or None
    if not main:
        return None, sub
    direct_map = {
        "Aria Blaze (Solo)": ("My Little Pony", "Aria Blaze"),
        "Sonata Dusk": ("My Little Pony", "Sonata Dusk"),
        "Dazzlings": ("My Little Pony", "Dazzlings"),
        "My Little Pony (Fluttershy)": ("My Little Pony", "Fluttershy"),
        "KPOP Demon Hunters (Huntrix)": ("KPOP Demon Hunters", "Huntrix"),
        "KPOP Demon Hunters (Mira)": ("KPOP Demon Hunters", "Mira"),
        "Resident Evil (Leon)": ("Resident Evil", "Leon"),
    }
    return direct_map.get(main, (main, sub))


def infer_category_pair(
    *,
    filename: str,
    media_kind: str,
    title: str | None = None,
    current_category: str | None = None,
    size: tuple[int, int] | None = None,
) -> tuple[str, str | None]:
    stem = Path(filename).stem.lower()
    current_main, current_sub = canonical_category_pair(current_category)
    tokens = _token_set(stem, title, current_main, current_sub)

    mlp_tokens = {
        "mlp", "pony", "ponies", "equestria", "rainbooms", "mane", "cutie", "cmc",
        "twilight", "pinkie", "fluttershy", "rarity", "applejack", "scootaloo",
        "rainbow", "dash", "derpy", "sunset", "starlight", "trixie", "celestia",
        "luna", "discord", "spike",
    }
    dazzlings_tokens = {"dazzlings", "adagio", "aria", "sonata"}
    fnaf_tokens = {
        "fnaf", "freddy", "bonnie", "chica", "foxy", "roxanne", "roxy", "fazbear",
        "animatronic", "frenni", "bonfie",
    }
    neptunia_tokens = {"neptunia", "neptune", "nepgear", "noire", "blanc", "vert", "uzume", "plutia"}
    xenoblade_tokens = {"xenoblade", "pyra", "mythra", "nia", "mio", "eunie", "taion", "lanz", "noah"}
    sonic_tokens = {"sonic", "tails", "amy", "shadow", "knuckles", "rouge"}
    resident_evil_tokens = {"resident", "evil", "leon", "ada", "jill", "claire", "wesker", "ashley"}
    kpop_demon_hunters_tokens = {"kpop", "demon", "hunters", "huntrix", "mira", "zoey", "rumi"}
    cartoon_tokens = {"boondocks", "cartoon", "anime"}
    meme_tokens = {"meme", "memes", "funny", "reaction"}

    mlp_subcategories = [
        ("Aria Blaze", {"aria"}),
        ("Sonata Dusk", {"sonata"}),
        ("Adagio Dazzle", {"adagio"}),
        ("Fluttershy", {"fluttershy"}),
        ("Twilight Sparkle", {"twilight"}),
        ("Rainbow Dash", {"rainbow", "dash"}),
        ("Pinkie Pie", {"pinkie"}),
        ("Rarity", {"rarity"}),
        ("Applejack", {"applejack"}),
        ("Sunset Shimmer", {"sunset"}),
        ("Starlight Glimmer", {"starlight"}),
        ("Scootaloo", {"scootaloo"}),
        ("Derpy", {"derpy"}),
        ("Trixie", {"trixie"}),
        ("Princess Celestia", {"celestia"}),
        ("Princess Luna", {"luna"}),
        ("Discord", {"discord"}),
        ("Spike", {"spike"}),
    ]
    neptunia_subcategories = [
        ("Neptune", {"neptune"}),
        ("Nepgear", {"nepgear"}),
        ("Noire", {"noire"}),
        ("Blanc", {"blanc"}),
        ("Vert", {"vert"}),
        ("Uzume", {"uzume"}),
        ("Plutia", {"plutia"}),
    ]
    xenoblade_subcategories = [
        ("Pyra", {"pyra"}),
        ("Mythra", {"mythra"}),
        ("Nia", {"nia"}),
        ("Mio", {"mio"}),
        ("Eunie", {"eunie"}),
        ("Taion", {"taion"}),
        ("Lanz", {"lanz"}),
        ("Noah", {"noah"}),
    ]
    sonic_subcategories = [
        ("Sonic", {"sonic"}),
        ("Shadow", {"shadow"}),
        ("Amy", {"amy"}),
        ("Tails", {"tails"}),
        ("Knuckles", {"knuckles"}),
        ("Rouge", {"rouge"}),
    ]
    fnaf_subcategories = [
        ("Freddy", {"freddy"}),
        ("Bonnie", {"bonnie"}),
        ("Chica", {"chica"}),
        ("Foxy", {"foxy"}),
        ("Roxanne Wolf", {"roxanne", "roxy"}),
    ]
    resident_evil_subcategories = [
        ("Leon", {"leon"}),
        ("Ada Wong", {"ada"}),
        ("Jill Valentine", {"jill"}),
        ("Claire Redfield", {"claire"}),
        ("Albert Wesker", {"wesker"}),
        ("Ashley Graham", {"ashley"}),
    ]
    kpop_subcategories = [
        ("Huntrix", {"huntrix"}),
        ("Mira", {"mira"}),
        ("Zoey", {"zoey"}),
        ("Rumi", {"rumi"}),
    ]

    if Path(filename).suffix.lower() == ".gif":
        return "GIFs", None

    if _has_any(tokens, kpop_demon_hunters_tokens) or (current_main and current_main == "KPOP Demon Hunters"):
        return "KPOP Demon Hunters", _single_match(tokens, kpop_subcategories)

    if _has_any(tokens, resident_evil_tokens) or (current_main and current_main == "Resident Evil"):
        return "Resident Evil", _single_match(tokens, resident_evil_subcategories)

    if _has_any(tokens, neptunia_tokens) or (current_main and current_main == "Hyperdimension Neptunia"):
        return "Hyperdimension Neptunia", _single_match(tokens, neptunia_subcategories)

    if _has_any(tokens, xenoblade_tokens) or (current_main and current_main == "Xenoblade"):
        return "Xenoblade", _single_match(tokens, xenoblade_subcategories)

    if _has_any(tokens, fnaf_tokens) or (current_main and current_main == "FNAF"):
        return "FNAF", _single_match(tokens, fnaf_subcategories)

    if (_has_any(tokens, sonic_tokens) and _has_any(tokens, mlp_tokens)) or (current_main and current_main == "Crossovers"):
        return "Crossovers", _single_match(tokens, sonic_subcategories) or _single_match(tokens, mlp_subcategories)

    if _has_any(tokens, mlp_tokens) or _has_any(tokens, dazzlings_tokens) or current_main == "My Little Pony":
        if _has_any(tokens, dazzlings_tokens):
            if "aria" in tokens and not _has_any(tokens, {"sonata", "adagio"}):
                return "My Little Pony", "Aria Blaze"
            if "sonata" in tokens and not _has_any(tokens, {"aria", "adagio"}):
                return "My Little Pony", "Sonata Dusk"
            if "adagio" in tokens and not _has_any(tokens, {"aria", "sonata"}):
                return "My Little Pony", "Adagio Dazzle"
            return "My Little Pony", "Dazzlings"
        mane_six_hits = sum(1 for _, aliases in mlp_subcategories[3:9] if _has_any(tokens, aliases))
        if mane_six_hits >= 2:
            return "My Little Pony", "Mane 6"
        return "My Little Pony", _single_match(tokens, mlp_subcategories)

    if _has_any(tokens, sonic_tokens) or (current_main and current_main == "Sonic"):
        return "Sonic", _single_match(tokens, sonic_subcategories)

    if _has_any(tokens, cartoon_tokens) or (current_main and current_main == "Cartoon"):
        return "Cartoon", None

    if _has_any(tokens, meme_tokens) or (current_main and current_main == "Memes"):
        return "Memes", None

    if size:
        width, height = size
        if width and height:
            ratio = width / max(1, height)
            if 0.9 <= ratio <= 1.1 and max(width, height) <= 2048:
                return "Profile Pictures", None
            if ratio < 0.85:
                return "Phone Backgrounds", None
            if ratio >= 1.2:
                return "Desktop Backgrounds", None

    if media_kind == "video":
        return current_main or "Videos", current_sub

    if current_main:
        return current_main, current_sub

    return "Wallpapers", None
