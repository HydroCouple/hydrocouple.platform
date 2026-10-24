"""Find and rewrite the file references a SWMM ``.inp`` declares.

The rules follow the GUI's reference scanner (``openswmm.gui``
``src/simulation/simulationrunner.cpp``, ``knownRunReferences``):

- ``[FILES]``: ``USE <type> <file>`` is an input; ``SAVE <type> <file>`` an output.
- ``[TIMESERIES]`` and ``[RAINGAGES]``: the token after ``FILE``. A time series
  ``path:column`` decorator is not part of the file name.
- ``[TEMPERATURE]``, ``[2D_MESH_FILE]`` and ``*INITIAL_QUALITY*``: ``FILE <path>``.
  An unquoted 2D mesh path is the raw tail of the line, as the engine reads it.
- ``[PLUGINS]``: the first token, a native library.
- ``[PROCESS_COMPONENTS]``: every ``config=<path>`` token.

Like the GUI scanner, this covers declared built-in references, not arbitrary
plugin arguments or nested datasource graphs; components that need more must
declare their own dependencies.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path


class RefKind(str, Enum):
    INPUT = "input"
    OUTPUT = "output"
    PLUGIN = "plugin"


@dataclass(frozen=True)
class Reference:
    kind: RefKind
    section: str
    line: int  # zero-based line index
    start: int  # span of the path text in the line, quotes excluded
    end: int
    text: str  # the path as written


def _strip_comment(line: str) -> str:
    in_quote = False
    for i, ch in enumerate(line):
        if ch == '"':
            in_quote = not in_quote
        elif ch == ";" and not in_quote:
            return line[:i]
    return line


def tokens(line: str) -> list[tuple[str, int, int]]:
    """Whitespace tokens with their spans; double quotes group and are excluded."""
    out: list[tuple[str, int, int]] = []
    i, n = 0, len(line)
    while i < n:
        if line[i].isspace():
            i += 1
            continue
        if line[i] == '"':
            j = line.find('"', i + 1)
            j = n if j < 0 else j
            out.append((line[i + 1:j], i + 1, j))
            i = j + 1
            continue
        j = i
        while j < n and not line[j].isspace():
            j += 1
        out.append((line[i:j], i, j))
        i = j
    return out


def scan(text: str) -> list[Reference]:
    refs: list[Reference] = []
    section = ""
    for idx, raw in enumerate(text.splitlines()):
        line = _strip_comment(raw).rstrip()
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("[") and stripped.endswith("]"):
            section = stripped[1:-1].strip().upper()
            continue
        toks = tokens(line)
        if not toks:
            continue
        head = toks[0][0].upper()

        if section == "FILES" and len(toks) >= 3 and head in ("USE", "SAVE"):
            text_, s, e = toks[2]
            if text_:
                kind = RefKind.INPUT if head == "USE" else RefKind.OUTPUT
                refs.append(Reference(kind, section, idx, s, e, text_))
        elif section in ("TIMESERIES", "RAINGAGES"):
            for i in range(1, len(toks) - 1):
                if toks[i][0].upper() == "FILE":
                    text_, s, e = toks[i + 1]
                    refs.append(Reference(RefKind.INPUT, section, idx, s, e, text_))
                    break
        elif ((section in ("TEMPERATURE", "2D_MESH_FILE") or "INITIAL_QUALITY" in section)
              and len(toks) >= 2 and head == "FILE"):
            text_, s, e = toks[1]
            if section == "2D_MESH_FILE" and line[toks[1][1] - 1] != '"':
                # Unquoted mesh paths are the raw tail of the line.
                s, e = toks[1][1], len(line)
                text_ = line[s:e]
            refs.append(Reference(RefKind.INPUT, section, idx, s, e, text_))
        elif section == "PLUGINS":
            text_, s, e = toks[0]
            refs.append(Reference(RefKind.PLUGIN, section, idx, s, e, text_))
        elif section == "PROCESS_COMPONENTS":
            for text_, s, _e in tokens(line):
                if text_.lower().startswith("config="):
                    value, vs = text_[7:], s + 7
                    if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
                        value, vs = value[1:-1], vs + 1
                    refs.append(Reference(RefKind.INPUT, section, idx, vs, vs + len(value),
                                          value))
    return refs


def resolve(ref: Reference, model_dir: Path) -> tuple[Path, str]:
    """The file a reference names and any decorator to keep after it."""
    candidate = Path(ref.text)
    path = candidate if candidate.is_absolute() else model_dir / candidate
    if ref.section == "TIMESERIES" and not path.exists():
        colon = ref.text.rfind(":")
        if colon > 1:
            base = Path(ref.text[:colon])
            base_path = base if base.is_absolute() else model_dir / base
            if base_path.exists():
                return base_path, ref.text[colon:]
    return path, ""


def rewrite(text: str, replacements: list[tuple[Reference, str]]) -> str:
    """Replace each reference's path text, quoting new values that contain spaces."""
    lines = text.splitlines(keepends=True)
    by_line: dict[int, list[tuple[Reference, str]]] = {}
    for ref, value in replacements:
        by_line.setdefault(ref.line, []).append((ref, value))
    for line_no, items in by_line.items():
        line = lines[line_no]
        for ref, value in sorted(items, key=lambda item: item[0].start, reverse=True):
            quoted = ref.start > 0 and line[ref.start - 1] == '"'
            new = value if quoted or " " not in value else f'"{value}"'
            line = line[:ref.start] + new + line[ref.end:]
        lines[line_no] = line
    return "".join(lines)
