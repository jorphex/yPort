from typing import List


def split_lines(lines: List[str], max_len: int, allow_mid_line_split: bool = True) -> List[str]:
    chunks: List[str] = []
    current = ""

    for line in lines:
        if line is None:
            continue
        if allow_mid_line_split:
            parts = _split_long_line(line, max_len)
        else:
            parts = [line] if len(line) <= max_len else [line[: max_len - 3] + "..."]
        for part in parts:
            if not current:
                current = part
                continue
            if len(current) + 1 + len(part) > max_len:
                chunks.append(current)
                current = part
            else:
                current = f"{current}\n{part}"

    if current:
        chunks.append(current)

    return chunks


def _split_long_line(line: str, max_len: int) -> List[str]:
    if len(line) <= max_len:
        return [line]

    parts: List[str] = []
    remaining = line
    while len(remaining) > max_len:
        slice_len = max_len - 10
        if slice_len <= 0:
            break
        parts.append(remaining[:slice_len] + "...")
        remaining = remaining[slice_len:]
    if remaining:
        parts.append(remaining)
    return parts
