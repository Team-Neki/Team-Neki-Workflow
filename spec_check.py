"""docs/spec 의 구현 anchor 가 코드를 가리키는지 확인한다.

정책 문서가 정본이 되려면 문서가 코드를 따라와야 한다. 항목마다 붙은
`` `경로:줄` `심볼` `` anchor 를 전부 열어 그 줄에 그 심볼이 있는지 본다. 줄이
밀리면 여기서 걸리고, 심볼이 사라졌다면 정책이 바뀐 것이므로 문서를 같이 고친다.

    make spec-check

exit 1 이면 어긋난 anchor 가 있는 것이다. 문서 하나가 통째로 없거나 anchor 가
하나도 없어도 실패한다. 조용히 통과하면 검사가 빠진 것과 구분되지 않기 때문이다.
"""

import re
import sys
from pathlib import Path

SPEC_DIR = Path("docs/spec")

# `경로:줄` 바로 뒤에 `심볼`. 사이는 공백 하나. 경로에 콜론이 없다고 가정한다.
ANCHOR = re.compile(r"`([^`:\s]+):(\d+)` `([^`]+)`")


def check(spec: Path) -> list[str]:
    errors: list[str] = []
    anchors = ANCHOR.findall(spec.read_text(encoding="utf-8"))
    if not anchors:
        return [f"{spec}: anchor 가 하나도 없습니다"]

    for path, line, symbol in anchors:
        target = Path(path)
        if not target.is_file():
            errors.append(f"{spec}: {path}:{line} 파일이 없습니다 ({symbol})")
            continue
        lines = target.read_text(encoding="utf-8").splitlines()
        number = int(line)
        if number < 1 or number > len(lines):
            errors.append(f"{spec}: {path}:{line} 줄이 없습니다 ({symbol})")
            continue
        if symbol not in lines[number - 1]:
            found = next((i + 1 for i, text in enumerate(lines) if symbol in text), None)
            hint = f", 지금은 {found}줄" if found else ", 파일 안에 없음"
            errors.append(f"{spec}: {path}:{line} 에 `{symbol}` 이 없습니다{hint}")
    return errors


def main() -> int:
    specs = sorted(SPEC_DIR.glob("*.md"))
    if not specs:
        print(f"{SPEC_DIR} 에 문서가 없습니다", file=sys.stderr)
        return 1

    errors = [error for spec in specs for error in check(spec)]
    total = sum(len(ANCHOR.findall(spec.read_text(encoding="utf-8"))) for spec in specs)
    for error in errors:
        print(error, file=sys.stderr)
    print(f"spec anchor {total}개 중 {total - len(errors)}개 일치")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
