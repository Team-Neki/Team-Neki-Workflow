"""지점 이름 앞머리의 브랜드 표기를 통일한다.

사이트가 한 브랜드를 여러 표기로 준다. 포토이즘은 '포토이즘 박스 OO점' 과
'포토이즘박스 OO점' 이 섞여 온다. 같은 브랜드가 검색과 표시에서 갈리지 않도록
앞머리만 바꾼다. 원문은 collect CSV(S3)에 그대로 남는다.

표기 통일만 한다. 브랜드와 지점명을 나누는 검색용 정규화는 여전히 서버
SearchNormalizer 몫이다.
"""

import re

from flows.common.platform import Platform

# 브랜드 -> (앞머리 표기, 통일한 표기). 뒤따르는 공백까지 먹고 한 칸으로 되돌린다.
PREFIX_ALIASES: dict[str, tuple[re.Pattern[str], str]] = {
    Platform.PHOTOISM: (re.compile(r"^포토이즘\s*박스\s*"), "포토이즘 "),
}


def unify_brand(platform: str, name: str) -> str:
    """앞머리 표기를 바꾼다. 등록되지 않은 브랜드와 맞지 않는 이름은 그대로다."""
    alias = PREFIX_ALIASES.get(platform)
    if alias is None:
        return name
    pattern, brand = alias
    return pattern.sub(brand, name, count=1).rstrip()
