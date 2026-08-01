"""수집 단계의 공통 지점 스키마.

브랜드마다 사이트가 주는 필드가 다르지만, 다음 단계(enrich)가 4개 브랜드를 한
번에 받으려면 모양이 같아야 한다. 브랜드가 주지 않는 값은 None으로 둔다.

이 단계는 사이트가 준 것만 담는다. 주소가 도로명인지 지번인지 판정하지 않고
상호명이나 층수도 떼지 않는다. 해석은 enrich가 맡는다. 경계를 가르는 기준은
"이 값이 틀렸을 때 누구 잘못인가"이며, 여기서는 언제나 사이트 잘못이다.

collected_at은 여기 없다. 사이트가 준 값이 아니라 우리가 언제 받았는지이므로
적재 시점에 storage가 붙인다.
"""

from dataclasses import dataclass

from flows.common.platform import Platform


@dataclass(frozen=True)
class CollectedStore:
    """수집한 지점 하나.

    idx는 사이트가 부여한 식별자다. 브랜드 안에서만 유일하므로 platform과
    묶어야 전역 식별자가 된다.
    """

    platform: Platform
    idx: str
    name: str
    address: str | None = None
    phone: str | None = None
    longitude: float | None = None
    latitude: float | None = None
