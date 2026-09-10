"""수집 단계의 공통 지점 스키마.

브랜드마다 사이트가 주는 필드가 다르지만, 다음 단계(enrich)가 4개 브랜드를 한
번에 받으려면 모양이 같아야 한다. 브랜드가 주지 않는 필드는 None으로 둔다.

이 단계는 사이트가 준 것만 담는다. 주소가 도로명인지 지번인지 판정하지 않고
상호명이나 층수도 떼지 않는다. 해석은 enrich가 맡는다. 경계를 가르는 기준은
"이 값이 틀렸을 때 누구 잘못인가"이며, 여기서는 언제나 사이트 잘못이다.

좌표만 예외다. 사이트가 좌표를 주지 않으면 그 지점은 색인에서 통째로 빠지므로
`flows/common/geocode.py`가 주소로 채운다. 그래서 좌표는 유일하게 우리가 만든
값이 섞일 수 있는 필드이고, coordinate_source가 그것을 드러낸다.

collected_at은 여기 없다. 사이트가 준 값이 아니라 우리가 언제 받았는지이므로
적재 시점에 storage가 붙인다.
"""

from dataclasses import dataclass
from typing import Literal

from flows.common.platform import Platform

# 좌표를 우리가 어떻게 채웠는지. 값이 아니라 신뢰도를 말해준다.
#
#   None           손대지 않았다. 수집원이 준 값이거나, 끝내 못 채웠다
#   kakao_address  주소를 Kakao 주소검색으로 변환했다
#   kakao_keyword  주소로는 안 잡혀 상호명으로 찾았다. 엉뚱한 가게일 수 있다
#
# 좌표가 None인지 먼저 보면 손대지 않은 둘을 가를 수 있다. 수집원이 준 값에
# "site"를 따로 찍지 않는 이유는, 좌표를 다 주는 브랜드는 보정을 아예 부르지
# 않아 브랜드마다 의미가 갈리기 때문이다.
CoordinateSource = Literal["kakao_address", "kakao_keyword"]


@dataclass(frozen=True)
class CollectedStore:
    """수집한 지점 하나.

    idx는 사이트가 부여한 식별자다. 브랜드 안에서만 유일하므로 platform과
    묶어야 전역 식별자가 된다.

    좌표와 그 출처를 한 줄에 같이 담는다. 필드를 나누면 다음 단계가 둘 중 어느
    것을 볼지 매번 정해야 한다. 좌표는 조건 없이 읽고, 신뢰도가 필요할 때만
    coordinate_source를 본다.
    """

    platform: Platform
    idx: str
    name: str
    address: str | None = None
    phone: str | None = None
    longitude: float | None = None
    latitude: float | None = None
    coordinate_source: CoordinateSource | None = None
