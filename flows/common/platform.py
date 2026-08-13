"""수집 대상 플랫폼 식별자.

브랜드마다 Store 정의가 다르더라도 이 값은 공유한다. 적재할 때 어느 브랜드에서
온 지점인지 구분하는 열쇠가 된다.
"""

from enum import StrEnum


class Platform(StrEnum):
    """지점 정보를 수집한 브랜드."""

    DONT_LXXK_UP = "DONT_LXXK_UP"
    LIFE_FOUR_CUT = "LIFE_FOUR_CUT"
    MONO_MANSION = "MONO_MANSION"
    PHOTOISM = "PHOTOISM"
    PHOTO_SIGNATURE = "PHOTO_SIGNATURE"
    PICDOT = "PICDOT"
    PLANB_STUDIO = "PLANB_STUDIO"
