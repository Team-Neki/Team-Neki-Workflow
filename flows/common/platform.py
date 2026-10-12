"""수집 대상 플랫폼 식별자.

브랜드마다 Store 정의가 다르더라도 이 값은 공유한다. 적재할 때 어느 브랜드에서
온 지점인지 구분하는 열쇠가 된다.

값은 서버 tb_brand.code 와 같다. 서버 색인과 stores-sync 가 이 값으로 브랜드를 찾는다
(BACKEND-228). 멤버 이름은 코드에서 부르는 이름일 뿐이라 code 와 달라도 된다.
"""

from enum import StrEnum


class Platform(StrEnum):
    """지점 정보를 수집한 브랜드."""

    BROOM_STUDIO = "BROOM_STUDIO"
    DONT_LXXK_UP = "DONT_LXXK_UP"
    HARU_FILM = "HARUFILM"
    LIFE_FOUR_CUT = "LIFEFOURCUTS"
    MONO_MANSION = "MONO_MANSION"
    PHOTOISM = "PHOTOISM"
    PHOTO_GRAY = "PHOTOGRAY"
    PHOTO_LAB_PLUS = "PHOTOLAB_PLUS"
    PHOTO_SIGNATURE = "PHOTOSIGNATURE"
    PICDOT = "PIC_DOT"
    PLANB_STUDIO = "PLANB_STUDIO"
