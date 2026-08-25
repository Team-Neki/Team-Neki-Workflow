"""브랜드 수집을 한 번에 돌린다.

브랜드마다 deployment 를 두면 스케줄이 흩어지고, 다음 단계가 "오늘 수집이 다
끝났나"를 시간으로 짐작해야 한다. 이 flow 하나가 순서와 완료를 책임진다.

브랜드 flow 를 직접 import 한다. 워크플로 디렉토리끼리는 서로를 모르는 것이
기본이지만, 묶는 쪽은 묶이는 쪽을 알아야 한다. 반대 방향은 없다.
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import date
from typing import Any, Callable

from prefect import flow, get_run_logger

from flows.broomstudio_stores import broomstudio_stores
from flows.common.platform import Platform
from flows.common.storage import latest_dt, put_run_manifest, read_manifest, today
from flows.harufilm_stores import harufilm_stores
from flows.lifefourcuts_stores import lifefourcuts_stores
from flows.monomansion_stores import monomansion_stores
from flows.photogray_stores import photogray_stores
from flows.photoism_stores import photoism_stores
from flows.photolabplus_stores import photolabplus_stores
from flows.photosignature_stores import photosignature_stores
from flows.picdot_stores import picdot_stores
from flows.planbstudio_stores import planbstudio_stores

BRANDS: dict[Platform, Callable[..., list[Any]]] = {
    Platform.LIFE_FOUR_CUT: lifefourcuts_stores,
    Platform.PHOTOISM: photoism_stores,
    Platform.PHOTO_SIGNATURE: photosignature_stores,
    Platform.PHOTO_GRAY: photogray_stores,
    Platform.PLANB_STUDIO: planbstudio_stores,
    Platform.PICDOT: picdot_stores,
    Platform.MONO_MANSION: monomansion_stores,
    Platform.HARU_FILM: harufilm_stores,
    Platform.PHOTO_LAB_PLUS: photolabplus_stores,
    Platform.BROOM_STUDIO: broomstudio_stores,
}

# 이보다 오래된 데이터로는 대신하지 않는다. 무한정 대신하면 파서가 깨진 채로
# 몇 주가 지나도 아무도 눈치채지 못한다. best effort 가 고장을 감추는 장치가
# 되면 안 된다.
MAX_STALE_DAYS = 7


def _fill_from_previous(
    results: dict[str, dict[str, Any]], *, dt: date, max_stale_days: int
) -> None:
    """실패한 브랜드를 이전 파티션으로 대신한다.

    이전 데이터를 오늘 파티션에 복사하지 않는다. 오늘 수집한 적 없는 것이 오늘
    것처럼 보이면 collect 계층이 거짓말을 하게 되고, 며칠이 지나도 신선도를
    알 수 없다. 대신 manifest 가 브랜드마다 어느 파티션을 읽을지 가리킨다.
    """
    logger = get_run_logger()

    for name, result in results.items():
        if result["status"] == "ok":
            result["source_dt"] = f"{dt:%Y-%m-%d}"
            result["age_days"] = 0
            continue

        previous = latest_dt(Platform(name))
        if previous is None:
            logger.error("%s: 대신할 이전 파티션이 없습니다.", name)
            continue

        age = (dt - previous).days

        if age == 0:
            # 오늘 파티션이 이미 있다. 앞선 실행이 성공했거나 단독 실행으로
            # 채워둔 경우다. 이번 시도는 실패했어도 데이터는 오늘 것이므로
            # 오래된 것으로 표시하지 않는다.
            result["status"] = "ok"
            result["source_dt"] = f"{previous:%Y-%m-%d}"
            result["age_days"] = 0
            result["count"] = read_manifest(Platform(name), previous).get("count")
            logger.info("%s: 이번 시도는 실패했으나 오늘 파티션이 이미 있습니다.", name)
            continue

        if age > max_stale_days:
            logger.error(
                "%s: 가장 최근 파티션이 %s로 %d일 지나 쓰지 않습니다.",
                name,
                previous,
                age,
            )
            result["stale_dt"] = f"{previous:%Y-%m-%d}"
            result["age_days"] = age
            continue

        result["status"] = "stale"
        result["source_dt"] = f"{previous:%Y-%m-%d}"
        result["age_days"] = age
        result["count"] = read_manifest(Platform(name), previous).get("count")

        logger.warning(
            "%s: 오늘 수집에 실패해 %s 파티션(%d일 전, %s건)으로 대신합니다.",
            name,
            previous,
            age,
            result["count"],
        )


@flow(name="stores-collect", log_prints=True)
def stores_collect(
    persist: bool = True,
    only: list[str] | None = None,
    max_stale_days: int = MAX_STALE_DAYS,
) -> dict[str, dict[str, Any]]:
    """브랜드를 병렬로 수집하고 실행 manifest 를 남긴다.

    Prefect 3 에서 동기 서브플로우 호출은 순차다. 스레드로 감싸야 실제로
    겹쳐 돈다. 사이트 입장에서는 여전히 한 곳당 순차 접근이다.

    한 브랜드가 실패해도 나머지는 적재한다. 사이트 하나가 개편돼 파싱이 깨졌을
    때 나머지까지 멈추는 것은 과하다. 그리고 실패한 브랜드는 이전 파티션으로
    대신해 다음 단계가 브랜드를 통째로 잃지 않게 한다.

    only 에 platform 이름을 주면 그것만 돌린다. 백필에 쓴다.
    """
    logger = get_run_logger()

    targets = {
        platform: run
        for platform, run in BRANDS.items()
        if not only or str(platform) in only
    }
    if not targets:
        raise ValueError(f"수집할 브랜드가 없습니다. only={only}")

    results: dict[str, dict[str, Any]] = {}

    with ThreadPoolExecutor(max_workers=len(targets)) as pool:
        futures = {
            platform: pool.submit(run, persist=persist)
            for platform, run in targets.items()
        }

        for platform, future in futures.items():
            try:
                stores = future.result()
            except Exception as error:
                # 여기서 예외를 다시 던지면 나머지 브랜드의 결과까지 버리게 된다.
                results[str(platform)] = {
                    "status": "failed",
                    "error": f"{type(error).__name__}: {error}",
                }
                logger.error("%s 수집 실패: %s", platform, error)
                continue

            results[str(platform)] = {"status": "ok", "count": len(stores)}
            logger.info("%s 수집 %d건", platform, len(stores))

    if persist:
        _fill_from_previous(results, dt=today(), max_stale_days=max_stale_days)

    succeeded = [name for name, r in results.items() if r["status"] == "ok"]
    stale = [name for name, r in results.items() if r["status"] == "stale"]
    failed = [name for name, r in results.items() if r["status"] == "failed"]

    if not succeeded and not stale:
        raise RuntimeError(
            f"쓸 수 있는 브랜드가 없습니다: {failed}. 다음 단계로 넘길 것이 없습니다."
        )

    if persist:
        put_run_manifest(results)

    if not succeeded:
        # 전부 이전 데이터로 버티는 상황이다. 개별 사이트 문제가 아니라 네트워크나
        # 배포에 문제가 있을 가능성이 높다.
        logger.error("오늘 새로 수집된 브랜드가 하나도 없습니다.")

    if stale:
        logger.warning("%d개 브랜드를 이전 파티션으로 대신합니다: %s", len(stale), stale)

    if failed:
        logger.error("%d개 브랜드가 빠진 채로 진행합니다: %s", len(failed), failed)

    logger.info(
        "수집 완료: 신선 %d, 이전 데이터 %d, 없음 %d, 합계 %d건",
        len(succeeded),
        len(stale),
        len(failed),
        sum(r.get("count") or 0 for r in results.values()),
    )
    return results
