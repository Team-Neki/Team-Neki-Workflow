"""브랜드 수집을 한 번에 돌린다.

브랜드마다 deployment 를 두면 스케줄이 흩어지고, 다음 단계가 "오늘 수집이 다
끝났나"를 시간으로 짐작해야 한다. 이 flow 하나가 순서와 완료를 책임진다.

브랜드 flow 를 직접 import 한다. 워크플로 디렉토리끼리는 서로를 모르는 것이
기본이지만, 묶는 쪽은 묶이는 쪽을 알아야 한다. 반대 방향은 없다.
"""

from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable

from prefect import flow, get_run_logger

from flows.common.platform import Platform
from flows.common.storage import put_run_manifest
from flows.lifefourcuts_stores import lifefourcuts_stores
from flows.monomansion_stores import monomansion_stores
from flows.photosignature_stores import photosignature_stores
from flows.picdot_stores import picdot_stores
from flows.planbstudio_stores import planbstudio_stores

BRANDS: dict[Platform, Callable[..., list[Any]]] = {
    Platform.LIFE_FOUR_CUT: lifefourcuts_stores,
    Platform.PHOTO_SIGNATURE: photosignature_stores,
    Platform.PLANB_STUDIO: planbstudio_stores,
    Platform.PICDOT: picdot_stores,
    Platform.MONO_MANSION: monomansion_stores,
}


@flow(name="stores-collect", log_prints=True)
def stores_collect(
    persist: bool = True,
    only: list[str] | None = None,
) -> dict[str, dict[str, Any]]:
    """브랜드를 병렬로 수집하고 실행 manifest 를 남긴다.

    Prefect 3 에서 동기 서브플로우 호출은 순차다. 스레드로 감싸야 실제로
    겹쳐 돈다. 사이트 입장에서는 여전히 한 곳당 순차 접근이다.

    한 브랜드가 실패해도 나머지는 적재한다. 사이트 하나가 개편돼 파싱이 깨졌을
    때 나머지까지 멈추는 것은 과하다. 대신 무엇이 빠졌는지를 manifest 에 남겨
    다음 단계가 알 수 있게 한다.

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

    succeeded = [name for name, r in results.items() if r["status"] == "ok"]
    failed = [name for name, r in results.items() if r["status"] != "ok"]

    if not succeeded:
        raise RuntimeError(
            f"모든 브랜드가 실패했습니다: {failed}. 다음 단계로 넘길 것이 없습니다."
        )

    if persist:
        put_run_manifest(results)

    if failed:
        logger.warning(
            "%d개 브랜드가 빠진 채로 진행합니다: %s", len(failed), failed
        )

    logger.info(
        "수집 완료: 성공 %d, 실패 %d, 합계 %d건",
        len(succeeded),
        len(failed),
        sum(r.get("count", 0) for r in results.values()),
    )
    return results
