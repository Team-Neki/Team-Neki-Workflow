"""collect 가 남긴 지점 좌표에 법정동 코드를 붙인다.

collect 와 별도 flow 다. 합치면 enrich 가 죽었을 때 부모 run 을 재시도하는 순간
사이트를 다시 긁는다. 나눠 두면 재시도가 S3 만 다시 읽는다. 순서는 시각으로
맞춘다. collect 04:00 KST, enrich 05:00 KST 이고, enrich 는 그 시점까지 적재된
것만 본다.

무엇을 읽을지는 collect 의 읽기 계약(docs/spec/collect-pipeline.md)대로
manifest.read_cycle 이 정한다. 브랜드마다 대상 일자 이하의 최신 적재를 집고,
7일 넘게 낡은 브랜드는 버린다. 늦게 끝난 collect 는 그날 enrich 에 안 들어가고
전날 것으로 대신하며 source_dt 가 그것을 드러낸다.

산출물은 둘이다. S3 enrich/dt=<사이클>/<실행 시각>.csv 는 이력과 재실행 원천이고,
Postgres tb_photo_booth_enriched 는 index(서버 batch)가 읽는 현재 세대다.
색인은 여기서 띄우지 않는다. 별도 flow search-index 가 시각으로 뒤에 돌며 그
시점의 현재 세대를 읽는다. 묶으면 enrich 재시도가 색인을 되풀이하고 색인 실패가
enrich 를 실패로 만든다.
"""

from collections import Counter
from datetime import date, datetime
from typing import Any

from prefect import flow, get_run_logger

from flows.common.manifest import KST, MAX_STALE_DAYS, ensure_table, read_cycle
from flows.common.manifest import target_date as cycle_date
from flows.common.platform import Platform
from flows.common.storage import put_enriched, read_stores
from flows.stores_enrich.region import (
    COLUMNS,
    EnrichedStore,
    enrich_stores,
    from_collect,
    mismatched,
)
from flows.stores_enrich.table import read_current, swap_table

# 브랜드 11개가 1,653건이다(2026-09-25). 절반 넘게 사라졌다면 S3 를 잘못 읽었거나
# 브랜드 대부분이 7일 넘게 낡아 버려진 것이므로 바꿔치우지 않는다. 브랜드 하나가
# 빠지는 것은 막지 않는다. 그것은 read_cycle 이 일부러 떨어뜨리는 동작이고, 그때
# 스왑을 멈추면 나머지 브랜드까지 갱신이 멈춘다. 브랜드가 늘면 올린다.
MIN_EXPECTED = 800


def _read_inputs(
    cycle: date, *, max_stale_days: int, enriched_at: datetime
) -> list[EnrichedStore]:
    """사이클의 브랜드별 CSV 를 읽어 판정 전 행으로 모은다.

    failed 브랜드는 경고 후 건너뛴다. (platform, idx) 중복은 첫 것만 남긴다.
    결과 테이블의 PK 라 둘 수 없다.
    """
    logger = get_run_logger()

    # 브랜드 전부가 한 번도 적재되지 않았으면 테이블이 없다. 그대로 조회하면
    # UndefinedTable 이 "읽을 것이 없다" 는 진짜 이유를 가린다.
    ensure_table()

    stores: list[EnrichedStore] = []
    seen: set[tuple[str, str]] = set()
    for name, outcome in read_cycle(cycle, max_stale_days=max_stale_days).items():
        if outcome["status"] == "failed":
            logger.warning("%s: %d일 안에 쓸 적재물이 없어 뺍니다.", name, max_stale_days)
            continue

        source_dt = outcome["source_target_date"]
        if outcome["status"] == "stale":
            logger.warning(
                "%s: %s 사이클(%d일 전)로 대신합니다.", name, source_dt, outcome["age_days"]
            )

        for record in read_stores(platform=Platform(name), target_date=source_dt):
            key = (record["platform"], record["idx"])
            if key in seen:
                logger.warning("%s idx=%s 가 중복이라 첫 것만 남깁니다.", *key)
                continue
            seen.add(key)
            stores.append(from_collect(record, source_dt=source_dt, enriched_at=enriched_at))

    return stores


@flow(name="stores-enrich", log_prints=True)
def stores_enrich(
    target_date: date | None = None,
    persist: bool = True,
    max_stale_days: int = MAX_STALE_DAYS,
) -> dict[str, Any]:
    """최신 collect 적재물에 법정동 코드를 붙여 S3 와 Postgres 에 남긴다.

    target_date 는 사이클 날짜다. 비우면 이 run 의 예약 시각(KST)이고 백필은
    지난 날짜를 준다. persist 를 끄면 S3 와 Postgres 에 쓰지 않는다. 직전 세대도
    읽지 않으므로 전 지점을 Kakao 에 묻는다.
    """
    logger = get_run_logger()

    cycle = target_date or cycle_date()
    enriched_at = datetime.now(KST).replace(tzinfo=None)

    stores = _read_inputs(cycle, max_stale_days=max_stale_days, enriched_at=enriched_at)
    if not stores:
        raise RuntimeError(
            f"{cycle:%Y-%m-%d} 사이클에 보강할 지점이 없습니다. collect 가 돌았는지 확인하세요."
        )

    previous = read_current() if persist else {}
    enriched = enrich_stores(stores, previous)

    counts = Counter(store.geocode_status for store in enriched)
    logger.info(
        "법정동 보강 %d건: ok %d, reused %d, no_coordinate %d, failed %d",
        len(enriched),
        counts["ok"],
        counts["reused"],
        counts["no_coordinate"],
        counts["failed"],
    )
    for store in enriched:
        if store.geocode_status in ("failed", "no_coordinate"):
            logger.warning(
                "법정동 없음 (%s): %s %s / %s",
                store.geocode_status,
                store.platform,
                store.name,
                store.address,
            )

    suspicious = [s for s in enriched if s.geocode_status == "ok" and mismatched(s)]
    for store in suspicious:
        logger.warning(
            "시군구 불일치: %s %s 주소 '%s' 인데 Kakao 는 '%s'",
            store.platform,
            store.name,
            store.address,
            store.sgg_name,
        )

    if len(enriched) < MIN_EXPECTED:
        raise ValueError(
            f"보강 결과가 {len(enriched)}건으로 하한 {MIN_EXPECTED}건에 못 미칩니다. "
            "S3 나 manifest 를 확인해야 합니다. 테이블은 바꿔치우지 않았습니다."
        )

    result: dict[str, Any] = {
        "cycle": f"{cycle:%Y-%m-%d}",
        "count": len(enriched),
        "status": dict(counts),
        "mismatched": len(suspicious),
    }
    if not persist:
        return result

    result["s3_path"] = put_enriched(enriched, columns=COLUMNS, target_date=cycle)

    swapped = swap_table(enriched, cycle=cycle)
    if swapped["unknown_codes"] > 0:
        logger.warning(
            "tb_legal_dong 에 없는 법정동 코드가 %d건입니다. 마스터가 낡았을 수 "
            "있습니다 (legal-dong flow).",
            swapped["unknown_codes"],
        )
    elif swapped["unknown_codes"] < 0:
        logger.warning("tb_legal_dong 이 없어 법정동 코드를 대조하지 못했습니다.")
    result["table"] = swapped
    return result
