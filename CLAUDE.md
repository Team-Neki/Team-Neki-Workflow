# CLAUDE.md

이 문서는 이 저장소에서 코드를 작성할 때 지켜야 할 규약과 빠지기 쉬운 함정을 다룹니다.

## 구조

워크플로 하나가 디렉토리 하나입니다. `@flow`와 `@task`를 같은 디렉토리에 두되
파일은 나눕니다. 스케줄만 따로 모읍니다.

```text
deployments/              언제 돌리나
  daily_sync.py           build()
flows/
  daily_sync/             무엇을 어떤 순서로 + 실제로 하는 일
    __init__.py           flow 재노출
    flow.py               @flow
    orders.py             @task
  common/                 여러 워크플로가 함께 쓰는 task
    store.py              수집 공통 스키마 (CollectedStore)
    storage.py            S3 적재
    imweb_map.py          imweb 지도 위젯 수집 (인생네컷, 포토이즘, 돈룩업)
aws/config                로컬 개발용 AWS 프로파일
compose.yaml              로컬 S3 (LocalStack)
```

의존은 `deployments -> flows` 단방향입니다. `flows/`가 스케줄을 알면 안 됩니다.

워크플로 안에서는 `flow.py`가 task 모듈을 import하고 그 반대는 없습니다. task
모듈이 `flow.py`를 import해야 할 것 같다면 대개 인자로 받아야 할 값을 import로
끌어온 것이므로, 시그니처를 먼저 의심해볼 수 있습니다.

`__init__.py`는 flow 함수만 재노출합니다. 그래야 `deployments/`가
`from flows.daily_sync import daily_sync`로 짧게 가져올 수 있습니다.

## 실행 모델

UI나 cron이 워크플로를 직접 실행하지 않습니다. flow run 레코드를 만들어둘 뿐이고,
`serve.py`나 worker가 API를 주기적으로 들여다보다 집어갑니다. pull 모델입니다.

```text
[UI / cron / API]
      |  1. flow run 레코드 생성 (SCHEDULED)
      v
[Prefect API]  <-----+
                     |  2. 10초마다 폴링
              [serve.py / worker]
                     |  3. entrypoint import 후 실행
                     v
                 [flow] -> [task]
```

여기서 따라오는 성질들이 있습니다.

- 실행 프로세스가 없으면 run이 `SCHEDULED`에 쌓임. 등록만으로는 실행되지 않음
- 버튼을 눌러도 최대 10초 지연이 있음 (`runner.poll_frequency`, `worker.query_seconds`)
- 서버가 워커에 접근할 필요 없음. 워커의 outbound만 열리면 됨
- task는 flow와 같은 프로세스에서 실행됨. `@task`는 재시도와 상태 추적 단위이지
  실행 격리 단위가 아님

**UI에서 실행하려면 deployment 등록과 실행 프로세스가 둘 다 필요합니다.** 하나만
있으면 UI에 보이지만 눌러도 진행되지 않거나, 아예 목록에 뜨지 않습니다.

## 수집 파이프라인의 경계

지점 수집은 collect, enrich, index 세 단계입니다. 단계를 섞으면 재실행 단위가
무너지므로 어디에 코드를 둘지 헷갈릴 때는 두 질문으로 가릅니다.

- 다시 돌리려면 사이트를 또 긁어야 하나. 아니면 collect 바깥임
- 값이 틀렸을 때 누구 잘못인가. 사이트면 collect, Kakao면 enrich, 우리 규칙이면 index

**collect에서 주소를 해석하지 않습니다.** 도로명인지 지번인지 판정하거나 상호명,
층수를 떼는 것은 전부 enrich의 일입니다. 픽닷은 수집원이 Kakao라 지번 주소를
공짜로 받을 수 있지만 담지 않습니다. 다른 브랜드에 없는 값을 담으면 collect
출력이 브랜드마다 달라지고, 그러면 enrich가 전 브랜드를 한 번에 받지 못합니다.

지오코딩 같은 외부 API 보강은 enrich에 모읍니다. 수집 flow에 붙이면 Kakao 장애가
수집 실패가 되고, 색인 규칙을 고칠 때마다 크롤링이 따라 돕니다. 픽닷, 모노맨션,
포토그레이, 하루필름, 포토랩플러스, 비룸스튜디오는 수집원 자체가 Kakao라 예외지만,
이 경우에도 받아온 값을 해석하지는 않습니다.

하루필름은 사이트에도 목록이 있지만 Kakao를 씁니다. 전체 목록 페이지에 목록이
없어 지역 페이지 8개를 순회해야 하고, 그 목록마저 게시판이 아니라 갤러리 위젯
캡션이라 주소가 마지막 `<br>` 뒤 텍스트로 들어 있습니다. 전화와 좌표도 주지
않습니다. 자세한 근거는 `flows/harufilm_stores/flow.py` 모듈 docstring에
남겼습니다.

포토랩플러스는 사이트에 목록이 있는데도 Kakao를 씁니다. 탭이 무규칙한 iframe으로
나뉘고 손으로 만든 텍스트 위젯이라 주소가 두 줄로 쪼개진 항목이 있으며, 제주 지점에
서울 주소가 들어가 있는 등 **사이트가 틀린 값을 줍니다.** 파서로는 걸러지지 않는
문제라 수집원 자체를 바꾼 것입니다.

비룸스튜디오는 브랜드 도메인이 NXDOMAIN이라 Kakao 말고 고를 수집원이 아예
없습니다.

### imweb 지도 위젯은 한 모듈이 담당합니다

인생네컷과 포토이즘, 돈룩업은 같은 imweb 위젯을 씁니다. 목록 페이지 HTML에는 지점이 없고
AJAX 엔드포인트가 HTML 조각을 돌려주므로, 페이지를 긁지 않고 엔드포인트를 직접
호출합니다. 엔드포인트와 셀렉터가 사이트를 넘나들며 그대로 먹습니다.

그래서 `flows/common/imweb_map.py` 하나가 순회와 파싱을 맡고 브랜드 flow는
`collect_board`에 넷만 넘깁니다.

```text
base_url  board_code  referer  platform
```

**브랜드별 분기를 이 모듈에 넣지 않습니다.** 넣으면 사이트가 개편됐을 때 한 곳만
고치고 넘어가게 되어 공용화한 의미가 사라집니다. 위 넷 말고 다른 것이 필요해
보이면 대개 collect가 아니라 enrich에서 해야 할 일입니다.

**범위를 벗어난 페이지가 빈 응답이 아니라 마지막 페이지로 고정됩니다.** 따라서
"빈 페이지면 중단"으로는 끝나지 않고, 직전 페이지와 항목 id 집합을 비교해야
합니다. `MAX_PAGES`는 이 판정이 어긋났을 때를 막는 안전장치일 뿐이므로 실제
페이지 수에 가깝게 잡지 않습니다. 가깝게 잡으면 지점이 조금 늘었을 때 상한에
걸려 조용히 잘립니다.

조각 상단 `.tit > span.text-brand`에 총 건수가 실려 옵니다. Kakao의
`total_count`처럼 대조 기준으로 쓰되, 위젯 설정에 따라 꺼져 있어(인생네컷) 값이
없으므로 파서는 `int | None`을 돌려주고 있을 때만 비교합니다.

전화번호는 `tel:` 링크가 있어도 href가 비어 있는 경우가 있습니다. 포토이즘은 488곳
전부가 그렇습니다. `""`가 그대로 담기면 다음 단계가 번호가 있는 줄로 오해하므로
`None`으로 맞춥니다.

### Kakao 장소검색의 45건 상한

한 질의로 꺼낼 수 있는 문서는 45건까지입니다. `total_count`는 상한과 무관하게
실제 개수를 알려주므로, `total_count > pageable_count`면 잘린 것입니다.

**행정구역 이름으로 쪼개지 않습니다.** 개편을 따라다녀야 하고, 실제 데이터에
`전남광주통합특별시`처럼 예상 못 한 표기가 섞입니다. 대신 `flows/common/kakao.py`의
`search_all`이 좌표 사각형을 넷으로 나눠 재귀합니다. 모노맨션 기준으로 호출 26회에
104건 전량이 나오며, 행정구역 순회(43회)보다 적습니다.

수집 후에는 건수를 `total_count`와 대조해야 합니다. 분할이 덜 내려갔을 때 이것
말고는 드러나는 곳이 없습니다.

### 공통 스키마

브랜드별 `Store` dataclass를 두지 않습니다. `flows/common/store.py`의
`CollectedStore` 하나를 모든 브랜드가 공유하고, 사이트가 주지 않는 필드는 `None`으로
둡니다. 브랜드마다 모양이 다르면 enrich가 공통으로 받을 수 없습니다.

`collected_at`은 `CollectedStore`에 없습니다. 사이트가 준 값이 아니라 우리가 언제
받았는지이므로 적재 시점에 `storage`가 붙입니다.

### 수집 스케줄은 한 곳에만 있습니다

정기 수집 스케줄은 `deployments/stores_collect.py` 하나뿐입니다. 브랜드별
deployment는 **cron 없이** 남겨둡니다. 스케줄이 없으면 자동으로 돌지 않지만 UI
목록에는 남아 실행 버튼이 동작하므로, 백필과 단일 브랜드 재수집에 그대로 씁니다.

`paused=True` 대신 스케줄을 아예 두지 않는 이유는, pause는 배포가 덮어쓰는 문제를
계속 신경 써야 하지만 스케줄이 없으면 그럴 일이 없기 때문입니다.

브랜드를 추가하면 `flows/stores_collect/flow.py`의 `BRANDS`에도 등록해야 합니다.
빠뜨리면 단독 실행은 되는데 정기 수집에서만 조용히 누락됩니다.

`stores_collect`가 브랜드 flow를 직접 import합니다. 워크플로 디렉토리끼리 서로를
모르는 것이 기본이지만, 묶는 쪽은 묶이는 쪽을 알아야 합니다. 반대 방향은 없습니다.

### 한 브랜드가 실패해도 멈추지 않습니다

Prefect 3에서 동기 서브플로우 호출은 순차입니다. `ThreadPoolExecutor`로 감싸야
실제로 겹쳐 돕니다. 실측으로 45.6초에서 22.1초가 됩니다.

각 브랜드의 예외는 `future.result()` 자리에서 잡아 결과에 담고 넘어갑니다.
**여기서 예외를 다시 던지면 나머지 브랜드의 결과까지 버리게 됩니다.** 사이트
하나가 개편돼 파싱이 깨졌을 때 나머지까지 멈추는 것은 과합니다.

실패한 브랜드는 이전 파티션으로 대신합니다. 그래야 사이트 하나가 깨졌다고 색인에서
브랜드가 통째로 사라지지 않습니다.

**이전 데이터를 오늘 파티션에 복사하지 않습니다.** 오늘 수집한 적 없는 것이 오늘
것처럼 보이면 collect 계층이 거짓말을 하게 되고, 며칠이 지나도 신선도를 알 수
없습니다. 대신 run manifest가 브랜드마다 `source_dt`로 어느 파티션을 읽을지
가리킵니다. 다음 단계는 그것만 보면 되고 신선한지 따로 판단할 필요가 없습니다.

`MAX_STALE_DAYS`(기본 7일)를 넘으면 대신하지 않고 `failed`로 둡니다. 무한정
대신하면 파서가 깨진 채로 몇 주가 지나도 아무도 눈치채지 못합니다. **best effort가
고장을 감추는 장치가 되면 안 됩니다.** 쓰지 않기로 한 경우에도 `stale_dt`와
`age_days`는 기록해 왜 버렸는지 남깁니다.

상태는 셋입니다.

- `ok` : 오늘 수집됨. 이번 시도가 실패해도 오늘 파티션이 이미 있으면 여기 속함
- `stale` : 이전 파티션으로 대신함. `age_days`가 며칠 전인지 알려줌
- `failed` : 쓸 수 있는 것이 없음

`ok`와 `stale`이 하나도 없을 때만 flow를 실패시킵니다. 다음 단계로 넘길 것이 없기
때문입니다. 신선한 것이 하나도 없으면(전부 `stale`) 개별 사이트 문제가 아니라
네트워크나 배포 쪽을 의심해야 하므로 별도로 에러 로그를 남깁니다.

### S3 적재

```text
raw/     platform=<브랜드>/dt=<날짜>/<이름>.gz
collect/ platform=<브랜드>/dt=<날짜>/stores.jsonl.gz
                                   /_manifest.json
runs/    dt=<날짜>/collect.json
```

`runs/`는 실행 하나를 설명합니다. 어느 브랜드가 성공하고 실패했는지, 실패 사유가
무엇인지 담습니다. 파티션마다 있는 `_manifest.json`으로는 답할 수 없습니다.
**없는 파티션은 없다는 사실 자체가 기록되지 않기 때문입니다.** enrich는 이걸 보고
무엇을 처리할지 정합니다.

`collect/` 밖에 두는 것이 중요합니다. 그쪽은 Hive 파티션만 있어야 Glue를 그대로
붙일 수 있고, 다른 것이 섞이면 파티션 인식이 깨집니다.

최신 파티션은 `latest_dt`가 목록으로 찾습니다. 포인터 객체를 따로 두지 않습니다.
포인터는 갱신 시점에 경합이 있고, 과거 날짜를 백필하면 최신이 뒤로 밀립니다.

- 파티션 날짜는 KST임. UTC로 끊으면 새벽 실행이 전날 파티션에 들어감
- 같은 날 재실행은 같은 키를 덮어씀. 단일 객체 PUT은 원자적이라 멱등함
- `_manifest.json`을 본문보다 **나중에** 올림. 순서가 뒤집히면 manifest만 있고
  데이터가 없는 창이 생김
- `read_stores`가 manifest의 `count`와 실제 줄 수를 대조함. 다르면 예외임

`boto3.Session().client("s3")`에 `endpoint_url`을 넘기지 않습니다. 로컬과 운영의
차이는 `AWS_PROFILE` 하나여야 합니다. 코드에 분기를 넣으면 이 성질이 깨집니다.

### 배치 컨테이너 실행

enrich와 index는 Spring Batch로 구현하고 Prefect가 k3s Job으로 띄웁니다. 스케줄과
순서, 재시도는 이 저장소에 남고 계산만 컨테이너가 합니다.

**Prefect의 Kubernetes work pool을 쓰지 않습니다.** 그것은 flow 자체를 컨테이너로
돌리는 기능이라 이미지 안에 Prefect와 flow 코드가 있어야 합니다. 우리는 임의의
Java 이미지를 돌리므로 process work pool에 flow를 두고 `flows/common/kubernetes.py`의
`run_job`이 Job을 만들어 지켜봅니다.

Job spec에서 놓치기 쉬운 것들입니다.

- `backoff_limit=0` : 재시도는 Prefect가 맡음. k8s가 같이 재시도하면 의미가 겹치고
  어느 시도의 로그인지 분간이 안 됨
- 이름은 `generate_name`으로 k8s가 붙임. 직접 지으면 DNS-1123 제약과 중복을 직접
  다뤄야 함
- `finally`에서 Job 삭제. 취소했을 때 pod가 남으면 안 됨
- pod 상태를 먼저 확인함. `ImagePullBackOff`에서 로그를 기다리면 타임아웃까지
  멈춰 있고 원인이 드러나지 않음

컨테이너 stdout은 Prefect 로그로 옮깁니다. 이게 없으면 UI에는 실패 사실만 남고
원인은 이미 사라진 pod 안에 있습니다.

권한은 `k8s/rbac.yaml`입니다. Job의 create/get/list/watch/delete, pod의
get/list/watch, `pods/log`의 get이면 충분합니다. pod 삭제 권한은 필요 없습니다.
Job을 지우면 ownerReference를 따라 정리됩니다.

### 최신 이미지와 재현성을 같이 얻는다

`:latest`를 그대로 넘기면 최신은 쓰지만 무엇이 돌았는지 모릅니다. 실패한 실행을
재현할 수 없고, 재시도 중간에 이미지가 바뀔 수 있으며, 노드마다 캐시가 달라
같은 태그가 다른 것을 가리킬 수 있습니다. 반대로 태그를 고정하면 새 배포가
반영되지 않습니다.

그래서 `flows/common/registry.py`의 `resolve`가 실행 시점에 태그가 지금 가리키는
다이제스트를 조회하고, `run_job`이 그것으로 실행합니다.

```text
alpine:3.20  ->  registry-1.docker.io/library/alpine@sha256:d9e853e8...
```

**태그는 배포 편의를 위해 남기고, 실행은 다이제스트로 합니다.** Spring 쪽은
`:latest`로 계속 밀어도 되고, 우리는 그때그때 최신을 집으면서 어느 다이제스트가
돌았는지 로그에 남깁니다. 롤백은 `image` 파라미터에 다이제스트를 직접 주면
됩니다.

`image_pull_policy`도 여기 맞춥니다. 다이제스트는 불변이라 `IfNotPresent`로
캐시를 믿어도 되고, 태그를 그대로 쓸 때만 `Always`여야 최신이 보장됩니다.
`pin_digest=False`는 레지스트리를 부를 수 없는 환경에서만 씁니다.

**해석은 flow run 단위로 한 번만 합니다.** task를 재시도할 때마다 다시 해석하면
그사이 새 이미지가 올라왔을 때 1차 시도와 2차 시도가 다른 코드를 돌게 됩니다.
한 번의 실행은 하나의 이미지로 끝나야 합니다. `_resolve_once`가 flow run id를
캐시 키로 써서 이를 보장하며, 다음 flow run은 id가 달라 다시 최신을 집습니다.

### 지하철 역은 Postgres 에 직접 적재합니다

지점 수집과 달리 S3 를 거치지 않습니다. `raw/`와 `collect/`는 `platform=` 파티션을
쓰고 그 값은 브랜드입니다. 역을 넣으려면 `Platform`에 브랜드가 아닌 값을 더해야
하고, 그러면 `stores_collect`의 `BRANDS` 순회에 섞여 들어갑니다.

수집원은 국가철도공단 표준데이터이고 **레일포털에서 로그인도 서비스키도 없이**
받아집니다. 공공데이터포털 사본(`15093755`)은 쓰지 않습니다. `atachFileYn`이 `N`인
링크형이라 첨부파일 다운로드가 404 로 끝나고, 사본이 원본보다 1년 반 낡았습니다.

```text
GET https://data.kric.go.kr/rips/dataset/download.file?type=filedata&id=32&operation=1
  -> 전체_도시철도역사정보_20260630.xlsx  1,099행 × 15열
```

- xlsx 만 옵니다. `openpyxl` 의존이 여기서 늘었습니다
- 실패해도 200 에 HTML 이 옵니다. xlsx 는 zip 이므로 앞 두 바이트(`PK`)로 가릅니다
- **`Content-Disposition`의 파일 이름이 utf-8 바이트인데 HTTP 헤더는 latin-1 로
  디코딩됩니다.** 되돌리지 않으면 한글이 깨진 채로 테이블 코멘트에 박힙니다
- 열은 이름으로 찾습니다. 순서로 읽으면 가운데에 열이 하나 끼었을 때 값이 통째로
  한 칸씩 밀린 채로 조용히 적재됩니다

세 단계는 법정동과 같습니다. `source`는 사이트가 준 것만 담고 `normalize`가 고칩니다.

#### 원문 행을 접지 않습니다

원문 한 행은 역이 아니라 **역 × 노선**입니다. 사용자가 "강남"을 검색하면
`강남역 2호선`과 `강남역 신분당선`이 각각 나오고, 그중 하나를 눌러 **그 역·노선
좌표 1km 안의 포토부스**를 받습니다. 접으면 고를 수가 없고, 노선마다 다른 좌표가
사라져 반경 결과도 같아집니다.

```text
광운대  경의중앙선  37.6239    127.062      <- 좌표가 다릅니다
광운대  경춘선      37.623635  127.061975
```

**1km 반경 매핑은 이 flow 가 하지 않습니다.** 역 좌표만 있으면 되는 일이고 지점
쪽에 붙으므로 포토부스 flow 의 몫입니다. 여기서는 좌표를 결측 없이 담는 데까지입니다.

#### 자연키가 없어 대리키를 씁니다

법정동은 10자리 코드가 PK 였지만 이 데이터에는 유일한 열 조합이 없습니다.

```text
역번호               고유   907 / 1,099   중복 146
(노선번호, 역번호)     고유 1,094 / 1,099   중복   5
```

`I4108` 하나에 경의중앙선과 경춘선이 같이 매달려 있어 **노선번호가 노선을 유일하게
식별하지 못합니다.** 원문 열에 PK 를 걸면 다음 스냅샷에 중복이 하나만 늘어도 적재가
통째로 죽으므로 `BIGSERIAL` 대리키를 쓰고 원문 식별자는 값으로만 담습니다.

`normalize`는 **모든 열이 같은 완전 중복만** 뺍니다(경인선 주안역 1건). 광운대의 두
노선은 값이 실제로 달라 남깁니다. 어느 쪽이 맞는지 판정하는 것은 수집의 일이 아닙니다.

#### 노선명은 다수결로 합치지 않습니다

같은 노선이 `7호선`과 `도시철도 7호선`으로 오고, 공백이 둘 들어간
`수도권  도시철도 9호선`도 있습니다. 검색 결과에 그대로 보이는 값이라 골라야 합니다.

**노선번호로 다수결을 내면 안 됩니다.** `I4108`에 경의중앙선 51행과 경춘선 3행이
매달려 있는데 그 둘은 실제로 다른 노선이라, 다수결이면 경춘선 3행이 경의중앙선으로
둔갑합니다. `I4101`의 1호선 10행과 경부선 37행도 같습니다. 노선명은 47종뿐이라
눈으로 확인할 수 있으므로 합칠 것만 `LINE_ALIAS`에 적습니다.

지역 접두는 떼지 않습니다. `대구 도시철도 1호선`에서 `대구`를 떼면 서울 1호선과
같아집니다.

#### 좌표는 부스 테이블과 같은 모양으로 담습니다

위경도를 컬럼으로 두지 않고 `location geometry(POINT, 4326)` 하나로 담습니다.
`tb_photo_booth_location`이 이미 그렇게 하고 `ST_X` / `ST_Y`로 돌려주고 있어
맞춘 것입니다. PostGIS 는 Team-Neki-Server 의 Flyway 가 이미 들여놓았으므로
확장 설치를 우리가 걱정할 일이 아닙니다.

반경 계산에서 두 테이블을 섞어 쓰는데 한쪽만 타입이 다르면 변환이 끼고, **변환이
끼면 인덱스가 죽습니다.**

**`location`에는 인덱스를 걸지 않습니다.** 반경 질의는 이 테이블이 아니라 부스
테이블에 겁니다. 사용자가 고른 역의 좌표는 여기서 읽어 상수로 넘어가고, 그 상수로
1km 안의 부스를 찾는 것은 `idx_photo_booth_location_location`(GiST)이 합니다.
반대 방향(부스마다 가까운 역 찾기)이 생기면 그때 GiST 한 줄을 더합니다.

인덱스는 `name`의 `text_pattern_ops` 하나뿐입니다. 법정동과 같은 이유입니다.

#### 원문이 주는 열을 다 담지 않습니다

쓰임이 역명 검색과 좌표 두 가지뿐이라 **영문명, 주소, 운영기관, 전화번호,
환승역구분, 데이터기준일자를 담지 않습니다.** 지점 수집에서 픽닷이 지번 주소를
공짜로 받으면서도 담지 않는 것과 같은 기준입니다. 쓰지 않는 값을 담으면 그 값이
틀렸을 때 누가 책임지는지가 흐려지고, 원문이 바뀔 때 따라 고칠 것만 늘어납니다.

담는 것은 여섯입니다.

```text
id  name  line_name  line_no  station_no  location
```

환승 여부는 컬럼이 아니라 같은 `name`의 행 수로 드러납니다. 시도를 담지 않으므로
동명이역(`송정` 서울 5호선 / 부산 동해선)은 `line_name`과 좌표로 갈립니다.

**시도를 담았다면 그 목록을 코드에 박아야 했을 것입니다.** 그러면 지역 개편마다
따라 고쳐야 하는데, 실제로 2026-07-01에 광주광역시와 전라남도가 폐지되고
전남광주통합특별시가 생겨 `tb_legal_dong`의 시도는 이미 16개입니다.

**부스 컬럼이 `geography`가 아니라 SRID 4326 `geometry`입니다.** 거리 단위가
미터가 아니라 도(degree)라서 `ST_DWithin(location, point, 1000)`은 1km 가 아닙니다.
도 단위로 걸러낸 뒤 미터를 따로 확인해야 합니다.

```sql
WHERE ST_DWithin(b.location, s.location, 0.012)            -- 인덱스가 타는 자리
  AND ST_DistanceSphere(b.location, s.location) <= 1000    -- 정확한 미터
```

`::geography` 로 캐스팅하면 미터가 바로 나오지만 기존 geometry GiST 를 타지 못합니다.

역명은 끝의 `역`을 뗍니다. 원문이 1,099행 중 358행에만 붙여 놓아 그대로 두면
"강남" 검색과 "강남역" 검색의 결과가 달라집니다. 표시할 때 붙이는 것은 앱의 일입니다.

적재는 법정동과 같은 테이블 바꿔치기입니다. 같은 마스터라서가 아니라 매 실행이 전량
스냅샷이라 같은 답이 나옵니다.

## build() 규약

`deployments/` 아래 모든 모듈은 `RunnerDeployment`를 반환하는 `build()`를 노출해야
합니다. `deployments/__init__.py`의 `collect()`가 이 이름으로 모듈을 찾습니다.

```python
from prefect.deployments.runner import RunnerDeployment

from flows.daily_sync import daily_sync


def build() -> RunnerDeployment:
    return daily_sync.to_deployment(name="daily-sync", cron="0 3 * * *")
```

- 함수명은 `build`로 고정. 다른 이름을 쓰면 `AttributeError`로 실패함
- 워크플로를 추가할 때 `serve.py`와 `deploy.py`는 건드리지 않음
- `deployments/<name>.py`와 `flows/<name>.py`는 파일명을 1:1로 맞춤

### 이름을 바꾸면 고아가 남습니다

deployment는 flow 이름과 deployment 이름의 조합(`hello/hello-local`)으로 식별됩니다.
따라서 flow 함수명이나 `@flow(name=)`을 바꾸면 새 deployment가 만들어지고 **예전
것은 지워지지 않은 채 남습니다.** entrypoint가 이미 없는 함수를 가리키므로 실행하면
실패하지만, UI 목록에는 계속 보입니다.

이름을 바꿀 때는 옛 deployment를 직접 지워야 합니다.

```bash
prefect deployment delete '<옛-flow-이름>/<deployment-이름>'
```

## 네이밍

- 파일, 모듈 : snake_case (e.g. `daily_sync.py`)
- flow 함수 : snake_case, 접미사 없음 (e.g. `def daily_sync(...)`)
- `@flow(name=)` : kebab-case (e.g. `"daily-sync"`). UI 표시명임
- deployment name : kebab-case (e.g. `"daily-sync"`, `"daily-sync-backfill"`)
- task 함수 : 동사로 시작 (e.g. `fetch_orders`, `upload_report`)

flow 함수에 `_flow` 접미사를 붙이지 않는 이유는 `flows.daily_sync.daily_sync`처럼
경로가 이미 역할을 말해주기 때문입니다. import 시 이름이 충돌하면 `as`로 호출부에서
해결합니다.

## serve.py vs. deploy.py

두 진입점은 용도가 다르며 섞어 쓰면 안 됩니다.

- `serve.py` : 로컬 개발용. 서버나 work pool 없이 한 프로세스로 즉시 확인함
- `deploy.py` : 운영용. 스케줄만 등록하고 실행은 worker가 담당함

**운영 스케줄 등록은 반드시 `deploy.py`를 거쳐야 합니다.** `prefect deploy`나
`flow.deploy()`를 직접 호출하면 아래 보존 로직을 건너뜁니다.

## pause는 배포가 덮어씁니다

Prefect는 deployment를 등록할 때 기존 정의를 통째로 덮어씁니다. 따라서 운영자가
UI에서 꺼둔 스케줄이 배포할 때마다 되살아납니다. Airflow에서 DAG를 pause하면 그
상태가 메타DB에 남아 유지되는 것과 다릅니다.

재등록이 일어나는 시점은 다음과 같습니다.

- `serve.py` 재시작 : 매번 재등록하므로 프로세스가 뜰 때마다 풀림
- `deploy()` 재실행 : 배포할 때마다 풀림
- Prefect 서버 재시작 : 유지됨 (DB에 남음)
- worker 재시작 : 유지됨 (worker는 정의를 건드리지 않음)

deployment의 `paused` 대신 스케줄의 `active`를 꺼도 마찬가지로 되살아납니다.
`deploy()`가 cron 인자를 그대로 다시 쓰므로 스케줄 객체가 통째로 교체되기
때문입니다.

`deploy.py`는 배포 전 `paused`와 각 스케줄의 `active`를 읽어두고 등록 후 되돌려
이를 막습니다. 반대로 UI에서 다시 켠 것을 배포가 도로 끄지도 않습니다. 이 파일을
수정할 때는 양방향이 모두 유지되는지 확인해야 합니다.

## 패키지 설정

저장소는 editable로 설치해야 합니다. 설치하지 않으면 `flows` import가 실행
위치(CWD)에 의존하게 되어 worker를 다른 경로에서 띄울 때 깨집니다.

```bash
make setup
```

- 명령은 `make`를 거치며 내부적으로 `uv run`을 씀. venv를 활성화하지 않음
- `uv run`은 `.venv`가 없으면 만들고 `uv.lock`에 맞춰 채운 뒤 실행함
- `uv.lock`은 커밋된 것을 그대로 씀. 지우고 다시 만들면 팀원 간 버전이 갈림
- venv에 `pip` 실행 파일이 없음. 직접 다뤄야 한다면 `uv pip`를 사용함

`uv run`은 `.env`를 자동으로 읽지 않습니다. Makefile이 `.env`가 있을 때만
`UV_ENV_FILE`을 지정해 이를 대신합니다. Makefile을 거치지 않고 실행할 때는
`uv run --env-file .env`로 직접 지정해야 하며, 그러지 않으면 API 키를 쓰는
워크플로가 키 없음 오류로 실패합니다.

최상위 패키지를 새로 추가하면 `pyproject.toml`의
`[tool.hatch.build.targets.wheel] packages`에 반드시 등록해야 합니다. 등록을
빠뜨려도 **로컬에서는 아무 문제가 없어 알아채기 어렵습니다.** editable 설치는
프로젝트 루트를 통째로 `sys.path`에 넣기 때문에 `packages` 목록과 무관하게 전부
import되기 때문입니다.

반면 wheel을 빌드하면 나열된 패키지만 포함됩니다. 즉, 로컬과 CI 테스트는
통과하는데 컨테이너 배포에서만 `ModuleNotFoundError`가 나는 형태로 드러납니다.
`packages`를 수정했다면 wheel 내용을 직접 확인하는 것이 좋습니다.

```bash
uv build --wheel --out-dir /tmp/dist
python -c "import zipfile,glob; w=sorted(glob.glob('/tmp/dist/*.whl'))[-1]; \
print(sorted({n.split('/')[0] for n in zipfile.ZipFile(w).namelist() if '/' in n}))"
```

## Prefect 3 API 함정

Prefect 3.8 기준입니다.

- `Flow.deploy()`와 `prefect.serve()`는 동기 함수임. 이벤트 루프 안에서 호출하면
  실패하므로 `asyncio.run()` 밖에서 불러야 함
- API가 돌려주는 `DeploymentResponse`에는 `flow_name`이 없음.
  `flow_name`은 `RunnerDeployment`에만 있음
- `apply()`는 스케줄을 새로 만들어 ID가 바뀜. 배포 전후로 스케줄을 대응시킬 때는
  ID가 아니라 `slug`를 쓰고, `slug`가 없으면 순번으로 맞춰야 함
- `to_deployment(paused=...)`로 등록 시점에 pause 상태를 정할 수 있음.
  등록 후 되돌리는 것보다 경합이 없어 안전함

## 변경 검증

구조를 바꿨다면 임포트와 deployment 수집부터 확인합니다.

```bash
make check
```

flow 로직만 바꿨다면 단독 실행으로 충분합니다. 수집 flow는 S3에 적재하므로
LocalStack이 먼저 떠 있어야 합니다.

```bash
make localstack
make hello
make lifefourcuts
make photoism
make dontlxxkup
make photosignature
make photogray
make planbstudio
make picdot
make monomansion
make harufilm
make photolabplus
make broomstudio
make collect
make s3-ls
make subway-station
```

`subway-station`은 `DATABASE_URL`이 있어야 돕니다. 파싱과 정규화만 볼 때는
`subway_station(persist=False)`로 끄면 `DATABASE_URL` 없이도 됩니다.

적재를 건드렸다면 **테이블이 없는 상태부터** 확인해야 합니다. 첫 실행에는 바꿔칠
대상이 없어 경로가 다릅니다. 그다음 여러 번 돌려 `tb_subway_station`과 `_prev`가
둘 다 1,098행인지, 인덱스 이름의 번호가 계속 올라가지 않는지 봅니다.

`normalize.py`를 건드렸다면 실행 로그의 "역명 접미사 제거"가 359, "노선명 표기
통일"이 66에서 움직이지 않는지, "시도를 채우지 못한 역" 경고가 없는지 봐야 합니다.

`picdot`, `monomansion`, `photogray`, `harufilm`, `photolabplus`, `broomstudio`는
`KAKAO_API_KEY`가 있어야 돕니다. 키가 없으면 `flows/common/kakao.py`의 `api_key()`가
`RuntimeError`로 막습니다.

적재를 건드렸다면 실행 결과가 아니라 적재물을 봐야 합니다. `make s3-ls`로 키가
빠짐없이 올라갔는지 보고, 같은 flow를 두 번 돌려 키 수가 늘지 않는지 확인합니다.
늘어난다면 파티션 경로에 실행마다 바뀌는 값이 섞인 것입니다.

`flows/common/`의 수집 모듈을 고쳤다면 그것을 쓰는 브랜드를 모두 돌려 건수가
전과 같은지 봐야 합니다. `imweb_map.py`는 인생네컷과 포토이즘, 돈룩업이 함께 씁니다.

파싱만 확인하고 싶으면 적재를 끕니다.

```bash
uv run --env-file .env python -c \
  "from flows.picdot_stores import picdot_stores; picdot_stores(persist=False)"
```

`pyproject.toml`의 `packages`를 건드렸다면 wheel 내용을 확인합니다.

```bash
make build
```

`deploy.py`나 `deployments/`를 건드렸다면 로컬 서버를 띄워 배포 사이클을 확인해야
합니다. 실제 스케줄을 다루는 코드라 import 성공만으로는 회귀를 잡을 수 없습니다.

```bash
PREFECT_HOME=/tmp/pf-test uv run prefect server start --host 127.0.0.1 --port 4301
export PREFECT_API_URL=http://127.0.0.1:4301/api
uv run prefect work-pool create neki-pool --type process
make deploy PORT=4301
```

검증에는 `PREFECT_HOME`을 임시 경로로 지정해 격리해야 합니다. 지정하지 않으면
`~/.prefect`의 실제 상태를 건드리게 되고, 테스트로 만든 deployment가 로컬 UI에
남습니다.

pause 관련 코드를 고쳤다면 양방향을 모두 확인해야 합니다. 끈 것이 배포 후에도
꺼져 있는지, 그리고 다시 켠 것을 배포가 도로 끄지는 않는지 둘 다 봐야 합니다.

## 로컬 UI

기본 프로파일이 `ephemeral`이라 `PREFECT_API_URL`이 비어 있습니다. 이 상태로
`serve.py`를 실행하면 프로세스 안에서 임시 서버가 뜨고 종료와 함께 사라지므로
**UI로 접근할 수 없습니다.** UI를 보려면 서버를 따로 띄우고 API 주소를 지정해야
합니다.

터미널 두 개가 필요합니다. `make serve`가 `PREFECT_API_URL`을 대신 넣어줍니다.

```bash
make server
make serve
```

UI는 `http://127.0.0.1:4200`이고, deployment 목록은 `/deployments`입니다.

## 작업 순서

team-neki 저장소이므로 코드를 건드리기 전에 Sprint 앱에 티켓을 먼저 만듭니다.
어느 에픽 하위에 둘지 불명확하면 임의로 정하지 않고 확인을 받아야 합니다.
