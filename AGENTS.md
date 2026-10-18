# AGENTS.md

이 저장소의 작업 안내입니다. 상세 규칙은 작업 대상에 맞는 문서만 읽습니다.

## 공통 규칙

- 파이프라인 정책의 정본은 `docs/spec/`입니다. 동작을 바꾸면 해당 spec을 같은 변경에서 먼저 고치고 코드를 맞춥니다.
- spec의 구현 위치 anchor(`경로:줄` `심볼`)가 밀리면 함께 고칩니다. `make spec-check`는 `make check`에 포함됩니다. 심볼이 사라졌다면 정책 본문을 고칩니다.
- 코드를 건드리기 전에 Sprint 앱에 티켓을 먼저 만듭니다. 에픽이 불명확하면 확인받습니다.

## 작업별 문서

| 작업 대상 | 먼저 읽을 문서 |
|---|---|
| 지점 수집(collect) | `docs/spec/collect-pipeline.md`, `docs/lld/pipeline-notes.md`의 수집 절 |
| 지점 보강(enrich) | `docs/spec/enrich-pipeline.md`, `docs/lld/pipeline-notes.md`의 보강 절 |
| 지점 동기화(stores-sync) | `docs/spec/stores-sync.md` |
| 검색 색인(search-index) | `docs/spec/search-index.md`, `docs/lld/pipeline-notes.md`의 검색 색인 절 |
| 법정동 | `docs/lld/legal-dong.md` |
| 지하철 역 | `docs/lld/subway-station.md` |
| 워크플로 구조·네이밍·패키지·Prefect API | `docs/lld/workflow-code.md` |
| 실행 모델 | `docs/hld/prefect-execution.md` |
| 배포·스케줄·로컬 UI | `docs/runbook/prefect-local.md`, `docs/runbook.md` |
| 변경 검증 | `docs/runbook/verification.md`; 수집 검증은 `docs/spec/collect-pipeline.md`의 변경 검증 절 |

정책과 구현 메모가 다르면 spec을 따릅니다.
