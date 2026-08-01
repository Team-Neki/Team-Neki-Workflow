"""deployments/ 의 모든 스케줄을 한 프로세스로 서빙한다.

로컬 개발용이다. 이 프로세스가 스케줄러와 워커를 겸하므로 서버나
work pool 없이 바로 확인할 수 있다.

운영에는 쓰지 않는다. serve() 는 프로세스가 뜰 때마다 deployment 를
다시 등록하기 때문에, UI 에서 꺼둔 스케줄이 재시작할 때마다 되살아난다.
운영 배포는 deploy.py 를 쓴다.
"""

from prefect import serve

from deployments import collect


def main() -> None:
    found = list(collect())
    if not found:
        raise SystemExit("서빙할 deployment가 없습니다.")

    serve(*found)


if __name__ == "__main__":
    main()
