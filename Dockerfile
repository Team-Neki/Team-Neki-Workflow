# 운영 이미지. worker 와 flow run(Job 파드)이 같은 이미지를 쓴다.
#
# 베이스의 prefect 는 3.8.5 다. pyproject 가 prefect==3.8.5 로 고정되어 있어
# uv.lock 을 그대로 설치해도 베이스의 prefect/prefect-kubernetes 가 교체되지
# 않는다. 베이스 태그를 올릴 때는 pyproject 의 prefect 버전도 같이 올린다.
FROM prefecthq/prefect:3.8.5-python3.13-kubernetes

# prefect 진입점이 시스템 파이썬을 쓰므로 .venv 를 만들지 않고 시스템에 설치한다.
# uv 는 베이스에 들어 있다. lock 파일은 런타임에 필요 없으므로 복사하지 않는다.
WORKDIR /opt/prefect
RUN --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    uv export --frozen --no-dev --no-emit-project -o /tmp/requirements.txt \
    && uv pip install --system --no-cache -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt

# to_deployment() 의 entrypoint 가 cwd 기준 상대경로(flows/<name>/flow.py:<fn>)라
# worker 와 Job 모두 여기서 실행돼야 한다. PYTHONPATH 는 prefect CLI 로 띄울 때도
# flows 를 찾게 한다. 파일은 root 소유 644 라 uid 1001 이 읽을 수 있다.
COPY deployments/ deployments/
COPY flows/ flows/
COPY deploy.py ./
ENV PYTHONPATH=/opt/prefect

# 이미지가 자기 참조를 알아야 deploy.py 가 Job 파드 이미지를 같은 태그로 지정한다.
ARG IMAGE_REF
ENV WORKFLOW_IMAGE=$IMAGE_REF
