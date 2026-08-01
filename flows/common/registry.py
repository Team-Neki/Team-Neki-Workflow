"""이미지 태그를 다이제스트로 고정한다.

`:latest` 를 그대로 쓰면 최신은 보장되지만 무엇이 돌았는지 모른다. 어제 실패한
실행을 재현할 수 없고, 재시도 중간에 이미지가 바뀔 수도 있으며, 노드마다 캐시가
달라 같은 태그가 다른 것을 가리킬 수도 있다.

반대로 태그를 고정하면 재현은 되지만 새 배포가 반영되지 않는다.

그래서 실행 시점에 태그가 지금 가리키는 다이제스트를 조회해 `repo@sha256:...`
형태로 넘긴다. 최신을 쓰면서 실제로 무엇이 돌았는지가 로그와 이력에 남는다.
다이제스트는 불변이므로 재시도와 노드 간 불일치도 사라진다.
"""

import re
from typing import Any
from urllib.parse import urlparse

import httpx

DOCKER_HUB = "registry-1.docker.io"

# 매니페스트 목록(멀티 아키텍처)을 우선 받는다. 이것의 다이제스트를 넘겨야
# 노드가 자기 아키텍처에 맞는 것을 고른다.
ACCEPT = ", ".join(
    (
        "application/vnd.oci.image.index.v1+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.docker.distribution.manifest.v2+json",
    )
)

_BEARER = re.compile(r'(\w+)="([^"]*)"')


def parse(image: str) -> tuple[str, str, str]:
    """이미지 참조를 (레지스트리, 저장소, 참조)로 나눈다.

    첫 조각에 점이나 콜론이 있으면 레지스트리로 본다. 없으면 Docker Hub이고,
    슬래시가 없는 저장소는 공식 이미지이므로 library/ 를 붙인다.
    """
    remainder, _, digest = image.partition("@")

    head, _, tail = remainder.partition("/")
    if tail and ("." in head or ":" in head or head == "localhost"):
        registry, path = head, tail
    else:
        registry, path = DOCKER_HUB, remainder

    if registry == DOCKER_HUB and "/" not in path:
        path = f"library/{path}"

    if digest:
        return registry, path, f"sha256:{digest.removeprefix('sha256:')}"

    path, _, tag = path.partition(":")
    return registry, path, tag or "latest"


def _authorize(client: httpx.Client, challenge: str, registry: str, repo: str) -> dict[str, str]:
    """401 의 Bearer 안내를 읽어 토큰을 받는다. public 이면 익명으로 발급된다."""
    fields = dict(_BEARER.findall(challenge))
    realm = fields.get("realm")
    if not realm:
        return {}

    response = client.get(
        realm,
        params={
            "service": fields.get("service", registry),
            "scope": fields.get("scope", f"repository:{repo}:pull"),
        },
    )
    response.raise_for_status()

    token = response.json().get("token") or response.json().get("access_token")
    return {"Authorization": f"Bearer {token}"} if token else {}


def resolve(image: str, *, timeout: float = 20.0) -> str:
    """태그를 다이제스트로 바꾼다. 이미 다이제스트면 그대로 둔다."""
    registry, repo, reference = parse(image)

    if reference.startswith("sha256:"):
        return image

    url = f"https://{registry}/v2/{repo}/manifests/{reference}"
    headers = {"Accept": ACCEPT}

    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        response = client.head(url, headers=headers)

        if response.status_code == 401:
            challenge = response.headers.get("WWW-Authenticate", "")
            headers |= _authorize(client, challenge, registry, repo)
            response = client.head(url, headers=headers)

        response.raise_for_status()

        digest = response.headers.get("Docker-Content-Digest")

    if not digest:
        raise RuntimeError(
            f"{image}: 레지스트리가 Docker-Content-Digest 를 주지 않았습니다. "
            "태그를 그대로 쓰려면 pin_digest=False 로 두세요."
        )

    return f"{registry}/{repo}@{digest}"


def describe(image: str) -> dict[str, Any]:
    """로그에 남길 조각들. 어느 태그가 어느 다이제스트였는지 보여준다."""
    registry, repo, reference = parse(image)
    return {"registry": registry, "repository": repo, "reference": reference}
