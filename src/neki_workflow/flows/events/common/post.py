"""브랜드 소식 원문 스키마. 기간 판정과 OCR 결과는 enrich가 만든다."""

from dataclasses import dataclass, field


@dataclass
class CollectedImage:
    image_order: int
    source_url: str
    s3_key: str | None = None
    sha256: str | None = None
    content_type: str | None = None
    byte_size: int | None = None


@dataclass
class CollectedPost:
    brand_id: int
    source_key: str
    external_id: str
    source_url: str
    observed_at: str
    raw_content: dict[str, str | None]
    title: str | None = None
    body: str | None = None
    published_at_raw: str | None = None
    frame_period_raw: str | None = None
    image_list_complete: bool | None = None
    images: list[CollectedImage] = field(default_factory=list)
    schema_version: str = "1"
    source_type: str = "website"
