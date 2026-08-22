#!/usr/bin/env python3
"""Collect point-based Ho Chi Minh City traffic-flow observations into CSV.

The collector uses TomTom's documented Flow Segment Data API.  Each configured
point is matched to the closest road fragment; the result is not an aggregate
for the full street.

The included GitHub Actions workflow runs one collection cycle every 10 minutes
and commits the updated local CSV to the repository. The script also retains two
output modes so it can still be used outside GitHub:

* Local mode appends observations to one CSV file.
* Cloud Run Job mode writes one immutable CSV object per scheduled execution to
  Google Cloud Storage. Set GCS_BUCKET (and optionally GCS_PREFIX) to enable it.

Cloud Run's filesystem is temporary, so cloud mode intentionally does not append
to a local file. Cloud Scheduler should invoke the job at the desired interval;
the default --samples=1 makes every job execution collect one batch and exit.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import math
import os
import random
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence


API_URL_TEMPLATE = (
    "https://api.tomtom.com/traffic/services/4/"
    "flowSegmentData/absolute/{zoom}/json"
)
API_KEY_ENV = "TOMTOM_API_KEY"
GCS_BUCKET_ENV = "GCS_BUCKET"
GCS_PREFIX_ENV = "GCS_PREFIX"
DEFAULT_GCS_PREFIX = "traffic-congestion/raw"
PROVIDER_NAME = "tomtom_flow_segment_v4"
HCMC_TIMEZONE = timezone(timedelta(hours=7), name="Asia/Ho_Chi_Minh")
RETRYABLE_HTTP_CODES = frozenset({429, 500, 502, 503, 504})
MIN_REPEATED_INTERVAL_SECONDS = 60.0


@dataclass(frozen=True)
class Location:
    """A WGS84 point used to select the nearest traffic-flow segment."""

    location_id: str
    street_name: str
    sample_description: str
    latitude: float
    longitude: float


@dataclass(frozen=True)
class FlowData:
    frc: str
    current_speed_kmph: float
    free_flow_speed_kmph: float
    current_travel_time_seconds: float
    free_flow_travel_time_seconds: float
    confidence: float
    road_closure: bool
    openlr: str
    coordinates: tuple[tuple[float, float], ...]


# Representative points on road sections, verified against OpenStreetMap in
# August 2026.  These are sampling locations, not claims that the full street
# has one uniform traffic state.  Use --locations-csv for project-specific
# points and add both carriageways when direction-level data matters.
DEFAULT_LOCATIONS: tuple[Location, ...] = (
    Location(
        "cong_hoa",
        "Cộng Hòa",
        "Mid-corridor representative section",
        10.801518,
        106.652097,
    ),
    Location(
        "truong_chinh",
        "Trường Chinh",
        "Mid-corridor representative section",
        10.819324,
        106.631220,
    ),
    Location(
        "dien_bien_phu",
        "Điện Biên Phủ",
        "Representative section near Hàng Xanh",
        10.801276,
        106.712832,
    ),
    Location(
        "nguyen_huu_canh",
        "Nguyễn Hữu Cảnh",
        "Representative section near Landmark 81",
        10.792699,
        106.718156,
    ),
    Location(
        "vo_nguyen_giap",
        "Võ Nguyên Giáp",
        "Representative eastern gateway section",
        10.801975,
        106.745974,
    ),
    Location(
        "nguyen_van_linh",
        "Nguyễn Văn Linh",
        "Representative section near Nguyễn Hữu Thọ",
        10.728215,
        106.699209,
    ),
    Location(
        "vo_van_kiet",
        "Võ Văn Kiệt",
        "Central-west representative section",
        10.750021,
        106.667447,
    ),
    Location(
        "cach_mang_thang_tam",
        "Cách Mạng Tháng Tám",
        "Representative central section",
        10.779471,
        106.678563,
    ),
    Location(
        "pham_van_dong",
        "Phạm Văn Đồng",
        "Representative north-eastern corridor section near Bình Thạnh",
        10.822810,
        106.701540,
    ),
    Location(
        "mai_chi_tho",
        "Mai Chí Thọ",
        "Representative eastern corridor section near An Lợi Đông",
        10.775210,
        106.727064,
    ),
    Location(
        "kinh_duong_vuong",
        "Kinh Dương Vương",
        "Representative western gateway section near Phú Lâm",
        10.749728,
        106.628556,
    ),
    Location(
        "nguyen_tat_thanh",
        "Nguyễn Tất Thành",
        "Representative southern port corridor section in District 4",
        10.762556,
        106.708299,
    ),
)


CSV_FIELDS = (
    "collection_id",
    "retrieved_at_utc",
    "retrieved_at_local",
    "provider",
    "location_id",
    "street_name",
    "sample_description",
    "query_latitude",
    "query_longitude",
    "status",
    "frc",
    "current_speed_kmph",
    "free_flow_speed_kmph",
    "speed_ratio",
    "speed_reduction_percent",
    "congestion_level",
    "current_travel_time_seconds",
    "free_flow_travel_time_seconds",
    "delay_seconds",
    "travel_time_delay_percent",
    "confidence",
    "road_closure",
    "openlr",
    "segment_start_latitude",
    "segment_start_longitude",
    "segment_end_latitude",
    "segment_end_longitude",
    "segment_point_count",
    "error_type",
    "error_message",
)


class CrawlerError(Exception):
    """Base error for configuration and provider failures."""


class ConfigurationError(CrawlerError):
    """Raised when local configuration is invalid."""


class ProviderError(CrawlerError):
    """A sanitized failure returned by, or while contacting, the provider."""

    def __init__(self, error_type: str, message: str) -> None:
        super().__init__(message)
        self.error_type = error_type


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _required_number(data: Mapping[str, Any], field: str) -> float:
    value = data.get(field)
    if not _is_number(value) or not math.isfinite(float(value)):
        raise ProviderError(
            "invalid_response", f"Response field {field!r} is missing or not numeric."
        )
    return float(value)


def _parse_coordinates(value: Any) -> tuple[tuple[float, float], ...]:
    if not isinstance(value, Mapping):
        raise ProviderError(
            "invalid_response", "Response field 'coordinates' is missing or invalid."
        )

    raw_points = value.get("coordinate", [])
    if isinstance(raw_points, Mapping):
        raw_points = [raw_points]
    if not isinstance(raw_points, list):
        raise ProviderError(
            "invalid_response", "Response coordinates.coordinate is not a list."
        )

    points: list[tuple[float, float]] = []
    for index, point in enumerate(raw_points):
        if not isinstance(point, Mapping):
            raise ProviderError(
                "invalid_response", f"Response coordinate {index} is not an object."
            )
        latitude = point.get("latitude")
        longitude = point.get("longitude")
        if (
            not _is_number(latitude)
            or not _is_number(longitude)
            or not math.isfinite(float(latitude))
            or not math.isfinite(float(longitude))
            or not -90.0 <= float(latitude) <= 90.0
            or not -180.0 <= float(longitude) <= 180.0
        ):
            raise ProviderError(
                "invalid_response", f"Response coordinate {index} is invalid."
            )
        points.append((float(latitude), float(longitude)))
    if not points:
        raise ProviderError(
            "invalid_response", "Provider returned an empty segment geometry."
        )
    return tuple(points)


def parse_flow_response(payload: Any) -> FlowData:
    """Validate and map a TomTom Flow Segment Data JSON response."""

    if not isinstance(payload, Mapping):
        raise ProviderError("invalid_response", "Provider response is not a JSON object.")
    data = payload.get("flowSegmentData")
    if not isinstance(data, Mapping):
        raise ProviderError(
            "invalid_response", "Provider response has no flowSegmentData object."
        )

    frc = data.get("frc")
    if not isinstance(frc, str) or not frc.strip():
        raise ProviderError(
            "invalid_response", "Response field 'frc' is missing or invalid."
        )
    road_closure = data.get("roadClosure")
    if not isinstance(road_closure, bool):
        raise ProviderError(
            "invalid_response", "Response field 'roadClosure' is not boolean."
        )
    openlr = data.get("openlr", "")
    if openlr is None:
        openlr = ""
    if not isinstance(openlr, str):
        raise ProviderError(
            "invalid_response", "Response field 'openlr' is not text."
        )

    flow = FlowData(
        frc=frc.strip(),
        current_speed_kmph=_required_number(data, "currentSpeed"),
        free_flow_speed_kmph=_required_number(data, "freeFlowSpeed"),
        current_travel_time_seconds=_required_number(data, "currentTravelTime"),
        free_flow_travel_time_seconds=_required_number(data, "freeFlowTravelTime"),
        confidence=_required_number(data, "confidence"),
        road_closure=road_closure,
        openlr=openlr,
        coordinates=_parse_coordinates(data.get("coordinates")),
    )
    if min(
        flow.current_speed_kmph,
        flow.free_flow_speed_kmph,
        flow.current_travel_time_seconds,
        flow.free_flow_travel_time_seconds,
    ) < 0:
        raise ProviderError(
            "invalid_response", "Provider response contains a negative speed or time."
        )
    if not 0.0 <= flow.confidence <= 1.0:
        raise ProviderError(
            "invalid_response", "Provider response confidence is outside 0..1."
        )
    return flow


def _sanitize_message(message: str, secret: str = "") -> str:
    """Keep credentials and multiline/oversized provider text out of logs/CSV."""

    sanitized = str(message)
    if secret:
        sanitized = sanitized.replace(secret, "[REDACTED]")
    sanitized = re.sub(
        r"(?i)([?&](?:key|api[_-]?key)=)[^&\s]+", r"\1[REDACTED]", sanitized
    )
    sanitized = " ".join(sanitized.split())
    return sanitized[:500]


def _provider_error_text(body: bytes) -> str:
    if not body:
        return "No response details."
    try:
        decoded = body.decode("utf-8", errors="replace")
        payload = json.loads(decoded)
    except (UnicodeError, json.JSONDecodeError):
        return "Provider returned a non-JSON error response."

    if isinstance(payload, Mapping):
        detailed = payload.get("detailedError")
        if isinstance(detailed, Mapping) and isinstance(detailed.get("message"), str):
            return detailed["message"]
        if isinstance(payload.get("error"), str):
            return payload["error"]
        if isinstance(payload.get("message"), str):
            return payload["message"]
    return "Provider returned an error response."


def _read_http_error_body(error: urllib.error.HTTPError) -> bytes:
    try:
        return error.read(8192)
    except (OSError, TimeoutError):
        return b""


def _retry_after_seconds(headers: Any) -> float | None:
    if headers is None:
        return None
    value = headers.get("Retry-After")
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        pass
    try:
        retry_at = parsedate_to_datetime(value)
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=timezone.utc)
        return max(0.0, (retry_at - datetime.now(timezone.utc)).total_seconds())
    except (TypeError, ValueError, OverflowError):
        return None


class JsonHttpClient:
    """Small retrying JSON client whose errors never expose the request URL."""

    def __init__(
        self,
        *,
        timeout_seconds: float,
        max_retries: int,
        opener: Callable[..., Any] = urllib.request.urlopen,
        sleeper: Callable[[float], None] = time.sleep,
        random_value: Callable[[], float] = random.random,
    ) -> None:
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self._opener = opener
        self._sleep = sleeper
        self._random = random_value

    def _wait_before_retry(self, attempt: int, retry_after: float | None) -> None:
        if retry_after is None:
            delay = min(30.0, (2**attempt) + self._random() * 0.25)
        else:
            delay = retry_after
        self._sleep(delay)

    def get_json(
        self, base_url: str, params: Mapping[str, str], *, secret: str
    ) -> Mapping[str, Any]:
        query = urllib.parse.urlencode(params)
        request = urllib.request.Request(
            f"{base_url}?{query}",
            headers={
                "Accept": "application/json",
                "User-Agent": "hcmc-traffic-csv-collector/1.0",
            },
            method="GET",
        )

        for attempt in range(self.max_retries + 1):
            try:
                with self._opener(request, timeout=self.timeout_seconds) as response:
                    body = response.read()
                try:
                    payload = json.loads(body.decode("utf-8"))
                except (UnicodeError, json.JSONDecodeError) as exc:
                    raise ProviderError(
                        "invalid_response", "Provider returned invalid JSON."
                    ) from exc
                if not isinstance(payload, Mapping):
                    raise ProviderError(
                        "invalid_response", "Provider JSON is not an object."
                    )
                return payload
            except urllib.error.HTTPError as exc:
                body = _read_http_error_body(exc)
                if exc.code in RETRYABLE_HTTP_CODES and attempt < self.max_retries:
                    self._wait_before_retry(
                        attempt, _retry_after_seconds(getattr(exc, "headers", None))
                    )
                    continue

                if exc.code in (401, 403):
                    error_type = "authentication_error"
                elif exc.code == 429:
                    error_type = "rate_limit_error"
                else:
                    error_type = f"http_{exc.code}"
                detail = _sanitize_message(_provider_error_text(body), secret)
                raise ProviderError(
                    error_type, f"Provider HTTP {exc.code}: {detail}"
                ) from exc
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                if attempt < self.max_retries:
                    self._wait_before_retry(attempt, None)
                    continue
                reason = getattr(exc, "reason", exc.__class__.__name__)
                detail = _sanitize_message(str(reason), secret)
                raise ProviderError(
                    "network_error", f"Network request failed: {detail}"
                ) from exc

        raise AssertionError("retry loop ended unexpectedly")


class TomTomFlowProvider:
    def __init__(self, api_key: str, http_client: JsonHttpClient, zoom: int) -> None:
        self._api_key = api_key
        self._http = http_client
        self._base_url = API_URL_TEMPLATE.format(zoom=zoom)

    def fetch(self, location: Location) -> FlowData:
        payload = self._http.get_json(
            self._base_url,
            {
                "key": self._api_key,
                "point": f"{location.latitude:.7f},{location.longitude:.7f}",
                "unit": "kmph",
                "openLr": "true",
            },
            secret=self._api_key,
        )
        return parse_flow_response(payload)


def _validate_location(location: Location, *, source: str) -> None:
    if not re.fullmatch(r"[A-Za-z0-9_-]+", location.location_id):
        raise ConfigurationError(
            f"{source}: location_id must contain only letters, digits, '_' or '-': "
            f"{location.location_id!r}"
        )
    if not location.street_name.strip():
        raise ConfigurationError(f"{source}: street_name cannot be empty.")
    if not math.isfinite(location.latitude) or not -90 <= location.latitude <= 90:
        raise ConfigurationError(f"{source}: latitude is outside -90..90.")
    if not math.isfinite(location.longitude) or not -180 <= location.longitude <= 180:
        raise ConfigurationError(f"{source}: longitude is outside -180..180.")


def validate_locations(locations: Sequence[Location], *, source: str) -> None:
    if not locations:
        raise ConfigurationError(f"{source}: no locations were configured.")
    seen: set[str] = set()
    for location in locations:
        _validate_location(location, source=source)
        if location.location_id in seen:
            raise ConfigurationError(
                f"{source}: duplicate location_id {location.location_id!r}."
            )
        seen.add(location.location_id)


def load_locations_csv(path: Path) -> tuple[Location, ...]:
    required = {"location_id", "street_name", "latitude", "longitude"}
    try:
        handle = path.open("r", encoding="utf-8-sig", newline="")
    except OSError as exc:
        raise ConfigurationError(f"Cannot open locations CSV {path}: {exc}") from exc

    with handle:
        reader = csv.DictReader(handle)
        headers = set(reader.fieldnames or ())
        missing = sorted(required - headers)
        if missing:
            raise ConfigurationError(
                f"Locations CSV {path} is missing columns: {', '.join(missing)}"
            )

        locations: list[Location] = []
        for line_number, row in enumerate(reader, start=2):
            try:
                latitude = float((row.get("latitude") or "").strip())
                longitude = float((row.get("longitude") or "").strip())
            except ValueError as exc:
                raise ConfigurationError(
                    f"{path}:{line_number}: latitude and longitude must be numbers."
                ) from exc
            locations.append(
                Location(
                    location_id=(row.get("location_id") or "").strip(),
                    street_name=(row.get("street_name") or "").strip(),
                    sample_description=(row.get("sample_description") or "").strip(),
                    latitude=latitude,
                    longitude=longitude,
                )
            )

    validate_locations(locations, source=str(path))
    return tuple(locations)


def _rounded(value: float | None, digits: int = 3) -> float | str:
    if value is None:
        return ""
    return round(value, digits)


def _derived_metrics(flow: FlowData) -> tuple[float | None, float | None, str]:
    if flow.road_closure:
        level = "closed"
    elif flow.free_flow_speed_kmph <= 0:
        level = "unknown"
    else:
        ratio = flow.current_speed_kmph / flow.free_flow_speed_kmph
        if ratio >= 0.90:
            level = "free_flow"
        elif ratio >= 0.75:
            level = "light"
        elif ratio >= 0.50:
            level = "moderate"
        elif ratio >= 0.25:
            level = "heavy"
        else:
            level = "severe"

    if flow.free_flow_speed_kmph > 0:
        speed_ratio = flow.current_speed_kmph / flow.free_flow_speed_kmph
        speed_reduction = max(0.0, (1.0 - speed_ratio) * 100.0)
    else:
        speed_ratio = None
        speed_reduction = None
    return speed_ratio, speed_reduction, level


def success_row(
    *,
    collection_id: str,
    retrieved_at: datetime,
    location: Location,
    flow: FlowData,
) -> dict[str, Any]:
    speed_ratio, speed_reduction, level = _derived_metrics(flow)
    delay_seconds = max(
        0.0, flow.current_travel_time_seconds - flow.free_flow_travel_time_seconds
    )
    if flow.free_flow_travel_time_seconds > 0:
        delay_percent: float | None = (
            delay_seconds / flow.free_flow_travel_time_seconds * 100.0
        )
    else:
        delay_percent = None

    start = flow.coordinates[0] if flow.coordinates else (None, None)
    end = flow.coordinates[-1] if flow.coordinates else (None, None)
    return {
        "collection_id": collection_id,
        "retrieved_at_utc": retrieved_at.astimezone(timezone.utc).isoformat(),
        "retrieved_at_local": retrieved_at.astimezone(HCMC_TIMEZONE).isoformat(),
        "provider": PROVIDER_NAME,
        "location_id": location.location_id,
        "street_name": location.street_name,
        "sample_description": location.sample_description,
        "query_latitude": f"{location.latitude:.7f}",
        "query_longitude": f"{location.longitude:.7f}",
        "status": "ok",
        "frc": flow.frc,
        "current_speed_kmph": _rounded(flow.current_speed_kmph),
        "free_flow_speed_kmph": _rounded(flow.free_flow_speed_kmph),
        "speed_ratio": _rounded(speed_ratio, 4),
        "speed_reduction_percent": _rounded(speed_reduction),
        "congestion_level": level,
        "current_travel_time_seconds": _rounded(flow.current_travel_time_seconds),
        "free_flow_travel_time_seconds": _rounded(flow.free_flow_travel_time_seconds),
        "delay_seconds": _rounded(delay_seconds),
        "travel_time_delay_percent": _rounded(delay_percent),
        "confidence": _rounded(flow.confidence, 4),
        "road_closure": str(flow.road_closure).lower(),
        "openlr": flow.openlr,
        "segment_start_latitude": _rounded(start[0], 7),
        "segment_start_longitude": _rounded(start[1], 7),
        "segment_end_latitude": _rounded(end[0], 7),
        "segment_end_longitude": _rounded(end[1], 7),
        "segment_point_count": len(flow.coordinates),
        "error_type": "",
        "error_message": "",
    }


def error_row(
    *,
    collection_id: str,
    retrieved_at: datetime,
    location: Location,
    error: ProviderError,
) -> dict[str, Any]:
    row = {field: "" for field in CSV_FIELDS}
    row.update(
        {
            "collection_id": collection_id,
            "retrieved_at_utc": retrieved_at.astimezone(timezone.utc).isoformat(),
            "retrieved_at_local": retrieved_at.astimezone(HCMC_TIMEZONE).isoformat(),
            "provider": PROVIDER_NAME,
            "location_id": location.location_id,
            "street_name": location.street_name,
            "sample_description": location.sample_description,
            "query_latitude": f"{location.latitude:.7f}",
            "query_longitude": f"{location.longitude:.7f}",
            "status": "error",
            "error_type": error.error_type,
            "error_message": _sanitize_message(str(error)),
        }
    )
    return row


class CsvSink:
    """Append observations while validating an existing file's schema."""

    def __init__(self, path: Path, *, overwrite: bool) -> None:
        self.path = path
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise ConfigurationError(
                f"Cannot create output directory {path.parent}: {exc}"
            ) from exc

        file_exists = path.exists() and path.stat().st_size > 0
        if file_exists and not overwrite:
            try:
                with path.open("r", encoding="utf-8", newline="") as existing:
                    header = next(csv.reader(existing), [])
            except OSError as exc:
                raise ConfigurationError(f"Cannot read output CSV {path}: {exc}") from exc
            if tuple(header) != CSV_FIELDS:
                raise ConfigurationError(
                    f"Output CSV {path} has a different header. Choose another file "
                    "or use --overwrite."
                )
            try:
                with path.open("rb") as existing_bytes:
                    existing_bytes.seek(-1, os.SEEK_END)
                    final_byte = existing_bytes.read(1)
            except OSError as exc:
                raise ConfigurationError(f"Cannot inspect output CSV {path}: {exc}") from exc
            if final_byte not in (b"\n", b"\r"):
                raise ConfigurationError(
                    f"Output CSV {path} does not end with a complete newline. Repair it "
                    "or choose another file before appending."
                )

        mode = "w" if overwrite else "a"
        try:
            self._handle = path.open(mode, encoding="utf-8", newline="")
        except OSError as exc:
            raise ConfigurationError(f"Cannot open output CSV {path}: {exc}") from exc
        self._writer = csv.DictWriter(self._handle, fieldnames=CSV_FIELDS)
        if overwrite or not file_exists:
            self._writer.writeheader()
            self._handle.flush()

    def write(self, row: Mapping[str, Any]) -> None:
        self._writer.writerow(row)
        self._handle.flush()

    def close(self) -> None:
        self._handle.close()

    def __enter__(self) -> "CsvSink":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


class GcsBatchSink:
    """Upload one complete collection batch as an immutable CSV object.

    Buffering one batch in memory is safe here because a scheduled execution
    normally contains only the configured location rows. The object is uploaded
    only after collection finishes, so incomplete CSV objects are not published.
    """

    def __init__(self, bucket_name: str, prefix: str) -> None:
        bucket_name = bucket_name.strip()
        if not bucket_name or bucket_name.startswith("gs://") or "/" in bucket_name:
            raise ConfigurationError(
                "GCS bucket must be a bucket name only, for example "
                "my-traffic-data; do not include gs:// or an object path."
            )
        self.bucket_name = bucket_name
        self.prefix = prefix.strip().strip("/") or DEFAULT_GCS_PREFIX
        self.path = f"gs://{self.bucket_name}/{self.prefix}/"
        self._rows: list[dict[str, Any]] = []
        self._closed = False

    def write(self, row: Mapping[str, Any]) -> None:
        if self._closed:
            raise ConfigurationError("Cannot write after the GCS sink is closed.")
        self._rows.append({field: row.get(field, "") for field in CSV_FIELDS})

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if not self._rows:
            return

        collection_ids = {str(row["collection_id"]) for row in self._rows}
        if len(collection_ids) != 1:
            raise ConfigurationError(
                "Cloud mode requires exactly one collection cycle per execution."
            )
        collection_id = next(iter(collection_ids))
        retrieved_date = str(self._rows[0]["retrieved_at_utc"])[:10]
        object_name = (
            f"{self.prefix}/date={retrieved_date}/{collection_id}.csv"
        )

        buffer = io.StringIO(newline="")
        writer = csv.DictWriter(
            buffer,
            fieldnames=CSV_FIELDS,
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(self._rows)

        try:
            from google.cloud import storage
        except ImportError as exc:
            raise ConfigurationError(
                "Cloud output requires the google-cloud-storage package. "
                "Install it with: python -m pip install google-cloud-storage"
            ) from exc

        try:
            client = storage.Client()
            blob = client.bucket(self.bucket_name).blob(object_name)
            blob.upload_from_string(
                buffer.getvalue().encode("utf-8"),
                content_type="text/csv; charset=utf-8",
                if_generation_match=0,
            )
        except Exception as exc:
            detail = _sanitize_message(str(exc))
            raise ConfigurationError(
                f"Cannot upload CSV batch to gs://{self.bucket_name}/{object_name}: "
                f"{detail}"
            ) from exc

        self.path = f"gs://{self.bucket_name}/{object_name}"

    def __enter__(self) -> "GcsBatchSink":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


def _new_collection_id(retrieved_at: datetime) -> str:
    stamp = retrieved_at.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{stamp}-{uuid.uuid4().hex[:10]}"


def _aware_timestamp(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value


def collect_once(
    *,
    provider: TomTomFlowProvider,
    locations: Sequence[Location],
    sink: CsvSink,
    request_delay_seconds: float,
    quiet: bool,
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    sleeper: Callable[[float], None] = time.sleep,
) -> tuple[int, int]:
    collection_started_at = _aware_timestamp(now())
    collection_id = _new_collection_id(collection_started_at)
    successes = 0
    failures = 0

    for index, location in enumerate(locations):
        stop_for_authentication = False
        if index and request_delay_seconds:
            sleeper(request_delay_seconds)
        try:
            flow = provider.fetch(location)
            retrieved_at = _aware_timestamp(now())
            row = success_row(
                collection_id=collection_id,
                retrieved_at=retrieved_at,
                location=location,
                flow=flow,
            )
            successes += 1
        except ProviderError as exc:
            retrieved_at = _aware_timestamp(now())
            row = error_row(
                collection_id=collection_id,
                retrieved_at=retrieved_at,
                location=location,
                error=exc,
            )
            failures += 1
            stop_for_authentication = exc.error_type == "authentication_error"
            if not quiet:
                print(
                    f"[{location.location_id}] {exc.error_type}: {exc}",
                    file=sys.stderr,
                )
        sink.write(row)

        if stop_for_authentication:
            skipped = ProviderError(
                "authentication_error",
                "Skipped after the provider rejected the API key.",
            )
            for remaining_location in locations[index + 1 :]:
                sink.write(
                    error_row(
                        collection_id=collection_id,
                        retrieved_at=_aware_timestamp(now()),
                        location=remaining_location,
                        error=skipped,
                    )
                )
                failures += 1
            break

    if not quiet:
        print(
            f"{collection_id}: wrote {successes} successful and {failures} error "
            f"rows to {sink.path}",
            file=sys.stderr,
        )
    return successes, failures


def _nonnegative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be at least 0")
    return parsed


def _nonnegative_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a number") from exc
    if not math.isfinite(parsed) or parsed < 0:
        raise argparse.ArgumentTypeError("must be a finite number at least 0")
    return parsed


def _positive_float(value: str) -> float:
    parsed = _nonnegative_float(value)
    if parsed == 0:
        raise argparse.ArgumentTypeError("must be greater than 0")
    return parsed


def _same_path(first: Path, second: Path) -> bool:
    """Compare existing or prospective paths safely across platforms."""

    try:
        first_resolved = first.resolve(strict=False)
        second_resolved = second.resolve(strict=False)
    except OSError:
        first_resolved = first.absolute()
        second_resolved = second.absolute()
    return os.path.normcase(str(first_resolved)) == os.path.normcase(str(second_resolved))


def _next_cycle_deadline(previous_deadline: float, interval: float, now: float) -> float:
    """Keep normal cadence but skip stale deadlines after an overrun/suspend."""

    scheduled = previous_deadline + interval
    return scheduled if scheduled > now else now + interval


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Collect current traffic flow for representative Ho Chi Minh City "
            "road points and append it to a CSV dataset."
        )
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("traffic_congestion.csv"),
        help=(
            "local output CSV path (default: traffic_congestion.csv); ignored when "
            "--gcs-bucket or GCS_BUCKET is set"
        ),
    )
    parser.add_argument(
        "--gcs-bucket",
        default=os.environ.get(GCS_BUCKET_ENV, "").strip(),
        help=(
            "Google Cloud Storage bucket name for Cloud Run mode; may also be "
            f"provided through {GCS_BUCKET_ENV}"
        ),
    )
    parser.add_argument(
        "--gcs-prefix",
        default=os.environ.get(GCS_PREFIX_ENV, DEFAULT_GCS_PREFIX).strip(),
        help=(
            "Cloud Storage object prefix "
            f"(default: {DEFAULT_GCS_PREFIX}; environment: {GCS_PREFIX_ENV})"
        ),
    )
    parser.add_argument(
        "--locations-csv",
        type=Path,
        help=(
            "custom location points; columns: location_id, street_name, "
            "latitude, longitude, and optional sample_description"
        ),
    )
    parser.add_argument(
        "--samples",
        type=_nonnegative_int,
        default=1,
        help="number of collection cycles; 0 means until interrupted (default: 1)",
    )
    parser.add_argument(
        "--interval-seconds",
        type=_positive_float,
        default=1200.0,
        help="seconds between repeated cycle start times (default: 1200; minimum: 60)",
    )
    parser.add_argument(
        "--request-delay-seconds",
        type=_nonnegative_float,
        default=0.15,
        help="delay between street requests within a cycle (default: 0.15)",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=_positive_float,
        default=15.0,
        help="timeout for each HTTP attempt (default: 15)",
    )
    parser.add_argument(
        "--max-retries",
        type=_nonnegative_int,
        default=3,
        help="retries for rate limits, server errors, and network errors (default: 3)",
    )
    parser.add_argument(
        "--zoom",
        type=int,
        choices=range(0, 23),
        default=18,
        metavar="0..22",
        help="TomTom road-detail zoom level (default: 18)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="replace the output file instead of appending",
    )
    parser.add_argument(
        "--list-locations",
        action="store_true",
        help="print configured points and exit without requiring an API key",
    )
    parser.add_argument("--quiet", action="store_true", help="suppress progress messages")
    return parser


def _print_locations(locations: Sequence[Location]) -> None:
    writer = csv.writer(sys.stdout, lineterminator="\n")
    writer.writerow(
        [
            "location_id",
            "street_name",
            "sample_description",
            "latitude",
            "longitude",
        ]
    )
    for location in locations:
        writer.writerow(
            [
                location.location_id,
                location.street_name,
                location.sample_description,
                f"{location.latitude:.7f}",
                f"{location.longitude:.7f}",
            ]
        )


def _configure_text_streams() -> None:
    """Use UTF-8 for Vietnamese names, including on legacy Windows consoles."""

    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            try:
                reconfigure(encoding="utf-8", errors="replace")
            except (OSError, ValueError):
                pass


def main(argv: Sequence[str] | None = None) -> int:
    _configure_text_streams()
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        if args.locations_csv and _same_path(args.locations_csv, args.output):
            raise ConfigurationError(
                "--locations-csv and --output must refer to different files."
            )

        locations = (
            load_locations_csv(args.locations_csv)
            if args.locations_csv
            else DEFAULT_LOCATIONS
        )
        location_source = str(args.locations_csv) if args.locations_csv else "built-in locations"
        validate_locations(locations, source=location_source)

        if args.list_locations:
            _print_locations(locations)
            return 0

        if args.samples != 1 and args.interval_seconds < MIN_REPEATED_INTERVAL_SECONDS:
            raise ConfigurationError(
                "Repeated collection requires --interval-seconds >= 60 because the "
                "traffic source refreshes once per minute."
            )

        cloud_mode = bool(args.gcs_bucket)
        if cloud_mode and args.samples != 1:
            raise ConfigurationError(
                "Cloud Run mode requires --samples 1. Let Cloud Scheduler start a "
                "new job execution for every collection interval."
            )
        if cloud_mode and args.overwrite:
            raise ConfigurationError(
                "--overwrite is only available for local CSV output. Cloud mode "
                "creates one immutable object per collection."
            )

        api_key = os.environ.get(API_KEY_ENV, "").strip()
        if not api_key:
            raise ConfigurationError(
                f"Set the {API_KEY_ENV} environment variable to a TomTom API key. "
                "See README.md for setup instructions."
            )

        http_client = JsonHttpClient(
            timeout_seconds=args.timeout_seconds,
            max_retries=args.max_retries,
        )
        provider = TomTomFlowProvider(api_key, http_client, args.zoom)

        cycles_completed = 0
        any_failure = False
        any_success = False
        next_cycle = time.monotonic()
        sink_context = (
            GcsBatchSink(args.gcs_bucket, args.gcs_prefix)
            if cloud_mode
            else CsvSink(args.output, overwrite=args.overwrite)
        )
        with sink_context as sink:
            while args.samples == 0 or cycles_completed < args.samples:
                if cycles_completed:
                    remaining = next_cycle - time.monotonic()
                    if remaining > 0:
                        time.sleep(remaining)

                successes, failures = collect_once(
                    provider=provider,
                    locations=locations,
                    sink=sink,
                    request_delay_seconds=args.request_delay_seconds,
                    quiet=args.quiet,
                )
                any_success = any_success or successes > 0
                any_failure = any_failure or failures > 0
                cycles_completed += 1
                current_time = time.monotonic()
                next_cycle = _next_cycle_deadline(
                    next_cycle, args.interval_seconds, current_time
                )
    except KeyboardInterrupt:
        if not getattr(args, "quiet", False):
            print("Interrupted; completed CSV rows were flushed.", file=sys.stderr)
        return 130
    except ConfigurationError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    # Local mode preserves the original strict exit status. In Cloud Run mode,
    # a partial batch is considered successful once it is stored; an all-error
    # batch still fails so the job's retry/alerting policy can react.
    if any_failure and (not cloud_mode or not any_success):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
