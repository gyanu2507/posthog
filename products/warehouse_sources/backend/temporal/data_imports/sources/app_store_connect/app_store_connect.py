import io
import re
import csv
import gzip
import time
import hashlib
import tempfile
import dataclasses
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from typing import IO, Any, Literal, Optional
from urllib.parse import urlsplit

import jwt
import requests
from structlog.types import FilteringBoundLogger

from products.warehouse_sources.backend.temporal.data_imports.sources.app_store_connect.settings import (
    ANALYTICS_GRANULARITY,
    ANALYTICS_MAX_INSTANCES_PER_RUN,
    APP_STORE_CONNECT_ENDPOINTS,
    MAX_PAGE_SIZE,
    SALES_REPORT_END_OFFSET_DAYS,
    SALES_REPORT_LOOKBACK_DAYS,
    SALES_REPORT_MAX_DAYS_PER_RUN,
    AppStoreConnectEndpointConfig,
)
from products.warehouse_sources.backend.temporal.data_imports.sources.common.http import make_tracked_session
from products.warehouse_sources.backend.temporal.data_imports.sources.common.resumable import ResumableSourceManager
from products.warehouse_sources.backend.temporal.data_imports.sources.common.typings import SourceResponse

BASE_URL = "https://api.appstoreconnect.apple.com"
API_HOST = "api.appstoreconnect.apple.com"

# Apple rejects a token whose `exp` is more than 20 minutes past `iat`, so mint just under the ceiling
# and re-mint while a couple of minutes still remain.
JWT_AUDIENCE = "appstoreconnect-v1"
JWT_LIFETIME_SECONDS = 1140
JWT_REFRESH_MARGIN_SECONDS = 120

REQUEST_TIMEOUT_SECONDS = 60
# Report bodies are whole files rather than a page of JSON.
REPORT_TIMEOUT_SECONDS = 300
CREDENTIALS_TIMEOUT_SECONDS = 15

# `/v1/salesReports` returns a gzipped TSV, not JSON, and only for this Accept type.
REPORT_ACCEPT = "application/a-gzip"

# Hard cap on pages walked for one collection (or one app's collection) so a pagination bug can't scan
# forever. At 200 rows a page that is 400k rows.
MAX_PAGES_PER_RESOURCE = 2000

# Analytics segment downloads are presigned object-store URLs on Apple's storage, not the API origin,
# and they expire about five minutes after the segments call. The Apple bearer token must never be
# sent to them, and the host allowlist keeps a tampered URL from pointing the download at an
# arbitrary or internal host.
ANALYTICS_SEGMENT_HOST_SUFFIXES = (".amazonaws.com", ".apple.com", ".mzstatic.com")

# In-memory cap while downloading one analytics segment; larger segments spool to disk so the
# checksum can be verified before any row is parsed, without holding whole files in memory.
ANALYTICS_SEGMENT_SPOOL_BYTES = 32 * 1024 * 1024

ANALYTICS_ROWS_PER_BATCH = 2000

_PEM_HEADER = "-----BEGIN PRIVATE KEY-----"
_PEM_FOOTER = "-----END PRIVATE KEY-----"
_NON_ALNUM = re.compile(r"[^0-9a-z]+")


class AppStoreConnectAuthError(Exception):
    """The .p8 private key, key ID, or issuer ID can't produce a signed token."""


class AppStoreConnectUrlError(Exception):
    """A request or pagination URL points somewhere other than the App Store Connect API origin."""


def _require_api_url(url: str) -> str:
    """Reject any URL that isn't ``https://api.appstoreconnect.apple.com`` on the default HTTPS port.

    ``links.next`` cursors from a response body and resume URLs loaded from persisted state are both
    attacker-influenceable: a tampered API response or a poisoned checkpoint could otherwise point the
    next request — which carries a freshly minted, replayable Apple bearer token — at an arbitrary host.
    Pinning every outbound request to Apple's origin makes a stray URL fail closed.
    """
    try:
        parts = urlsplit(url)
    except Exception as e:
        raise AppStoreConnectUrlError(f"Unparseable App Store Connect URL: {url!r}") from e

    if parts.scheme != "https" or parts.hostname != API_HOST or parts.port not in (None, 443):
        raise AppStoreConnectUrlError(f"Refusing to request a non-App Store Connect URL: {url!r}")
    return url


@dataclasses.dataclass
class AppStoreConnectResumeConfig:
    # Fan-out bookmark: the app currently being walked. A stable Apple id rather than a positional
    # index, so apps added or removed between a crash and the retry can't resume into the wrong app.
    app_id: str | None = None
    # Fully-formed `links.next` URL (carries Apple's opaque cursor) for the collection being walked.
    next_url: str | None = None
    # Report streams bookmark: the next report date to fetch, as `YYYY-MM-DD`.
    report_date: str | None = None
    # Analytics streams bookmark: the next instance processing date to fetch, as
    # `YYYY-MM-DD`. Dates are walked ascending across every app, so no app bookmark is
    # needed. Optional so states saved before this field existed still parse.
    processing_date: str | None = None
    # Whether this job's first attempt decided to ingest the one-time historical snapshot.
    # Pipelines can persist the incremental watermark per batch, so a retried attempt of a first
    # sync may arrive with a watermark even though the snapshot hasn't fully landed — the recorded
    # decision keeps the retry from silently dropping the history. Optional so states saved before
    # this field existed still parse.
    include_snapshot: bool | None = None


def _normalize_private_key(private_key: str) -> str:
    """Coerce a pasted App Store Connect .p8 key into PEM.

    Pastes arrive three ways: real PEM, PEM whose newlines were flattened into literal ``\\n``, or just
    the base64 body with the header and footer stripped off.
    """
    key = (private_key or "").strip().replace("\\n", "\n").replace("\r\n", "\n")
    if not key:
        raise AppStoreConnectAuthError("Add the contents of your App Store Connect .p8 private key file.")
    if "-----BEGIN" in key:
        return key

    body = "".join(key.split())
    lines = [body[index : index + 64] for index in range(0, len(body), 64)]
    return "\n".join([_PEM_HEADER, *lines, _PEM_FOOTER]) + "\n"


class AppStoreConnectTokenProvider:
    """Mints and caches the short-lived ES256 JWT every App Store Connect request carries."""

    def __init__(self, issuer_id: str, key_id: str, private_key: str) -> None:
        self._issuer_id = issuer_id
        self._key_id = key_id
        self._private_key = _normalize_private_key(private_key)
        self._token: str | None = None
        self._expires_at: float = 0.0

    def token(self, force_refresh: bool = False) -> str:
        now = time.time()
        if force_refresh or self._token is None or now >= self._expires_at - JWT_REFRESH_MARGIN_SECONDS:
            self._token = self._mint(int(now))
            self._expires_at = now + JWT_LIFETIME_SECONDS
        return self._token

    def _mint(self, issued_at: int) -> str:
        payload: dict[str, Any] = {
            "iss": self._issuer_id,
            "iat": issued_at,
            "exp": issued_at + JWT_LIFETIME_SECONDS,
            "aud": JWT_AUDIENCE,
        }
        try:
            return jwt.encode(payload, self._private_key, algorithm="ES256", headers={"kid": self._key_id})
        except Exception as e:
            raise AppStoreConnectAuthError(
                "Could not sign a token with that private key. Paste the whole contents of the .p8 file "
                "you downloaded from App Store Connect, including the BEGIN and END lines."
            ) from e


def _make_session(private_key: str, capture: bool = True) -> requests.Session:
    # The private key itself is never sent — only the signature it produces — but redact it so a future
    # change can't leak it into a captured sample. Redirects stay off so a 3xx can't quietly forward a
    # bearer-token-bearing request to another host; `_get` treats any redirect as a failure.
    # `capture=False` keeps a session's responses out of HTTP sample capture, for calls whose bodies
    # carry values the name-based scrubbers can't recognise (the presigned analytics segment URLs).
    return make_tracked_session(redact_values=(private_key,), allow_redirects=False, capture=capture)


def _make_segment_download_session(presigned_query: str) -> requests.Session:
    # The presigned query string is a short-lived credential: redact it so the request log
    # line can't carry a usable signature, and keep the bulk report body out of sample capture.
    return make_tracked_session(
        redact_values=(presigned_query,) if presigned_query else (),
        allow_redirects=False,
        capture=False,
    )


def _get(
    session: requests.Session,
    url: str,
    *,
    token_provider: AppStoreConnectTokenProvider,
    logger: FilteringBoundLogger,
    params: dict[str, Any] | None = None,
    accept: str = "application/json",
    timeout: int = REQUEST_TIMEOUT_SECONDS,
    tolerate: tuple[int, ...] = (),
) -> requests.Response:
    """GET with a freshly-valid token. 429 and transient 5xx are already retried by the tracked adapter."""

    # Pin the target to Apple's origin before attaching a token — covers freshly built URLs, `links.next`
    # cursors, and resume URLs alike, since every request funnels through here.
    _require_api_url(url)

    def _send(token: str) -> requests.Response:
        return session.get(
            url,
            params=params,
            headers={"Authorization": f"Bearer {token}", "Accept": accept},
            timeout=timeout,
        )

    response = _send(token_provider.token())
    if response.status_code == 401:
        # Tokens live 20 minutes and a long sync outlives one. Forcing a single re-mint separates a
        # merely stale token from a genuinely bad key, which stays a 401 and fails non-retryably.
        response = _send(token_provider.token(force_refresh=True))

    if 300 <= response.status_code < 400:
        # Redirects are pinned off on the session, so a 3xx is Apple's origin (or something posing as it)
        # trying to forward the request elsewhere. Fail closed rather than chase it with a live token.
        logger.error(f"App Store Connect unexpected redirect: status={response.status_code}, url={url}")
        raise AppStoreConnectUrlError(f"Unexpected redirect from App Store Connect: {url!r}")

    if response.status_code in tolerate:
        return response

    if not response.ok:
        logger.error(
            f"App Store Connect API error: status={response.status_code}, body={response.text[:500]}, url={url}"
        )
        response.raise_for_status()

    return response


def _flatten_resource(resource: dict[str, Any]) -> dict[str, Any]:
    """Lift a JSON:API resource's ``attributes`` to the row root alongside its ``id`` and ``type``.

    ``relationships`` is dropped: it holds link envelopes rather than data, and the related rows are
    already available as their own tables.
    """
    attributes = resource.get("attributes")
    row: dict[str, Any] = dict(attributes) if isinstance(attributes, dict) else {}
    row["id"] = resource.get("id")
    row["type"] = resource.get("type")
    return row


@dataclasses.dataclass(frozen=True, kw_only=True)
class _Page:
    """One JSON:API page. ``resources`` and ``included`` share a type, so construction is
    keyword-only to keep a caller from silently swapping them."""

    resources: list[dict[str, Any]]
    included: list[dict[str, Any]]
    next_url: str | None


def _iter_pages(
    session: requests.Session,
    token_provider: AppStoreConnectTokenProvider,
    logger: FilteringBoundLogger,
    url: str,
    params: dict[str, Any] | None,
) -> Iterator[_Page]:
    """Walk a JSON:API collection, yielding each page's ``data`` and ``included`` resources plus the
    next-page URL (``None`` at the end).

    ``params is None`` means ``url`` is already a fully-formed ``links.next`` — re-sending params there
    would duplicate the limit and cursor query args.
    """
    page_params: dict[str, Any] | None = {**params, "limit": MAX_PAGE_SIZE} if params is not None else None
    pages = 0

    while True:
        body = _get(session, url, token_provider=token_provider, logger=logger, params=page_params).json()
        data = body.get("data") if isinstance(body, dict) else None
        included = body.get("included") if isinstance(body, dict) else None

        links = body.get("links") if isinstance(body, dict) else None
        next_url = links.get("next") if isinstance(links, dict) else None

        pages += 1
        if pages >= MAX_PAGES_PER_RESOURCE and next_url:
            logger.warning(f"App Store Connect: page cap reached, truncating collection. url={url}, pages={pages}")
            next_url = None

        yield _Page(
            resources=[resource for resource in (data or []) if isinstance(resource, dict)],
            included=[resource for resource in (included or []) if isinstance(resource, dict)],
            next_url=next_url,
        )

        if not next_url:
            return

        url = next_url
        page_params = None


def _page_rows(config: AppStoreConnectEndpointConfig, page: _Page) -> list[dict[str, Any]]:
    """Rows for one page: the flattened ``data`` resources, or, for endpoints configured to read a
    related resource off another collection's pages, the flattened ``included`` resources of that type.
    """
    if config.rows_from_included_type is None:
        return [_flatten_resource(resource) for resource in page.resources]

    # JSON:API full linkage guarantees every included resource is referenced from a primary
    # resource's relationship linkage; that linkage is where each row's parent id comes from.
    parent_ids: dict[str, str] = {}
    for resource in page.resources:
        relationships = resource.get("relationships")
        if not isinstance(relationships, dict) or resource.get("id") is None:
            continue
        for relationship in relationships.values():
            linkage = relationship.get("data") if isinstance(relationship, dict) else None
            if (
                isinstance(linkage, dict)
                and linkage.get("type") == config.rows_from_included_type
                and linkage.get("id") is not None
            ):
                parent_ids[str(linkage["id"])] = str(resource["id"])

    rows: list[dict[str, Any]] = []
    for resource in page.included:
        if resource.get("type") != config.rows_from_included_type:
            continue
        row = _flatten_resource(resource)
        row[config.included_parent_column] = parent_ids.get(str(resource.get("id")))
        rows.append(row)
    return rows


def _load_resume(
    manager: ResumableSourceManager[AppStoreConnectResumeConfig],
) -> AppStoreConnectResumeConfig | None:
    return manager.load_state() if manager.can_resume() else None


def _list_app_ids(
    session: requests.Session,
    token_provider: AppStoreConnectTokenProvider,
    logger: FilteringBoundLogger,
) -> list[str]:
    app_ids: list[str] = []
    for page in _iter_pages(session, token_provider, logger, f"{BASE_URL}/v1/apps", {}):
        app_ids.extend(str(resource["id"]) for resource in page.resources if resource.get("id"))
    return app_ids


def _get_collection(
    session: requests.Session,
    config: AppStoreConnectEndpointConfig,
    token_provider: AppStoreConnectTokenProvider,
    logger: FilteringBoundLogger,
    manager: ResumableSourceManager[AppStoreConnectResumeConfig],
) -> Iterator[list[dict[str, Any]]]:
    resume = _load_resume(manager)
    resumed_url = resume.next_url if resume is not None else None

    url = resumed_url or f"{BASE_URL}{config.path}"
    params: dict[str, Any] | None = None if resumed_url else dict(config.params)

    for page in _iter_pages(session, token_provider, logger, url, params):
        rows = _page_rows(config, page)
        if rows:
            yield rows
        # Save AFTER yielding so a crash re-fetches the page we just emitted rather than skipping it;
        # merge dedupes the re-pulled rows on the primary key.
        if page.next_url:
            manager.save_state(AppStoreConnectResumeConfig(next_url=page.next_url))


def _get_app_fanout(
    session: requests.Session,
    config: AppStoreConnectEndpointConfig,
    token_provider: AppStoreConnectTokenProvider,
    logger: FilteringBoundLogger,
    manager: ResumableSourceManager[AppStoreConnectResumeConfig],
) -> Iterator[list[dict[str, Any]]]:
    app_ids = _list_app_ids(session, token_provider, logger)
    resume = _load_resume(manager)

    start = 0
    resumed_url: str | None = None
    if resume is not None and resume.app_id:
        index = next((i for i, app_id in enumerate(app_ids) if app_id == resume.app_id), None)
        # A bookmarked app that no longer exists restarts the fan-out; merge dedupes the re-pulled rows.
        if index is not None:
            start = index
            resumed_url = resume.next_url

    for position in range(start, len(app_ids)):
        app_id = app_ids[position]
        if position == start and resumed_url:
            url: str = resumed_url
            params: dict[str, Any] | None = None
        else:
            url = f"{BASE_URL}{config.path.format(app_id=app_id)}"
            params = dict(config.params)

        for page in _iter_pages(session, token_provider, logger, url, params):
            rows = _page_rows(config, page)
            if rows:
                for row in rows:
                    row["app_id"] = app_id
                yield rows

            if page.next_url:
                manager.save_state(AppStoreConnectResumeConfig(app_id=app_id, next_url=page.next_url))
            elif position + 1 < len(app_ids):
                manager.save_state(AppStoreConnectResumeConfig(app_id=app_ids[position + 1]))


def _to_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        aware = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return aware.astimezone(UTC).date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _normalize_report_column(name: str) -> str:
    """Turn an Apple report header (``Developer Proceeds``) into a column name (``developer_proceeds``)."""
    slug = _NON_ALNUM.sub("_", name.strip().lower()).strip("_")
    return slug or "column"


def _decompress_report(payload: bytes) -> str:
    try:
        raw = gzip.decompress(payload)
    except (OSError, EOFError):
        # urllib3 already unwraps a `Content-Encoding: gzip` body, so the payload can arrive as plain TSV.
        raw = payload
    return raw.decode("utf-8-sig", errors="replace")


def _parse_report(payload: bytes, report_date: date) -> list[dict[str, Any]]:
    reader = csv.reader(io.StringIO(_decompress_report(payload)), delimiter="\t")
    try:
        header = next(reader)
    except StopIteration:
        return []

    columns = [_normalize_report_column(column) for column in header]
    report_date_str = report_date.isoformat()
    rows: list[dict[str, Any]] = []

    for values in reader:
        if not any(value.strip() for value in values):
            continue
        row: dict[str, Any] = {
            column: (values[index] if index < len(values) else None) for index, column in enumerate(columns)
        }
        row["report_date"] = report_date_str
        # 1-based position in the file. A published day's report is immutable, so (report_date, _line)
        # is a stable unique key and re-reading a day merges instead of duplicating.
        row["_line"] = len(rows) + 1
        rows.append(row)

    return rows


def _fetch_report(
    session: requests.Session,
    config: AppStoreConnectEndpointConfig,
    token_provider: AppStoreConnectTokenProvider,
    logger: FilteringBoundLogger,
    vendor_number: str,
    report_date: date,
) -> list[dict[str, Any]]:
    params: dict[str, str] = {
        "filter[frequency]": config.report_frequency,
        "filter[reportDate]": report_date.isoformat(),
        "filter[reportType]": config.report_type,
        "filter[reportSubType]": config.report_sub_type,
        "filter[vendorNumber]": vendor_number,
    }
    if config.report_version:
        params["filter[version]"] = config.report_version

    response = _get(
        session,
        f"{BASE_URL}/v1/salesReports",
        token_provider=token_provider,
        logger=logger,
        params=params,
        accept=REPORT_ACCEPT,
        timeout=REPORT_TIMEOUT_SECONDS,
        tolerate=config.missing_report_status_codes,
    )
    if response.status_code in config.missing_report_status_codes:
        # Apple 404s any date with no activity at all — normal for quiet days and for dates before the
        # app shipped — so a missing day is not an error. Subscription-family report types 400 for the
        # same condition instead (see `missing_report_status_codes`).
        return []

    return _parse_report(response.content, report_date)


def _get_sales_report(
    session: requests.Session,
    config: AppStoreConnectEndpointConfig,
    token_provider: AppStoreConnectTokenProvider,
    logger: FilteringBoundLogger,
    manager: ResumableSourceManager[AppStoreConnectResumeConfig],
    vendor_number: str | None,
    should_use_incremental_field: bool,
    db_incremental_field_last_value: Any,
) -> Iterator[list[dict[str, Any]]]:
    if not vendor_number:
        raise ValueError(
            "Syncing App Store Connect sales reports needs your vendor number. "
            "Add it in the source settings, then run the sync again."
        )

    today = datetime.now(UTC).date()
    end = today - timedelta(days=SALES_REPORT_END_OFFSET_DAYS)
    start = today - timedelta(days=SALES_REPORT_LOOKBACK_DAYS)

    if should_use_incremental_field:
        watermark = _to_date(db_incremental_field_last_value)
        if watermark is not None:
            # Start on the watermark day itself rather than the day after: the file for a published day
            # never changes, so re-reading it merges idempotently and a half-written day self-heals.
            start = max(start, min(watermark, end))

    resume = _load_resume(manager)
    if resume is not None and resume.report_date:
        resumed = _to_date(resume.report_date)
        if resumed is not None and start <= resumed <= end:
            start = resumed

    report_date = start
    days_fetched = 0
    while report_date <= end and days_fetched < SALES_REPORT_MAX_DAYS_PER_RUN:
        rows = _fetch_report(session, config, token_provider, logger, vendor_number, report_date)
        if rows:
            yield rows

        days_fetched += 1
        report_date += timedelta(days=1)
        if report_date <= end:
            manager.save_state(AppStoreConnectResumeConfig(report_date=report_date.isoformat()))

    if report_date <= end:
        logger.info(
            f"App Store Connect: hit the per-run report day cap, resuming later. "
            f"endpoint={config.name}, next_report_date={report_date.isoformat()}"
        )


def _require_segment_url(url: str) -> str:
    """Allow only https URLs on Apple's storage hosts for analytics segment downloads."""
    try:
        parts = urlsplit(url)
    except Exception as e:
        raise AppStoreConnectUrlError(f"Unparseable analytics segment URL: {url!r}") from e

    hostname = parts.hostname or ""
    if parts.scheme != "https" or not hostname.endswith(ANALYTICS_SEGMENT_HOST_SUFFIXES):
        raise AppStoreConnectUrlError(f"Refusing to download an analytics segment from: {url!r}")
    return url


def _post_json(
    session: requests.Session,
    url: str,
    *,
    token_provider: AppStoreConnectTokenProvider,
    logger: FilteringBoundLogger,
    payload: dict[str, Any],
    tolerate: tuple[int, ...] = (),
) -> requests.Response:
    _require_api_url(url)

    def _send(token: str) -> requests.Response:
        return session.post(
            url,
            json=payload,
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )

    response = _send(token_provider.token())
    if response.status_code == 401:
        response = _send(token_provider.token(force_refresh=True))

    if 300 <= response.status_code < 400:
        logger.error(f"App Store Connect unexpected redirect: status={response.status_code}, url={url}")
        raise AppStoreConnectUrlError(f"Unexpected redirect from App Store Connect: {url!r}")

    if response.status_code in tolerate:
        return response

    if not response.ok:
        logger.error(
            f"App Store Connect API error: status={response.status_code}, body={response.text[:500]}, url={url}"
        )
        response.raise_for_status()

    return response


ONGOING_ACCESS_TYPE = "ONGOING"
SNAPSHOT_ACCESS_TYPE = "ONE_TIME_SNAPSHOT"


def _list_report_requests(
    session: requests.Session,
    token_provider: AppStoreConnectTokenProvider,
    logger: FilteringBoundLogger,
    app_id: str,
) -> list[dict[str, Any]]:
    """All of the app's analytics report requests, flattened, both access types.

    Listed unfiltered and split client-side on ``accessType``: one call serves the ongoing and
    snapshot ensures, and a response that ignores the server-side filter can't misclassify a
    request as the wrong kind.
    """
    url = f"{BASE_URL}/v1/apps/{app_id}/analyticsReportRequests"
    report_requests: list[dict[str, Any]] = []
    for page in _iter_pages(session, token_provider, logger, url, {}):
        report_requests.extend(_flatten_resource(resource) for resource in page.resources)
    return [row for row in report_requests if row.get("id")]


def _create_report_request(
    session: requests.Session,
    token_provider: AppStoreConnectTokenProvider,
    logger: FilteringBoundLogger,
    app_id: str,
    access_type: str,
) -> str | None:
    """POST a new analytics report request; ``None`` when Apple reports one already exists (409).

    Creating a request is the only call in this source that mutates the customer's App Store
    Connect account, so every caller has to stay idempotent around it.
    """
    payload = {
        "data": {
            "type": "analyticsReportRequests",
            "attributes": {"accessType": access_type},
            "relationships": {"app": {"data": {"type": "apps", "id": app_id}}},
        }
    }
    response = _post_json(
        session,
        f"{BASE_URL}/v1/analyticsReportRequests",
        token_provider=token_provider,
        logger=logger,
        payload=payload,
        tolerate=(409,),
    )
    if response.status_code == 409:
        return None

    body = response.json()
    data = body.get("data") if isinstance(body, dict) else None
    request_id = data.get("id") if isinstance(data, dict) else None
    return str(request_id) if request_id else None


def _active_ongoing_request_id(report_requests: list[dict[str, Any]]) -> str | None:
    for row in report_requests:
        if row.get("accessType") != ONGOING_ACCESS_TYPE:
            continue
        # A request Apple stopped due to inactivity no longer generates reports, so it doesn't
        # count as active.
        if row.get("stoppedDueToInactivity"):
            continue
        return str(row["id"])
    return None


def _ensure_report_request(
    session: requests.Session,
    token_provider: AppStoreConnectTokenProvider,
    logger: FilteringBoundLogger,
    app_id: str,
    report_requests: list[dict[str, Any]],
) -> tuple[str | None, bool]:
    """Reuse the app's active ONGOING analytics report request, creating one only if none exists.

    Returns ``(request_id, created_now)``. An existing active request is always reused. Apple
    rejects a duplicate create with a 409, which resolves by re-reading the list.
    """
    existing = _active_ongoing_request_id(report_requests)
    if existing:
        return existing, False

    created = _create_report_request(session, token_provider, logger, app_id, ONGOING_ACCESS_TYPE)
    if created is None:
        # A concurrent sync beat us to it.
        return _active_ongoing_request_id(_list_report_requests(session, token_provider, logger, app_id)), False
    return created, True


def _normalize_report_name(name: str) -> str:
    # Apple's report names drift in case, spacing, and hyphenation ("Pre-Orders" vs "Pre-orders").
    # Match on alphanumerics only, so a cosmetic rename cannot silently blank a stream.
    return re.sub(r"[^a-z0-9]", "", name.casefold())


def _list_reports(
    session: requests.Session,
    token_provider: AppStoreConnectTokenProvider,
    logger: FilteringBoundLogger,
    config: AppStoreConnectEndpointConfig,
    request_id: str,
) -> dict[str, str]:
    """Map of report name to report id under one report request, within the endpoint's category."""
    url = f"{BASE_URL}/v1/analyticsReportRequests/{request_id}/reports"
    report_ids: dict[str, str] = {}
    for page in _iter_pages(
        session, token_provider, logger, url, {"filter[category]": config.analytics_report_category}
    ):
        for resource in page.resources:
            row = _flatten_resource(resource)
            if row.get("name") and row.get("id"):
                report_ids[str(row["name"])] = str(row["id"])
    return report_ids


def _match_report(config: AppStoreConnectEndpointConfig, report_ids: dict[str, str]) -> str | None:
    by_normalized = {_normalize_report_name(name): report_id for name, report_id in report_ids.items()}
    for name in config.analytics_report_names:
        report_id = by_normalized.get(_normalize_report_name(name))
        if report_id is not None:
            return report_id
    return None


def _find_analytics_report(
    session: requests.Session,
    token_provider: AppStoreConnectTokenProvider,
    logger: FilteringBoundLogger,
    config: AppStoreConnectEndpointConfig,
    request_id: str,
) -> str | None:
    report_ids = _list_reports(session, token_provider, logger, config, request_id)
    report_id = _match_report(config, report_ids)
    if report_id is not None:
        return report_id

    logger.warning(
        f"App Store Connect: no report named {config.analytics_report_names} under this request "
        f"(endpoint={config.name}, available={sorted(report_ids)}). The account may not be entitled "
        f"to this report, Apple may have renamed it, or the first reports may still be generating."
    )
    return None


def _analytics_instances(
    session: requests.Session,
    token_provider: AppStoreConnectTokenProvider,
    logger: FilteringBoundLogger,
    report_id: str,
) -> list[tuple[str, date]]:
    """Every DAILY ``(instance_id, processing_date)`` of a report, ascending by processing date.

    Unfiltered on purpose: the caller applies its watermark bound for the walk, but also needs the
    full listing to place the snapshot's report-date cutoff at the earliest ongoing instance.
    """
    url = f"{BASE_URL}/v1/analyticsReports/{report_id}/instances"
    instances: list[tuple[str, date]] = []
    for page in _iter_pages(session, token_provider, logger, url, {"filter[granularity]": ANALYTICS_GRANULARITY}):
        for resource in page.resources:
            row = _flatten_resource(resource)
            processing_date = _to_date(row.get("processingDate"))
            if not row.get("id") or processing_date is None:
                continue
            instances.append((str(row["id"]), processing_date))
    instances.sort(key=lambda instance: instance[1])
    return instances


@dataclasses.dataclass(frozen=True, kw_only=True)
class _SnapshotPlan:
    """Outcome of resolving an app's one-time historical snapshot for one report.

    ``ready``: walkable instances exist. ``pending``: Apple is (or may still be) generating one.
    ``absent``: every fulfilled snapshot request lists reports and none of them is this one, so
    the account isn't entitled to it and there is nothing to wait for.
    """

    state: Literal["ready", "pending", "absent"]
    instances: list[tuple[str, date]] = dataclasses.field(default_factory=list)


def _resolve_snapshot_plan(
    session: requests.Session,
    token_provider: AppStoreConnectTokenProvider,
    logger: FilteringBoundLogger,
    config: AppStoreConnectEndpointConfig,
    app_id: str,
    report_requests: list[dict[str, Any]],
) -> _SnapshotPlan:
    """Find (or request) the app's ONE_TIME_SNAPSHOT and this report's instances under it.

    A fulfilled snapshot request is always reused — a second ensure never re-creates or errors.
    Creating is reserved for two cases: no snapshot request exists at all, or a fulfilled one's
    instances have aged out (Apple retains them only for a limited window), where only a fresh
    snapshot can regenerate the history. A request with no reports yet is still generating, so it
    is waited on rather than duplicated.
    """
    snapshot_requests = [row for row in report_requests if row.get("accessType") == SNAPSHOT_ACCESS_TYPE]
    if not snapshot_requests:
        _create_report_request(session, token_provider, logger, app_id, SNAPSHOT_ACCESS_TYPE)
        logger.info(
            f"App Store Connect: requested a one-time historical snapshot for app {app_id}; "
            f"Apple generates it in 1-2 days. endpoint={config.name}"
        )
        return _SnapshotPlan(state="pending")

    generating = False
    resolved_report_ids: list[str] = []
    for row in snapshot_requests:
        report_ids = _list_reports(session, token_provider, logger, config, str(row["id"]))
        if not report_ids:
            # No reports in this category yet: the snapshot is most likely still generating.
            generating = True
            continue
        report_id = _match_report(config, report_ids)
        if report_id is not None:
            resolved_report_ids.append(report_id)

    # One instance per processing date: snapshot rows are numbered from -1 within their instance,
    # so walking two instances of one date would hand different rows the same merge key. The
    # lowest instance id wins so re-runs pick the same one.
    instances: dict[date, str] = {}
    for report_id in resolved_report_ids:
        for instance_id, processing_date in _analytics_instances(session, token_provider, logger, report_id):
            current = instances.get(processing_date)
            if current is None or instance_id < current:
                instances[processing_date] = instance_id

    if instances:
        return _SnapshotPlan(
            state="ready",
            instances=sorted(
                ((instance_id, processing_date) for processing_date, instance_id in instances.items()),
                key=lambda instance: instance[1],
            ),
        )
    if resolved_report_ids and not generating:
        # Fulfilled once, but the instances aged out before they were downloaded — and no other
        # snapshot request is mid-generation, so re-requesting won't pile requests up while a
        # replacement is already on its way.
        _create_report_request(session, token_provider, logger, app_id, SNAPSHOT_ACCESS_TYPE)
        logger.info(
            f"App Store Connect: the existing historical snapshot for app {app_id} has expired; "
            f"requested a fresh one. endpoint={config.name}"
        )
        return _SnapshotPlan(state="pending")
    if generating or resolved_report_ids:
        return _SnapshotPlan(state="pending")
    return _SnapshotPlan(state="absent")


def _analytics_segments(
    session: requests.Session,
    token_provider: AppStoreConnectTokenProvider,
    logger: FilteringBoundLogger,
    instance_id: str,
) -> list[dict[str, Any]]:
    url = f"{BASE_URL}/v1/analyticsReportInstances/{instance_id}/segments"
    segments: list[dict[str, Any]] = []
    for page in _iter_pages(session, token_provider, logger, url, {}):
        segments.extend(row for row in (_flatten_resource(resource) for resource in page.resources) if row.get("url"))
    # Row keys carry the line's position within the instance, so segment order has to be
    # deterministic across re-reads or the same key would name a different row each time.
    segments.sort(key=lambda segment: str(segment.get("id")))
    return segments


def _download_segment(logger: FilteringBoundLogger, segment: dict[str, Any]) -> IO[bytes]:
    """Download one segment to a spooled file, hashing as it streams.

    The URL is presigned, so no Authorization header is attached: sending the Apple bearer token to
    the storage host would hand it to a third party. The checksum's algorithm is undocumented (the
    value is shaped like an MD5), so a mismatch is logged rather than fatal; failing hard on a wrong
    algorithm guess would brick the table, and gzip's own CRC still rejects corrupted payloads at
    decompression time.
    """
    url = _require_segment_url(str(segment["url"]))
    spool = tempfile.SpooledTemporaryFile(max_size=ANALYTICS_SEGMENT_SPOOL_BYTES)
    # Download-integrity check against Apple's checksum, not a cryptographic use; corrupted
    # payloads are also rejected by the gzip CRC.
    digest = hashlib.md5(usedforsecurity=False)  # nosemgrep

    session = _make_segment_download_session(urlsplit(url).query)
    response = session.get(url, stream=True, timeout=REPORT_TIMEOUT_SECONDS)
    try:
        if not response.ok:
            logger.error(f"App Store Connect analytics segment download failed: status={response.status_code}")
            response.raise_for_status()
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            digest.update(chunk)
            spool.write(chunk)
    finally:
        response.close()

    expected = segment.get("checksum")
    if expected and digest.hexdigest() != expected:
        logger.warning(
            f"App Store Connect: analytics segment checksum mismatch "
            f"(expected={expected}, got={digest.hexdigest()}, sizeInBytes={segment.get('sizeInBytes')}). "
            f"Continuing; the gzip CRC rejects genuinely corrupt payloads."
        )

    spool.seek(0)
    return spool


def _open_segment_text(spool: IO[bytes]) -> IO[str]:
    # The transport can hand the payload over decompressed (a `Content-Encoding: gzip` body is
    # unwrapped by urllib3), so sniff the magic bytes instead of assuming.
    magic = spool.read(2)
    spool.seek(0)
    if magic == b"\x1f\x8b":
        return io.TextIOWrapper(gzip.GzipFile(fileobj=spool, mode="rb"), encoding="utf-8-sig", errors="replace")
    return io.TextIOWrapper(spool, encoding="utf-8-sig", errors="replace")


def _iter_segment_rows(text: IO[str], processing_date: date, line_start: int) -> Iterator[dict[str, Any]]:
    header_line = text.readline()
    if not header_line.strip():
        return

    # Apple's segment objects are named `.csv.gz` but its docs describe the files only as
    # delimited text, so sniff the delimiter from the header instead of assuming one.
    delimiter = "\t" if "\t" in header_line else ","
    columns = [_normalize_report_column(column) for column in next(csv.reader([header_line], delimiter=delimiter))]
    processing_date_str = processing_date.isoformat()
    line = line_start

    for values in csv.reader(text, delimiter=delimiter):
        if not any(value.strip() for value in values):
            continue
        row: dict[str, Any] = {
            column: (values[index] if index < len(values) else None) for index, column in enumerate(columns)
        }
        row["processing_date"] = processing_date_str
        # 1-based position within the instance, continuing across its segments. A published
        # instance is immutable, so (app_id, processing_date, _line) stays a stable unique key
        # and re-reading an instance merges instead of duplicating.
        line += 1
        row["_line"] = line
        yield row


@dataclasses.dataclass(frozen=True, kw_only=True)
class _WalkInstance:
    """One analytics report instance scheduled into the date-ordered walk."""

    app_id: str
    instance_id: str
    is_snapshot: bool = False
    # Snapshot rows are kept only when their report date is strictly before this cutoff — the
    # earliest listed ongoing instance's processing date. Report dates at or after it are the
    # ongoing stream's to deliver, which keeps one report date from landing from both streams.
    snapshot_cutoff: date | None = None


def _get_analytics_report(
    session: requests.Session,
    segments_session: requests.Session,
    config: AppStoreConnectEndpointConfig,
    token_provider: AppStoreConnectTokenProvider,
    logger: FilteringBoundLogger,
    manager: ResumableSourceManager[AppStoreConnectResumeConfig],
    should_use_incremental_field: bool,
    db_incremental_field_last_value: Any,
) -> Iterator[list[dict[str, Any]]]:
    app_ids = _list_app_ids(session, token_provider, logger)
    resume = _load_resume(manager)
    resumed_date = _to_date(resume.processing_date) if resume is not None else None
    watermark = _to_date(db_incremental_field_last_value) if should_use_incremental_field else None

    lower_bound: date | None = None
    for candidate in (watermark, resumed_date):
        if candidate is not None and (lower_bound is None or candidate > lower_bound):
            lower_bound = candidate

    # The one-time snapshot restates all history, so it only joins the walk when the table is
    # known to be empty: a first sync, a resync (the reset clears the watermark before the run),
    # or a full refresh. An established incremental table holds ongoing history the source can't
    # enumerate, so no report-date boundary could dedupe a snapshot against it. Retried attempts
    # of one job reuse the first attempt's decision from the resume state, because a pipeline may
    # persist the watermark per batch mid-job.
    if resume is not None and resume.include_snapshot is not None:
        include_snapshot = resume.include_snapshot
    else:
        include_snapshot = watermark is None

    # Discover every app's report and instances up front, then walk processing dates in
    # ascending order ACROSS apps. Yields are then globally date-ordered, so the pipeline's
    # per-batch watermark checkpoint can never advance past an app whose older instances are
    # still unfetched, and the walk can stop cleanly at the first gap or at the per-run cap:
    # the watermark stands at the last date reached, and because the lower bound is inclusive
    # the next run re-reads that boundary date in full and the merge dedupes it. Resume state
    # is job-scoped (it survives retries of the same job, never the next scheduled run), so
    # the watermark has to carry cross-run progress by itself.
    instances_by_date: dict[date, list[_WalkInstance]] = {}
    hold_for_snapshot = False
    snapshot_ceiling: date | None = None
    for app_id in app_ids:
        report_requests = _list_report_requests(session, token_provider, logger, app_id)
        request_id, created_now = _ensure_report_request(session, token_provider, logger, app_id, report_requests)

        snapshot_plan: _SnapshotPlan | None = None
        if include_snapshot:
            snapshot_plan = _resolve_snapshot_plan(session, token_provider, logger, config, app_id, report_requests)

        if created_now or request_id is None:
            if created_now:
                logger.info(
                    f"App Store Connect: created an ONGOING analytics report request for app {app_id}; "
                    f"Apple generates the first reports in 1-2 days. endpoint={config.name}"
                )
            # The app can't be walked this run, so an available snapshot can't be emitted in
            # order either — everything for this app has to land together on a later run.
            if snapshot_plan is not None and snapshot_plan.state != "absent":
                hold_for_snapshot = True
            continue

        report_id = _find_analytics_report(session, token_provider, logger, config, request_id)
        ongoing_instances: list[tuple[str, date]] = []
        if report_id is not None:
            # An unavailable report degrades this table for this app; other apps and tables
            # are unaffected.
            ongoing_instances = _analytics_instances(session, token_provider, logger, report_id)

        for instance_id, processing_date in ongoing_instances:
            # The lower bound is inclusive: an instance's rows can restate earlier data
            # dates, and re-reading the boundary merges idempotently on the primary key.
            if lower_bound is not None and processing_date < lower_bound:
                continue
            instances_by_date.setdefault(processing_date, []).append(
                _WalkInstance(app_id=app_id, instance_id=instance_id)
            )

        if snapshot_plan is None:
            continue
        if snapshot_plan.state == "pending" and report_id is not None:
            # Only an app whose ongoing report is live can strand its history by emitting ahead
            # of the snapshot; an unentitled app never will, so it must not stall the others.
            hold_for_snapshot = True
        snapshot_cutoff = min((processing_date for _, processing_date in ongoing_instances), default=None)
        for instance_id, processing_date in snapshot_plan.instances:
            if lower_bound is not None and processing_date < lower_bound:
                continue
            instances_by_date.setdefault(processing_date, []).append(
                _WalkInstance(app_id=app_id, instance_id=instance_id, is_snapshot=True, snapshot_cutoff=snapshot_cutoff)
            )
            if snapshot_ceiling is None or processing_date > snapshot_ceiling:
                snapshot_ceiling = processing_date

    if hold_for_snapshot and should_use_incremental_field:
        # An incremental table's first emission ratchets the watermark past the snapshot's
        # report dates for good, so nothing is emitted until the snapshot can be emitted with
        # it. A full refresh never holds: it rebuilds the whole table every run, so the next
        # rebuild picks the history up on its own.
        logger.info(
            f"App Store Connect: waiting for Apple to generate the one-time historical snapshot "
            f"(typically 1-2 days) before the first ingest, so history lands ahead of the "
            f"ongoing stream. endpoint={config.name}"
        )
        return

    has_snapshot_instances = any(
        walk_instance.is_snapshot for walk_instances in instances_by_date.values() for walk_instance in walk_instances
    )
    if should_use_incremental_field and snapshot_ceiling is not None:
        # Probe every instance at or below the snapshot for downloadable files before emitting
        # anything: a not-ready instance below the snapshot would stop the walk mid-emission,
        # ratchet the watermark, and strand the history until a manual resync.
        for processing_date in sorted(candidate for candidate in instances_by_date if candidate <= snapshot_ceiling):
            for walk_instance in instances_by_date[processing_date]:
                if not _analytics_segments(segments_session, token_provider, logger, walk_instance.instance_id):
                    logger.info(
                        f"App Store Connect: an analytics instance below the historical snapshot "
                        f"has no files yet; waiting so the snapshot isn't stranded. "
                        f"endpoint={config.name}, app_id={walk_instance.app_id}, "
                        f"processing_date={processing_date.isoformat()}"
                    )
                    return

    instances_fetched = 0
    for processing_date in sorted(instances_by_date):
        for walk_instance in instances_by_date[processing_date]:
            if instances_fetched >= ANALYTICS_MAX_INSTANCES_PER_RUN and not has_snapshot_instances:
                # An incremental sync continues from the watermark next run. A full refresh
                # has no watermark to continue from, so a cap-hit there means a truncated
                # table until the backlog fits in one run. A run carrying the snapshot is
                # never truncated: stopping below the snapshot would strand the history the
                # same way a mid-walk gap would, and its backlog is bounded by Apple's
                # instance retention plus one snapshot per app.
                logger.warning(
                    f"App Store Connect: hit the per-run analytics instance cap at "
                    f"{processing_date.isoformat()}; later dates are left for the next "
                    f"incremental run. endpoint={config.name}"
                )
                manager.save_state(
                    AppStoreConnectResumeConfig(
                        processing_date=processing_date.isoformat(), include_snapshot=include_snapshot
                    )
                )
                return

            segments = _analytics_segments(segments_session, token_provider, logger, walk_instance.instance_id)
            if not segments:
                # The instance is listed but its files aren't ready. Stop the whole walk at
                # this date so no newer date is emitted past the gap: the watermark then
                # stays at or below this date, and the next run re-reads it once the files
                # exist.
                logger.info(
                    f"App Store Connect: analytics instance has no segments yet, stopping the "
                    f"walk at this date. endpoint={config.name}, app_id={walk_instance.app_id}, "
                    f"processing_date={processing_date.isoformat()}"
                )
                manager.save_state(
                    AppStoreConnectResumeConfig(
                        processing_date=processing_date.isoformat(), include_snapshot=include_snapshot
                    )
                )
                return

            line = 0
            batch: list[dict[str, Any]] = []
            for segment in segments:
                spool = _download_segment(logger, segment)
                try:
                    with _open_segment_text(spool) as text:
                        for row in _iter_segment_rows(text, processing_date, line):
                            line = row["_line"]
                            if walk_instance.is_snapshot:
                                if walk_instance.snapshot_cutoff is not None:
                                    row_date = _to_date(row.get("date"))
                                    # A row without a parseable report date can't be checked
                                    # against the ongoing stream's coverage, so it's dropped
                                    # rather than risked as a duplicate.
                                    if row_date is None or row_date >= walk_instance.snapshot_cutoff:
                                        continue
                                # Negative line numbers keyed by file position: they can never
                                # collide with the ongoing instance processed on the same date,
                                # and they don't shift when the cutoff moves, so a re-run
                                # re-emits identical keys and the merge folds it to no-ops.
                                row["_line"] = -line
                            row["app_id"] = walk_instance.app_id
                            batch.append(row)
                            if len(batch) >= ANALYTICS_ROWS_PER_BATCH:
                                yield batch
                                batch = []
                finally:
                    spool.close()
            if batch:
                yield batch

            instances_fetched += 1

        # The date is complete for every app, so a retried attempt of this job can start at
        # the next one. Saved AFTER the date's rows are yielded, so a crash re-reads the
        # date rather than skipping it; the merge dedupes the re-read.
        manager.save_state(
            AppStoreConnectResumeConfig(
                processing_date=(processing_date + timedelta(days=1)).isoformat(),
                include_snapshot=include_snapshot,
            )
        )


def check_credentials(issuer_id: str, key_id: str, private_key: str) -> tuple[int | None, str | None]:
    """Probe ``/v1/apps`` with a minted token.

    Returns ``(http_status, message)``. The status is ``None`` when the request never left the process
    (a key we can't sign with, or a network failure), in which case ``message`` explains why when we know.
    """
    try:
        token_provider = AppStoreConnectTokenProvider(issuer_id, key_id, private_key)
        token = token_provider.token()
    except AppStoreConnectAuthError as e:
        return None, str(e)

    try:
        response = _make_session(private_key).get(
            f"{BASE_URL}/v1/apps",
            params={"limit": 1},
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
            timeout=CREDENTIALS_TIMEOUT_SECONDS,
        )
        return response.status_code, None
    except Exception:
        return None, None


def get_rows(
    issuer_id: str,
    key_id: str,
    private_key: str,
    vendor_number: str | None,
    endpoint: str,
    logger: FilteringBoundLogger,
    resumable_source_manager: ResumableSourceManager[AppStoreConnectResumeConfig],
    should_use_incremental_field: bool = False,
    db_incremental_field_last_value: Any = None,
) -> Iterator[list[dict[str, Any]]]:
    config = APP_STORE_CONNECT_ENDPOINTS[endpoint]
    session = _make_session(private_key)
    token_provider = AppStoreConnectTokenProvider(issuer_id, key_id, private_key)

    if config.kind == "collection":
        yield from _get_collection(session, config, token_provider, logger, resumable_source_manager)
    elif config.kind == "app_fanout":
        yield from _get_app_fanout(session, config, token_provider, logger, resumable_source_manager)
    elif config.kind == "analytics_report":
        yield from _get_analytics_report(
            session,
            # Segment listings ride a capture-disabled session: their bodies carry presigned
            # URLs whose query strings are short-lived credentials the name-based scrubbers
            # can't recognise.
            _make_session(private_key, capture=False),
            config,
            token_provider,
            logger,
            resumable_source_manager,
            should_use_incremental_field,
            db_incremental_field_last_value,
        )
    else:  # "sales_report"
        yield from _get_sales_report(
            session,
            config,
            token_provider,
            logger,
            resumable_source_manager,
            vendor_number,
            should_use_incremental_field,
            db_incremental_field_last_value,
        )

    # Walked to completion, so drop the checkpoint — leaving it would let a later attempt on this job
    # resume mid-stream instead of restarting cleanly.
    resumable_source_manager.clear_state()


def app_store_connect_source(
    issuer_id: str,
    key_id: str,
    private_key: str,
    vendor_number: str | None,
    endpoint: str,
    logger: FilteringBoundLogger,
    resumable_source_manager: ResumableSourceManager[AppStoreConnectResumeConfig],
    should_use_incremental_field: bool = False,
    db_incremental_field_last_value: Optional[Any] = None,
) -> SourceResponse:
    config = APP_STORE_CONNECT_ENDPOINTS[endpoint]

    return SourceResponse(
        name=endpoint,
        items=lambda: get_rows(
            issuer_id=issuer_id,
            key_id=key_id,
            private_key=private_key,
            vendor_number=vendor_number,
            endpoint=endpoint,
            logger=logger,
            resumable_source_manager=resumable_source_manager,
            should_use_incremental_field=should_use_incremental_field,
            db_incremental_field_last_value=db_incremental_field_last_value,
        ),
        primary_keys=config.primary_keys,
        partition_count=1,
        partition_size=1,
        partition_mode="datetime" if config.partition_key else None,
        partition_format="month" if config.partition_key else None,
        partition_keys=[config.partition_key] if config.partition_key else None,
        # Report streams walk dates oldest-first (analytics streams date-major across apps, so
        # per-batch watermark checkpoints stay safe despite the fan-out), and collections are
        # full refreshes merged on a unique key, so asc fits everything.
        sort_mode="asc",
    )
