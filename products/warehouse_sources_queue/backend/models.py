from django.db import models

from posthog.models.scoping.product_mixin import ProductTeamModel
from posthog.models.utils import UUIDModel, sane_repr


class SourceBatch(UUIDModel):
    class SyncType(models.TextChoices):
        FULL_REFRESH = "full_refresh", "full_refresh"
        INCREMENTAL = "incremental", "incremental"
        APPEND = "append", "append"
        CDC = "cdc", "cdc"

    class LatestState(models.TextChoices):
        # 'pending' means "no status row yet" — deliberately distinct from
        # SourceBatchStatus.State.WAITING, which claim semantics treat differently.
        PENDING = "pending", "pending"
        WAITING = "waiting", "waiting"
        EXECUTING = "executing", "executing"
        SUCCEEDED = "succeeded", "succeeded"
        WAITING_RETRY = "waiting_retry", "waiting_retry"
        FAILED = "failed", "failed"

    team_id = models.BigIntegerField()
    schema_id = models.CharField(max_length=200)
    source_id = models.CharField(max_length=200)
    job_id = models.CharField(max_length=200, help_text="FK to ExternalDataJob (UUID as string).")
    run_uuid = models.CharField(max_length=200)

    batch_index = models.IntegerField()
    s3_path = models.TextField()
    row_count = models.IntegerField()
    byte_size = models.BigIntegerField()
    is_final_batch = models.BooleanField()
    total_batches = models.IntegerField(null=True, blank=True)
    total_rows = models.BigIntegerField(null=True, blank=True)
    sync_type = models.CharField(max_length=32, choices=SyncType.choices)
    cumulative_row_count = models.BigIntegerField(default=0)

    resource_name = models.CharField(max_length=400)
    is_resume = models.BooleanField(default=False)
    is_first_ever_sync = models.BooleanField(default=False)

    metadata = models.JSONField(
        default=dict,
        blank=True,
        help_text="Stores partitioning config, CDC mode, primary keys, schema path, data folder, etc.",
    )

    # Denormalized mirror of the latest sourcebatchstatus row, maintained by the
    # dual-write CTEs in jobs_db so hot readers don't re-derive state from the
    # append-only log. sourcebatchstatus remains the source of truth.
    latest_state = models.CharField(
        max_length=32, choices=LatestState.choices, default=LatestState.PENDING, db_default="pending"
    )
    latest_attempt = models.SmallIntegerField(default=0, db_default=0)
    # NULL means "never dual-written" — the backfill command's target marker.
    state_changed_at = models.DateTimeField(null=True, blank=True)
    # Denormalized from the failed status row's error payload ({"superseded": true},
    # written only by supersede_other_runs). Lets the reconcile sweep judge
    # candidacy from this table alone instead of a per-batch status lateral,
    # whose cost melted down under failure storms.
    superseded = models.BooleanField(default=False, db_default=False)

    created_at = models.DateTimeField(auto_now_add=True)

    __repr__ = sane_repr("id", "team_id", "schema_id", "batch_index")

    class Meta:
        db_table = "sourcebatch"
        indexes = [
            models.Index(fields=["team_id", "schema_id"], name="sb_team_schema_idx"),
            models.Index(fields=["run_uuid"], name="sb_run_uuid_idx"),
            models.Index(fields=["run_uuid", "batch_index"], name="sb_run_uuid_bi_idx"),
            # Serves the job-scoped scans (supersede_other_runs on every fresh run's
            # first batch, lock-takeover activity summary, orphan reconcile counts),
            # which otherwise seq-scan every retained partition per call.
            models.Index(fields=["job_id"], name="sb_job_id_idx"),
            models.Index(
                fields=["team_id", "created_at", "batch_index"],
                name="sb_claimable_idx",
                condition=models.Q(latest_state__in=["pending", "waiting_retry"]),
            ),
            models.Index(
                fields=["run_uuid", "latest_state", "batch_index"],
                name="sb_run_gate_idx",
                condition=models.Q(latest_state__in=["executing", "waiting_retry", "failed"]),
            ),
            models.Index(
                fields=["team_id", "schema_id"],
                name="sb_schema_busy_idx",
                condition=models.Q(latest_state="executing"),
            ),
            models.Index(
                fields=["state_changed_at"],
                name="sb_failed_changed_idx",
                condition=models.Q(latest_state="failed"),
            ),
        ]


class SourceBatchStatus(UUIDModel):
    class State(models.TextChoices):
        WAITING = "waiting", "waiting"
        EXECUTING = "executing", "executing"
        SUCCEEDED = "succeeded", "succeeded"
        WAITING_RETRY = "waiting_retry", "waiting_retry"
        FAILED = "failed", "failed"

    # No DB-level FK constraint: sourcebatch is range-partitioned on
    # created_at, making its PK composite (id, created_at). A real FK
    # would require batch_created_at here. Referential integrity is
    # enforced in application code — statuses are only inserted for
    # known batch IDs.
    batch = models.ForeignKey(
        SourceBatch,
        on_delete=models.DO_NOTHING,
        db_constraint=False,
        related_name="statuses",
    )
    job_state = models.CharField(max_length=32, choices=State.choices)
    attempt = models.SmallIntegerField(default=0)
    exec_time = models.DateTimeField(null=True, blank=True)
    error_response = models.JSONField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "sourcebatchstatus"
        indexes = [
            models.Index(
                fields=["batch_id", "-created_at", "-id", "job_state"],
                name="sbs_batch_id_desc_state_idx",
            ),
        ]


class SourceGroupLease(models.Model):
    """Lease-based mutual exclusion for processing a (team_id, schema_id) group.

    Replaces the session-scoped Postgres advisory lock that previously gated
    group claiming. A lease row is claimed via a conditional upsert and renewed
    by the consumer heartbeat; an abandoned lease (pod SIGKILLed, pgbouncer
    session lingering, node lost) simply expires, so any surviving pod can
    reclaim the group once ``expires_at`` passes. All access is via raw SQL in
    ``postgres_queue/jobs_db.py`` — this model exists for migration/introspection.
    """

    team_id = models.BigIntegerField()
    schema_id = models.CharField(max_length=200)
    owner_token = models.CharField(max_length=64, help_text="Per-pod identity (uuid4) of the current lease holder.")
    expires_at = models.DateTimeField()
    acquired_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    __repr__ = sane_repr("team_id", "schema_id", "owner_token", "expires_at")

    class Meta:
        db_table = "sourcegrouplease"
        constraints = [
            models.UniqueConstraint(fields=["team_id", "schema_id"], name="sgl_team_schema_uniq"),
        ]
        indexes = [
            models.Index(fields=["expires_at"], name="sgl_expires_at_idx"),
        ]


class SourceRunDestination(ProductTeamModel, UUIDModel):
    """Where one extraction run is supposed to deliver, as the queue sees it.

    The queue database cannot read the app database, so the run's resolved destination set
    is snapshotted here when the run enqueues its first batch. Rows exist only for runs on
    the multi-destination path; unflagged runs claim through `SourceBatch.latest_state`.

    `config_snapshot` carries target naming and mode hints only — credentials stay in the
    app database on the destination's `Integration`.
    """

    schema_id = models.CharField(max_length=200)
    source_id = models.CharField(max_length=200)
    job_id = models.CharField(max_length=200, help_text="FK to ExternalDataJob (UUID as string).")
    run_uuid = models.CharField(max_length=200)

    destination_job_id = models.CharField(
        max_length=200, help_text="FK to ExternalDataDestinationJob (UUID as string)."
    )
    destination_id = models.CharField(max_length=200)
    destination_type = models.CharField(max_length=64)
    config_snapshot = models.JSONField(default=dict, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    __repr__ = sane_repr("id", "run_uuid", "destination_type")

    class Meta(ProductTeamModel.Meta):
        db_table = "sourcerundestination"
        constraints = [
            models.UniqueConstraint(fields=["run_uuid", "destination_job_id"], name="srd_run_dest_uniq"),
        ]
        indexes = [
            models.Index(fields=["run_uuid"], name="srd_run_uuid_idx"),
            models.Index(fields=["team_id", "schema_id"], name="srd_team_schema_idx"),
            models.Index(fields=["job_id"], name="srd_job_id_idx"),
        ]


class SourceBatchDestination(ProductTeamModel, UUIDModel):
    """One batch's delivery to one destination — the unit the destination consumers claim.

    Mirrors the denormalized-state design `SourceBatch` uses for the legacy path:
    `latest_state` is maintained by the same dual-write CTEs that append to
    `SourceBatchDestinationStatus`, which stays the source of truth. Claims read these
    columns behind partial indexes rather than re-deriving state from the append-only log,
    because the per-batch lateral that replaced melted down under failure storms.

    `batch_index`, `is_final_batch` and `sync_type` are denormalized off `SourceBatch` so
    the head-of-line and incomplete-run gates never join the (partitioned) batch table.
    """

    class LatestState(models.TextChoices):
        # 'pending' means "no status row yet" — deliberately distinct from
        # SourceBatchDestinationStatus.State.WAITING, which claim semantics treat differently.
        PENDING = "pending", "pending"
        WAITING = "waiting", "waiting"
        EXECUTING = "executing", "executing"
        SUCCEEDED = "succeeded", "succeeded"
        WAITING_RETRY = "waiting_retry", "waiting_retry"
        FAILED = "failed", "failed"

    # No DB-level FK constraint: sourcebatch is range-partitioned on created_at, making its
    # PK composite. Same reasoning as SourceBatchStatus.batch.
    batch = models.ForeignKey(
        SourceBatch,
        on_delete=models.DO_NOTHING,
        db_constraint=False,
        related_name="destination_work_items",
    )
    schema_id = models.CharField(max_length=200)
    run_uuid = models.CharField(max_length=200)
    destination_job_id = models.CharField(max_length=200)
    destination_type = models.CharField(max_length=64)

    batch_index = models.IntegerField()
    is_final_batch = models.BooleanField()
    sync_type = models.CharField(max_length=32, choices=SourceBatch.SyncType.choices)

    latest_state = models.CharField(
        max_length=32, choices=LatestState.choices, default=LatestState.PENDING, db_default="pending"
    )
    latest_attempt = models.SmallIntegerField(default=0, db_default=0)
    state_changed_at = models.DateTimeField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    __repr__ = sane_repr("id", "run_uuid", "batch_index", "destination_type", "latest_state")

    class Meta(ProductTeamModel.Meta):
        db_table = "sourcebatchdestination"
        indexes = [
            # Claim candidates. destination_type leads so a fleet restricted to one set of
            # destination types never scans the others' work.
            models.Index(
                fields=["destination_type", "team_id", "created_at", "batch_index"],
                name="sbd_claimable_idx",
                condition=models.Q(latest_state__in=["pending", "waiting_retry"]),
            ),
            # Head-of-line gate: is an earlier batch of this run still in flight for this destination?
            models.Index(
                fields=["run_uuid", "destination_job_id", "latest_state", "batch_index"],
                name="sbd_run_gate_idx",
                condition=models.Q(latest_state__in=["executing", "waiting_retry", "failed"]),
            ),
            # One-executing-batch-per-group gate.
            models.Index(
                fields=["team_id", "schema_id", "destination_job_id"],
                name="sbd_group_busy_idx",
                condition=models.Q(latest_state="executing"),
            ),
            # Cross-run serialization: does an older unfinished run of this schema x destination
            # still owe batches? Ordered by created_at so the oldest is the first row read.
            models.Index(
                fields=["team_id", "schema_id", "destination_job_id", "created_at"],
                name="sbd_incomplete_run_idx",
                condition=models.Q(latest_state__in=["pending", "waiting", "executing", "waiting_retry"]),
            ),
            models.Index(fields=["destination_job_id"], name="sbd_dest_job_idx"),
            models.Index(fields=["batch_id", "destination_job_id"], name="sbd_batch_dest_idx"),
            models.Index(
                fields=["state_changed_at"],
                name="sbd_failed_changed_idx",
                condition=models.Q(latest_state="failed"),
            ),
        ]


class SourceBatchDestinationStatus(UUIDModel):
    """Append-only state log for `SourceBatchDestination`, the source of truth for its state."""

    class State(models.TextChoices):
        WAITING = "waiting", "waiting"
        EXECUTING = "executing", "executing"
        SUCCEEDED = "succeeded", "succeeded"
        WAITING_RETRY = "waiting_retry", "waiting_retry"
        FAILED = "failed", "failed"

    # db_constraint=False: sourcebatchdestination is range-partitioned on created_at.
    work_item = models.ForeignKey(
        SourceBatchDestination,
        on_delete=models.DO_NOTHING,
        db_constraint=False,
        related_name="statuses",
    )
    job_state = models.CharField(max_length=32, choices=State.choices)
    attempt = models.SmallIntegerField(default=0)
    exec_time = models.DateTimeField(null=True, blank=True)
    error_response = models.JSONField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "sourcebatchdestinationstatus"
        indexes = [
            models.Index(
                fields=["work_item_id", "-created_at", "-id", "job_state"],
                name="sbds_item_desc_state_idx",
            ),
        ]


class SourceBatchDestinationApply(ProductTeamModel, UUIDModel):
    """Idempotency marker: this batch has been applied to this destination.

    A unique row per (run, batch, destination) is what makes a re-claimed batch safe to skip
    after a crash between the destination commit and the state write.
    """

    schema_id = models.CharField(max_length=200)
    run_uuid = models.CharField(max_length=200)
    batch_index = models.IntegerField()
    destination_job_id = models.CharField(max_length=200)
    row_count = models.IntegerField(default=0, db_default=0)
    created_at = models.DateTimeField(auto_now_add=True)

    __repr__ = sane_repr("id", "run_uuid", "batch_index", "destination_job_id")

    class Meta(ProductTeamModel.Meta):
        db_table = "sourcebatchdestinationapply"
        constraints = [
            models.UniqueConstraint(
                fields=["team_id", "schema_id", "run_uuid", "batch_index", "destination_job_id"],
                name="sbda_unique_batch_apply",
            ),
        ]
        indexes = [
            models.Index(fields=["team_id", "schema_id", "run_uuid"], name="sbda_run_idx"),
            models.Index(fields=["destination_job_id"], name="sbda_dest_job_idx"),
        ]


class SourceDestinationGroupLease(ProductTeamModel):
    """Lease-based mutual exclusion for a (team_id, schema_id, destination_job_id) group.

    A separate table from `SourceGroupLease` on purpose: destinations process the same runs
    independently and must never contend for one lease row. All access is via raw SQL in
    ``destinations_queue/jobs_db.py`` — this model exists for migration/introspection.
    """

    schema_id = models.CharField(max_length=200)
    destination_job_id = models.CharField(max_length=200)
    owner_token = models.CharField(max_length=64, help_text="Per-pod identity (uuid4) of the current lease holder.")
    expires_at = models.DateTimeField()
    acquired_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    __repr__ = sane_repr("team_id", "schema_id", "destination_job_id", "owner_token", "expires_at")

    class Meta(ProductTeamModel.Meta):
        db_table = "sourcedestinationgrouplease"
        constraints = [
            models.UniqueConstraint(
                fields=["team_id", "schema_id", "destination_job_id"], name="sdestgl_team_schema_dest_uniq"
            ),
        ]
        indexes = [
            models.Index(fields=["expires_at"], name="sdestgl_expires_at_idx"),
        ]
