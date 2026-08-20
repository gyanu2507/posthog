import datetime

import django.db.models.manager
import django.db.models.deletion
from django.db import migrations, models

import posthog.uuidt

PARTITIONS_AHEAD = 7


def _precreate_daily_partitions(schema_editor, parent_table):
    """Pre-create one daily range partition per day for today + next N days.

    Emitted as individual ``CREATE TABLE ... PARTITION OF`` statements from
    Python rather than a server-side ``DO`` block, so it applies against
    Postgres-wire targets that don't implement PL/pgSQL, ``generate_series``,
    or ``EXECUTE format(...)``.
    """
    today = datetime.date.today()
    for offset in range(PARTITIONS_AHEAD + 1):
        day = today + datetime.timedelta(days=offset)
        next_day = day + datetime.timedelta(days=1)
        suffix = day.strftime("%Y%m%d")
        schema_editor.execute(
            f"CREATE TABLE IF NOT EXISTS {parent_table}_{suffix} "
            f"PARTITION OF {parent_table} "
            f"FOR VALUES FROM ('{day.isoformat()}') TO ('{next_day.isoformat()}')"
        )


def _create_partitioned_tables(apps, schema_editor):
    """Create the two range-partitioned tables Django cannot express in state.

    One statement per execute(): psycopg3 sends DDL over the extended query protocol,
    which parses a single statement at a time.
    """
    schema_editor.execute("""
        CREATE TABLE sourcebatchdestination (
            id UUID NOT NULL DEFAULT gen_random_uuid(),
            team_id BIGINT NOT NULL,
            batch_id UUID NOT NULL,
            schema_id VARCHAR(200) NOT NULL,
            run_uuid VARCHAR(200) NOT NULL,
            destination_job_id VARCHAR(200) NOT NULL,
            destination_type VARCHAR(64) NOT NULL,
            batch_index INT NOT NULL,
            is_final_batch BOOLEAN NOT NULL,
            sync_type VARCHAR(32) NOT NULL,
            latest_state VARCHAR(32) NOT NULL DEFAULT 'pending',
            latest_attempt SMALLINT NOT NULL DEFAULT 0,
            state_changed_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (id, created_at)
        ) PARTITION BY RANGE (created_at)
    """)
    # Claim candidates. destination_type leads so a fleet restricted to one set of
    # destination types never scans the others' work.
    schema_editor.execute(
        "CREATE INDEX sbd_claimable_idx ON sourcebatchdestination "
        "(destination_type, team_id, created_at, batch_index) "
        "WHERE latest_state IN ('pending', 'waiting_retry')"
    )
    schema_editor.execute(
        "CREATE INDEX sbd_run_gate_idx ON sourcebatchdestination "
        "(run_uuid, destination_job_id, latest_state, batch_index) "
        "WHERE latest_state IN ('executing', 'waiting_retry', 'failed')"
    )
    schema_editor.execute(
        "CREATE INDEX sbd_group_busy_idx ON sourcebatchdestination "
        "(team_id, schema_id, destination_job_id) WHERE latest_state = 'executing'"
    )
    schema_editor.execute(
        "CREATE INDEX sbd_incomplete_run_idx ON sourcebatchdestination "
        "(team_id, schema_id, destination_job_id, created_at) "
        "WHERE latest_state IN ('pending', 'waiting', 'executing', 'waiting_retry')"
    )
    schema_editor.execute("CREATE INDEX sbd_dest_job_idx ON sourcebatchdestination (destination_job_id)")
    schema_editor.execute("CREATE INDEX sbd_batch_dest_idx ON sourcebatchdestination (batch_id, destination_job_id)")
    schema_editor.execute(
        "CREATE INDEX sbd_failed_changed_idx ON sourcebatchdestination (state_changed_at) WHERE latest_state = 'failed'"
    )
    schema_editor.execute("CREATE INDEX sbd_team_id_idx ON sourcebatchdestination (team_id)")
    schema_editor.execute("CREATE TABLE sourcebatchdestination_default PARTITION OF sourcebatchdestination DEFAULT")

    schema_editor.execute("""
        CREATE TABLE sourcebatchdestinationstatus (
            id UUID NOT NULL DEFAULT gen_random_uuid(),
            work_item_id UUID NOT NULL,
            job_state VARCHAR(32) NOT NULL,
            attempt SMALLINT NOT NULL DEFAULT 0,
            exec_time TIMESTAMPTZ,
            error_response JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (id, created_at)
        ) PARTITION BY RANGE (created_at)
    """)
    schema_editor.execute(
        "CREATE INDEX sbds_item_desc_state_idx "
        "ON sourcebatchdestinationstatus (work_item_id, created_at DESC, id DESC, job_state)"
    )
    schema_editor.execute(
        "CREATE TABLE sourcebatchdestinationstatus_default PARTITION OF sourcebatchdestinationstatus DEFAULT"
    )

    _precreate_daily_partitions(schema_editor, "sourcebatchdestination")
    _precreate_daily_partitions(schema_editor, "sourcebatchdestinationstatus")


def _drop_partitioned_tables(apps, schema_editor):
    schema_editor.execute("DROP VIEW IF EXISTS v_latest_source_batch_destination_status")
    schema_editor.execute("DROP TABLE IF EXISTS sourcebatchdestinationstatus CASCADE")
    schema_editor.execute("DROP TABLE IF EXISTS sourcebatchdestination CASCADE")


def _create_latest_destination_status_view(apps, schema_editor):
    schema_editor.execute("""
        CREATE VIEW v_latest_source_batch_destination_status AS
        SELECT DISTINCT ON (work_item_id) *
        FROM sourcebatchdestinationstatus
        ORDER BY work_item_id ASC, created_at DESC, id DESC
    """)


class Migration(migrations.Migration):
    dependencies = [
        ("warehouse_sources_queue", "0009_sourcebatch_job_id_idx"),
    ]

    operations = [
        # Unpartitioned tables: ordinary Django DDL.
        migrations.CreateModel(
            name="SourceBatchDestinationApply",
            fields=[
                (
                    "id",
                    models.UUIDField(default=posthog.uuidt.uuid7, editable=False, primary_key=True, serialize=False),
                ),
                ("team_id", models.BigIntegerField(db_index=True)),
                ("schema_id", models.CharField(max_length=200)),
                ("run_uuid", models.CharField(max_length=200)),
                ("batch_index", models.IntegerField()),
                ("destination_job_id", models.CharField(max_length=200)),
                ("row_count", models.IntegerField(db_default=0, default=0)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
            ],
            options={
                "db_table": "sourcebatchdestinationapply",
                "abstract": False,
                "default_manager_name": "all_teams",
                "indexes": [
                    models.Index(fields=["team_id", "schema_id", "run_uuid"], name="sbda_run_idx"),
                    models.Index(fields=["destination_job_id"], name="sbda_dest_job_idx"),
                ],
                "constraints": [
                    models.UniqueConstraint(
                        fields=("team_id", "schema_id", "run_uuid", "batch_index", "destination_job_id"),
                        name="sbda_unique_batch_apply",
                    )
                ],
            },
            managers=[
                ("all_teams", django.db.models.manager.Manager()),
            ],
        ),
        migrations.CreateModel(
            name="SourceDestinationGroupLease",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("team_id", models.BigIntegerField(db_index=True)),
                ("schema_id", models.CharField(max_length=200)),
                ("destination_job_id", models.CharField(max_length=200)),
                (
                    "owner_token",
                    models.CharField(help_text="Per-pod identity (uuid4) of the current lease holder.", max_length=64),
                ),
                ("expires_at", models.DateTimeField()),
                ("acquired_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "sourcedestinationgrouplease",
                "abstract": False,
                "default_manager_name": "all_teams",
                "indexes": [models.Index(fields=["expires_at"], name="sdestgl_expires_at_idx")],
                "constraints": [
                    models.UniqueConstraint(
                        fields=("team_id", "schema_id", "destination_job_id"), name="sdestgl_team_schema_dest_uniq"
                    )
                ],
            },
            managers=[
                ("all_teams", django.db.models.manager.Manager()),
            ],
        ),
        migrations.CreateModel(
            name="SourceRunDestination",
            fields=[
                (
                    "id",
                    models.UUIDField(default=posthog.uuidt.uuid7, editable=False, primary_key=True, serialize=False),
                ),
                ("team_id", models.BigIntegerField(db_index=True)),
                ("schema_id", models.CharField(max_length=200)),
                ("source_id", models.CharField(max_length=200)),
                ("job_id", models.CharField(help_text="FK to ExternalDataJob (UUID as string).", max_length=200)),
                ("run_uuid", models.CharField(max_length=200)),
                (
                    "destination_job_id",
                    models.CharField(help_text="FK to ExternalDataDestinationJob (UUID as string).", max_length=200),
                ),
                ("destination_id", models.CharField(max_length=200)),
                ("destination_type", models.CharField(max_length=64)),
                ("config_snapshot", models.JSONField(blank=True, default=dict)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
            ],
            options={
                "db_table": "sourcerundestination",
                "abstract": False,
                "default_manager_name": "all_teams",
                "indexes": [
                    models.Index(fields=["run_uuid"], name="srd_run_uuid_idx"),
                    models.Index(fields=["team_id", "schema_id"], name="srd_team_schema_idx"),
                    models.Index(fields=["job_id"], name="srd_job_id_idx"),
                ],
                "constraints": [
                    models.UniqueConstraint(fields=("run_uuid", "destination_job_id"), name="srd_run_dest_uniq")
                ],
            },
            managers=[
                ("all_teams", django.db.models.manager.Manager()),
            ],
        ),
        # Partitioned tables: Django tracks the state, the RunPython below owns the DDL.
        migrations.SeparateDatabaseAndState(
            state_operations=[
                migrations.CreateModel(
                    name="SourceBatchDestination",
                    fields=[
                        (
                            "id",
                            models.UUIDField(
                                default=posthog.uuidt.uuid7, editable=False, primary_key=True, serialize=False
                            ),
                        ),
                        ("team_id", models.BigIntegerField(db_index=True)),
                        ("schema_id", models.CharField(max_length=200)),
                        ("run_uuid", models.CharField(max_length=200)),
                        ("destination_job_id", models.CharField(max_length=200)),
                        ("destination_type", models.CharField(max_length=64)),
                        ("batch_index", models.IntegerField()),
                        ("is_final_batch", models.BooleanField()),
                        (
                            "sync_type",
                            models.CharField(
                                choices=[
                                    ("full_refresh", "full_refresh"),
                                    ("incremental", "incremental"),
                                    ("append", "append"),
                                    ("cdc", "cdc"),
                                ],
                                max_length=32,
                            ),
                        ),
                        (
                            "latest_state",
                            models.CharField(
                                choices=[
                                    ("pending", "pending"),
                                    ("waiting", "waiting"),
                                    ("executing", "executing"),
                                    ("succeeded", "succeeded"),
                                    ("waiting_retry", "waiting_retry"),
                                    ("failed", "failed"),
                                ],
                                db_default="pending",
                                default="pending",
                                max_length=32,
                            ),
                        ),
                        ("latest_attempt", models.SmallIntegerField(db_default=0, default=0)),
                        ("state_changed_at", models.DateTimeField(blank=True, null=True)),
                        ("created_at", models.DateTimeField(auto_now_add=True)),
                        (
                            "batch",
                            models.ForeignKey(
                                db_constraint=False,
                                on_delete=django.db.models.deletion.DO_NOTHING,
                                related_name="destination_work_items",
                                to="warehouse_sources_queue.sourcebatch",
                            ),
                        ),
                    ],
                    options={
                        "db_table": "sourcebatchdestination",
                        "abstract": False,
                        "default_manager_name": "all_teams",
                    },
                    managers=[
                        ("all_teams", django.db.models.manager.Manager()),
                    ],
                ),
                migrations.CreateModel(
                    name="SourceBatchDestinationStatus",
                    fields=[
                        (
                            "id",
                            models.UUIDField(
                                default=posthog.uuidt.uuid7, editable=False, primary_key=True, serialize=False
                            ),
                        ),
                        (
                            "job_state",
                            models.CharField(
                                choices=[
                                    ("waiting", "waiting"),
                                    ("executing", "executing"),
                                    ("succeeded", "succeeded"),
                                    ("waiting_retry", "waiting_retry"),
                                    ("failed", "failed"),
                                ],
                                max_length=32,
                            ),
                        ),
                        ("attempt", models.SmallIntegerField(default=0)),
                        ("exec_time", models.DateTimeField(blank=True, null=True)),
                        ("error_response", models.JSONField(blank=True, null=True)),
                        ("created_at", models.DateTimeField(auto_now_add=True)),
                        (
                            "work_item",
                            models.ForeignKey(
                                db_constraint=False,
                                on_delete=django.db.models.deletion.DO_NOTHING,
                                related_name="statuses",
                                to="warehouse_sources_queue.sourcebatchdestination",
                            ),
                        ),
                    ],
                    options={
                        "db_table": "sourcebatchdestinationstatus",
                    },
                ),
                migrations.AddIndex(
                    model_name="sourcebatchdestination",
                    index=models.Index(
                        condition=models.Q(("latest_state__in", ["pending", "waiting_retry"])),
                        fields=["destination_type", "team_id", "created_at", "batch_index"],
                        name="sbd_claimable_idx",
                    ),
                ),
                migrations.AddIndex(
                    model_name="sourcebatchdestination",
                    index=models.Index(
                        condition=models.Q(("latest_state__in", ["executing", "waiting_retry", "failed"])),
                        fields=["run_uuid", "destination_job_id", "latest_state", "batch_index"],
                        name="sbd_run_gate_idx",
                    ),
                ),
                migrations.AddIndex(
                    model_name="sourcebatchdestination",
                    index=models.Index(
                        condition=models.Q(("latest_state", "executing")),
                        fields=["team_id", "schema_id", "destination_job_id"],
                        name="sbd_group_busy_idx",
                    ),
                ),
                migrations.AddIndex(
                    model_name="sourcebatchdestination",
                    index=models.Index(
                        condition=models.Q(("latest_state__in", ["pending", "waiting", "executing", "waiting_retry"])),
                        fields=["team_id", "schema_id", "destination_job_id", "created_at"],
                        name="sbd_incomplete_run_idx",
                    ),
                ),
                migrations.AddIndex(
                    model_name="sourcebatchdestination",
                    index=models.Index(fields=["destination_job_id"], name="sbd_dest_job_idx"),
                ),
                migrations.AddIndex(
                    model_name="sourcebatchdestination",
                    index=models.Index(fields=["batch_id", "destination_job_id"], name="sbd_batch_dest_idx"),
                ),
                migrations.AddIndex(
                    model_name="sourcebatchdestination",
                    index=models.Index(
                        condition=models.Q(("latest_state", "failed")),
                        fields=["state_changed_at"],
                        name="sbd_failed_changed_idx",
                    ),
                ),
                migrations.AddIndex(
                    model_name="sourcebatchdestinationstatus",
                    index=models.Index(
                        fields=["work_item_id", "-created_at", "-id", "job_state"],
                        name="sbds_item_desc_state_idx",
                    ),
                ),
            ],
            database_operations=[],
        ),
        migrations.RunPython(_create_partitioned_tables, _drop_partitioned_tables),
        migrations.RunPython(
            _create_latest_destination_status_view,
            lambda apps, schema_editor: schema_editor.execute(
                "DROP VIEW IF EXISTS v_latest_source_batch_destination_status"
            ),
        ),
    ]
