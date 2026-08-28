#!/usr/bin/env python3
"""Manual Qdrant schema migration runner.

Collections are never created by the API or Celery worker at runtime (see
dev-docs/generic-pdf-processing-service.md §9) — a job whose `qdrant_collection` doesn't already
exist fails clearly instead of silently auto-creating it. Collections are created here, by hand,
the same way Postgres schema changes are applied by hand via Alembic:

    uv run alembic upgrade head                          # Postgres migrations (existing)
    uv run python qdrant-migrations/run_migrations.py upgrade   # Qdrant migrations (this)

Commands:
    upgrade    Apply every pending migration, in order.
    downgrade  Revert only the single most-recently-applied migration.
    status     List every discovered migration and whether it's applied or pending.
"""

import argparse

from _lib.runner_support import (
    discover_migrations,
    get_applied_revisions,
    get_pg_engine,
    get_qdrant_client,
    record_applied,
    remove_applied,
)


def cmd_upgrade() -> None:
    engine = get_pg_engine()
    client = get_qdrant_client()
    applied = get_applied_revisions(engine)

    pending = [m for m in discover_migrations() if m["revision"] not in applied]
    if not pending:
        print("Nothing to apply — all migrations already applied.")
        return

    for migration in pending:
        print(f"Applying {migration['revision']} — {migration['description']}...")
        migration["upgrade"](client)
        record_applied(engine, migration["revision"], migration["description"])
        print("  done.")


def cmd_downgrade() -> None:
    engine = get_pg_engine()
    client = get_qdrant_client()
    applied = get_applied_revisions(engine)

    candidates = [m for m in discover_migrations() if m["revision"] in applied]
    if not candidates:
        print("Nothing to downgrade — no migrations are applied.")
        return

    migration = candidates[-1]  # most recently applied, by revision order
    print(f"Reverting {migration['revision']} — {migration['description']}...")
    migration["downgrade"](client)
    remove_applied(engine, migration["revision"])
    print("  done.")


def cmd_status() -> None:
    engine = get_pg_engine()
    applied = get_applied_revisions(engine)

    for migration in discover_migrations():
        state = "applied" if migration["revision"] in applied else "pending"
        print(f"{migration['revision']}  [{state:8}]  {migration['description']}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Manual Qdrant schema migrations.")
    parser.add_argument("command", choices=["upgrade", "downgrade", "status"])
    args = parser.parse_args()

    commands = {"upgrade": cmd_upgrade, "downgrade": cmd_downgrade, "status": cmd_status}
    commands[args.command]()


if __name__ == "__main__":
    main()
