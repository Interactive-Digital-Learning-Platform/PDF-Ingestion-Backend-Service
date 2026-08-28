"""Add qdrant_migrations tracking table

Revision ID: 53c71aeac252
Revises: 11e6756e0752
Create Date: 2026-08-25 00:00:01.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = '53c71aeac252'
down_revision: Union[str, Sequence[str], None] = '11e6756e0752'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Tracks which qdrant-migrations/ scripts have been applied — see
    # qdrant-migrations/run_migrations.py and dev-docs/generic-pdf-processing-service.md §9.
    # This table lives in Postgres (reusing the DB this service already owns) even though it
    # tracks changes to Qdrant, not Postgres — there is deliberately no ORM model for it, since
    # nothing in the FastAPI/Celery app ever reads or writes it; only the standalone migration
    # runner script touches this table.
    op.create_table(
        'qdrant_migrations',
        sa.Column('revision', sa.String(length=20), nullable=False),
        sa.Column('description', sa.String(length=255), nullable=False),
        sa.Column('applied_at', sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('revision'),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table('qdrant_migrations')
