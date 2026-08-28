"""Generalize jobs for external callers

Revision ID: 11e6756e0752
Revises: 54430a1edae3
Create Date: 2026-08-25 00:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = '11e6756e0752'
down_revision: Union[str, Sequence[str], None] = '54430a1edae3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        'jobs',
        sa.Column(
            'source_service',
            sa.String(length=100),
            nullable=False,
            server_default='pdf-ingestion-web-application',
        ),
    )
    op.add_column('jobs', sa.Column('external_reference_id', sa.String(length=255), nullable=True))
    op.add_column('jobs', sa.Column('caller_metadata', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.add_column('jobs', sa.Column('max_pages', sa.Integer(), nullable=True))
    op.add_column(
        'jobs',
        sa.Column('callback_required', sa.Boolean(), nullable=False, server_default='false'),
    )
    op.add_column('jobs', sa.Column('callback_status', sa.String(length=20), nullable=True))
    op.add_column(
        'jobs',
        sa.Column('callback_attempts', sa.Integer(), nullable=False, server_default='0'),
    )
    op.add_column('jobs', sa.Column('callback_last_error', sa.Text(), nullable=True))
    op.add_column('jobs', sa.Column('callback_delivered_at', sa.DateTime(timezone=True), nullable=True))
    op.add_column('jobs', sa.Column('error_code', sa.String(length=50), nullable=True))

    op.create_index(op.f('ix_jobs_source_service'), 'jobs', ['source_service'], unique=False)
    op.create_index(
        op.f('ix_jobs_external_reference_id'), 'jobs', ['external_reference_id'], unique=False
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(op.f('ix_jobs_external_reference_id'), table_name='jobs')
    op.drop_index(op.f('ix_jobs_source_service'), table_name='jobs')

    op.drop_column('jobs', 'error_code')
    op.drop_column('jobs', 'callback_delivered_at')
    op.drop_column('jobs', 'callback_last_error')
    op.drop_column('jobs', 'callback_attempts')
    op.drop_column('jobs', 'callback_status')
    op.drop_column('jobs', 'callback_required')
    op.drop_column('jobs', 'max_pages')
    op.drop_column('jobs', 'caller_metadata')
    op.drop_column('jobs', 'external_reference_id')
    op.drop_column('jobs', 'source_service')
