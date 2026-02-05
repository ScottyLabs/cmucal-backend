"""add unique constraint to courses.course_number

Revision ID: 7d654afb7791
Revises: c0f134426e07
Create Date: 2026-01-03 00:20:11.464347

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '7d654afb7791'
down_revision: Union[str, Sequence[str], None] = 'c0f134426e07'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_unique_constraint(
        "courses_course_number_key",
        "courses",
        ["course_number"],
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint(
        "courses_course_number_key",
        "courses",
        type_="unique",
    )
