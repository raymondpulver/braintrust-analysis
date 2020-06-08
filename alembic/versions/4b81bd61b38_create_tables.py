"""create tables

Revision ID: 4b81bd61b38
Revises: None
Create Date: 2020-06-05 19:10:25.173140

"""

# revision identifiers, used by Alembic.
revision = "4b81bd61b38"
down_revision = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.create_table(
        "bars_1_min",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("ts", sa.Integer),
        sa.Column("exchange", sa.CHAR(8)),
        sa.Column("symbol", sa.CHAR(12)),
        sa.Column("open", sa.FLOAT(8)),
        sa.Column("high", sa.FLOAT(8)),
        sa.Column("low", sa.FLOAT(8)),
        sa.Column("close", sa.FLOAT(8)),
        sa.Column("volume", sa.FLOAT(8)),
        sa.Column("optional1", sa.FLOAT(8)),
    )

    op.create_table(
        "perpfunding",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("ts", sa.Integer),
        sa.Column("exchange", sa.CHAR(8), nullable=False),
        sa.Column("symbol", sa.CHAR(12), nullable=False),
        sa.Column("ts", sa.Integer, nullable=False),
        sa.Column("value", sa.FLOAT(8), nullable=False),
        sa.Column("optional1", sa.FLOAT(8)),
    )

    pass


def downgrade():

    op.drop_table("bitmex",)
    op.drop_table("binance",)
    op.drop_table("binance_futures")
    op.drop_table("perpfunding",)
    pass
