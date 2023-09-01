#!/usr/bin/python3
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------------------

from sqlalchemy import MetaData, Table, Column, text
from sqlalchemy.dialects.mssql import INTEGER, NCHAR, NVARCHAR, TINYINT

metadata = MetaData()

AltNames = Table(
    "AltNames",
    metadata,
    Column("Pid", INTEGER, primary_key=True, autoincrement=True),
    Column("OldCode", NCHAR(19), nullable=False),
    Column("NewCode", NCHAR(19), nullable=False),
    Column("Level", NCHAR(1), nullable=False),
)
TRUNCATE_AltNames_SQL = text("TRUNCATE TABLE dbo.[AltNames]")

SocrBase = Table(
    "SocrBase",
    metadata,
    Column("Pid", INTEGER, primary_key=True, autoincrement=True),
    Column("Level", NVARCHAR(5), nullable=False),
    Column("ScName", NVARCHAR(10), nullable=True),
    Column("SocrName", NVARCHAR(29), nullable=True),
    Column("KodTST", NVARCHAR(3), nullable=False),
)
TRUNCATE_SocrBase_SQL = text("TRUNCATE TABLE dbo.[SocrBase]")

Kladr = Table(
    "Kladr",
    metadata,
    Column("Code", NCHAR(13), primary_key=True),
    Column("Name", NVARCHAR(40), nullable=False),
    Column("Socr", NVARCHAR(10), nullable=False),
    Column("Index", NVARCHAR(6), nullable=True),
    Column("Gninmb", NVARCHAR(4), nullable=False),
    Column("Uno", NVARCHAR(4), nullable=True),
    Column("Ocatd", NVARCHAR(11), nullable=False),
    Column("Status", TINYINT, nullable=False),
)
TRUNCATE_Kladr_SQL = text("TRUNCATE TABLE dbo.[Kladr]")

Street = Table(
    "Street",
    metadata,
    Column("Code", NCHAR(17), primary_key=True),
    Column("Name", NVARCHAR(40), nullable=False),
    Column("Socr", NVARCHAR(10), nullable=False),
    Column("Index", NVARCHAR(6), nullable=True),
    Column("Gninmb", NVARCHAR(4), nullable=False),
    Column("Uno", NVARCHAR(4), nullable=True),
    Column("Ocatd", NVARCHAR(11), nullable=False),
)
TRUNCATE_Street_SQL = text("TRUNCATE TABLE dbo.[Street]")

Doma = Table(
    "Doma",
    metadata,
    Column("Code", NCHAR(19), primary_key=True),
    Column("Name", NVARCHAR(40), nullable=False),
    Column("Korp", NVARCHAR(10), nullable=True),
    Column("Socr", NVARCHAR(10), nullable=False),
    Column("Index", NVARCHAR(6), nullable=True),
    Column("Gninmb", NVARCHAR(4), nullable=False),
    Column("Uno", NVARCHAR(4), nullable=True),
    Column("Ocatd", NVARCHAR(11), nullable=False),
)
TRUNCATE_Doma_SQL = text("TRUNCATE TABLE dbo.[Doma]")

NameMap = Table(
    "NameMap",
    metadata,
    Column("Code", NCHAR(19), primary_key=True),
    Column("Name", NVARCHAR(1000), nullable=False),
    Column("ShName", NVARCHAR(40), nullable=False),
    Column("ScName", NVARCHAR(10), nullable=False),
)
TRUNCATE_NameMap_SQL = text("TRUNCATE TABLE dbo.[NameMap]")


# ----------------------------------------------------------------------------------------
if __name__ == "__main__":
    pass
