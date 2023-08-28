#!/usr/bin/python3
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------------------

from sqlalchemy import MetaData, Table, Column, text
from sqlalchemy.dialects.mssql import INTEGER, NCHAR, NVARCHAR, TINYINT

metadata = MetaData()

AltNames = Table(
    "AltNames",
    metadata,
    Column("pid", INTEGER, primary_key=True, autoincrement=True),
    Column("oldcode", NCHAR(19), nullable=False),
    Column("newcode", NCHAR(19), nullable=False),
    Column("level", NCHAR(1), nullable=False),
)
TRUNCATE_AltNames_SQL = text("TRUNCATE TABLE dbo.[AltNames]")

SocrBase = Table(
    "SocrBase",
    metadata,
    Column("pid", INTEGER, primary_key=True, autoincrement=True),
    Column("level", NVARCHAR(5), nullable=False),
    Column("scname", NVARCHAR(10), nullable=True),
    Column("socrname", NVARCHAR(29), nullable=True),
    Column("kod_t_st", NVARCHAR(3), nullable=False),
)
TRUNCATE_SocrBase_SQL = text("TRUNCATE TABLE dbo.[SocrBase]")

Kladr = Table(
    "Kladr",
    metadata,
    Column("code", NCHAR(13), primary_key=True),
    Column("name", NVARCHAR(40), nullable=False),
    Column("socr", NVARCHAR(10), nullable=False),
    Column("index", NVARCHAR(6), nullable=True),
    Column("gninmb", NVARCHAR(4), nullable=False),
    Column("uno", NVARCHAR(4), nullable=True),
    Column("ocatd", NVARCHAR(11), nullable=False),
    Column("status", TINYINT, nullable=False),
)
TRUNCATE_Kladr_SQL = text("TRUNCATE TABLE dbo.[Kladr]")

Street = Table(
    "Street",
    metadata,
    Column("code", NCHAR(17), primary_key=True),
    Column("name", NVARCHAR(40), nullable=False),
    Column("socr", NVARCHAR(10), nullable=False),
    Column("index", NVARCHAR(6), nullable=True),
    Column("gninmb", NVARCHAR(4), nullable=False),
    Column("uno", NVARCHAR(4), nullable=True),
    Column("ocatd", NVARCHAR(11), nullable=False),
)
TRUNCATE_Street_SQL = text("TRUNCATE TABLE dbo.[Street]")

Doma = Table(
    "Doma",
    metadata,
    Column("code", NCHAR(19), primary_key=True),
    Column("name", NVARCHAR(40), nullable=False),
    Column("korp", NVARCHAR(10), nullable=True),
    Column("socr", NVARCHAR(10), nullable=False),
    Column("index", NVARCHAR(6), nullable=True),
    Column("gninmb", NVARCHAR(4), nullable=False),
    Column("uno", NVARCHAR(4), nullable=True),
    Column("ocatd", NVARCHAR(11), nullable=False),
)
TRUNCATE_Doma_SQL = text("TRUNCATE TABLE dbo.[Doma]")

NameMap = Table(
    "NameMap",
    metadata,
    Column("code", NCHAR(19), primary_key=True),
    Column("name", NVARCHAR(1000), nullable=False),
    Column("shname", NVARCHAR(40), nullable=False),
    Column("scname", NVARCHAR(10), nullable=False),
)
TRUNCATE_NameMap_SQL = text("TRUNCATE TABLE dbo.[NameMap]")


# ----------------------------------------------------------------------------------------
if __name__ == "__main__":
    pass
