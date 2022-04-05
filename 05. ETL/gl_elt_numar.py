import fn_sp_exec as sp
import datetime
import pandas as pd
import conn_db as db

starttime = datetime.datetime.now()

############################################## A. EXTRACT & LOAD NOMENCLATURE ##########################################################################################

tbl_name = "dbo.GL_INT_FURN"
sql_delete = "TRUNCATE TABLE dbo.GL_INT_FURN;"
sql_insert = "INSERT INTO dbo.GL_INT_FURN ( COD, NUME, MOD_DE) VALUES ( ?, ?, ? ) ;"
sql_count = "SELECT COUNT(*) FROM dbo.GL_INT_FURN;"

print("01 - START : UPLOAD INREGISTRARI IN TABELA DE " + tbl_name + " !")

src_int = pd.read_excel(
    r"C:\Users\i6542\Documents\07. Baza de Date\03. Productiv\02. GL_MD\03 TABEL INT\01 NUMAR\GL_INT_FURN.xlsx",
    sheet_name="FURN",
    converters={
        "COD": str,
        "NUME": str,
        "MOD_DE": str,
    },
)
nr_int_src = str(len(src_int.index))
df = pd.DataFrame(src_int, columns=["COD", "NUME", "MOD_DE"])
src_calc = [tuple(x) for x in df.values]

print(
    "02 - NUMAR DE INREGISTRARI : "
    + nr_int_src
    + " DE PROCESAT IN TABELA : "
    + tbl_name
    + " !"
)

with db.azure_conn() as conn:
    conn.execute(sql_delete)

with db.azure_conn() as conn:
    conn.fast_executemany = True
    conn.executemany(sql_insert, src_calc)

with db.azure_conn() as conn:
    conn.execute(sql_count)
    nr_tgt = conn.fetchone()
    print("03 - NUMAR INREGISTRARI PROCESATE : " + str(nr_tgt[0]))

print("99 - FINISH : UPLOAD INREGISTRARI IN TABELA " + tbl_name + " !")

############################################## B. EXTRACT & LOAD GAINED CUSTOMERS ##########################################################################################

tbl_name = "dbo.GL_INT_NUMAR_GAINED"
sql_delete = "TRUNCATE TABLE dbo.GL_INT_NUMAR_GAINED;"
sql_insert = (
    "INSERT INTO dbo.GL_INT_NUMAR_GAINED (  DENUMIRE_SURSA, DIVIZIE, DENUMIREPARTENER, PARTENERDEAFACERI, SEGMENTCLIENT, SUBSEGMENT, IDMANAGER, "
    "DENUMIREMANAGER, PUNCTDECONSUM, NUMARCONTRACT, DATAMOVIN, CATEGORIETARIF, DENUMIREDISTRIBUITOR_CHEIE, "
    "INSTALATIE, FURNIZOR_NOU_ANTERIOR_CHEIE, INVOICING_PARTY, CUI_CNP, MOTIVMOV_IN_COD, MOTIVMOV_IN_DESC, "
    "CONTCONTRACT, DATACREARE, URBANRURAL, JUDET, MOD_DE)"
    "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ; "
)
sql_count = "SELECT COUNT(*) FROM dbo.GL_INT_NUMAR_GAINED;"

print("01 - START : UPLOAD INREGISTRARI IN TABELA DE " + tbl_name + " !")

src_int = pd.read_excel(
    r"C:\Users\i6542\Documents\07. Baza de Date\03. Productiv\02. GL_MD\03 TABEL INT\01 NUMAR\GL_INT_NUMAR_GAINED.xlsx",
    sheet_name="NUMAR_GAINED",
    converters={
        "DENUMIRE_SURSA": str,
        "DIVIZIE": str,
        "DENUMIREPARTENER": str,
        "PARTENERDEAFACERI": str,
        "SEGMENTCLIENT": str,
        "SUBSEGMENT": str,
        "IDMANAGER": str,
        "DENUMIREMANAGER": str,
        "PUNCTDECONSUM": str,
        "NUMARCONTRACT": str,
        "DATAMOVIN": str,
        "CATEGORIETARIF": str,
        "DENUMIREDISTRIBUITOR_CHEIE": str,
        "INSTALATIE": str,
        "FURNIZOR_NOU_ANTERIOR_CHEIE": str,
        "INVOICING_PARTY": str,
        "CUI_CNP": str,
        "MOTIVMOV_IN_COD": str,
        "MOTIVMOV_IN_DESC": str,
        "CONTCONTRACT": str,
        "DATACREARE": str,
        "URBANRURAL": str,
        "JUDET": str,
        "MOD_DE": str,
    },
)

nr_int_src = str(len(src_int.index))
df = pd.DataFrame(
    src_int,
    columns=[
        "DENUMIRE_SURSA",
        "DIVIZIE",
        "DENUMIREPARTENER",
        "PARTENERDEAFACERI",
        "SEGMENTCLIENT",
        "SUBSEGMENT",
        "IDMANAGER",
        "DENUMIREMANAGER",
        "PUNCTDECONSUM",
        "NUMARCONTRACT",
        "DATAMOVIN",
        "CATEGORIETARIF",
        "DENUMIREDISTRIBUITOR_CHEIE",
        "INSTALATIE",
        "FURNIZOR_NOU_ANTERIOR_CHEIE",
        "INVOICING_PARTY",
        "CUI_CNP",
        "MOTIVMOV_IN_COD",
        "MOTIVMOV_IN_DESC",
        "CONTCONTRACT",
        "DATACREARE",
        "URBANRURAL",
        "JUDET",
        "MOD_DE",
    ],
).fillna("")
src_calc = [tuple(x) for x in df.values]

print(
    "02 - NUMAR DE INREGISTRARI : "
    + nr_int_src
    + " DE PROCESAT IN TABELA : "
    + tbl_name
    + " !"
)

with db.azure_conn() as conn:
    conn.execute(sql_delete)

with db.azure_conn() as conn:
    conn.fast_executemany = True
    conn.executemany(sql_insert, src_calc)

with db.azure_conn() as conn:
    conn.execute(sql_count)
    nr_tgt = conn.fetchone()
    print("03 - NUMAR INREGISTRARI PROCESATE : " + str(nr_tgt[0]))

print("99 - FINISH : UPLOAD INREGISTRARI IN TABELA " + tbl_name + " !")

############################################## C. EXTRACT & LOAD LOST CUSTOMERS ##########################################################################################

tbl_name = "dbo.GL_INT_NUMAR_LOST"
sql_delete = "TRUNCATE TABLE dbo.GL_INT_NUMAR_LOST;"
sql_insert = (
    "INSERT INTO dbo.GL_INT_NUMAR_LOST ( DENUMIRE_SURSA, DIVIZIE, DENUMIREPARTENER, PARTENERDEAFACERI, SEGMENTCLIENT, SUBSEGMENT, IDMANAGER, DENUMIREMANAGER,"
    "PUNCTDECONSUM, NUMARCONTRACT, DATAMOVIN, DATAMOVOUT, CATEGORIETARIF, DENUMIREDISTRIBUITOR_CHEIE, INSTALATIE,"
    "FURNIZOR_NOU_ANTERIOR_CHEIE, INVOICING_PARTY, CODCOMPANIE, CONTCONTRACT, CUI_CNP, MOTIVMOV_IN_COD, MOTIVMOV_IN_DESC,"
    "DATACREARE, JUDET, URBANRURAL, MOD_DE )"
    "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ; "
)
sql_count = "SELECT COUNT(*) FROM dbo.GL_INT_NUMAR_LOST;"

print("01 - START : UPLOAD INREGISTRARI IN TABELA DE " + tbl_name + " !")

src_int = pd.read_excel(
    r"C:\Users\i6542\Documents\07. Baza de Date\03. Productiv\02. GL_MD\03 TABEL INT\01 NUMAR\GL_INT_NUMAR_LOST.xlsx",
    sheet_name="NUMAR_LOST",
    converters={
        "DENUMIRE_SURSA": str,
        "DIVIZIE": str,
        "DENUMIREPARTENER": str,
        "PARTENERDEAFACERI": str,
        "SEGMENTCLIENT": str,
        "SUBSEGMENT": str,
        "IDMANAGER": str,
        "DENUMIREMANAGER": str,
        "PUNCTDECONSUM": str,
        "NUMARCONTRACT": str,
        "DATAMOVIN": str,
        "DATAMOVOUT": str,
        "CATEGORIETARIF": str,
        "DENUMIREDISTRIBUITOR_CHEIE": str,
        "INSTALATIE": str,
        "FURNIZOR_NOU_ANTERIOR_CHEIE": str,
        "INVOICING_PARTY": str,
        "CODCOMPANIE": str,
        "CONTCONTRACT": str,
        "CUI_CNP": str,
        "MOTIVMOV_IN_COD": str,
        "MOTIVMOV_IN_DESC": str,
        "DATACREARE": str,
        "JUDET": str,
        "URBANRURAL": str,
        "MOD_DE": str,
    },
)
nr_int_src = str(len(src_int.index))
df = pd.DataFrame(
    src_int,
    columns=[
        "DENUMIRE_SURSA",
        "DIVIZIE",
        "DENUMIREPARTENER",
        "PARTENERDEAFACERI",
        "SEGMENTCLIENT",
        "SUBSEGMENT",
        "IDMANAGER",
        "DENUMIREMANAGER",
        "PUNCTDECONSUM",
        "NUMARCONTRACT",
        "DATAMOVIN",
        "DATAMOVOUT",
        "CATEGORIETARIF",
        "DENUMIREDISTRIBUITOR_CHEIE",
        "INSTALATIE",
        "FURNIZOR_NOU_ANTERIOR_CHEIE",
        "INVOICING_PARTY",
        "CODCOMPANIE",
        "CONTCONTRACT",
        "CUI_CNP",
        "MOTIVMOV_IN_COD",
        "MOTIVMOV_IN_DESC",
        "DATACREARE",
        "JUDET",
        "URBANRURAL",
        "MOD_DE",
    ],
).fillna("")
src_calc = [tuple(x) for x in df.values]

print(
    "02 - NUMAR DE INREGISTRARI : "
    + nr_int_src
    + " DE PROCESAT IN TABELA : "
    + tbl_name
    + " !"
)

with db.azure_conn() as conn:
    conn.execute(sql_delete)

with db.azure_conn() as conn:
    conn.fast_executemany = True
    conn.executemany(sql_insert, src_calc)

with db.azure_conn() as conn:
    conn.execute(sql_count)
    nr_tgt = conn.fetchone()
    print("03 - NUMAR INREGISTRARI PROCESATE : " + str(nr_tgt[0]))

print("99 - FINISH : UPLOAD INREGISTRARI IN TABELA " + tbl_name + " !")
print(
    "_________________________________________________________________________________________________________________________"
)
############################################## TRANSFORM AND PROCESS QUANTITY FOR B2B ########################################################################

tbl_name = "dbo.master_GL_INREG_NR_GAINED"
sp.fn_sp_exec(tbl_name)
print(
    "_________________________________________________________________________________________________________________________"
)
tbl_name = "dbo.master_GL_INREG_NR_LOST"
sp.fn_sp_exec(tbl_name)


endtime = datetime.datetime.now()
print("TIMP PROCESARE  : ", endtime - starttime)
