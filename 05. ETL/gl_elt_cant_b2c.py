import fn_sp_exec as sp
import datetime
import pandas as pd
import conn_db as db

starttime = datetime.datetime.now()

############################################## A. EXTRACT & LOAD AVG QUANTITY #################################################################################

tbl_name = "dbo.GL_INT_CANT_MED_TOTAL"
sql_delete = "TRUNCATE TABLE dbo.GL_INT_CANT_MED_TOTAL"
sql_insert = "INSERT INTO dbo.GL_INT_CANT_MED_TOTAL ( DIVIZIE, SEGMENTCLIENT, CANT_MED_AN, MOD_DE) VALUES ( ?, ?, ?, ? ) ;"
sql_count = "SELECT COUNT(*) FROM dbo.GL_INT_CANT_MED_TOTAL"

print("01 - START : UPLOAD INREGISTRARI IN TABELA DE " + tbl_name + " !")

src_int = pd.read_excel(
    r"C:\Users\i6542\Documents\07. Baza de Date\03. Productiv\02. GL_MD\03 TABEL INT\02 CANT B2C\GL_INT_CANT_MED_TOTAL.xlsx",
    sheet_name="CANT_MED_AN",
    converters={
        "DIVIZIE": str,
        "SEGMENTCLIENT": str,
        "CANT_MED_AN": float,
        "MOD_DE": str,
    },
)

nr_int_src = str(len(src_int.index))
df = pd.DataFrame(
    src_int, columns=["DIVIZIE", "SEGMENTCLIENT", "CANT_MED_AN", "MOD_DE"]
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


print("04 - FINISH : UPLOAD INREGISTRARI IN TABELA " + tbl_name + " !")

############################################## B. EXTRACT & LOAD AVG QUANTITY FOR COUNTY ################################################################################

tbl_name = "dbo.GL_INT_CANT_MED_JUDET"
sql_delete = "TRUNCATE TABLE dbo.GL_INT_CANT_MED_JUDET;"
sql_insert = "INSERT INTO dbo.GL_INT_CANT_MED_JUDET ( DIVIZIE, SEGMENTCLIENT, JUDET, CANT_MED_AN, MOD_DE) VALUES ( ?, ?, ?, ?, ? ) ;"
sql_count = "SELECT COUNT(*) FROM dbo.GL_INT_CANT_MED_JUDET"

print("01 - START : UPLOAD INREGISTRARI IN TABELA DE " + tbl_name + " !")

src_int = pd.read_excel(
    r"C:\Users\i6542\Documents\07. Baza de Date\03. Productiv\02. GL_MD\03 TABEL INT\02 CANT B2C\GL_INT_CANT_MED_JUDET.xlsx",
    sheet_name="CANT_MED_JUDET",
    converters={
        "DIVIZIE": str,
        "SEGMENTCLIENT": str,
        "JUDET": str,
        "CANT_MED_AN": float,
        "MOD_DE": str,
    },
)

nr_int_src = str(len(src_int.index))
df = pd.DataFrame(
    src_int, columns=["DIVIZIE", "SEGMENTCLIENT", "JUDET", "CANT_MED_AN", "MOD_DE"]
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


print("04 - FINISH : UPLOAD INREGISTRARI IN TABELA " + tbl_name + " !")

############################################## C. EXTRACT & LOAD LOST QUANTITY #########################################################################################

tbl_name = "dbo.GL_INT_CANT_B2C_LOST"
sql_delete = "TRUNCATE TABLE dbo.GL_INT_CANT_B2C_LOST;"
sql_insert = "INSERT INTO dbo.GL_INT_CANT_B2C_LOST ( INSTALATIE, CONSUM_AN, MOD_DE) VALUES ( ?, ?, ? ) ;"
sql_count = "SELECT COUNT(*) FROM dbo.GL_INT_CANT_B2C_LOST"


print("01 - START : UPLOAD INREGISTRARI IN TABELA DE " + tbl_name + " !")

src_int = pd.read_excel(
    r"C:\Users\i6542\Documents\07. Baza de Date\03. Productiv\02. GL_MD\03 TABEL INT\02 CANT B2C\GL_INT_CANT_B2C_LOST.xlsx",
    sheet_name="CANT_B2C_LOST",
    converters={
        "INSTALATIE": str,
        "CONSUM_AN": float,
        "MOD_DE": str,
    },
)

nr_int_src = str(len(src_int.index))
df = pd.DataFrame(src_int, columns=["INSTALATIE", "CONSUM_AN", "MOD_DE"]).fillna("")
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


print("04 - FINISH : UPLOAD INREGISTRARI IN TABELA " + tbl_name + " !")
print(
    "_________________________________________________________________________________________________________________________"
)
############################################## TRANSFORM AND PROCESS QUANTITY FOR B2B ########################################################################

tbl_name = "dbo.master_GL_INREG_CANT_B2C"
sp.fn_sp_exec(tbl_name)


endtime = datetime.datetime.now()
print("TIMP PROCESARE  : ", endtime - starttime)
