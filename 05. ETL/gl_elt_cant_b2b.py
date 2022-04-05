import fn_sp_exec as sp
import datetime
import pandas as pd
import conn_db as db


tbl_name = "dbo.GL_INT_CANT_B2B"
sql_delete = "TRUNCATE TABLE dbo.GL_INT_CANT_B2B;"
sql_insert = "INSERT INTO dbo.GL_INT_CANT_B2B ( TIP_SURSA, INSTALATIE, MOTIVMOV_OUT_IN_TEXT, FURNIZOR_NOU_ANTERIOR_TEXT, REGIUNE_PORTOFOLIU, TIP_PA, CONSUM_AN,  MOD_DE) VALUES(?, ?, ?, ?, ?, ?, ?, ?) ;"
sql_count = "SELECT COUNT(*) FROM dbo.GL_INT_CANT_B2B"


############################################## EXTRACT CANTITATE B2B ##########################################################################################

starttime = datetime.datetime.now()
print("01 - START : UPLOAD INREGISTRARI IN TABELA DE " + tbl_name + " !")

src_int = pd.read_excel(
    r"FILE PATH",
    sheet_name="CANT_B2B",
    converters={
        "TIP_SURSA": str,
        "INSTALATIE": str,
        "MOTIVMOV_OUT_IN_TEXT": str,
        "FURNIZOR_NOU_ANTERIOR_TEXT": str,
        "REGIUNE_PORTOFOLIU": str,
        "TIP_PA": str,
        "CONSUM_AN": float,
        "MOD_DE": str,
    },
)

nr_int_src = str(len(src_int.index))
df = pd.DataFrame(
    src_int,
    columns=[
        "TIP_SURSA",
        "INSTALATIE",
        "MOTIVMOV_OUT_IN_TEXT",
        "FURNIZOR_NOU_ANTERIOR_TEXT",
        "REGIUNE_PORTOFOLIU",
        "TIP_PA",
        "CONSUM_AN",
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

############################################## LOAD QUANTITY FOR B2B ##########################################################################################

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

tbl_name = "dbo.master_GL_INREG_CANT_B2B"
sp.fn_sp_exec(tbl_name)


endtime = datetime.datetime.now()
print("TIMP PROCESARE  : ", endtime - starttime)
