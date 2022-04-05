import conn_db as db
import fn_qoute as q
import pandas as pd

pd.set_option("display.width", 500)
pd.set_option("display.max_colwidth", 500)
pd.set_option("display.max_rows", 100)
pd.set_option("display.max_columns", 5)


def fn_sp_exec(tbl_name):

    sql_rezult = (
        "SELECT  per.AN, per.LUNA, lg.DATA_RUN, lg.MESAGE_TYPE, lg.MESAGE_TEXT, lg.ORDINE \
            FROM    ( SELECT AN, LUNA FROM vw_GL_LOST_PER GROUP BY AN, LUNA ) per,\
                    ( SELECT AN, LUNA, DATA_RUN, MESAGE_TYPE, MESAGE_TEXT, ORDINE FROM fn_GL_LOG ("
        + q.single_quote(tbl_name)
        + ") GROUP BY AN, LUNA, DATA_RUN, MESAGE_TYPE, MESAGE_TEXT, ORDINE ) lg \
            WHERE per.AN=lg.AN AND per.LUNA=lg.LUNA GROUP BY per.AN, per.LUNA,lg.DATA_RUN, lg.MESAGE_TYPE, lg.MESAGE_TEXT, lg.ORDINE \
            ORDER BY lg.ORDINE DESC;"
    )

    sql_cmd = (
        "DECLARE @AN SMALLINT ;\
           DECLARE @LUNA SMALLINT;\
           DECLARE c1 CURSOR FOR\
					  SELECT AN, LUNA\
					  FROM vw_GL_LOST_PER \
					  GROUP BY AN, LUNA\
            OPEN c1;\
            FETCH NEXT FROM c1 INTO @AN, @LUNA ;\
            WHILE @@FETCH_STATUS=0\
                BEGIN\
                    EXEC "
        + tbl_name
        + " @AN, @LUNA;\
                    FETCH NEXT FROM c1 INTO @AN, @LUNA ;\
                END\
            CLOSE c1;\
            DEALLOCATE c1;"
    )

    with db.azure_conn() as conn:
        conn.execute(sql_cmd)

    with db.azure_sql() as conn:
        sql_rezult = pd.read_sql(sql_rezult, conn)

    df = pd.DataFrame(
        sql_rezult, columns=["AN", "LUNA", "DATA_RUN", "MESAGE_TEXT", "ORDINE"]
    )

    print(df.head(20))

