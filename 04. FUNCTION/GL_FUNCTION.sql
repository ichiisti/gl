CREATE FUNCTION [dbo].[fn_GL_LOG] 
( 
@SP_NUME CHAR(100)
)
RETURNS TABLE 
AS
RETURN

	
	SELECT AN, LUNA, [OBJECT_NAME], DATA_RUN, USER_RUN, MESAGE_TYPE, MESAGE_TEXT, ORDINE
	FROM dbo.GL_LOG_C
	WHERE [OBJECT_NAME] IN 
							( 
							  SELECT DISTINCT referenced_entity_name 
							  FROM sys.dm_sql_referenced_entities ( @SP_NUME , 'OBJECT')
								UNION ALL
							  SELECT REPLACE(@SP_NUME,'dbo.','')
							)                                                                                  
	GROUP BY AN, LUNA, [OBJECT_NAME], DATA_RUN, USER_RUN, MESAGE_TYPE, MESAGE_TEXT, ORDINE
GO
