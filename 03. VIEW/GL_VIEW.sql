CREATE VIEW [dbo].[vw_GL_GAINED_PER]
AS
SELECT DISTINCT YEAR(CONVERT(DATE, DATAMOVIN, 104)) AS AN, MONTH(CONVERT(DATE, DATAMOVIN, 104)) AS LUNA
FROM            dbo.GL_INT_NUMAR_GAINED
GO
/

CREATE VIEW [dbo].[vw_GL_LOST_PER]
AS
SELECT DISTINCT YEAR(CONVERT(DATE, DATAMOVOUT, 104)) AS AN, MONTH(CONVERT(DATE, DATAMOVOUT, 104)) AS LUNA
FROM            dbo.GL_INT_NUMAR_LOST
GO
