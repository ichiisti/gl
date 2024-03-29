CREATE TABLE [dbo].[GL_INT_CANT_B2B](
	[TIP_SURSA] [varchar](255) NOT NULL,
	[INSTALATIE] [varchar](255) NOT NULL,
	[FURNIZOR_NOU_ANTERIOR_TEXT] [varchar](255) NULL,
	[MOTIVMOV_OUT_IN_TEXT] [varchar](255) NOT NULL,
	[REGIUNE_PORTOFOLIU] [varchar](255) NULL,
	[TIP_PA] [varchar](255) NULL,
	[CONSUM_AN] [float] NULL,
	[MOD_DE] [varchar](255) NOT NULL,
	[MOD_TIMP] [datetime] NULL,
 CONSTRAINT [PK_GL_INT_CANT_B2B] PRIMARY KEY CLUSTERED 
(
	[TIP_SURSA] ASC,
	[INSTALATIE] ASC
)WITH (STATISTICS_NORECOMPUTE = ON, IGNORE_DUP_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/

CREATE TABLE [dbo].[GL_INT_CANT_B2C_LOST](
	[INSTALATIE] [varchar](255) NOT NULL,
	[CONSUM_AN] [float] NOT NULL,
	[MOD_DE] [varchar](255) NOT NULL,
	[MOD_TIMP] [datetime] NULL,
 CONSTRAINT [PK_GL_INT_CANT_B2C_LOST] PRIMARY KEY CLUSTERED 
(
	[INSTALATIE] ASC
)WITH (STATISTICS_NORECOMPUTE = ON, IGNORE_DUP_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/

CREATE TABLE [dbo].[GL_INT_CANT_MED_JUDET](
	[DIVIZIE] [varchar](255) NOT NULL,
	[SEGMENTCLIENT] [varchar](255) NOT NULL,
	[JUDET] [varchar](255) NOT NULL,
	[CANT_MED_AN] [float] NOT NULL,
	[MOD_DE] [varchar](255) NOT NULL,
	[MOD_TIMP] [datetime] NULL,
 CONSTRAINT [PK_GL_INT_CANT_MED_JUDET] PRIMARY KEY CLUSTERED 
(
	[DIVIZIE] ASC,
	[SEGMENTCLIENT] ASC,
	[JUDET] ASC
)WITH (STATISTICS_NORECOMPUTE = ON, IGNORE_DUP_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/

CREATE TABLE [dbo].[GL_INT_CANT_MED_TOTAL](
	[DIVIZIE] [varchar](255) NOT NULL,
	[SEGMENTCLIENT] [varchar](255) NOT NULL,
	[CANT_MED_AN] [float] NOT NULL,
	[MOD_DE] [varchar](255) NOT NULL,
	[MOD_TIMP] [datetime] NULL,
 CONSTRAINT [PK_GL_INT_CANT_MED_TOTAL] PRIMARY KEY CLUSTERED 
(
	[DIVIZIE] ASC,
	[SEGMENTCLIENT] ASC
)WITH (STATISTICS_NORECOMPUTE = ON, IGNORE_DUP_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/

CREATE TABLE [dbo].[GL_INT_FURN](
	[COD] [varchar](255) NOT NULL,
	[NUME] [varchar](255) NOT NULL,
	[MOD_DE] [varchar](255) NOT NULL,
	[MOD_TIMP] [datetime] NULL,
 CONSTRAINT [PK_GL_INT_FURN] PRIMARY KEY CLUSTERED 
(
	[COD] ASC
)WITH (STATISTICS_NORECOMPUTE = ON, IGNORE_DUP_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/

CREATE TABLE [dbo].[GL_INT_MOTIVMOV_IN](
	[COD] [varchar](3) NOT NULL,
	[NUME] [nvarchar](100) NOT NULL,
	[MOTIV_GROUP] [varchar](50) NULL,
	[MOD_DE] [varchar](20) NOT NULL,
	[MOD_TIMP] [datetime] NULL,
 CONSTRAINT [PK_GL_INT_MOTIVMOV_IN] PRIMARY KEY CLUSTERED 
(
	[COD] ASC
)WITH (STATISTICS_NORECOMPUTE = ON, IGNORE_DUP_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/

CREATE TABLE [dbo].[GL_INT_MOTIVMOV_OUT](
	[COD] [varchar](3) NOT NULL,
	[NUME] [nvarchar](100) NOT NULL,
	[MOTIV_GROUP] [varchar](50) NULL,
	[MOD_DE] [varchar](20) NOT NULL,
	[MOD_TIMP] [datetime] NULL,
 CONSTRAINT [PK_GL_INT_MOTIVMOV_OUT] PRIMARY KEY CLUSTERED 
(
	[COD] ASC
)WITH (STATISTICS_NORECOMPUTE = ON, IGNORE_DUP_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/

CREATE TABLE [dbo].[GL_INT_NUMAR_GAINED](
	[DENUMIRE_SURSA] [varchar](255) NULL,
	[DIVIZIE] [varchar](255) NULL,
	[DENUMIREPARTENER] [varchar](255) NULL,
	[PARTENERDEAFACERI] [varchar](255) NULL,
	[SEGMENTCLIENT] [varchar](255) NULL,
	[SUBSEGMENT] [varchar](255) NULL,
	[IDMANAGER] [varchar](255) NULL,
	[DENUMIREMANAGER] [varchar](255) NULL,
	[PUNCTDECONSUM] [varchar](255) NULL,
	[NUMARCONTRACT] [varchar](255) NULL,
	[DATAMOVIN] [varchar](255) NULL,
	[CATEGORIETARIF] [varchar](255) NULL,
	[DENUMIREDISTRIBUITOR_CHEIE] [varchar](255) NULL,
	[INSTALATIE] [varchar](255) NULL,
	[FURNIZOR_NOU_ANTERIOR_CHEIE] [varchar](255) NULL,
	[INVOICING_PARTY] [varchar](255) NULL,
	[CUI_CNP] [varchar](255) NULL,
	[MOTIVMOV_IN_COD] [varchar](255) NULL,
	[MOTIVMOV_IN_DESC] [varchar](255) NULL,
	[CONTCONTRACT] [varchar](255) NULL,
	[DATACREARE] [varchar](255) NULL,
	[URBANRURAL] [varchar](255) NULL,
	[JUDET] [varchar](255) NULL,
	[MOD_TIMP] [datetime] NULL,
	[MOD_DE] [varchar](255) NOT NULL
) ON [PRIMARY]
GO
/

CREATE TABLE [dbo].[GL_INT_NUMAR_LOST](
	[DENUMIRE_SURSA] [varchar](255) NULL,
	[DIVIZIE] [varchar](255) NULL,
	[DENUMIREPARTENER] [varchar](255) NULL,
	[PARTENERDEAFACERI] [varchar](255) NULL,
	[SEGMENTCLIENT] [varchar](255) NULL,
	[SUBSEGMENT] [varchar](255) NULL,
	[IDMANAGER] [varchar](255) NULL,
	[DENUMIREMANAGER] [varchar](255) NULL,
	[PUNCTDECONSUM] [varchar](255) NULL,
	[NUMARCONTRACT] [varchar](255) NULL,
	[DATAMOVIN] [varchar](255) NULL,
	[DATAMOVOUT] [varchar](255) NULL,
	[CATEGORIETARIF] [varchar](255) NULL,
	[DENUMIREDISTRIBUITOR_CHEIE] [varchar](255) NULL,
	[INSTALATIE] [varchar](255) NULL,
	[FURNIZOR_NOU_ANTERIOR_CHEIE] [varchar](255) NULL,
	[INVOICING_PARTY] [varchar](255) NULL,
	[CODCOMPANIE] [varchar](255) NULL,
	[CUI_CNP] [varchar](255) NULL,
	[MOTIVMOV_IN_COD] [varchar](255) NULL,
	[MOTIVMOV_IN_DESC] [varchar](255) NULL,
	[CONTCONTRACT] [varchar](255) NULL,
	[DATACREARE] [varchar](255) NULL,
	[URBANRURAL] [varchar](255) NULL,
	[JUDET] [varchar](255) NULL,
	[MOD_TIMP] [datetime] NULL,
	[MOD_DE] [varchar](255) NOT NULL
) ON [PRIMARY]
GO
/

CREATE TABLE [dbo].[GL_INT_SEGMENT](
	[COD] [varchar](3) NOT NULL,
	[NUME] [nvarchar](100) NOT NULL,
	[MOD_DE] [varchar](20) NOT NULL,
	[MOD_TIMP] [datetime] NULL,
	[SEGMENTGROUP] [varchar](10) NULL,
 CONSTRAINT [PK_GL_INT_SEGMENT] PRIMARY KEY CLUSTERED 
(
	[COD] ASC
)WITH (STATISTICS_NORECOMPUTE = ON, IGNORE_DUP_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[GL_INT_CANT_B2B] ADD  CONSTRAINT [DF_GL_INT_CANT_B2B_MOD_TIMP]  DEFAULT (switchoffset(getdate(),'+03:00')) FOR [MOD_TIMP]
GO
ALTER TABLE [dbo].[GL_INT_CANT_B2C_LOST] ADD  CONSTRAINT [DF_GL_INT_CANT_B2C_LOST]  DEFAULT (switchoffset(getdate(),'+03:00')) FOR [MOD_TIMP]
GO
ALTER TABLE [dbo].[GL_INT_CANT_MED_JUDET] ADD  CONSTRAINT [DF__GL_INT_CANT_MED_JUDET]  DEFAULT (switchoffset(getdate(),'+03:00')) FOR [MOD_TIMP]
GO
ALTER TABLE [dbo].[GL_INT_CANT_MED_TOTAL] ADD  CONSTRAINT [DF__GL_INT_CANT_MED_TOTAL]  DEFAULT (switchoffset(getdate(),'+03:00')) FOR [MOD_TIMP]
GO
ALTER TABLE [dbo].[GL_INT_FURN] ADD  CONSTRAINT [DF_GL_INT_FURN_MOD_TIMP]  DEFAULT (switchoffset(getdate(),'+02:00')) FOR [MOD_TIMP]
GO
ALTER TABLE [dbo].[GL_INT_MOTIVMOV_IN] ADD  CONSTRAINT [DF__GL_INT_MOTIVMOV_IN]  DEFAULT (switchoffset(getdate(),'+02:00')) FOR [MOD_TIMP]
GO
ALTER TABLE [dbo].[GL_INT_MOTIVMOV_OUT] ADD  CONSTRAINT [DF_GL_INT_MOTIVMOV_OUT]  DEFAULT (switchoffset(getdate(),'+02:00')) FOR [MOD_TIMP]
GO
ALTER TABLE [dbo].[GL_INT_NUMAR_GAINED] ADD  CONSTRAINT [DF_GL_INT_NUMAR_GAINED_MOD_TIMP]  DEFAULT (switchoffset(getdate(),'+02:00')) FOR [MOD_TIMP]
GO
ALTER TABLE [dbo].[GL_INT_NUMAR_LOST] ADD  CONSTRAINT [DF_GL_INT_NUMAR_LOST_MOD_TIMP]  DEFAULT (switchoffset(getdate(),'+02:00')) FOR [MOD_TIMP]
GO
ALTER TABLE [dbo].[GL_INT_SEGMENT] ADD  CONSTRAINT [DF__GL_INT_S__MOD_T__15E52B55]  DEFAULT (switchoffset(getdate(),'+02:00')) FOR [MOD_TIMP]
GO
/

CREATE TRIGGER  [dbo].[GL_INT_CANT_B2B_T1]  ON [dbo].[GL_INT_CANT_B2B]
INSTEAD OF INSERT 
AS

BEGIN ;

-- for insert 

		     INSERT INTO dbo.GL_INT_CANT_B2B ( TIP_SURSA, INSTALATIE, 
													FURNIZOR_NOU_ANTERIOR_TEXT, 
													MOTIVMOV_OUT_IN_TEXT,
													REGIUNE_PORTOFOLIU,
													TIP_PA,
													CONSUM_AN, 
													MOD_DE, MOD_TIMP  )
		     SELECT          TRIM(UPPER(TIP_SURSA)) TIP_SURSA, TRIM(UPPER(INSTALATIE)) INSTALATIE,
							 TRIM(UPPER(FURNIZOR_NOU_ANTERIOR_TEXT)) FURNIZOR_NOU_ANTERIOR_TEXT, 
							 TRIM(UPPER(MOTIVMOV_OUT_IN_TEXT)) MOTIVMOV_OUT_IN_TEXT ,
							 TRIM(UPPER(REGIUNE_PORTOFOLIU)) REGIUNE_PORTOFOLIU ,
							 TRIM(UPPER(TIP_PA)) TIP_PA ,
							 CONSUM_AN, 
							 MOD_DE, SWITCHOFFSET(getdate(), '+03:00')  AS MOD_TIMP
		    FROM             INSERTED 
		    WHERE            NOT EXISTS ( SELECT * FROM DELETED  WHERE DELETED.INSTALATIE=INSERTED.INSTALATIE AND DELETED.TIP_SURSA=INSERTED.TIP_SURSA )

END;
GO
ALTER TABLE [dbo].[GL_INT_CANT_B2B] DISABLE TRIGGER [GL_INT_CANT_B2B_T1]
GO
/

CREATE TRIGGER  [dbo].[GL_INT_CANT_B2C_LOST_T1]  ON [dbo].[GL_INT_CANT_B2C_LOST]
INSTEAD OF INSERT 
AS

BEGIN ;

-- for insert 

		     INSERT INTO dbo.GL_INT_CANT_B2C_LOST ( INSTALATIE, 
													 CONSUM_AN, 
													 MOD_DE, MOD_TIMP  )
		     SELECT          INSTALATIE,
							 CONSUM_AN, 
							 MOD_DE, SWITCHOFFSET(getdate(), '+03:00')  AS MOD_TIMP
		    FROM             INSERTED 
		    WHERE            NOT EXISTS ( SELECT * FROM DELETED  WHERE DELETED.INSTALATIE=INSERTED.INSTALATIE )

END;
GO
ALTER TABLE [dbo].[GL_INT_CANT_B2C_LOST] ENABLE TRIGGER [GL_INT_CANT_B2C_LOST_T1]
GO
/

CREATE TRIGGER  [dbo].[GL_INT_CANT_MED_JUDET_T1]  ON [dbo].[GL_INT_CANT_MED_JUDET]
INSTEAD OF INSERT 
AS

BEGIN ;

-- for insert 

		     INSERT INTO dbo.GL_INT_CANT_MED_JUDET ( DIVIZIE, 
													 SEGMENTCLIENT, 
													 JUDET ,CANT_MED_AN, 
													 MOD_DE, MOD_TIMP  )
		     SELECT          CASE WHEN DIVIZIE='1' THEN '01'
								  WHEN DIVIZIE='2' THEN '02'
								  ELSE UPPER(TRIM(DIVIZIE)) 
							 END 
							 "DIVIZIE",
							 UPPER(TRIM(SEGMENTCLIENT)) AS SEGMENTCLIENT , 
							 UPPER(TRIM(JUDET)) AS JUDET, CANT_MED_AN, 
							 MOD_DE, SWITCHOFFSET(getdate(), '+03:00')  AS MOD_TIMP
		    FROM             INSERTED 
		    WHERE            NOT EXISTS ( SELECT * FROM DELETED  WHERE DELETED.DIVIZIE=INSERTED.DIVIZIE				 AND 
																	   DELETED.SEGMENTCLIENT=INSERTED.SEGMENTCLIENT  AND
																	   DELETED.JUDET=INSERTED.JUDET     )

END;
GO
ALTER TABLE [dbo].[GL_INT_CANT_MED_JUDET] ENABLE TRIGGER [GL_INT_CANT_MED_JUDET_T1]
GO
/

CREATE TRIGGER  [dbo].[GL_INT_CANT_MED_TOTAL_T1]  ON [dbo].[GL_INT_CANT_MED_TOTAL]
INSTEAD OF INSERT 
AS

BEGIN ;

-- for insert 

		     INSERT INTO dbo.GL_INT_CANT_MED_TOTAL ( DIVIZIE, 
													 SEGMENTCLIENT, CANT_MED_AN, 
													 MOD_DE, MOD_TIMP  )
		     SELECT          CASE WHEN DIVIZIE='1' THEN '01'
								  WHEN DIVIZIE='2' THEN '02'
								  ELSE UPPER(TRIM(DIVIZIE)) 
							 END 
							 "DIVIZIE",
							 UPPER(TRIM(SEGMENTCLIENT)) AS SEGMENTCLIENT , CANT_MED_AN, 
							 MOD_DE, SWITCHOFFSET(getdate(), '+03:00')  AS MOD_TIMP
		    FROM             INSERTED 
		    WHERE            NOT EXISTS ( SELECT * FROM DELETED  WHERE DELETED.DIVIZIE=INSERTED.DIVIZIE AND DELETED.SEGMENTCLIENT=INSERTED.SEGMENTCLIENT )

END;

GO
ALTER TABLE [dbo].[GL_INT_CANT_MED_TOTAL] ENABLE TRIGGER [GL_INT_CANT_MED_TOTAL_T1]
GO
/

CREATE TRIGGER  [dbo].[GL_INT_FURN_T1]  ON [dbo].[GL_INT_FURN]
INSTEAD OF INSERT, UPDATE 
AS

BEGIN ;

-- for insert 

		     INSERT INTO dbo.GL_INT_FURN (  COD,
											NUME,
											MOD_DE,
											MOD_TIMP )
		     SELECT          UPPER(TRIM(COD)) COD, 
							 UPPER(TRIM(NUME)) NUME ,
							 MOD_DE, 
							 SWITCHOFFSET(getdate(), '+03:00')  MOD_TIMP
		    FROM             INSERTED 
		    WHERE           NOT EXISTS ( SELECT * FROM DELETED  WHERE DELETED.COD=INSERTED.COD )

-- for update

		     UPDATE  a
		     SET    
					 NUME=I.NUME,
				     MOD_DE=I.MOD_DE,
				     MOD_TIMP= SWITCHOFFSET(getdate(), '+03:00')
		FROM dbo.GL_INT_FURN a INNER JOIN INSERTED I ON a.COD=I.COD
		WHERE EXISTS ( SELECT * FROM DELETED WHERE DELETED.COD=I.COD )

END;
GO
ALTER TABLE [dbo].[GL_INT_FURN] ENABLE TRIGGER [GL_INT_FURN_T1]
GO
/

CREATE  TRIGGER  [dbo].[GL_INT_FURN_T2] ON [dbo].[GL_INT_FURN]
INSTEAD OF DELETE
AS
BEGIN;
SET NOCOUNT ON;

	THROW 50001,'INREGISTRARILE NU POT FII STERSE DIN TABEL!', 1; 

END
GO
ALTER TABLE [dbo].[GL_INT_FURN] ENABLE TRIGGER [GL_INT_FURN_T2]
GO
/

CREATE TRIGGER  [dbo].[GL_INT_MOTIVMOV_IN_T1]  ON [dbo].[GL_INT_MOTIVMOV_IN]
INSTEAD OF INSERT, UPDATE 
AS

BEGIN ;

-- for insert 

		     INSERT INTO dbo.GL_INT_MOTIVMOV_IN ( COD,
												  NUME,
												  MOTIV_GROUP,
												  MOD_DE,
												  MOD_TIMP )
		     SELECT          UPPER(TRIM(COD)) COD, 
							 UPPER(TRIM(NUME)) NUME ,
							 UPPER(TRIM(MOTIV_GROUP)) MOTIV_GROUP, 
							 MOD_DE, 
							 SWITCHOFFSET(getdate(), '+03:00')  MOD_TIMP
		    FROM             INSERTED 
		    WHERE           NOT EXISTS ( SELECT * FROM DELETED  WHERE DELETED.COD=INSERTED.COD )

-- for update

		     UPDATE  a
		     SET    
					 NUME=I.NUME,
					 MOTIV_GROUP=I.MOTIV_GROUP,
				     MOD_DE=I.MOD_DE,
				     MOD_TIMP= SWITCHOFFSET(getdate(), '+03:00')
		FROM dbo.GL_INT_MOTIVMOV_IN a INNER JOIN INSERTED I ON a.COD=I.COD
		WHERE EXISTS ( SELECT * FROM DELETED WHERE DELETED.COD=I.COD )

END;
GO
ALTER TABLE [dbo].[GL_INT_MOTIVMOV_IN] ENABLE TRIGGER [GL_INT_MOTIVMOV_IN_T1]
GO
/

CREATE  TRIGGER  [dbo].[GL_INT_MOTIVMOV_IN_T2] ON [dbo].[GL_INT_MOTIVMOV_IN]
INSTEAD OF DELETE
AS
BEGIN;
SET NOCOUNT ON;

	THROW 50001,'INREGISTRARILE NU POT FII STERSE DIN TABEL!', 1; 

END
GO
ALTER TABLE [dbo].[GL_INT_MOTIVMOV_IN] ENABLE TRIGGER [GL_INT_MOTIVMOV_IN_T2]
GO
/

CREATE TRIGGER  [dbo].[GL_INT_MOTIVMOV_OUT_T1]  ON [dbo].[GL_INT_MOTIVMOV_OUT]
INSTEAD OF INSERT, UPDATE 
AS

BEGIN ;

-- for insert 

		     INSERT INTO dbo.GL_INT_MOTIVMOV_OUT ( COD,
												  NUME,
												  MOTIV_GROUP,
												  MOD_DE,
												  MOD_TIMP )
		     SELECT          UPPER(TRIM(COD)) COD, 
							 UPPER(TRIM(NUME)) NUME ,
							 UPPER(TRIM(MOTIV_GROUP)) MOTIV_GROUP, 
							 MOD_DE, 
							 SWITCHOFFSET(getdate(), '+03:00')  MOD_TIMP
		    FROM             INSERTED 
		    WHERE           NOT EXISTS ( SELECT * FROM DELETED  WHERE DELETED.COD=INSERTED.COD )

-- for update

		     UPDATE  a
		     SET    
					 NUME=I.NUME,
					 MOTIV_GROUP=I.MOTIV_GROUP,
				     MOD_DE=I.MOD_DE,
				     MOD_TIMP= SWITCHOFFSET(getdate(), '+03:00')
		FROM dbo.GL_INT_MOTIVMOV_OUT a INNER JOIN INSERTED I ON a.COD=I.COD
		WHERE EXISTS ( SELECT * FROM DELETED WHERE DELETED.COD=I.COD )

END;
GO
ALTER TABLE [dbo].[GL_INT_MOTIVMOV_OUT] ENABLE TRIGGER [GL_INT_MOTIVMOV_OUT_T1]
GO
/

CREATE  TRIGGER  [dbo].[GL_INT_MOTIVMOV_OUT_T2] ON [dbo].[GL_INT_MOTIVMOV_OUT]
INSTEAD OF DELETE
AS
BEGIN;
SET NOCOUNT ON;

	THROW 50001,'INREGISTRARILE NU POT FII STERSE DIN TABEL!', 1; 

END
GO
ALTER TABLE [dbo].[GL_INT_MOTIVMOV_OUT] ENABLE TRIGGER [GL_INT_MOTIVMOV_OUT_T2]
GO
/

CREATE TRIGGER  [dbo].[GL_INT_SEGMENT_T1]  ON [dbo].[GL_INT_SEGMENT]
INSTEAD OF INSERT, UPDATE 
AS

BEGIN ;

-- for insert 

		     INSERT INTO dbo.GL_INT_SEGMENT ( COD,NUME,MOD_DE,MOD_TIMP, SEGMENTGROUP )
		     SELECT          UPPER(TRIM(COD)) AS COD, UPPER(TRIM(NUME)) AS NUME , MOD_DE, SWITCHOFFSET(getdate(), '+03:00')  AS MOD_TIMP, SEGMENTGROUP
		    FROM             INSERTED 
		    WHERE           NOT EXISTS ( SELECT * FROM DELETED  WHERE DELETED.COD=INSERTED.COD )

-- for update

		     UPDATE  a
		     SET          NUME=UPPER(TRIM(I.NUME)),
				     MOD_DE=I.MOD_DE,
				     MOD_TIMP= SWITCHOFFSET(getdate(), '+03:00'),
				     SEGMENTGROUP=I.SEGMENTGROUP
		FROM dbo.GL_INT_SEGMENT a INNER JOIN INSERTED I ON a.COD=I.COD
		WHERE EXISTS ( SELECT * FROM DELETED WHERE DELETED.COD=I.COD )

END;
GO
ALTER TABLE [dbo].[GL_INT_SEGMENT] ENABLE TRIGGER [GL_INT_SEGMENT_T1]
GO
/

CREATE  TRIGGER  [dbo].[GL_INT_SEGMENT_T2] ON [dbo].[GL_INT_SEGMENT]
INSTEAD OF DELETE
AS
BEGIN;
SET NOCOUNT ON;

	THROW 50001,'INREGISTRARILE NU POT FII STERSE DIN TABEL!', 1; 

END
GO
ALTER TABLE [dbo].[GL_INT_SEGMENT] ENABLE TRIGGER [GL_INT_SEGMENT_T2]
GO
