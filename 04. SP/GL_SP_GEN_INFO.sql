/*

    This procedure highlights the steps to be followed for gained & lost calculation on customer level.

*/
CREATE PROCEDURE [dbo].[gen_info_GL]
( 
@AN SMALLINT,
@LUNA SMALLINT
 )
AS
BEGIN

		SET XACT_ABORT, NOCOUNT ON;

EXEC dbo.master_GL_INREG_NR_GAINED @AN, @LUNA ; 
EXEC dbo.master_GL_INREG_NR_LOST @AN, @LUNA ;  
EXEC dbo.master_GL_INREG_CANT_B2C @AN, @LUNA ; 
EXEC dbo.master_GL_INREG_CANT_B2B @AN, @LUNA ; 
END


GO
