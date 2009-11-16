Imports MySql.Data.MySqlClient

Public Class Helper

	Public Shared Sub MaintainReplicationInfo(ByVal ClientId As UInteger, ByVal connection As MySqlConnection)

		Dim command As MySqlCommand = New MySqlCommand()

		command.Connection = connection
		command.CommandText = "" & _
	   "INSERT " & _
	   "INTO   Usersettings.AnalitFReplicationInfo " & _
	   "       ( " & _
	   "              UserId, " & _
	   "              FirmCode " & _
	   "       ) " & _
	   "SELECT   ouar.RowId, " & _
	   "         supplier.FirmCode " & _
	   "FROM     usersettings.clientsdata AS drugstore " & _
	   "         JOIN usersettings.OsUserAccessRight ouar " & _
	   "         ON       ouar.ClientCode = drugstore.FirmCode " & _
	   "         JOIN clientsdata supplier " & _
	   "         ON       supplier.firmsegment = drugstore.firmsegment " & _
	   "         LEFT JOIN Usersettings.AnalitFReplicationInfo ari " & _
	   "         ON       ari.UserId   = ouar.RowId " & _
	   "              AND ari.FirmCode = supplier.FirmCode " & _
	   "WHERE    ari.UserId IS NULL " & _
	   "     AND supplier.firmtype                          = 0 " & _
	   "     AND drugstore.FirmCode                         = " & ClientId & _
	   "     AND drugstore.firmtype                         = 1 " & _
	   "     AND supplier.maskregion & drugstore.maskregion > 0 " & _
	   "GROUP BY ouar.RowId, " & _
	   "         supplier.FirmCode; "

		command.ExecuteNonQuery()
	End Sub


End Class
