Imports System.Net.Mail
Imports PrgData.Common
Imports MySql.Data.MySqlClient
Imports System.Reflection
Imports System.Text

Public Class Utils

	Public Shared Sub Execute(ByVal CommandText As String)
		Execute(Nothing)
	End Sub

	Public Shared Sub Execute(ByVal CommandText As String, ByRef ParametersAsAnonymousObject As Object)
        Using connection = New MySqlConnection(Settings.ConnectionString)
            connection.Open()
            Dim command = New MySqlCommand(CommandText, connection)
            BindParameters(command, ParametersAsAnonymousObject)
            command.ExecuteNonQuery()
        End Using
	End Sub

	Public Shared Function Request(ByVal CommandText As String, ByRef ParametersAsAnonymousObject As Object) As IList(Of ClientStatus)
        Using connection = New Global.Common.MySql.ConnectionManager().GetConnection()
            connection.Open()
            Dim command = New MySqlCommand(CommandText, connection)
            BindParameters(command, ParametersAsAnonymousObject)
            Dim statuses = New List(Of ClientStatus)
            Using Reader = command.ExecuteReader
                While Reader.Read
                    statuses.Add(New ClientStatus(Reader.GetInt32("Id"), Reader.GetUInt32("ClientCode"), Reader.GetString("MethodName"), Reader.GetDateTime("StartTime")))
                End While
            End Using
            Return statuses
        End Using
	End Function

	Public Shared Function RequestScalar(Of T)(ByVal CommandText As String) As T
        Using connection = New Global.Common.MySql.ConnectionManager().GetConnection()
            connection.Open()
            Dim command = New MySqlCommand(CommandText, connection)
            Return CType(command.ExecuteScalar(), T)
        End Using
	End Function

	Public Shared Function Request(ByVal CommandText As String) As IList(Of ClientStatus)
		Return Request(CommandText, Nothing)
	End Function

	Public Shared Sub BindParameters(ByRef Command As MySqlCommand, ByRef ParametersAsAnonymousObject As Object)
		If Not ParametersAsAnonymousObject Is Nothing Then
			For Each PropertyInfo In ParametersAsAnonymousObject.GetType().GetProperties(BindingFlags.GetProperty Or BindingFlags.Public Or BindingFlags.Instance)
				Dim Value = PropertyInfo.GetValue(ParametersAsAnonymousObject, Nothing)
				Command.Parameters.AddWithValue("?" & PropertyInfo.Name, Value)
			Next
		End If
	End Sub

	Public Shared Sub Mail(ByVal MessageText As String, ByVal Subject As String)
		Try
			Dim MailAddress As New MailAddress("service@analit.net", "Сервис AF", Encoding.UTF8)
			Dim message As New MailMessage("service@analit.net", "service@analit.net")
            Dim SC As New SmtpClient("box.analit.net")
			message.From = MailAddress
			message.Subject = Subject
			message.SubjectEncoding = Encoding.UTF8
			message.BodyEncoding = Encoding.UTF8
			message.Body = MessageText
			SC.Send(message)
        Catch err As Exception
        End Try
	End Sub

End Class
