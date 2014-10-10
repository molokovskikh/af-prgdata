Imports PrgData.FileHandlers
Imports log4net.Config
Imports log4net
Imports PrgData.Common
Imports PrgData.Common.Counters
Imports System.IO




Public Class Global_asax
	Inherits HttpApplication

	Private log As ILog = LogManager.GetLogger(GetType(HttpApplication))

	Sub Application_Start(ByVal sender As Object, ByVal e As EventArgs)
		try
			XmlConfigurator.Configure()
			SmartOrderHelper.InitializeIoC()
			GetFileHandler.ReadConfig()


#If DEBUG Then
			Dim dirs = New String() { _
				"FtpRoot", _
				"FtpRoot\1349", "FtpRoot\1349\Docs", "FtpRoot\1349\Orders", "FtpRoot\1349\Rejects", "FtpRoot\1349\Waybills", _
				"FtpRoot\10068", "FtpRoot\10068\Docs", "FtpRoot\10068\Orders", "FtpRoot\10068\Rejects", "FtpRoot\10068\Waybills", _
				"FtpRoot\10069", "FtpRoot\10069\Docs", "FtpRoot\10069\Orders", "FtpRoot\10069\Rejects", "FtpRoot\10069\Waybills"}


			Dim parentDir = AppDomain.CurrentDomain.BaseDirectory
			If Directory.Exists(Path.Combine(parentDir, "bin")) Then
				parentDir = Path.Combine(parentDir, "bin")
			End If
			ConfigurationManager.AppSettings("DocumentsPath") = Path.Combine(parentDir, "FtpRoot") & "\"
			For Each dir As String In dirs
				dir = Path.Combine(parentDir, dir)
				If Not Directory.Exists(dir) Then Directory.CreateDirectory(dir)
			Next

			ServiceContext.SetupDebugContext()
#End If
		Catch ex As Exception
			log.Error("Ошибка при запуске приложения", ex)
			Throw
		End Try
	End Sub

	Sub Application_EndRequest(ByVal sender As Object, ByVal e As EventArgs)
		'Логируем все запросы при статусе 500
		If Context.Response.StatusCode = 500 Then
			Try
				ThreadContext.Properties("user") = ServiceContext.GetUserName()
				LogRequestHelper.MailWithRequest(Nothing, "Данный запрос сгенерировал ошибку 500", Nothing)
			Finally
				ThreadContext.Properties.Clear()
			End Try
		End If
	End Sub

End Class