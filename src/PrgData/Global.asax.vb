Imports log4net.Config
Imports log4net
Imports PrgData.Common
Imports PrgData.Common.Counters
Imports System.IO




Public Class Global_asax
    Inherits System.Web.HttpApplication

    Shared Logger As ILog = LogManager.GetLogger(GetType(Global_asax))

    Sub Application_Start(ByVal sender As Object, ByVal e As EventArgs)
        XmlConfigurator.Configure()
        SmartOrderHelper.InitializeIoC()
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
        ConfigurationManager.AppSettings("WaybillPath") = Path.Combine(parentDir, "FtpRoot")
        For Each dir As String In dirs
            dir = Path.Combine(parentDir, dir)
            If Not Directory.Exists(dir) Then Directory.CreateDirectory(dir)
        Next

        ServiceContext.SetupDebugContext()
#End If
        'Logger.Debug("Приложение запущено")
    End Sub

    Sub Session_Start(ByVal sender As Object, ByVal e As EventArgs)
    End Sub

    Sub Application_BeginRequest(ByVal sender As Object, ByVal e As EventArgs)
        ' Fires at the beginning of each request
    End Sub

    Sub Application_EndRequest(ByVal sender As Object, ByVal e As EventArgs)
        'Логируем все запросы при статусе 500
        If Context.Response.StatusCode = 500 AndAlso LogRequestHelper.NeedLogged() Then
            LogRequestHelper.MailWithRequest(Nothing, "Данный запрос сгенерировал ошибку 500", Nothing)
        End If
    End Sub

    Sub Application_AuthorizeRequest(ByVal sender As Object, ByVal e As EventArgs)
        Try
            If String.IsNullOrEmpty(ServiceContext.GetUserName()) Then
                Using connection = New Global.Common.MySql.SimpleConnectionManager().GetConnection()
                    connection.Open()

                    Dim updateData = UpdateHelper.GetUpdateData(connection, ServiceContext.GetShortUserName())

                    If updateData Is Nothing Then
                        Logger.DebugFormat("Для логина {0} услуга не предоставляется, поэтому удалить блокировки нельзя", ServiceContext.GetUserName())
                    Else
                        Dim deletedLocks = Counter.ClearByUserId(updateData.UserId)
                        If deletedLocks > 0 Then
                            Logger.DebugFormat("Удалили устаревшие блокировки для пользователя {0}: {1}", ServiceContext.GetUserName(), deletedLocks)
                        End If
                    End If
                End Using
            End If
        Catch ex As Exception
            Logger.Error("Ошибка при очистке таблицы блокировок для пользователя", ex)
        End Try
    End Sub

    Sub Application_AuthenticateRequest(ByVal sender As Object, ByVal e As EventArgs)
        ' Fires upon attempting to authenticate the use
    End Sub

    Sub Application_Error(ByVal sender As Object, ByVal e As EventArgs)
        ' Fires when an error occurs
    End Sub

    Sub Session_End(ByVal sender As Object, ByVal e As EventArgs)
        ' Fires when the session ends
    End Sub

    Sub Application_End(ByVal sender As Object, ByVal e As EventArgs)
        'Logger.Debug("Приложение остановлено")
    End Sub

End Class