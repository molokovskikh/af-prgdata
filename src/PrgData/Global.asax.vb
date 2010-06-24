Imports log4net.Config
Imports log4net
Imports PrgData.Common




Public Class Global_asax
    Inherits System.Web.HttpApplication

    Shared Logger As ILog = LogManager.GetLogger(GetType(Global_asax))

    Sub Application_Start(ByVal sender As Object, ByVal e As EventArgs)
        XmlConfigurator.Configure()
		SmartOrderHelper.InitializeIoC()
#If DEBUG Then
		ServiceContext.SetupDebugContext()
#End If
		'Logger.Debug("Приложение запущено")
    End Sub

	Sub Session_Start(ByVal sender As Object, ByVal e As EventArgs)
		Try
			Counter.Counter.Clear()
		Catch ex As Exception
			Logger.Error("Ошибка при очистке таблицы блокировок", ex)
		End Try
	End Sub

    Sub Application_BeginRequest(ByVal sender As Object, ByVal e As EventArgs)
        ' Fires at the beginning of each request
    End Sub

    Sub Application_EndRequest(ByVal sender As Object, ByVal e As EventArgs)
        'Логируем все запросы при статусе 500
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