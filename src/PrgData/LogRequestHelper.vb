Imports log4net
Imports System.IO
Imports System.Net.Mail
Imports System.Configuration
Imports PrgData.Common


Public Class LogRequestHelper

	Private Shared _Logger As ILog = LogManager.GetLogger(GetType(LogRequestHelper))

	Public Shared Function NeedLogged() As Boolean
		Return Convert.ToBoolean(ConfigurationManager.AppSettings("MustLogHttpcontext"))
	End Function

	Public Shared Sub ForceMailWithRequest(ByVal MessageText As String, ByVal exception As Exception)
		InternalMailWithRequest(MessageText, exception)
	End Sub

	Public Shared Sub MailWithRequest(ByVal Logger As ILog, ByVal MessageText As String, ByVal exception As Exception)
		'Если логгер не установлен, то будем использовать логгер LogRequestHelper
		Dim l = Logger
		If l Is Nothing Then l = _Logger
		'Если exception не установлен, то будем брать исключение от сервера
		Dim ex = exception
		If ex Is Nothing Then ex = HttpContext.Current.Server.GetLastError()
		If NeedLogged() Then
			InternalMailWithRequest(MessageText, exception)
		Else
			l.Error(MessageText, exception)
		End If
	End Sub

	Private Shared Sub InternalMailWithRequest(ByVal MessageText As String, ByVal exception As Exception)
		Try
			Dim tmpRequestFileName As String = Path.GetTempFileName()
			HttpContext.Current.Request.SaveAs(tmpRequestFileName, True)
			Try
				_Logger.Error(MessageText, exception)

				If exception IsNot Nothing Then
					MessageText = String.Format("{0} " & vbCrLf & "{1}", MessageText, exception)
				End If
				MessageText = String.Format( _
					"Date: {0}" & vbCrLf & _
					"User: {1}" & vbCrLf & _
					"{2}", _
					DateTime.Now, _
					ServiceContext.GetUserName(), _
					MessageText)

				Dim httpRequestContent = File.ReadAllText(tmpRequestFileName)

				MailHelper.Mail(MessageText, Nothing, httpRequestContent, "HTTPReguest.txt")

			Finally
				Try
					File.Delete(tmpRequestFileName)
				Catch ex As Exception
					_Logger.Error("Ошибка при удалении временного файла для хранения HTTP-запроса", ex)
				End Try
			End Try

		Catch err As Exception
			_Logger.Error("Ошибка в MailWithRequest", err)
		End Try
	End Sub

End Class
