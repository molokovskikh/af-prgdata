Imports log4net
Imports System.IO
Imports System.Net.Mail
Imports System.Configuration


Public Class LogRequestHelper

    Shared Logger As ILog = LogManager.GetLogger(GetType(LogRequestHelper))

    Public Shared Sub MailWithRequest(ByVal MessageText As String)
        Try
            Dim tmpRequestFileName As String = Path.GetTempFileName()
            HttpContext.Current.Request.SaveAs(tmpRequestFileName, True)
            Try
                MessageText = String.Format( _
                    "Date: {0}" & vbCrLf & _
                    "User: {1}" & vbCrLf & _
                    "{2}", _
                    DateTime.Now, _
                    HttpContext.Current.User.Identity.Name, _
                    MessageText)

                Dim MailAddress As New MailAddress("service@analit.net", "Сервис AF", Encoding.UTF8)
                Dim message As New MailMessage("service@analit.net", ConfigurationManager.AppSettings("ErrorMail"))
                message.From = MailAddress
                message.Subject = "Ошибка в сервисе подготовки данных"
                message.SubjectEncoding = Encoding.UTF8
                message.BodyEncoding = Encoding.UTF8
                message.Body = MessageText


                Using stream = File.Open(tmpRequestFileName, FileMode.Open)

                    message.Attachments.Add(New Attachment(stream, "HTTPReguest.txt"))

                    Dim SC As New SmtpClient("box.analit.net")
                    SC.Send(message)

                End Using

            Finally
                Try
                    File.Delete(tmpRequestFileName)
                Catch ex As Exception
                    Logger.Error("Ошибка при удалении временного файла для хранения HTTP-запроса", ex)
                End Try
            End Try

        Catch err As Exception
            Logger.Error("Ошибка в MailWithRequest", err)
        End Try
    End Sub

End Class
