Imports System.Web.Services
Imports System.Threading
Imports System.IO
Imports System.Web
Imports System.Text
Imports Counter
Imports log4net
Imports MySql.Data.MySqlClient
Imports MySQLResultFile = System.IO.File
Imports Counter.Counter
Imports PrgData.Common
Imports System.Net.Mail
Imports log4net.Core
Imports PrgData.Common.Orders


<WebService(Namespace:="IOS.Service")> _
Public Class PrgDataEx
    Inherits System.Web.Services.WebService

    Public Sub New()
        MyBase.New()

        InitializeComponent()

        ResultFileName = Server.MapPath("/Results") & "\"
        ConnectionManager = New Global.Common.MySql.ConnectionManager()

    End Sub

    Private ConnectionManager As Global.Common.MySql.ConnectionManager
    Private WithEvents SelProc As MySql.Data.MySqlClient.MySqlCommand
    Private WithEvents ArchDA As MySql.Data.MySqlClient.MySqlDataAdapter
    Private WithEvents dataTable4 As System.Data.DataTable
    Private WithEvents ArchCmd As MySql.Data.MySqlClient.MySqlCommand
    Private WithEvents ArchCn As MySql.Data.MySqlClient.MySqlConnection
    Private WithEvents DA As MySql.Data.MySqlClient.MySqlDataAdapter
    Friend WithEvents DataTable3 As System.Data.DataTable
    Friend WithEvents DataTable5 As System.Data.DataTable
    Friend WithEvents DataTable6 As System.Data.DataTable

    Private components As System.ComponentModel.IContainer

    Protected Overloads Overrides Sub Dispose(ByVal disposing As Boolean)
        If disposing Then
            If Not (components Is Nothing) Then
                components.Dispose()
            End If
        End If
        MyBase.Dispose(disposing)
    End Sub

    ReadOnly ПутьКДокументам As String = System.Configuration.ConfigurationManager.AppSettings("DocumentsPath")
    ReadOnly MySqlFilePath As String = System.Configuration.ConfigurationManager.AppSettings("MySqlFilePath")

    ReadOnly ZipProcessorAffinityMask As Integer = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings("ZipProcessorAffinity"))

    Private Const IsoLevel As System.Data.IsolationLevel = IsolationLevel.ReadCommitted
    Dim zipfilecount As Int32
    Private FileInfo As System.IO.FileInfo
    Private FileCount As Integer = 16
    Private tspan As New TimeSpan()
    Private Запрос, UserName, MessageD, MailMessage As String
    'Строка с кодами прайс-листов, у которых отсутствуют синонимы на клиенте
    Private AbsentPriceCodes As String
    Private MessageH As String
    Private rowcount, i, NewMDBVer As Integer
    Private MinCount As UInt32
    Private ErrorFlag, Documents As Boolean
    Private Addition, ClientLog As String
    Private Reclame As Boolean
    Private ResultFileName As String
    Dim ArhiveStartTime As DateTime
    Dim ArhiveTS As TimeSpan

    'Потоки
    Private ThreadZipStream As New Thread(AddressOf ZipStream)
    Private BaseThread As Thread 'New Thread(AddressOf BaseProc)
    Private ProtocolUpdatesThread As New Thread(AddressOf ProtocolUpdates)
    Private SetResultCodes As New Thread(AddressOf SetCodesProc)

    Private CurUpdTime, OldUpTime As DateTime
    Private ResultRow As Data.DataRow
    Private BuildNo, AllowBuildNo, UpdateType, MDBVer As Integer
    Private ResultLenght, OrderId As UInt32
    Dim CCode, UserId As UInt32
    Private SpyHostsFile, SpyAccount As Boolean
    Dim UpdateData As UpdateData
    Private UserHost, UniqueCID, UID, Message, ReclamePath As String
    Private myTrans As MySqlTransaction
    Private UncDT As Date
    Private Active, EnableUpdate, CheckID, NotUpdActive, GED, PackFinished, CalculateLeader As Boolean
    Private NewZip As Boolean = True
    Dim GUpdateId As UInt32? = 0
    Private WithEvents DS As System.Data.DataSet
    Public WithEvents DataTable1 As System.Data.DataTable
    Public WithEvents DataColumn1 As System.Data.DataColumn
    Public WithEvents DataColumn2 As System.Data.DataColumn
    Public WithEvents DataColumn3 As System.Data.DataColumn
    Public WithEvents DataTable2 As System.Data.DataTable
    Public WithEvents DataColumn4 As System.Data.DataColumn
    Public WithEvents DataColumn5 As System.Data.DataColumn
    Private WithEvents Cm As MySql.Data.MySqlClient.MySqlCommand
    Private WithEvents LogCm As MySql.Data.MySqlClient.MySqlCommand

    Public WithEvents OrdersL As System.Data.DataTable
    Private WithEvents OrderInsertCm As MySql.Data.MySqlClient.MySqlCommand
    Private WithEvents OrderInsertDA As MySql.Data.MySqlClient.MySqlDataAdapter
    Private WithEvents ReadOnlyCn As MySql.Data.MySqlClient.MySqlConnection
    Private ReadWriteCn As MySql.Data.MySqlClient.MySqlConnection

    Private FilesForArchive As Queue(Of FileForArchive) = New Queue(Of FileForArchive)


    Private Log As ILog = LogManager.GetLogger(GetType(PrgDataEx))



    'UpdateType: 1 - обычное, 2 - накопительное, 3 - докачка, 4 - заказы, 5 - запрет, 6 - ошибка, 7 - Подтверждение получения, 8 - только документы


    'Получает письмо и отправляет его
    <WebMethod()> _
    Public Function SendLetter(ByVal subject As String, ByVal body As String, ByVal attachment() As Byte) As String
        Try
            Dim updateData As UpdateData
            Using connection As MySqlConnection = ConnectionManager.GetConnection()
                connection.Open()
                updateData = UpdateHelper.GetUpdateData(ConnectionManager.GetConnection(), HttpContext.Current.User.Identity.Name)
            End Using

            If updateData Is Nothing Then
                Throw New Exception("Клиент не найден")
            End If

            Dim mess As MailMessage = New MailMessage(New MailAddress("farm@analit.net", String.Format("{0} [{1}]", updateData.ShortName, updateData.ClientId)), New MailAddress("tech@analit.net"))
            mess.Body = body
            mess.IsBodyHtml = False
            mess.BodyEncoding = Encoding.UTF8
            mess.Subject = " UserId:" & updateData.UserId & ": " & subject
            If (Not IsNothing(attachment)) Then
                mess.Attachments.Add(New Attachment(New MemoryStream(attachment), "Attach.7z"))
            End If
            Dim sc As SmtpClient = New SmtpClient("box.analit.net")
            sc.Send(mess)

            Return "Res=OK"
        Catch ex As Exception
            Log.Error("Ошибка при отправке письма", ex)
            Return "Error=" & "Не удалось отправить письмо. Попробуйте позднее."
        End Try
    End Function

    <WebMethod()> Public Function GetInfo( _
       ByVal LibraryName() As String, _
       ByVal LibraryVersion() As String, _
       ByVal LibraryHash() As String) As String

        Dim LibraryNameWOPath As String


        'If DBConnect("GetInfo") Then
        '    GetClientCode()
        '    Cm.Transaction = Nothing
        '    Cm.CommandText = "SELECT libraryname, libraryhash, DeleteOnClient FROM usersettings.AnalitF_Library_Hashs ALH where exeversion=" & BuildNo
        '    DA.Fill(DS, "ALH")

        '    MailMessage = ""
        '    For i = 0 To LibraryName.Length - 1

        '        If LibraryName(i).IndexOf("\") > 0 Then
        '            LibraryNameWOPath = LibraryName(i).Substring(LibraryName(i).IndexOf("\") + 1)
        '        Else
        '            LibraryNameWOPath = LibraryName(i)
        '        End If

        '        If DS.Tables("ALH").Select("LibraryName='" & LibraryNameWOPath & "'").Length > 0 Then

        '            'If DS.Tables("ALH").Select("LibraryName='" & LibraryName(i) & "'")(0).Item("libraryhash") <> LibraryHash(i) Then
        '            '    MailMessage &= "Hash библиотеки не совпал: " & LibraryName(i) & ", у клиента: Hash: " & LibraryHash(i) & ", версия: " & LibraryVersion(i) & "; "
        '            'End If
        '        Else
        '            'MailMessage &= "Не описанная на сервере библиотека: " & LibraryName(i) & ", версия: " & LibraryVersion(i) & ", hash: " & LibraryHash(i) & "; "
        '        End If

        '    Next
        '    If MailMessage.Length > 0 Then
        '        'Addition &= MailMessage
        '        'MailErr("Ошибка проверки версий библиотек", MailMessage)
        '        MailMessage = ""
        '    End If
        '    DBDisconnect()
        'End If
        Return ""
    End Function

    <WebMethod()> Public Function GetUserData( _
    ByVal AccessTime As Date, _
    ByVal GetEtalonData As Boolean, _
    ByVal EXEVersion As String, _
    ByVal MDBVersion As Int16, _
    ByVal UniqueID As String, _
    ByVal WINVersion As String, _
    ByVal WINDesc As String, _
    ByVal WayBillsOnly As Boolean) As String

        Return GetUserDataEx( _
        AccessTime, _
        GetEtalonData, _
        EXEVersion, _
        MDBVersion, _
        UniqueID, _
        WINVersion, _
        WINDesc, _
        WayBillsOnly, _
        Nothing)

    End Function

    <WebMethod()> Public Function GetUserDataEx( _
    ByVal AccessTime As Date, _
    ByVal GetEtalonData As Boolean, _
    ByVal EXEVersion As String, _
    ByVal MDBVersion As Int16, _
    ByVal UniqueID As String, _
    ByVal WINVersion As String, _
    ByVal WINDesc As String, _
    ByVal WayBillsOnly As Boolean, _
    ByVal ClientHFile As String) As String
        Dim LockCount As Int32
        Dim ResStr As String = String.Empty
        Dim NeedFreeLock As Boolean = False
        Addition = " ОС: " & WINVersion & " " & WINDesc & "; "

        Try


            If DBConnect("GetUserData") Then

                'Начинаем обычное обновление
                UpdateType = 1

                'Нет критических ошибок
                ErrorFlag = False

                'Только накладные
                Documents = WayBillsOnly

                'Присваиваем версии приложения и базы
                MDBVer = MDBVersion
                GED = GetEtalonData
                For i = 2 To 4
                    If Left(Right(EXEVersion, i), 1) = "." Then Exit For
                Next
                BuildNo = CInt(Right(EXEVersion, i - 1))


                CCode = 0

                'Получаем код и параметры клиента клиента
                GetClientCode()

                If CCode < 1 Then
                    MessageH = "Доступ закрыт."
                    MessageD = "Пожалуйста, обратитесь в АК «Инфорум».[1]"
                    Addition &= "Для логина " & UserName & " услуга не предоставляется; "
                    UpdateType = 5
                    ErrorFlag = True
                    GoTo endproc
                End If


                If Not Counter.Counter.TryLock(UserId, "GetUserData") Then

                    MessageH = "Обновление данных в настоящее время невозможно."
                    MessageD = "Пожалуйста, повторите попытку через несколько минут.[6]"
                    Addition &= "Перегрузка; "
                    UpdateType = 6
                    ErrorFlag = True
                    GoTo endproc

                Else

                    NeedFreeLock = True

                End If

                Cm.Transaction = Nothing
                Cm.CommandText = "" & _
                "SELECT AFAppVersion " & _
                "FROM   UserUpdateInfo " & _
                "WHERE  UserId=" & UserId
                AllowBuildNo = CType(Cm.ExecuteScalar, Int16)

                If BuildNo < AllowBuildNo Then
                    MessageH = "Доступ закрыт."
                    MessageD = "Используемая версия программы не актуальна, необходимо обновление до версии №" & AllowBuildNo & ".[5]"
                    Addition &= "Попытка обновить устаревшую версию; "
                    UpdateType = 5
                    ErrorFlag = True
                    GoTo endproc
                End If


                If CheckID Then
                    UID = UniqueID
                    If Not FnCheckID() Then
                        MessageH = "Обновление программы на данном компьютере запрещено."
                        MessageD = "Пожалуйста, обратитесь в АК «Инфорум».[2]"
                        Addition &= "Несоответствие UIN; "
                        UpdateType = 5
                        ErrorFlag = True
                        GoTo endproc
                    End If
                End If


                'Если с момента последнего обновления менее установленного времени
                If Not Documents Then

                    If AllowBuildNo < BuildNo Then
                        Cm.Connection = ReadWriteCn
                        Cm.Transaction = myTrans
                        Cm.CommandText = "update usersettings.UserUpdateInfo set AFAppVersion=" & BuildNo & " where UserId=" & UserId
RestartInsertTrans:
                        Try

                            myTrans = ReadWriteCn.BeginTransaction(IsoLevel)
                            Cm.ExecuteNonQuery()
                            myTrans.Commit()

                        Catch MySQLErr As MySqlException
                            If Not (ReadOnlyCn.State = ConnectionState.Closed Or ReadOnlyCn.State = ConnectionState.Broken) Then myTrans.Rollback()
                            If MySQLErr.Number = 1213 Or MySQLErr.Number = 1205 _
                             Or MySQLErr.Number = 1422 Or MySQLErr.Number = 1062 Then
                                myTrans.Rollback()
                                System.Threading.Thread.Sleep(500)
                                GoTo RestartInsertTrans
                            End If
                            MailErr("Проверка версии EXE", MySQLErr.Source & ": " & MySQLErr.Message)
                        End Try


                    End If

                    'Если несовпадает время последнего обновления на клиете и сервере
                    If Not CheckUpdateTime(AccessTime.ToLocalTime, GED) Then
                        GED = True
                        Addition &= "Время обновления не совпало на клиенте и сервере, готовим КО; "
                    End If


                    'В зависимости от версии используем одну из процедур подготовки данных: для сервера Firebird и для сервера MySql
                    If BuildNo > 716 Then
                        'Если производим обновление 945 версии на новую с поддержкой МНН или версия уже с поддержкой МНН, то добавляем еще два файла: мнн и описания
                        If ((BuildNo = 945) And UpdateData.EnableUpdate) Or (BuildNo > 945) Then
                            FileCount = 18
                        Else
                            If (BuildNo >= 829) And (BuildNo <= 837) And UpdateData.EnableUpdate Then
                                FileCount = 18
                                Addition &= "Производится обновление программы с 800-х версий на MySql; "
                            Else
                                FileCount = 16
                            End If
                        End If
                        BaseThread = New Thread(AddressOf MySqlProc)
                    Else
                        Dim CheckEnableUpdate As Boolean = Convert.ToBoolean(MySqlHelper.ExecuteScalar(ReadOnlyCn, "select EnableUpdate from retclientsset where clientcode=" & CCode))
                        If ((BuildNo >= 705) And (BuildNo <= 716)) And CheckEnableUpdate Then
                            BaseThread = New Thread(AddressOf MySqlProc)
                            FileCount = 18
                            GED = True
                            Addition &= "Производится обновление программы с Firebird на MySql, готовим КО; "
                        Else
                            BaseThread = New Thread(AddressOf FirebirdProc)
                        End If
                    End If

                    'Готовим кумулятивное
                    If GED Then

                        UpdateType = 2
                        Cm.Connection = ReadWriteCn
                        Cm.CommandText = "update UserUpdateInfo set ReclameDate = NULL where UserId=" & UserId & "; "
                        myTrans = ReadWriteCn.BeginTransaction(IsoLevel)
                        Cm.Transaction = myTrans
                        Cm.ExecuteNonQuery()
                        myTrans.Commit()

                    End If

                End If

                If Documents Then

                    CurUpdTime = Now()

                    UpdateType = 8
                    Try
                        MySQLFileDelete(ResultFileName & UserId & ".zip")
                    Catch ex As Exception
                        Addition &= "Не удалось удалить предыдущие данные (получение только документов): " & ex.Message & "; "
                        UpdateType = 5
                        ErrorFlag = True
                        GoTo endproc
                    End Try

                Else

                    PackFinished = False

                    If CkeckZipTimeAndExist(GetEtalonData) Then


                        If Not File.GetAttributes(ResultFileName & UserId & ".zip") = FileAttributes.NotContentIndexed Then

                            UpdateType = 3
                            NewZip = False
                            PackFinished = True
                            GoTo endproc

                        End If

                    Else

                        Try

                            MySQLFileDelete(ResultFileName & UserId & ".zip")

                        Catch ex As Exception
                            Addition &= "Не удалось удалить предыдущие данные: " & ex.Message & "; "
                            UpdateType = 5
                            ErrorFlag = True
                            GoTo endproc
                        End Try


                        Try

                            With Cm

                                .Connection = ReadWriteCn
                                .CommandText = String.Empty

                                If UpdateType <> 3 Then

                                    .CommandText = "update UserUpdateInfo set UncommitedUpdateDate=now() where UserId=" & UserId & "; "

                                End If

                                .CommandText &= "select UncommitedUpdateDate from UserUpdateInfo where UserId=" & UserId

                                .Transaction = myTrans

                            End With

Restart:
                            myTrans = ReadWriteCn.BeginTransaction(IsoLevel)
                            Cm.Transaction = myTrans
                            Using SQLdr = Cm.ExecuteReader()
                                SQLdr.Read()
                                CurUpdTime = SQLdr.GetDateTime(0)
                            End Using
                            myTrans.Commit()

                        Catch MySQLErr As MySqlException
                            If Not (ReadWriteCn.State = ConnectionState.Closed Or ReadWriteCn.State = ConnectionState.Broken) Then myTrans.Rollback()
                            If MySQLErr.Number = 1213 Or MySQLErr.Number = 1205 Then
                                System.Threading.Thread.Sleep(500)
                                GoTo Restart
                            End If
                            MailErr("Присвоение неподтвержденного времени, клиент: " & CCode, MySQLErr.Message)
                            ErrorFlag = True
                        Catch ex As Exception
                            MailErr("Присвоение неподтвержденного времени, клиент: " & CCode, ex.Message)
                            ErrorFlag = True
                        End Try



                    End If


                End If





                If Documents Then

                    'Начинаем архивирование
                    ThreadZipStream.Start()

                Else

                    'Начинаем готовить данные
                    BaseThread.Start()
                    System.Threading.Thread.Sleep(500)

                End If

                LockCount = 0
endproc:

                If Not PackFinished And (((BaseThread IsNot Nothing) AndAlso BaseThread.IsAlive) Or ThreadZipStream.IsAlive) And Not ErrorFlag Then

                    'Если есть ошибка, прекращаем подготовку данных
                    If ErrorFlag Then

                        If (BaseThread IsNot Nothing) AndAlso BaseThread.IsAlive Then BaseThread.Abort()
                        If ThreadZipStream.IsAlive Then ThreadZipStream.Abort()

                        PackFinished = True

                    End If
                    Thread.Sleep(1000)
                    LockCount += 1


                    GoTo endproc

                ElseIf Not PackFinished And Not ErrorFlag And UpdateType <> 5 And Not WayBillsOnly Then

                    Addition &= "; Нет работающих потоков, данные не готовы."
                    UpdateType = 5

                    ErrorFlag = True

                End If
            End If


            If Len(Addition) = 0 Then Addition = MessageH & " " & MessageD

            If NewZip And Not ErrorFlag Then
                ArhiveTS = Now().Subtract(ArhiveStartTime)

                If Math.Round(ArhiveTS.TotalSeconds, 0) > 30 Then

                    Addition &= "Архивирование: " & Math.Round(ArhiveTS.TotalSeconds, 0) & "; "

                End If

            End If

            ProtocolUpdatesThread.Start()

            If ErrorFlag Then

                If Len(MessageH) = 0 Then
                    ResStr = "Error=При подготовке обновления произошла ошибка.;Desc=Пожалуйста, повторите запрос данных через несколько минут."
                Else
                    ResStr = "Error=" & MessageH & ";Desc=" & MessageD
                End If

            Else


                While GUpdateId = 0
                    Thread.Sleep(500)
                End While

                ResStr = "URL=" & Context.Request.Url.Scheme & Uri.SchemeDelimiter & Context.Request.Url.Authority & Context.Request.ApplicationPath & "/GetFileHandler.ashx?Id=" & GUpdateId & ";New=" & NewZip & ";Cumulative=" & (UpdateType = 2)

                If Message.Length > 0 Then ResStr &= ";Addition=" & Message

                'Если параметр ClientHFile имеет значение Nothing, то произошел вызов метода GetUserData и в этом случае работать с файлом hosts не надо
                'производим подмену DNS, если версия программы старше 960
                If (ClientHFile IsNot Nothing) And (BuildNo > 960) Then
                    Try
                        ResStr &= HostsFileHelper.ProcessDNS(SpyHostsFile)
                    Catch HostsException As Exception
                        MailErr("Ошибка во время обработки DNS", HostsException.ToString())
                    End Try
                End If

                'Если поднят флаг SpyAccount, то надо отправлять данные с логином и паролем
                If SpyAccount Then ResStr &= ";SendUData=True"

            End If
            GetUserDataEx = ResStr

        Catch ex As Exception
            Log.Error("Параметры " & _
                String.Format("AccessTime = {0}, ", AccessTime) & _
                String.Format("GetEtalonData = {0}, ", GetEtalonData) & _
                String.Format("EXEVersion = {0}, ", EXEVersion) & _
                String.Format("MDBVersion = {0}, ", MDBVersion) & _
                String.Format("UniqueID = {0}, ", UniqueID) & _
                String.Format("WINVersion = {0}, ", WINVersion) & _
                String.Format("WINDesc = {0}, ", WINDesc) & _
                String.Format("WayBillsOnly = {0}", WayBillsOnly), ex)
            UpdateType = 6
            GetUserDataEx = "Error=При подготовке обновления произошла ошибка.;Desc=Пожалуйста, повторите запрос данных через несколько минут."
        Finally

            If NeedFreeLock Then ReleaseLock(UserId, "GetUserData")

        End Try
        DBDisconnect()
        GC.Collect()
    End Function


    Private Sub MySQLFileDelete(ByVal FileName As String)
        If MySQLResultFile.Exists(FileName) Then MySQLResultFile.Delete(FileName)
    End Sub


    Enum ТипДокумента As Integer
        WayBills = 1
        Rejects = 2
        Docs = 3
    End Enum



    Public Sub ZipStream() ' В потоке ThreadZipStream
        Try


            ArhiveStartTime = Now()
            Const SevenZipExe As String = "C:\Program Files\7-Zip\7z.exe"
            Dim SevenZipParam As String = " -mx7 -bd -slp -mmt=6 -w" & Path.GetTempPath
            Dim SevenZipTmpArchive, Name As String
            Dim xRow As DataRow
            Dim FileName, Вывод7Z, Ошибка7Z As String
            zipfilecount = 0
            Dim xset As New DataTable
            Dim ArchTrans As MySqlTransaction
            Dim ef(), СписокФайлов() As String

            If ArchCn.State = ConnectionState.Closed Then
                ArchCn = ConnectionManager.GetConnection()
                ArchCn.Open()
            End If


            Dim Pr As Process
            Dim startInfo As ProcessStartInfo



            If Reclame Then
                SevenZipTmpArchive = Path.GetTempPath() & "r" & UserId
            Else
                SevenZipTmpArchive = Path.GetTempPath() & UserId
            End If

            SevenZipTmpArchive &= "T.zip"
            MySQLFileDelete(SevenZipTmpArchive)


            'Если не реклама
            Dim helper = New UpdateHelper(UpdateData, Nothing, Nothing)
            If Not Reclame Then

                Try
                    ArchCmd.Connection = ArchCn
                    ArchCmd.CommandText = helper.GetDocumentsCommand()
                    ArchCmd.Parameters.AddWithValue("?UserId", UpdateData.UserId)
                    ArchCmd.Parameters.AddWithValue("?ClientCode", UpdateData.ClientId)

                    ArchDA.SelectCommand = ArchCmd
                    ArchDA.Fill(DS, "DocumentsToClient")

                    If DS.Tables("DocumentsToClient").Rows.Count > 0 Then

                        ArchCmd.CommandText = "" & _
                          "SELECT  * " & _
                          "FROM    AnalitFDocumentsProcessing limit 0"
                        ArchDA.FillSchema(DS, SchemaType.Source, "ProcessingDocuments")


                        Dim Row As DataRow

                        For Each Row In DS.Tables("DocumentsToClient").Rows

                            СписокФайлов = Directory.GetFiles(ПутьКДокументам & _
                                Row.Item("ClientCode").ToString & _
                                "\" & _
                                CType(Row.Item("DocumentType"), ТипДокумента).ToString, _
                                Row.Item("RowId").ToString & "_*")

                            If СписокФайлов.Length = 1 Then

                                xRow = DS.Tables("ProcessingDocuments").NewRow

                                startInfo = New ProcessStartInfo(SevenZipExe)
                                startInfo.CreateNoWindow = True
                                startInfo.RedirectStandardOutput = True
                                startInfo.RedirectStandardError = True
                                startInfo.UseShellExecute = False
                                startInfo.StandardOutputEncoding = System.Text.Encoding.GetEncoding(866)
                                'startInfo.UserName = Пользователь
                                'startInfo.Password = БезопасныйПароль

                                startInfo.Arguments = "a """ & _
                                   SevenZipTmpArchive & """ " & _
                                   " -i!""" & _
                                   CType(Row.Item("DocumentType"), ТипДокумента).ToString & "\" & _
                                   Path.GetFileName(СписокФайлов(0)) & _
                                   """ " & _
                                   SevenZipParam

                                startInfo.WorkingDirectory = ПутьКДокументам & _
                                   Row.Item("ClientCode").ToString

                                xRow.Item("DocumentId") = Row.Item("RowId").ToString

                                Pr = New Process
                                Pr.StartInfo = startInfo
                                Pr = Process.Start(startInfo)
                                '#If Not Debug Then
                                '                                Pr.ProcessorAffinity = New IntPtr(ZipProcessorAffinityMask)
                                '#End If

                                Pr.WaitForExit()


                                Вывод7Z = Pr.StandardOutput.ReadToEnd
                                Ошибка7Z = Pr.StandardError.ReadToEnd

                                If Pr.ExitCode <> 0 Then

                                    MySQLFileDelete(SevenZipTmpArchive)
                                    Addition &= "Архивирование документов, Вышли из 7Z с ошибкой: " & _
                                       Вывод7Z & _
                                       "-" & _
                                       Ошибка7Z & _
                                       "; "

                                    If Documents Then

                                        Throw New Exception(String.Format("SevenZip error: {0}", Вывод7Z & _
                                         "-" & _
                                         Ошибка7Z))

                                    Else

                                        Utils.Mail("Архивирование документов", "Вышли из 7Z с ошибкой: " & ": " & _
                                          Вывод7Z & _
                                         "-" & _
                                          Ошибка7Z)


                                    End If

                                End If

                                DS.Tables("ProcessingDocuments").Rows.Add(xRow)

                            ElseIf СписокФайлов.Length = 0 Then
                                Addition &= "При подготовке документов в папке: " & _
                                 ПутьКДокументам & _
                                   Row.Item("ClientCode").ToString & _
                                   "\" & _
                                   CType(Row.Item("DocumentType"), ТипДокумента).ToString & _
                                   " не найден документ № " & _
                                   Row.Item("RowId").ToString & _
                                   " ; "

                            Else


                            End If


                        Next

                    End If


                Catch ex As Exception
                    Log.Error("Ошибка при архивировании документов", ex)
                    MailErr("Архивирование документов", ex.Source & ": " & ex.Message)
                    Addition &= "Архивирование документов" & ": " & ex.Message & "; "

                    If Documents Then ErrorFlag = True

                    MySQLFileDelete(SevenZipTmpArchive)

                End Try


                If Documents Then
                    If File.Exists(SevenZipTmpArchive) Then

                        File.Move(SevenZipTmpArchive, ResultFileName & UserId & ".zip")
                        File.SetAttributes(ResultFileName & UserId & ".zip", FileAttributes.NotContentIndexed)
                        PackFinished = True
                        FileInfo = New FileInfo(ResultFileName & UserId & ".zip")
                        ResultLenght = Convert.ToUInt32(FileInfo.Length)
                        If ArchCn.State = ConnectionState.Open Then ArchCn.Close()
                        Exit Sub

                    Else

                        MessageH = "Новых файлов документов нет."
                        Addition &= " Нет новых документов"
                        ErrorFlag = True
                        PackFinished = True
                        If ArchCn.State = ConnectionState.Open Then ArchCn.Close()
                        Exit Sub

                    End If

                End If




                'Если не документы
                If Not Documents Then

                    'Архивирование обновления программы
                    Try
                        ArchTrans = ArchCn.BeginTransaction(IsoLevel)
                        ArchCmd.CommandText = "select EnableUpdate from retclientsset where clientcode=" & CCode
                        EnableUpdate = Convert.ToBoolean(ArchCmd.ExecuteScalar)
                        ArchTrans.Commit()

                        If EnableUpdate And Directory.Exists(ResultFileName & "Updates\Future_" & BuildNo & "\EXE") Then

                            ef = Directory.GetFiles(ResultFileName & "Updates\Future_" & BuildNo & "\EXE")

                            If ef.Length > 0 Then
                                'Pr.StartInfo.UserName = Пользователь
                                'Pr.StartInfo.Password = БезопасныйПароль
                                Pr = System.Diagnostics.Process.Start(SevenZipExe, "a """ & SevenZipTmpArchive & """  """ & ResultFileName & "Updates\Future_" & BuildNo & "\EXE"" " & SevenZipParam)

#If Not Debug Then
                                try
                                Pr.ProcessorAffinity = New IntPtr(ZipProcessorAffinityMask)
                                catch
                                End try
#End If

                                Pr.WaitForExit()

                                If Pr.ExitCode <> 0 Then
                                    MailErr("Архивирование EXE", "Вышли из 7Z с кодом " & ": " & Pr.ExitCode)
                                    Addition &= "Архивирование обновления версии, Вышли из 7Z с кодом " & ": " & Pr.ExitCode & "; "
                                    'Try
                                    '    If Not pr Is Nothing Then pr.Kill()
                                    '    System.Threading.Thread.Sleep(2000)
                                    'Catch
                                    'End Try
                                    MySQLFileDelete(SevenZipTmpArchive)
                                Else

                                    'Mail("service@analit.net", "Обновление программы с версии " & BuildNo, MailFormat.Text, "Код клиента: " & CCode, "service@analit.net", System.Text.Encoding.UTF8)
                                    Addition &= "Обновление включает в себя новую версию программы; "
                                End If

                            End If

                        End If

                    Catch ex As ThreadAbortException

                        'ErrorFlag = True
                        If Not Pr Is Nothing Then
                            If Not Pr.HasExited Then Pr.Kill()
                            Pr.WaitForExit()
                        End If
                        MySQLFileDelete(SevenZipTmpArchive)

                    Catch ex As Exception
                        MailErr("Архивирование Exe", ex.Source & ": " & ex.Message)
                        Addition &= " Архивирование обновления " & ": " & ex.Message & "; "
                        If Not Pr Is Nothing Then
                            If Not Pr.HasExited Then Pr.Kill()
                            Pr.WaitForExit()
                        End If
                        MySQLFileDelete(SevenZipTmpArchive)
                    End Try

                    ArchTrans = Nothing
                    ArchCmd.Transaction = Nothing


                    'Архивирование FRF
                    Try
                        If EnableUpdate And Directory.Exists(ResultFileName & "Updates\Future_" & BuildNo & "\FRF") Then
                            ef = Directory.GetFiles(ResultFileName & "Updates\Future_" & BuildNo & "\FRF")
                            If ef.Length > 0 Then
                                For Each Name In ef
                                    FileInfo = New FileInfo(Name)
                                    If FileInfo.Extension = ".frf" And FileInfo.LastWriteTime.Subtract(OldUpTime).TotalSeconds > 0 Then
                                        'Pr.StartInfo.UserName = Пользователь
                                        'Pr.StartInfo.Password = БезопасныйПароль
                                        Pr = System.Diagnostics.Process.Start(SevenZipExe, "a """ & SevenZipTmpArchive & """  """ & FileInfo.FullName & """  " & SevenZipParam)


#If Not Debug Then
                                        try
                                Pr.ProcessorAffinity = New IntPtr(ZipProcessorAffinityMask)
                                        catch
                                        End try
#End If

                                        Pr.WaitForExit()

                                        If Pr.ExitCode <> 0 Then
                                            MailErr("Архивирование Frf", "Вышли из 7Z с кодом " & ": " & Pr.ExitCode)
                                            Addition &= " Архивирование Frf, Вышли из 7Z с кодом " & ": " & Pr.ExitCode & "; "
                                            'If Not pr Is Nothing Then pr.Kill()
                                            'System.Threading.Thread.Sleep(2000)

                                            MySQLFileDelete(SevenZipTmpArchive)
                                        End If
                                    End If
                                Next
                            End If
                        End If

                    Catch ex As ThreadAbortException

                        If Not Pr Is Nothing Then
                            If Not Pr.HasExited Then Pr.Kill()
                            Pr.WaitForExit()
                        End If
                        MySQLFileDelete(SevenZipTmpArchive)

                    Catch ex As Exception

                        Addition &= " Архивирование Frf: " & ex.Message & "; "
                        MailErr("Архивирование Frf", ex.Source & ": " & ex.Message)

                        If Not Pr Is Nothing Then
                            If Not Pr.HasExited Then Pr.Kill()
                            Pr.WaitForExit()
                        End If
                        MySQLFileDelete(SevenZipTmpArchive)

                    End Try
                End If
            End If


            'Архивирование данных, или рекламы
            Try
                Dim FileForArchive As FileForArchive
                If Not Documents Then

StartZipping:
                    If ErrorFlag Then Exit Sub

                    If FilesForArchive.Count > 0 Then

                        SyncLock (FilesForArchive)
                            FileForArchive = FilesForArchive.Dequeue
                        End SyncLock

                    Else

                        Thread.Sleep(500)
                        GoTo StartZipping

                    End If




                    If Reclame Then
                        FileName = ReclamePath & FileForArchive.FileName
                    Else

                        FileName = MySqlFilePath & FileForArchive.FileName & UserId & ".txt"


                        While Not File.Exists(FileName)
                            i += 1
                            Thread.Sleep(500)
                            If i > 50 Then Err.Raise(1, , "Файл" & FileName & " так и не появился")
                        End While
                    End If

                    Pr = New Process

                    startInfo = New ProcessStartInfo(SevenZipExe)
                    startInfo.CreateNoWindow = True
                    startInfo.RedirectStandardOutput = True
                    startInfo.RedirectStandardError = True
                    startInfo.UseShellExecute = False
                    startInfo.StandardOutputEncoding = System.Text.Encoding.GetEncoding(866)
                    startInfo.Arguments = String.Format(" a ""{0}"" ""{1}"" {2}", SevenZipTmpArchive, FileName, SevenZipParam)
                    startInfo.FileName = SevenZipExe

                    Pr.StartInfo = startInfo

                    Pr.Start()
                    If Not Pr.HasExited Then
#If Not Debug Then
                        Try
                            Pr.ProcessorAffinity = New IntPtr(ZipProcessorAffinityMask)
                        Catch
                        End Try
#End If
                    End If

                    Вывод7Z = Pr.StandardOutput.ReadToEnd
                    Ошибка7Z = Pr.StandardError.ReadToEnd

                    Pr.WaitForExit()

                    If Pr.ExitCode <> 0 Then
                        Addition &= String.Format(" SevenZip exit code : {0}, :" & Pr.StandardError.ReadToEnd, Pr.ExitCode)
                        MySQLFileDelete(SevenZipTmpArchive)
                        Throw New Exception(String.Format("SevenZip exit code : {0}, {1}, {2}, {3}; ", Pr.ExitCode, startInfo.Arguments, Вывод7Z, Ошибка7Z))
                    End If
                    Pr = Nothing
                    If Not Reclame Then MySQLFileDelete(FileName)
                    zipfilecount += 1

                    If zipfilecount >= FileCount Then

                        'ArchCmd.CommandText = "delete from ready_client_files where clientcode=" & CCode
                        'ArchCmd.CommandText &= " and reclame="

                        If Reclame Then

                            'ArchCmd.CommandText &= "1"
                            File.Move(SevenZipTmpArchive, ResultFileName & "r" & UserId & ".zip")

                        Else

                            'ArchCmd.CommandText &= "0"
                            File.Move(SevenZipTmpArchive, ResultFileName & UserId & ".zip")
                            If UpdateType = 2 Then File.SetAttributes(ResultFileName & UserId & ".zip", FileAttributes.Normal)

                            FileInfo = New FileInfo(ResultFileName & UserId & ".zip")
                            ResultLenght = Convert.ToUInt32(FileInfo.Length)

                        End If
                        'ArchCmd.ExecuteNonQuery()

                        PackFinished = True
                        Exit Sub

                    Else

                        GoTo StartZipping

                    End If

                End If




            Catch ex As ThreadAbortException
                MySQLFileDelete(SevenZipTmpArchive)

                Try

                    Pr.Kill()
                    Pr.WaitForExit()

                Catch
                End Try


            Catch ex As MySqlException

                'If Not ArchTrans Is Nothing Then ArchTrans.Rollback()
                If Not Pr Is Nothing Then
                    If Not Pr.HasExited Then Pr.Kill()
                    Pr.WaitForExit()
                End If
                MySQLFileDelete(SevenZipTmpArchive)

                If Not TypeOf ex.InnerException Is ThreadAbortException Then
                    ErrorFlag = True
                    UpdateType = 6
                    MailErr("Архивирование", ex.Source & ": " & ex.ToString())
                End If
                Addition &= " Архивирование: " & ex.ToString() & "; "

            Catch Unhandled As Exception

                ErrorFlag = True
                UpdateType = 6
                If Not Pr Is Nothing Then
                    If Not Pr.HasExited Then Pr.Kill()
                    Pr.WaitForExit()
                End If
                Addition &= " Архивирование: " & Unhandled.ToString()
                MySQLFileDelete(SevenZipTmpArchive)
                MailErr("Архивирование", Unhandled.Source & ": " & Unhandled.ToString())
                Addition &= " Архивирование: " & Unhandled.ToString() & "; "
                'If Not ArchTrans Is Nothing Then ArchTrans.Rollback()

            Finally
                If ArchCn.State = ConnectionState.Open Then ArchCn.Close()
            End Try

        Catch tae As ThreadAbortException

        Catch Unhandled As Exception
            MailErr("Архивирование general", Unhandled.Source & ": " & Unhandled.ToString())
            ErrorFlag = True
        End Try
    End Sub





    <WebMethod()> _
    Public Function MaxSynonymCode(ByVal Log As String, _
       ByVal PriceCode As UInt32(), _
       ByVal UpdateId As UInt32, _
       ByVal WayBillsOnly As Boolean) As Date
        Dim UpdateTime As Date
        Cm.Transaction = Nothing
        ClientLog = Log
        GUpdateId = UpdateId

        If DBConnect("MaxSynonymCode") Then
            GetClientCode()
            UpdateType = 7

            If Not Counter.Counter.TryLock(UserId, "MaxSynonymCode") Then
                Return DateTime.Now
            End If

            If Not WayBillsOnly Or Not File.GetAttributes(ResultFileName & UserId & ".zip") = FileAttributes.NotContentIndexed Then

                AbsentPriceCodes = String.Empty
                If (PriceCode IsNot Nothing) AndAlso (PriceCode.Length > 0) AndAlso (PriceCode(0) <> 0) Then
                    AbsentPriceCodes = PriceCode(0).ToString
                    Dim I As Integer
                    For I = 1 To PriceCode.Length - 1
                        AbsentPriceCodes &= "," & PriceCode(I)
                    Next
                End If
                SetResultCodes.Start()
                SetResultCodes.Join()

            End If

            Try

                If Not WayBillsOnly Then
                    Cm.Connection = ReadOnlyCn
                    Запрос = "select UncommitedUpdateDate from UserUpdateInfo  where UserId=" & UserId & "; "
                    Cm.CommandText = Запрос
                    Using SQLdr As MySqlDataReader = Cm.ExecuteReader
                        SQLdr.Read()
                        UpdateTime = SQLdr.GetDateTime(0)
                    End Using
                End If

            Catch ex As Exception
                MailErr("Выборка даты обновления ", ex.Message & ex.Source)
                UpdateTime = Now().ToUniversalTime
            End Try

            If SetResultCodes.IsAlive Then SetResultCodes.Join()

            'ProtocolUpdatesThread.Start()

            MaxSynonymCode = UpdateTime.ToUniversalTime
        Else
            MaxSynonymCode = Now().ToUniversalTime
        End If

        Try

            Cm.CommandText = "select SaveAFDataFiles from UserUpdateInfo  where UserId=" & UserId & "; "
            If Convert.ToBoolean(Cm.ExecuteScalar) Then
                If Not Directory.Exists(ResultFileName & "\Archive\" & UserId) Then Directory.CreateDirectory(ResultFileName & "\Archive\" & UserId)
                File.Copy(ResultFileName & UserId & ".zip", ResultFileName & "\Archive\" & UserId & "\" & UpdateId & ".zip")
            End If

            MySQLFileDelete(ResultFileName & UserId & ".zip")
            MySQLFileDelete(ResultFileName & "r" & UserId & "Old.zip")

        Catch ex As Exception
            'MailErr("Удаление полученных файлов;", ex.Message)
        End Try

        DBDisconnect()
        ProtocolUpdatesThread.Start()
        ReleaseLock(UserId, "MaxSynonymCode")
    End Function

    Private Sub GetClientCode()
        UserName = HttpContext.Current.User.Identity.Name
        If Left(UserName, 7) = "ANALIT\" Then
            UserName = Mid(UserName, 8)
        End If
        Try
            UpdateData = UpdateHelper.GetUpdateData(ReadOnlyCn, UserName)

            If Not UpdateData Is Nothing Then
                CCode = UpdateData.ClientId
                UserId = UpdateData.UserId
                CheckID = UpdateData.CheckCopyId
                Message = UpdateData.Message
                OldUpTime = UpdateData.OldUpdateTime
                UncDT = UpdateData.UncommitedUpdateTime
                SpyHostsFile = UpdateData.Spy
                SpyAccount = UpdateData.SpyAccount

            End If

            With Cm
                .Parameters.Add(New MySqlParameter("?UserName", MySqlDbType.VarString))
                .Parameters("?UserName").Value = UserName

                .Parameters.Add(New MySqlParameter("?ClientCode", MySqlDbType.Int32))
                .Parameters("?ClientCode").Value = CCode
            End With

            If CCode > 0 Then
                Cm.Connection = ReadWriteCn
                Cm.Transaction = Nothing
                Cm.CommandText = "" & _
                 "UPDATE Logs.AuthorizationDates A " & _
                 "SET     AFTime    =now() " & _
                 "WHERE   UserId=" & UserId
                Dim AuthorizationDatesCounter As Integer = Cm.ExecuteNonQuery()

                If AuthorizationDatesCounter <> 1 Then

                    Addition &= "Нет записи в AuthorizationDates (" & UserId & "); "

                End If
            End If


        Catch ErrorTXT As Exception
            UpdateType = 6
            ErrorFlag = True
            MailErr("Получение информации о клиенте", ErrorTXT.Message)
        End Try
    End Sub



    Private Function DBConnect(ByVal FromProcess As String) As Boolean
        UserHost = HttpContext.Current.Request.UserHostAddress
        Try
            'ReadOnlyCn = New MySqlConnection
            ReadOnlyCn = ConnectionManager.GetConnection()
            ReadOnlyCn.Open()

            ReadWriteCn = New MySqlConnection
            ReadWriteCn.ConnectionString = Settings.ConnectionString()
            ReadWriteCn.Open()

            Return True
        Catch err As Exception
            MailErr("Соединение с БД", err.Message)
            ErrorFlag = True
            UpdateType = 6
            Return False
        End Try
    End Function





    Private Sub DBDisconnect()

        Dim ResultCode As Int16 = 2
        Try
            'MailErr("Отсоединение от БД", "Поток №" & ReadOnlyCn.ServerThread)

            ReadOnlyCn.Close()
            'ReadOnlyCn.Dispose()

            ReadWriteCn.Close()
            'ReadWriteCn.Dispose()

            'Cm.Dispose()
            'ReadOnlyCn.Dispose()
        Catch err As Exception
            MailErr("Закрытие соединения", err.Message)
            ErrorFlag = True
            UpdateType = 6
        End Try
    End Sub

    <WebMethod()> Public Function GetArchivedOrdersList() As String
        'If DBConnect("GetArchivedOrdersList") Then

        '    'TODO: Встроить логирование в prgdataex
        '    Try
        '        GetClientCode()

        '        'Если смогли получить код клиента
        '        If CCode > 0 Then
        '            Dim dsOrderList As DataSet = MySqlHelper.ExecuteDataset(Cm.Connection, "SELECT o.ClientOrderId FROM orders.ordershead o LEFT JOIN orders.orderslist ol ON ol.OrderID=o.RowID where ol.OrderID is null and o.WriteTime between '2007-09-16 20:34:02' and '2007-09-24 11:02:44' and o.ClientCode = ?ClientCode limit 50", New MySqlParameter("?ClientCode", CCode))
        '            Dim list As List(Of String) = New List(Of String)
        '            Dim drOrderId As DataRow

        '            For Each drOrderId In dsOrderList.Tables(0).Rows
        '                list.Add(drOrderId.Item("ClientOrderId").ToString())
        '            Next
        '            'MailErr("Запросили у клиента архивные заказы", list.Count & " шт.")
        '            Return String.Join(";", list.ToArray())
        '        Else
        '            Return String.Empty
        '        End If

        '    Catch Exp As Exception
        '        MailErr("Ошибка при получении списка архивных заказов", Exp.Message & ": " & Exp.StackTrace)
        '        Addition = Exp.Message
        '        ErrorFlag = True
        '        UpdateType = 5
        '        Return String.Empty
        '    Finally
        '        DBDisconnect()
        '    End Try
        'Else
        '    Return String.Empty
        'End If
        Return String.Empty
    End Function




    <WebMethod()> _
    Public Function PostOrder2(ByVal UniqueID As String, _
     ByVal ServerOrderId As UInt32, _
     ByVal ClientCode As UInt32, _
     ByVal PriceCode As UInt32, _
     ByVal RegionCode As UInt64, _
     ByVal PriceDate As Date, _
     ByVal ClientAddition As String, _
     ByVal RowCount As UInt16, _
     ByVal ProductID() As UInt32, _
     ByVal ClientOrderID As UInt32, _
     ByVal CodeFirmCr() As String, _
     ByVal SynonymCode() As UInt32, _
     ByVal SynonymFirmCrCode() As String, _
     ByVal Code() As String, _
     ByVal CodeCr() As String, _
     ByVal Quantity As UInt16(), _
     ByVal Junk As Boolean(), _
     ByVal Await As Boolean(), _
     ByVal Cost As Decimal(), _
     ByVal MinCost() As String, _
     ByVal MinPriceCode() As String, _
     ByVal LeaderMinCost() As String, _
     ByVal LeaderMinPriceCode() As String) As String



        Return PostOrder(UniqueID, _
           ServerOrderId, _
           ClientCode, _
           PriceCode, _
           RegionCode, _
           PriceDate, _
           ClientAddition, _
           RowCount, _
           ProductID, _
           ClientOrderID, _
           CodeFirmCr, _
           SynonymCode, _
           SynonymFirmCrCode, _
           Code, _
           CodeCr, _
           Quantity, _
           Junk, _
           Await, _
           Cost, _
           MinCost, _
           MinPriceCode, _
           LeaderMinCost, _
           Nothing, _
           Nothing, _
           Nothing, _
           LeaderMinPriceCode)

    End Function

    'Private Function ParseDecimal(ByVal InString As String()) As Decimal()
    '    Dim ResDecimal As Decimal()
    '    Dim i As Integer

    '    If InString.Length > 0 Then

    '        For i = 0 To InString.Length - 1

    '            ResDecimal(i) = New Decimal
    '            Decimal.TryParse(InString(i), ResDecimal(i))

    '        Next

    '    Else
    '        Return ResDecimal
    '        Return Nothing
    '    End If

    'End Function



    <WebMethod()> _
    Public Function PostOrder(ByVal UniqueID As String, _
       ByVal ServerOrderId As UInt32, _
       ByVal ClientCode As UInt32, _
       ByVal PriceCode As UInt32, _
       ByVal RegionCode As UInt64, _
       ByVal PriceDate As Date, _
       ByVal ClientAddition As String, _
       ByVal RowCount As UInt16, _
       ByVal ProductID As UInt32(), _
       ByVal ClientOrderID As UInt32, _
       ByVal CodeFirmCr As String(), _
       ByVal SynonymCode As UInt32(), _
       ByVal SynonymFirmCrCode As String(), _
       ByVal Code As String(), _
       ByVal CodeCr As String(), _
       ByVal Quantity As UInt16(), _
       ByVal Junk As Boolean(), _
       ByVal Await As Boolean(), _
       ByVal Cost As Decimal(), _
       ByVal MinCost As String(), _
       ByVal MinPriceCode As String(), _
       ByVal LeaderMinCost As String(), _
       ByVal RequestRatio As String(), _
       ByVal OrderCost As String(), _
       ByVal MinOrderCount As String(), _
       ByVal LeaderMinPriceCode As String()) As String

        Dim ResStr As String
        Dim OID As UInt64
        Dim LockCount As Integer
        Dim SumOrder, WeeklySumOrder As UInt32
        Dim it As Integer

        Try

            If DBConnect("PostOrder") Then

                GetClientCode()

                If CCode = Nothing Then
                    CCode = ClientCode
                    UpdateType = 5
                    MessageH = "Доступ закрыт."
                    MessageD = "Пожалуйста, обратитесь в АК ""Инфорум""."
                    ErrorFlag = True

                    GoTo ItsEnd
                End If



                If Not Counter.Counter.TryLock(UserId, "PostOrder") Then
                    MessageH = "Отправка заказов в настоящее время невозможна."
                    MessageD = "Пожалуйста, повторите попытку через несколько минут.[7]"
                    Addition &= "Перегрузка; "
                    UpdateType = 5
                    ErrorFlag = True
                    GoTo ItsEnd
                End If

                Dim helper = New OrderHelper(UpdateData, ReadOnlyCn, ReadWriteCn)

                If ServerOrderId = 0 Then

                    'Проверяем совпадение уникального идентификатора
                    If CheckID Then
                        UID = UniqueID
                        If Not FnCheckID() Then
                            MessageH = "Отправка заказов на данном компьютере запрещена."
                            MessageD = "Пожалуйста, обратитесь в АК «Инфорум».[2]"
                            Addition = "Несоответствие UIN."
                            UpdateType = 5
                            ErrorFlag = True

                            GoTo ItsEnd
                        End If
                    End If

                    With Cm
                        .Connection = ReadOnlyCn
                        .Transaction = Nothing
                    End With

                    If Not helper.CanPostOrder(ClientCode) Then
                        UpdateType = 5
                        MessageH = "Отправка заказов запрещена."
                        MessageD = "Пожалуйста обратитесь в АК ""Инфорум""."
                        ErrorFlag = True
                        'MailErr("Недопустимый код клиента при отправке заказов.", "Код клиента: " & CCode)
                    End If

                    If ErrorFlag Then GoTo ItsEnd

                    'Контроль превышения суммы закупок за неделю

                    Cm.CommandText = "" & _
                        "SELECT ROUND(IF(SUM(cost                  *quantity)>RCS.MaxWeeklyOrdersSum " & _
                        "   AND CheCkWeeklyOrdersSum,SUM(cost*quantity), 0),0) " & _
                        "FROM   orders.OrdersHead Oh, " & _
                        "       orders.OrdersList Ol, " & _
                        "       RetClientsSet RCS " & _
                        "WHERE  WriteTime               >curdate() - interval dayofweek(curdate())-2 DAY " & _
                        "   AND Oh.RowId                =ol.OrderId " & _
                        "   AND RCS.ClientCode          =oh.ClientCode " & _
                        "   AND RCS.CheCkWeeklyOrdersSum=1 " & _
                        "   AND RCS.clientcode          =" & CCode

                    WeeklySumOrder = Convert.ToUInt32(Cm.ExecuteScalar)

                    If WeeklySumOrder > 0 Then
                        UpdateType = 5
                        MessageH = "Превышен недельный лимит заказа (уже заказано на " & WeeklySumOrder & " руб.)"
                        ' MessageD = 
                        ErrorFlag = True

                    End If

                    'начинаем проверять минимальный заказ
                    Try
                        Dim minReq = helper.GetMinReq(ClientCode, RegionCode, PriceCode)

                        If Not minReq Is Nothing And minReq.ControlMinReq And minReq.MinReq > 0 Then
                            SumOrder = 0
                            For it = 0 To Cost.Length - 1
                                SumOrder += Convert.ToUInt32(Math.Round(Quantity(it) * Cost(it), 0))
                            Next
                            If SumOrder < minReq.MinReq Then
                                MessageD = "Поставщик отказал в приеме заказа."
                                MessageH = "Сумма заказа меньше минимально допустимой."
                                UpdateType = 5
                                ErrorFlag = True
                                GoTo ItsEnd
                            End If
                        End If

                    Catch err As Exception
                        Log.Error("Учет минимальной цены", err)
                        UpdateType = 6
                        ErrorFlag = True
                        MailErr("Учет минимальной цены. Клиент: " & CCode, "Ошибка: " & err.Source & ": " & err.StackTrace)
                    End Try

                    If ErrorFlag Then GoTo ItsEnd

                    With Cm

                        .Connection = ReadWriteCn
                        .Transaction = myTrans

                        ResStr = String.Empty
                        Try
                            For i = 1 To Len(ClientAddition) Step 3
                                ResStr &= Chr(Convert.ToInt16(Left(Mid(ClientAddition, i), 3)))
                            Next
                        Catch err As Exception
                            'MailErr("Формирование сообщения поставщику ", ResStr & "; Cимвол: " & Left(Mid(ClientAddition, i), 3) & "; Исходная строка:" & ClientAddition & "; Ошибка:" & err.Message)
                        End Try

                    End With
                End If
RestartInsertTrans:

                Try

                    myTrans = ReadWriteCn.BeginTransaction(IsoLevel)

                    If ServerOrderId = 0 Then
                        OID = helper.SaveOrder(ClientCode, PriceCode, RegionCode, PriceDate, RowCount, ClientOrderID, ResStr)
                        Cm.CommandText = "select CalculateLeader from retclientsset where clientcode=?ClientCode"
                        CalculateLeader = Convert.ToBoolean(Cm.ExecuteScalar)
                    Else

                        'Cm.CommandText = "SELECT RowId FROM orders.ordershead where ClientOrderid=" & ServerOrderId
                        'OID = Convert.ToUInt64(Cm.ExecuteScalar)
                        'MailErr("Приняли архивный заказ", "Заказ №" & ServerOrderId)

                    End If
                OrderInsertCm.Connection = ReadWriteCn
                OrderInsertCm.CommandText = "SELECT " & _
                 "        `MinCost`          , " & _
                 "        `LeaderMinCost`    , " & _
                 "        `PriceCode`         , " & _
                 "        `LeaderPriceCode`   , " & _
                 "        `ProductID`         , " & _
                 "        `CodeFirmCr`       , " & _
                 "        `SynonymCode`      , " & _
                 "        `SynonymFirmCrCode`, " & _
                 "        `Code`             , " & _
                 "        `CodeCr`           , " & _
                 "        `Quantity`         , " & _
                 "        `Junk`             , " & _
                 "        `Await`            , " & _
                 "        `Cost` " & _
                 "FROM    orders.orderslist, " & _
                 "        orders.leaders"

                    OrderInsertDA.FillSchema(DS, SchemaType.Source, "OrdersL")

                    If PostOrderDB(ClientCode, ClientOrderID, ProductID, OID, CodeFirmCr, SynonymCode, SynonymFirmCrCode, Code, CodeCr, Quantity, Junk, Await, Cost, PriceCode, MinCost, MinPriceCode, LeaderMinCost, LeaderMinPriceCode) Then
                        myTrans.Commit()
                        ResultLenght = Convert.ToUInt32(OID)


                        UpdateType = 4
                    Else
                        Try
                            myTrans.Rollback()
                        Catch
                        End Try
                        OID = 0
                    End If



                Catch MySQLErr As MySqlException
                    Try
                        myTrans.Rollback()
                    Catch
                    End Try

                    If MySQLErr.Number = 1213 Or MySQLErr.Number = 1205 _
                     Or MySQLErr.Number = 1422 Or MySQLErr.Number = 1062 Then
                        System.Threading.Thread.Sleep(500)
                        LockCount += 1
                        GoTo RestartInsertTrans
                    End If

                    MailErr("Постинг заказа. Клиент: " & CCode, "Ошибка MySQL(" & MySQLErr.Number & "): " & MySQLErr.Message & " в " & MySQLErr.Source & ": " & MySQLErr.StackTrace)

                    UpdateType = 6
                    ErrorFlag = True

                Catch err As Exception

                    MailErr("Постинг заказа. Клиент: " & CCode, "Ошибка: " & err.Message & ": " & err.Source & ": " & err.StackTrace)

                    Try
                        myTrans.Rollback()
                    Catch
                    End Try

                    UpdateType = 6
                    ErrorFlag = True


                End Try

ItsEnd:

                DBDisconnect()


            Else
                MessageH = "Отправка заказов завершилась неудачно."
                MessageD = "Пожалуйста повторите попытку через несколько минут."
                ErrorFlag = True
            End If

        Catch ex As Exception
            Log.Error("Ошибка при отправке заказа", ex)
            MessageH = "Отправка заказов завершилась неудачно."
            MessageD = "Пожалуйста повторите попытку через несколько минут."
            ErrorFlag = True
        Finally
            ReleaseLock(UserId, "PostOrder")
        End Try

        If ErrorFlag Or UpdateType > 4 Then
            If Len(MessageH) = 0 Then
                PostOrder = "Error=Отправка заказов завершилась неудачно.;Desc=Некоторые заявки не были обработанны."
            Else
                Addition = MessageH & " " & MessageD
                PostOrder = "Error=" & MessageH & ";Desc=" & MessageD
            End If
        Else
            PostOrder = "OrderID=" & OID
        End If

    End Function

    'Отправляем несколько заказов скопом и по ним все формируем ответ
    <WebMethod()> _
    Public Function PostSomeOrders( _
        ByVal UniqueID As String, _
        ByVal ForceSend As Boolean, _
        ByVal UseCorrectOrders As Boolean, _
        ByVal ClientCode As UInt32, _
        ByVal OrderCount As UInt16, _
        ByVal ClientOrderID As UInt64(), _
        ByVal PriceCode As UInt64(), _
        ByVal RegionCode As UInt64(), _
        ByVal PriceDate As Date(), _
        ByVal ClientAddition As String(), _
        ByVal RowCount As UInt16(), _
        ByVal ClientPositionID As UInt64(), _
        ByVal ClientServerCoreID As UInt64(), _
        ByVal ProductID As UInt64(), _
        ByVal CodeFirmCr As String(), _
        ByVal SynonymCode As UInt64(), _
        ByVal SynonymFirmCrCode As String(), _
        ByVal Code As String(), _
        ByVal CodeCr As String(), _
        ByVal Junk As Boolean(), _
        ByVal Await As Boolean(), _
        ByVal RequestRatio As String(), _
        ByVal OrderCost As String(), _
        ByVal MinOrderCount As String(), _
        ByVal Quantity As UInt16(), _
        ByVal Cost As Decimal(), _
        ByVal MinCost As String(), _
        ByVal MinPriceCode As String(), _
        ByVal LeaderMinCost As String(), _
        ByVal LeaderMinPriceCode As String()) As String

        Dim ResStr As String = String.Empty

        Try

            If DBConnect("PostOrder") Then

                GetClientCode()

                If CCode = Nothing Then
                    CCode = ClientCode
                    Throw New OrderUpdateException(True, 5, "Доступ закрыт.", "Пожалуйста, обратитесь в АК ""Инфорум"".")
                End If

                If Not Counter.Counter.TryLock(UserId, "PostOrder") Then
                    Addition &= "Перегрузка; "
                    Throw New OrderUpdateException(True, 5, _
                        "Отправка заказов в настоящее время невозможна.", _
                        "Пожалуйста, повторите попытку через несколько минут.[7]")
                End If

                'Проверяем совпадение уникального идентификатора
                If CheckID Then
                    UID = UniqueID
                    If Not FnCheckID() Then
                        Addition = "Несоответствие UIN."
                        Throw New OrderUpdateException(True, 5, _
                            "Отправка заказов на данном компьютере запрещена.", _
                            "Пожалуйста, обратитесь в АК «Инфорум».[2]")
                    End If
                End If

                Dim helper = New ReorderHelper(UpdateData, ReadOnlyCn, ReadWriteCn, ForceSend, ClientCode, UseCorrectOrders)

                helper.ParseOrders( _
                    OrderCount, _
                    ClientOrderID, _
                    PriceCode, _
                    RegionCode, _
                    PriceDate, _
                    ClientAddition, _
                    RowCount, _
                    ClientPositionID, _
                    ClientServerCoreID, _
                    ProductID, _
                    CodeFirmCr, _
                    SynonymCode, _
                    SynonymFirmCrCode, _
                    Code, _
                    CodeCr, _
                    Junk, _
                    Await, _
                    RequestRatio, _
                    OrderCost, _
                    MinOrderCount, _
                    Quantity, _
                    Cost, _
                    MinCost, _
                    MinPriceCode, _
                    LeaderMinCost, _
                    LeaderMinPriceCode _
                )


                ResStr = helper.PostSameOrders()

                DBDisconnect()
            Else
                MessageH = "Отправка заказов завершилась неудачно."
                MessageD = "Пожалуйста повторите попытку через несколько минут."
                ErrorFlag = True
            End If

        Catch updateException As OrderUpdateException
            UpdateType = updateException.UpdateType
            MessageH = updateException.MessageHeader
            MessageD = updateException.MessageDescription
            ErrorFlag = updateException.ErrorFlag
        Catch ex As Exception
            Log.Error("Ошибка при отправке заказа", ex)
            MessageH = "Отправка заказов завершилась неудачно."
            MessageD = "Пожалуйста повторите попытку через несколько минут."
            ErrorFlag = True
        Finally
            ReleaseLock(UserId, "PostOrder")
        End Try

        If ErrorFlag Or UpdateType > 4 Then
            If Len(MessageH) = 0 Then
                Return "Error=Отправка заказов завершилась неудачно.;Desc=Некоторые заявки не были обработанны."
            Else
                Addition = MessageH & " " & MessageD
                Return "Error=" & MessageH & ";Desc=" & MessageD
            End If
        Else
            Return ResStr
        End If

    End Function


    Public Sub MailErr(ByVal ErrSource As String, ByVal ErrDesc As String)
        Utils.Mail("Клиент: " & CCode & Chr(10) & Chr(13) & "Процесс: " & ErrSource & Chr(10) & Chr(13) & "Описание: " & ErrDesc, "Ошибка в сервисе подготовки данных")
    End Sub

    ' Private Sub MailUpdate(ByVal OldMDBVersion As Int32, ByVal NewMDBVersion As Int32, ByVal OldEXEVersion As Int32, ByVal NewEXEVersion As Int32)
    '        Cm.CommandText = " insert into logs.programmupgrade values(null, now(), " & CCode & ", " & OldMDBVersion & ", " & NewMDBVersion & ", " & OldEXEVersion & ", " & NewEXEVersion & "); SELECT email," & _
    '" ShortName FROM clientsdata, accessright.regionaladmins" & _
    '" where SendAlert=1 and RegionCode & regionaladmins.regionmask>0 and firmcode=" & CCode
    '        SQLdr = Cm.ExecuteReader
    '        While SQLdr.Read
    '            Mail("service@analit.net", "Обновление программы - " & SQLdr.Item(1), MailFormat.Text, "Код клиента: ", SQLdr.Item(0), System.Text.Encoding.UTF8)
    '        End While
    '        SQLdr.Close()

    '    End Sub

    Private Function FnCheckID() As Boolean
        '#If DEBUG Then
        '        Return True
        '#Else

        Cm.Transaction = myTrans
RePost:

        Cm.CommandText = "select AFCopyId from UserUpdateInfo where UserId=" & UserId
        'Cm.Parameters.Add(New MySqlParameter("?ClientCode", MySqlDbType.Int32))
        'Cm.Parameters("?ClientCode").Value = CCode
        Cm.Transaction = myTrans

        myTrans = ReadOnlyCn.BeginTransaction(IsoLevel)
        ' Cm.Parameters.
        Using SQLdr As MySqlDataReader = Cm.ExecuteReader
            SQLdr.Read()
            UniqueCID = SQLdr.GetString(0)
            SQLdr.Close()
        End Using


        If UniqueCID.Length < 1 Then
            Try
                Cm.CommandText = "update  UserUpdateInfo set AFCopyId='" & UID & "' where UserId=" & UserId

                Cm.ExecuteNonQuery()
                FnCheckID = True
            Catch ex As MySqlException
                If Not (ReadOnlyCn.State = ConnectionState.Closed Or ReadOnlyCn.State = ConnectionState.Broken) Then myTrans.Rollback()
                If ex.Number = 1213 Or ex.Number = 1205 Then
                    System.Threading.Thread.Sleep(500)
                    GoTo RePost
                End If
            End Try
        Else

            If UniqueCID <> UID Then
                'MailErr("Несоответствие UIN", UniqueCID & " " & UID)
                FnCheckID = False
            Else
                FnCheckID = True
            End If

        End If

        myTrans.Commit()
        '#End If
    End Function

    Private Sub ProtocolUpdates()
        Dim LogTrans As MySqlTransaction
        Dim LogCn As New MySqlConnection
        Dim LogCb As New MySqlCommandBuilder
        Dim LogDA As New MySqlDataAdapter
        Dim NoNeedProcessDocuments As Boolean = False


        LogCn.ConnectionString = Settings.ConnectionString
        Try
            LogCn.Open()

            LogCm.Connection = LogCn
            LogCb.DataAdapter = DA

            If (BaseThread IsNot Nothing) AndAlso BaseThread.IsAlive Then BaseThread.Join()
            If ThreadZipStream.IsAlive Then ThreadZipStream.Join()

            If UserId < 1 Then
                GetClientCode()
                NoNeedProcessDocuments = True
            End If

            Try
                If UpdateType = 1 _
                Or UpdateType = 2 _
                Or UpdateType = 5 _
                Or UpdateType = 6 _
                Or UpdateType = 8 Then

                    LogTrans = LogCn.BeginTransaction(IsoLevel)

                    If CurUpdTime < Now().AddDays(-1) Then CurUpdTime = Now()

                    With LogCm

                        .CommandText = "insert into `logs`.`AnalitFUpdates`(`RequestTime`, `UpdateType`, `UserId`, `AppVersion`,  `ResultSize`, `Addition`) values(?UpdateTime, ?UpdateType, ?UserId, ?exeversion,  ?Size, ?Addition); "
                        .CommandText &= "select last_insert_id()"


                        .Transaction = LogTrans
                        .Parameters.Add(New MySqlParameter("?UserId", UserId))

                        .Parameters.Add(New MySqlParameter("?ClientHost", UserHost))

                        .Parameters.Add(New MySqlParameter("?UpdateType", UpdateType))

                        .Parameters.Add(New MySqlParameter("?EXEVersion", BuildNo))

                        .Parameters.Add(New MySqlParameter("?Size", ResultLenght))

                        .Parameters.Add(New MySqlParameter("?Addition", Addition))

                        .Parameters.Add(New MySqlParameter("?UpdateTime", CurUpdTime))

                    End With

                    'Exit Sub




PostLog:

                    GUpdateId = Convert.ToUInt32(LogCm.ExecuteScalar)


                    LogTrans.Commit()

                    If DS.Tables("ProcessingDocuments").Rows.Count > 0 Then
                        Dim DocumentsIdRow As DataRow
                        Dim DocumentsProcessingCommandBuilder As New MySqlCommandBuilder

                        For Each DocumentsIdRow In DS.Tables("ProcessingDocuments").Rows
                            DocumentsIdRow.Item("UpdateId") = GUpdateId
                        Next

                        LogDA.SelectCommand = New MySqlCommand
                        LogDA.SelectCommand.Connection = LogCn
                        LogDA.SelectCommand.CommandText = "" & _
                             "SELECT  * " & _
                             "FROM    AnalitFDocumentsProcessing limit 0"

                        DocumentsProcessingCommandBuilder.DataAdapter = LogDA
                        LogDA.InsertCommand = DocumentsProcessingCommandBuilder.GetInsertCommand
                        LogDA.InsertCommand.Transaction = LogTrans

                        LogTrans = LogCn.BeginTransaction(IsoLevel)
                        LogDA.Update(DS.Tables("ProcessingDocuments"))
                        LogTrans.Commit()

                    End If

                    DS.Tables.Clear()

                End If
                If UpdateType = 3 Then

                    LogCm.CommandText = "" & _
                          "SELECT  MAX(UpdateId) " & _
                            "FROM    `logs`.AnalitFUpdates " & _
                            "WHERE   UpdateType IN (1, 2) " & _
                          "    AND `Commit`    =0 " & _
                          "    AND UserId  =" & UserId

                    GUpdateId = Convert.ToUInt32(LogCm.ExecuteScalar)
                    If GUpdateId < 1 Then GUpdateId = Nothing
                    'GUpdateId = UpdateId

                End If

                If Not NoNeedProcessDocuments Then

                    If UpdateType = 7 Then
                        Dim СписокФайлов() As String

                        LogTrans = LogCn.BeginTransaction(IsoLevel)
                        LogCm.CommandText = "update `logs`.`AnalitFUpdates` set Commit=1, Log=?Log, Addition=concat(Addition, ifnull(?Addition, ''))  where UpdateId=" & GUpdateId

                        LogCm.Parameters.Add(New MySqlParameter("?Log", MySqlDbType.VarString))
                        LogCm.Parameters("?Log").Value = ClientLog

                        LogCm.Parameters.Add(New MySqlParameter("?Addition", MySqlDbType.VarString))
                        LogCm.Parameters("?Addition").Value = Addition

                        LogCm.ExecuteNonQuery()

                        Dim helper = New UpdateHelper(UpdateData, ReadOnlyCn, ReadWriteCn)
                        Dim processedDocuments = helper.GetProcessedDocuments(GUpdateId)

                        If processedDocuments.Rows.Count > 0 Then
                            Dim DocumentsIdRow As DataRow

                            For Each DocumentsIdRow In processedDocuments.Rows

                                СписокФайлов = Directory.GetFiles(ПутьКДокументам & _
                                      DocumentsIdRow.Item("ClientCode").ToString & _
                                      "\" & _
                                      CType(DocumentsIdRow.Item("DocumentType"), ТипДокумента).ToString, _
                                      DocumentsIdRow.Item("DocumentId").ToString & "_*")

                                MySQLResultFile.Delete(СписокФайлов(0))

                            Next

                            LogCm.CommandText = "" & _
                                 "UPDATE AnalitFDocumentsProcessing A, " & _
                                 "        `logs`.document_logs d " & _
                                 "SET     d.UpdateId=A.UpdateId " & _
                                 "WHERE   d.RowId   =A.DocumentId " & _
                                 "    AND A.UpdateId=" & GUpdateId & _
                                 "; "

                            LogCm.CommandText &= "" & _
                                 "DELETE " & _
                                 "FROM    AnalitFDocumentsProcessing " & _
                                 "WHERE   UpdateId=" & GUpdateId

                            LogCm.ExecuteNonQuery()

                        End If

                        LogTrans.Commit()
                    End If

                End If

            Catch MySQLErr As MySqlException
                GUpdateId = Nothing
                LogTrans.Rollback()
                If MySQLErr.Number = 1213 Or MySQLErr.Number = 1205 Then
                    MailErr("Log " & CCode, "Deadlock")
                    System.Threading.Thread.Sleep(500)
                    GoTo PostLog
                End If
                Log.Error("Запись лога", MySQLErr)
            Catch err As Exception
                LogTrans.Rollback()
                Throw
            Finally
                Try
                    LogCn.Close()
                    LogCm.Dispose()
                    LogCn.Dispose()
                Catch err As Exception
                    Log.Error("Закрытие соединения Log ", err)
                End Try
            End Try
        Catch err As Exception
            GUpdateId = Nothing
            Log.Error("Запись лога", err)
        End Try
    End Sub



    Private Sub InitializeComponent()
        Me.DS = New System.Data.DataSet
        Me.DataTable1 = New System.Data.DataTable
        Me.DataColumn1 = New System.Data.DataColumn
        Me.DataColumn2 = New System.Data.DataColumn
        Me.DataColumn3 = New System.Data.DataColumn
        Me.DataTable2 = New System.Data.DataTable
        Me.DataColumn4 = New System.Data.DataColumn
        Me.DataColumn5 = New System.Data.DataColumn
        Me.OrdersL = New System.Data.DataTable
        Me.dataTable4 = New System.Data.DataTable
        Me.DataTable3 = New System.Data.DataTable
        Me.DataTable5 = New System.Data.DataTable
        Me.DataTable6 = New System.Data.DataTable
        Me.Cm = New MySql.Data.MySqlClient.MySqlCommand
        Me.ReadOnlyCn = New MySql.Data.MySqlClient.MySqlConnection
        Me.LogCm = New MySql.Data.MySqlClient.MySqlCommand
        Me.OrderInsertCm = New MySql.Data.MySqlClient.MySqlCommand
        Me.OrderInsertDA = New MySql.Data.MySqlClient.MySqlDataAdapter
        Me.SelProc = New MySql.Data.MySqlClient.MySqlCommand
        Me.ArchDA = New MySql.Data.MySqlClient.MySqlDataAdapter
        Me.ArchCmd = New MySql.Data.MySqlClient.MySqlCommand
        Me.ArchCn = New MySql.Data.MySqlClient.MySqlConnection
        Me.DA = New MySql.Data.MySqlClient.MySqlDataAdapter
        CType(Me.DS, System.ComponentModel.ISupportInitialize).BeginInit()
        CType(Me.DataTable1, System.ComponentModel.ISupportInitialize).BeginInit()
        CType(Me.DataTable2, System.ComponentModel.ISupportInitialize).BeginInit()
        CType(Me.OrdersL, System.ComponentModel.ISupportInitialize).BeginInit()
        CType(Me.dataTable4, System.ComponentModel.ISupportInitialize).BeginInit()
        CType(Me.DataTable3, System.ComponentModel.ISupportInitialize).BeginInit()
        CType(Me.DataTable5, System.ComponentModel.ISupportInitialize).BeginInit()
        CType(Me.DataTable6, System.ComponentModel.ISupportInitialize).BeginInit()
        '
        'DS
        '
        Me.DS.DataSetName = "DS"
        Me.DS.Locale = New System.Globalization.CultureInfo("ru-RU")
        Me.DS.RemotingFormat = System.Data.SerializationFormat.Binary
        Me.DS.Tables.AddRange(New System.Data.DataTable() {Me.DataTable1, Me.DataTable2, Me.OrdersL, Me.dataTable4, Me.DataTable3, Me.DataTable5, Me.DataTable6})
        '
        'DataTable1
        '
        Me.DataTable1.Columns.AddRange(New System.Data.DataColumn() {Me.DataColumn1, Me.DataColumn2, Me.DataColumn3})
        Me.DataTable1.RemotingFormat = System.Data.SerializationFormat.Binary
        Me.DataTable1.TableName = "Archiving"
        '
        'DataColumn1
        '
        Me.DataColumn1.ColumnName = "Path"
        '
        'DataColumn2
        '
        Me.DataColumn2.ColumnName = "ResultName"
        '
        'DataColumn3
        '
        Me.DataColumn3.ColumnName = "NeedDelete"
        Me.DataColumn3.DataType = GetType(Boolean)
        '
        'DataTable2
        '
        Me.DataTable2.Columns.AddRange(New System.Data.DataColumn() {Me.DataColumn4, Me.DataColumn5})
        Me.DataTable2.RemotingFormat = System.Data.SerializationFormat.Binary
        Me.DataTable2.TableName = "Logs"
        '
        'DataColumn4
        '
        Me.DataColumn4.ColumnName = "Message"
        '
        'DataColumn5
        '
        Me.DataColumn5.ColumnName = "Source"
        '
        'OrdersL
        '
        Me.OrdersL.RemotingFormat = System.Data.SerializationFormat.Binary
        Me.OrdersL.TableName = "OrdersL"
        '
        'dataTable4
        '
        Me.dataTable4.RemotingFormat = System.Data.SerializationFormat.Binary
        Me.dataTable4.TableName = "results"
        '
        'DataTable3
        '
        Me.DataTable3.RemotingFormat = System.Data.SerializationFormat.Binary
        Me.DataTable3.TableName = "OrdersDouble"
        '
        'DataTable5
        '
        Me.DataTable5.RemotingFormat = System.Data.SerializationFormat.Binary
        Me.DataTable5.TableName = "Documents"
        '
        'DataTable6
        '
        Me.DataTable6.RemotingFormat = System.Data.SerializationFormat.Binary
        Me.DataTable6.TableName = "ProcessingDocuments"
        '
        'Cm
        '
        Me.Cm.Connection = Me.ReadOnlyCn
        Me.Cm.Transaction = Nothing
        '
        'ReadOnlyCn
        '
        Me.ReadOnlyCn.ConnectionString = Nothing
        '
        'LogCm
        '
        Me.LogCm.Connection = Nothing
        Me.LogCm.Transaction = Nothing
        '
        'OrderInsertCm
        '
        Me.OrderInsertCm.Connection = Me.ReadOnlyCn
        Me.OrderInsertCm.Transaction = Nothing
        '
        'OrderInsertDA
        '
        Me.OrderInsertDA.DeleteCommand = Nothing
        Me.OrderInsertDA.InsertCommand = Me.OrderInsertCm
        Me.OrderInsertDA.SelectCommand = Me.OrderInsertCm
        Me.OrderInsertDA.UpdateCommand = Nothing
        '
        'SelProc
        '
        Me.SelProc.Connection = Me.ReadOnlyCn
        Me.SelProc.Transaction = Nothing
        '
        'ArchDA
        '
        Me.ArchDA.DeleteCommand = Nothing
        Me.ArchDA.InsertCommand = Nothing
        Me.ArchDA.SelectCommand = Me.ArchCmd
        Me.ArchDA.UpdateCommand = Me.ArchCmd
        '
        'ArchCmd
        '
        Me.ArchCmd.Connection = Me.ArchCn
        Me.ArchCmd.Transaction = Nothing
        '
        'ArchCn
        '
        Me.ArchCn.ConnectionString = Nothing
        '
        'DA
        '
        Me.DA.DeleteCommand = Nothing
        Me.DA.InsertCommand = Nothing
        Me.DA.SelectCommand = Me.Cm
        Me.DA.UpdateCommand = Nothing
        CType(Me.DS, System.ComponentModel.ISupportInitialize).EndInit()
        CType(Me.DataTable1, System.ComponentModel.ISupportInitialize).EndInit()
        CType(Me.DataTable2, System.ComponentModel.ISupportInitialize).EndInit()
        CType(Me.OrdersL, System.ComponentModel.ISupportInitialize).EndInit()
        CType(Me.dataTable4, System.ComponentModel.ISupportInitialize).EndInit()
        CType(Me.DataTable3, System.ComponentModel.ISupportInitialize).EndInit()
        CType(Me.DataTable5, System.ComponentModel.ISupportInitialize).EndInit()
        CType(Me.DataTable6, System.ComponentModel.ISupportInitialize).EndInit()

    End Sub

    Private Function CkeckZipTimeAndExist(ByVal GetEtalonData As Boolean) As Boolean

        'Todo KO
        Cm.Connection = ReadOnlyCn
        Cm.Transaction = Nothing
        Cm.CommandText = "" & _
                 "SELECT  count(UpdateId) " & _
                 "FROM    logs.AnalitFUpdates " & _
                 "WHERE   UpdateType IN (1, 2) " & _
                 "    AND Commit    =0 " & _
                 "    AND RequestTime > curdate() - interval 1 DAY " & _
                 "    AND UserId  =" & UserId

        If Convert.ToUInt32(Cm.ExecuteScalar) < 1 Then Return False


        FileInfo = New FileInfo(ResultFileName & UserId & ".zip")

        If FileInfo.Exists Then

            CkeckZipTimeAndExist = (((Date.UtcNow.Subtract(UncDT.ToUniversalTime).TotalHours < 1 And Not GetEtalonData) _
           Or (OldUpTime.Year = 2003 And DateTime.UtcNow.Subtract(UncDT.ToUniversalTime).TotalHours < 8))) Or (File.GetAttributes(ResultFileName & UserId & ".zip") = FileAttributes.Normal And GetEtalonData)

        Else

            CkeckZipTimeAndExist = False

        End If
        'CkeckZipTimeAndExist = True
    End Function

    Private Function CheckUpdateTime(ByVal AccessTime As DateTime, ByVal GetEtalonData As Boolean) As Boolean
        Dim LCheckUpdateTime As Boolean
        LCheckUpdateTime = (OldUpTime = AccessTime Or _
                GetEtalonData)

        Return LCheckUpdateTime



    End Function

    Private Function CheckUpdatePeriod() As UInt32
        Dim Min1, Min2, MinRes As UInt32
        'Min1 = CType(Math.Round(Now().Subtract(OldUpTime).TotalMinutes), UInt32)
        'Min2 = CType(Math.Round(Now().Subtract(UncDT).TotalMinutes), UInt32)


        'Cm.Transaction = Nothing

        'Cm.CommandText = "select not exists(SELECT * FROM UserUpdateInfo rui, logs.AnalitFUpdates p " & _
        '"where p.requesttime >= rui.UncommitedUpdateDate " & _
        '"and rui.UserId=p.UserId " & _
        '"and rui.UserId=" & UserId & ")"

        'If CType(Cm.ExecuteScalar, UInt16) = 1 Then

        '    MinRes = Min1
        '    If Min2 < MinRes Then MinRes = Min2

        'Else
        MinRes = 10

        'End If


        Return MinRes
    End Function


    Private Sub FirebirdProc()
        Dim SQLText As String
        Dim StartTime As DateTime = Now()
        Dim TS As TimeSpan

        Try
            Try


                Dim helper As UpdateHelper = New UpdateHelper(UpdateData, ReadOnlyCn, ReadWriteCn)


RestartTrans2:
                If ErrorFlag Then Exit Try

                MySQLFileDelete(MySqlFilePath & "Products" & UserId & ".txt")
                MySQLFileDelete(MySqlFilePath & "Catalog" & UserId & ".txt")
                MySQLFileDelete(MySqlFilePath & "CatalogCurrency" & UserId & ".txt")
                MySQLFileDelete(MySqlFilePath & "CatDel" & UserId & ".txt")
                MySQLFileDelete(MySqlFilePath & "Clients" & UserId & ".txt")
                MySQLFileDelete(MySqlFilePath & "ClientsDataN" & UserId & ".txt")
                MySQLFileDelete(MySqlFilePath & "Core" & UserId & ".txt")

                MySQLFileDelete(MySqlFilePath & "PriceAvg" & UserId & ".txt")
                MySQLFileDelete(MySqlFilePath & "PricesData" & UserId & ".txt")
                MySQLFileDelete(MySqlFilePath & "PricesRegionalData" & UserId & ".txt")
                MySQLFileDelete(MySqlFilePath & "RegionalData" & UserId & ".txt")
                MySQLFileDelete(MySqlFilePath & "Regions" & UserId & ".txt")
                MySQLFileDelete(MySqlFilePath & "Section" & UserId & ".txt")
                MySQLFileDelete(MySqlFilePath & "Synonym" & UserId & ".txt")
                MySQLFileDelete(MySqlFilePath & "SynonymFirmCr" & UserId & ".txt")
                MySQLFileDelete(MySqlFilePath & "Rejects" & UserId & ".txt")
                MySQLFileDelete(MySqlFilePath & "CatalogFarmGroups" & UserId & ".txt")
                MySQLFileDelete(MySqlFilePath & "CatalogNames" & UserId & ".txt")
                MySQLFileDelete(MySqlFilePath & "CatFarmGroupsDel" & UserId & ".txt")

                helper.MaintainReplicationInfo()

                If ThreadZipStream.IsAlive Then
                    ThreadZipStream.Abort()
                End If

                SelProc = New MySqlCommand
                SelProc.Connection = ReadOnlyCn
                SelProc.Parameters.AddWithValue("?ClientCode", CCode)
                SelProc.Parameters.AddWithValue("?Cumulative", GED)
                SelProc.Parameters.AddWithValue("?UserId", UserId)
                SelProc.Parameters.AddWithValue("?UpdateTime", OldUpTime)

                Cm = New MySqlCommand
                Cm.Connection = ReadWriteCn
                Cm.Parameters.AddWithValue("?UpdateTime", OldUpTime)

                Cm.Parameters.AddWithValue("?OfferRegionCode", UpdateData.OffersRegionCode)

                myTrans = ReadOnlyCn.BeginTransaction(IsoLevel)
                SelProc.Transaction = myTrans

                SelProc.CommandText = "drop temporary table IF EXISTS MaxCodesSynFirmCr, MinCosts, ActivePrices, Prices, Core, tmpprd, MaxCodesSyn, ParentCodes; "
                SelProc.ExecuteNonQuery()

                MySQLFileDelete(MySqlFilePath & "MinPrices" & UserId & ".txt")
                GetMySQLFile("PriceAvg", SelProc, "select ''")

                SQLText = "SELECT P.Id       ," & _
             " P.CatalogId" & _
             " FROM   Catalogs.Products P" & _
             " WHERE hidden                = 0"

                If Not GED Then
                    SQLText &= " AND P.UpdateTime >= ?UpdateTime"
                End If

                GetMySQLFile("Products", SelProc, SQLText)

                ThreadZipStream = New Thread(AddressOf ZipStream)
                ThreadZipStream.Start()

                SQLText = "SELECT C.Id             , " & _
                "       CN.Id            , " & _
                "       LEFT(CN.name, 250)  , " & _
                "       LEFT(form, 250)  , " & _
                "       vitallyimportant , " & _
                "       needcold         , " & _
                "       fragile " & _
                "FROM   Catalogs.Catalog C       , " & _
                "       Catalogs.CatalogForms CF , " & _
                "       Catalogs.CatalogNames CN " & _
                "WHERE  C.NameId                        =CN.Id " & _
                "   AND C.FormId                        =CF.Id " & _
                "   AND hidden    =0"

                If Not GED Then
                    SQLText &= "   AND C.UpdateTime >= ?UpdateTime "
                End If

                GetMySQLFile("Catalog", SelProc, SQLText)

                GetMySQLFile("CatDel", SelProc, _
                " SELECT C.Id " & _
                " FROM   Catalogs.Catalog C " & _
                " WHERE  C.UpdateTime > ?UpdateTime " & _
                "   AND hidden        = 1 " & _
                "   AND NOT ?Cumulative")

                SelProc.Parameters.AddWithValue("?OffersClientCode", UpdateData.OffersClientCode)
                SelProc.Parameters.AddWithValue("?OffersRegionCode", UpdateData.OffersRegionCode)
                SelProc.Parameters.AddWithValue("?ShowAvgCosts", UpdateData.ShowAvgCosts)

                GetMySQLFile("Regions", SelProc, helper.GetRegionsCommand())

                helper.SelectPrices()

                If UpdateData.ShowJunkOffers Then

                    SelProc.CommandText = "" & _
                    "      CREATE TEMPORARY TABLE PricesTMP " & _
                    "      SELECT * " & _
                    "      FROM   Prices " & _
                    "      WHERE  PriceCode=2647; " & _
                    "       " & _
                    "      CALL GetPrices2(?OffersClientCode); " & _
                    "      INSERT " & _
                    "      INTO   Prices " & _
                    "      SELECT * " & _
                    "      FROM   PricesTMP; " & _
                    "       " & _
                    "      DROP TEMPORARY TABLE PricesTMP;"

                    SelProc.ExecuteNonQuery()

                End If

                GetMySQLFile("ClientsDataN", SelProc, _
                "SELECT firm.FirmCode, " & _
                "       firm.FullName, " & _
                "       firm.Fax     , " & _
                "       '-'          , " & _
                "       '-'          , " & _
                "       '-'          , " & _
                "       '-'          , " & _
                "       '-'          , " & _
                "       '-'          , " & _
                "       '-'          , " & _
                "       '-'          , " & _
                "       '-'          , " & _
                "       '-'          , " & _
                "       '-'          , " & _
                "       '-' " & _
                "FROM   clientsdata AS firm " & _
                "WHERE  firmcode IN " & _
                "                   (SELECT DISTINCT FirmCode " & _
                "                   FROM             Prices " & _
                "                   )")

                GetMySQLFile("RegionalData", SelProc, _
                "SELECT DISTINCT regionaldata.FirmCode  , " & _
                "                regionaldata.RegionCode, " & _
                "                supportphone           , " & _
                "                LEFT(adminmail, 50)    , " & _
                "                ContactInfo            , " & _
                "                OperativeInfo " & _
                "FROM            regionaldata, " & _
                "                Prices " & _
                "WHERE           regionaldata.firmcode  = Prices.firmcode " & _
                "            AND regionaldata.regioncode= Prices.regioncode")


                GetMySQLFile("PricesRegionalData", SelProc, _
                "SELECT PriceCode           , " & _
                "       RegionCode          , " & _
                "       STORAGE             , " & _
                "       PublicUpCost        , " & _
                "       MinReq              , " & _
                "       MainFirm            , " & _
                "       NOT disabledbyclient, " & _
                "       CostCorrByClient    , " & _
                "       ControlMinReq " & _
                "FROM   Prices")

                SelProc.CommandText = "" & _
                "CREATE TEMPORARY TABLE tmpprd ( FirmCode INT unsigned, PriceCount MediumINT unsigned )engine=MEMORY; " & _
                "INSERT " & _
                "INTO   tmpprd " & _
                "SELECT   firmcode, " & _
                "         COUNT(pricecode) " & _
                "FROM     Prices " & _
                "GROUP BY FirmCode, " & _
                "         RegionCode;"

                SelProc.ExecuteNonQuery()


                GetMySQLFile("PricesData", SelProc, _
                  "SELECT   Prices.FirmCode , " & _
                  "         Prices.pricecode, " & _
                  "                  concat(firm.shortname, IF(PriceCount> 1 " & _
                  "      OR ShowPriceName                                = 1, concat(' (', pricename, ')'), '')), " & _
                  "          0                                                                  , " & _
                  "         ''                                                                                  , " & _
                  "        date_sub(PriceDate, interval time_to_sec(date_sub(now(), interval unix_timestamp() second)) second)  , " & _
                  "         if(?OffersClientCode is null, ((ForceReplication != 0) " & _
                  "          OR (actual = 0) or ?Cumulative), 1)  , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         ''          , " & _
                  "         '0' " & _
                  "FROM     clientsdata AS firm, " & _
                  "         tmpprd             , " & _
                  "         Prices, " & _
                  "         AnalitFReplicationInfo ARI " & _
                  "WHERE    tmpprd.firmcode = firm.firmcode " & _
                  "     AND firm.firmcode   = Prices.FirmCode " & _
                  "     AND ARI.FirmCode    = Prices.FirmCode " & _
                  "     AND ARI.UserId    = ?UserId " & _
                  "GROUP BY Prices.FirmCode, " & _
                  "         Prices.pricecode")

                SQLText = "SELECT FirmCr        , " & _
                  "       CountryCr     , " & _
                  "       FullName      , " & _
                  "       Series        , " & _
                  "       LetterNo      , " & _
                  "       LetterDate    , " & _
                  "       LaboratoryName, " & _
                  "       CauseRejects " & _
                  "FROM   addition.rejects, " & _
                  "       retclientsset rcs " & _
                  "WHERE rcs.clientcode = ?ClientCode" & _
                  "   AND alowrejection  = 1 "

                If Not GED Then
                    SQLText &= "   AND accessTime     > ?UpdateTime"
                End If

                GetMySQLFile("Rejects", SelProc, SQLText)
                GetMySQLFile("Clients", SelProc, helper.GetClientsCommand(True))

                helper.SelectActivePrices()

                SelProc.CommandText = "" & _
                "CREATE TEMPORARY TABLE ParentCodes ENGINE=memory " & _
                "SELECT   PriceSynonymCode PriceCode, " & _
                "         MaxSynonymCode                 , " & _
                "         MaxSynonymFirmCrCode " & _
                "FROM     ActivePrices Prices        " & _
                "GROUP BY 1; "

                SelProc.ExecuteNonQuery()

                SQLText = "" & _
                "SELECT synonymfirmcr.synonymfirmcrcode, " & _
                "       LEFT(SYNONYM, 250) " & _
                "FROM   farm.synonymfirmcr, " & _
                "       ParentCodes " & _
                "WHERE  synonymfirmcr.pricecode        = ParentCodes.PriceCode "
                If Not GED Then

                    SQLText &= "AND synonymfirmcr.synonymfirmcrcode > MaxSynonymFirmCrCode "

                End If

                SQLText &= " " & _
               "UNION " & _
               " " & _
               "SELECT 1, " & _
               "       '-' "

                GetMySQLFile("SynonymFirmCr", SelProc, SQLText)

                SQLText = "" & _
                "SELECT synonym.synonymcode, " & _
                "       LEFT(synonym.synonym, 250) " & _
                "FROM   farm.synonym, " & _
                "       ParentCodes " & _
                "WHERE  synonym.pricecode  = ParentCodes.PriceCode "

                If Not GED Then
                    SQLText &= "AND synonym.synonymcode > MaxSynonymCode"
                End If

                GetMySQLFile("Synonym", SelProc, SQLText)

                GetMySQLFile("CatalogCurrency", SelProc, _
                "SELECT currency, " & _
                "       exchange " & _
                "FROM   farm.catalogcurrency " & _
                "WHERE  currency='$' " & _
                "    OR currency='Eu'")


                If UpdateData.OffersClientCode Is Nothing Then

                    SelProc.CommandText = "" & _
                    "SELECT IFNULL(SUM(fresh), 0) " & _
                    "FROM   ActivePrices"
                    If CType(SelProc.ExecuteScalar, Integer) > 0 Or GED Then
                        helper.SelectOffers()
                        CostOptimizer.OptimizeCostIfNeeded(ReadOnlyCn, ReadWriteCn, CCode)

                        SelProc.CommandText = "" & _
                        "UPDATE ActivePrices Prices, " & _
                        "       Core " & _
                        "SET    CryptCost       = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(AES_ENCRYPT(Cost, (SELECT BaseCostPassword FROM   retclientsset WHERE  clientcode=?ClientCode)), CHAR(37), '%25'), CHAR(32), '%20'), CHAR(159), '%9F'), CHAR(161), '%A1'), CHAR(0), '%00') " & _
                        "WHERE  Prices.PriceCode= Core.PriceCode " & _
                        "   AND IF(?Cumulative, 1, Fresh) " & _
                        "   AND Core.PriceCode!=2647 ; " & _
                        " " & _
                        "UPDATE Core " & _
                        "SET    CryptCost        =concat(LEFT(CryptCost, 1), CHAR(ROUND((rand()*110)+32,0)), SUBSTRING(CryptCost,2,LENGTH(CryptCost)-4), CHAR(ROUND((rand()*110)+32,0)), RIGHT(CryptCost, 3)) " & _
                        "WHERE  LENGTH(CryptCost)>0 " & _
                        "   AND Core.PriceCode!=2647;"
                        SelProc.ExecuteNonQuery()


                        GetMySQLFile("MinPrices", SelProc, _
                         "SELECT RIGHT(MinCosts.ID, 9), " & _
                         "       MinCosts.ProductId   , " & _
                         "       MinCosts.RegionCode  , " & _
                         "       IF(PriceCode=2647, '', (99999900 ^ TRUNCATE((MinCost*100), 0))) " & _
                         "FROM   MinCosts")

                        GetMySQLFile("Core", SelProc, _
                        "SELECT CT.PriceCode                      , " & _
                        "       CT.regioncode                     , " & _
                        "       CT.ProductId                      , " & _
                        "       ifnull(Core.codefirmcr, '')       , " & _
                        "       Core.synonymcode                  , " & _
                        "       if(ifnull(Core.SynonymFirmCrCode, 0)<1, 1, Core.SynonymFirmCrCode), " & _
                        "       Core.Code                         , " & _
                        "       Core.CodeCr                       , " & _
                        "       Core.unit                         , " & _
                        "       Core.volume                       , " & _
                        "       Core.Junk                         , " & _
                        "       Core.Await                        , " & _
                        "       Core.quantity                     , " & _
                        "       Core.note                         , " & _
                        "       Core.period                       , " & _
                        "       Core.doc                          , " & _
                        "       ifnull(Core.RegistryCost, '')     , " & _
                        "       Core.VitallyImportant             , " & _
                        "       ifnull(Core.RequestRatio, '')     , " & _
                        "       CT.CryptCost                      , " & _
                        "       RIGHT(CT.ID, 9)                   , " & _
                        "       ifnull(OrderCost, '')             , " & _
                        "       ifnull(MinOrderCount, '') " & _
                        "FROM   Core CT        , " & _
                        "       ActivePrices AT, " & _
                        "       farm.core0 Core " & _
                        "WHERE  ct.pricecode =at.pricecode " & _
                        "   AND ct.regioncode=at.regioncode " & _
                        "   AND Core.id      =CT.id " & _
                        "   AND IF(?Cumulative, 1, fresh)")
                    Else
                        SelProc.CommandText = "SELECT ''" & _
                        " INTO OUTFILE 'C:/AFFiles/Core" & UserId & ".txt' FIELDS TERMINATED BY '" & Chr(159) & "' OPTIONALLY ENCLOSED BY '' ESCAPED BY '' LINES TERMINATED BY ''"
                        SelProc.ExecuteNonQuery()


                        SelProc.CommandText = "SELECT ''" & _
                          " INTO OUTFILE 'C:/AFFiles/MinPrices" & UserId & ".txt' FIELDS TERMINATED BY '" & Chr(159) & "' OPTIONALLY ENCLOSED BY '' ESCAPED BY '' LINES TERMINATED BY ''"
                        SelProc.ExecuteNonQuery()



                        SyncLock (FilesForArchive)

                            FilesForArchive.Enqueue(New FileForArchive("Core", False))
                            FilesForArchive.Enqueue(New FileForArchive("MinPrices", False))

                        End SyncLock


                    End If

                Else

                    SelProc.CommandText = "" & _
                     "DROP TEMPORARY TABLE IF EXISTS  ActivePrices; " & _
                     "CREATE TEMPORARY TABLE ActivePrices engine=MEMORY " & _
                     "SELECT pricesdata.firmcode                            , " & _
                     "       i.pricecode                                    , " & _
                     "       i.costcode                                     , " & _
                     "       i.RegionCode                                   , " & _
                     "       1 UpCost, " & _
                     "       pricesdata.CostType " & _
                     "FROM   usersettings.intersection i " & _
                     "       JOIN usersettings.pricesdata " & _
                     "       ON     pricesdata.pricecode = i.pricecode " & _
                     "       JOIN usersettings.PricesCosts pc " & _
                     "       ON     pc.CostCode = i.CostCode " & _
                     "       JOIN usersettings.PriceItems pi " & _
                     "       ON     pi.Id = pc.PriceItemId " & _
                     "       JOIN farm.formrules f " & _
                     "       ON     f.Id = pi.FormRuleId " & _
                     "       JOIN usersettings.clientsdata " & _
                     "       ON     clientsdata.firmcode = pricesdata.firmcode " & _
                     "       JOIN usersettings.pricesregionaldata " & _
                     "       ON     pricesregionaldata.regioncode = i.regioncode " & _
                     "          AND pricesregionaldata.pricecode  = pricesdata.pricecode " & _
                     "       JOIN usersettings.RegionalData rd " & _
                     "       ON     rd.RegionCode = i.regioncode " & _
                     "          AND rd.FirmCode   = pricesdata.firmcode " & _
                     "       JOIN usersettings.clientsdata AS AClientsData " & _
                     "       ON     AClientsData.firmcode   = i.clientcode " & _
                     "          AND clientsdata.firmsegment = AClientsData.firmsegment " & _
                     "       JOIN usersettings.retclientsset r " & _
                     "       ON     r.clientcode    = AClientsData.FirmCode " & _
                     "WHERE  i.DisabledByAgency     = 0 " & _
                     "   AND clientsdata.firmstatus = 1 " & _
                     "   AND clientsdata.firmtype   = 0 " & _
                     "   AND ( " & _
                     "              clientsdata.maskregion & i.regioncode " & _
                     "       ) " & _
                     "       > 0 " & _
                     "   AND ( " & _
                     "              AClientsData.maskregion & i.regioncode " & _
                     "       ) " & _
                     "                                                    > 0 " & _
                     "   AND pricesdata.agencyenabled                     = 1 " & _
                     "   AND pricesdata.enabled                           = 1 " & _
                     "   AND pricesdata.pricetype                        <> 1 " & _
                     "   AND pricesregionaldata.enabled                   = 1 " & _
                     "   AND clientsdata.FirmCode!=234                        " & _
                     "   AND to_days(Now()) - to_days(pi.PriceDate) < f.maxold " & _
                     "   AND i.DisabledByClient=0 " & _
                     "   AND i.InvisibleOnClient=0 " & _
                     "   AND i.DisabledByFirm=0 " & _
                     "   AND i.clientcode                                 = ?OffersClientCode;"




                    helper.SelectOffers()

                    SelProc.CommandText &= "" & _
                     "DROP TEMPORARY TABLE " & _
                     "IF EXISTS CoreT, CoreTP , CoreT2; " & _
                     "        CREATE TEMPORARY TABLE CoreT(ProductId                  INT unsigned, CodeFirmCr INT unsigned, Cost DECIMAL(8,2), CryptCost VARCHAR(32),UNIQUE MultiK(ProductId, CodeFirmCr))engine=MEMORY; " & _
                     "                CREATE TEMPORARY TABLE CoreT2(ProductId         INT unsigned, CodeFirmCr INT unsigned, Cost DECIMAL(8,2), CryptCost VARCHAR(32),UNIQUE MultiK(ProductId, CodeFirmCr))engine=MEMORY; " & _
                     "                        CREATE TEMPORARY TABLE CoreTP(ProductId INT unsigned, Cost DECIMAL(8,2), CryptCost VARCHAR(32), UNIQUE MultiK(ProductId))engine                                    =MEMORY; " & _
                     "                                INSERT " & _
                     "                                INTO   CoreT " & _
                     "                                       ( " & _
                     "                                              ProductId , " & _
                     "                                              CodeFirmCr, " & _
                     "                                              Cost " & _
                     "                                       ) " & _
                     "                                SELECT   core0.ProductId , " & _
                     "                                         core0.codefirmcr, " & _
                     "                                         ROUND(AVG(cost), 2) " & _
                     "                                FROM     farm.core0, " & _
                     "                                         Core " & _
                     "                                WHERE    core0.id=Core.id " & _
                     "                                GROUP BY ProductId, " & _
                     "                                         CodeFirmCr; " & _
                     "                                 " & _
                     "                                UPDATE CoreT " & _
                     "                                SET    CryptCost = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(AES_ENCRYPT(Cost, (SELECT BaseCostPassword FROM   retclientsset WHERE  clientcode=?ClientCode)), CHAR(37), '%25'), CHAR(32), '%20'), CHAR(159), '%9F'), CHAR(161), '%A1'), CHAR(0), '%00'); " & _
                     "                                 " & _
                     "                                UPDATE CoreT " & _
                     "                                SET    CryptCost=concat(LEFT(CryptCost, 1), CHAR(ROUND((rand()*110)+32,0)), SUBSTRING(CryptCost,2,LENGTH(CryptCost)-4), CHAR(ROUND((rand()*110)+32,0)), RIGHT(CryptCost, 3)); " & _
                     "                                 " & _
                     "                                INSERT " & _
                     "                                INTO   CoreTP " & _
                     "                                       ( " & _
                     "                                              ProductId, " & _
                     "                                              Cost " & _
                     "                                       ) " & _
                     "                                SELECT   ProductId, " & _
                     "                                         ROUND(AVG(cost), 2) " & _
                     "                                FROM     CoreT " & _
                     "                                GROUP BY ProductId; " & _
                     "                                 " & _
                     "                                UPDATE CoreTP " & _
                     "                                SET    CryptCost = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(AES_ENCRYPT(Cost, (SELECT BaseCostPassword FROM   retclientsset WHERE  clientcode=?ClientCode)), CHAR(37), '%25'), CHAR(32), '%20'), CHAR(159), '%9F'), CHAR(161), '%A1'), CHAR(0), '%00'); " & _
                     "                                 " & _
                     "                                UPDATE CoreTP " & _
                     "                                SET    CryptCost=concat(LEFT(CryptCost, 1), CHAR(ROUND((rand()*110)+32,0)), SUBSTRING(CryptCost,2,LENGTH(CryptCost)-4), CHAR(ROUND((rand()*110)+32,0)), RIGHT(CryptCost, 3)); " & _
                     "                                 " & _
                     "                                INSERT " & _
                     "                                INTO   CoreT2 " & _
                     "                                SELECT * " & _
                     "                                FROM   CoreT; " & _
                     "SET @RowId :=1;"
                    SelProc.ExecuteNonQuery()

                    'Err.Raise(1, "Технический запрет обновления")

                    GetMySQLFile("Core", SelProc, "" & _
                     "SELECT 2647                             , " & _
                     "       ?OffersRegionCode                , " & _
                     "       A.ProductId                      , " & _
                     "       A.CodeFirmCr                     , " & _
                     "       S.SynonymCode                    , " & _
                     "       SF.SynonymFirmCrCode             , " & _
                     "       ''                               , " & _
                     "       ''                               , " & _
                     "       ''                               , " & _
                     "       ''                               , " & _
                     "       0                                , " & _
                     "       0                                , " & _
                     "       ''                               , " & _
                     "       ''                               , " & _
                     "       ''                               , " & _
                     "       ''                               , " & _
                     "       ''                               , " & _
                     "       0                                , " & _
                     "       ''                               , " & _
                     "       IF(?ShowAvgCosts, CryptCost, '') , " & _
                     "       @RowId := @RowId + 1             , " & _
                     "       ''                               , " & _
                     "       ''                                 " & _
                     "FROM   farm.Synonym S                   , " & _
                     "       farm.SynonymFirmCr SF            , " & _
                     "       CoreT A " & _
                     "WHERE  S.PriceCode   =2647 " & _
                     "   AND SF.PriceCode  =2647 " & _
                     "   AND S.ProductId   =A.ProductId " & _
                     "   AND SF.CodeFirmCr =A.CodeFirmCr " & _
                     "   AND A.CodeFirmCr IS NOT NULL " & _
                     " " & _
                     "UNION " & _
                     " " & _
                     "SELECT 2647                              , " & _
                     "       ?OffersRegionCode                 , " & _
                     "       A.ProductId                       , " & _
                     "       ''                                , " & _
                     "       S.SynonymCode                     , " & _
                     "       1                                 , " & _
                     "       ''                                , " & _
                     "       ''                                , " & _
                     "       ''                                , " & _
                     "       ''                                , " & _
                     "       0                                 , " & _
                     "       0                                 , " & _
                     "       ''                                , " & _
                     "       ''                                , " & _
                     "       ''                                , " & _
                     "       ''                                , " & _
                     "       ''                                , " & _
                     "       0                                 , " & _
                     "       ''                                , " & _
                     "       IF(?ShowAvgCosts, A.CryptCost, ''), " & _
                     "       @RowId := @RowId + 1              , " & _
                     "       ''                                , " & _
                     "       ''                                  " & _
                     "FROM   farm.Synonym S                    , " & _
                     "       CoreTP A " & _
                     "WHERE  S.PriceCode =2647 " & _
                     "   AND S.ProductId =A.ProductId")


                    GetMySQLFile("MinPrices", SelProc, "SELECT 0        , " & _
                     "       ProductId, " & _
                     "       ?OffersRegionCode         , " & _
                     "       ''        " & _
                     "FROM   CoreTP")




                End If


                SelProc.CommandText = "drop temporary table IF EXISTS MaxCodesSynFirmCr, MinCosts, ActivePrices, Prices, Core, tmpprd, MaxCodesSyn, ParentCodes; "
                SelProc.ExecuteNonQuery()

                myTrans.Commit()

                helper.UpdateReplicationInfo()

                TS = Now().Subtract(StartTime)

                If Math.Round(TS.TotalSeconds, 0) > 30 Then

                    Addition &= "Sel: " & Math.Round(TS.TotalSeconds, 0) & "; "

                End If




            Catch ex As ThreadAbortException

                'ErrorFlag = True

                ' If Not (ReadOnlyCn.State = ConnectionState.Closed Or ReadOnlyCn.State = ConnectionState.Broken) Then myTrans.Rollback()

            Catch MySQLErr As MySqlException

                If Not (ReadOnlyCn.State = ConnectionState.Closed Or ReadOnlyCn.State = ConnectionState.Broken) Then myTrans.Rollback()


                If ThreadZipStream.IsAlive Then ThreadZipStream.Abort()
                ThreadZipStream.Join()


                If MySQLErr.Number = 1213 Or MySQLErr.Number = 1205 Or MySQLErr.Number = 1086 Then
                    System.Threading.Thread.Sleep(2500)
                    GoTo RestartTrans2
                End If

                MailErr("Основной поток выборки: " & MySQLErr.Message, SelProc.CommandText & MySQLErr.StackTrace)
                ErrorFlag = True
                UpdateType = 6
                'NeedCloseCn = True
                Addition &= MySQLErr.Message


            Catch ErrorTXT As Exception

#If DEBUG Then

                'If ErrorTXT.Message = "Определенная приложением или объектом ошибка." Then

                '    If Not (ReadOnlyCn.State = ConnectionState.Closed Or ReadOnlyCn.State = ConnectionState.Broken) Then myTrans.Rollback()

                '    If ThreadZipStream.IsAlive Then ThreadZipStream.Abort()
                '    ThreadZipStream.Join()

                '    System.Threading.Thread.Sleep(2500)

                '    GoTo RestartTrans2

                'End If
#End If


                ErrorFlag = True
                'NeedCloseCn = True
                UpdateType = 6
                Addition &= ErrorTXT.Message
                MailErr("Основной поток выборки, клиент: " & CCode, ErrorTXT.Message & ErrorTXT.StackTrace)
                If ThreadZipStream.IsAlive Then ThreadZipStream.Abort()
                'If Not (ReadOnlyCn.State = ConnectionState.Closed Or ReadOnlyCn.State = ConnectionState.Broken) Then myTrans.Rollback()
            End Try
        Catch err As Exception
            MailErr("Основной поток выборки, general " & CCode, err.Message & ": " & err.StackTrace)
            ErrorFlag = True
        End Try
    End Sub


    Private Sub MySqlProc()
        Dim SQLText As String
        Dim StartTime As DateTime = Now()
        Dim TS As TimeSpan

        Try
            Try


                Dim helper As UpdateHelper = New UpdateHelper(UpdateData, ReadOnlyCn, ReadWriteCn)

RestartTrans2:
                If ErrorFlag Then Exit Try

                MySQLFileDelete(MySqlFilePath & "Products" & UserId & ".txt")
                MySQLFileDelete(MySqlFilePath & "User" & UserId & ".txt")
                MySQLFileDelete(MySqlFilePath & "Client" & UserId & ".txt")
                MySQLFileDelete(MySqlFilePath & "Catalogs" & UserId & ".txt")
                MySQLFileDelete(MySqlFilePath & "CatDel" & UserId & ".txt")
                MySQLFileDelete(MySqlFilePath & "Clients" & UserId & ".txt")
                MySQLFileDelete(MySqlFilePath & "DelayOfPayments" & UserId & ".txt")
                MySQLFileDelete(MySqlFilePath & "Providers" & UserId & ".txt")
                MySQLFileDelete(MySqlFilePath & "Core" & UserId & ".txt")
                MySQLFileDelete(MySqlFilePath & "PricesData" & UserId & ".txt")
                MySQLFileDelete(MySqlFilePath & "PricesRegionalData" & UserId & ".txt")
                MySQLFileDelete(MySqlFilePath & "RegionalData" & UserId & ".txt")
                MySQLFileDelete(MySqlFilePath & "Regions" & UserId & ".txt")
                MySQLFileDelete(MySqlFilePath & "Synonyms" & UserId & ".txt")
                MySQLFileDelete(MySqlFilePath & "SynonymFirmCr" & UserId & ".txt")
                MySQLFileDelete(MySqlFilePath & "Rejects" & UserId & ".txt")
                MySQLFileDelete(MySqlFilePath & "CatalogNames" & UserId & ".txt")
                MySQLFileDelete(MySqlFilePath & "MNN" & UserId & ".txt")
                MySQLFileDelete(MySqlFilePath & "Descriptions" & UserId & ".txt")

                helper.MaintainReplicationInfo()

                If ThreadZipStream.IsAlive Then
                    ThreadZipStream.Abort()
                End If

                SelProc = New MySqlCommand
                SelProc.Connection = ReadOnlyCn
                SelProc.Parameters.AddWithValue("?ClientCode", CCode)
                SelProc.Parameters.AddWithValue("?Cumulative", GED)
                SelProc.Parameters.AddWithValue("?UserId", UserId)
                SelProc.Parameters.AddWithValue("?UpdateTime", OldUpTime)



                myTrans = ReadOnlyCn.BeginTransaction(IsoLevel)
                SelProc.Transaction = myTrans

                SelProc.CommandText = "drop temporary table IF EXISTS MaxCodesSynFirmCr, MinCosts, ActivePrices, Prices, Core, tmpprd, MaxCodesSyn, ParentCodes; "
                SelProc.ExecuteNonQuery()



                GetMySQLFileWithDefault("User", SelProc, helper.GetUserCommand())
                GetMySQLFileWithDefault("Client", SelProc, helper.GetClientCommand())

                GetMySQLFileWithDefault("Products", SelProc, _
                 "SELECT P.Id       ," & _
                " P.CatalogId" & _
                " FROM   Catalogs.Products P" & _
                " WHERE(If(Not ?Cumulative, (P.UpdateTime > ?UpdateTime), 1))" & _
                " AND hidden                                = 0")



                ThreadZipStream = New Thread(AddressOf ZipStream)
                ThreadZipStream.Start()

                If (BuildNo > 945) Or (UpdateData.EnableUpdate And ((BuildNo = 945) Or ((BuildNo >= 705) And (BuildNo <= 716)) Or ((BuildNo >= 829) And (BuildNo <= 837)))) _
                Then
                    GetMySQLFileWithDefaultEx("Catalogs", SelProc, _
                    "SELECT C.Id             , " & _
                    "       CN.Id            , " & _
                    "       LEFT(CN.name, 250)  , " & _
                    "       LEFT(CF.form, 250)  , " & _
                    "       C.vitallyimportant , " & _
                    "       C.needcold         , " & _
                    "       C.fragile, " & _
                    "       C.MandatoryList , " & _
                    "       CN.MnnId, " & _
                    "       CN.DescriptionId " & _
                    "FROM   Catalogs.Catalog C       , " & _
                    "       Catalogs.CatalogForms CF , " & _
                    "       Catalogs.CatalogNames CN " & _
                    "WHERE  C.NameId                        =CN.Id " & _
                    "   AND C.FormId                        =CF.Id " & _
                    "   AND (IF(NOT ?Cumulative, C.UpdateTime > ?UpdateTime, 1) or IF(NOT ?Cumulative, CN.UpdateTime > ?UpdateTime, 1)) " & _
                    "   AND C.hidden                          =0", _
                    ((BuildNo = 945) Or ((BuildNo >= 829) And (BuildNo <= 837))) And UpdateData.EnableUpdate)

                    GetMySQLFileWithDefaultEx( _
                        "MNN", _
                        SelProc, _
                        helper.GetMNNCommand(), _
                        ((BuildNo = 945) Or ((BuildNo >= 829) And (BuildNo <= 837))) And UpdateData.EnableUpdate)

                    GetMySQLFileWithDefaultEx( _
                    "Descriptions", _
                    SelProc, _
                    helper.GetDescriptionCommand(), _
                    ((BuildNo = 945) Or ((BuildNo >= 829) And (BuildNo <= 837))) And UpdateData.EnableUpdate)
                Else
                    GetMySQLFileWithDefault("Catalogs", SelProc, _
                    "SELECT C.Id             , " & _
                    "       CN.Id            , " & _
                    "       LEFT(CN.name, 250)  , " & _
                    "       LEFT(CF.form, 250)  , " & _
                    "       C.vitallyimportant , " & _
                    "       C.needcold         , " & _
                    "       C.fragile " & _
                    "FROM   Catalogs.Catalog C       , " & _
                    "       Catalogs.CatalogForms CF , " & _
                    "       Catalogs.CatalogNames CN " & _
                    "WHERE  C.NameId                        =CN.Id " & _
                    "   AND C.FormId                        =CF.Id " & _
                    "   AND (IF(NOT ?Cumulative, C.UpdateTime > ?UpdateTime, 1) or IF(NOT ?Cumulative, CN.UpdateTime > ?UpdateTime, 1)) " & _
                    "   AND C.hidden                          =0")
                End If


                GetMySQLFileWithDefault("CatDel", SelProc, _
                " SELECT C.Id " & _
                " FROM   Catalogs.Catalog C " & _
                " WHERE  C.UpdateTime > ?UpdateTime " & _
                "   AND hidden        = 1 " & _
                "   AND NOT ?Cumulative")

                SelProc.Parameters.AddWithValue("?OffersClientCode", UpdateData.OffersClientCode)
                SelProc.Parameters.AddWithValue("?OffersRegionCode", UpdateData.OffersRegionCode)
                SelProc.Parameters.AddWithValue("?ShowAvgCosts", UpdateData.ShowAvgCosts)

                GetMySQLFileWithDefault("Regions", SelProc, helper.GetRegionsCommand())

                helper.SelectPrices()

                If UpdateData.ShowJunkOffers Then

                    SelProc.CommandText = "" & _
                    "      CREATE TEMPORARY TABLE PricesTMP " & _
                    "      SELECT * " & _
                    "      FROM   Prices " & _
                    "      WHERE  PriceCode=2647; " & _
                    "       " & _
                    "      CALL GetPrices2(?OffersClientCode); " & _
                    "      INSERT " & _
                    "      INTO   Prices " & _
                    "      SELECT * " & _
                    "      FROM   PricesTMP; " & _
                    "       " & _
                    "      DROP TEMPORARY TABLE PricesTMP;"

                    SelProc.ExecuteNonQuery()

                End If


                'Подготовка временной таблицы с контактами
                SelProc.CommandText = "" & _
                "drop TEMPORARY TABLE IF EXISTS ProviderContacts; " & _
                "CREATE TEMPORARY TABLE ProviderContacts engine=MEMORY " & _
                "AS " & _
                "        SELECT DISTINCT c.contactText, " & _
                "                        cd.FirmCode " & _
                "        FROM            usersettings.clientsdata cd " & _
                "                        JOIN contacts.contact_groups cg " & _
                "                        ON              cd.ContactGroupOwnerId = cg.ContactGroupOwnerId " & _
                "                        JOIN contacts.contacts c " & _
                "                        ON              cg.Id = c.ContactOwnerId " & _
                "        WHERE           firmcode IN " & _
                "                                    (SELECT DISTINCT FirmCode " & _
                "                                    FROM             Prices " & _
                "                                    ) " & _
                "                    AND cg.Type = 1 " & _
                "                    AND c.Type  = 0;" & _
                "INSERT " & _
                "INTO   ProviderContacts " & _
                "SELECT DISTINCT c.contactText, " & _
                "                cd.FirmCode " & _
                "FROM            usersettings.clientsdata cd " & _
                "                JOIN contacts.contact_groups cg " & _
                "                ON              cd.ContactGroupOwnerId = cg.ContactGroupOwnerId " & _
                "                JOIN contacts.persons p " & _
                "                ON              cg.id = p.ContactGroupId " & _
                "                JOIN contacts.contacts c " & _
                "                ON              p.Id = c.ContactOwnerId " & _
                "WHERE           firmcode IN " & _
                "                            (SELECT DISTINCT FirmCode " & _
                "                            FROM             Prices " & _
                "                            ) " & _
                "            AND cg.Type = 1 " & _
                "            AND c.Type  = 0;"
                SelProc.ExecuteNonQuery()

                GetMySQLFileWithDefault("Providers", SelProc, "" & _
                "SELECT   firm.FirmCode, " & _
                "         firm.FullName, " & _
                "         firm.Fax     , " & _
                "         LEFT(ifnull(group_concat(DISTINCT ProviderContacts.ContactText), ''), 255) " & _
                "FROM     clientsdata AS firm " & _
                "         LEFT JOIN ProviderContacts " & _
                "         ON       ProviderContacts.FirmCode = firm.FirmCode " & _
                "WHERE    firm.firmcode IN " & _
                "                          (SELECT DISTINCT FirmCode " & _
                "                          FROM             Prices " & _
                "                          ) " & _
                "GROUP BY firm.firmcode")

                SelProc.CommandText = "drop TEMPORARY TABLE IF EXISTS ProviderContacts"
                SelProc.ExecuteNonQuery()

                GetMySQLFileWithDefault("RegionalData", SelProc, _
                "SELECT DISTINCT regionaldata.FirmCode  , " & _
                "                regionaldata.RegionCode, " & _
                "                supportphone           , " & _
                "                ContactInfo            , " & _
                "                OperativeInfo " & _
                "FROM            regionaldata, " & _
                "                Prices " & _
                "WHERE           regionaldata.firmcode  = Prices.firmcode " & _
                "            AND regionaldata.regioncode= Prices.regioncode")


                GetMySQLFileWithDefault("PricesRegionalData", SelProc, _
                "SELECT PriceCode           , " & _
                "       RegionCode          , " & _
                "       STORAGE             , " & _
                "       MinReq              , " & _
                "       MainFirm            , " & _
                "       NOT disabledbyclient, " & _
                "       ControlMinReq " & _
                "FROM   Prices")

                SelProc.CommandText = "" & _
                "CREATE TEMPORARY TABLE tmpprd ( FirmCode INT unsigned, PriceCount MediumINT unsigned )engine=MEMORY; " & _
                "INSERT " & _
                "INTO   tmpprd " & _
                "SELECT   firmcode, " & _
                "         COUNT(pricecode) " & _
                "FROM     Prices " & _
                "GROUP BY FirmCode, " & _
                "         RegionCode;"

                SelProc.ExecuteNonQuery()

                GetMySQLFileWithDefault("PricesData", SelProc, _
                  "SELECT   Prices.FirmCode , " & _
                  "         Prices.pricecode, " & _
                  "                  concat(firm.shortname, IF(PriceCount> 1 " & _
                  "      OR ShowPriceName                                = 1, concat(' (', pricename, ')'), '')), " & _
                  "         ''                                                                                  , " & _
                  "        date_sub(PriceDate, interval time_to_sec(date_sub(now(), interval unix_timestamp() second)) second) , " & _
                  "         if(?OffersClientCode is null, ((ForceReplication != 0) " & _
                  "          OR (actual = 0) or ?Cumulative), 1)   " & _
                  "FROM     clientsdata AS firm, " & _
                  "         tmpprd             , " & _
                  "         Prices, " & _
                  "         AnalitFReplicationInfo ARI " & _
                  "WHERE    tmpprd.firmcode = firm.firmcode " & _
                  "     AND firm.firmcode   = Prices.FirmCode " & _
                  "     AND ARI.FirmCode    = Prices.FirmCode " & _
                  "     AND ARI.UserId    = ?UserId " & _
                  "GROUP BY Prices.FirmCode, " & _
                  "         Prices.pricecode")

                SQLText = "SELECT rejects.RowId        , " & _
                 "       rejects.FullName      , " & _
                 "       rejects.FirmCr     , " & _
                 "       rejects.CountryCr     , " & _
                 "       rejects.Series        , " & _
                 "       rejects.LetterNo      , " & _
                 "       rejects.LetterDate    , " & _
                 "       rejects.LaboratoryName, " & _
                 "       rejects.CauseRejects " & _
                  "FROM   addition.rejects, " & _
                  "       retclientsset rcs " & _
                  "WHERE rcs.clientcode = ?ClientCode" & _
                  "   AND alowrejection  = 1 "

                If Not GED Then
                    SQLText &= "   AND accessTime     > ?UpdateTime"
                End If

                GetMySQLFileWithDefault("Rejects", SelProc, SQLText)
                GetMySQLFileWithDefault("Clients", SelProc, helper.GetClientsCommand(False))
                GetMySQLFileWithDefault("DelayOfPayments", SelProc, helper.GetDelayOfPaymentsCommand())

                helper.SelectActivePrices()

                SelProc.CommandText = "" & _
                "CREATE TEMPORARY TABLE ParentCodes ENGINE=memory " & _
                "SELECT   PriceSynonymCode PriceCode, " & _
                "         MaxSynonymCode                 , " & _
                "         MaxSynonymFirmCrCode " & _
                "FROM     ActivePrices Prices        " & _
                "GROUP BY 1; "
                SelProc.ExecuteNonQuery()

                SQLText = "" & _
                "SELECT synonymfirmcr.synonymfirmcrcode, " & _
                "       LEFT(SYNONYM, 250) " & _
                "FROM   farm.synonymfirmcr, " & _
                "       ParentCodes " & _
                "WHERE  synonymfirmcr.pricecode        = ParentCodes.PriceCode "
                If Not GED Then

                    SQLText &= "AND synonymfirmcr.synonymfirmcrcode > MaxSynonymFirmCrCode "

                Else

                    SQLText &= " " & _
                   "UNION " & _
                   " " & _
                   "SELECT synonymfirmcrcode, " & _
                   "       LEFT(SYNONYM, 250) " & _
                   "FROM   farm.synonymfirmcr " & _
                   "WHERE  synonymfirmcrcode=0"

                End If

                GetMySQLFileWithDefault("SynonymFirmCr", SelProc, SQLText)

                SQLText = "" & _
                "SELECT synonym.synonymcode, " & _
                "       LEFT(synonym.synonym, 250) " & _
                "FROM   farm.synonym, " & _
                "       ParentCodes " & _
                "WHERE  synonym.pricecode  = ParentCodes.PriceCode "

                If Not GED Then
                    SQLText &= "AND synonym.synonymcode > MaxSynonymCode"
                End If

                GetMySQLFileWithDefault("Synonyms", SelProc, SQLText)


                If UpdateData.OffersClientCode Is Nothing Then

                    SelProc.CommandText = "" & _
                    "SELECT IFNULL(SUM(fresh), 0) " & _
                    "FROM   ActivePrices"
                    If CType(SelProc.ExecuteScalar, Integer) > 0 Or GED Then
                        helper.SelectOffers()
                        '"UPDATE ActivePrices Prices, " & _
                        '"       Core " & _
                        '"SET    CryptCost       = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(AES_ENCRYPT(Cost, (SELECT BaseCostPassword FROM   retclientsset WHERE  clientcode=?ClientCode)), CHAR(37), '%25'), CHAR(32), '%20'), CHAR(159), '%9F'), CHAR(161), '%A1'), CHAR(0), '%00') " & _
                        '"WHERE  Prices.PriceCode= Core.PriceCode " & _
                        '"   AND IF(?Cumulative, 1, Fresh) " & _
                        '"   AND Core.PriceCode!=2647 ; " & _
                        '" " & _
                        '"UPDATE Core " & _
                        '"SET    CryptCost        =concat(LEFT(CryptCost, 1), CHAR(ROUND((rand()*110)+32,0)), SUBSTRING(CryptCost,2,LENGTH(CryptCost)-4), CHAR(ROUND((rand()*110)+32,0)), RIGHT(CryptCost, 3)) " & _
                        '"WHERE  LENGTH(CryptCost)>0 " & _
                        '"   AND Core.PriceCode!=2647;"

                        CostOptimizer.OptimizeCostIfNeeded(ReadOnlyCn, ReadWriteCn, CCode)

                        GetMySQLFileWithDefault("Core", SelProc, _
                        "SELECT CT.PriceCode                      , " & _
                        "       CT.regioncode                     , " & _
                        "       CT.ProductId                      , " & _
                        "       ifnull(Core.codefirmcr, 0)        , " & _
                        "       Core.synonymcode                  , " & _
                        "       Core.SynonymFirmCrCode, " & _
                        "       Core.Code                         , " & _
                        "       Core.CodeCr                       , " & _
                        "       Core.unit                         , " & _
                        "       Core.volume                       , " & _
                        "       Core.Junk                         , " & _
                        "       Core.Await                        , " & _
                        "       Core.quantity                     , " & _
                        "       Core.note                         , " & _
                        "       Core.period                       , " & _
                        "       Core.doc                          , " & _
                        "       Core.RegistryCost                 , " & _
                        "       Core.VitallyImportant             , " & _
                        "       Core.RequestRatio                 , " & _
                        "       CT.Cost                           , " & _
                        "       RIGHT(CT.ID, 9)                   , " & _
                        "       OrderCost                         , " & _
                        "       MinOrderCount                       " & _
                        "FROM   Core CT        , " & _
                        "       ActivePrices AT, " & _
                        "       farm.core0 Core " & _
                        "WHERE  ct.pricecode =at.pricecode " & _
                        "   AND ct.regioncode=at.regioncode " & _
                        "   AND Core.id      =CT.id " & _
                        "   AND IF(?Cumulative, 1, fresh)")
                    Else
                        SelProc.CommandText = "SELECT ''" & _
                        " INTO OUTFILE 'C:/AFFiles/Core" & UserId & ".txt' FIELDS TERMINATED BY '" & Chr(159) & "' OPTIONALLY ENCLOSED BY '' ESCAPED BY '' LINES TERMINATED BY ''"
                        SelProc.ExecuteNonQuery()




                        SyncLock (FilesForArchive)

                            FilesForArchive.Enqueue(New FileForArchive("Core", False))


                        End SyncLock


                    End If

                Else

                    SelProc.CommandText = "" & _
                     "DROP TEMPORARY TABLE IF EXISTS  ActivePrices; " & _
                     "CREATE TEMPORARY TABLE ActivePrices engine=MEMORY " & _
                     "SELECT pricesdata.firmcode                            , " & _
                     "       i.pricecode                                    , " & _
                     "       i.costcode                                     , " & _
                     "       i.RegionCode                                   , " & _
                     "       1 UpCost, " & _
                     "       pricesdata.CostType " & _
                     "FROM   usersettings.intersection i " & _
                     "       JOIN usersettings.pricesdata " & _
                     "       ON     pricesdata.pricecode = i.pricecode " & _
                     "       JOIN usersettings.PricesCosts pc " & _
                     "       ON     pc.CostCode = i.CostCode " & _
                     "       JOIN usersettings.PriceItems pi " & _
                     "       ON     pi.Id = pc.PriceItemId " & _
                     "       JOIN farm.formrules f " & _
                     "       ON     f.Id = pi.FormRuleId " & _
                     "       JOIN usersettings.clientsdata " & _
                     "       ON     clientsdata.firmcode = pricesdata.firmcode " & _
                     "       JOIN usersettings.pricesregionaldata " & _
                     "       ON     pricesregionaldata.regioncode = i.regioncode " & _
                     "          AND pricesregionaldata.pricecode  = pricesdata.pricecode " & _
                     "       JOIN usersettings.RegionalData rd " & _
                     "       ON     rd.RegionCode = i.regioncode " & _
                     "          AND rd.FirmCode   = pricesdata.firmcode " & _
                     "       JOIN usersettings.clientsdata AS AClientsData " & _
                     "       ON     AClientsData.firmcode   = i.clientcode " & _
                     "          AND clientsdata.firmsegment = AClientsData.firmsegment " & _
                     "       JOIN usersettings.retclientsset r " & _
                     "       ON     r.clientcode    = AClientsData.FirmCode " & _
                     "WHERE  i.DisabledByAgency     = 0 " & _
                     "   AND clientsdata.firmstatus = 1 " & _
                     "   AND clientsdata.firmtype   = 0 " & _
                     "   AND ( " & _
                     "              clientsdata.maskregion & i.regioncode " & _
                     "       ) " & _
                     "       > 0 " & _
                     "   AND ( " & _
                     "              AClientsData.maskregion & i.regioncode " & _
                     "       ) " & _
                     "                                                    > 0 " & _
                     "   AND pricesdata.agencyenabled                     = 1 " & _
                     "   AND pricesdata.enabled                           = 1 " & _
                     "   AND pricesdata.pricetype                        <> 1 " & _
                     "   AND pricesregionaldata.enabled                   = 1 " & _
                     "   AND clientsdata.FirmCode!=234                        " & _
                     "   AND to_days(Now()) - to_days(pi.PriceDate) < f.maxold " & _
                     "   AND i.DisabledByClient=0 " & _
                     "   AND i.InvisibleOnClient=0 " & _
                     "   AND i.DisabledByFirm=0 " & _
                     "   AND i.clientcode                                 = ?OffersClientCode;"


                    SelProc.CommandText &= "" & _
                    "CALL GetOffers(?OffersClientCode, 0); "

                    SelProc.CommandText &= "" & _
                  "DROP TEMPORARY TABLE " & _
                  "IF EXISTS CoreT, CoreTP , CoreT2; " & _
                  "CREATE TEMPORARY TABLE CoreT  (ProductId INT unsigned, CodeFirmCr INT unsigned, Cost DECIMAL(8,2), CryptCost VARCHAR(32),UNIQUE MultiK(ProductId, CodeFirmCr))engine=MEMORY; " & _
                  "CREATE TEMPORARY TABLE CoreT2 (ProductId INT unsigned, CodeFirmCr INT unsigned, Cost DECIMAL(8,2), CryptCost VARCHAR(32),UNIQUE MultiK(ProductId, CodeFirmCr))engine=MEMORY; " & _
                  "CREATE TEMPORARY TABLE CoreTP (ProductId INT unsigned, Cost DECIMAL(8,2), CryptCost VARCHAR(32), UNIQUE MultiK(ProductId))engine                                    =MEMORY; " & _
                  "INSERT " & _
                  "  INTO   CoreT " & _
                  "    ( " & _
                  "      ProductId , " & _
                  "      CodeFirmCr, " & _
                  "      Cost " & _
                  "    ) " & _
                  "  SELECT   core0.ProductId , " & _
                  "    core0.codefirmcr, " & _
                  "    ROUND(AVG(cost), 2) " & _
                  "  FROM     farm.core0, " & _
                  "                 Core " & _
                  "  WHERE    core0.id=Core.id " & _
                  "  GROUP BY ProductId, " & _
                  "          CodeFirmCr; " & _
                  "  " & _
                  "  INSERT " & _
                  "    INTO   CoreTP " & _
                  "      ( " & _
                  "        ProductId, " & _
                  "        Cost " & _
                  "      ) " & _
                  "  SELECT   ProductId, " & _
                  "     ROUND(AVG(cost), 2) " & _
                  "  FROM     CoreT " & _
                  "  GROUP BY ProductId; " & _
                  "  " & _
                  "  INSERT " & _
                  "    INTO   CoreT2 " & _
                  "    SELECT * " & _
                  "    FROM   CoreT; " & _
                  "SET @RowId :=1;"
                    SelProc.ExecuteNonQuery()

                    'Выгрузка данных для ГУП
                    GetMySQLFileWithDefault("Core", SelProc, "" & _
                     "SELECT 2647                             , " & _
                     "       ?OffersRegionCode                , " & _
                     "       A.ProductId                      , " & _
                     "       A.CodeFirmCr                     , " & _
                     "       S.SynonymCode                    , " & _
                     "       SF.SynonymFirmCrCode             , " & _
                     "       ''                               , " & _
                     "       ''                               , " & _
                     "       ''                               , " & _
                     "       ''                               , " & _
                     "       0                                , " & _
                     "       0                                , " & _
                     "       ''                               , " & _
                     "       ''                               , " & _
                     "       ''                               , " & _
                     "       ''                               , " & _
                     "       ''                               , " & _
                     "       0                                , " & _
                     "       ''                               , " & _
                     "       IF(?ShowAvgCosts, a.Cost, '')    , " & _
                     "       @RowId := @RowId + 1             , " & _
                     "       ''                               , " & _
                     "       ''                                 " & _
                     "FROM   farm.Synonym S                   , " & _
                     "       farm.SynonymFirmCr SF            , " & _
                     "       CoreT A " & _
                     "WHERE  S.PriceCode   =2647 " & _
                     "   AND SF.PriceCode  =2647 " & _
                     "   AND S.ProductId   =A.ProductId " & _
                     "   AND SF.CodeFirmCr =A.CodeFirmCr " & _
                     "   AND A.CodeFirmCr IS NOT NULL " & _
                     " " & _
                     "UNION " & _
                     " " & _
                     "SELECT 2647                              , " & _
                     "       ?OffersRegionCode                 , " & _
                     "       A.ProductId                       , " & _
                     "       1                                 , " & _
                     "       S.SynonymCode                     , " & _
                     "       0                                 , " & _
                     "       ''                                , " & _
                     "       ''                                , " & _
                     "       ''                                , " & _
                     "       ''                                , " & _
                     "       0                                 , " & _
                     "       0                                 , " & _
                     "       ''                                , " & _
                     "       ''                                , " & _
                     "       ''                                , " & _
                     "       ''                                , " & _
                     "       ''                                , " & _
                     "       0                                 , " & _
                     "       ''                                , " & _
                     "       IF(?ShowAvgCosts, A.Cost, '')     , " & _
                     "       @RowId := @RowId + 1              , " & _
                     "       ''                                , " & _
                     "       ''                                  " & _
                     "FROM   farm.Synonym S                    , " & _
                     "       CoreTP A " & _
                     "WHERE  S.PriceCode =2647 " & _
                     "   AND S.ProductId =A.ProductId")


                End If

                SelProc.CommandText = "drop temporary table IF EXISTS MaxCodesSynFirmCr, MinCosts, ActivePrices, Prices, Core, tmpprd, MaxCodesSyn, ParentCodes; "
                SelProc.ExecuteNonQuery()

                myTrans.Commit()

                helper.UpdateReplicationInfo()

                TS = Now().Subtract(StartTime)

                If Math.Round(TS.TotalSeconds, 0) > 30 Then

                    Addition &= "Sel: " & Math.Round(TS.TotalSeconds, 0) & "; "

                End If

            Catch ex As ThreadAbortException
                If Not (ReadOnlyCn.State = ConnectionState.Closed Or ReadOnlyCn.State = ConnectionState.Broken) Then myTrans.Rollback()

            Catch MySQLErr As MySqlException

                If ThreadZipStream.IsAlive Then ThreadZipStream.Abort()
                If Not (ReadOnlyCn.State = ConnectionState.Closed Or ReadOnlyCn.State = ConnectionState.Broken) Then myTrans.Rollback()


                If MySQLErr.Number = 1213 Or MySQLErr.Number = 1205 Then
                    System.Threading.Thread.Sleep(500)
                    GoTo RestartTrans2
                End If

                MailErr("Основной поток выборки: " & MySQLErr.Message, SelProc.CommandText & MySQLErr.StackTrace)
                ErrorFlag = True
                UpdateType = 6
                'NeedCloseCn = True
                Addition &= MySQLErr.Message


            Catch ErrorTXT As Exception
                ErrorFlag = True
                'NeedCloseCn = True
                UpdateType = 6
                Addition &= ErrorTXT.Message
                MailErr("Основной поток выборки, клиент: " & CCode, ErrorTXT.Message & ErrorTXT.StackTrace)
                If ThreadZipStream.IsAlive Then ThreadZipStream.Abort()
                If Not (ReadOnlyCn.State = ConnectionState.Closed Or ReadOnlyCn.State = ConnectionState.Broken) Then myTrans.Rollback()
            End Try
        Catch err As Exception
            MailErr("Основной поток выборки, general " & CCode, err.Message & ": " & err.StackTrace)
            ErrorFlag = True
        End Try
    End Sub


    Private Function PostOrderDB(ByVal ClientCode As UInt32, ByVal ClientOrderID As UInt32, _
             ByVal ProductID As UInt32(), ByVal OrderID As UInt64, ByVal CodeFirmCr As String(), _
             ByVal SynonymCode As UInt32(), ByVal SynonymFirmCrCode As String(), _
             ByVal Code As String(), _
             ByVal CodeCr As String(), ByVal Quantity As UInt16(), ByVal Junk As Boolean(), _
             ByVal Await As Boolean(), ByVal Cost As Decimal(), ByVal PriceCode As UInt32, _
             ByVal MinCost As String(), _
             ByVal MinPriceCode As String(), _
             ByVal LeaderMinCost As String(), _
             ByVal LeaderMinPriceCode As String() _
             ) As Boolean

        Dim newrow As DataRow
        Dim PosID As Integer = 0

        Dim LeaderMinPriceCodeP As UInt32 = 0
        Dim MinPriceCodeP As UInt32 = 0
        Dim SynonymFirmCrCodeP As UInt32
        Dim MinCostP As Decimal = 0
        Dim LaederMinCostP As Decimal = 0
        Dim ProblemStr As String = String.Empty

        DS.Tables("OrdersL").Clear()
        DS.Tables("OrdersDouble").Clear()


        For i = 0 To ProductID.Length - 1
            newrow = DS.Tables("OrdersL").NewRow
            newrow.Item("ProductID") = ProductID(i)
            newrow.Item("SynonymCode") = SynonymCode(i)

            If UInt32.TryParse(SynonymFirmCrCode(i), SynonymFirmCrCodeP) Then

                If SynonymFirmCrCodeP > 2 Then

                    newrow.Item("SynonymFirmCrCode") = SynonymFirmCrCodeP

                Else

                    newrow.Item("SynonymFirmCrCode") = DBNull.Value

                End If

            Else

                newrow.Item("SynonymFirmCrCode") = DBNull.Value

            End If


            If CodeFirmCr(i).Length < 1 Then
                newrow.Item("CodeFirmCr") = DBNull.Value
            Else
                newrow.Item("CodeFirmCr") = CodeFirmCr(i)
            End If



            If Left(Code(i), 1) = "?" Then
                Dim ResStr As String = String.Empty
                Try
                    For PosID = 2 To Len(Code(i)) Step 3
                        ResStr &= Chr(Convert.ToInt32(Left(Mid(Code(i), PosID), 3)))
                    Next
                Catch err As Exception
                    MailErr("Формирование Code", err.Message)
                End Try
                newrow.Item("Code") = ResStr

            Else
                newrow.Item("Code") = Code(i)
            End If



            If Left(CodeCr(i), 1) = "?" Then
                Dim ResStr As String = String.Empty
                Try
                    For PosID = 2 To Len(CodeCr(i)) Step 3
                        ResStr &= Chr(Convert.ToInt32(Left(Mid(CodeCr(i), PosID), 3)))
                    Next
                Catch err As Exception
                    MailErr("Формирование CodeCr", err.Message)
                End Try
                newrow.Item("CodeCr") = ResStr
            Else
                newrow.Item("CodeCr") = CodeCr(i)
            End If


            newrow.Item("Quantity") = Quantity(i)
            newrow.Item("Junk") = Junk(i)
            newrow.Item("Await") = Await(i)
            newrow.Item("Cost") = Cost(i)

            If CalculateLeader Then

                If UInt32.TryParse(MinPriceCode(i), MinPriceCodeP) Then newrow.Item("PriceCode") = MinPriceCodeP
                If UInt32.TryParse(LeaderMinPriceCode(i), LeaderMinPriceCodeP) Then newrow.Item("LeaderPriceCode") = LeaderMinPriceCodeP

                If Decimal.TryParse(LeaderMinCost(i), System.Globalization.NumberStyles.Currency, System.Globalization.CultureInfo.InvariantCulture.NumberFormat, LaederMinCostP) Then newrow.Item("LeaderMinCost") = LaederMinCostP
                If Decimal.TryParse(MinCost(i), System.Globalization.NumberStyles.Currency, System.Globalization.CultureInfo.InvariantCulture.NumberFormat, MinCostP) Then newrow.Item("MinCost") = MinCostP

            End If


            DS.Tables("OrdersL").Rows.Add(newrow)
        Next


        OrderInsertCm.CommandText = "" & _
        "SELECT  ol.* " & _
      "FROM    orders.ordershead oh, " & _
      "        orders.orderslist ol " & _
      "WHERE   clientorderid=" & ClientOrderID & _
      "    AND writetime    >ifnull( " & _
      "        (SELECT MAX(requesttime) " & _
      "        FROM    logs.AnalitFUpdates px " & _
      "        WHERE   updatetype                  =2 " & _
      "            AND px.UserId               =" & UserId & _
      "        ), now() - interval 2 week) " & _
      "    AND clientcode=" & ClientCode & _
      "    AND ol.orderid=oh.rowid"


        If OrderInsertDA.Fill(DS, "OrdersDouble") > 0 Then

            Dim DelRowId As List(Of DataRow) = New List(Of DataRow)
            Dim Row As DataRow

            For i = 0 To DS.Tables("OrdersL").Rows.Count - 1

                For Each Row In DS.Tables("OrdersDouble").Rows


                    If DS.Tables("OrdersL").Rows(i).Item("ProductID").Equals(Row.Item("ProductID")) _
                      And DS.Tables("OrdersL").Rows(i).Item("CodeFirmCr").Equals(Row.Item("CodeFirmCr")) _
                      And DS.Tables("OrdersL").Rows(i).Item("SynonymCode").Equals(Row.Item("SynonymCode")) _
                      And DS.Tables("OrdersL").Rows(i).Item("SynonymFirmCrCode").Equals(Row.Item("SynonymFirmCrCode")) _
                      And DS.Tables("OrdersL").Rows(i).Item("Code").Equals(Row.Item("Code")) _
                      And DS.Tables("OrdersL").Rows(i).Item("CodeCr").Equals(Row.Item("CodeCr")) _
                      And DS.Tables("OrdersL").Rows(i).Item("Junk").Equals(Row.Item("Junk")) _
                      And DS.Tables("OrdersL").Rows(i).Item("Await").Equals(Row.Item("Await")) _
                       Then
                        If DS.Tables("OrdersL").Rows(i).Item("Quantity").Equals(Row.Item("Quantity")) Then

                            DelRowId.Add(DS.Tables("OrdersL").Rows(i))
                            ProblemStr &= "В новом заказе №" & OrderID & " удалена дублирующаяся строка с заказом №" & Row.Item("OrderID").ToString & _
                                ", строка №" & Row.Item("rowid").ToString & Chr(10) & Chr(13)

                        Else
                            Try
                                DS.Tables("OrdersL").Rows(i).Item("Quantity") = Convert.ToUInt16(DS.Tables("OrdersL").Rows(i).Item("Quantity")) - Convert.ToUInt16(Row.Item("Quantity"))
                                ProblemStr &= "В новом заказе №" & OrderID & " изменено колличество товара в сявязи с дублированием с заказом №" & Row.Item("OrderID").ToString & _
                                    ", строка №" & Row.Item("rowid").ToString & Chr(10) & Chr(13)
                            Catch e As Exception
                                MailErr("Дублирующийся заказ", e.Message & ": " & e.StackTrace)
                            End Try

                        End If
                    End If

                Next

            Next

            If DelRowId.Count >= DS.Tables("OrdersL").Rows.Count Then
                DS.Tables("OrdersL").Clear()
            Else

                For Each RowForDelete As DataRow In DelRowId
                    DS.Tables("OrdersL").Rows.Remove(RowForDelete)
                Next
            End If
        End If


        If DS.Tables("OrdersL").Rows.Count = 0 Then
            ProblemStr = "Заказ №" & ClientOrderID & "(по клиенту) не принят как полностью повторяющийся."
            Return False

        End If

        With OrderInsertCm
            .CommandText = String.Empty
            .Parameters.Clear()
            If ProblemStr <> String.Empty Then
                Addition = ProblemStr
                .CommandText = "update orders.ordershead set rowcount=" & DS.Tables("OrdersL").Rows.Count & " where rowid=" & OrderID & "; "
                'MailErr("Дубли в заказе", ProblemStr)
            End If

            .CommandText &= " insert into orders.orderslist (OrderID, ProductId, CodeFirmCr, SynonymCode, SynonymFirmCrCode, Code, CodeCr, Quantity, Junk, Await, Cost)" & _
                   " select  " & OrderID & ", products.ID, if(Prod.Id is null, sfcr.codefirmcr, Prod.Id) , syn.synonymcode, sfcr.SynonymFirmCrCode, ?Code, ?CodeCr, ?Quantity, ?Junk, ?Await, ?Cost" & _
                   " from catalogs.products" & _
                   " left join farm.synonym  syn on syn.synonymcode=?SynonymCode" & _
                   " left join farm.synonymfirmcr sfcr on sfcr.SynonymFirmCrCode=?SynonymFirmCrCode" & _
                   " left join  catalogs.Producers Prod on Prod.Id=?CodeFirmCr" & _
                   " where products.ID=?ProductID; "

            If CalculateLeader And (MinPriceCodeP > 0 Or LeaderMinPriceCodeP > 0) And (LaederMinCostP > 0 Or MinCostP > 0) Then

                .CommandText &= " insert into orders.leaders " & _
                                "values(last_insert_id(), nullif(?MinCost, 0), nullif(?LeaderMinCost, 0), nullif(?PriceCode, 0), nullif(?LeaderPriceCode, 0))"

                .Parameters.Add(New MySql.Data.MySqlClient.MySqlParameter("?PriceCode", MySqlDbType.UInt32, 0, "PriceCode"))
                .Parameters.Add(New MySql.Data.MySqlClient.MySqlParameter("?LeaderPriceCode", MySqlDbType.UInt32, 0, "LeaderPriceCode"))
                .Parameters.Add(New MySql.Data.MySqlClient.MySqlParameter("?MinCost", MySqlDbType.Decimal, 0, "MinCost"))
                .Parameters.Add(New MySql.Data.MySqlClient.MySqlParameter("?LeaderMinCost", MySqlDbType.Decimal, 0, "LeaderMinCost"))


            End If

            .Parameters.Add(New MySql.Data.MySqlClient.MySqlParameter("?ProductID", MySqlDbType.UInt32, 0, "ProductID"))
            .Parameters.Add(New MySql.Data.MySqlClient.MySqlParameter("?CodeFirmCr", MySqlDbType.UInt32, 0, "CodeFirmCr"))
            .Parameters.Add(New MySql.Data.MySqlClient.MySqlParameter("?SynonymCode", MySqlDbType.UInt32, 0, "SynonymCode"))
            .Parameters.Add(New MySql.Data.MySqlClient.MySqlParameter("?SynonymFirmCrCode", MySqlDbType.UInt32, 0, "SynonymFirmCrCode"))
            .Parameters.Add(New MySql.Data.MySqlClient.MySqlParameter("?Code", MySqlDbType.VarString, 0, "Code"))
            .Parameters.Add(New MySql.Data.MySqlClient.MySqlParameter("?CodeCr", MySqlDbType.VarString, 0, "CodeCr"))
            .Parameters.Add(New MySql.Data.MySqlClient.MySqlParameter("?Quantity", MySqlDbType.UInt16, 0, "Quantity"))
            .Parameters.Add(New MySql.Data.MySqlClient.MySqlParameter("?Junk", MySqlDbType.Bit, 0, "Junk"))
            .Parameters.Add(New MySql.Data.MySqlClient.MySqlParameter("?Await", MySqlDbType.Bit, 0, "Await"))
            .Parameters.Add(New MySql.Data.MySqlClient.MySqlParameter("?Cost", MySqlDbType.Decimal, 0, "Cost"))
            .Transaction = myTrans
        End With

        OrderInsertDA.Update(DS.Tables("OrdersL"))

        Return True

    End Function

    'Исходная строка преобразуется в набор символов Hex-кодов
    Private Function ToHex(ByVal Src As String) As String
        Dim sb As System.Text.StringBuilder = New System.Text.StringBuilder
        Dim t As Char
        For Each t In Src
            sb.Append(Convert.ToInt32(t).ToString("X2"))
        Next
        Return sb.ToString()
    End Function


    <WebMethod()> _
    Public Sub SendUDataFull( _
        ByVal Login As String, _
        ByVal Data As String, _
        ByVal OriginalData As String, _
        ByVal SerialData As String, _
        ByVal MaxWriteTime As Date, _
        ByVal MaxWriteFileName As String, _
        ByVal OrderWriteTime As Date, _
        ByVal ClientTimeZoneBias As Integer, _
        ByVal RSTUIN As String)

        SendUDataFullEx(Login, Data, OriginalData, SerialData, MaxWriteTime, MaxWriteFileName, OrderWriteTime, ClientTimeZoneBias, _
                        -1, Nothing, Nothing, Nothing, Nothing, Nothing, Nothing, _
                        RSTUIN)
    End Sub


    <WebMethod()> _
    Public Sub SendUDataFullEx( _
        ByVal Login As String, _
        ByVal Data As String, _
        ByVal OriginalData As String, _
        ByVal SerialData As String, _
        ByVal MaxWriteTime As Date, _
        ByVal MaxWriteFileName As String, _
        ByVal OrderWriteTime As Date, _
        ByVal ClientTimeZoneBias As Integer, _
        ByVal DNSChangedState As Integer, _
        ByVal RASEntry As String, _
        ByVal DefaultGateway As String, _
        ByVal IsDynamicDnsEnabled As Boolean, _
        ByVal ConnectionSettingId As String, _
        ByVal PrimaryDNS As String, _
        ByVal AlternateDNS As String, _
        ByVal RSTUIN As String)

        If DBConnect("SendUData") Then

            Dim Logger As ILog = LogManager.GetLogger(Me.GetType())

            Try
                GetClientCode()

                Dim ResStrRSTUIN As String = String.Empty
                Try
                    For i = 1 To Len(RSTUIN) Step 3
                        ResStrRSTUIN &= Chr(Convert.ToInt16(Left(Mid(RSTUIN, i), 3)))
                    Next
                Catch err As Exception
                    Logger.ErrorFormat("Ошибка в SendUData при формировании RSTUIN : {0}\n{1}", RSTUIN, err)
                End Try

                Dim accountMessage As String = String.Format( _
                        "ClientCode = {0} Login = {1} Password = {2} OriginalPassword = {3} Serial = {4} MaxWriteTime = {5} " & _
                        "MaxWriteFileName = {6} OrderWriteTime = {7} ClientTimeZoneBias = {8} " & _
                        "DNSChangedState = {9} RASEntry = {10} DefaultGateway = {11} IsDynamicDnsEnabled = {12} " & _
                        "ConnectionSettingId = {13} PrimaryDNS = {14} AlternateDNS = {15} RSTUIN = {16}", _
                        CCode, Login, Data, OriginalData, SerialData, _
                        If(MaxWriteTime < New System.DateTime(2000, 1, 1), Nothing, MaxWriteTime), _
                        If(String.IsNullOrEmpty(MaxWriteFileName), Nothing, MaxWriteFileName), _
                        If(OrderWriteTime < New System.DateTime(2000, 1, 1), Nothing, OrderWriteTime), _
                        If(ClientTimeZoneBias = 0, Nothing, ClientTimeZoneBias), _
                        DNSChangedState, _
                        RASEntry, _
                        DefaultGateway, _
                        IsDynamicDnsEnabled, _
                        ConnectionSettingId, _
                        PrimaryDNS, _
                        AlternateDNS, _
                        ResStrRSTUIN)

                Logger.Info(accountMessage)

                Dim command As MySqlCommand = New MySqlCommand( _
                    "insert into logs.SpyInfo (UserId, Login, Password, OriginalPassword, SerialNumber, MaxWriteTime, MaxWriteFileName, OrderWriteTime, ClientTimeZoneBias, " & _
                        "DNSChangedState, RASEntry, DefaultGateway, IsDynamicDnsEnabled, ConnectionSettingId, PrimaryDNS, AlternateDNS, RostaUIN) " & _
                    "values (?UserId, ?Login, ?Password, ?OriginalPassword, ?SerialNumber, ?MaxWriteTime, ?MaxWriteFileName, ?OrderWriteTime, ?ClientTimeZoneBias, " & _
                        "?DNSChangedState, ?RASEntry, ?DefaultGateway, ?IsDynamicDnsEnabled, ?ConnectionSettingId, ?PrimaryDNS, ?AlternateDNS, ?RostaUIN);", _
                    ReadWriteCn)

                command.Parameters.AddWithValue("?UserId", UserId)
                command.Parameters.AddWithValue("?Login", Login)
                command.Parameters.AddWithValue("?Password", Data)
                command.Parameters.AddWithValue("?OriginalPassword", OriginalData)
                command.Parameters.AddWithValue("?SerialNumber", SerialData)
                command.Parameters.Add("?MaxWriteTime", MySqlDbType.DateTime)
                If (MaxWriteTime > New System.DateTime(1900, 1, 1)) Then command.Parameters.Item("?MaxWriteTime").Value = MaxWriteTime.ToLocalTime
                command.Parameters.AddWithValue("?MaxWriteFileName", MaxWriteFileName)
                command.Parameters.Add("?OrderWriteTime", MySqlDbType.DateTime)
                If (OrderWriteTime > New System.DateTime(1900, 1, 1)) Then command.Parameters.Item("?OrderWriteTime").Value = OrderWriteTime.ToLocalTime
                command.Parameters.AddWithValue("?ClientTimeZoneBias", ClientTimeZoneBias)
                If (DNSChangedState = -1) Then
                    command.Parameters.AddWithValue("?DNSChangedState", DBNull.Value)
                Else
                    command.Parameters.AddWithValue("?DNSChangedState", DNSChangedState)
                End If
                command.Parameters.AddWithValue("?RASEntry", If(String.IsNullOrEmpty(RASEntry), Nothing, RASEntry))
                command.Parameters.AddWithValue("?DefaultGateway", If(String.IsNullOrEmpty(DefaultGateway), Nothing, DefaultGateway))
                command.Parameters.AddWithValue("?IsDynamicDnsEnabled", IsDynamicDnsEnabled)
                command.Parameters.AddWithValue("?ConnectionSettingId", If(String.IsNullOrEmpty(ConnectionSettingId), Nothing, ConnectionSettingId))
                command.Parameters.AddWithValue("?PrimaryDNS", If(String.IsNullOrEmpty(PrimaryDNS), Nothing, PrimaryDNS))
                command.Parameters.AddWithValue("?AlternateDNS", If(String.IsNullOrEmpty(AlternateDNS), Nothing, AlternateDNS))
                command.Parameters.AddWithValue("?RostaUIN", If(String.IsNullOrEmpty(ResStrRSTUIN), Nothing, ResStrRSTUIN))

                command.ExecuteNonQuery()
            Catch ex As Exception
                Logger.Error("Ошибка в SendUData", ex)
                MailErr("Ошибка в SendUData", ex.ToString())
            Finally
                DBDisconnect()
            End Try
        End If

    End Sub


    <WebMethod()> _
    Public Function GetPasswords(ByVal UniqueID As String) As String
        Dim ErrorFlag As Boolean = False
        Dim BasecostPassword As String

        If DBConnect("GetPasswords") Then

            'TODO: Встроить логирование в prgdataex
            Try
                GetClientCode()

                'Проверяем совпадение уникального идентификатора
                If CheckID Then
                    UID = UniqueID
                    If Not FnCheckID() Then
                        MessageH = "Обновление программы на данном компьютере запрещено."
                        MessageD = "Пожалуйста, обратитесь в АК «Инфорум».[2]"
                        Addition = "Несоответствие UIN."
                        'MailErr("Несоответствие уникального идентификатора при получении паролей", "")
                        UpdateType = 5
                        ErrorFlag = True
                        'ProtocolThread.Start()
                        Return "Error=" & MessageH & ";Desc=" & MessageD
                    End If
                End If

                Cm.CommandText = "select BaseCostPassword from retclientsset where clientcode=" & CCode
                Using SQLdr As MySqlDataReader = Cm.ExecuteReader
                    SQLdr.Read()
                    BasecostPassword = SQLdr.GetString(0)
                End Using

				'Получаем маску разрешенных для сохранения гридов
				If Not UpdateData.IsFutureClient Then
					Cm.CommandText = "SELECT ifnull(sum(SaveGridID), 0) FROM ret_save_grids r where ClientCode = " & CCode
				Else
					Cm.CommandText = "select IFNULL(sum(up.SecurityMask), 0) " & _
	  "from usersettings.AssignedPermissions ap " & _
	  "join usersettings.UserPermissions up on up.Id = ap.PermissionId " & _
	  "where ap.UserId=" & UpdateData.UserId
				End If

				Dim SaveGridMask As UInt64 = Convert.ToUInt64(Cm.ExecuteScalar())

				If (BasecostPassword <> Nothing) Then
					Dim S As String = "Basecost=" & ToHex(BasecostPassword) & ";SaveGridMask=" & SaveGridMask.ToString("X7") & ";"
					Return S
				Else
					MailErr("Ошибка при получении паролей", "У клиента не заданы пароли для шифрации данных")
					Addition = "Не заданы пароли для шифрации данных"
					ErrorFlag = True
					UpdateType = 5
					'ProtocolThread.Start()
				End If
			Catch Exp As Exception
				MailErr("Ошибка при получении паролей", Exp.Message & ": " & Exp.StackTrace)
				Addition = Exp.Message
				ErrorFlag = True
				UpdateType = 5
				'ProtocolThread.Start()
			Finally
				DBDisconnect()
			End Try
        End If

        If ErrorFlag Then
            Return "Error=При выполнении Вашего запроса произошла ошибка.;Desc=Пожалуйста повторите попытку через несколько минут."
        End If
    End Function

    <WebMethod()> Public Function PostPriceDataSettings(ByVal UniqueID As String, ByVal PriceCodes As Int32(), ByVal RegionCodes As Int64(), ByVal INJobs As Boolean()) As String
        Dim ErrorFlag As Boolean = False
        Dim tran As MySqlTransaction = Nothing

        If DBConnect("PostPriceDataSettings") Then
            Try
                GetClientCode()

                'Проверяем совпадение уникального идентификатора
                If CheckID Then
                    UID = UniqueID
                    If Not FnCheckID() Then
                        MessageH = "Обновление программы на данном компьютере запрещено."
                        MessageD = "Пожалуйста, обратитесь в АК «Инфорум».[2]"
                        Addition = "Несоответствие UIN."
                        'MailErr("Несоответствие уникального идентификатора при обновлении настроек прайс-листов", "")
                        UpdateType = 5
                        ErrorFlag = True
                        'ProtocolThread.Start()
                        Return "Error=" & MessageH & ";Desc=" & MessageD
                    End If
                End If

                If UpdateData.IsFutureClient Then
                    Dim helper = New UpdateHelper(UpdateData, ReadOnlyCn, ReadWriteCn)
                    helper.UpdatePriceSettings(PriceCodes, RegionCodes, INJobs)
                    Return ""
                End If

                'Проверяем длины массивов
                If ((PriceCodes.Length > 0) And (PriceCodes.Length = INJobs.Length) And (RegionCodes.Length = PriceCodes.Length)) Then
                    Dim dtIntersection As DataTable = New DataTable
                    'Команда на выборку данных из Intersection
                    Dim cmdSel As MySqlCommand = New MySqlCommand("SELECT i.Id, i.RegionCode, i.PriceCode, i.DisabledByClient FROM usersettings.intersection i where ClientCode = ?ClientCode", ReadWriteCn)
                    cmdSel.Parameters.AddWithValue("?ClientCode", CCode)
                    Dim daSel As MySqlDataAdapter = New MySqlDataAdapter(cmdSel)
                    Dim cmdUp As MySqlCommand = New MySqlCommand

                    'Заполняем команду на обновление
                    daSel.UpdateCommand = cmdUp
                    cmdUp.Connection = ReadWriteCn
                    cmdUp.CommandText = String.Empty
                    'cmdUp.CommandText &= "set @INUser = ?OperatorName; set @INHost = ?OperatorHost; "
                    cmdUp.CommandText &= "update intersection i set " & _
                         "i.DisabledByClient=?DisabledByClient " & _
                    " where i.id = ?Id;"
                    cmdUp.Parameters.AddWithValue("?OperatorName", UserName & "[AF]")
                    cmdUp.Parameters.AddWithValue("?OperatorHost", UserHost)
                    cmdUp.Parameters.AddWithValue("?INUser", UserName & "[AF]")
                    cmdUp.Parameters.AddWithValue("?INHost", UserHost)
                    cmdUp.Parameters.Add("?ID", MySqlDbType.Int64, 0, "ID")
                    cmdUp.Parameters.Add("?DisabledByClient", MySqlDbType.Bit, 0, "DisabledByClient")

                    'Заполнили таблицу пересечений
                    daSel.Fill(dtIntersection)

                    Dim drs() As DataRow

                    For I As Integer = 0 To PriceCodes.Length - 1
                        drs = dtIntersection.Select("PriceCode = " & PriceCodes(I) & " and RegionCode = " & RegionCodes(I))
                        If ((Not (drs Is Nothing)) And (drs.Length > 0)) Then

                            If (CByte(drs(0)("DisabledByClient")) <> CByte(IIf(Not INJobs(I), 1, 0))) Then
                                drs(0)("DisabledByClient") = IIf(Not INJobs(I), 1, 0)
                            End If
                        End If
                    Next

                    Dim Quit As Boolean = False
                    Dim ErrCount As Integer = 0
                    Dim dtChanges As DataTable = dtIntersection.GetChanges()

                    If Not (dtChanges Is Nothing) Then
                        Do
                            Try
                                tran = ReadWriteCn.BeginTransaction()
                                cmdSel.Transaction = tran
                                cmdUp.Transaction = tran


                                daSel.Update(dtChanges)

                                tran.Commit()
                                Quit = True
                                Return "Res=OK"

                            Catch MySQLError As MySqlException
                                If Not (tran Is Nothing) Then
                                    Try
                                        tran.Rollback()
                                    Catch
                                    End Try
                                End If
                                If (ErrCount < 10) And ((MySQLError.Number = 1213) Or (MySQLError.Number = 1205)) Then
                                    'MailErr("Deadlock при применении обновлений настроек прайс-листов", MySQLError.Message & " - код: " & MySQLError.Number)
                                    ErrCount += 1
                                    System.Threading.Thread.Sleep(300)
                                Else
                                    Quit = True
                                    ErrorFlag = True
                                    MailErr("Ошибка при применении обновлений настроек прайс-листов", MySQLError.ToString())
                                End If
                            Catch ex As Exception
                                Quit = True
                                If Not (tran Is Nothing) Then
                                    Try
                                        tran.Rollback()
                                    Catch
                                    End Try
                                End If
                                ErrorFlag = True
                                MailErr("Ошибка при применении обновлений настроек прайс-листов", ex.ToString())
                            End Try
                        Loop Until Quit
                    End If

                Else
                    MailErr("Ошибка при обновлении настроек прайс-листов", "Не совпадают длины полученных массивов")
                    ErrorFlag = True
                End If

            Catch Exp As Exception
                MailErr("Ошибка при обновлении настроек прайс-листов", Exp.Message)
                ErrorFlag = True
            Finally
                DBDisconnect()
            End Try
        End If

        If ErrorFlag Then
            Return "Error=При выполнении Вашего запроса произошла ошибка.;Desc=Пожалуйста повторите попытку через несколько минут."
        End If
    End Function


    <WebMethod()> Public Function GetReclame() As String
        Dim MaxReclameFileDate As Date
        Dim NewZip As Boolean = True

        If DBConnect("GetReclame") Then
            Try
                GetClientCode()

                Dim updateHelpe = New UpdateHelper(UpdateData, ReadOnlyCn, ReadWriteCn)
                Dim reclameData = updateHelpe.GetReclame()
                MaxReclameFileDate = reclameData.ReclameDate

                Reclame = True
                ReclamePath = ResultFileName & "Reclame\" & reclameData.Region & "\"

                MySQLFileDelete(ResultFileName & "r" & UserId & ".zip")

                Dim FileList As String()
                Dim FileName As String

                If Not Directory.Exists(ReclamePath) Then Directory.CreateDirectory(ReclamePath)

                FileList = Directory.GetFiles(ReclamePath)
                FileCount = 0

                For Each FileName In FileList

                    FileInfo = New FileInfo(FileName)

                    If FileInfo.LastWriteTime.Subtract(reclameData.ReclameDate).TotalSeconds > 1 Then

                        FileCount += 1

                        SyncLock (FilesForArchive)

                            FilesForArchive.Enqueue(New FileForArchive(FileInfo.Name, True))

                        End SyncLock

                        If FileInfo.LastWriteTime > MaxReclameFileDate Then MaxReclameFileDate = FileInfo.LastWriteTime

                    End If

                Next

                If MaxReclameFileDate > Now() Then MaxReclameFileDate = Now()

                If FileCount > 0 Then

                    ZipStream()

                    FileInfo = New FileInfo(ResultFileName & "r" & UserId & ".zip")
                    FileInfo.CreationTime = MaxReclameFileDate

                End If

            Catch ex As Exception
                Log.Error("Ошибка при загрузке рекламы", ex)
                ErrorFlag = True
            Finally
                DBDisconnect()
            End Try
        End If

        If ErrorFlag Then
            GetReclame = ""
        Else
            If FileCount > 0 Then

                GetReclame = "URL=" & Context.Request.Url.Scheme & Uri.SchemeDelimiter & Context.Request.Url.Authority & Context.Request.ApplicationPath & "/GetFileReclameHandler.ashx;New=" & True

            Else
                GetReclame = ""
            End If
        End If

    End Function

    <WebMethod()> Public Function ReclameComplete() As Boolean
        Try
            DBConnect("ReclameComplete")
            GetClientCode()

            FileInfo = New FileInfo(ResultFileName & "r" & UserId & ".zip")

            If FileInfo.Exists Then

                myTrans = ReadWriteCn.BeginTransaction(IsoLevel)
                Cm.CommandText = "update UserUpdateInfo set ReclameDate=?ReclameDate where UserId=" & UserId
                Cm.Parameters.AddWithValue("?ReclameDate", FileInfo.CreationTime)
                Cm.Connection = ReadWriteCn
                Cm.ExecuteNonQuery()
                myTrans.Commit()


            End If

            DBDisconnect()
            Reclame = True
            MySQLFileDelete(ResultFileName & "r" & UserId & ".zip")
            ReclameComplete = True
        Catch ex As Exception
            MailErr("Подтверждение рекламы", ex.Message)
            ReclameComplete = False
        End Try
    End Function



    Private Sub SetCodesProc()
        Try



            SelProc.Connection = ReadWriteCn
            SelProc.Transaction = myTrans


            SelProc.CommandText = "" & _
            "UPDATE AnalitFReplicationInfo " & _
            "SET    ForceReplication    =0 " & _
            "WHERE  UserId           =" & UserId & _
            " AND ForceReplication=2; "

            SelProc.CommandText &= "" & _
              "UPDATE UserUpdateInfo " & _
              "SET    UpdateDate=UncommitedUpdateDate," & _
              "       MessageShowCount=if(MessageShowCount > 0, MessageShowCount - 1, 0)" & _
              "WHERE  UserId    =" & UserId

            If Len(AbsentPriceCodes) > 0 Then

                SelProc.CommandText &= "; " & _
                 "UPDATE AnalitFReplicationInfo ARI, PricesData Pd " & _
                 "SET    MaxSynonymFirmCrCode=0, " & _
                 "MaxSynonymCode=0, " & _
                 "UncMaxSynonymCode=0, " & _
                 "UncMaxSynonymFirmCrCode=0 " & _
                 "WHERE  UserId           =" & UserId & _
                 " AND Pd.FirmCode=ARI.FirmCode" & _
                 " AND Pd.PriceCode in (" & AbsentPriceCodes & ")"

                Addition &= "!!! " & AbsentPriceCodes

            Else

                SelProc.CommandText &= "; " & _
                "UPDATE AnalitFReplicationInfo " & _
                "SET    MaxSynonymFirmCrCode    =UncMaxSynonymFirmCrCode " & _
                "WHERE  UncMaxSynonymFirmCrCode!=0 " & _
                "   AND UserId                  =" & UserId


                SelProc.CommandText &= "; " & _
                 "UPDATE AnalitFReplicationInfo " & _
                 "SET    MaxSynonymCode    =UncMaxSynonymCode " & _
                 "WHERE  UncMaxSynonymCode!=0 " & _
                 "   AND UserId                  =" & UserId

            End If


RestartMaxCodesSet:
            myTrans = ReadWriteCn.BeginTransaction(IsoLevel)

            SelProc.ExecuteNonQuery()

            myTrans.Commit()


        Catch MySQLErr As MySqlException
            Try
                If Not (ReadOnlyCn.State = ConnectionState.Closed Or ReadOnlyCn.State = ConnectionState.Broken) Then myTrans.Rollback()
            Catch
            End Try
            If MySQLErr.Number = 1213 Or MySQLErr.Number = 1205 Then
                'MailErr("Присвоение значений максимальных синонимов", "Deadlock ")
                System.Threading.Thread.Sleep(1500)
                GoTo RestartMaxCodesSet
            End If
            MailErr("Присвоение значений максимальных синонимов", MySQLErr.Message)
            Addition = MySQLErr.Message
            UpdateType = 6
            ErrorFlag = True

        Catch err As Exception
            MailErr("Присвоение значений максимальных синонимов", err.Message)
            Addition = err.Message
            If Not (ReadOnlyCn.State = ConnectionState.Closed Or ReadOnlyCn.State = ConnectionState.Broken) Then myTrans.Rollback()
            UpdateType = 6
            ErrorFlag = True
        Finally

        End Try

    End Sub

    Private Sub GetMySQLFile(ByVal FileName As String, ByVal MyCommand As MySqlCommand, ByVal SQLText As String)
        Dim SQL As String = SQLText


        SQL &= " INTO OUTFILE 'C:/AFFiles/" & FileName & UserId & ".txt' FIELDS TERMINATED BY '" & Chr(159) & "' OPTIONALLY ENCLOSED BY '' ESCAPED BY '' LINES TERMINATED BY '" & Chr(161) & "'"
        MyCommand.CommandText = SQL
        MyCommand.ExecuteNonQuery()

        SyncLock (FilesForArchive)

            FilesForArchive.Enqueue(New FileForArchive(FileName, False))

        End SyncLock

    End Sub

    Private Sub GetMySQLFileWithDefault(ByVal FileName As String, ByVal MyCommand As MySqlCommand, ByVal SQLText As String)

        GetMySQLFileWithDefaultEx(FileName, MyCommand, SQLText, False)

    End Sub

    Private Sub GetMySQLFileWithDefaultEx(ByVal FileName As String, ByVal MyCommand As MySqlCommand, ByVal SQLText As String, ByVal SetCumulative As Boolean)
        Dim SQL As String = SQLText
        Dim oldCumulative As Boolean

        Try
            If SetCumulative And MyCommand.Parameters.Contains("?Cumulative") Then
                oldCumulative = MyCommand.Parameters("?Cumulative").Value
                MyCommand.Parameters("?Cumulative").Value = True
            End If

            SQL &= " INTO OUTFILE 'C:/AFFiles/" & FileName & UserId & ".txt' "
            MyCommand.CommandText = SQL
            MyCommand.ExecuteNonQuery()

        Finally
            If SetCumulative And MyCommand.Parameters.Contains("?Cumulative") Then
                MyCommand.Parameters("?Cumulative").Value = oldCumulative
            End If
        End Try

        SyncLock (FilesForArchive)

            FilesForArchive.Enqueue(New FileForArchive(FileName, False))

        End SyncLock

    End Sub

End Class

