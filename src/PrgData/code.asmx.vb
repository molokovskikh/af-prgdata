Imports System.Web.Services
Imports System.Threading
Imports System.IO
Imports System.Web
Imports System.Text
Imports System.Globalization
Imports log4net
Imports Common.MySql
Imports MySql.Data.MySqlClient
Imports MySQLResultFile = System.IO.File
Imports Counter.Counter
Imports PrgData.Common
Imports System.Net.Mail
Imports log4net.Core
Imports PrgData.Common.Orders
Imports System.Linq
Imports System.Collections.Generic
Imports Inforoom.Common
Imports SmartOrderFactory
Imports SmartOrderFactory.Domain
Imports Common.Models

<WebService(Namespace:="IOS.Service")> _
Public Class PrgDataEx
	Inherits System.Web.Services.WebService

	Const SevenZipExe As String = "C:\Program Files\7-Zip\7z.exe"

	Public Sub New()
		MyBase.New()

		InitializeComponent()

		Try
			ConnectionManager = New Global.Common.MySql.ConnectionManager()
			ArchiveHelper.SevenZipExePath = SevenZipExe
			ResultFileName = ServiceContext.GetResultPath()
		Catch ex As Exception
			Log.Error("Ошибка при инициализации приложения", ex)
		End Try

	End Sub

	Private ConnectionManager As Global.Common.MySql.ConnectionManager
	Private WithEvents SelProc As MySql.Data.MySqlClient.MySqlCommand
	Private WithEvents dataTable4 As System.Data.DataTable
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
	Private FileInfo As System.IO.FileInfo
	Private Запрос, UserName, MessageD, MailMessage As String
	'Строка с кодами прайс-листов, у которых отсутствуют синонимы на клиенте
	Private AbsentPriceCodes As String
	Private MessageH As String
	Private i As Integer
	Private ErrorFlag, Documents As Boolean
	Private Addition, ClientLog As String
	Private Reclame As Boolean
	Public ResultFileName As String
	Dim ArhiveStartTime As DateTime

	'Потоки
	Private ThreadZipStream As New Thread(AddressOf ZipStream)
	Private BaseThread As Thread 'New Thread(AddressOf BaseProc)
	Private ProtocolUpdatesThread As New Thread(AddressOf ProtocolUpdates)

	Private CurUpdTime, OldUpTime As DateTime
	Private LimitedCumulative As Boolean
	Private BuildNo, AllowBuildNo As Integer
	Private UpdateType As RequestType
	Private ResultLenght, OrderId As UInt32
	Dim CCode, UserId As UInt32
	Private SpyHostsFile, SpyAccount As Boolean
	Dim UpdateData As UpdateData
	Private UserHost, Message, ReclamePath As String
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
			Using connection = ConnectionManager.GetConnection()
				connection.Open()
				updateData = UpdateHelper.GetUpdateData(ConnectionManager.GetConnection(), HttpContext.Current.User.Identity.Name)
			End Using

			If updateData Is Nothing Then
				Throw New Exception("Клиент не найден")
			End If

			Dim mess As MailMessage = New MailMessage( _
			 New MailAddress("afmail@analit.net", String.Format("{0} [{1}]", updateData.ShortName, updateData.ClientId)), _
			 New MailAddress("tech@analit.net"))
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
			Return "Error=Не удалось отправить письмо. Попробуйте позднее."
		End Try
	End Function

	'Получает письмо и отправляет его
	<WebMethod()> _
	Public Function SendWaybills( _
 ByVal ClientId As UInt32, _
 ByVal ProviderIds As UInt64(), _
 ByVal FileNames As String(), _
 ByVal Waybills() As Byte) As String
		Try
			Dim updateData As UpdateData

			Using connection = New MySqlConnection(Settings.ConnectionString())
				connection.Open()

				updateData = UpdateHelper.GetUpdateData(connection, ServiceContext.GetUserName())

				If updateData Is Nothing Then
					Throw New Exception("Клиент не найден")
				End If

				Dim tmpWaybillFolder = Path.GetTempPath() + Path.GetFileNameWithoutExtension(Path.GetTempFileName())
				Dim tmpWaybillArchive = tmpWaybillFolder + "\waybills.7z"


				Directory.CreateDirectory(tmpWaybillFolder)

				Try

					Using fileWaybills As New FileStream(tmpWaybillArchive, FileMode.CreateNew)
						fileWaybills.Write(Waybills, 0, Waybills.Length)
					End Using

					If Not ArchiveHelper.TestArchive(tmpWaybillArchive) Then
						Throw New Exception("Полученный архив поврежден.")
					End If

					If GenerateDocsHelper.ParseWaybils(connection, updateData, ClientId, ProviderIds, FileNames, tmpWaybillArchive) Then
						Return "Status=0"
					Else
						Return "Status=2"
					End If


				Finally
					If Directory.Exists(tmpWaybillFolder) Then
						Try
							Directory.Delete(tmpWaybillFolder, True)
						Catch ex As Exception
							Log.Error("Ошибка при удалении временнной директории при обработке накладных", ex)
						End Try
					End If
				End Try
			End Using

		Catch ex As Exception
			Log.Error("Ошибка при загрузке накладных", ex)
			Return "Status=1"
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

		Return GetUserDataWithPriceCodes( _
		AccessTime, _
		GetEtalonData, _
		EXEVersion, _
		MDBVersion, _
		UniqueID, _
		WINVersion, _
		WINDesc, _
		WayBillsOnly, _
		Nothing, _
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

		Return GetUserDataWithPriceCodes( _
		AccessTime, _
		GetEtalonData, _
		EXEVersion, _
		MDBVersion, _
		UniqueID, _
		WINVersion, _
		WINDesc, _
		WayBillsOnly, _
		Nothing, _
		Nothing)

	End Function

    <WebMethod()> Public Function GetUserDataWithPriceCodes( _
 ByVal AccessTime As Date, _
 ByVal GetEtalonData As Boolean, _
 ByVal EXEVersion As String, _
 ByVal MDBVersion As Int16, _
 ByVal UniqueID As String, _
 ByVal WINVersion As String, _
 ByVal WINDesc As String, _
 ByVal WayBillsOnly As Boolean, _
 ByVal ClientHFile As String, _
 ByVal PriceCodes As UInt32()) As String

        Return InternalGetUserData( _
          AccessTime, _
          GetEtalonData, _
          EXEVersion, _
          MDBVersion, _
          UniqueID, _
          WINVersion, _
          WINDesc, _
          WayBillsOnly, _
          ClientHFile, _
          PriceCodes,
          False)
    End Function

    Private Function InternalGetUserData( _
     ByVal AccessTime As Date, _
     ByVal GetEtalonData As Boolean, _
     ByVal EXEVersion As String, _
     ByVal MDBVersion As Int16, _
     ByVal UniqueID As String, _
     ByVal WINVersion As String, _
     ByVal WINDesc As String, _
     ByVal WayBillsOnly As Boolean, _
     ByVal ClientHFile As String, _
     ByVal PriceCodes As UInt32(),
     ByVal ProcessBatch As Boolean) As String
        Dim ResStr As String = String.Empty
        Addition = " ОС: " & WINVersion & " " & WINDesc & "; "

        Try

            'Начинаем обычное обновление
            If (Not ProcessBatch) Then UpdateType = RequestType.GetData
            LimitedCumulative = False

            'Нет критических ошибок
            ErrorFlag = False

            'Только накладные
            Documents = WayBillsOnly

            'Присваиваем версии приложения и базы
            GED = GetEtalonData
            For i = 2 To 4
                If Left(Right(EXEVersion, i), 1) = "." Then Exit For
            Next
            BuildNo = CInt(Right(EXEVersion, i - 1))


            'Получаем код и параметры клиента клиента
            If (Not ProcessBatch) Then
                CCode = 0
                DBConnect()
                GetClientCode()
                Counter.Counter.TryLock(UserId, "GetUserData")
                FnCheckID(UniqueID)
            End If

            Dim helper = New UpdateHelper(UpdateData, ReadOnlyCn, ReadWriteCn)

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
                UpdateType = RequestType.Forbidden
                ErrorFlag = True
                GoTo endproc
            End If

            'Если с момента последнего обновления менее установленного времени
            If Not Documents Then

                If AllowBuildNo < BuildNo Then
                    Cm.Connection = ReadWriteCn
                    Cm.CommandText = "update usersettings.UserUpdateInfo set AFAppVersion=" & BuildNo & " where UserId=" & UserId
                    Dim transaction As MySqlTransaction
RestartInsertTrans:
                    Try

                        transaction = ReadWriteCn.BeginTransaction(IsoLevel)
                        Cm.Transaction = transaction
                        Cm.ExecuteNonQuery()
                        transaction.Commit()

                    Catch MySQLErr As MySqlException
                        ConnectionHelper.SafeRollback(transaction)
                        If ExceptionHelper.IsDeadLockOrSimilarExceptionInChain(MySQLErr) Then
                            System.Threading.Thread.Sleep(500)
                            GoTo RestartInsertTrans
                        End If
                        Me.Log.Error("Обновление номера версии", MySQLErr)
                    End Try


                End If

                'Если несовпадает время последнего обновления на клиете и сервере
                If OldUpTime <> AccessTime.ToLocalTime Then
                    If (BuildNo > 1079) And (Now.AddDays(-Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings("AccessTimeHistoryDepth"))) < AccessTime.ToLocalTime) Then
                        Try
                            Addition &= String.Format("Время обновления не совпало на клиенте и сервере, готовим частичное КО; Последнее обновление сервер {0}, клиент {1}", OldUpTime, AccessTime.ToLocalTime)
                            LimitedCumulative = True
                            OldUpTime = AccessTime.ToLocalTime()
                            helper.PrepareLimitedCumulative(OldUpTime)
                        Catch err As Exception
                            MailErr("Подготовка к частичному КО", err.ToString())
                            Addition = err.Message
                            UpdateType = RequestType.Error
                            ErrorFlag = True
                            GoTo endproc
                        End Try
                    Else
                        GED = True
                        Addition &= String.Format("Время обновления не совпало на клиенте и сервере, готовим КО; Последнее обновление сервер {0}, клиент {1}", OldUpTime, AccessTime.ToLocalTime)
                    End If
                End If


                'В зависимости от версии используем одну из процедур подготовки данных: для сервера Firebird и для сервера MySql
                If BuildNo > 716 Then
                    'Если производим обновление 945 версии на новую с поддержкой МНН или версия уже с поддержкой МНН, то добавляем еще два файла: мнн и описания
                    If ((BuildNo = 945) And UpdateData.EnableUpdate) Or (BuildNo > 945) Then
                    Else
                        If (BuildNo >= 829) And (BuildNo <= 837) And UpdateData.EnableUpdate Then
                            Addition &= "Производится обновление программы с 800-х версий на MySql; "
                        Else
                            'FileCount = 16
                        End If
                    End If
                    BaseThread = New Thread(AddressOf MySqlProc)
                Else
                    Dim CheckEnableUpdate As Boolean = Convert.ToBoolean(MySql.Data.MySqlClient.MySqlHelper.ExecuteScalar(ReadOnlyCn, "select EnableUpdate from retclientsset where clientcode=" & CCode))
                    If ((BuildNo >= 705) And (BuildNo <= 716)) And CheckEnableUpdate Then
                        BaseThread = New Thread(AddressOf MySqlProc)
                        'FileCount = 19
                        GED = True
                        Addition &= "Производится обновление программы с Firebird на MySql, готовим КО; "
                    Else
                        BaseThread = New Thread(AddressOf FirebirdProc)
                    End If
                End If

                'Готовим кумулятивное
                If GED Then

                    If (Not ProcessBatch) Then UpdateType = RequestType.GetCumulative
                    Cm.Connection = ReadWriteCn
                    Cm.CommandText = "update UserUpdateInfo set ReclameDate = NULL where UserId=" & UserId & "; "
                    Dim transaction = ReadWriteCn.BeginTransaction(IsoLevel)
                    Try
                        Cm.Transaction = transaction
                        Cm.ExecuteNonQuery()
                        transaction.Commit()
                    Catch ex As Exception
                        ConnectionHelper.SafeRollback(transaction)
                        Throw
                    End Try
                Else

                    'Сбрасываем коды прайс-листов, у которых нехватает синонимов
                    AbsentPriceCodes = String.Empty
                    If (PriceCodes IsNot Nothing) AndAlso (PriceCodes.Length > 0) AndAlso (PriceCodes(0) <> 0) Then
                        AbsentPriceCodes = PriceCodes(0).ToString
                        Dim I As Integer
                        For I = 1 To PriceCodes.Length - 1
                            AbsentPriceCodes &= "," & PriceCodes(I)
                        Next
                    End If
                    If Not String.IsNullOrEmpty(AbsentPriceCodes) Then ProcessResetAbsentPriceCodes(AbsentPriceCodes)
                End If

            End If

            If Documents Then

                CurUpdTime = Now()

                UpdateType = RequestType.GetDocs
                Try
                    MySQLFileDelete(ResultFileName & UserId & ".zip")
                    Log.DebugFormat("При подготовке документов удален предыдущий файл: {0}", ResultFileName & UserId & ".zip")
                Catch ex As Exception
                    Addition &= "Не удалось удалить предыдущие данные (получение только документов): " & ex.Message & "; "
                    UpdateType = RequestType.Forbidden
                    ErrorFlag = True
                    GoTo endproc
                End Try

            Else

                PackFinished = False

                If CkeckZipTimeAndExist(GetEtalonData) Then

                    Log.DebugFormat("Атрибуты подготовленного файла {1}: {0}", ResultFileName & UserId & ".zip", File.GetAttributes(ResultFileName & UserId & ".zip"))
                    If Not File.GetAttributes(ResultFileName & UserId & ".zip") = FileAttributes.NotContentIndexed Then

                        UpdateType = RequestType.ResumeData
                        NewZip = False
                        PackFinished = True
                        Log.DebugFormat("Файл будет докачиваться: {0}", ResultFileName & UserId & ".zip")
                        GoTo endproc

                    End If
                    Log.DebugFormat("Файл будет архивироваться заново: {0}", ResultFileName & UserId & ".zip")

                Else

                    Try

                        MySQLFileDelete(ResultFileName & UserId & ".zip")
                        Log.DebugFormat("Удалили предыдущие подготовленные данные: {0}", ResultFileName & UserId & ".zip")

                    Catch ex As Exception
                        Addition &= "Не удалось удалить предыдущие данные: " & ex.Message & "; "
                        UpdateType = RequestType.Forbidden
                        ErrorFlag = True
                        GoTo endproc
                    End Try

                    CurUpdTime = helper.GetCurrentUpdateDate(UpdateType)

                End If
            End If

            If Documents Then

                'Начинаем архивирование
                ThreadZipStream.Start()

            Else

                'Начинаем готовить данные
                BaseThread.Start()
                Thread.Sleep(500)

            End If

endproc:

            If Not PackFinished And (((BaseThread IsNot Nothing) AndAlso BaseThread.IsAlive) Or ThreadZipStream.IsAlive) And Not ErrorFlag Then

                'Если есть ошибка, прекращаем подготовку данных
                If ErrorFlag Then

                    If (BaseThread IsNot Nothing) AndAlso BaseThread.IsAlive Then BaseThread.Abort()
                    If ThreadZipStream.IsAlive Then ThreadZipStream.Abort()

                    PackFinished = True

                End If
                Thread.Sleep(1000)

                GoTo endproc

            ElseIf Not PackFinished And Not ErrorFlag And (UpdateType <> RequestType.Forbidden) And Not WayBillsOnly Then

                Addition &= "; Нет работающих потоков, данные не готовы."
                UpdateType = RequestType.Forbidden

                ErrorFlag = True

            End If

            If Len(Addition) = 0 Then Addition = MessageH & " " & MessageD

            If NewZip And Not ErrorFlag Then
                Dim ArhiveTS = Now().Subtract(ArhiveStartTime)

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

                ResStr = "URL=" & UpdateHelper.GetDownloadUrl() & "/GetFileHandler.ashx?Id=" & GUpdateId & ";New=" & NewZip & ";Cumulative=" & (UpdateType = RequestType.GetCumulative Or (UpdateType = RequestType.PostOrderBatch AndAlso GED))

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
            InternalGetUserData = ResStr
        Catch updateException As UpdateException
            UpdateType = updateException.UpdateType
            Addition += updateException.Addition
            ErrorFlag = True
            ProtocolUpdatesThread.Start()
            Return updateException.GetAnalitFMessage()
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
            InternalGetUserData = "Error=При подготовке обновления произошла ошибка.;Desc=Пожалуйста, повторите запрос данных через несколько минут."
        Finally
            If (Not ProcessBatch) Then
                DBDisconnect()
                ReleaseLock(UserId, "GetUserData")
            End If
        End Try

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

		Dim ArchCmd As MySqlCommand = New MySqlCommand()
		Dim ArchDA As MySqlDataAdapter = New MySqlDataAdapter()
		Try
			ThreadContext.Properties("user") = UpdateData.UserName


			ArhiveStartTime = Now()
			Dim SevenZipParam As String = " -mx7 -bd -slp -mmt=6 -w" & Path.GetTempPath
			Dim SevenZipTmpArchive, Name As String
			Dim xRow As DataRow
			Dim FileName, Вывод7Z, Ошибка7Z As String
			Dim zipfilecount = 0
			Dim xset As New DataTable
			Dim ArchTrans As MySqlTransaction
			Dim ef(), СписокФайлов() As String


			Using connection = ConnectionManager.GetConnection()
				connection.Open()


				Dim Pr As Process
				Dim startInfo As ProcessStartInfo



				If Reclame Then
					SevenZipTmpArchive = Path.GetTempPath() & "r" & UserId
					MySQLFileDelete(ResultFileName & "r" & UserId & ".zip")
				Else
					SevenZipTmpArchive = Path.GetTempPath() & UserId
					MySQLFileDelete(ResultFileName & UserId & ".zip")
					Log.DebugFormat("Удалили предыдущие подготовленные данные при начале архивирования: {0}", ResultFileName & UserId & ".zip")
				End If

				SevenZipTmpArchive &= "T.zip"
				MySQLFileDelete(SevenZipTmpArchive)


				'Если не реклама
				Dim helper = New UpdateHelper(UpdateData, Nothing, Nothing)
				If Not Reclame Then

					Try
						ArchCmd.Connection = connection
						ArchCmd.CommandText = helper.GetDocumentsCommand()
						ArchCmd.Parameters.Clear()
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
									xRow("Committed") = False

									startInfo = New ProcessStartInfo(SevenZipExe)
									startInfo.CreateNoWindow = True
									startInfo.RedirectStandardOutput = True
									startInfo.RedirectStandardError = True
									startInfo.UseShellExecute = False
									startInfo.StandardOutputEncoding = System.Text.Encoding.GetEncoding(866)

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

											Counter.Utils.Mail("Архивирование документов", "Вышли из 7Z с ошибкой: " & ": " & _
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

							If BuildNo >= 1027 And DS.Tables("ProcessingDocuments").Rows.Count > 0 Then
								MySQLFileDelete(MySqlFilePath & "DocumentHeaders" & UserId & ".txt")
								MySQLFileDelete(MySqlFilePath & "DocumentBodies" & UserId & ".txt")

								'Необходима задержка после удаления файлов накладных, т.к. файлы удаляются не сразу
								ShareFileHelper.WaitDeleteFile(MySqlFilePath & "DocumentHeaders" & UserId & ".txt")
								ShareFileHelper.WaitDeleteFile(MySqlFilePath & "DocumentBodies" & UserId & ".txt")

								Dim ids As String = String.Empty
								For Each documentRow As DataRow In DS.Tables("ProcessingDocuments").Rows
									If String.IsNullOrEmpty(ids) Then
										ids = documentRow("DocumentId").ToString()
									Else
										ids += ", " & documentRow("DocumentId").ToString()
									End If
								Next

								GetMySQLFileWithDefaultEx("DocumentHeaders", ArchCmd, helper.GetDocumentHeadersCommand(ids), False, False)
								GetMySQLFileWithDefaultEx("DocumentBodies", ArchCmd, helper.GetDocumentBodiesCommand(ids), False, False)

								ShareFileHelper.WaitFile(MySqlFilePath & "DocumentHeaders" & UserId & ".txt")
								ShareFileHelper.WaitFile(MySqlFilePath & "DocumentBodies" & UserId & ".txt")

								Pr = New Process

								startInfo = New ProcessStartInfo(SevenZipExe)
								startInfo.CreateNoWindow = True
								startInfo.RedirectStandardOutput = True
								startInfo.RedirectStandardError = True
								startInfo.UseShellExecute = False
								startInfo.StandardOutputEncoding = System.Text.Encoding.GetEncoding(866)
								startInfo.Arguments = String.Format(" a ""{0}"" ""{1}"" {2}", SevenZipTmpArchive, MySqlFilePath & "Document*" & UserId & ".txt", SevenZipParam)
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

								MySQLFileDelete(MySqlFilePath & "DocumentHeaders" & UserId & ".txt")
								MySQLFileDelete(MySqlFilePath & "DocumentBodies" & UserId & ".txt")

								ShareFileHelper.WaitDeleteFile(MySqlFilePath & "DocumentHeaders" & UserId & ".txt")
								ShareFileHelper.WaitDeleteFile(MySqlFilePath & "DocumentBodies" & UserId & ".txt")
							End If

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
							Exit Sub

						Else

							MessageH = "Новых файлов документов нет."
							Addition &= " Нет новых документов"
							ErrorFlag = True
							PackFinished = True
							Exit Sub

						End If

					End If




					'Если не документы
					If Not Documents Then

						'Архивирование обновления программы
						Try
							ArchCmd.CommandText = "select EnableUpdate from retclientsset where clientcode=" & CCode
							EnableUpdate = Convert.ToBoolean(ArchCmd.ExecuteScalar)

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


						If FileForArchive.FileName.StartsWith("EndOfFiles.txt") Then
							If Reclame Then

								'ArchCmd.CommandText &= "1"
								File.Move(SevenZipTmpArchive, ResultFileName & "r" & UserId & ".zip")

							Else

								'ArchCmd.CommandText &= "0"
								File.Move(SevenZipTmpArchive, ResultFileName & UserId & ".zip")
								Log.DebugFormat("Закончено архивирование файла: {0}", ResultFileName & UserId & ".zip")
                                If (UpdateType = RequestType.GetCumulative Or (UpdateType = RequestType.PostOrderBatch AndAlso GED)) Then
                                    File.SetAttributes(ResultFileName & UserId & ".zip", FileAttributes.Normal)
                                    Log.DebugFormat("Для файла выставлен атрибут Normal: {0}", ResultFileName & UserId & ".zip")
                                End If

								FileInfo = New FileInfo(ResultFileName & UserId & ".zip")
								ResultLenght = Convert.ToUInt32(FileInfo.Length)

							End If
							'ArchCmd.ExecuteNonQuery()

							PackFinished = True
							Exit Sub
						End If

						If Reclame Then
							FileName = ReclamePath & FileForArchive.FileName
                        Else

                            If FileForArchive.FileType Then
                                FileName = FileForArchive.FileName
                            Else
                                FileName = MySqlFilePath & FileForArchive.FileName & UserId & ".txt"
                            End If


                            ShareFileHelper.WaitFile(FileName)
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

						'If zipfilecount >= FileCount Then

						'    'ArchCmd.CommandText = "delete from ready_client_files where clientcode=" & CCode
						'    'ArchCmd.CommandText &= " and reclame="

						'Else

						'End If

						GoTo StartZipping

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
						UpdateType = RequestType.Error
						MailErr("Архивирование", ex.Source & ": " & ex.ToString())
					End If
					Addition &= " Архивирование: " & ex.ToString() & "; "

				Catch Unhandled As Exception

					ErrorFlag = True
					UpdateType = RequestType.Error
					If Not Pr Is Nothing Then
						If Not Pr.HasExited Then Pr.Kill()
						Pr.WaitForExit()
					End If
					Addition &= " Архивирование: " & Unhandled.ToString()
					MySQLFileDelete(SevenZipTmpArchive)
					MailErr("Архивирование", Unhandled.Source & ": " & Unhandled.ToString())
					Addition &= " Архивирование: " & Unhandled.ToString() & "; "
					'If Not ArchTrans Is Nothing Then ArchTrans.Rollback()
				End Try
			End Using

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
		Try

			UpdateType = RequestType.CommitExchange

			DBConnect()
			GetClientCode()
			Counter.Counter.TryLock(UserId, "MaxSynonymCode")

			If Not WayBillsOnly Or Not File.GetAttributes(ResultFileName & UserId & ".zip") = FileAttributes.NotContentIndexed Then

				AbsentPriceCodes = String.Empty
				If (PriceCode IsNot Nothing) AndAlso (PriceCode.Length > 0) AndAlso (PriceCode(0) <> 0) Then
					AbsentPriceCodes = PriceCode(0).ToString
					Dim I As Integer
					For I = 1 To PriceCode.Length - 1
						AbsentPriceCodes &= "," & PriceCode(I)
					Next
				End If

				ProcessOldCommit(AbsentPriceCodes)

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

						Dim masterUpdateTime As Object = MySql.Data.MySqlClient.MySqlHelper.ExecuteScalar(ReadWriteCn, "select UncommitedUpdateDate from UserUpdateInfo  where UserId=" & UserId & "; ")
						Me.Log.DebugFormat("MaxSynonymCode: slave UncommitedUpdateDate {0}  master UncommitedUpdateDate {1}", UpdateTime, masterUpdateTime)
						If IsDate(masterUpdateTime) And (CType(masterUpdateTime, DateTime) > UpdateTime) Then
							UpdateTime = CType(masterUpdateTime, DateTime)
							Me.Log.Debug("MaxSynonymCode: дата, выбранная из мастера, больше, чем дата из slave")
						End If
					End If

				Catch ex As Exception
					MailErr("Выборка даты обновления ", ex.Message & ex.Source)
					UpdateTime = Now().ToUniversalTime
				End Try

				MaxSynonymCode = UpdateTime.ToUniversalTime

				Try

					Cm.CommandText = "select SaveAFDataFiles from UserUpdateInfo  where UserId=" & UserId & "; "
					If Convert.ToBoolean(Cm.ExecuteScalar) Then
						If Not Directory.Exists(ResultFileName & "\Archive\" & UserId) Then Directory.CreateDirectory(ResultFileName & "\Archive\" & UserId)
						File.Copy(ResultFileName & UserId & ".zip", ResultFileName & "\Archive\" & UserId & "\" & UpdateId & ".zip")
					End If

					MySQLFileDelete(ResultFileName & UserId & ".zip")
					Me.Log.DebugFormat("Удалили подготовленные данные после подтверждения: {0}", ResultFileName & UserId & ".zip")
					MySQLFileDelete(ResultFileName & "r" & UserId & "Old.zip")

				Catch ex As Exception
					Me.Log.Error("Ошибка при сохранении подготовленных данных", ex)
				End Try
			ProtocolUpdatesThread.Start()
		Catch e As Exception
			Me.Log.Error(String.Format("Ошибка при подтверждении обновления, вернул {0}, дальше КО", Now().ToUniversalTime), e)
			Return Now().ToUniversalTime
		Finally
			ReleaseLock(UserId, "MaxSynonymCode")
			DBDisconnect()
		End Try

	End Function

	<WebMethod()> _
	Public Function CommitExchange( _
	ByVal UpdateId As UInt32, _
	ByVal WayBillsOnly As Boolean) As Date
		Dim UpdateTime As Date
		Cm.Transaction = Nothing
		GUpdateId = UpdateId

		Try
			UpdateType = RequestType.CommitExchange

			DBConnect()
			GetClientCode()
			Counter.Counter.TryLock(UserId, "CommitExchange")

			If Not WayBillsOnly Or Not File.GetAttributes(ResultFileName & UserId & ".zip") = FileAttributes.NotContentIndexed Then
				' Здесь сбрасывались коды прайс-листов
				ProcessCommitExchange()
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

					Dim masterUpdateTime As Object = MySql.Data.MySqlClient.MySqlHelper.ExecuteScalar(ReadWriteCn, "select UncommitedUpdateDate from UserUpdateInfo  where UserId=" & UserId & "; ")
					Me.Log.DebugFormat("CommitExchange: slave UncommitedUpdateDate {0}  master UncommitedUpdateDate {1}", UpdateTime, masterUpdateTime)
					If IsDate(masterUpdateTime) And (CType(masterUpdateTime, DateTime) > UpdateTime) Then
						UpdateTime = CType(masterUpdateTime, DateTime)
						Me.Log.Debug("CommitExchange: дата, выбранная из мастера, больше, чем дата из slave")
					End If
				End If

			Catch ex As Exception
				MailErr("Выборка даты обновления ", ex.Message & ex.Source)
				UpdateTime = Now().ToUniversalTime
			End Try

			CommitExchange = UpdateTime.ToUniversalTime

			Try

				Cm.CommandText = "select SaveAFDataFiles from UserUpdateInfo  where UserId=" & UserId & "; "
				If Convert.ToBoolean(Cm.ExecuteScalar) Then
					If Not Directory.Exists(ResultFileName & "\Archive\" & UserId) Then Directory.CreateDirectory(ResultFileName & "\Archive\" & UserId)
					File.Copy(ResultFileName & UserId & ".zip", ResultFileName & "\Archive\" & UserId & "\" & UpdateId & ".zip")
				End If

				MySQLFileDelete(ResultFileName & UserId & ".zip")
				Me.Log.DebugFormat("Удалили подготовленные данные после подтверждения: {0}", ResultFileName & UserId & ".zip")
				MySQLFileDelete(ResultFileName & "r" & UserId & "Old.zip")

			Catch ex As Exception
				'MailErr("Удаление полученных файлов;", ex.Message)
			End Try
			ProtocolUpdatesThread.Start()
		Catch e As Exception
			Me.Log.Error("Ошибка при подтверждении обновления", e)
			CommitExchange = Now().ToUniversalTime
		Finally
			DBDisconnect()
			ReleaseLock(UserId, "CommitExchange")
		End Try
	End Function

	<WebMethod()> _
	Public Function SendClientLog( _
  ByVal UpdateId As UInt32, _
  ByVal Log As String _
  ) As String

		Try
			DBConnect()
			GetClientCode()
			Counter.Counter.TryLock(UserId, "SendClientLog")
			Try
				MySql.Data.MySqlClient.MySqlHelper.ExecuteNonQuery( _
				 ReadWriteCn, _
				 "update logs.AnalitFUpdates set Log=?Log  where UpdateId=?UpdateId", _
				 New MySqlParameter("?Log", Log), _
				 New MySqlParameter("?UpdateId", UpdateId))
			Catch ex As Exception
				Me.Log.Error("Ошибка при сохранении лога клиента", ex)
			End Try
			SendClientLog = "OK"
		Catch e As Exception
			Me.Log.Error("Ошибка при сохранении лога клиента", e)
			SendClientLog = "Error"
		Finally
			DBDisconnect()
			ReleaseLock(UserId, "SendClientLog")
		End Try
	End Function

	Private Sub GetClientCode()
		ThreadContext.Properties("user") = UserName
		UserName = ServiceContext.GetUserName()
		If Left(UserName, 7) = "ANALIT\" Then
			UserName = Mid(UserName, 8)
		End If
		UpdateData = UpdateHelper.GetUpdateData(ReadOnlyCn, UserName)

		If UpdateData Is Nothing Then
			Throw New UpdateException("Доступ закрыт.", "Пожалуйста, обратитесь в АК «Инфорум».[1]", "Для логина " & UserName & " услуга не предоставляется; ", RequestType.Forbidden)
		End If

		CCode = UpdateData.ClientId
		UserId = UpdateData.UserId
		CheckID = UpdateData.CheckCopyId
		Message = UpdateData.Message
		OldUpTime = UpdateData.OldUpdateTime
		UncDT = UpdateData.UncommitedUpdateTime
		SpyHostsFile = UpdateData.Spy
		SpyAccount = UpdateData.SpyAccount
		ThreadContext.Properties("user") = UpdateData.UserName

		With Cm
			.Parameters.Add(New MySqlParameter("?UserName", MySqlDbType.VarString))
			.Parameters("?UserName").Value = UserName

			.Parameters.Add(New MySqlParameter("?ClientCode", MySqlDbType.Int32))
			.Parameters("?ClientCode").Value = CCode
		End With

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
	End Sub

	Private Function DBConnect()
		UserHost = ServiceContext.GetUserHost()
		Try
			ReadOnlyCn = ConnectionManager.GetConnection()
			ReadOnlyCn.Open()

			ReadWriteCn = New MySqlConnection
			ReadWriteCn.ConnectionString = Settings.ConnectionString()
			ReadWriteCn.Open()

			Return True
		Catch ex As Exception
			DBDisconnect()
			Throw
		End Try
	End Function

	Private Sub DBDisconnect()
		Try
			If Not ReadOnlyCn Is Nothing Then ReadOnlyCn.Dispose()
		Catch e As Exception
			Log.Error("Ошибка при закритии соединения", e)
		End Try

		Try
			If Not ReadWriteCn Is Nothing Then ReadWriteCn.Dispose()
		Catch e As Exception
			Log.Error("Ошибка при закритии соединения", e)
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
			DBConnect()
			GetClientCode()
			Counter.Counter.TryLock(UserId, "PostOrder")

			Dim helper = New OrderHelper(UpdateData, ReadOnlyCn, ReadWriteCn)

			If ServerOrderId = 0 Then

				FnCheckID(UniqueID, "PostOrder")

				With Cm
					.Connection = ReadOnlyCn
					.Transaction = Nothing
				End With

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
					Throw New UpdateException("Превышен недельный лимит заказа (уже заказано на " & WeeklySumOrder & " руб.)", "", RequestType.Error)
				End If

				'начинаем проверять минимальный заказ
				Dim minReq = helper.GetMinReq(ClientCode, RegionCode, PriceCode)

				If Not minReq Is Nothing And minReq.ControlMinReq And minReq.MinReq > 0 Then
					SumOrder = 0
					For it = 0 To Cost.Length - 1
						SumOrder += Convert.ToUInt32(Math.Round(Quantity(it) * Cost(it), 0))
					Next
					If SumOrder < minReq.MinReq Then
						Throw New UpdateException("Сумма заказа меньше минимально допустимой.", "Поставщик отказал в приеме заказа.", RequestType.Forbidden)
					End If
				End If

				With Cm

					.Connection = ReadWriteCn

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

			Dim transaction As MySqlTransaction
			Try

				transaction = ReadWriteCn.BeginTransaction(IsoLevel)

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
					transaction.Commit()
					ResultLenght = Convert.ToUInt32(OID)
					UpdateType = RequestType.SendOrders
				Else
					transaction.Rollback()
					OID = 0
				End If

			Catch ex As Exception
				ConnectionHelper.SafeRollback(transaction)
				If ExceptionHelper.IsDeadLockOrSimilarExceptionInChain(ex) Then
					Thread.Sleep(500)
					LockCount += 1
					GoTo RestartInsertTrans
				End If
				Throw
			End Try
		Catch ex As UpdateException
			Return ex.GetAnalitFMessage()
		Catch ex As Exception
			Log.Error("Ошибка при отправке заказа", ex)
			Return "Error=Отправка заказов завершилась неудачно.;Desc=Пожалуйста повторите попытку через несколько минут."
		Finally
			ReleaseLock(UserId, "PostOrder")
			DBDisconnect()
		End Try

		If ErrorFlag Or (UpdateType > RequestType.SendOrders) Then
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

		'генерируем массив наценок поставщика размером с общее кол-во позиций в заказах, значения в массиве - пустые строчки ("")
		Dim SupplierPriceMarkup As IEnumerable(Of String) = Enumerable.Repeat("", New List(Of UInt16)(RowCount).Sum(Function(item) item))
		Dim DelayOfPayment As IEnumerable(Of String) = Enumerable.Repeat("", OrderCount)

		Dim CoreQuantity As IEnumerable(Of String) = Enumerable.Repeat("", New List(Of UInt16)(RowCount).Sum(Function(item) item))
		Dim Unit As IEnumerable(Of String) = Enumerable.Repeat("", New List(Of UInt16)(RowCount).Sum(Function(item) item))
		Dim Volume As IEnumerable(Of String) = Enumerable.Repeat("", New List(Of UInt16)(RowCount).Sum(Function(item) item))
		Dim Note As IEnumerable(Of String) = Enumerable.Repeat("", New List(Of UInt16)(RowCount).Sum(Function(item) item))
		Dim Period As IEnumerable(Of String) = Enumerable.Repeat("", New List(Of UInt16)(RowCount).Sum(Function(item) item))
		Dim Doc As IEnumerable(Of String) = Enumerable.Repeat("", New List(Of UInt16)(RowCount).Sum(Function(item) item))
		Dim RegistryCost As IEnumerable(Of String) = Enumerable.Repeat("", New List(Of UInt16)(RowCount).Sum(Function(item) item))
		Dim VitallyImportant As IEnumerable(Of Boolean) = Enumerable.Repeat(False, New List(Of UInt16)(RowCount).Sum(Function(item) item))
		Dim RetailMarkup As IEnumerable(Of String) = Enumerable.Repeat("", New List(Of UInt16)(RowCount).Sum(Function(item) item))
		Dim ProducerCost As IEnumerable(Of String) = Enumerable.Repeat("", New List(Of UInt16)(RowCount).Sum(Function(item) item))
		Dim NDS As IEnumerable(Of String) = Enumerable.Repeat("", New List(Of UInt16)(RowCount).Sum(Function(item) item))

		Return _
		 PostSomeOrdersFull( _
		  UniqueID, _
		  ForceSend, _
		  UseCorrectOrders, _
		  ClientCode, _
		  OrderCount, _
		  ClientOrderID, _
		  PriceCode, _
		  RegionCode, _
		  PriceDate, _
		  ClientAddition, _
		  RowCount, _
		  DelayOfPayment.ToArray(), _
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
		  LeaderMinPriceCode, _
		  SupplierPriceMarkup.ToArray(), _
		  CoreQuantity.ToArray(), _
		  Unit.ToArray(), _
		  Volume.ToArray(), _
		  Note.ToArray(), _
		  Period.ToArray(), _
		  Doc.ToArray(), _
		  RegistryCost.ToArray(), _
		  VitallyImportant.ToArray(), _
		  RetailMarkup.ToArray(), _
		  ProducerCost.ToArray(), _
		  NDS.ToArray() _
		  )
	End Function

	'Отправляем несколько заказов скопом и по ним все формируем ответ
	<WebMethod()> _
	Public Function PostSomeOrdersFull( _
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
  ByVal DelayOfPayment As String(), _
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
  ByVal LeaderMinPriceCode As String(), _
  ByVal SupplierPriceMarkup As String(), _
  ByVal CoreQuantity As String(), _
  ByVal Unit As String(), _
  ByVal Volume As String(), _
  ByVal Note As String(), _
  ByVal Period As String(), _
  ByVal Doc As String(), _
  ByVal RegistryCost As String(), _
  ByVal VitallyImportant As Boolean(), _
  ByVal RetailMarkup As String(), _
  ByVal ProducerCost As String(), _
  ByVal NDS As String()) As String

		Dim ResStr As String = String.Empty

		Try
			DBConnect()
			GetClientCode()
			Counter.Counter.TryLock(UserId, "PostOrder")
			FnCheckID(UniqueID, "PostOrder")

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
			 LeaderMinPriceCode, _
			 SupplierPriceMarkup, _
			 DelayOfPayment, _
			 CoreQuantity, _
			 Unit, _
			 Volume, _
			 Note, _
			 Period, _
			 Doc, _
			 RegistryCost, _
			 VitallyImportant, _
			 RetailMarkup, _
			 ProducerCost, _
			 NDS _
			)

			Return helper.PostSomeOrders()
		Catch ex As UpdateException
			Return ex.GetAnalitFMessage()
		Catch ex As Exception
			'Log.Error("Ошибка при отправке заказа", ex)
			LogRequestHelper.MailWithRequest("Ошибка при отправке заказа" & vbCrLf & ex.ToString())
			Return "Error=Отправка заказов завершилась неудачно.;Desc=Пожалуйста повторите попытку через несколько минут."
		Finally
			ReleaseLock(UserId, "PostOrder")
			DBDisconnect()
		End Try

    End Function

    <WebMethod()> Public Function PostOrderBatch( _
        ByVal AccessTime As Date, _
        ByVal GetEtalonData As Boolean, _
        ByVal EXEVersion As String, _
        ByVal MDBVersion As Int16, _
        ByVal UniqueID As String, _
        ByVal WINVersion As String, _
        ByVal WINDesc As String, _
        ByVal PriceCodes As UInt32(), _
        ByVal ClientId As UInt32, _
        ByVal BatchFile As String, _
        ByVal MaxOrderId As UInt32, _
        ByVal MaxOrderListId As UInt32, _
        ByVal MaxBatchId As UInt32) As String

        Dim ResStr As String = String.Empty

        Try
            DBConnect()
            GetClientCode()
            Counter.Counter.TryLock(UserId, "PostOrderBatch")
            FnCheckID(UniqueID, "PostOrderBatch")

            UpdateType = RequestType.PostOrderBatch

            Dim helper = New SmartOrderHelper(UpdateData, ReadOnlyCn, ReadWriteCn, ClientId, MaxOrderId, MaxOrderListId, MaxBatchId)

            Try
                helper.PrepareBatchFile(BatchFile)

                helper.ProcessBatchFile()

                AddFileToQueue(helper.BatchReportFileName)
                AddFileToQueue(helper.BatchOrderFileName)
                AddFileToQueue(helper.BatchOrderItemsFileName)

                'ResStr = "Error=sdlsjdslj"
                ResStr = InternalGetUserData(AccessTime, GetEtalonData, EXEVersion, MDBVersion, UniqueID, WINVersion, WINDesc, False, Nothing, PriceCodes, True)

            Finally
                helper.DeleteTemporaryFiles()
            End Try


            Return ResStr
        Catch ex As UpdateException
            ProtocolUpdatesThread.Start()
            Return ex.GetAnalitFMessage()
        Catch ex As Exception
            'Log.Error("Ошибка при отправке заказа", ex)
            LogRequestHelper.MailWithRequest("Ошибка при отправке дефектуры" & vbCrLf & ex.ToString())
            Return "Error=Отправка дефектуры завершилась неудачно.;Desc=Пожалуйста повторите попытку через несколько минут."
        Finally
            ReleaseLock(UserId, "PostOrderBatch")
            DBDisconnect()
        End Try
    End Function



    Public Sub MailErr(ByVal ErrSource As String, ByVal ErrDesc As String)
        Counter.Utils.Mail("Клиент: " & CCode & Chr(10) & Chr(13) & "Процесс: " & ErrSource & Chr(10) & Chr(13) & "Описание: " & ErrDesc, "Ошибка в сервисе подготовки данных")
    End Sub


    Private Sub FnCheckID(ByVal uin As String)
        FnCheckID(uin, "")
    End Sub

    Private Sub FnCheckID(ByVal uin As String, ByVal method As String)
RePost:
        Dim knownUin As String
        Dim command = New MySqlCommand()
        Dim transaction = ReadWriteCn.BeginTransaction(IsoLevel)
        Try
            command.Transaction = transaction
            command.Connection = ReadWriteCn

            command.CommandText = "select AFCopyId from UserUpdateInfo where UserId=" & UserId
            Using SQLdr As MySqlDataReader = command.ExecuteReader
                SQLdr.Read()
                knownUin = SQLdr.GetString(0)
                SQLdr.Close()
            End Using

            If knownUin.Length < 1 Then
                command.CommandText = "update UserUpdateInfo set AFCopyId='" & uin & "' where UserId=" & UserId

                command.ExecuteNonQuery()
            ElseIf knownUin <> uin Then
                Dim description = "Обновление программы на данном компьютере запрещено."
                If method = "PostOrder" Then
                    description = "Отправка заказов на данном компьютере запрещена."
                End If
                Throw New UpdateException(description,
                   "Пожалуйста, обратитесь в АК «Инфорум».[2]",
                   "Несоответствие UIN.",
                   RequestType.Forbidden)
            End If

            transaction.Commit()
        Catch ex As Exception
            ConnectionHelper.SafeRollback(transaction)
            If ExceptionHelper.IsDeadLockOrSimilarExceptionInChain(ex) Then
                Thread.Sleep(500)
                GoTo RePost
            End If
            Throw
        End Try
    End Sub

    Private Sub ProtocolUpdates()
        Dim transaction As MySqlTransaction
        Dim LogCb As New MySqlCommandBuilder
        Dim LogDA As New MySqlDataAdapter
        Dim LogCm As New MySqlCommand
        Dim NoNeedProcessDocuments As Boolean = False

        Using connection = New MySqlConnection
            Try
                ThreadContext.Properties("user") = UpdateData.UserName

                connection.ConnectionString = Settings.ConnectionString
                connection.Open()

                LogCm.Connection = connection
                LogCb.DataAdapter = DA

                If (BaseThread IsNot Nothing) AndAlso BaseThread.IsAlive Then BaseThread.Join()
                If ThreadZipStream.IsAlive Then ThreadZipStream.Join()

                If UserId < 1 Then
                    GetClientCode()
                    NoNeedProcessDocuments = True
                End If

                If (UpdateType = RequestType.GetData) _
                 Or (UpdateType = RequestType.GetCumulative) _
                 Or (UpdateType = RequestType.PostOrderBatch) _
                 Or (UpdateType = RequestType.Forbidden) _
                 Or (UpdateType = RequestType.Error) _
                 Or (UpdateType = RequestType.GetDocs) Then

                    transaction = connection.BeginTransaction(IsoLevel)

                    If CurUpdTime < Now().AddDays(-1) Then CurUpdTime = Now()

                    With LogCm

                        .CommandText = "insert into `logs`.`AnalitFUpdates`(`RequestTime`, `UpdateType`, `UserId`, `AppVersion`,  `ResultSize`, `Addition`) values(?UpdateTime, ?UpdateType, ?UserId, ?exeversion,  ?Size, ?Addition); "
                        .CommandText &= "select last_insert_id()"


                        .Transaction = transaction
                        .Parameters.Add(New MySqlParameter("?UserId", UserId))

                        .Parameters.Add(New MySqlParameter("?ClientHost", UserHost))

                        If (UpdateType = RequestType.GetData) And LimitedCumulative Then
                            .Parameters.Add(New MySqlParameter("?UpdateType", Convert.ToInt32(RequestType.GetCumulative)))
                        Else
                            .Parameters.Add(New MySqlParameter("?UpdateType", Convert.ToInt32(UpdateType)))
                        End If

                        .Parameters.Add(New MySqlParameter("?EXEVersion", BuildNo))

                        .Parameters.Add(New MySqlParameter("?Size", ResultLenght))

                        .Parameters.Add(New MySqlParameter("?Addition", Addition))

                        .Parameters.Add(New MySqlParameter("?UpdateTime", CurUpdTime))

                    End With

PostLog:

                    GUpdateId = Convert.ToUInt32(LogCm.ExecuteScalar)


                    transaction.Commit()

                    If DS.Tables("ProcessingDocuments").Rows.Count > 0 Then
                        Dim DocumentsIdRow As DataRow
                        Dim DocumentsProcessingCommandBuilder As New MySqlCommandBuilder

                        For Each DocumentsIdRow In DS.Tables("ProcessingDocuments").Rows
                            DocumentsIdRow.Item("UpdateId") = GUpdateId
                        Next

                        LogDA.SelectCommand = New MySqlCommand
                        LogDA.SelectCommand.Connection = connection
                        LogDA.SelectCommand.CommandText = "" & _
                          "SELECT  * " & _
                          "FROM    AnalitFDocumentsProcessing limit 0"

                        DocumentsProcessingCommandBuilder.DataAdapter = LogDA
                        LogDA.InsertCommand = DocumentsProcessingCommandBuilder.GetInsertCommand
                        LogDA.InsertCommand.Transaction = transaction

                        transaction = connection.BeginTransaction(IsoLevel)
                        LogDA.Update(DS.Tables("ProcessingDocuments"))
                        transaction.Commit()

                    End If

                    DS.Tables.Clear()

                End If
                If (UpdateType = RequestType.ResumeData) Then

                    LogCm.CommandText = "" & _
                       "SELECT  MAX(UpdateId) " & _
                      "FROM    `logs`.AnalitFUpdates " & _
                      "WHERE   UpdateType IN (1, 2) " & _
                       "    AND `Commit`    =0 " & _
                       "    AND UserId  =" & UserId

                    GUpdateId = Convert.ToUInt32(LogCm.ExecuteScalar)
                    If GUpdateId < 1 Then GUpdateId = Nothing
                End If

                If Not NoNeedProcessDocuments Then

                    If (UpdateType = RequestType.CommitExchange) Then
                        Dim СписокФайлов() As String

                        transaction = connection.BeginTransaction(IsoLevel)
                        LogCm.CommandText = "update `logs`.`AnalitFUpdates` set Commit=1, Log=?Log, Addition=concat(Addition, ifnull(?Addition, ''))  where UpdateId=" & GUpdateId

                        LogCm.Parameters.Add(New MySqlParameter("?Log", MySqlDbType.VarString))
                        LogCm.Parameters("?Log").Value = ClientLog

                        LogCm.Parameters.Add(New MySqlParameter("?Addition", MySqlDbType.VarString))
                        LogCm.Parameters("?Addition").Value = Addition

                        LogCm.ExecuteNonQuery()

                        Dim helper = New UpdateHelper(UpdateData, ReadOnlyCn, ReadWriteCn)

                        LogCm.CommandText = "delete from future.ClientToAddressMigrations where UserId = " & UpdateData.UserId
                        LogCm.ExecuteNonQuery()

                        Dim processedDocuments = helper.GetProcessedDocuments(GUpdateId)

                        If processedDocuments.Rows.Count > 0 Then

                            If Not UpdateData.IsFutureClient Then
                                Dim DocumentsIdRow As DataRow

                                For Each DocumentsIdRow In processedDocuments.Rows

                                    СписокФайлов = Directory.GetFiles(ПутьКДокументам & _
                                       DocumentsIdRow.Item("ClientCode").ToString & _
                                       "\" & _
                                       CType(DocumentsIdRow.Item("DocumentType"), ТипДокумента).ToString, _
                                       DocumentsIdRow.Item("DocumentId").ToString & "_*")

                                    MySQLResultFile.Delete(СписокФайлов(0))

                                Next

                            End If
                            LogCm.CommandText = helper.GetConfirmDocumentsCommnad(GUpdateId)
                            LogCm.ExecuteNonQuery()

                        End If

                        transaction.Commit()
                    End If

                End If
            Catch ex As Exception
                ConnectionHelper.SafeRollback(transaction)
                GUpdateId = Nothing
                If ExceptionHelper.IsDeadLockOrSimilarExceptionInChain(ex) Then
                    Thread.Sleep(500)
                    GoTo PostLog
                End If
                Log.Error("Запись лога", ex)
            End Try
        End Using
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
        Me.OrderInsertCm = New MySql.Data.MySqlClient.MySqlCommand
        Me.OrderInsertDA = New MySql.Data.MySqlClient.MySqlDataAdapter
        Me.SelProc = New MySql.Data.MySqlClient.MySqlCommand
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

        If Convert.ToUInt32(Cm.ExecuteScalar) < 1 Then
            Log.DebugFormat("Не найден предыдущий неподтвержденный запрос данных: {0}", UserId)
            Return False
        Else
            Log.DebugFormat("Найден предыдущий неподтвержденный запрос данных: {0}", UserId)
        End If


        FileInfo = New FileInfo(ResultFileName & UserId & ".zip")

        If FileInfo.Exists Then

            Log.DebugFormat("Файл с подготовленными данными существует: {0}", ResultFileName & UserId & ".zip")
            CkeckZipTimeAndExist = _
             (Date.UtcNow.Subtract(UncDT.ToUniversalTime).TotalHours < 1 And Not GetEtalonData) _
             Or (OldUpTime.Year = 2003 And DateTime.UtcNow.Subtract(UncDT.ToUniversalTime).TotalHours < 8) _
             Or (File.GetAttributes(ResultFileName & UserId & ".zip") = FileAttributes.Normal And GetEtalonData)

            Log.DebugFormat( _
             "Результат проверки CkeckZipTimeAndExist: {0}  " & vbCrLf & _
             "Параметры " & vbCrLf & _
             "GetEtalonData  : {1}" & vbCrLf & _
             "UncDT          : {2}" & vbCrLf & _
             "OldUpTime      : {3}" & vbCrLf & _
             "FileName       : {4}" & vbCrLf & _
             "FileAttributes : {5}" & vbCrLf & _
             "Expression1    : {6}" & vbCrLf & _
             "Expression2    : {7}" & vbCrLf & _
             "Expression3    : {8}" _
             , _
             CkeckZipTimeAndExist, _
             GetEtalonData, _
             UncDT, _
             OldUpTime, _
             ResultFileName & UserId & ".zip", _
             File.GetAttributes(ResultFileName & UserId & ".zip"), _
             (Date.UtcNow.Subtract(UncDT.ToUniversalTime).TotalHours < 1 And Not GetEtalonData), _
             (OldUpTime.Year = 2003 And DateTime.UtcNow.Subtract(UncDT.ToUniversalTime).TotalHours < 8), _
             (File.GetAttributes(ResultFileName & UserId & ".zip") = FileAttributes.Normal And GetEtalonData))
        Else

            Log.DebugFormat("Файл с подготовленными данными не существует: {0}", ResultFileName & UserId & ".zip")
            CkeckZipTimeAndExist = False

        End If
    End Function

    Private Sub FirebirdProc()
        Dim SQLText As String
        Dim StartTime As DateTime = Now()
        Dim TS As TimeSpan

        Try
            ThreadContext.Properties("user") = UpdateData.UserName
            Dim transaction As MySqlTransaction
            Dim helper As UpdateHelper = New UpdateHelper(UpdateData, ReadOnlyCn, ReadWriteCn)
            Try
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

                transaction = ReadOnlyCn.BeginTransaction(IsoLevel)
                SelProc.Transaction = transaction

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

                GetMySQLFile("SynonymFirmCr", SelProc, helper.GetSynonymFirmCrCommand(GED))

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

                AddEndOfFiles()
                SelProc.CommandText = "drop temporary table IF EXISTS MaxCodesSynFirmCr, MinCosts, ActivePrices, Prices, Core, tmpprd, MaxCodesSyn, ParentCodes; "
                SelProc.ExecuteNonQuery()

                transaction.Commit()
            Catch ex As Exception
                ConnectionHelper.SafeRollback(transaction)
                If ExceptionHelper.IsDeadLockOrSimilarExceptionInChain(ex) Then
                    Thread.Sleep(2500)
                    GoTo RestartTrans2
                End If
                Throw
            End Try

            Try
                helper.UpdateReplicationInfo()
                TS = Now().Subtract(StartTime)
                If Math.Round(TS.TotalSeconds, 0) > 30 Then
                    Addition &= "Sel: " & Math.Round(TS.TotalSeconds, 0) & "; "
                End If
            Catch ex As Exception
                If Not ExceptionHelper.IsDeadLockOrSimilarExceptionInChain(ex) Then
                    Throw
                End If
                Addition &= "Не удалось сохранить информацию об подготовленных данных из-за блокировок в базе данных, в следующем обновлении отдадим больше данных"
            End Try

        Catch ex As Exception
            Me.Log.Error("Основной поток выборки, general " & CCode, ex)
            ErrorFlag = True
            UpdateType = RequestType.Error
            Addition &= ex.Message
            If ThreadZipStream.IsAlive Then ThreadZipStream.Abort()
        End Try
    End Sub


    Private Sub MySqlProc()
        Dim SQLText As String
        Dim StartTime As DateTime = Now()
        Dim TS As TimeSpan

        Dim transaction As MySqlTransaction
        Try
            ThreadContext.Properties("user") = UpdateData.UserName
            Dim helper As UpdateHelper = New UpdateHelper(UpdateData, ReadOnlyCn, ReadWriteCn)
            Try

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
                MySQLFileDelete(MySqlFilePath & "MaxProducerCosts" & UserId & ".txt")
                MySQLFileDelete(MySqlFilePath & "Producers" & UserId & ".txt")
                MySQLFileDelete(MySqlFilePath & "UpdateInfo" & UserId & ".txt")
                MySQLFileDelete(MySqlFilePath & "ClientToAddressMigrations" & UserId & ".txt")
                MySQLFileDelete(MySqlFilePath & "MinReqRules" & UserId & ".txt")

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
                SelProc.Parameters.AddWithValue("?LastUpdateTime", CurUpdTime)



                transaction = ReadOnlyCn.BeginTransaction(IsoLevel)
                SelProc.Transaction = transaction

                SelProc.CommandText = "drop temporary table IF EXISTS MaxCodesSynFirmCr, MinCosts, ActivePrices, Prices, Core, tmpprd, MaxCodesSyn, ParentCodes; "
                SelProc.ExecuteNonQuery()

                GetMySQLFileWithDefault( _
                 "UpdateInfo", _
                 SelProc, _
                 "select " & _
                 "  date_sub(?LastUpdateTime, interval time_to_sec(date_sub(now(), interval unix_timestamp() second)) second)," & _
                 "  ?Cumulative " & _
                 "from UserUpdateInfo where UserId=" & UserId)

                If helper.NeedClientToAddressMigration() Then
                    GetMySQLFileWithDefault("ClientToAddressMigrations", SelProc, helper.GetClientToAddressMigrationCommand())
                End If

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

                    If (BuildNo >= 1150) Or (UpdateData.EnableUpdate And ((BuildNo >= 1079) And (BuildNo < 1150))) Then
                        'Подготовка данных для версии программы >= 1150 или обновление на нее
                        GetMySQLFileWithDefaultEx( _
                         "Catalogs", _
                         SelProc, _
                         helper.GetCatalogCommand(False, GED), _
                         ((BuildNo = 945) Or ((BuildNo >= 829) And (BuildNo <= 837))) And UpdateData.EnableUpdate, _
                         True)

                        GetMySQLFileWithDefaultEx( _
                         "MNN", _
                         SelProc, _
                         helper.GetMNNCommand(False, GED), _
                         ((BuildNo = 945) Or ((BuildNo >= 829) And (BuildNo <= 837)) Or (BuildNo <= 1035)) And UpdateData.EnableUpdate, _
                         True)

                        GetMySQLFileWithDefaultEx( _
                         "Descriptions", _
                         SelProc, _
                         helper.GetDescriptionCommand(False, GED), _
                         ((BuildNo = 945) Or ((BuildNo >= 829) And (BuildNo <= 837))) And UpdateData.EnableUpdate, _
                         True)

                        If (UpdateData.EnableUpdate And ((BuildNo >= 1079) And (BuildNo < 1150))) Then
                            'Если производим обновление на версию 1159 и выше, то надо полностью отдать каталог производителей
                            GetMySQLFileWithDefaultEx( _
                             "Producers", _
                             SelProc, _
                             helper.GetProducerCommand(True), _
                             ((BuildNo = 945) Or ((BuildNo >= 829) And (BuildNo <= 837))) And UpdateData.EnableUpdate, _
                             True)
                        Else
                            GetMySQLFileWithDefaultEx( _
                             "Producers", _
                             SelProc, _
                             helper.GetProducerCommand(GED), _
                             ((BuildNo = 945) Or ((BuildNo >= 829) And (BuildNo <= 837))) And UpdateData.EnableUpdate, _
                             True)
                        End If

                    Else
                        GetMySQLFileWithDefaultEx( _
                         "Catalogs", _
                         SelProc, _
                         helper.GetCatalogCommand(True, GED), _
                         ((BuildNo = 945) Or ((BuildNo >= 829) And (BuildNo <= 837))) And UpdateData.EnableUpdate, _
                         True)

                        GetMySQLFileWithDefaultEx( _
                         "MNN", _
                         SelProc, _
                         helper.GetMNNCommand(True, GED), _
                         ((BuildNo = 945) Or ((BuildNo >= 829) And (BuildNo <= 837)) Or (BuildNo <= 1035)) And UpdateData.EnableUpdate, _
                         True)

                        GetMySQLFileWithDefaultEx( _
                         "Descriptions", _
                         SelProc, _
                         helper.GetDescriptionCommand(True, GED), _
                         ((BuildNo = 945) Or ((BuildNo >= 829) And (BuildNo <= 837))) And UpdateData.EnableUpdate, _
                         True)
                    End If
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
                "         LEFT(ifnull(group_concat(DISTINCT ProviderContacts.ContactText), ''), 255), " & _
                "         firm.ShortName " & _
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

                GetMySQLFileWithDefault("MinReqRules", SelProc, helper.GetMinReqRuleCommand())

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

                GetMySQLFileWithDefault("SynonymFirmCr", SelProc, helper.GetSynonymFirmCrCommand(GED))

                GetMySQLFileWithDefault("Synonyms", SelProc, helper.GetSynonymCommand(GED))


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

                        GetMySQLFileWithDefaultEx( _
                         "Core", _
                         SelProc, _
                         helper.GetCoreCommand( _
                          False, _
                          (BuildNo > 1027) Or (UpdateData.EnableUpdate And ((BuildNo >= 945) Or ((BuildNo >= 705) And (BuildNo <= 716)) Or ((BuildNo >= 829) And (BuildNo <= 837)))) _
                         ), _
                         (BuildNo <= 1027) And UpdateData.EnableUpdate, _
                         True _
                        )
                    Else
                        'Выгружаем пустую таблицу Core
                        'Делаем запрос из любой таблице (в данном случае из ActivePrices), чтобы получить 0 записей
                        GetMySQLFileWithDefault("Core", SelProc, "SELECT * from ActivePrices limit 0")
                    End If

                    If (BuildNo > 945) Or (UpdateData.EnableUpdate And ((BuildNo = 945) Or ((BuildNo >= 705) And (BuildNo <= 716)) Or ((BuildNo >= 829) And (BuildNo <= 837)))) Then
                        If helper.DefineMaxProducerCostsCostId() Then
                            If GED _
                             Or (UpdateData.EnableUpdate And ((BuildNo < 1049) Or ((BuildNo >= 1079) And (BuildNo < 1150)))) _
                             Or helper.MaxProducerCostIsFresh() _
                            Then
                                GetMySQLFileWithDefault("MaxProducerCosts", SelProc, helper.GetMaxProducerCostsCommand())
                            Else
                                'Если прайс-лист не обновлен, то отдаем пустой файл
                                GetMySQLFileWithDefault("MaxProducerCosts", SelProc, helper.GetMaxProducerCostsCommand() & " limit 0")
                            End If
                        Else
                            GetMySQLFileWithDefault("MaxProducerCosts", SelProc, helper.GetMaxProducerCostsCommand() & " limit 0")
                            Log.WarnFormat("Не возможно определить базовую цены для прайс-листа с максимальными ценами производителей. Код прайс-листа: {0}", helper.MaxProducerCostsPriceId)
                        End If
                    Else
                        GetMySQLFileWithDefault("MaxProducerCosts", SelProc, helper.GetMaxProducerCostsCommand() & " limit 0")
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
                    GetMySQLFileWithDefault("Core", SelProc, helper.GetCoreCommand(True, True))
                    'выгружаем пустую таблицу MaxProducerCosts
                    GetMySQLFileWithDefault("MaxProducerCosts", SelProc, helper.GetMaxProducerCostsCommand() & " limit 0")
                End If

                AddEndOfFiles()

                SelProc.CommandText = "drop temporary table IF EXISTS MaxCodesSynFirmCr, MinCosts, ActivePrices, Prices, Core, tmpprd, MaxCodesSyn, ParentCodes; "
                SelProc.ExecuteNonQuery()
                transaction.Commit()
            Catch ex As Exception
                ConnectionHelper.SafeRollback(transaction)
                If ExceptionHelper.IsDeadLockOrSimilarExceptionInChain(ex) Then
                    Thread.Sleep(500)
                    GoTo RestartTrans2
                End If
                Throw
            End Try

            Try
                helper.UpdateReplicationInfo()
                TS = Now().Subtract(StartTime)
                If Math.Round(TS.TotalSeconds, 0) > 30 Then
                    Addition &= "Sel: " & Math.Round(TS.TotalSeconds, 0) & "; "
                End If
            Catch ex As Exception
                If Not ExceptionHelper.IsDeadLockOrSimilarExceptionInChain(ex) Then
                    Throw
                End If
                Addition &= "Не удалось сохранить информацию об подготовленных данных из-за блокировок в базе данных, в следующем обновлении отдадим больше данных"
            End Try

        Catch ex As Exception
            Me.Log.Error("Основной поток подготовки данных, Код клиента " & CCode, ex)
            If ThreadZipStream.IsAlive Then ThreadZipStream.Abort()
            ErrorFlag = True
            UpdateType = RequestType.Error
            Addition &= ex.Message
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

                If Decimal.TryParse(LeaderMinCost(i), NumberStyles.Currency, CultureInfo.InvariantCulture.NumberFormat, LaederMinCostP) Then newrow.Item("LeaderMinCost") = LaederMinCostP
                If Decimal.TryParse(MinCost(i), NumberStyles.Currency, CultureInfo.InvariantCulture.NumberFormat, MinCostP) Then newrow.Item("MinCost") = MinCostP

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

                .Parameters.Add(New MySqlParameter("?PriceCode", MySqlDbType.UInt32, 0, "PriceCode"))
                .Parameters.Add(New MySqlParameter("?LeaderPriceCode", MySqlDbType.UInt32, 0, "LeaderPriceCode"))
                .Parameters.Add(New MySqlParameter("?MinCost", MySqlDbType.Decimal, 0, "MinCost"))
                .Parameters.Add(New MySqlParameter("?LeaderMinCost", MySqlDbType.Decimal, 0, "LeaderMinCost"))


            End If

            .Parameters.Add(New MySqlParameter("?ProductID", MySqlDbType.UInt32, 0, "ProductID"))
            .Parameters.Add(New MySqlParameter("?CodeFirmCr", MySqlDbType.UInt32, 0, "CodeFirmCr"))
            .Parameters.Add(New MySqlParameter("?SynonymCode", MySqlDbType.UInt32, 0, "SynonymCode"))
            .Parameters.Add(New MySqlParameter("?SynonymFirmCrCode", MySqlDbType.UInt32, 0, "SynonymFirmCrCode"))
            .Parameters.Add(New MySqlParameter("?Code", MySqlDbType.VarString, 0, "Code"))
            .Parameters.Add(New MySqlParameter("?CodeCr", MySqlDbType.VarString, 0, "CodeCr"))
            .Parameters.Add(New MySqlParameter("?Quantity", MySqlDbType.UInt16, 0, "Quantity"))
            .Parameters.Add(New MySqlParameter("?Junk", MySqlDbType.Bit, 0, "Junk"))
            .Parameters.Add(New MySqlParameter("?Await", MySqlDbType.Bit, 0, "Await"))
            .Parameters.Add(New MySqlParameter("?Cost", MySqlDbType.Decimal, 0, "Cost"))
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

        Try
            DBConnect()
            GetClientCode()

            Dim ResStrRSTUIN As String = String.Empty
            Try
                For i = 1 To Len(RSTUIN) Step 3
                    ResStrRSTUIN &= Chr(Convert.ToInt16(Left(Mid(RSTUIN, i), 3)))
                Next
            Catch err As Exception
                Log.ErrorFormat("Ошибка в SendUData при формировании RSTUIN : {0}\n{1}", RSTUIN, err)
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

            Log.Info(accountMessage)

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
            Log.Error("Ошибка в SendUData", ex)
        Finally
            DBDisconnect()
        End Try
    End Sub


    <WebMethod()> _
    Public Function GetPasswords(ByVal UniqueID As String) As String
        Dim ErrorFlag As Boolean = False
        Dim BasecostPassword As String

        Try
            DBConnect()
            GetClientCode()
            FnCheckID(UniqueID)

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
            End If
        Catch ex As Exception
            Me.Log.Error("Ошибка при получении паролей", ex)
            Return "Error=При выполнении Вашего запроса произошла ошибка.;Desc=Пожалуйста повторите попытку через несколько минут."
        Finally
            DBDisconnect()
        End Try

        If ErrorFlag Then
            Return "Error=При выполнении Вашего запроса произошла ошибка.;Desc=Пожалуйста повторите попытку через несколько минут."
        End If
    End Function

    <WebMethod()> Public Function PostPriceDataSettings(ByVal UniqueID As String, ByVal PriceCodes As Int32(), ByVal RegionCodes As Int64(), ByVal INJobs As Boolean()) As String
        Dim ErrorFlag As Boolean = False
        Dim transaction As MySqlTransaction = Nothing

        Try
            DBConnect()
            GetClientCode()
            FnCheckID(UniqueID)

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
                            transaction = ReadWriteCn.BeginTransaction()
                            cmdSel.Transaction = transaction
                            cmdUp.Transaction = transaction


                            daSel.Update(dtChanges)

                            transaction.Commit()
                            Quit = True
                            Return "Res=OK"
                        Catch ex As Exception
                            ConnectionHelper.SafeRollback(transaction)
                            If ExceptionHelper.IsDeadLockOrSimilarExceptionInChain(ex) And ErrCount > 10 Then
                                ErrCount += 1
                                Thread.Sleep(300)
                            Else
                                Throw
                            End If
                        End Try
                    Loop Until Quit
                End If

            Else
                MailErr("Ошибка при обновлении настроек прайс-листов", "Не совпадают длины полученных массивов")
                ErrorFlag = True
            End If

        Catch ex As Exception
            Log.Error("Ошибка при применении обновлений настроек прайс-листов", ex)
            ErrorFlag = True
        Finally
            DBDisconnect()
        End Try

        If ErrorFlag Then
            Return "Error=При выполнении Вашего запроса произошла ошибка.;Desc=Пожалуйста повторите попытку через несколько минут."
        End If
    End Function


    <WebMethod()> Public Function GetReclame() As String
        Dim MaxReclameFileDate As Date
        Dim NewZip As Boolean = True

        Dim FileCount = 0
        Try
            DBConnect()
            GetClientCode()

            Dim updateHelpe = New UpdateHelper(UpdateData, ReadOnlyCn, ReadWriteCn)

            Dim reclameData = updateHelpe.GetReclame()

            If Not reclameData.ShowAdvertising Then
                GetReclame = ""
                Exit Function
            End If

            MaxReclameFileDate = reclameData.ReclameDate

            Reclame = True
            ReclamePath = ResultFileName & "Reclame\" & reclameData.Region & "\"

            MySQLFileDelete(ResultFileName & "r" & UserId & ".zip")

            Dim FileList As String()
            Dim FileName As String

            If Not Directory.Exists(ReclamePath) Then Directory.CreateDirectory(ReclamePath)

            FileList = Directory.GetFiles(ReclamePath)
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

                AddEndOfFiles()

                ZipStream()

                FileInfo = New FileInfo(ResultFileName & "r" & UserId & ".zip")
                FileInfo.CreationTime = MaxReclameFileDate

            End If

        Catch ex As Exception
            Log.Error("Ошибка при загрузке рекламы", ex)
            ErrorFlag = True
            Return ""
        Finally
            DBDisconnect()
        End Try

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
        Dim transaction As MySqlTransaction
        Try
            DBConnect()
            GetClientCode()

            FileInfo = New FileInfo(ResultFileName & "r" & UserId & ".zip")

            If FileInfo.Exists Then

                transaction = ReadWriteCn.BeginTransaction(IsoLevel)
                Cm.CommandText = "update UserUpdateInfo set ReclameDate=?ReclameDate where UserId=" & UserId
                Cm.Parameters.AddWithValue("?ReclameDate", FileInfo.CreationTime)
                Cm.Connection = ReadWriteCn
                Cm.ExecuteNonQuery()
                transaction.Commit()

            End If

            Reclame = True
            MySQLFileDelete(ResultFileName & "r" & UserId & ".zip")
            ReclameComplete = True
        Catch ex As Exception
            ConnectionHelper.SafeRollback(transaction)
            Me.Log.Error("Подтверждение рекламы", ex)
            ReclameComplete = False
        Finally
            DBDisconnect()
        End Try
    End Function

    Private Sub SetCodesProc()
        Dim transaction As MySqlTransaction
        Try
            SelProc.Connection = ReadWriteCn

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

            transaction = ReadWriteCn.BeginTransaction(IsoLevel)
            SelProc.Transaction = transaction

            SelProc.ExecuteNonQuery()

            transaction.Commit()

        Catch ex As Exception
            ConnectionHelper.SafeRollback(transaction)
            If ExceptionHelper.IsDeadLockOrSimilarExceptionInChain(ex) Then
                Me.Log.Info("Deadlock повторяем попытку")
                Thread.Sleep(1500)
                GoTo RestartMaxCodesSet
            End If
            Me.Log.Error("Присвоение значений максимальных синонимов", ex)
            Addition = ex.Message
            UpdateType = RequestType.Error
            ErrorFlag = True
        End Try

    End Sub

    Private Sub ProcessCommitExchange()
        Try
            Dim helper = New UpdateHelper(UpdateData, ReadOnlyCn, ReadWriteCn)
            helper.CommitExchange()
        Catch err As Exception
            MailErr("Присвоение значений максимальных синонимов", err.Message)
            Addition = err.Message
            UpdateType = RequestType.Error
            ErrorFlag = True
        End Try
    End Sub

    Private Sub ProcessOldCommit(ByVal AbsentPriceCodes As String)
        Try
            Dim helper = New UpdateHelper(UpdateData, ReadOnlyCn, ReadWriteCn)
            helper.OldCommit(AbsentPriceCodes)
            Addition &= "!!! " & AbsentPriceCodes
        Catch err As Exception
            MailErr("Присвоение значений максимальных синонимов", err.Message)
            Addition = err.Message
            UpdateType = RequestType.Error
            ErrorFlag = True
        End Try
    End Sub

    Private Sub ProcessResetAbsentPriceCodes(ByVal AbsentPriceCodes As String)
        Try
            Dim helper = New UpdateHelper(UpdateData, ReadOnlyCn, ReadWriteCn)
            helper.ResetAbsentPriceCodes(AbsentPriceCodes)
            Addition &= "!!! " & AbsentPriceCodes
        Catch err As Exception
            MailErr("Сброс информации по прайс-листам с недостающими синонимами", err.Message)
            Addition = err.Message
            UpdateType = RequestType.Error
            ErrorFlag = True
        End Try
    End Sub

    Private Sub AddFileToQueue(ByVal FileName As String)
        SyncLock (FilesForArchive)
            FilesForArchive.Enqueue(New FileForArchive(FileName, True))
        End SyncLock
    End Sub


    Private Sub AddEndOfFiles()
        SyncLock (FilesForArchive)
            FilesForArchive.Enqueue(New FileForArchive("EndOfFiles.txt", False))
        End SyncLock
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

        GetMySQLFileWithDefaultEx(FileName, MyCommand, SQLText, False, True)

    End Sub

    Private Sub GetMySQLFileWithDefaultEx(ByVal FileName As String, ByVal MyCommand As MySqlCommand, ByVal SQLText As String, ByVal SetCumulative As Boolean, ByVal AddToQueue As Boolean)
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

        If AddToQueue Then
            SyncLock (FilesForArchive)

                FilesForArchive.Enqueue(New FileForArchive(FileName, False))

            End SyncLock
        End If

    End Sub

End Class

