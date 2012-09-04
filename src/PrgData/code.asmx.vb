Imports System.Web.Services
Imports System.Threading
Imports System.IO
Imports System.Web
Imports System.Text
Imports System.Globalization
Imports log4net
Imports Common.MySql
Imports PrgData.Common.Models
Imports MySql.Data.MySqlClient
Imports MySQLResultFile = System.IO.File
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
Imports PrgData.Common.Counters
Imports Common.Tools

<WebService(Namespace:="IOS.Service")> _
Public Class PrgDataEx
	Inherits PrgDataNew

	Const SevenZipExe As String = "C:\Program Files\7-Zip\7z.exe"

	Public Sub New()
		MyBase.New()

		Log = LogManager.GetLogger(GetType(PrgDataEx))

		Cm = new MySqlCommand()

		ProtocolUpdatesThread = New Thread(AddressOf ProtocolUpdates)

		InitializeComponent()

		Try
            ArchiveHelper.SevenZipExePath = SevenZipExe
            ResultFileName = ServiceContext.GetResultPath()
        Catch ex As Exception
            Log.Error("Ошибка при инициализации приложения", ex)
        End Try

    End Sub

    Private WithEvents SelProc As MySql.Data.MySqlClient.MySqlCommand
    Private WithEvents DA As MySql.Data.MySqlClient.MySqlDataAdapter
    Friend WithEvents dtProcessingDocuments As System.Data.DataTable

    Private components As System.ComponentModel.IContainer

    Protected Overloads Overrides Sub Dispose(ByVal disposing As Boolean)
        If disposing Then
            If Not (components Is Nothing) Then
                components.Dispose()
            End If
        End If
        MyBase.Dispose(disposing)
    End Sub

	Private Const IsoLevel As System.Data.IsolationLevel = IsolationLevel.ReadCommitted
    Private FileInfo As System.IO.FileInfo
    Private MessageD As String
    'Строка с кодами прайс-листов, у которых отсутствуют синонимы на клиенте
    Private AbsentPriceCodes As String
    Private MessageH As String
    Private Documents, RequestAttachments As Boolean
    Private ClientLog As String
    Private Reclame As Boolean
    Private GetHistory As Boolean
    Public ResultFileName As String
    Dim ArhiveStartTime As DateTime

    'Потоки
    Private ThreadZipStream As New Thread(AddressOf ZipStream)
    Private BaseThread As Thread 'New Thread(AddressOf BaseProc)

	Private CurUpdTime As DateTime
    Private LimitedCumulative As Boolean
	Private ResultLenght As UInt32
    Private ReclamePath As String
	Private PackFinished As Boolean
    Private NewZip As Boolean = True
    Private WithEvents DS As System.Data.DataSet

    Private FilesForArchive As Queue(Of FileForArchive) = New Queue(Of FileForArchive)

    'Получает письмо и отправляет его
    <WebMethod()> _
    Public Function SendLetter(ByVal subject As String, ByVal body As String, ByVal attachment() As Byte) As String
        Return SendLetterEx(subject, body, attachment, 0)
    End Function

    'Получает письмо и отправляет его
    <WebMethod()> _
    Public Function SendLetterEx(ByVal subject As String, ByVal body As String, ByVal attachment() As Byte, ByVal emailGroup As Byte) As String
        Try
            Dim updateData As UpdateData
            Using connection = Settings.GetConnection()
                connection.Open()

                Dim letterUserName = ServiceContext.GetShortUserName()
                ThreadContext.Properties("user") = ServiceContext.GetUserName()

                updateData = UpdateHelper.GetUpdateData(connection, letterUserName)

                If updateData Is Nothing Then
                    Throw New Exception(String.Format("Не удалось найти клиента для указанных учетных данных: {0}", ServiceContext.GetUserName()))
                End If

                updateData.ClientHost = ServiceContext.GetUserHost()

                Dim groupMail As String
                If EmailGroup = 2 Then
                    groupMail = ConfigurationManager.AppSettings("OfficeMail")
                Else
                    If EmailGroup = 1 Then
                        groupMail = ConfigurationManager.AppSettings("BillingMail")
                    Else
                        groupMail = ConfigurationManager.AppSettings("TechMail")
                    End If
                End If


                Dim mess As MailMessage = New MailMessage( _
                 New MailAddress("afmail@analit.net", String.Format("{0} [{1}]", updateData.ShortName, updateData.ClientId)), _
                 New MailAddress(groupMail))
                mess.Body = body
                mess.IsBodyHtml = False
                mess.BodyEncoding = Encoding.UTF8
                mess.Subject = " UserId:" & updateData.UserId & ": " & subject
                If (Not IsNothing(attachment)) Then
                    mess.Attachments.Add(New Attachment(New MemoryStream(attachment), "Attach.7z"))
                End If
                Dim sc As SmtpClient = New SmtpClient("box.analit.net")
                sc.Send(mess)

            End Using

            Return "Res=OK"
        Catch ex As Exception
            LogRequestHelper.MailWithRequest(Log, "Ошибка при отправке письма", ex)
            Return "Error=Не удалось отправить письмо. Попробуйте позднее."
        End Try
    End Function

    'Принимает накладные
    <WebMethod()> _
    Public Function SendWaybills( _
 ByVal ClientId As UInt32, _
 ByVal ProviderIds As UInt64(), _
 ByVal FileNames As String(), _
 ByVal Waybills() As Byte) As String

        Return InternalSendWaybills(ClientId, ProviderIds, FileNames, Waybills, Nothing, Nothing, False)

    End Function

    <WebMethod()> _
    Public Function SendWaybillsEx( _
 ByVal ClientId As UInt32, _
 ByVal ProviderIds As UInt64(), _
 ByVal FileNames As String(), _
 ByVal Waybills() As Byte, _
 ByVal UniqueID As String, _
 ByVal EXEVersion As String) As String

        Return InternalSendWaybills(ClientId, ProviderIds, FileNames, Waybills, UniqueID, EXEVersion, True)

    End Function

    Private Function InternalSendWaybills( _
 ByVal ClientId As UInt32, _
 ByVal ProviderIds As UInt64(), _
 ByVal FileNames As String(), _
 ByVal Waybills() As Byte, _
 ByVal UniqueID As String, _
 ByVal EXEVersion As String, _
 ByVal CheckUIN As Boolean) As String
        Try
            UpdateType = RequestType.SendWaybills
            DBConnect()
            GetClientCode()
            If CheckUIN Then UpdateHelper.CheckUniqueId(readWriteConnection, UpdateData, UniqueID, UpdateType)
            If CheckUIN Then
                UpdateData.ParseBuildNumber(EXEVersion)
                UpdateHelper.UpdateBuildNumber(readWriteConnection, UpdateData)
            End If

            Dim tmpWaybillFolder = Path.GetTempPath() + Path.GetRandomFileName()
            Dim tmpWaybillArchive = tmpWaybillFolder + "\waybills.7z"


            Directory.CreateDirectory(tmpWaybillFolder)

            Try

                Using fileWaybills As New FileStream(tmpWaybillArchive, FileMode.CreateNew)
                    fileWaybills.Write(Waybills, 0, Waybills.Length)
                End Using

                If Not ArchiveHelper.TestArchive(tmpWaybillArchive) Then
                    Throw New Exception("Полученный архив поврежден.")
                End If

                If GenerateDocsHelper.ParseWaybils(readWriteConnection, UpdateData, ClientId, ProviderIds, FileNames, tmpWaybillArchive) Then
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

        Catch updateException As UpdateException
            ProcessUpdateException(updateException)
            Return "Status=1"
        Catch ex As Exception
            LogRequestHelper.MailWithRequest(Log, "Ошибка при загрузке накладных", ex)
            Return "Status=1"
        Finally
            DBDisconnect()
        End Try
    End Function


    <WebMethod()> Public Function GetInfo( _
 ByVal LibraryName() As String, _
 ByVal LibraryVersion() As String, _
 ByVal LibraryHash() As String) As String

        'Dim LibraryNameWOPath As String

        'If DBConnect("GetInfo") Then
        '    GetClientCode()
        '    Cm.Transaction = Nothing
        '    Cm.CommandText = "SELECT libraryname, libraryhash, DeleteOnClient FROM usersettings.AnalitF_Library_Hashs ALH where exeversion=" & UpdateData.BuildNumber
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
        '        'MailHelper.MailErr(CCode, "Ошибка проверки версий библиотек", MailMessage)
        '        MailMessage = ""
        '    End If
        '    DBDisconnect()
        'End If

        Return ""
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
          PriceCodes, _
          False, _
          Nothing, _
          Nothing, _
          False, _
          Nothing, _
          Nothing, _
          False, _
          Nothing)
    End Function

    <WebMethod()> Public Function GetUserDataWithOrders( _
 ByVal AccessTime As Date, _
 ByVal GetEtalonData As Boolean, _
 ByVal EXEVersion As String, _
 ByVal MDBVersion As Int16, _
 ByVal UniqueID As String, _
 ByVal WINVersion As String, _
 ByVal WINDesc As String, _
 ByVal WayBillsOnly As Boolean, _
 ByVal ClientHFile As String, _
 ByVal MaxOrderId As UInt32, _
 ByVal MaxOrderListId As UInt32, _
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
          PriceCodes, _
          False, _
          MaxOrderId, _
          MaxOrderListId, _
          False, _
          Nothing, _
          Nothing, _
          False, _
          Nothing)
    End Function

    <WebMethod()> Public Function GetUserDataWithOrdersAsync( _
 ByVal AccessTime As Date, _
 ByVal GetEtalonData As Boolean, _
 ByVal EXEVersion As String, _
 ByVal MDBVersion As Int16, _
 ByVal UniqueID As String, _
 ByVal WINVersion As String, _
 ByVal WINDesc As String, _
 ByVal WayBillsOnly As Boolean, _
 ByVal ClientHFile As String, _
 ByVal MaxOrderId As UInt32, _
 ByVal MaxOrderListId As UInt32, _
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
          PriceCodes, _
          False, _
          MaxOrderId, _
          MaxOrderListId, _
          True AndAlso Not WayBillsOnly, _
          Nothing, _
          Nothing, _
          False, _
          Nothing)
    End Function

    <WebMethod()> Public Function GetUserDataWithOrdersAsyncCert( _
 ByVal AccessTime As Date, _
 ByVal GetEtalonData As Boolean, _
 ByVal EXEVersion As String, _
 ByVal MDBVersion As Int16, _
 ByVal UniqueID As String, _
 ByVal WINVersion As String, _
 ByVal WINDesc As String, _
 ByVal WayBillsOnly As Boolean, _
 ByVal ClientHFile As String, _
 ByVal MaxOrderId As UInt32, _
 ByVal MaxOrderListId As UInt32, _
 ByVal PriceCodes As UInt32(), _
 ByVal DocumentBodyIds As UInt32()) As String

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
          PriceCodes, _
          False, _
          MaxOrderId, _
          MaxOrderListId, _
          True AndAlso Not WayBillsOnly, _
          DocumentBodyIds, _
          Nothing, _
          False, _
          Nothing)
    End Function

    <WebMethod()> Public Function GetUserDataWithAttachments( _
 ByVal AccessTime As Date, _
 ByVal GetEtalonData As Boolean, _
 ByVal EXEVersion As String, _
 ByVal MDBVersion As Int16, _
 ByVal UniqueID As String, _
 ByVal WINVersion As String, _
 ByVal WINDesc As String, _
 ByVal WayBillsOnly As Boolean, _
 ByVal ClientHFile As String, _
 ByVal MaxOrderId As UInt32, _
 ByVal MaxOrderListId As UInt32, _
 ByVal PriceCodes As UInt32(), _
 ByVal DocumentBodyIds As UInt32(), _
 ByVal AttachmentIds As UInt32()) As String

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
          PriceCodes, _
          False, _
          MaxOrderId, _
          MaxOrderListId, _
          False, _
          DocumentBodyIds, _
          AttachmentIds, _
          False, _
          Nothing)
    End Function

    <WebMethod()> Public Function GetUserDataWithAttachmentsAsync( _
 ByVal AccessTime As Date, _
 ByVal GetEtalonData As Boolean, _
 ByVal EXEVersion As String, _
 ByVal MDBVersion As Int16, _
 ByVal UniqueID As String, _
 ByVal WINVersion As String, _
 ByVal WINDesc As String, _
 ByVal WayBillsOnly As Boolean, _
 ByVal ClientHFile As String, _
 ByVal MaxOrderId As UInt32, _
 ByVal MaxOrderListId As UInt32, _
 ByVal PriceCodes As UInt32(), _
 ByVal DocumentBodyIds As UInt32(), _
 ByVal AttachmentIds As UInt32()) As String

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
          PriceCodes, _
          False, _
          MaxOrderId, _
          MaxOrderListId, _
          True, _
          DocumentBodyIds, _
          AttachmentIds, _
          False, _
          Nothing)
    End Function

    <WebMethod()> Public Function GetUserDataWithRequestAttachments( _
 ByVal AccessTime As Date, _
 ByVal GetEtalonData As Boolean, _
 ByVal EXEVersion As String, _
 ByVal MDBVersion As Int16, _
 ByVal UniqueID As String, _
 ByVal WINVersion As String, _
 ByVal WINDesc As String, _
 ByVal WayBillsOnly As Boolean, _
 ByVal ClientHFile As String, _
 ByVal MaxOrderId As UInt32, _
 ByVal MaxOrderListId As UInt32, _
 ByVal PriceCodes As UInt32(), _
 ByVal DocumentBodyIds As UInt32(), _
 ByVal AttachmentIds As UInt32()) As String

        Return InternalGetUserData( _
          AccessTime, _
          GetEtalonData, _
          EXEVersion, _
          MDBVersion, _
          UniqueID, _
          WINVersion, _
          WINDesc, _
          False, _
          ClientHFile, _
          PriceCodes, _
          False, _
          MaxOrderId, _
          MaxOrderListId, _
          False, _
          DocumentBodyIds, _
          AttachmentIds, _
          True, _
          Nothing)
    End Function

    <WebMethod()> Public Function GetUserDataWithMissingProductsAsync( _
 ByVal AccessTime As Date, _
 ByVal GetEtalonData As Boolean, _
 ByVal EXEVersion As String, _
 ByVal MDBVersion As Int16, _
 ByVal UniqueID As String, _
 ByVal WINVersion As String, _
 ByVal WINDesc As String, _
 ByVal WayBillsOnly As Boolean, _
 ByVal ClientHFile As String, _
 ByVal MaxOrderId As UInt32, _
 ByVal MaxOrderListId As UInt32, _
 ByVal PriceCodes As UInt32(), _
 ByVal DocumentBodyIds As UInt32(), _
 ByVal AttachmentIds As UInt32(), _
 ByVal MissingProductIds As UInt32()) As String

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
          PriceCodes, _
          False, _
          MaxOrderId, _
          MaxOrderListId, _
          True, _
          DocumentBodyIds, _
          AttachmentIds, _
          False, _
          MissingProductIds)
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
     ByVal PriceCodes As UInt32(), _
     ByVal ProcessBatch As Boolean, _
     ByVal MaxOrderId As UInt32, _
     ByVal MaxOrderListId As UInt32,
     ByVal Async As Boolean, _
     ByVal DocumentBodyIds As UInt32(), _
	 ByVal AttachmentIds As UInt32(), _
	 ByVal RequestAttachments As Boolean,
     ByVal MissingProductIds As UInt32()) As String
        Dim ResStr As String = String.Empty

        If (Not ProcessBatch) Then
            Addition = " ОС: " & WINVersion & " " & WINDesc & "; "
        Else
            Addition &= " ОС: " & WINVersion & " " & WINDesc & "; "
        End If

        Try

            'Начинаем обычное обновление
            If (Not ProcessBatch) Then UpdateType = RequestType.GetData
            LimitedCumulative = False

            'Нет критических ошибок
            ErrorFlag = False

            'Только накладные
			Documents = WayBillsOnly
			Me.RequestAttachments = RequestAttachments

			'Получаем код и параметры клиента клиента
            If (Not ProcessBatch) Then
                CCode = 0
                DBConnect()
                GetClientCode()
                UpdateData.LastLockId = Counter.TryLock(UserId, "GetUserData")
                UpdateHelper.CheckUniqueId(readWriteConnection, UpdateData, UniqueID)
				UpdateData.AsyncRequest = Async
				'От клиента пришел запрос на КО
				UpdateData.Cumulative = GetEtalonData
				'Присваиваем версии приложения и базы
				UpdateData.ParseBuildNumber(EXEVersion)
				UpdateHelper.UpdateBuildNumber(readWriteConnection, UpdateData)
				If MaxOrderId > 0 AndAlso MaxOrderListId > 0 Then
					UpdateData.MaxOrderId = MaxOrderId
					UpdateData.MaxOrderListId = MaxOrderListId
				End If
				UpdateData.ParseMissingProductIds(MissingProductIds)
			End If

			Dim helper = New UpdateHelper(UpdateData, readWriteConnection)

			'Если с момента последнего обновления менее установленного времени
			If Not Documents AndAlso Not Me.RequestAttachments Then

				'Если несовпадает время последнего обновления на клиете и сервере
				If Not UpdateData.Cumulative AndAlso (UpdateData.OldUpdateTime <> AccessTime.ToLocalTime) Then
					If (UpdateData.BuildNumber > 1079) And (Now.AddDays(-Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings("AccessTimeHistoryDepth"))) < AccessTime.ToLocalTime) And (AccessTime.ToLocalTime < UpdateData.OldUpdateTime) Then
						Try
							Addition &= String.Format("Время обновления не совпало на клиенте и сервере, готовим частичное КО; Последнее обновление сервер {0}, клиент {1}", UpdateData.OldUpdateTime, AccessTime.ToLocalTime)
							LimitedCumulative = True
							UpdateType = RequestType.GetLimitedCumulative
							UpdateData.OldUpdateTime = AccessTime.ToLocalTime()
							helper.PrepareLimitedCumulative(UpdateData.OldUpdateTime)
						Catch err As Exception
							Log.Error("Подготовка к частичному КО", err)
							Addition = err.Message
							UpdateType = RequestType.Error
							ErrorFlag = True
							GoTo endproc
						End Try
					Else
						UpdateData.Cumulative = True
						Addition &= String.Format("Время обновления не совпало на клиенте и сервере, готовим КО; Последнее обновление сервер {0}, клиент {1}", UpdateData.OldUpdateTime, AccessTime.ToLocalTime)
					End If
				End If


				'В зависимости от версии используем одну из процедур подготовки данных: для сервера Firebird и для сервера MySql
				'для Firebird поддержка удалена, но если версия все таки придет, то вызовем исключение
				If UpdateData.BuildNumber > 716 Then
					'Если производим обновление 945 версии на новую с поддержкой МНН или версия уже с поддержкой МНН, то добавляем еще два файла: мнн и описания
					If ((UpdateData.BuildNumber = 945) And UpdateData.EnableUpdate()) Or (UpdateData.BuildNumber > 945) Then
					Else
						If (UpdateData.BuildNumber >= 829) And (UpdateData.BuildNumber <= 837) And UpdateData.EnableUpdate() Then
							Addition &= "Производится обновление программы с 800-х версий на MySql; "
						Else
							'FileCount = 16
						End If
					End If
					BaseThread = New Thread(AddressOf MySqlProc)
				Else
			        Throw New UpdateException( _
			            "Доступ закрыт.", _
			            "Пожалуйста, обратитесь в АК ""Инфорум"".[1]", _
						"Попытка обновиться с неподдерживаемой версии: " & UpdateData.BuildNumber.ToString() & "; ", _
			            RequestType.Forbidden)
				End If

				'Готовим кумулятивное
				If UpdateData.Cumulative Then

					If (Not ProcessBatch) Then UpdateType = RequestType.GetCumulative

					helper.ResetDocumentCommited(AccessTime.ToLocalTime())
					helper.ResetReclameDate()

				Else

					If LimitedCumulative Then helper.ResetReclameDate()

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

					If UpdateData.NeedUpdateToBuyingMatrix Or UpdateData.NeedUpdateForRetailVitallyImportant() Or UpdateData.NeedUpdateForEAN13() Then helper.SetForceReplication()

				End If

			End If

			If Documents Then
				If DocumentBodyIds IsNot Nothing AndAlso (DocumentBodyIds.Length > 0) AndAlso (DocumentBodyIds(0) <> 0) Then
					UpdateData.FillDocumentBodyIds(DocumentBodyIds)
				End If
				CurUpdTime = Now()

				UpdateType = RequestType.GetDocs
			Else
				'Здесь должен помещать запрос на почтовые вложения
				If AttachmentIds IsNot Nothing AndAlso (AttachmentIds.Length > 0) AndAlso (AttachmentIds(0) <> 0) Then
					UpdateData.FillAttachmentIds(AttachmentIds)
				End If

				If Me.RequestAttachments Then
					CurUpdTime = Now()

					UpdateType = RequestType.RequestAttachments
				Else 
					PackFinished = False

					If CheckZipTimeAndExist(UpdateData.Cumulative) Then

						UpdateType = RequestType.ResumeData
						Dim fileInfo = New FileInfo(UpdateData.GetPreviousFile())
						Addition &= "Отдаем предыдущие подготовленные данные: " & fileInfo.LastWriteTime.ToString() & "; "
						NewZip = False
						PackFinished = True
						Log.DebugFormat("Файл будет докачиваться: {0}", UpdateData.GetPreviousFile())
						GoTo endproc

					Else

						Try
							DeletePreviousFiles()
						Catch ex As Exception
							Addition &= "Не удалось удалить предыдущие данные: " & ex.Message & "; "
							UpdateType = RequestType.Forbidden
							ErrorFlag = True
							GoTo endproc
						End Try

						Log.DebugFormat("Файл будет архивироваться заново: {0}", UpdateData.GetCurrentTempFile())

						CurUpdTime = helper.GetCurrentUpdateDate(UpdateType)

					End If
				End If
			End If

			If Documents Or Me.RequestAttachments Then

				'Начинаем архивирование
				ThreadZipStream.Start()

			Else

				'Начинаем готовить данные
				BaseThread.Start()
				Thread.Sleep(500)

			End If

endproc:
			If Async Then
				GUpdateId = GetUpdateId()
			Else
endprocNew:
				If Not PackFinished And (((BaseThread IsNot Nothing) AndAlso BaseThread.IsAlive) Or ThreadZipStream.IsAlive) And Not ErrorFlag Then

					'Если есть ошибка, прекращаем подготовку данных
					If ErrorFlag Then

						If (BaseThread IsNot Nothing) AndAlso BaseThread.IsAlive Then BaseThread.Abort()
						If ThreadZipStream.IsAlive Then ThreadZipStream.Abort()

						PackFinished = True

					End If
					Thread.Sleep(1000)

					GoTo endprocNew

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
			End If


			If ErrorFlag Then

				If Len(MessageH) = 0 Then
					ResStr = "Error=При подготовке обновления произошла ошибка.;Desc=Пожалуйста, повторите запрос данных через несколько минут."
				Else
					ResStr = "Error=" & MessageH & ";Desc=" & MessageD
				End If

			Else

				If Not UpdateData.AsyncRequest Then
					While GUpdateId = 0
						Thread.Sleep(500)
					End While

					If UpdateType <> RequestType.ResumeData Then
						If File.Exists(UpdateData.GetCurrentFile(GUpdateId)) Then
							Me.Log.DebugFormat("Производим попытку удаления файла: {0}", UpdateData.GetCurrentFile(GUpdateId))
							File.Delete(UpdateData.GetCurrentFile(GUpdateId))
						End If
						File.Move(UpdateData.GetCurrentTempFile(), UpdateData.GetCurrentFile(GUpdateId))
					End If
				End If

				ResStr = "URL=" & UpdateHelper.GetFullUrl("GetFileHandler.ashx") & "?Id=" & GUpdateId & ";New=" & NewZip & ";Cumulative=" & (UpdateType = RequestType.GetCumulative Or (UpdateType = RequestType.PostOrderBatch AndAlso UpdateData.Cumulative))

				If Not String.IsNullOrEmpty(UpdateData.Message) Then ResStr &= ";Addition=" & UpdateData.Message

				'Если параметр ClientHFile имеет значение Nothing, то произошел вызов метода GetUserData и в этом случае работать с файлом hosts не надо
				'производим подмену DNS, если версия программы старше 960
				If (ClientHFile IsNot Nothing) And (UpdateData.BuildNumber > 960) Then
					Try
						ResStr &= HostsFileHelper.ProcessDNS(SpyHostsFile)
					Catch HostsException As Exception
						Log.Error("Ошибка во время обработки DNS", HostsException)
					End Try
				End If

				'Если поднят флаг SpyAccount, то надо отправлять данные с логином и паролем
				If SpyAccount Then ResStr &= ";SendUData=True"

			End If


			InternalGetUserData = ResStr
		Catch updateException As UpdateException
			InternalGetUserData = ProcessUpdateException(updateException)
		Catch ex As Exception
			If LogRequestHelper.NeedLogged() Then
				LogRequestHelper.MailWithRequest(Log, "Ошибка при подготовке данных", ex)
			Else
				Log.Error("Параметры " & _
				 String.Format("AccessTime = {0}, ", AccessTime) & _
				 String.Format("GetEtalonData = {0}, ", GetEtalonData) & _
				 String.Format("EXEVersion = {0}, ", EXEVersion) & _
				 String.Format("MDBVersion = {0}, ", MDBVersion) & _
				 String.Format("UniqueID = {0}, ", UniqueID) & _
				 String.Format("WINVersion = {0}, ", WINVersion) & _
				 String.Format("WINDesc = {0}, ", WINDesc) & _
				 String.Format("WayBillsOnly = {0}", WayBillsOnly), ex)
			End If
			InternalGetUserData = "Error=При подготовке обновления произошла ошибка.;Desc=Пожалуйста, повторите запрос данных через несколько минут."
		Finally
			If (Not ProcessBatch) Then
				'если не асинхронный запрос или не содержится в списке сервисов, производящих асинхронную подготовку, то освобождаем соединение
				If Not Async Or Not AsyncPrgDatas.Contains(Me) Then DBDisconnect()
				Counter.ReleaseLock(UserId, "GetUserData", UpdateData)
			End If
		End Try

		GC.Collect()
	End Function


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
			Dim SevenZipTmpArchive As String
			Dim xRow As DataRow
			Dim FileName, Вывод7Z, Ошибка7Z As String
			Dim zipfilecount = 0
			Dim ArchTrans As MySqlTransaction
			Dim ef(), ListOfDocs() As String


			Using connection = Settings.GetConnection()
				connection.Open()


				Dim Pr As Process = Nothing
				Dim startInfo As ProcessStartInfo


				If GetHistory Then
					SevenZipTmpArchive = Path.GetTempPath() & "Orders" & UserId
					ShareFileHelper.MySQLFileDelete(UpdateData.GetOrdersFile())
				ElseIf Reclame Then
					SevenZipTmpArchive = Path.GetTempPath() & "r" & UserId
					ShareFileHelper.MySQLFileDelete(UpdateData.GetReclameFile())
				Else
					SevenZipTmpArchive = Path.GetTempPath() & UserId
					If File.Exists(UpdateData.GetCurrentTempFile()) Then
						ShareFileHelper.MySQLFileDelete(UpdateData.GetCurrentTempFile())
						Log.DebugFormat("Удалили предыдущие подготовленные данные при начале архивирования: {0}", UpdateData.GetCurrentTempFile())
					End If
				End If

				SevenZipTmpArchive &= "T.zip"
				ShareFileHelper.MySQLFileDelete(SevenZipTmpArchive)


				'Если не реклама
				Dim helper = New UpdateHelper(UpdateData, connection)
				If Not Reclame AndAlso (UpdateData.AllowHistoryDocs() Or Not GetHistory) Then

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
							  "SELECT  AnalitFDocumentsProcessing.*, 0 as FileDelivered, 0 as DocumentDelivered " & _
							  "FROM    AnalitFDocumentsProcessing limit 0"
							ArchDA.FillSchema(DS, SchemaType.Source, "ProcessingDocuments")
							For Each Row As DataRow In DS.Tables("DocumentsToClient").Rows

								'даже если нет документа мы должны подтвердить его, что бы не пытаться его отдавать затем всегда
								'или если документ фиктивный
								xRow = DS.Tables("ProcessingDocuments").NewRow
								xRow("Committed") = False
								xRow("FileDelivered") = False
								xRow("DocumentDelivered") = False
								xRow.Item("DocumentId") = Row.Item("RowId").ToString
								DS.Tables("ProcessingDocuments").Rows.Add(xRow)

								If Not Convert.ToBoolean(Row.Item("IsFake")) AndAlso Convert.IsDBNull(Row.Item("SendUpdateId")) AndAlso UpdateData.AllowDocumentType(CType(Row.Item("DocumentType"), Int32)) _
								Then

									ListOfDocs = Directory.GetFiles(ServiceContext.GetDocumentsPath() & _
									 Row.Item("ClientCode").ToString & _
									 "\" & _
									 CType(Row.Item("DocumentType"), ТипДокумента).ToString, _
									 Row.Item("RowId").ToString & "_*")

									If ListOfDocs.Length = 1 Then

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
										   Path.GetFileName(ListOfDocs(0)) & _
										   """ " & _
										   SevenZipParam

										startInfo.WorkingDirectory = ServiceContext.GetDocumentsPath() & _
										   Row.Item("ClientCode").ToString

										Pr = New Process
										Pr.StartInfo = startInfo
										Pr = Process.Start(startInfo)
										Pr.WaitForExit()

										Вывод7Z = Pr.StandardOutput.ReadToEnd
										Ошибка7Z = Pr.StandardError.ReadToEnd

										If Pr.ExitCode <> 0 Then

											ShareFileHelper.MySQLFileDelete(SevenZipTmpArchive)
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
												Log.Error( _
													"Архивирование документов" & vbCrLf & _
													"Вышли из 7Z с ошибкой: " & Вывод7Z & "-" & Ошибка7Z)
											End If
										Else
											xRow("FileDelivered") = True
										End If
									ElseIf ListOfDocs.Length = 0 Then
										If DateTime.Now.Subtract(Convert.ToDateTime(Row.Item("LogTime"))).TotalHours < 1 Then
											'Если документ моложе часа, то попытаемся его отдать позже и не будем формировать уведомление
											DS.Tables("ProcessingDocuments").Rows.Remove(xRow)
											Log.DebugFormat("Не найден файл документа: {0}", Row.Item("RowId"))
										Else
											Addition &= "При подготовке документов в папке: " & _
											 ServiceContext.GetDocumentsPath() & _
											   Row.Item("ClientCode").ToString & _
											   "\" & _
											   CType(Row.Item("DocumentType"), ТипДокумента).ToString & _
											   " не найден документ № " & _
											   Row.Item("RowId").ToString & _
											   " ; "
										End If
									End If

								End If
							Next

							If UpdateData.BuildNumber >= 1027 And DS.Tables("ProcessingDocuments").Rows.Count > 0 Then
								ShareFileHelper.MySQLFileDelete(ServiceContext.GetFileByLocal("DocumentHeaders" & UserId & ".txt"))
								ShareFileHelper.MySQLFileDelete(ServiceContext.GetFileByLocal("DocumentBodies" & UserId & ".txt"))
								ShareFileHelper.MySQLFileDelete(ServiceContext.GetFileByLocal("InvoiceHeaders" & UserId & ".txt"))
                                ShareFileHelper.MySQLFileDelete(ServiceContext.GetFileByLocal("WaybillOrders" & UserId & ".txt"))

								'Необходима задержка после удаления файлов накладных, т.к. файлы удаляются не сразу
								ShareFileHelper.WaitDeleteFile(ServiceContext.GetFileByLocal("DocumentHeaders" & UserId & ".txt"))
								ShareFileHelper.WaitDeleteFile(ServiceContext.GetFileByLocal("DocumentBodies" & UserId & ".txt"))
								ShareFileHelper.WaitDeleteFile(ServiceContext.GetFileByLocal("InvoiceHeaders" & UserId & ".txt"))
                                ShareFileHelper.WaitDeleteFile(ServiceContext.GetFileByLocal("WaybillOrders" & UserId & ".txt"))

								'Данный метод должен работать только в релизе, чтобы время тестов не увеличивалось
#If Not Debug Then
								helper.WaitParsedDocs()
#End If

								Dim ids As String = String.Empty
								For Each documentRow As DataRow In DS.Tables("ProcessingDocuments").Rows
									If String.IsNullOrEmpty(ids) Then
										ids = documentRow("DocumentId").ToString()
									Else
										ids += ", " & documentRow("DocumentId").ToString()
									End If
								Next

								GetMySQLFileWithDefaultEx("DocumentHeaders", ArchCmd, helper.GetDocumentHeadersCommand(ids), False, False)
								If Not String.IsNullOrEmpty(ids) Then
									Log.DebugFormat("Список запрашиваемых Id документов: {0}", ids)
									Try
										Dim exportDosc = MySql.Data.MySqlClient.MySqlHelper.ExecuteDataset(ArchCmd.Connection, helper.GetDocumentHeadersCommand(ids))
										Log.DebugFormat("Кол-во таблиц в датасет: {0}", exportDosc.Tables.Count)
										For Each table As DataTable In exportDosc.Tables
											If String.IsNullOrEmpty(table.TableName) Then
												table.TableName = Path.GetFileNameWithoutExtension(Path.GetRandomFileName())
											End If
											Log.DebugFormat("Содержимое таблицы {0}: {1}", table.TableName, DebugReplicationHelper.TableToString(exportDosc, table.TableName))

											If table.Columns.Contains("DownloadId") Then
												For Each documentProcessedRow As DataRow In DS.Tables("ProcessingDocuments").Rows
													Dim finded = table.Select("DownloadId = " & documentProcessedRow("DocumentId").ToString())
													If finded.Length > 0 Then
														documentProcessedRow("DocumentDelivered") = True
													End If
													If String.IsNullOrEmpty(ids) Then
														ids = documentProcessedRow("DocumentId").ToString()
													Else
														ids += ", " & documentProcessedRow("DocumentId").ToString()
													End If
												Next
											End If

										Next
									Catch ex As Exception
										Log.DebugFormat("Ошибка при экпорте таблиц: {0}", ex)
									End Try
								Else
									Log.DebugFormat("Список запрашиваемых Id документов пуст")
								End If
								GetMySQLFileWithDefaultEx("DocumentBodies", ArchCmd, helper.GetDocumentBodiesCommand(ids), False, False)
								If UpdateData.AllowInvoiceHeaders() Then
									GetMySQLFileWithDefaultEx("InvoiceHeaders", ArchCmd, helper.GetInvoiceHeadersCommand(ids), False, False)
								End If

#If DEBUG Then
								ShareFileHelper.WaitFile(ServiceContext.GetFileByShared("DocumentHeaders" & UserId & ".txt"))
								ShareFileHelper.WaitFile(ServiceContext.GetFileByShared("DocumentBodies" & UserId & ".txt"))
								If UpdateData.AllowInvoiceHeaders() Then
									ShareFileHelper.WaitFile(ServiceContext.GetFileByShared("InvoiceHeaders" & UserId & ".txt"))
								End If
#End If

								Pr = New Process

								startInfo = New ProcessStartInfo(SevenZipExe)
								startInfo.CreateNoWindow = True
								startInfo.RedirectStandardOutput = True
								startInfo.RedirectStandardError = True
								startInfo.UseShellExecute = False
								startInfo.StandardOutputEncoding = System.Text.Encoding.GetEncoding(866)
								startInfo.Arguments = String.Format(" a ""{0}"" ""{1}"" {2}", SevenZipTmpArchive, ServiceContext.GetFileByLocal("Document*" & UserId & ".txt"), SevenZipParam)
								startInfo.FileName = SevenZipExe

								Pr.StartInfo = startInfo

								Pr.Start()

								Вывод7Z = Pr.StandardOutput.ReadToEnd
								Ошибка7Z = Pr.StandardError.ReadToEnd

								Pr.WaitForExit()

								If Pr.ExitCode <> 0 Then
									Addition &= String.Format(" SevenZip exit code : {0}, :" & Pr.StandardError.ReadToEnd, Pr.ExitCode)
									ShareFileHelper.MySQLFileDelete(SevenZipTmpArchive)
									Throw New Exception(String.Format("SevenZip exit code : {0}, {1}, {2}, {3}; ", Pr.ExitCode, startInfo.Arguments, Вывод7Z, Ошибка7Z))
								End If
								Pr = Nothing

								If UpdateData.AllowInvoiceHeaders() Then
									Pr = New Process

									startInfo = New ProcessStartInfo(SevenZipExe)
									startInfo.CreateNoWindow = True
									startInfo.RedirectStandardOutput = True
									startInfo.RedirectStandardError = True
									startInfo.UseShellExecute = False
									startInfo.StandardOutputEncoding = System.Text.Encoding.GetEncoding(866)
									startInfo.Arguments = String.Format(" a ""{0}"" ""{1}"" {2}", SevenZipTmpArchive, ServiceContext.GetFileByLocal("InvoiceHeaders*" & UserId & ".txt"), SevenZipParam)
									startInfo.FileName = SevenZipExe

									Pr.StartInfo = startInfo

									Pr.Start()

									Вывод7Z = Pr.StandardOutput.ReadToEnd
									Ошибка7Z = Pr.StandardError.ReadToEnd

									Pr.WaitForExit()

									If Pr.ExitCode <> 0 Then
										Addition &= String.Format(" SevenZip exit code : {0}, :" & Pr.StandardError.ReadToEnd, Pr.ExitCode)
										ShareFileHelper.MySQLFileDelete(SevenZipTmpArchive)
										Throw New Exception(String.Format("SevenZip exit code : {0}, {1}, {2}, {3}; ", Pr.ExitCode, startInfo.Arguments, Вывод7Z, Ошибка7Z))
									End If
									Pr = Nothing
								End If

								ShareFileHelper.MySQLFileDelete(ServiceContext.GetFileByLocal("DocumentHeaders" & UserId & ".txt"))
								ShareFileHelper.MySQLFileDelete(ServiceContext.GetFileByLocal("DocumentBodies" & UserId & ".txt"))
								If UpdateData.AllowInvoiceHeaders() Then
									ShareFileHelper.MySQLFileDelete(ServiceContext.GetFileByLocal("InvoiceHeaders" & UserId & ".txt"))
								End If
                                If UpdateData.AllowMatchWaybillsToOrders() then
                                    ShareFileHelper.MySQLFileDelete(ServiceContext.GetFileByLocal("WaybillOrders" & UserId & ".txt"))
								End If

								ShareFileHelper.WaitDeleteFile(ServiceContext.GetFileByLocal("DocumentHeaders" & UserId & ".txt"))
								ShareFileHelper.WaitDeleteFile(ServiceContext.GetFileByLocal("DocumentBodies" & UserId & ".txt"))
								If UpdateData.AllowInvoiceHeaders() Then
									ShareFileHelper.WaitDeleteFile(ServiceContext.GetFileByLocal("InvoiceHeaders" & UserId & ".txt"))
								End If
                                If UpdateData.AllowMatchWaybillsToOrders() then
                                    ShareFileHelper.WaitDeleteFile(ServiceContext.GetFileByLocal("WaybillOrders" & UserId & ".txt"))
								End If
							End If

						End If

						'если версия с поддержкой AllowHistoryDocs() и пользователю отдается реклама, то архивируем рекламу
						If UpdateData.ShowAdvertising AndAlso UpdateData.AllowHistoryDocs() Then
							Dim MaxReclameFileDate As Date
							Dim CurrentFilesSize As Long = 0
							Dim MaxFilesSize As Long = 1024 * 1024
							Dim FileCount = 0
							Dim RelamePathTemp As String

							Dim reclameData = helper.GetReclame()

							MaxReclameFileDate = reclameData.ReclameDate
							If Log.IsDebugEnabled Then Log.DebugFormat("Прочитали из базы reclameData.ReclameDate {0}", reclameData.ReclameDate)

							RelamePathTemp = ResultFileName & "Reclame\" & reclameData.Region & "\"
							Dim reclamePreffix = "Reclame\" & reclameData.Region & "\"

							If Log.IsDebugEnabled Then Log.DebugFormat("Путь к рекламе {0}", RelamePathTemp)


							Dim FileList As String() = New String() {}
							Dim ReclameFileName As String

							If Not Directory.Exists(RelamePathTemp) Then
								Try
									Directory.CreateDirectory(RelamePathTemp)
									FileList = reclameData.GetReclameFiles(RelamePathTemp)
								Catch ex As Exception
									Log.ErrorFormat("Ошибка при создании директории '{0}': {1}", RelamePathTemp, ex)
								End Try
							Else
								FileList = reclameData.GetReclameFiles(RelamePathTemp)
							End If

							If Log.IsDebugEnabled Then Log.DebugFormat("Кол-во файлов в каталоге с рекламой {0}", FileList.Length)

							For Each ReclameFileName In FileList

								FileInfo = New FileInfo(ReclameFileName)

								If FileInfo.LastWriteTime.Subtract(reclameData.ReclameDate).TotalSeconds > 1 Then

									If CurrentFilesSize + FileInfo.Length < MaxFilesSize Then
										If Log.IsDebugEnabled Then Log.DebugFormat("Добавили файл в архив {0}", FileInfo.Name)
										FileCount += 1


										startInfo = New ProcessStartInfo(SevenZipExe)
										startInfo.CreateNoWindow = True
										startInfo.RedirectStandardOutput = True
										startInfo.RedirectStandardError = True
										startInfo.UseShellExecute = False
										startInfo.StandardOutputEncoding = System.Text.Encoding.GetEncoding(866)

										startInfo.Arguments = "a """ & _
										   SevenZipTmpArchive & """ " & _
										   " -i!""" & _
										   reclamePreffix & _
										   Path.GetFileName(FileInfo.Name) & _
										   """ " & _
										   SevenZipParam

										startInfo.WorkingDirectory = ResultFileName

										Pr = New Process
										Pr.StartInfo = startInfo
										Pr = Process.Start(startInfo)
										Pr.WaitForExit()

										Вывод7Z = Pr.StandardOutput.ReadToEnd
										Ошибка7Z = Pr.StandardError.ReadToEnd

										If Pr.ExitCode <> 0 Then

											ShareFileHelper.MySQLFileDelete(SevenZipTmpArchive)
											Addition &= "Архивирование рекламы, Вышли из 7Z с ошибкой: " & _
											   Вывод7Z & _
											   "-" & _
											   Ошибка7Z & _
											   "; "

											If Documents Then

												Throw New Exception(String.Format("SevenZip error: {0}", Вывод7Z & _
												 "-" & _
												 Ошибка7Z))

											Else
												Log.Error( _
													"Архивирование рекламы" & vbCrLf & _
													"Вышли из 7Z с ошибкой: " & Вывод7Z & "-" & Ошибка7Z)
											End If
										End If

										If FileInfo.LastWriteTime > MaxReclameFileDate Then MaxReclameFileDate = FileInfo.LastWriteTime
									Else
										Log.ErrorFormat("Файл {0} превышает допустимый размер рекламы в 1 Мб", ReclameFileName)
										Exit For
									End If

								End If

							Next

							If MaxReclameFileDate > Now() Then MaxReclameFileDate = Now()

							If Log.IsDebugEnabled Then Log.DebugFormat("После обработки файлов MaxReclameFileDate {0}", MaxReclameFileDate)

							reclameData.SetUncommitedReclameDate(connection, MaxReclameFileDate)

						End If


					Catch ex As Exception
						Log.Error("Ошибка при архивировании документов", ex)
						Addition &= "Архивирование документов" & ": " & ex.Message & "; "

						If Documents Then ErrorFlag = True

						ShareFileHelper.MySQLFileDelete(SevenZipTmpArchive)

					End Try

					'здесь будем выгружать сертификаты
					If Documents AndAlso UpdateData.NeedExportCertificates Then
						helper.ArchiveCertificates(connection, SevenZipTmpArchive, CurUpdTime, Addition, ClientLog, GUpdateId, FilesForArchive)
					End If

					Dim processor = New ExportProcessor(UpdateData, connection, FilesForArchive)
					processor.Archive(UpdateType, SevenZipTmpArchive)

					If Documents Then
						If File.Exists(SevenZipTmpArchive) Then

							File.Move(SevenZipTmpArchive, UpdateData.GetCurrentTempFile())
							PackFinished = True
							FileInfo = New FileInfo(UpdateData.GetCurrentTempFile())
							ResultLenght = Convert.ToUInt32(FileInfo.Length)
							PackProtocols()
							Exit Sub

						Else

							MessageH = "Новых файлов документов нет."
							Addition &= " Нет новых документов"
							ErrorFlag = True
							PackFinished = True
							PackProtocols()
							Exit Sub

						End If

					End If

					If Me.RequestAttachments Then
						If File.Exists(SevenZipTmpArchive) Then

							File.Move(SevenZipTmpArchive, UpdateData.GetCurrentTempFile())
							PackFinished = True
							FileInfo = New FileInfo(UpdateData.GetCurrentTempFile())
							ResultLenght = Convert.ToUInt32(FileInfo.Length)
							PackProtocols()
							Exit Sub

						Else

							MessageH = "Запрошенные вложения не найдены."
							Addition &= " Запрошенные вложения не найдены"
							ErrorFlag = True
							PackFinished = True
							PackProtocols()
							Exit Sub

						End If

					End If



					'Если не документы
					If Not Documents AndAlso Not GetHistory AndAlso Not Me.RequestAttachments Then

						'Архивирование обновления программы
						Try
							If UpdateData.EnableUpdate() Then

								ef = UpdateData.GetUpdateFiles()
								If ef.Length > 0 Then
									Pr = Process.Start(SevenZipExe, "a """ & SevenZipTmpArchive & """  """ & Path.GetDirectoryName(ef(0)) & """ " & SevenZipParam)

									Pr.WaitForExit()

									If Pr.ExitCode <> 0 Then
										Log.ErrorFormat("Архивирование EXE" & vbCrLf & "Вышли из 7Z с кодом : {0}", Pr.ExitCode)
										Addition &= "Архивирование обновления версии, Вышли из 7Z с кодом " & ": " & Pr.ExitCode & "; "
										ShareFileHelper.MySQLFileDelete(SevenZipTmpArchive)
									Else

										Addition &= "Обновление включает в себя новую версию программы; "
									End If

								End If

							End If

						Catch ex As ThreadAbortException
							Log.Debug("Ошибка ThreadAbortException при архивировании обновления программы")
							If Not Pr Is Nothing Then
								If Not Pr.HasExited Then Pr.Kill()
								Pr.WaitForExit()
							End If
							ShareFileHelper.MySQLFileDelete(SevenZipTmpArchive)
						Catch ex As Exception
							Log.Error("Архивирование Exe", ex)
							Addition &= " Архивирование обновления " & ": " & ex.Message & "; "
							If Not Pr Is Nothing Then
								If Not Pr.HasExited Then Pr.Kill()
								Pr.WaitForExit()
							End If
							ShareFileHelper.MySQLFileDelete(SevenZipTmpArchive)
						End Try

						ArchTrans = Nothing
						ArchCmd.Transaction = Nothing

					End If
				End If


				'Архивирование данных, или рекламы
				Try
					Dim FileForArchive As FileForArchive
					If Not Documents AndAlso Not Me.RequestAttachments Then

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
							If GetHistory Then
								File.Move(SevenZipTmpArchive, UpdateData.GetOrdersFile())

								FileInfo = New FileInfo(UpdateData.GetOrdersFile())
								ResultLenght = Convert.ToUInt32(FileInfo.Length)
							ElseIf Reclame Then

								File.Move(SevenZipTmpArchive, UpdateData.GetReclameFile())

							Else

								File.Move(SevenZipTmpArchive, UpdateData.GetCurrentTempFile())
								Log.DebugFormat("Закончено архивирование файла: {0}", UpdateData.GetCurrentTempFile())

								FileInfo = New FileInfo(UpdateData.GetCurrentTempFile())
								ResultLenght = Convert.ToUInt32(FileInfo.Length)

							End If

							PackFinished = True
							Log.Debug("Будет вызывать PackProtocols()")
							PackProtocols()
							Exit Sub
						End If

						If Reclame Then
							FileName = ReclamePath & FileForArchive.FileName
						Else

							If FileForArchive.FileType Then
								FileName = FileForArchive.FileName
							Else
								FileName = ServiceContext.GetFileByLocal(FileForArchive.FileName & UserId & ".txt")
							End If


#If DEBUG Then
							ShareFileHelper.WaitFile(FileName)
#End If
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

						Вывод7Z = Pr.StandardOutput.ReadToEnd
						Ошибка7Z = Pr.StandardError.ReadToEnd

						Pr.WaitForExit()

						If Pr.ExitCode <> 0 Then
							Addition &= String.Format(" SevenZip exit code : {0}, :" & Pr.StandardError.ReadToEnd, Pr.ExitCode)
							ShareFileHelper.MySQLFileDelete(SevenZipTmpArchive)
							Throw New Exception(String.Format("SevenZip exit code : {0}, {1}, {2}, {3}; ", Pr.ExitCode, startInfo.Arguments, Вывод7Z, Ошибка7Z))
						End If
						If Not Reclame Then ShareFileHelper.MySQLFileDelete(FileName)
						zipfilecount += 1

						GoTo StartZipping

					End If

				Catch ex As ThreadAbortException
					Log.Debug("Ошибка ThreadAbortException при архивировании данных")
					ShareFileHelper.MySQLFileDelete(SevenZipTmpArchive)

					Try

						Pr.Kill()
						Pr.WaitForExit()

					Catch
					End Try


				Catch ex As MySqlException

					If Not Pr Is Nothing Then
						If Not Pr.HasExited Then Pr.Kill()
						Pr.WaitForExit()
					End If
					ShareFileHelper.MySQLFileDelete(SevenZipTmpArchive)

					Log.Error("Архивирование", ex)
					If Not TypeOf ex.InnerException Is ThreadAbortException Then
						ErrorFlag = True
						UpdateType = RequestType.Error
						if UpdateData.AsyncRequest Then AnalitFUpdate.SetErrorUpdateType(GUpdateId)
					End If
					Addition &= " Архивирование: " & ex.ToString() & "; "

				Catch Unhandled As Exception

					ErrorFlag = True
					UpdateType = RequestType.Error
					if UpdateData.AsyncRequest Then AnalitFUpdate.SetErrorUpdateType(GUpdateId)
					If Not Pr Is Nothing Then
						If Not Pr.HasExited Then Pr.Kill()
						Pr.WaitForExit()
					End If
					Addition &= " Архивирование: " & Unhandled.ToString()
					ShareFileHelper.MySQLFileDelete(SevenZipTmpArchive)
					Log.Error("Архивирование", Unhandled)
					Addition &= " Архивирование: " & Unhandled.ToString() & "; "
				End Try
			End Using

		Catch tae As ThreadAbortException
			Log.Debug("Ошибка ThreadAbortException глобальное в ZipStream")

		Catch Unhandled As Exception
			Log.Error("Архивирование general", Unhandled)
			ErrorFlag = True
			if UpdateData.AsyncRequest Then AnalitFUpdate.SetErrorUpdateType(GUpdateId)
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
			UpdateData.LastLockId = Counter.TryLock(UserId, "MaxSynonymCode")

			If Not WayBillsOnly AndAlso UpdateData.PreviousRequest.UpdateId = UpdateId Then

				AbsentPriceCodes = String.Empty
				If (PriceCode IsNot Nothing) AndAlso (PriceCode.Length > 0) AndAlso (PriceCode(0) <> 0) Then
					AbsentPriceCodes = PriceCode(0).ToString
					Dim I As Integer
					For I = 1 To PriceCode.Length - 1
						AbsentPriceCodes &= "," & PriceCode(I)
					Next
				End If

				ProcessOldCommit(AbsentPriceCodes)
			Else
				If Not WayBillsOnly Then
					Me.Log.DebugFormat("Не смогли обработать подтверждение, т.к. не совпал UpdateId: ClientUpdateId:{0}; ServerUpdateId:{1}", UpdateId, UpdateData.PreviousRequest.UpdateId)
				End If
			End If

			Try

				If Not WayBillsOnly Then
					Cm.Connection = readWriteConnection
					Cm.CommandText = "select UncommitedUpdateDate from UserUpdateInfo  where UserId=" & UserId & "; "
					Using SQLdr As MySqlDataReader = Cm.ExecuteReader
						SQLdr.Read()
						UpdateTime = SQLdr.GetDateTime(0)
					End Using

					Dim masterUpdateTime As Object = MySql.Data.MySqlClient.MySqlHelper.ExecuteScalar(readWriteConnection, "select UncommitedUpdateDate from UserUpdateInfo  where UserId=" & UserId & "; ")
					Me.Log.DebugFormat("MaxSynonymCode: slave UncommitedUpdateDate {0}  master UncommitedUpdateDate {1}", UpdateTime, masterUpdateTime)
					If IsDate(masterUpdateTime) And (CType(masterUpdateTime, DateTime) > UpdateTime) Then
						UpdateTime = CType(masterUpdateTime, DateTime)
						Me.Log.Debug("MaxSynonymCode: дата, выбранная из мастера, больше, чем дата из slave")
					End If
				End If

			Catch ex As Exception
				Me.Log.Error("Выборка даты обновления", ex)
				UpdateTime = Now().ToUniversalTime
			End Try

			MaxSynonymCode = UpdateTime.ToUniversalTime

			Try
				If UpdateData.SaveAFDataFiles Then
					If Not Directory.Exists(ResultFileName & "\Archive\" & UserId) Then Directory.CreateDirectory(ResultFileName & "\Archive\" & UserId)
					File.Copy(UpdateData.GetCurrentFile(UpdateId), ResultFileName & "\Archive\" & UserId & "\" & UpdateId & ".zip")
				End If

				ShareFileHelper.MySQLFileDelete(UpdateData.GetCurrentFile(UpdateId))
				Me.Log.DebugFormat("Удалили подготовленные данные после подтверждения: {0}", UpdateData.GetCurrentFile(UpdateId))
			Catch ex As Exception
				Me.Log.Error("Ошибка при сохранении подготовленных данных", ex)
			End Try

			ProtocolUpdatesThread.Start()
		Catch e As Exception
			LogRequestHelper.MailWithRequest(Me.Log, String.Format("Ошибка при подтверждении обновления, вернул {0}, дальше КО", Now().ToUniversalTime), e)
			Return Now().ToUniversalTime
		Finally
			Counter.ReleaseLock(UserId, "MaxSynonymCode", UpdateData)
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
			UpdateData.LastLockId = Counter.TryLock(UserId, "CommitExchange")

			If Not WayBillsOnly AndAlso UpdateData.PreviousRequest.UpdateId = UpdateId Then
				If UpdateData.PreviousRequest.RequestType = RequestType.GetData _
				 Or UpdateData.PreviousRequest.RequestType = RequestType.GetCumulative _
				 Or UpdateData.PreviousRequest.RequestType = RequestType.GetLimitedCumulative _
				Then
					Dim exportList = UnconfirmedOrdersExporter.DeleteUnconfirmedOrders(UpdateData, readWriteConnection, UpdateId)
					If Not String.IsNullOrEmpty(exportList) Then
						Addition &= "Экспортированные неподтвержденные заказы: " & exportList & "; "
					End If
				End If
				' Здесь сбрасывались коды прайс-листов
				ProcessCommitExchange()
			Else
				If Not WayBillsOnly Then
					Me.Log.DebugFormat("Не смогли обработать подтверждение, т.к. не совпал UpdateId: ClientUpdateId:{0}; ServerUpdateId:{1}", UpdateId, UpdateData.PreviousRequest.UpdateId)
				End If
			End If

			Try

				If Not WayBillsOnly Then


					If UpdateData.AllowHistoryDocs() Then
						Dim transaction = readWriteConnection.BeginTransaction(IsoLevel)
						Cm.CommandText = "update UserUpdateInfo set ReclameDate=UncommitedReclameDate where UserId=" & UserId
						Cm.Connection = readWriteConnection
						If Log.IsDebugEnabled Then Log.DebugFormat("Обновляем дату рекламы из UncommitedReclameDate")
						Cm.ExecuteNonQuery()
						transaction.Commit()
					End If


					Cm.Connection = readWriteConnection
					Cm.CommandText = "select UncommitedUpdateDate from UserUpdateInfo  where UserId=" & UserId & "; "
					Using SQLdr As MySqlDataReader = Cm.ExecuteReader
						SQLdr.Read()
						UpdateTime = SQLdr.GetDateTime(0)
					End Using

					Dim masterUpdateTime As Object = MySql.Data.MySqlClient.MySqlHelper.ExecuteScalar(readWriteConnection, "select UncommitedUpdateDate from UserUpdateInfo  where UserId=" & UserId & "; ")
					Me.Log.DebugFormat("CommitExchange: slave UncommitedUpdateDate {0}  master UncommitedUpdateDate {1}", UpdateTime, masterUpdateTime)
					If IsDate(masterUpdateTime) And (CType(masterUpdateTime, DateTime) > UpdateTime) Then
						UpdateTime = CType(masterUpdateTime, DateTime)
						Me.Log.Debug("CommitExchange: дата, выбранная из мастера, больше, чем дата из slave")
					End If
				End If

			Catch ex As Exception
				Me.Log.Error("Выборка даты обновления", ex)
				UpdateTime = Now().ToUniversalTime
			End Try

			CommitExchange = UpdateTime.ToUniversalTime

			Try
				If UpdateData.SaveAFDataFiles Then
					If Not Directory.Exists(ResultFileName & "\Archive\" & UserId) Then Directory.CreateDirectory(ResultFileName & "\Archive\" & UserId)
					File.Copy(UpdateData.GetCurrentFile(UpdateId), ResultFileName & "\Archive\" & UserId & "\" & UpdateId & ".zip")
				End If

				ShareFileHelper.MySQLFileDelete(UpdateData.GetCurrentFile(UpdateId))
				Me.Log.DebugFormat("Удалили подготовленные данные после подтверждения: {0}", UpdateData.GetCurrentFile(UpdateId))
			Catch ex As Exception
				Me.Log.Error("Ошибка при сохранении подготовленных данных", ex)
			End Try

			ProtocolUpdatesThread.Start()
		Catch e As Exception
			LogRequestHelper.MailWithRequest(Log, "Ошибка при подтверждении обновления", e)
			CommitExchange = Now().ToUniversalTime
		Finally
			DBDisconnect()
			Counter.ReleaseLock(UserId, "CommitExchange", UpdateData)
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
			UpdateData.LastLockId = Counter.TryLock(UserId, "SendClientLog")
			Try
				AnalitFUpdate.UpdateLog(readWriteConnection, UpdateId, Log)
			Catch ex As Exception
				Me.Log.Error("Ошибка при сохранении лога клиента", ex)
			End Try
			SendClientLog = "OK"
		Catch e As Exception
			LogRequestHelper.MailWithRequest(Me.Log, "Ошибка при сохранении лога клиента", e)
			SendClientLog = "Error"
		Finally
			DBDisconnect()
			Counter.ReleaseLock(UserId, "SendClientLog", UpdateData)
		End Try
	End Function

	<WebMethod()> _
	Public Function SendClientArchivedLog( _
  ByVal UpdateId As UInt32, _
  ByVal Log As String, _
  ByVal LogSize As UInt32
 ) As String
		Try
			DBConnect()
			GetClientCode()
			UpdateData.LastLockId = Counter.TryLock(UserId, "SendClientLog")
			Try

				Dim helper = New SendClientLogHandler(UpdateData, UpdateId)

				Try
					helper.PrepareLogFile(Log)

					Dim logContent = helper.GetLogContent()

					AnalitFUpdate.UpdateLog(readWriteConnection, UpdateId, logContent)

					Me.Log.DebugFormat("Размер лога от клиента: {0}, полученный размера лога: {1}", logContent.Length, LogSize)

				Finally
					helper.DeleteTemporaryFiles()
				End Try

			Catch ex As Exception
				Me.Log.Error("Ошибка при сохранении лога клиента", ex)
			End Try
			SendClientArchivedLog = "OK"
		Catch e As Exception
			LogRequestHelper.MailWithRequest(Me.Log, "Ошибка при сохранении лога клиента", e)
			SendClientArchivedLog = "Error"
		Finally
			DBDisconnect()
			Counter.ReleaseLock(UserId, "SendClientLog", UpdateData)
		End Try
	End Function

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
		'            'MailHelper.MailErr(CCode, "Запросили у клиента архивные заказы", list.Count & " шт.")
		'            Return String.Join(";", list.ToArray())
		'        Else
		'            Return String.Empty
		'        End If

		'    Catch Exp As Exception
		'        MailHelper.MailErr(CCode, "Ошибка при получении списка архивных заказов", Exp.Message & ": " & Exp.StackTrace)
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
		Return _
		 PostSomeOrdersFullEx( _
		  UniqueID, _
		  Nothing, _
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
		  DelayOfPayment, _
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
	End Function

	<WebMethod()> _
	Public Function PostSomeOrdersFullEx( _
  ByVal UniqueID As String, _
  ByVal EXEVersion As String, _
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

		Dim RetailCost As IEnumerable(Of String) = Enumerable.Repeat("", New List(Of UInt16)(RowCount).Sum(Function(item) item))
		Dim VitallyImportantDelayOfPayment As IEnumerable(Of String) = Enumerable.Repeat("", OrderCount)
		Dim CostWithDelayOfPayment = ReorderHelper.PrepareCostWithDelayOfPayment(Cost, OrderCount, DelayOfPayment, RowCount)
		Dim EAN13 As IEnumerable(Of String) = Enumerable.Repeat("", New List(Of UInt16)(RowCount).Sum(Function(item) item))
		Dim CodeOKP As IEnumerable(Of String) = Enumerable.Repeat("", New List(Of UInt16)(RowCount).Sum(Function(item) item))
		Dim Series As IEnumerable(Of String) = Enumerable.Repeat("", New List(Of UInt16)(RowCount).Sum(Function(item) item))

		Return _
		 InternalPostSomeOrdersFullEx( _
		  UniqueID, _
		  EXEVersion, _
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
		  DelayOfPayment, _
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
		  NDS, _
		  RetailCost.ToArray(), _
		  VitallyImportantDelayOfPayment.ToArray(), _
		  CostWithDelayOfPayment, _
		  EAN13.ToArray(), _
		  CodeOKP.ToArray(), _
		  Series.ToArray() _
		  )
	End Function

	'Отправляем несколько заказов скопом и по ним все формируем ответ
	<WebMethod()> _
	Public Function PostSomeOrdersFullExtend( _
  ByVal UniqueID As String, _
  ByVal EXEVersion As String, _
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
  ByVal RetailCost As String(), _
  ByVal ProducerCost As String(), _
  ByVal NDS As String()) As String

		Dim RetailMarkup As IEnumerable(Of String) = Enumerable.Repeat("", New List(Of UInt16)(RowCount).Sum(Function(item) item))
		Dim VitallyImportantDelayOfPayment As IEnumerable(Of String) = Enumerable.Repeat("", OrderCount)
		Dim CostWithDelayOfPayment = ReorderHelper.PrepareCostWithDelayOfPayment(Cost, OrderCount, DelayOfPayment, RowCount)
		Dim EAN13 As IEnumerable(Of String) = Enumerable.Repeat("", New List(Of UInt16)(RowCount).Sum(Function(item) item))
		Dim CodeOKP As IEnumerable(Of String) = Enumerable.Repeat("", New List(Of UInt16)(RowCount).Sum(Function(item) item))
		Dim Series As IEnumerable(Of String) = Enumerable.Repeat("", New List(Of UInt16)(RowCount).Sum(Function(item) item))

		Return _
		 InternalPostSomeOrdersFullEx( _
		  UniqueID, _
		  EXEVersion, _
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
		  DelayOfPayment, _
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
		  CoreQuantity, _
		  Unit, _
		  Volume, _
		  Note, _
		  Period, _
		  Doc, _
		  RegistryCost, _
		  VitallyImportant, _
		  RetailMarkup.ToArray(), _
		  ProducerCost, _
		  NDS, _
		  RetailCost, _
		  VitallyImportantDelayOfPayment.ToArray(), _
		  CostWithDelayOfPayment, _
		  EAN13.ToArray(), _
		  CodeOKP.ToArray(), _
		  Series.ToArray() _
		  )
	End Function

	'Отправляем несколько заказов скопом и по ним все формируем ответ
	<WebMethod()> _
	Public Function PostSomeOrdersWithDelays( _
  ByVal UniqueID As String, _
  ByVal EXEVersion As String, _
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
  ByVal RetailCost As String(), _
  ByVal ProducerCost As String(), _
  ByVal NDS As String(), _
  ByVal VitallyImportantDelayOfPayment As String(), _
  ByVal CostWithDelayOfPayment As Decimal()) As String

		Dim RetailMarkup As IEnumerable(Of String) = Enumerable.Repeat("", New List(Of UInt16)(RowCount).Sum(Function(item) item))
		Dim EAN13 As IEnumerable(Of String) = Enumerable.Repeat("", New List(Of UInt16)(RowCount).Sum(Function(item) item))
		Dim CodeOKP As IEnumerable(Of String) = Enumerable.Repeat("", New List(Of UInt16)(RowCount).Sum(Function(item) item))
		Dim Series As IEnumerable(Of String) = Enumerable.Repeat("", New List(Of UInt16)(RowCount).Sum(Function(item) item))

		Return _
		 InternalPostSomeOrdersFullEx( _
		  UniqueID, _
		  EXEVersion, _
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
		  SupplierPriceMarkup, _
		  CoreQuantity, _
		  Unit, _
		  Volume, _
		  Note, _
		  Period, _
		  Doc, _
		  RegistryCost, _
		  VitallyImportant, _
		  RetailMarkup.ToArray(), _
		  ProducerCost, _
		  NDS, _
		  RetailCost, _
		  VitallyImportantDelayOfPayment, _
		  CostWithDelayOfPayment, _
		  EAN13.ToArray(), _
		  CodeOKP.ToArray(), _
		  Series.ToArray() _
		  )
	End Function

	'Отправляем несколько заказов скопом и по ним все формируем ответ
	<WebMethod()> _
	Public Function PostSomeOrdersWithSeries( _
  ByVal UniqueID As String, _
  ByVal EXEVersion As String, _
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
  ByVal RetailCost As String(), _
  ByVal ProducerCost As String(), _
  ByVal NDS As String(), _
  ByVal VitallyImportantDelayOfPayment As String(), _
  ByVal CostWithDelayOfPayment As Decimal(), _
  ByVal EAN13 As String(), _
  ByVal CodeOKP As String(), _
  ByVal Series As String()) As String

		Dim RetailMarkup As IEnumerable(Of String) = Enumerable.Repeat("", New List(Of UInt16)(RowCount).Sum(Function(item) item))

		Return _
		 InternalPostSomeOrdersFullEx( _
		  UniqueID, _
		  EXEVersion, _
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
		  SupplierPriceMarkup, _
		  CoreQuantity, _
		  Unit, _
		  Volume, _
		  Note, _
		  Period, _
		  Doc, _
		  RegistryCost, _
		  VitallyImportant, _
		  RetailMarkup.ToArray(), _
		  ProducerCost, _
		  NDS, _
		  RetailCost, _
		  VitallyImportantDelayOfPayment, _
		  CostWithDelayOfPayment, _
		  EAN13, _
		  CodeOKP, _
		  Series
		  )
	End Function

	Private Function InternalPostSomeOrdersFullEx( _
  ByVal UniqueID As String, _
  ByVal EXEVersion As String, _
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
  ByVal NDS As String(), _
  ByVal RetailCost As String(), _
  ByVal VitallyImportantDelayOfPayment As String(), _
  ByVal CostWithDelayOfPayment As Decimal(), _
  ByVal EAN13 As String(), _
  ByVal CodeOKP As String(), _
  ByVal Series As String()) As String

		Try
			UpdateType = RequestType.SendOrders
			DBConnect()
			GetClientCode()
			UpdateData.LastLockId = Counter.TryLock(UserId, "PostOrder")
			UpdateHelper.CheckUniqueId(readWriteConnection, UpdateData, UniqueID, UpdateType)
			If Not String.IsNullOrEmpty(EXEVersion) Then
				UpdateData.ParseBuildNumber(EXEVersion)
				UpdateHelper.UpdateBuildNumber(readWriteConnection, UpdateData)
			End If

			Dim helper = New ReorderHelper(UpdateData, readWriteConnection, ForceSend, ClientCode, UseCorrectOrders)

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
			 NDS, _
			 RetailCost, _
			 VitallyImportantDelayOfPayment, _
			 CostWithDelayOfPayment, _
			 EAN13, _
			 CodeOKP, _
			 Series _
			)

			Return helper.PostSomeOrders()
		Catch updateException As UpdateException
			Return ProcessUpdateException(updateException)
		Catch ex As NotEnoughElementsException
			Log.Warn("Ошибка при отправке заказа", ex)
			Return "Error=Отправка заказов завершилась неудачно.;Desc=Пожалуйста, повторите попытку через несколько минут."
		Catch ex As Exception
			Console.WriteLine(ex)
			LogRequestHelper.MailWithRequest(Log, "Ошибка при отправке заказов", ex)
			Return "Error=Отправка заказов завершилась неудачно.;Desc=Пожалуйста, повторите попытку через несколько минут."
		Finally
			Counter.ReleaseLock(UserId, "PostOrder", UpdateData)
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
		Dim currentBatchFile As String = String.Empty
		Dim currentUpdateId As UInt32? = Nothing

		Try
			UpdateType = RequestType.PostOrderBatch

			DBConnect()
			GetClientCode()
			UpdateData.LastLockId = Counter.TryLock(UserId, "PostOrderBatch")
			UpdateHelper.CheckUniqueId(readWriteConnection, UpdateData, UniqueID, UpdateType)
			UpdateData.ParseBuildNumber(EXEVersion)
			UpdateHelper.UpdateBuildNumber(readWriteConnection, UpdateData)

			Dim helper = New SmartOrderHelper(UpdateData, ClientId, MaxOrderId, MaxOrderListId, MaxBatchId)

			Try
				helper.PrepareBatchFile(BatchFile)

				Addition &= "Файл-дефектура: " & helper.ExtractBatchFileName & "; "
				If UpdateData.SaveAFDataFiles Then
					If Not Directory.Exists(ResultFileName & "\Archive\" & UserId) Then Directory.CreateDirectory(ResultFileName & "\Archive\" & UserId)
					currentBatchFile = ResultFileName & "\Archive\" & UserId & "\" & DateTime.Now.ToString("yyyyMMddHHmmssfff") & ".7z"
					File.Copy(helper.TmpBatchArchiveFileName, currentBatchFile)
				End If

				'UpdateHelper.GenerateSessionKey(readWriteConnection, UpdateData)

				helper.ProcessBatchFile()

				AddFileToQueue(helper.BatchReportFileName)
				AddFileToQueue(helper.BatchOrderFileName)
				AddFileToQueue(helper.BatchOrderItemsFileName)
				If (UpdateData.BuildNumber > 1271) Then
					AddFileToQueue(helper.BatchReportServiceFieldsFileName)
				End If

				ResStr = InternalGetUserData(AccessTime, GetEtalonData, EXEVersion, MDBVersion, UniqueID, WINVersion, WINDesc, False, Nothing, PriceCodes, True, 0, 0, False, Nothing, Nothing, False, Nothing)

				currentUpdateId = GUpdateId

			Finally
				helper.DeleteTemporaryFiles()
			End Try

			Return ResStr
		Catch updateException As UpdateException
			Dim updateExceptionMessage = ProcessUpdateException(updateException, True)
			currentUpdateId = GUpdateId
			Return updateExceptionMessage
		Catch OnParse As ParseDefectureException
			LogRequestHelper.MailWithRequest(Log, "Ошибка при разборе дефектуры", OnParse.InnerException)
			currentUpdateId = AnalitFUpdate.InsertAnalitFUpdatesLog(readWriteConnection, UpdateData, RequestType.Error, Addition & OnParse.Message & ": " & OnParse.InnerException.Message)
			Return "Error=Не удалось разобрать дефектуру.;Desc=Проверьте корректность формата файла дефектуры."
		Catch OnEmpty As EmptyDefectureException
			currentUpdateId = AnalitFUpdate.InsertAnalitFUpdatesLog(readWriteConnection, UpdateData, RequestType.Error, Addition & "Представленная дефектура не содержит данных.")
			Return "Error=Представленная дефектура не содержит данных.;Desc=Пожалуйста, выберите другой файл."
		Catch ex As Exception
			LogRequestHelper.MailWithRequest(Log, "Ошибка при обработке дефектуры", ex)
			currentUpdateId = AnalitFUpdate.InsertAnalitFUpdatesLog(readWriteConnection, UpdateData, RequestType.Error, Addition & "Ошибка при обработке дефектуры" & vbCrLf & ex.ToString())
			Return "Error=Отправка дефектуры завершилась неудачно.;Desc=Пожалуйста, повторите попытку через несколько минут."
		Finally

			Try
				If UpdateData.SaveAFDataFiles Then
					If currentUpdateId IsNot Nothing Then
						File.Move(currentBatchFile, ResultFileName & "\Archive\" & UserId & "\" & currentUpdateId & "_Batch.7z")
					Else
						Log.DebugFormat("При разборе дефектуры не был установлен UpdateId: {0}  FileName: {1}", currentUpdateId, currentBatchFile)
					End If
				End If
			Catch onSaveBatch As Exception
				Log.Error("Ошибка при сохранении файла-дефектуры", onSaveBatch)
			End Try

			Counter.ReleaseLock(UserId, "PostOrderBatch", UpdateData)
			DBDisconnect()
		End Try

	End Function

	Private Function GetUpdateId() As ULong
		Dim transaction As MySqlTransaction
		Dim LogCm As New MySqlCommand

		If (UpdateType = RequestType.ResumeData) Then
			GUpdateId = UpdateData.PreviousRequest.UpdateId
			Return GUpdateId
			Exit Function
		End If

		Using connection = New MySqlConnection
			ThreadContext.Properties("user") = UpdateData.UserName

			connection.ConnectionString = Settings.ConnectionString
			connection.Open()

			LogCm.Connection = connection

			If (UpdateType = RequestType.GetData) _
			 Or (UpdateType = RequestType.GetCumulative) _
			 Or (UpdateType = RequestType.GetLimitedCumulative) _
			 Or (UpdateType = RequestType.PostOrderBatch) _
			 Or (UpdateType = RequestType.Forbidden) _
			 Or (UpdateType = RequestType.Error) _
			 Or (UpdateType = RequestType.GetDocs) _
			 Or (UpdateType = RequestType.GetHistoryOrders) _ 
			 Or (UpdateType = RequestType.RequestAttachments) Then

				transaction = connection.BeginTransaction(IsoLevel)

				If CurUpdTime < Now().AddDays(-1) Then CurUpdTime = Now()

				'если нет новых документов то и подтверждения не будет
				'а в интерейсе неподтвержденное обновление это тревога
				'что бы не было тревог
				Dim commit = False
				If MessageH = "Новых файлов документов нет." Then
					commit = True
				End If

				With LogCm
					.CommandText = _
					 "insert into `logs`.`AnalitFUpdates` " _
					 & "(`RequestTime`, `UpdateType`, `UserId`, `AppVersion`,  `ResultSize`, `Addition`, Commit, ClientHost) " _
					 & " values " _
					 & "(?UpdateTime, ?UpdateType, ?UserId, ?exeversion,  ?Size, ?Addition, ?Commit, ?ClientHost); "
					.CommandText &= "select last_insert_id()"
					.Transaction = transaction
					.Parameters.Add(New MySqlParameter("?UserId", UpdateData.UserId))

					Dim resultUpdateType As RequestType = UpdateType
					'If (UpdateType = RequestType.GetData) And LimitedCumulative Then resultUpdateType = RequestType.GetCumulative
					If (resultUpdateType = RequestType.GetData) Then resultUpdateType = RequestType.GetDataAsync
					If (resultUpdateType = RequestType.GetCumulative) Then resultUpdateType = RequestType.GetCumulativeAsync
					If (resultUpdateType = RequestType.GetLimitedCumulative) Then resultUpdateType = RequestType.GetLimitedCumulativeAsync
					.Parameters.Add(New MySqlParameter("?UpdateType", Convert.ToInt32(resultUpdateType)))

					.Parameters.Add(New MySqlParameter("?EXEVersion", UpdateData.BuildNumber))
					.Parameters.Add(New MySqlParameter("?Size", ResultLenght))
					.Parameters.Add(New MySqlParameter("?Addition", Addition))
					.Parameters.Add(New MySqlParameter("?UpdateTime", CurUpdTime))
					.Parameters.AddWithValue("?Commit", commit)
					.Parameters.AddWithValue("?ClientHost", UserHost)
				End With

				GUpdateId = Convert.ToUInt32(LogCm.ExecuteScalar)

				transaction.Commit()

				Return GUpdateId

			End If
		End Using

		Log.ErrorFormat("Неожидаемый тип обновления при получении UpdateId: {0}", UpdateType)
		Return 0

	End Function

	Private Sub PackProtocols()
		If UpdateData.AsyncRequest Then
			If Len(Addition) = 0 Then Addition = MessageH & " " & MessageD

			If NewZip And Not ErrorFlag Then
				Dim ArhiveTS = Now().Subtract(ArhiveStartTime)

				If Math.Round(ArhiveTS.TotalSeconds, 0) > 30 Then

					Addition &= "Архивирование: " & Math.Round(ArhiveTS.TotalSeconds, 0) & "; "

				End If

			End If

			Log.Debug("Попытка запустить ProtocolUpdatesThread.Start()")
			ProtocolUpdatesThread.Start()

			If Not ErrorFlag AndAlso UpdateType <> RequestType.ResumeData Then
				If File.Exists(UpdateData.GetCurrentFile(GUpdateId)) Then
					Me.Log.DebugFormat("Производим попытку удаления файла: {0}", UpdateData.GetCurrentFile(GUpdateId))
					File.Delete(UpdateData.GetCurrentFile(GUpdateId))
				End If
				File.Move(UpdateData.GetCurrentTempFile(), UpdateData.GetCurrentFile(GUpdateId))
			End If

			Log.Debug("Попытка обновить тип обновления")
			Using connection = New MySqlConnection
				connection.ConnectionString = Settings.ConnectionString
				connection.Open()

				If UpdateType = RequestType.Error then
					AnalitFUpdate.UpdateLog(connection, GUpdateId, UpdateType, Addition)
				Else 
					UpdateHelper.UpdateRequestType(connection, UpdateData, GUpdateId, Addition, ResultLenght)
				End If
			End Using
		End If
	End Sub

	Private Sub ProtocolUpdates()
		Dim transaction As MySqlTransaction = Nothing
		Dim LogCb As New MySqlCommandBuilder
		Dim LogDA As New MySqlDataAdapter
		Dim LogCm As New MySqlCommand
		Dim NoNeedProcessDocuments As Boolean = False


		Try
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
						NoNeedProcessDocuments = True
					End If

					If (UpdateType = RequestType.GetData) _
					 Or (UpdateType = RequestType.GetCumulative) _
					 Or (UpdateType = RequestType.GetLimitedCumulative) _
					 Or (UpdateType = RequestType.PostOrderBatch) _
					 Or (UpdateType = RequestType.Forbidden) _
					 Or (UpdateType = RequestType.Error) _
					 Or (UpdateType = RequestType.GetDocs) _
					 Or (UpdateType = RequestType.GetHistoryOrders) _ 
					 Or (UpdateType = RequestType.RequestAttachments) Then

PostLog:
						If GUpdateId = 0 Then

							transaction = connection.BeginTransaction(IsoLevel)

							If CurUpdTime < Now().AddDays(-1) Then CurUpdTime = Now()

							'если нет новых документов то и подтверждения не будет
							'а в интерейсе неподтвержденное обновление это тревога
							'что бы не было тревог
							Dim commit = False
							If MessageH = "Новых файлов документов нет." Then
								commit = True
							End If

							GUpdateId = AnalitFUpdate.InsertAnalitFUpdatesLog(connection, UpdateData, UpdateType, Addition, commit, ResultLenght, ClientLog)

							transaction.Commit()

						End If

						If DS.Tables("ProcessingDocuments").Rows.Count > 0 Then
							Dim DocumentsProcessingCommandBuilder As New MySqlCommandBuilder

							For Each DocumentsIdRow As DataRow In DS.Tables("ProcessingDocuments").Rows
								DocumentsIdRow.Item("UpdateId") = GUpdateId
							Next

							LogDA.SelectCommand = New MySqlCommand
							LogDA.SelectCommand.Connection = connection
							LogDA.SelectCommand.CommandText = "" & _
							  "SELECT  * " & _
						"from AnalitFDocumentsProcessing limit 0"

							DocumentsProcessingCommandBuilder.DataAdapter = LogDA
							LogDA.InsertCommand = DocumentsProcessingCommandBuilder.GetInsertCommand

							transaction = connection.BeginTransaction(IsoLevel)
							Dim command = New MySqlCommand("update Logs.DocumentSendLogs set UpdateId = ?UpdateId, FileDelivered = ?FileDelivered, DocumentDelivered = ?DocumentDelivered where UserId = ?UserId and DocumentId = ?DocumentId", connection)
							command.Parameters.AddWithValue("?UserId", UpdateData.UserId)
							command.Parameters.AddWithValue("?UpdateId", GUpdateId)
							command.Parameters.Add("?DocumentId", MySqlDbType.UInt32)
							command.Parameters.Add("?FileDelivered", MySqlDbType.Byte)
							command.Parameters.Add("?DocumentDelivered", MySqlDbType.Byte)

							For Each row As DataRow In DS.Tables("ProcessingDocuments").Rows
								command.Parameters("?DocumentId").Value = row("DocumentId")
								command.Parameters("?FileDelivered").Value = row("FileDelivered")
								command.Parameters("?DocumentDelivered").Value = row("DocumentDelivered")
								command.ExecuteNonQuery()
							Next

							transaction.Commit()

						End If

						UnconfirmedOrdersExporter.InsertUnconfirmedOrdersLogs(UpdateData, connection, GUpdateId)

						AnalitFUpdate.Log(UpdateData, connection, GUpdateId)

						DS.Tables.Clear()

					End If

					If (UpdateType = RequestType.ResumeData) Then

						transaction = connection.BeginTransaction(IsoLevel)

						LogCm.CommandText = "" & _
						   "SELECT  MAX(UpdateId) " & _
						  "FROM    `logs`.AnalitFUpdates " & _
						  "WHERE   UpdateType IN (1, 2, 18) " & _
						   "    AND `Commit`    =0 " & _
						   "    AND UserId  =" & UpdateData.UserId

						GUpdateId = Convert.ToUInt32(LogCm.ExecuteScalar)
						If GUpdateId < 1 Then
							GUpdateId = Nothing
						Else
							LogCm.CommandText = "update `logs`.`AnalitFUpdates` set Addition=if(instr(ifnull(?Addition, ''), Addition) = 1, ifnull(?Addition, ''), concat(Addition, ifnull(?Addition, '')))   where UpdateId=" & GUpdateId
							LogCm.Parameters.Add(New MySqlParameter("?Addition", MySqlDbType.VarString))
							LogCm.Parameters("?Addition").Value = Addition

							LogCm.ExecuteNonQuery()
						End If
						transaction.Commit()
					End If

					If Not NoNeedProcessDocuments Then

						If (UpdateType = RequestType.CommitExchange) Then
							Dim СписокФайлов() As String

							transaction = connection.BeginTransaction(IsoLevel)
							LogCm.CommandText = "update `logs`.`AnalitFUpdates` set Commit=1, Log = if(?Log is null, Log, concat(ifnull(Log, ''), ifnull(?Log, ''))) , Addition=concat(Addition, ifnull(?Addition, ''))  where UpdateId=" & GUpdateId

							LogCm.Parameters.Add(New MySqlParameter("?Log", MySqlDbType.VarString))
							LogCm.Parameters("?Log").Value = ClientLog

							LogCm.Parameters.Add(New MySqlParameter("?Addition", MySqlDbType.VarString))
							LogCm.Parameters("?Addition").Value = Addition

							LogCm.ExecuteNonQuery()

							Dim helper = New UpdateHelper(UpdateData, connection)

							LogCm.CommandText = "delete from Customers.ClientToAddressMigrations where UserId = " & UpdateData.UserId
							LogCm.ExecuteNonQuery()

							Dim processedDocuments = helper.GetProcessedDocuments(GUpdateId)

							For Each DocumentsIdRow As DataRow In processedDocuments.Rows

								СписокФайлов = Directory.GetFiles(ServiceContext.GetDocumentsPath() & _
								   DocumentsIdRow.Item("ClientCode").ToString & _
								   "\" & _
								   CType(DocumentsIdRow.Item("DocumentType"), ТипДокумента).ToString, _
								   DocumentsIdRow.Item("DocumentId").ToString & "_*")

								If СписокФайлов.Length > 0 Then MySQLResultFile.Delete(СписокФайлов(0))

							Next
							LogCm.CommandText = UpdateHelper.GetConfirmDocumentsCommnad(GUpdateId)
							LogCm.ExecuteNonQuery()

							LogCm.CommandText = helper.GetConfirmMailsCommnad(GUpdateId)
							LogCm.ExecuteNonQuery()

							transaction.Commit()
						End If

					End If
				Catch ex As Exception
					PrgData.Common.ConnectionHelper.SafeRollback(transaction)
					GUpdateId = Nothing
					If ExceptionHelper.IsDeadLockOrSimilarExceptionInChain(ex) Then
						Thread.Sleep(500)
						GoTo PostLog
					End If
					Log.Error("Запись лога", ex)
				End Try
			End Using

		Catch unhandled As Exception
			Log.Error("Ошибка при завершении подготовки данных", unhandled)
		End Try

	End Sub



	Private Sub InitializeComponent()
		Me.DS = New System.Data.DataSet
		Me.dtProcessingDocuments = New System.Data.DataTable
		Me.readWriteConnection = New MySql.Data.MySqlClient.MySqlConnection
		Me.SelProc = New MySql.Data.MySqlClient.MySqlCommand
		Me.DA = New MySql.Data.MySqlClient.MySqlDataAdapter
		CType(Me.DS, System.ComponentModel.ISupportInitialize).BeginInit()
		CType(Me.dtProcessingDocuments, System.ComponentModel.ISupportInitialize).BeginInit()
		'
		'DS
		'
		Me.DS.DataSetName = "DS"
		Me.DS.Locale = New System.Globalization.CultureInfo("ru-RU")
		Me.DS.RemotingFormat = System.Data.SerializationFormat.Binary
		Me.DS.Tables.AddRange(New System.Data.DataTable() {Me.dtProcessingDocuments})
		'
		'dtProcessingDocuments
		'
		Me.dtProcessingDocuments.RemotingFormat = System.Data.SerializationFormat.Binary
		Me.dtProcessingDocuments.TableName = "ProcessingDocuments"
		'
		'ReadOnlyCn
		'
		Me.readWriteConnection.ConnectionString = Nothing
		'
		'SelProc
		'
		Me.SelProc.Connection = Me.readWriteConnection
		Me.SelProc.Transaction = Nothing

		Me.DA.DeleteCommand = Nothing
		Me.DA.InsertCommand = Nothing
		Me.DA.SelectCommand = Me.Cm
		Me.DA.UpdateCommand = Nothing
		CType(Me.DS, System.ComponentModel.ISupportInitialize).EndInit()
		CType(Me.dtProcessingDocuments, System.ComponentModel.ISupportInitialize).EndInit()

	End Sub

	Private Function CheckZipTimeAndExist(ByVal GetEtalonData As Boolean) As Boolean

		If Not UpdateData.PreviousRequest.UpdateId.HasValue Or UpdateData.PreviousRequest.Commit Then
			Log.DebugFormat( _
			 "Не найден предыдущий неподтвержденный запрос данных: UserId:{0}; UpdateId: {1}; Commit: {2}; RequestTime: {3}; RequestType: {4}", _
			 UserId, _
			 UpdateData.PreviousRequest.UpdateId, _
			 UpdateData.PreviousRequest.Commit, _
			 UpdateData.PreviousRequest.RequestTime, _
			 UpdateData.PreviousRequest.RequestType)
			Return False
		Else
			If UpdateData.PreviousRequest.UpdateId.HasValue AndAlso Not UpdateData.PreviousRequest.Commit AndAlso UpdateData.PreviousRequest.RequestType = RequestType.PostOrderBatch Then
				Log.DebugFormat( _
				 "Предыдущим неподтвержденным запрос данных является автозаказ, поэтому заново будем готовить данные: UserId:{0}; UpdateId: {1}; Commit: {2}; RequestTime: {3}; RequestType: {4}", _
				 UserId, _
				 UpdateData.PreviousRequest.UpdateId, _
				 UpdateData.PreviousRequest.Commit, _
				 UpdateData.PreviousRequest.RequestTime, _
				 UpdateData.PreviousRequest.RequestType)
				Return False
			Else
				Log.DebugFormat( _
				 "Найден предыдущий неподтвержденный запрос данных: UserId:{0}; UpdateId: {1}; Commit: {2}; RequestTime: {3}; RequestType: {4}", _
				 UserId, _
				 UpdateData.PreviousRequest.UpdateId, _
				 UpdateData.PreviousRequest.Commit, _
				 UpdateData.PreviousRequest.RequestTime, _
				 UpdateData.PreviousRequest.RequestType)
			End If
		End If

		FileInfo = New FileInfo(UpdateData.GetPreviousFile())

		If FileInfo.Exists Then
			Log.DebugFormat("Файл с подготовленными данными существует: {0}", UpdateData.GetPreviousFile())
			CheckZipTimeAndExist = _
			 (Date.UtcNow.Subtract(UncDT.ToUniversalTime).TotalHours < 1 And Not GetEtalonData And (UpdateType <> RequestType.GetLimitedCumulative)) _
			 Or (UpdateData.OldUpdateTime.Year = 2003 And DateTime.UtcNow.Subtract(UncDT.ToUniversalTime).TotalHours < 8) _
			 Or (UpdateData.PreviousRequest.RequestType = RequestType.GetCumulative And GetEtalonData) _
			 Or (UpdateData.PreviousRequest.RequestType = RequestType.GetLimitedCumulative _
			  And UpdateType = RequestType.GetLimitedCumulative _
			  And UpdateData.PreviousRequest.Addition.Contains(String.Format(", клиент {0}", UpdateData.OldUpdateTime)))

			Log.DebugFormat( _
			 "Результат проверки CheckZipTimeAndExist: {0}  " & vbCrLf & _
			 "Параметры " & vbCrLf & _
			 "GetEtalonData  : {1}" & vbCrLf & _
			 "UncDT          : {2}" & vbCrLf & _
			 "OldUpTime      : {3}" & vbCrLf & _
			 "FileName       : {4}" & vbCrLf & _
			 "CurrentType    : {5}" & vbCrLf & _
			 "PreviousType   : {6}" & vbCrLf & _
			 "Expression1    : {7}" & vbCrLf & _
			 "Expression2    : {8}" & vbCrLf & _
			 "Expression3    : {9}" & vbCrLf & _
			 "Expression4    : {10}" & vbCrLf & _
			 "Expression5    : {11}" _
			 , _
			 CheckZipTimeAndExist, _
			 GetEtalonData, _
			 UncDT, _
			 UpdateData.OldUpdateTime, _
			 UpdateData.GetPreviousFile(), _
			 UpdateType, _
			 UpdateData.PreviousRequest.RequestType, _
			 (Date.UtcNow.Subtract(UncDT.ToUniversalTime).TotalHours < 1 And Not GetEtalonData), _
			 (UpdateData.OldUpdateTime.Year = 2003 And DateTime.UtcNow.Subtract(UncDT.ToUniversalTime).TotalHours < 8), _
			 (UpdateData.PreviousRequest.RequestType = RequestType.GetCumulative And GetEtalonData), _
			 UpdateData.PreviousRequest.RequestType = RequestType.GetLimitedCumulative And UpdateType = RequestType.GetLimitedCumulative, _
			 Addition.Contains(String.Format(", клиент {0}", UpdateData.OldUpdateTime)))
		Else
			Log.DebugFormat("Файл с подготовленными данными не существует: {0}", UpdateData.GetPreviousFile())
			CheckZipTimeAndExist = False
		End If

	End Function

	Private Sub MySqlProc()
		Dim StartTime As DateTime = Now()
		Dim TS As TimeSpan

		Dim transaction As MySqlTransaction = Nothing
		Try
			ThreadContext.Properties("user") = UpdateData.UserName
			If UpdateData.AsyncRequest Then AsyncPrgDatas.AddToList(Me)
			Dim helper As UpdateHelper = New UpdateHelper(UpdateData, readWriteConnection)
			Try

RestartTrans2:
				If ErrorFlag Then Exit Try

				ShareFileHelper.MySQLFileDelete(ServiceContext.GetFileByLocal("Products" & UserId & ".txt"))
				ShareFileHelper.MySQLFileDelete(ServiceContext.GetFileByLocal("User" & UserId & ".txt"))
				ShareFileHelper.MySQLFileDelete(ServiceContext.GetFileByLocal("Client" & UserId & ".txt"))
				ShareFileHelper.MySQLFileDelete(ServiceContext.GetFileByLocal("Catalogs" & UserId & ".txt"))
				ShareFileHelper.MySQLFileDelete(ServiceContext.GetFileByLocal("CatDel" & UserId & ".txt"))
				ShareFileHelper.MySQLFileDelete(ServiceContext.GetFileByLocal("Clients" & UserId & ".txt"))
				ShareFileHelper.MySQLFileDelete(ServiceContext.GetFileByLocal("DelayOfPayments" & UserId & ".txt"))
				ShareFileHelper.MySQLFileDelete(ServiceContext.GetFileByLocal("Providers" & UserId & ".txt"))
				ShareFileHelper.MySQLFileDelete(ServiceContext.GetFileByLocal("Core" & UserId & ".txt"))
				ShareFileHelper.MySQLFileDelete(ServiceContext.GetFileByLocal("PricesData" & UserId & ".txt"))
				ShareFileHelper.MySQLFileDelete(ServiceContext.GetFileByLocal("PricesRegionalData" & UserId & ".txt"))
				ShareFileHelper.MySQLFileDelete(ServiceContext.GetFileByLocal("RegionalData" & UserId & ".txt"))
				ShareFileHelper.MySQLFileDelete(ServiceContext.GetFileByLocal("Regions" & UserId & ".txt"))
				ShareFileHelper.MySQLFileDelete(ServiceContext.GetFileByLocal("Synonyms" & UserId & ".txt"))
				ShareFileHelper.MySQLFileDelete(ServiceContext.GetFileByLocal("SynonymFirmCr" & UserId & ".txt"))
				ShareFileHelper.MySQLFileDelete(ServiceContext.GetFileByLocal("CatalogNames" & UserId & ".txt"))
				ShareFileHelper.MySQLFileDelete(ServiceContext.GetFileByLocal("MNN" & UserId & ".txt"))
				ShareFileHelper.MySQLFileDelete(ServiceContext.GetFileByLocal("Descriptions" & UserId & ".txt"))
				ShareFileHelper.MySQLFileDelete(ServiceContext.GetFileByLocal("MaxProducerCosts" & UserId & ".txt"))
				ShareFileHelper.MySQLFileDelete(ServiceContext.GetFileByLocal("Producers" & UserId & ".txt"))
				ShareFileHelper.MySQLFileDelete(ServiceContext.GetFileByLocal("UpdateInfo" & UserId & ".txt"))
				ShareFileHelper.MySQLFileDelete(ServiceContext.GetFileByLocal("ClientToAddressMigrations" & UserId & ".txt"))
				ShareFileHelper.MySQLFileDelete(ServiceContext.GetFileByLocal("MinReqRules" & UserId & ".txt"))
				ShareFileHelper.MySQLFileDelete(ServiceContext.GetFileByLocal("SupplierPromotions" & UserId & ".txt"))
				ShareFileHelper.MySQLFileDelete(ServiceContext.GetFileByLocal("PromotionCatalogs" & UserId & ".txt"))
				ShareFileHelper.MySQLFileDelete(ServiceContext.GetFileByLocal("Schedules" & UserId & ".txt"))
				ShareFileHelper.MySQLFileDelete(ServiceContext.GetFileByLocal("Mails" & UserId & ".txt"))
				ShareFileHelper.MySQLFileDelete(ServiceContext.GetFileByLocal("Attachments" & UserId & ".txt"))
				ShareFileHelper.MySQLFileDelete(ServiceContext.GetFileByLocal("UpdateValues" & UserId & ".txt"))

				'ShareFileHelper.MySQLFileDelete(ServiceContext.GetFileByLocal("CoreTest" & UserId & ".txt"))

				ShareFileHelper.WaitDeleteFile(ServiceContext.GetFileByLocal("Products" & UserId & ".txt"))
				ShareFileHelper.WaitDeleteFile(ServiceContext.GetFileByLocal("Catalog" & UserId & ".txt"))
				ShareFileHelper.WaitDeleteFile(ServiceContext.GetFileByLocal("UpdateInfo" & UserId & ".txt"))

				helper.MaintainReplicationInfo()

				If ThreadZipStream.IsAlive Then
					ThreadZipStream.Abort()
				End If

				SelProc = New MySqlCommand
				SelProc.Connection = readWriteConnection
				helper.SetUpdateParameters(SelProc, CurUpdTime)

				Dim debugHelper = New DebugReplicationHelper(UpdateData, readWriteConnection, SelProc)

				transaction = readWriteConnection.BeginTransaction(IsolationLevel.RepeatableRead)
				SelProc.Transaction = transaction

				SelProc.CommandText = "drop temporary table IF EXISTS MaxCodesSynFirmCr, MinCosts, ActivePrices, Prices, Core, PriceCounts, MaxCodesSyn, ParentCodes, CurrentReplicationInfo; "
				SelProc.ExecuteNonQuery()

				GetMySQLFileWithDefault( _
				 "UpdateInfo", _
				 SelProc, _
				 "select " & _
				 "  date_sub(?LastUpdateTime, interval time_to_sec(date_sub(now(), interval unix_timestamp() second)) second)," & _
				 "  ?Cumulative " & _
				 "from UserUpdateInfo where UserId=" & UserId)

				If UpdateType <> RequestType.PostOrderBatch Then
					helper.UnconfirmedOrdersExport(ServiceContext.MySqlSharedExportPath(), FilesForArchive)
				End If

				If helper.NeedClientToAddressMigration() Then
					GetMySQLFileWithDefault("ClientToAddressMigrations", SelProc, helper.GetClientToAddressMigrationCommand())
				End If

				If UpdateData.NeedUpdateForHistoryDocs() Then
					GetMySQLFileWithDefault("UpdateValues", SelProc, helper.GetUpdateValuesCommand())
				End If

				Dim processor = New ExportProcessor(UpdateData, SelProc.Connection, FilesForArchive)
				processor.Process()

				GetMySQLFileWithDefault("User", SelProc, helper.GetUserCommand())
				GetMySQLFileWithDefault("Client", SelProc, helper.GetClientCommand())

				If UpdateData.SupportAnalitFSchedule Then
					GetMySQLFileWithDefault("Schedules", SelProc, helper.GetSchedulesCommand())
				End If

				GetMySQLFileWithDefault("Products", SelProc, helper.GetProductsCommand())


				ThreadZipStream = New Thread(AddressOf ZipStream)
				ThreadZipStream.Start()

				If (UpdateData.BuildNumber > 945) Or (UpdateData.EnableUpdate() And ((UpdateData.BuildNumber = 945) Or ((UpdateData.BuildNumber >= 705) And (UpdateData.BuildNumber <= 716)) Or ((UpdateData.BuildNumber >= 829) And (UpdateData.BuildNumber <= 837)))) _
				Then

					If (UpdateData.BuildNumber >= 1150) Or (UpdateData.EnableUpdate() And ((UpdateData.BuildNumber >= 1079) And (UpdateData.BuildNumber < 1150))) Then
						'Подготовка данных для версии программы >= 1150 или обновление на нее
						GetMySQLFileWithDefaultEx( _
						 "Catalogs", _
						 SelProc, _
						 helper.GetCatalogCommand(False), _
						 UpdateData.NeedUpdateTo945(), _
						 True)

						'Обновляем на новую структуру MNN без RussianMNN = (UpdateData.BuildNumber > 1263) Or UpdateData.NeedUpdateToNewMNN)
						GetMySQLFileWithDefaultEx( _
						 "MNN", _
						 SelProc, _
						 helper.GetMNNCommand( _
						  False, _
						  (UpdateData.BuildNumber > 1263) Or UpdateData.NeedUpdateToNewMNN), _
						 ((UpdateData.BuildNumber = 945) Or ((UpdateData.BuildNumber >= 829) And (UpdateData.BuildNumber <= 837)) Or (UpdateData.BuildNumber <= 1035)) And UpdateData.EnableUpdate(), _
						 True)

						GetMySQLFileWithDefaultEx( _
						 "Descriptions", _
						 SelProc, _
						 helper.GetDescriptionCommand(False), _
						 UpdateData.NeedUpdateTo945(), _
						 True)

						If (UpdateData.EnableUpdate() And ((UpdateData.BuildNumber >= 1079) And (UpdateData.BuildNumber < 1150))) Then
							'Если производим обновление на версию 1159 и выше, то надо полностью отдать каталог производителей
							GetMySQLFileWithDefaultEx( _
							 "Producers", _
							 SelProc, _
							 helper.GetProducerCommand(True), _
							 UpdateData.NeedUpdateTo945(), _
							 True)
						Else
							GetMySQLFileWithDefaultEx( _
							 "Producers", _
							 SelProc, _
							 helper.GetProducerCommand(UpdateData.Cumulative), _
							 UpdateData.NeedUpdateTo945(), _
							 True)
						End If

					Else
						GetMySQLFileWithDefaultEx( _
						 "Catalogs", _
						 SelProc, _
						 helper.GetCatalogCommand(True), _
						 UpdateData.NeedUpdateTo945(), _
						 True)

						GetMySQLFileWithDefaultEx( _
						 "MNN", _
						 SelProc, _
						 helper.GetMNNCommand(True, False), _
						 ((UpdateData.BuildNumber = 945) Or ((UpdateData.BuildNumber >= 829) And (UpdateData.BuildNumber <= 837)) Or (UpdateData.BuildNumber <= 1035)) And UpdateData.EnableUpdate(), _
						 True)

						GetMySQLFileWithDefaultEx( _
						 "Descriptions", _
						 SelProc, _
						 helper.GetDescriptionCommand(True), _
						 UpdateData.NeedUpdateTo945(), _
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


				GetMySQLFileWithDefault("Regions", SelProc, helper.GetRegionsCommand())

				helper.SelectPrices()
				helper.PreparePricesData(SelProc)
				helper.SelectReplicationInfo()
				helper.SelectActivePrices()
				helper.FillParentCodes()

				If Not UpdateData.EnableImpersonalPrice Then
					debugHelper.CopyActivePrices()
				End If

				Try
					'Подготовка временной таблицы с контактами
					helper.PrepareProviderContacts(SelProc)

					GetMySQLFileWithDefault("Providers", SelProc, helper.GetProvidersCommand())
				Finally
					helper.ClearProviderContacts(SelProc)
				End Try

				GetMySQLFileWithDefault("RegionalData", SelProc, helper.GetRegionalDataCommand())

				GetMySQLFileWithDefault("PricesRegionalData", SelProc, helper.GetPricesRegionalDataCommand())

				GetMySQLFileWithDefault("MinReqRules", SelProc, helper.GetMinReqRuleCommand())

				GetMySQLFileWithDefault("Clients", SelProc, helper.GetClientsCommand())

				GetMySQLFileWithDefault("DelayOfPayments", SelProc, helper.GetDelayOfPaymentsCommand())


				If UpdateData.EnableImpersonalPrice And (UpdateData.OldUpdateTime < New DateTime(2010, 8, 18, 5, 18, 0)) Then
					GetMySQLFileWithDefault("SynonymFirmCr", SelProc, helper.GetSynonymFirmCrCommand(True))

					GetMySQLFileWithDefault("Synonyms", SelProc, helper.GetSynonymCommand(True))
				Else
					GetMySQLFileWithDefault("SynonymFirmCr", SelProc, helper.GetSynonymFirmCrCommand(UpdateData.Cumulative))

					GetMySQLFileWithDefault("Synonyms", SelProc, helper.GetSynonymCommand(UpdateData.Cumulative))
				End If

				If Not UpdateData.EnableImpersonalPrice Then

					SelProc.CommandText = "" & _
					"SELECT IFNULL(SUM(fresh), 0) " & _
					"FROM   ActivePrices"
					If CType(SelProc.ExecuteScalar, Integer) > 0 Or UpdateData.Cumulative Then

						helper.SelectOffers()
						'"UPDATE ActivePrices Prices, " & _
						'"       Core " & _
						'"SET    CryptCost       = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(AES_ENCRYPT(Cost, (SELECT BaseCostPassword FROM   retclientsset WHERE  clientcode=?ClientCode)), CHAR(37), '%25'), CHAR(32), '%20'), CHAR(159), '%9F'), CHAR(161), '%A1'), CHAR(0), '%00') " & _
						'"WHERE  Prices.PriceCode= Core.PriceCode " & _
						'"   AND IF(?Cumulative, 1, Fresh) " & _
						'"   AND Core.PriceCode != ?ImpersonalPriceId ; " & _
						'" " & _
						'"UPDATE Core " & _
						'"SET    CryptCost        =concat(LEFT(CryptCost, 1), CHAR(ROUND((rand()*110)+32,0)), SUBSTRING(CryptCost,2,LENGTH(CryptCost)-4), CHAR(ROUND((rand()*110)+32,0)), RIGHT(CryptCost, 3)) " & _
						'"WHERE  LENGTH(CryptCost)>0 " & _
						'"   AND Core.PriceCode != ?ImpersonalPriceId;"

						CostOptimizer.OptimizeCostIfNeeded(readWriteConnection, CCode)

						'SelProc.CommandText = _
						'    "UPDATE ActivePrices Prices, " & _
						'    "       Core " & _
						'    "SET    CryptCost       = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(AES_ENCRYPT(Cost, (SELECT BaseCostPassword FROM   retclientsset WHERE  clientcode=?ClientCode)), CHAR(37), '%25'), CHAR(32), '%20'), CHAR(159), '%9F'), CHAR(161), '%A1'), CHAR(0), '%00') " & _
						'    "WHERE  Prices.PriceCode= Core.PriceCode " & _
						'    "   AND IF(?Cumulative, 1, Fresh) " & _
						'    "   AND Core.PriceCode != ?ImpersonalPriceId ; "
						'SelProc.ExecuteNonQuery()

						'If UpdateData.BuildNumber > 1271 Or UpdateData.NeedUpdateToCryptCost Then
						'    SelProc.CommandText = _
						'        "UPDATE ActivePrices Prices, " & _
						'        "       Core " & _
						'        "SET    CryptCost       = AES_ENCRYPT(Cost, '" & UpdateData.CostSessionKey & "') " & _
						'        "WHERE  Prices.PriceCode= Core.PriceCode " & _
						'        "   AND IF(?Cumulative, 1, Fresh) " & _
						'        "   AND Core.PriceCode != ?ImpersonalPriceId ; "
						'    SelProc.ExecuteNonQuery()
						'End If

						'GetMySQLFileWithDefaultEx( _
						' "CoreTest", _
						' SelProc, _
						' " select " & _
						' "   Core.Id, Core.CryptCost " & _
						' " from " & _
						' "   ActivePrices Prices, " & _
						' "   Core " & _
						' " where " & _
						' "       Prices.PriceCode = Core.PriceCode " & _
						' "   AND IF(?Cumulative, 1, Fresh) " & _
						' "   AND Core.PriceCode != ?ImpersonalPriceId ", _
						' False, _
						' True _
						')

						debugHelper.CopyActivePrices()

						debugHelper.Logger.DebugFormat("Before Core GED = {0}", UpdateData.Cumulative)

						debugHelper.ExportCoreCount = GetMySQLFileForCore( _
						 "Core", _
						 SelProc, _
						 helper.GetCoreCommand( _
						  False, _
						  (UpdateData.BuildNumber > 1027) Or (UpdateData.EnableUpdate() And ((UpdateData.BuildNumber >= 945) Or ((UpdateData.BuildNumber >= 705) And (UpdateData.BuildNumber <= 716)) Or ((UpdateData.BuildNumber >= 829) And (UpdateData.BuildNumber <= 837)))), _
						  (UpdateData.BuildNumber >= 1249) Or UpdateData.NeedUpdateToBuyingMatrix, _
						  False
						 ), _
						 (UpdateData.BuildNumber <= 1027) And UpdateData.EnableUpdate(), _
						 True, _
						 debugHelper
						)

						debugHelper.Logger.DebugFormat("ExportCoreCount = {0}", debugHelper.ExportCoreCount)
					Else
						'Выгружаем пустую таблицу Core
						'Делаем запрос из любой таблице (в данном случае из ActivePrices), чтобы получить 0 записей
						GetMySQLFileWithDefault("Core", SelProc, "SELECT * from ActivePrices limit 0")
					End If

					If (UpdateData.BuildNumber > 945) Or (UpdateData.EnableUpdate() And ((UpdateData.BuildNumber = 945) Or ((UpdateData.BuildNumber >= 705) And (UpdateData.BuildNumber <= 716)) Or ((UpdateData.BuildNumber >= 829) And (UpdateData.BuildNumber <= 837)))) Then
						If helper.DefineMaxProducerCostsCostId() Then
							If UpdateData.Cumulative _
							 Or (UpdateData.EnableUpdate() And ((UpdateData.BuildNumber < 1049) Or ((UpdateData.BuildNumber >= 1079) And (UpdateData.BuildNumber < 1150)))) _
							 Or helper.MaxProducerCostIsFresh() _
							Then
								GetMySQLFileWithDefault("MaxProducerCosts", SelProc, helper.GetMaxProducerCostsCommand())
							Else
								'Если прайс-лист не обновлен, то отдаем пустой файл
								GetMySQLFileWithDefault("MaxProducerCosts", SelProc, helper.GetMaxProducerCostsCommand() & " limit 0")
							End If
						Else
							GetMySQLFileWithDefault("MaxProducerCosts", SelProc, helper.GetMaxProducerCostsCommand() & " limit 0")
							Log.WarnFormat("Невозможно определить базовую цены для прайс-листа с максимальными ценами производителей. Код прайс-листа: {0}", helper.MaxProducerCostsPriceId)
						End If
					Else
						GetMySQLFileWithDefault("MaxProducerCosts", SelProc, helper.GetMaxProducerCostsCommand() & " limit 0")
					End If
				Else

					If Convert.ToInt32(SelProc.Parameters("?ImpersonalPriceFresh").Value) = 1 Then
						helper.PrepareImpersonalOffres(SelProc)

						'Выгрузка данных для обезличенного прайс-листа
						GetMySQLFileWithDefault("Core", SelProc, helper.GetCoreCommand(True, True, (UpdateData.BuildNumber >= 1249) Or UpdateData.NeedUpdateToBuyingMatrix, False))
					Else
						'выгружаем пустую таблицу Core
						GetMySQLFileWithDefault("Core", SelProc, helper.GetMaxProducerCostsCommand() & " limit 0")
					End If
					'выгружаем пустую таблицу MaxProducerCosts
					GetMySQLFileWithDefault("MaxProducerCosts", SelProc, helper.GetMaxProducerCostsCommand() & " limit 0")
				End If

				GetMySQLFileWithDefault("PricesData", SelProc, helper.GetPricesDataCommand())

				If Not UpdateData.EnableImpersonalPrice Then
					If debugHelper.NeedDebugInfo() Then
						debugHelper.FillTable("PricesData", helper.GetPricesDataCommand())
						debugHelper.FillTable("ActivePrices", "select * from ActivePrices")
						debugHelper.FillTable("AnalitFReplicationInfo", "select * from AnalitFReplicationInfo where UserId = ?UserId")
						debugHelper.FillTable("CurrentReplicationInfo", "select * from CurrentReplicationInfo")
						debugHelper.SendMail()
					End If
				End If

				AddEndOfFiles()

				helper.UpdateReplicationInfo()

				transaction.Commit()

				TS = Now().Subtract(StartTime)
				If Math.Round(TS.TotalSeconds, 0) > 30 Then
					Addition &= "Sel: " & Math.Round(TS.TotalSeconds, 0) & "; "
				End If

			Catch ex As Exception
				PrgData.Common.ConnectionHelper.SafeRollback(transaction)
				If ExceptionHelper.IsDeadLockOrSimilarExceptionInChain(ex) Then
					Log.DebugFormat("Перезапускаем транзакцию из-за deadlock")
					Thread.Sleep(2500)
					GoTo RestartTrans2
				End If
				Throw
			End Try

		Catch ex As Exception
			Me.Log.Error("Основной поток подготовки данных, Код клиента " & CCode, ex)
			If ThreadZipStream.IsAlive Then ThreadZipStream.Abort()
			ErrorFlag = True
			UpdateType = RequestType.Error
			Addition &= ex.Message
			PackProtocols()
		Finally
			if UpdateData.AsyncRequest Then DBDisconnect()
		End Try
	End Sub

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
				Dim localI As Integer
				For localI = 1 To Len(RSTUIN) Step 3
					ResStrRSTUIN &= Chr(Convert.ToInt16(Left(Mid(RSTUIN, localI), 3)))
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
			 readWriteConnection)

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
			LogRequestHelper.MailWithRequest(Log, "Ошибка в SendUData", ex)
		Finally
			DBDisconnect()
		End Try
	End Sub

	<WebMethod()> _
	Public Function GetPasswords(ByVal UniqueID As String) As String
		Return GetPasswordsEx(UniqueID, Nothing)
	End Function

	<WebMethod()> _
	Public Function GetPasswordsEx(ByVal UniqueID As String, ByVal EXEVersion As String) As String
		Dim ErrorFlag As Boolean = False
		Dim BasecostPassword As String

		Try
			DBConnect()
			GetClientCode()
			UpdateHelper.CheckUniqueId(readWriteConnection, UpdateData, UniqueID)
			If Not String.IsNullOrEmpty(EXEVersion) Then
				UpdateData.ParseBuildNumber(EXEVersion)
				UpdateHelper.UpdateBuildNumber(readWriteConnection, UpdateData)
			End If

			Dim needSessionKey = False

			If needSessionKey Then
				Cm.CommandText = "select CostSessionKey from UserUpdateInfo where UserId = " & UpdateData.UserId
				BasecostPassword = Convert.ToString(Cm.ExecuteScalar())
			Else
				Cm.CommandText = "select BaseCostPassword from retclientsset where clientcode=" & CCode
				BasecostPassword = Convert.ToString(Cm.ExecuteScalar())
			End If

			'Получаем маску разрешенных для сохранения гридов
			Cm.CommandText = "select IFNULL(sum(up.SecurityMask), 0) " & _
	"from usersettings.AssignedPermissions ap " & _
	"join usersettings.UserPermissions up on up.Id = ap.PermissionId " & _
	"where ap.UserId=" & UpdateData.UserId

			Dim SaveGridMask As UInt64 = Convert.ToUInt64(Cm.ExecuteScalar())

			If (BasecostPassword <> Nothing) Then
				Dim S As String = "Basecost=" & ToHex(BasecostPassword) & ";SaveGridMask=" & SaveGridMask.ToString("X7") & ";"
				If needSessionKey Then
					S = "SessionKey=" & ToHex(BasecostPassword) & ";SaveGridMask=" & SaveGridMask.ToString("X7") & ";"
				End If
				Return S
			Else
				Log.Error("Ошибка при получении паролей" & vbCrLf & "У клиента не заданы пароли для шифрации данных")
				Addition = "Не заданы пароли для шифрации данных"
				ErrorFlag = True
				Return "Error=При выполнении Вашего запроса произошла ошибка.;Desc=Пожалуйста, повторите попытку через несколько минут."
			End If
		Catch updateException As UpdateException
			If UpdateData IsNot Nothing Then
				Log.Warn(updateException)
			Else
				Log.Error(updateException)
			End If
			Return "Error=При выполнении Вашего запроса произошла ошибка.;Desc=Пожалуйста, повторите попытку через несколько минут."
		Catch ex As Exception
			LogRequestHelper.MailWithRequest(Log, "Ошибка при получении паролей", ex)
			Return "Error=При выполнении Вашего запроса произошла ошибка.;Desc=Пожалуйста, повторите попытку через несколько минут."
		Finally
			DBDisconnect()
		End Try
	End Function

	<WebMethod()> Public Function PostPriceDataSettings(ByVal UniqueID As String, ByVal PriceCodes As Int32(), ByVal RegionCodes As Int64(), ByVal INJobs As Boolean()) As String

		Return PostPriceDataSettingsEx(UniqueID, Nothing, PriceCodes, RegionCodes, INJobs)

	End Function

	<WebMethod()> Public Function PostPriceDataSettingsEx(ByVal UniqueID As String, ByVal EXEVersion As String, ByVal PriceCodes As Int32(), ByVal RegionCodes As Int64(), ByVal INJobs As Boolean()) As String
		Dim ErrorFlag As Boolean = False
		Dim transaction As MySqlTransaction = Nothing

		Try
			UpdateType = RequestType.PostPriceDataSettings
			DBConnect()
			GetClientCode()
			UpdateHelper.CheckUniqueId(readWriteConnection, UpdateData, UniqueID, UpdateType)
			If Not String.IsNullOrEmpty(EXEVersion) Then
				UpdateData.ParseBuildNumber(EXEVersion)
				UpdateHelper.UpdateBuildNumber(readWriteConnection, UpdateData)
			End If

			Dim helper = New UpdateHelper(UpdateData, readWriteConnection)
			helper.UpdatePriceSettings(PriceCodes, RegionCodes, INJobs)
			Return "Res=OK"

		Catch updateException As UpdateException
			Return ProcessUpdateException(updateException)
		Catch ex As Exception
			LogRequestHelper.MailWithRequest(Log, "Ошибка при применении обновлений настроек прайс-листов", ex)
			ErrorFlag = True
			Return "Error=При выполнении Вашего запроса произошла ошибка.;Desc=Пожалуйста, повторите попытку через несколько минут."
		Finally
			DBDisconnect()
		End Try
	End Function

	<WebMethod()> Public Function GetReclame() As String
		Dim MaxReclameFileDate As Date
		Dim NewZip As Boolean = True
		Dim CurrentFilesSize As Long = 0
		Dim MaxFilesSize As Long = 1024*1024

		If Log.IsDebugEnabled Then Log.Debug("Вызвали GetReclame")

		Dim FileCount = 0
		Try
			DBConnect()
			GetClientCode()

			Dim updateHelpe = New UpdateHelper(UpdateData, readWriteConnection)

			Dim reclameData = updateHelpe.GetReclame()

			If Not reclameData.ShowAdvertising Then
				GetReclame = ""
				If Log.IsDebugEnabled Then Log.Debug("Закончили GetReclame с результатом (Not reclameData.ShowAdvertising)")
				Exit Function
			End If

			MaxReclameFileDate = reclameData.ReclameDate
			If Log.IsDebugEnabled Then Log.DebugFormat("Прочитали из базы reclameData.ReclameDate {0}", reclameData.ReclameDate)

			Reclame = True
			ReclamePath = ResultFileName & "Reclame\" & reclameData.Region & "\"
			If Log.IsDebugEnabled Then Log.DebugFormat("Путь к рекламе {0}", ReclamePath)

			ShareFileHelper.MySQLFileDelete(UpdateData.GetReclameFile())

			Dim FileList As String()
			Dim FileName As String

			If Not Directory.Exists(ReclamePath) Then
				Try
					Directory.CreateDirectory(ReclamePath)
				Catch ex As Exception
					Throw New Exception(String.Format("Ошибка при создании директории '{0}'", ReclamePath), ex)
				End Try
			End If

			FileList = reclameData.GetReclameFiles(ReclamePath)
			If Log.IsDebugEnabled Then Log.DebugFormat("Кол-во файлов в каталоге с рекламой {0}", FileList.Length)
			For Each FileName In FileList

				FileInfo = New FileInfo(FileName)

				If FileInfo.LastWriteTime.Subtract(reclameData.ReclameDate).TotalSeconds > 1 Then

					if CurrentFilesSize + FileInfo.Length < MaxFilesSize Then
						If Log.IsDebugEnabled Then Log.DebugFormat("Добавили файл в архив {0}", FileInfo.Name)
						FileCount += 1

						SyncLock (FilesForArchive)

							FilesForArchive.Enqueue(New FileForArchive(FileInfo.Name, True))

						End SyncLock

						If FileInfo.LastWriteTime > MaxReclameFileDate Then MaxReclameFileDate = FileInfo.LastWriteTime
					Else 
						Log.ErrorFormat("Файл {0} превышает допустимый размер рекламы в 1 Мб", FileName)
						Exit For
					End If

				End If

			Next

			If MaxReclameFileDate > Now() Then MaxReclameFileDate = Now()

			If Log.IsDebugEnabled Then Log.DebugFormat("После обработки файлов MaxReclameFileDate {0}", MaxReclameFileDate)

			If FileCount > 0 Then

				AddEndOfFiles()

				ZipStream()

				If Log.IsDebugEnabled Then Log.Debug("Успешно завершили архивирование")

				FileInfo = New FileInfo(UpdateData.GetReclameFile())
				FileInfo.CreationTime = MaxReclameFileDate

				If Log.IsDebugEnabled Then Log.Debug("Установили дату создания файла-архива")
			End If

		Catch updateException As UpdateException
			ErrorFlag = True
			If UpdateData IsNot Nothing Then
				Log.Warn(updateException)
			Else
				Log.Error(updateException)
			End If
			Return ""
		Catch ex As Exception
			LogRequestHelper.MailWithRequest(Log, "Ошибка при загрузке рекламы", ex)
			ErrorFlag = True
			Return ""
		Finally
			DBDisconnect()
		End Try

		If ErrorFlag Then
			GetReclame = ""
			If Log.IsDebugEnabled Then Log.Debug("Закончили GetReclame с результатом (ErrorFlag)")
		Else
			If FileCount > 0 Then

				GetReclame = "URL=" & UpdateHelper.GetFullUrl("GetFileReclameHandler.ashx") & ";New=" & True
				If Log.IsDebugEnabled Then Log.Debug("Закончили GetReclame с результатом (URL)")

			Else
				GetReclame = ""
				If Log.IsDebugEnabled Then Log.Debug("Закончили GetReclame с результатом (FileCount <= 0)")
			End If
		End If
	End Function

	<WebMethod()> Public Function ReclameComplete() As Boolean
		Dim transaction As MySqlTransaction = Nothing
		If Log.IsDebugEnabled Then Log.Debug("Вызвали ReclameComplete")
		Try
			DBConnect()
			GetClientCode()

			FileInfo = New FileInfo(UpdateData.GetReclameFile())

			If FileInfo.Exists Then

				If Log.IsDebugEnabled Then Log.DebugFormat("Устанавливаем дату рекламы FileInfo.CreationTime {0}", FileInfo.CreationTime)

				transaction = readWriteConnection.BeginTransaction(IsoLevel)
				Cm.CommandText = "update UserUpdateInfo set ReclameDate=?ReclameDate where UserId=" & UserId
				Cm.Parameters.AddWithValue("?ReclameDate", FileInfo.CreationTime)
				Cm.Connection = readWriteConnection
				Cm.ExecuteNonQuery()
				transaction.Commit()

				If Log.IsDebugEnabled Then Log.Debug("Дата рекламы успешно установлена")
			Else
				If Log.IsDebugEnabled Then Log.DebugFormat("Файл-архив с рекламой не существует {0}", UpdateData.GetReclameFile())
			End If

			Reclame = True
			ShareFileHelper.MySQLFileDelete(UpdateData.GetReclameFile())
			ReclameComplete = True
			If Log.IsDebugEnabled Then Log.Debug("Успешно завершили ReclameComplete")
		Catch ex As Exception
			PrgData.Common.ConnectionHelper.SafeRollback(transaction)
			LogRequestHelper.MailWithRequest(Log, "Подтверждение рекламы", ex)
			ReclameComplete = False
		Finally
			DBDisconnect()
		End Try
	End Function

	Private Sub ProcessCommitExchange()
		Try
			Dim helper = New UpdateHelper(UpdateData, readWriteConnection)
			helper.CommitExchange()
		Catch err As Exception
			Log.Error("Присвоение значений максимальных синонимов", err)
			Addition = err.Message
			UpdateType = RequestType.Error
			ErrorFlag = True
		End Try
	End Sub

	Private Sub ProcessOldCommit(ByVal AbsentPriceCodes As String)
		Try
			Dim helper = New UpdateHelper(UpdateData, readWriteConnection)
			helper.OldCommit(AbsentPriceCodes)
			Addition &= "!!! " & AbsentPriceCodes
		Catch err As Exception
			Log.Error("Присвоение значений максимальных синонимов", err)
			Addition = err.Message
			UpdateType = RequestType.Error
			ErrorFlag = True
		End Try
	End Sub

	Private Sub ProcessResetAbsentPriceCodes(ByVal AbsentPriceCodes As String)
		Try
			Dim helper = New UpdateHelper(UpdateData, readWriteConnection)
			helper.ResetAbsentPriceCodes(AbsentPriceCodes)
			Addition &= "!!! " & AbsentPriceCodes
		Catch err As Exception
			Log.Error("Сброс информации по прайс-листам с недостающими синонимами", err)
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

	Private Function GetFileNameForMySql(ByVal outFileName As String) As String
		Dim fullName = ServiceContext.GetFileByShared(outFileName)
		Return MySql.Data.MySqlClient.MySqlHelper.EscapeString(fullName)
	End Function

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

			SQL &= " INTO OUTFILE '" & GetFileNameForMySql(FileName & UserId & ".txt") & "' "
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

	Private Function GetMySQLFileForCore(ByVal FileName As String, ByVal MyCommand As MySqlCommand, ByVal SQLText As String, ByVal SetCumulative As Boolean, ByVal AddToQueue As Boolean, ByRef debughelper As DebugReplicationHelper) As Integer
		Dim SQL As String = SQLText
		Dim oldCumulative As Boolean
		Dim Result As Integer = 0

		Try
			debughelper.Logger.DebugFormat("For Core flag SetCumulative = {0}", SetCumulative)
			debughelper.Logger.DebugFormat("For Core Params: {0}", MyCommand.Parameters.Cast(Of MySqlParameter)().Select(Function(param) String.Format("{0} = {1}", param.ParameterName, param.Value)).Implode())
			If SetCumulative And MyCommand.Parameters.Contains("?Cumulative") Then
				oldCumulative = MyCommand.Parameters("?Cumulative").Value
				MyCommand.Parameters("?Cumulative").Value = True
			End If

			SQL &= " INTO OUTFILE '" & GetFileNameForMySql(FileName & UserId & ".txt") & "' "
			MyCommand.CommandText = SQL
			Result = MyCommand.ExecuteNonQuery()

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

		Return Result

	End Function

	<WebMethod()> _
	Public Function GetHistoryOrders( _
		ByVal EXEVersion As String, _
		ByVal UniqueID As String, _
		ByVal ExistsServerOrderIds As UInt64(), _
		ByVal MaxOrderId As UInt64, _
		ByVal MaxOrderListId As UInt64 _
	) As String

		Return GetHistoryOrdersWithDocs(EXEVersion, UniqueID, ExistsServerOrderIds, MaxOrderId, MaxOrderListId, Nothing)

	End Function

	<WebMethod()> _
	Public Function GetHistoryOrdersWithDocs( _
		ByVal EXEVersion As String, _
		ByVal UniqueID As String, _
		ByVal ExistsServerOrderIds As UInt64(), _
		ByVal MaxOrderId As UInt64, _
		ByVal MaxOrderListId As UInt64, _
		ByVal ExistsDocIds As UInt64() _
	) As String

		Dim ResStr As String = String.Empty

		Try
			UpdateType = RequestType.GetHistoryOrders
			GetHistory = True

			DBConnect()
			GetClientCode()
			UpdateData.LastLockId = Counter.TryLock(UserId, "GetHistoryOrders")
			UpdateHelper.CheckUniqueId(readWriteConnection, UpdateData, UniqueID, UpdateType)
			UpdateData.ParseBuildNumber(EXEVersion)
			UpdateHelper.UpdateBuildNumber(readWriteConnection, UpdateData)

			If UpdateData.EnableImpersonalPrice Then
				Throw New UpdateException( _
					"Доступ закрыт.", _
					"Для копии с обезличенным прайс-листом недоступна загрузка истории заказов.", _
					"Логин " & UserName & " с обезличенным прайс-листом недоступна загрузка истории заказов; ", _
					RequestType.Forbidden)
			End If

			Dim historyIds As String = String.Empty
			If (ExistsServerOrderIds.Length > 0) AndAlso (ExistsServerOrderIds(0) <> 0) Then
				Dim d = ExistsServerOrderIds.Select(Function(item) item.ToString())
				If d.Count > 0 Then historyIds = String.Join(",", d.ToArray())
			End If

			Dim historyDocIds As String = Nothing
			if UpdateData.AllowHistoryDocs() Then
				If (ExistsDocIds IsNot Nothing) AndAlso (ExistsDocIds.Length > 0) AndAlso (ExistsDocIds(0) <> 0) Then
					Dim d = ExistsDocIds.Select(Function(item) item.ToString())
					If d.Count > 0 Then historyDocIds = String.Join(",", d.ToArray())
				Else 
					historyDocIds = String.Empty
				End If
			End If

			SelProc = New MySqlCommand
			SelProc.Connection = readWriteConnection
			Dim transaction = readWriteConnection.BeginTransaction(IsoLevel)
			SelProc.Transaction = transaction

			Try

				ShareFileHelper.MySQLFileDelete(ServiceContext.GetFileByLocal("PostedOrderHeads" & UserId & ".txt"))
				ShareFileHelper.MySQLFileDelete(ServiceContext.GetFileByLocal("PostedOrderLists" & UserId & ".txt"))

				SelProc.Parameters.Clear()
				SelProc.Parameters.AddWithValue("?UserId", UpdateData.UserId)
				SelProc.Parameters.AddWithValue("?ClientId", UpdateData.ClientId)

				SelProc.CommandText = _
					"set @MaxOrderId := " & MaxOrderId & ";" & _
					"set @MaxOrderListId := " & MaxOrderListId & ";"
				SelProc.ExecuteNonQuery()

				SelProc.CommandText = _
					"drop temporary table IF EXISTS HistoryIds;" & _
					"create temporary table HistoryIds engine=MEMORY as " & _
					"select " & _
						" (@MaxOrderId := @MaxOrderId + 1) as PostOrderId, " & _
						" RowId as ServerOrderId " & _
					" from orders.OrdersHead " & _
					"      left join Customers.useraddresses on useraddresses.AddressId = OrdersHead.AddressId " & _
					" where "

				Dim ClientIdAsField As String
				SelProc.CommandText &= " (useraddresses.UserId is not null and useraddresses.UserId = ?UserId)  "
				ClientIdAsField = "OrdersHead.AddressId"

				SelProc.CommandText &= " and OrdersHead.deleted = 0 and OrdersHead.processed = 1 "

				If Not String.IsNullOrEmpty(historyIds) Then
					SelProc.CommandText &= " and OrdersHead.RowId not in (" & historyIds & ");"
				Else
					SelProc.CommandText &= ";"
				End If

				SelProc.ExecuteNonQuery()

				SelProc.CommandText = "select count(*) from HistoryIds"
				Dim historyOrdersCount = Convert.ToInt32(SelProc.ExecuteScalar())

				Dim historyDocsCount As Int32 = 0

				if (historyDocIds Isnot Nothing) Then
					SelProc.CommandText = _
						" update Logs.DocumentSendLogs ds " & _
						" set ds.Committed = 0, ds.FileDelivered = 0, ds.DocumentDelivered = 0 " & _
						" where ds.UserId = " & + UpdateData.UserId.ToString()
						
					if Not String.IsNullOrEmpty(historyDocIds) then
						SelProc.CommandText &= " and ds.DocumentId not in (" & historyDocIds & ")"
					End If

					historyDocsCount = SelProc.ExecuteNonQuery()
				End If


				if historyDocIds Is Nothing then
					If historyOrdersCount = 0 Then
						AnalitFUpdate.InsertAnalitFUpdatesLog(readWriteConnection, UpdateData, UpdateType, "С сервера загружена вся история заказов")
						Return "FullHistory=True"
					End If
				Else 
					If historyOrdersCount = 0 AndAlso historyDocsCount = 0 Then
						AnalitFUpdate.InsertAnalitFUpdatesLog(readWriteConnection, UpdateData, UpdateType, "С сервера загружена вся история заказов/документов")
						Return "FullHistory=True"
					End If
				End If

				'OrdersHead
				'AddressId, UserId,  PriceDate, RowCount, Processed, Submited, Deleted, SubmitDate, ClientOrderId, CalculateLeader, 
				GetMySQLFileWithDefault( _
				 "PostedOrderHeads", _
				 SelProc, _
				 "select " & _
				 "  HistoryIds.PostOrderId as OrderId, " & _
				 "  OrdersHead.RowID as ServerOrderId, " & _
				 "  " & ClientIdAsField & " as ClientId, " & _
				 "  OrdersHead.PriceCode, " & _
				 "  OrdersHead.RegionCode, " & _
				 "  OrdersHead.WriteTime as SendDate, " & _
				 "  OrdersHead.ClientAddition as MessageTO, " & _
				 "  OrdersHead.DelayOfPayment,  " & _
				 "  date_sub(OrdersHead.PriceDate, interval time_to_sec(date_sub(now(), interval unix_timestamp() second)) second) as PriceDate  " & _
				 "from " & _
				 " HistoryIds " & _
				 " inner join orders.OrdersHead on OrdersHead.RowId = HistoryIds.ServerOrderId ")

				'OrdersList
				'RowID, OrderID, CoreId, 
				'OrderedOffers
				'ID,  MinBoundCost, MaxBoundCost, CoreUpdateTime, CoreQuantityUpdate, 
				GetMySQLFileWithDefault( _
				 "PostedOrderLists", _
				 SelProc, _
				 "select " & _
				 "  (@MaxOrderListId := @MaxOrderListId + 1) as PostOrderListId, " & _
				 "  HistoryIds.PostOrderId as OrderId, " & _
				 "  " & ClientIdAsField & " as ClientId, " & _
				 "  OrdersList.ProductId, " & _
				 "  OrdersList.CodeFirmCr, " & _
				 "  OrdersList.SynonymCode, " & _
				 "  OrdersList.SynonymFirmCrCode, " & _
				 "  OrdersList.Code, " & _
				 "  OrdersList.CodeCr, " & _
				 "  OrdersList.Await, " & _
				 "  OrdersList.Junk, " & _
				 "  OrdersList.Quantity as OrderCount, " & _
				 "  if(OrdersHead.DelayOfPayment is null or OrdersHead.DelayOfPayment = 0, OrdersList.Cost, cast(OrdersList.Cost * (1 + OrdersHead.DelayOfPayment/100) as decimal(18, 2)))  as Price, " & _
				 "  OrdersList.Cost as RealPrice, " & _
				 "  OrdersList.RequestRatio, " & _
				 "  OrdersList.OrderCost, " & _
				 "  OrdersList.MinOrderCount, " & _
				 "  OrdersList.SupplierPriceMarkup, " & _
				 "  OrdersList.RetailMarkup, " & _
				 "  OrderedOffers.Unit, " & _
				 "  OrderedOffers.Volume, " & _
				 "  OrderedOffers.Note, " & _
				 "  OrderedOffers.Period, " & _
				 "  OrderedOffers.Doc, " & _
				 "  OrderedOffers.VitallyImportant, " & _
				 "  OrderedOffers.Quantity as CoreQuantity, " & _
				 "  OrderedOffers.RegistryCost, " & _
				 "  OrderedOffers.ProducerCost, " & _
				 "  OrderedOffers.NDS, " & _
				 "  OrdersList.RetailCost, " & _
				 "  OrdersList.RowID as ServerOrderListId " & _
				 "from " & _
				 " HistoryIds " & _
				 " inner join orders.OrdersHead on OrdersHead.RowId = HistoryIds.ServerOrderId " & _
				 " inner join orders.OrdersList on OrdersList.OrderId = HistoryIds.ServerOrderId " & _
				 " left join orders.OrderedOffers on OrderedOffers.Id = OrdersList.RowId ")

				transaction.Commit()

				'Начинаем архивирование
				ThreadZipStream.Start()

				AddEndOfFiles()

			Catch ex As Exception
				PrgData.Common.ConnectionHelper.SafeRollback(transaction)
				Me.Log.Error("Подготовка истории заказов, Код клиента " & CCode, ex)
				If ThreadZipStream.IsAlive Then ThreadZipStream.Abort()
				ErrorFlag = True
				UpdateType = RequestType.Error
				Addition &= ex.Message
			End Try


endproc:
			If Not PackFinished And ThreadZipStream.IsAlive And Not ErrorFlag Then

				'Если есть ошибка, прекращаем подготовку данных
				If ErrorFlag Then

					If ThreadZipStream.IsAlive Then ThreadZipStream.Abort()

					PackFinished = True

				End If
				Thread.Sleep(1000)

				GoTo endproc

			ElseIf Not PackFinished And Not ErrorFlag And (UpdateType <> RequestType.Forbidden) Then

				Addition &= "; Нет работающих потоков, данные c историей заказов не готовы."
				UpdateType = RequestType.Forbidden

				ErrorFlag = True

			End If

			If Len(Addition) = 0 Then Addition = MessageH & " " & MessageD

			If Not ErrorFlag Then
				Dim ArhiveTS = Now().Subtract(ArhiveStartTime)

				If Math.Round(ArhiveTS.TotalSeconds, 0) > 30 Then
					Addition &= "Архивирование: " & Math.Round(ArhiveTS.TotalSeconds, 0) & "; "
				End If
			End If


			ProtocolUpdatesThread.Start()

			If ErrorFlag Then

				If Len(MessageH) = 0 Then
					ResStr = "Error=При подготовке истории заказов произошла ошибка.;Desc=Пожалуйста, повторите запрос данных через несколько минут."
				Else
					ResStr = "Error=" & MessageH & ";Desc=" & MessageD
				End If

			Else

				While GUpdateId = 0
					Thread.Sleep(500)
				End While

				ResStr = "URL=" & UpdateHelper.GetFullUrl("GetFileHistoryHandler.ashx") & "?Id=" & GUpdateId

			End If

			Return ResStr
		Catch updateException As UpdateException
			Return ProcessUpdateException(updateException)
		Catch ex As Exception
			LogRequestHelper.MailWithRequest(Log, "Ошибка при запросе истории заказов", ex)
			Return "Error=Запрос истории заказов завершился неудачно.;Desc=Пожалуйста, повторите попытку через несколько минут."
		Finally
			Counter.ReleaseLock(UserId, "GetHistoryOrders", UpdateData)
			DBDisconnect()
		End Try

	End Function

    <WebMethod()> _
    Public Function CommitHistoryOrders( _
        ByVal UpdateId As UInt64) As Boolean

        Dim transaction As MySqlTransaction = Nothing
        If Log.IsDebugEnabled Then Log.Debug("Вызвали CommitHistoryOrders")
        Try
            DBConnect()
            GetClientCode()

            FileInfo = New FileInfo(UpdateData.GetOrdersFile())

            If FileInfo.Exists Then

                If Log.IsDebugEnabled Then Log.DebugFormat("Устанавливаем дату рекламы FileInfo.CreationTime {0}", FileInfo.CreationTime)

                transaction = readWriteConnection.BeginTransaction(IsoLevel)

                Cm.CommandText = "update `logs`.`AnalitFUpdates` set Commit=1 where UpdateId = ?UpdateId"
                Cm.Parameters.AddWithValue("?UpdateId", UpdateId)
                Cm.Connection = readWriteConnection
                Cm.ExecuteNonQuery()

				Cm.CommandText = UpdateHelper.GetConfirmDocumentsCommnad(UpdateId)
				Cm.ExecuteNonQuery()

                transaction.Commit()

                If Log.IsDebugEnabled Then Log.Debug("Архив с заказами успешно подтвержден")
            Else
                If Log.IsDebugEnabled Then Log.DebugFormat("Файл-архив с историей заказов не существует {0}", UpdateData.GetOrdersFile())
            End If

            GetHistory = True
            ShareFileHelper.MySQLFileDelete(UpdateData.GetOrdersFile())
            CommitHistoryOrders = True
            If Log.IsDebugEnabled Then Log.Debug("Успешно завершили CommitHistoryOrders")
        Catch ex As Exception
            PrgData.Common.ConnectionHelper.SafeRollback(transaction)
            LogRequestHelper.MailWithRequest(Log, "Подтверждение истории заказов", ex)
            CommitHistoryOrders = False
        Finally
            DBDisconnect()
        End Try
    End Function

    Private Sub DeletePreviousFiles()
        Dim deleteFiles = Directory.GetFiles(ResultFileName, UpdateData.GetOldFileMask())

        For Each deleteFile In deleteFiles
            If File.Exists(deleteFile) Then
                ShareFileHelper.MySQLFileDelete(deleteFile)
                Log.DebugFormat("Удалили файл с предыдущими подготовленными данными: {0}", deleteFile)
            End If
        Next
    End Sub

    <WebMethod()> _
    Public Function ConfirmUserMessage( _
        ByVal EXEVersion As String, _
        ByVal UniqueID As String, _
        ByVal ConfirmedMessage As String _
    ) As String

        Try
            UpdateType = RequestType.ConfirmUserMessage
            DBConnect()
            GetClientCode()
            UpdateHelper.CheckUniqueId(readWriteConnection, UpdateData, UniqueID, UpdateType)
            UpdateData.ParseBuildNumber(EXEVersion)
            UpdateHelper.UpdateBuildNumber(readWriteConnection, UpdateData)

            If Not UpdateData.Message.Equals(ConfirmedMessage.Trim(), StringComparison.OrdinalIgnoreCase) Then
                Me.Log.DebugFormat("Пользовательское сообщение уже подтверждено или изменено: ConfirmedMessage:{0};  Message:{1};", ConfirmedMessage, UpdateData.Message)
            End If

            Dim helper = New UpdateHelper(UpdateData, readWriteConnection)

            helper.ConfirmUserMessage(ConfirmedMessage)

            Return "Res=OK"

        Catch updateException As UpdateException
            Return ProcessUpdateException(updateException)
        Catch ex As Exception
            LogRequestHelper.MailWithRequest(Log, "Ошибка при подтверждении пользовательского сообщения", ex)
            ErrorFlag = True
			Return "Error=При выполнении Вашего запроса произошла ошибка.;Desc=Пожалуйста, повторите попытку через несколько минут."
        Finally
            DBDisconnect()
        End Try
    End Function

    <WebMethod()> _
    Public Function SendUserActions( _
        ByVal EXEVersion As String, _
        ByVal UpdateId As UInt64, _
        ByVal UserActionLogsFile As String _
    ) As String

        Try
            UpdateType = RequestType.SendUserActions
            DBConnect()
            GetClientCode()
            UpdateData.ParseBuildNumber(EXEVersion)
            UpdateHelper.UpdateBuildNumber(readWriteConnection, UpdateData)

            Dim helper = New SendUserActionsHandler(UpdateData, UpdateId, readWriteConnection)

            Try
                helper.PrepareLogFile(UserActionLogsFile)

                Dim importCount = helper.ImportLogFile()

                Log.DebugFormat("Количество импортированных записей статистики пользователя: {0}", importCount)

            Finally
                helper.DeleteTemporaryFiles()
            End Try

            Return "Res=OK"

        Catch updateException As UpdateException
            Return ProcessUpdateException(updateException)
        Catch ex As Exception
            LogRequestHelper.MailWithRequest(Log, "Ошибка при обработки пользовательской статистики", ex)
            ErrorFlag = True
            Return "Error=При выполнении Вашего запроса произошла ошибка.;Desc=Пожалуйста, повторите попытку через несколько минут."
        Finally
            DBDisconnect()
        End Try
    End Function

End Class

