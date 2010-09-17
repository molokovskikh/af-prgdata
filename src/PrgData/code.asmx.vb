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

<WebService(Namespace:="IOS.Service")> _
Public Class PrgDataEx
	Inherits System.Web.Services.WebService

	Const SevenZipExe As String = "C:\Program Files\7-Zip\7z.exe"

	Public Sub New()
		MyBase.New()

		InitializeComponent()

		Try
            _simpleConnectionManager = New Global.Common.MySql.SimpleConnectionManager()
            ArchiveHelper.SevenZipExePath = SevenZipExe
            ResultFileName = ServiceContext.GetResultPath()
        Catch ex As Exception
            Log.Error("������ ��� ������������� ����������", ex)
        End Try

    End Sub

    Private _simpleConnectionManager As Global.Common.MySql.SimpleConnectionManager
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

    ReadOnly ��������������� As String = System.Configuration.ConfigurationManager.AppSettings("DocumentsPath")

    'ReadOnly ZipProcessorAffinityMask As Integer = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings("ZipProcessorAffinity"))

    Private Const IsoLevel As System.Data.IsolationLevel = IsolationLevel.ReadCommitted
    Private FileInfo As System.IO.FileInfo
    Private UserName, MessageD As String
    '������ � ������ �����-������, � ������� ����������� �������� �� �������
    Private AbsentPriceCodes As String
    Private MessageH As String
    Private i As Integer
    Private ErrorFlag, Documents As Boolean
    Private Addition, ClientLog As String
    Private Reclame As Boolean
    Private GetHistory As Boolean
    Public ResultFileName As String
    Dim ArhiveStartTime As DateTime

    '������
    Private ThreadZipStream As New Thread(AddressOf ZipStream)
    Private BaseThread As Thread 'New Thread(AddressOf BaseProc)
    Private ProtocolUpdatesThread As New Thread(AddressOf ProtocolUpdates)

    Private CurUpdTime, OldUpTime As DateTime
    Private LimitedCumulative As Boolean
    Private UpdateType As RequestType
    Private ResultLenght, OrderId As UInt32
    Dim CCode, UserId As UInt32
    Private SpyHostsFile, SpyAccount As Boolean
    Dim UpdateData As UpdateData
    Private UserHost, ReclamePath As String
    Private UncDT As Date
    Private GED, PackFinished, CalculateLeader As Boolean
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
    Private readWriteConnection As MySql.Data.MySqlClient.MySqlConnection

    Private FilesForArchive As Queue(Of FileForArchive) = New Queue(Of FileForArchive)

    Private Log As ILog = LogManager.GetLogger(GetType(PrgDataEx))


    Private Function MySqlFilePath() As String
#If DEBUG Then
        Return System.Configuration.ConfigurationManager.AppSettings("MySqlFilePath") & "\"
#Else
        Return Path.Combine("\\" & Environment.MachineName, System.Configuration.ConfigurationManager.AppSettings("MySqlFilePath")) & "\"
#End If
    End Function


    Private Function MySqlLocalFilePath() As String
#If DEBUG Then
        Return System.Configuration.ConfigurationManager.AppSettings("MySqlLocalFilePath")
#Else
        Return System.Configuration.ConfigurationManager.AppSettings("MySqlLocalFilePath")
#End If
    End Function

    '�������� ������ � ���������� ���
    <WebMethod()> _
    Public Function SendLetter(ByVal subject As String, ByVal body As String, ByVal attachment() As Byte) As String
        Try
            Dim updateData As UpdateData
            Using connection = _simpleConnectionManager.GetConnection()
                connection.Open()
                updateData = UpdateHelper.GetUpdateData(connection, HttpContext.Current.User.Identity.Name)
                updateData.ClientHost = ServiceContext.GetUserHost()

                If updateData Is Nothing Then
                    Throw New Exception("������ �� ������")
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

            End Using

            Return "Res=OK"
        Catch ex As Exception
            LogRequestHelper.MailWithRequest(Log, "������ ��� �������� ������", ex)
            Return "Error=�� ������� ��������� ������. ���������� �������."
        End Try
    End Function

    '��������� ���������
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
            If CheckUIN Then UpdateData.ParseBuildNumber(EXEVersion)

            Dim tmpWaybillFolder = Path.GetTempPath() + Path.GetFileNameWithoutExtension(Path.GetTempFileName())
            Dim tmpWaybillArchive = tmpWaybillFolder + "\waybills.7z"


            Directory.CreateDirectory(tmpWaybillFolder)

            Try

                Using fileWaybills As New FileStream(tmpWaybillArchive, FileMode.CreateNew)
                    fileWaybills.Write(Waybills, 0, Waybills.Length)
                End Using

                If Not ArchiveHelper.TestArchive(tmpWaybillArchive) Then
                    Throw New Exception("���������� ����� ���������.")
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
                        Log.Error("������ ��� �������� ���������� ���������� ��� ��������� ���������", ex)
                    End Try
                End If
            End Try

        Catch updateException As UpdateException
            ProcessUpdateException(updateException)
            Return "Status=1"
        Catch ex As Exception
            LogRequestHelper.MailWithRequest(Log, "������ ��� �������� ���������", ex)
            Return "Status=1"
        Finally
            DBDisconnect()
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
        '            '    MailMessage &= "Hash ���������� �� ������: " & LibraryName(i) & ", � �������: Hash: " & LibraryHash(i) & ", ������: " & LibraryVersion(i) & "; "
        '            'End If
        '        Else
        '            'MailMessage &= "�� ��������� �� ������� ����������: " & LibraryName(i) & ", ������: " & LibraryVersion(i) & ", hash: " & LibraryHash(i) & "; "
        '        End If

        '    Next
        '    If MailMessage.Length > 0 Then
        '        'Addition &= MailMessage
        '        'MailHelper.MailErr(CCode, "������ �������� ������ ���������", MailMessage)
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
        Addition = " ��: " & WINVersion & " " & WINDesc & "; "

        Try

            '�������� ������� ����������
            If (Not ProcessBatch) Then UpdateType = RequestType.GetData
            LimitedCumulative = False

            '��� ����������� ������
            ErrorFlag = False

            '������ ���������
            Documents = WayBillsOnly

            '�� ������� ������ ������ �� ��
            GED = GetEtalonData

            '�������� ��� � ��������� ������� �������
            If (Not ProcessBatch) Then
                CCode = 0
                DBConnect()
                GetClientCode()
                Counter.TryLock(UserId, "GetUserData")
                UpdateHelper.CheckUniqueId(readWriteConnection, UpdateData, UniqueID)
                '����������� ������ ���������� � ����
                UpdateData.ParseBuildNumber(EXEVersion)
            End If

            Dim helper = New UpdateHelper(UpdateData, readWriteConnection)

            '���� � ������� ���������� ���������� ����� �������������� �������
            If Not Documents Then

                helper.UpdateBuildNumber()

                '���� ����������� ����� ���������� ���������� �� ������ � �������
                If Not GED AndAlso (OldUpTime <> AccessTime.ToLocalTime) Then
                    If (UpdateData.BuildNumber > 1079) And (Now.AddDays(-Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings("AccessTimeHistoryDepth"))) < AccessTime.ToLocalTime) Then
                        Try
                            Addition &= String.Format("����� ���������� �� ������� �� ������� � �������, ������� ��������� ��; ��������� ���������� ������ {0}, ������ {1}", OldUpTime, AccessTime.ToLocalTime)
                            LimitedCumulative = True
                            OldUpTime = AccessTime.ToLocalTime()
                            helper.PrepareLimitedCumulative(OldUpTime)
                        Catch err As Exception
                            MailHelper.MailErr(CCode, "���������� � ���������� ��", err.ToString())
                            Addition = err.Message
                            UpdateType = RequestType.Error
                            ErrorFlag = True
                            GoTo endproc
                        End Try
                    Else
                        GED = True
                        Addition &= String.Format("����� ���������� �� ������� �� ������� � �������, ������� ��; ��������� ���������� ������ {0}, ������ {1}", OldUpTime, AccessTime.ToLocalTime)
                    End If
                End If


                '� ����������� �� ������ ���������� ���� �� �������� ���������� ������: ��� ������� Firebird � ��� ������� MySql
                If UpdateData.BuildNumber > 716 Then
                    '���� ���������� ���������� 945 ������ �� ����� � ���������� ��� ��� ������ ��� � ���������� ���, �� ��������� ��� ��� �����: ��� � ��������
                    If ((UpdateData.BuildNumber = 945) And UpdateData.EnableUpdate) Or (UpdateData.BuildNumber > 945) Then
                    Else
                        If (UpdateData.BuildNumber >= 829) And (UpdateData.BuildNumber <= 837) And UpdateData.EnableUpdate Then
                            Addition &= "������������ ���������� ��������� � 800-� ������ �� MySql; "
                        Else
                            'FileCount = 16
                        End If
                    End If
                    BaseThread = New Thread(AddressOf MySqlProc)
                Else
                    If ((UpdateData.BuildNumber >= 705) And (UpdateData.BuildNumber <= 716)) And UpdateData.EnableUpdate Then
                        BaseThread = New Thread(AddressOf MySqlProc)
                        'FileCount = 19
                        GED = True
                        Addition &= "������������ ���������� ��������� � Firebird �� MySql, ������� ��; "
                    Else
                        BaseThread = New Thread(AddressOf FirebirdProc)
                    End If
                End If

                '������� ������������
                If GED Then

                    If (Not ProcessBatch) Then UpdateType = RequestType.GetCumulative

                    helper.ResetReclameDate()

                Else

                    If LimitedCumulative Then helper.ResetReclameDate()

                    '���������� ���� �����-������, � ������� ��������� ���������
                    AbsentPriceCodes = String.Empty
                    If (PriceCodes IsNot Nothing) AndAlso (PriceCodes.Length > 0) AndAlso (PriceCodes(0) <> 0) Then
                        AbsentPriceCodes = PriceCodes(0).ToString
                        Dim I As Integer
                        For I = 1 To PriceCodes.Length - 1
                            AbsentPriceCodes &= "," & PriceCodes(I)
                        Next
                    End If
                    If Not String.IsNullOrEmpty(AbsentPriceCodes) Then ProcessResetAbsentPriceCodes(AbsentPriceCodes)

                    If UpdateData.NeedUpdateToBuyingMatrix Then helper.SetForceReplication()

                End If

            End If

            If Documents Then

                CurUpdTime = Now()

                UpdateType = RequestType.GetDocs
                Try
                    ShareFileHelper.MySQLFileDelete(ResultFileName & UserId & ".zip")
                    Log.DebugFormat("��� ���������� ���������� ������ ���������� ����: {0}", ResultFileName & UserId & ".zip")
                Catch ex As Exception
                    Addition &= "�� ������� ������� ���������� ������ (��������� ������ ����������): " & ex.Message & "; "
                    UpdateType = RequestType.Forbidden
                    ErrorFlag = True
                    GoTo endproc
                End Try

            Else

                PackFinished = False

                If CkeckZipTimeAndExist(GetEtalonData) Then

                    Log.DebugFormat("�������� ��������������� ����� {1}: {0}", ResultFileName & UserId & ".zip", File.GetAttributes(ResultFileName & UserId & ".zip"))
                    If Not File.GetAttributes(ResultFileName & UserId & ".zip") = FileAttributes.NotContentIndexed Then

                        UpdateType = RequestType.ResumeData
                        Dim fileInfo = New FileInfo(ResultFileName & UserId & ".zip")
                        Addition &= "������ ���������� �������������� ������: " & fileInfo.LastWriteTime.ToString() & "; "
                        NewZip = False
                        PackFinished = True
                        Log.DebugFormat("���� ����� ������������: {0}", ResultFileName & UserId & ".zip")
                        GoTo endproc

                    End If
                    Log.DebugFormat("���� ����� �������������� ������: {0}", ResultFileName & UserId & ".zip")

                Else

                    Try

                        ShareFileHelper.MySQLFileDelete(ResultFileName & UserId & ".zip")
                        Log.DebugFormat("������� ���������� �������������� ������: {0}", ResultFileName & UserId & ".zip")

                    Catch ex As Exception
                        Addition &= "�� ������� ������� ���������� ������: " & ex.Message & "; "
                        UpdateType = RequestType.Forbidden
                        ErrorFlag = True
                        GoTo endproc
                    End Try

                    CurUpdTime = helper.GetCurrentUpdateDate(UpdateType)

                End If
            End If

            If Documents Then

                '�������� �������������
                ThreadZipStream.Start()

            Else

                '�������� �������� ������
                BaseThread.Start()
                Thread.Sleep(500)

            End If

endproc:

            If Not PackFinished And (((BaseThread IsNot Nothing) AndAlso BaseThread.IsAlive) Or ThreadZipStream.IsAlive) And Not ErrorFlag Then

                '���� ���� ������, ���������� ���������� ������
                If ErrorFlag Then

                    If (BaseThread IsNot Nothing) AndAlso BaseThread.IsAlive Then BaseThread.Abort()
                    If ThreadZipStream.IsAlive Then ThreadZipStream.Abort()

                    PackFinished = True

                End If
                Thread.Sleep(1000)

                GoTo endproc

            ElseIf Not PackFinished And Not ErrorFlag And (UpdateType <> RequestType.Forbidden) And Not WayBillsOnly Then

                Addition &= "; ��� ���������� �������, ������ �� ������."
                UpdateType = RequestType.Forbidden

                ErrorFlag = True

            End If

            If Len(Addition) = 0 Then Addition = MessageH & " " & MessageD

            If NewZip And Not ErrorFlag Then
                Dim ArhiveTS = Now().Subtract(ArhiveStartTime)

                If Math.Round(ArhiveTS.TotalSeconds, 0) > 30 Then

                    Addition &= "�������������: " & Math.Round(ArhiveTS.TotalSeconds, 0) & "; "

                End If

            End If

            ProtocolUpdatesThread.Start()

            If ErrorFlag Then

                If Len(MessageH) = 0 Then
                    ResStr = "Error=��� ���������� ���������� ��������� ������.;Desc=����������, ��������� ������ ������ ����� ��������� �����."
                Else
                    ResStr = "Error=" & MessageH & ";Desc=" & MessageD
                End If

            Else


                While GUpdateId = 0
                    Thread.Sleep(500)
                End While

                ResStr = "URL=" & UpdateHelper.GetDownloadUrl() & "/GetFileHandler.ashx?Id=" & GUpdateId & ";New=" & NewZip & ";Cumulative=" & (UpdateType = RequestType.GetCumulative Or (UpdateType = RequestType.PostOrderBatch AndAlso GED))

                If Not String.IsNullOrEmpty(UpdateData.Message) Then ResStr &= ";Addition=" & UpdateData.Message

                '���� �������� ClientHFile ����� �������� Nothing, �� ��������� ����� ������ GetUserData � � ���� ������ �������� � ������ hosts �� ����
                '���������� ������� DNS, ���� ������ ��������� ������ 960
                If (ClientHFile IsNot Nothing) And (UpdateData.BuildNumber > 960) Then
                    Try
                        ResStr &= HostsFileHelper.ProcessDNS(SpyHostsFile)
                    Catch HostsException As Exception
                        MailHelper.MailErr(CCode, "������ �� ����� ��������� DNS", HostsException.ToString())
                    End Try
                End If

                '���� ������ ���� SpyAccount, �� ���� ���������� ������ � ������� � �������
                If SpyAccount Then ResStr &= ";SendUData=True"

            End If
            InternalGetUserData = ResStr
        Catch updateException As UpdateException
            Return ProcessUpdateException(updateException)
        Catch ex As Exception
            If LogRequestHelper.NeedLogged() Then
                LogRequestHelper.MailWithRequest(Log, "������ ��� ���������� ������", ex)
            Else
                Log.Error("��������� " & _
                 String.Format("AccessTime = {0}, ", AccessTime) & _
                 String.Format("GetEtalonData = {0}, ", GetEtalonData) & _
                 String.Format("EXEVersion = {0}, ", EXEVersion) & _
                 String.Format("MDBVersion = {0}, ", MDBVersion) & _
                 String.Format("UniqueID = {0}, ", UniqueID) & _
                 String.Format("WINVersion = {0}, ", WINVersion) & _
                 String.Format("WINDesc = {0}, ", WINDesc) & _
                 String.Format("WayBillsOnly = {0}", WayBillsOnly), ex)
            End If
            InternalGetUserData = "Error=��� ���������� ���������� ��������� ������.;Desc=����������, ��������� ������ ������ ����� ��������� �����."
        Finally
            If (Not ProcessBatch) Then
                DBDisconnect()
                Counter.ReleaseLock(UserId, "GetUserData")
            End If
        End Try

        GC.Collect()
    End Function


    Enum ������������ As Integer
        WayBills = 1
        Rejects = 2
        Docs = 3
    End Enum



    Public Sub ZipStream() ' � ������ ThreadZipStream

        Dim ArchCmd As MySqlCommand = New MySqlCommand()
        Dim ArchDA As MySqlDataAdapter = New MySqlDataAdapter()
        Try
            ThreadContext.Properties("user") = UpdateData.UserName


            ArhiveStartTime = Now()
            Dim SevenZipParam As String = " -mx7 -bd -slp -mmt=6 -w" & Path.GetTempPath
            Dim SevenZipTmpArchive, Name As String
            Dim xRow As DataRow
            Dim FileName, �����7Z, ������7Z As String
            Dim zipfilecount = 0
            Dim xset As New DataTable
            Dim ArchTrans As MySqlTransaction
            Dim ef(), ������������() As String


            Using connection = _simpleConnectionManager.GetConnection()
                connection.Open()


                Dim Pr As Process
                Dim startInfo As ProcessStartInfo


                If GetHistory Then
                    SevenZipTmpArchive = Path.GetTempPath() & "Orders" & UserId
                    ShareFileHelper.MySQLFileDelete(ResultFileName & "Orders" & UserId & ".zip")
                ElseIf Reclame Then
                    SevenZipTmpArchive = Path.GetTempPath() & "r" & UserId
                    ShareFileHelper.MySQLFileDelete(ResultFileName & "r" & UserId & ".zip")
                Else
                    SevenZipTmpArchive = Path.GetTempPath() & UserId
                    ShareFileHelper.MySQLFileDelete(ResultFileName & UserId & ".zip")
                    Log.DebugFormat("������� ���������� �������������� ������ ��� ������ �������������: {0}", ResultFileName & UserId & ".zip")
                End If

                SevenZipTmpArchive &= "T.zip"
                ShareFileHelper.MySQLFileDelete(SevenZipTmpArchive)


                '���� �� �������
                Dim helper = New UpdateHelper(UpdateData, Nothing)
                If Not Reclame AndAlso Not GetHistory Then

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
                            For Each Row As DataRow In DS.Tables("DocumentsToClient").Rows

                                ������������ = Directory.GetFiles(��������������� & _
                                 Row.Item("ClientCode").ToString & _
                                 "\" & _
                                 CType(Row.Item("DocumentType"), ������������).ToString, _
                                 Row.Item("RowId").ToString & "_*")

                                '���� ���� ��� ��������� �� ������ ����������� ���, ��� �� �� �������� ��� �������� ����� ������
                                xRow = DS.Tables("ProcessingDocuments").NewRow
                                xRow("Committed") = False
                                xRow.Item("DocumentId") = Row.Item("RowId").ToString
                                DS.Tables("ProcessingDocuments").Rows.Add(xRow)

                                If ������������.Length = 1 Then

                                    startInfo = New ProcessStartInfo(SevenZipExe)
                                    startInfo.CreateNoWindow = True
                                    startInfo.RedirectStandardOutput = True
                                    startInfo.RedirectStandardError = True
                                    startInfo.UseShellExecute = False
                                    startInfo.StandardOutputEncoding = System.Text.Encoding.GetEncoding(866)

                                    startInfo.Arguments = "a """ & _
                                       SevenZipTmpArchive & """ " & _
                                       " -i!""" & _
                                       CType(Row.Item("DocumentType"), ������������).ToString & "\" & _
                                       Path.GetFileName(������������(0)) & _
                                       """ " & _
                                       SevenZipParam

                                    startInfo.WorkingDirectory = ��������������� & _
                                       Row.Item("ClientCode").ToString

                                    Pr = New Process
                                    Pr.StartInfo = startInfo
                                    Pr = Process.Start(startInfo)
                                    Pr.WaitForExit()

                                    �����7Z = Pr.StandardOutput.ReadToEnd
                                    ������7Z = Pr.StandardError.ReadToEnd

                                    If Pr.ExitCode <> 0 Then

                                        ShareFileHelper.MySQLFileDelete(SevenZipTmpArchive)
                                        Addition &= "������������� ����������, ����� �� 7Z � �������: " & _
                                           �����7Z & _
                                           "-" & _
                                           ������7Z & _
                                           "; "

                                        If Documents Then

                                            Throw New Exception(String.Format("SevenZip error: {0}", �����7Z & _
                                             "-" & _
                                             ������7Z))

                                        Else

                                            MailHelper.Mail("������������� ����������", "����� �� 7Z � �������: " & ": " & _
                                              �����7Z & _
                                             "-" & _
                                              ������7Z)
                                        End If
                                    End If
                                ElseIf ������������.Length = 0 Then
                                    Addition &= "��� ���������� ���������� � �����: " & _
                                     ��������������� & _
                                       Row.Item("ClientCode").ToString & _
                                       "\" & _
                                       CType(Row.Item("DocumentType"), ������������).ToString & _
                                       " �� ������ �������� � " & _
                                       Row.Item("RowId").ToString & _
                                       " ; "
                                End If
                            Next

                            If UpdateData.BuildNumber >= 1027 And DS.Tables("ProcessingDocuments").Rows.Count > 0 Then
                                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "DocumentHeaders" & UserId & ".txt")
                                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "DocumentBodies" & UserId & ".txt")

                                '�������� ��� �������� ��������� ������ �� �����
                                '���������� �������� ����� �������� ������ ���������, �.�. ����� ��������� �� �����
#If DEBUG Then
                                ShareFileHelper.WaitDeleteFile(MySqlLocalFilePath() & "DocumentHeaders" & UserId & ".txt")
                                ShareFileHelper.WaitDeleteFile(MySqlLocalFilePath() & "DocumentBodies" & UserId & ".txt")
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
                                GetMySQLFileWithDefaultEx("DocumentBodies", ArchCmd, helper.GetDocumentBodiesCommand(ids), False, False)

#If DEBUG Then
                                ShareFileHelper.WaitFile(MySqlFilePath() & "DocumentHeaders" & UserId & ".txt")
                                ShareFileHelper.WaitFile(MySqlFilePath() & "DocumentBodies" & UserId & ".txt")
#End If

                                Pr = New Process

                                startInfo = New ProcessStartInfo(SevenZipExe)
                                startInfo.CreateNoWindow = True
                                startInfo.RedirectStandardOutput = True
                                startInfo.RedirectStandardError = True
                                startInfo.UseShellExecute = False
                                startInfo.StandardOutputEncoding = System.Text.Encoding.GetEncoding(866)
                                startInfo.Arguments = String.Format(" a ""{0}"" ""{1}"" {2}", SevenZipTmpArchive, MySqlLocalFilePath() & "Document*" & UserId & ".txt", SevenZipParam)
                                startInfo.FileName = SevenZipExe

                                Pr.StartInfo = startInfo

                                Pr.Start()
                                '                                If Not Pr.HasExited Then
                                '#If Not Debug Then
                                '                                    Try
                                '                                        Pr.ProcessorAffinity = New IntPtr(ZipProcessorAffinityMask)
                                '                                    Catch
                                '                                    End Try
                                '#End If
                                '                                End If

                                �����7Z = Pr.StandardOutput.ReadToEnd
                                ������7Z = Pr.StandardError.ReadToEnd

                                Pr.WaitForExit()

                                If Pr.ExitCode <> 0 Then
                                    Addition &= String.Format(" SevenZip exit code : {0}, :" & Pr.StandardError.ReadToEnd, Pr.ExitCode)
                                    ShareFileHelper.MySQLFileDelete(SevenZipTmpArchive)
                                    Throw New Exception(String.Format("SevenZip exit code : {0}, {1}, {2}, {3}; ", Pr.ExitCode, startInfo.Arguments, �����7Z, ������7Z))
                                End If
                                Pr = Nothing

                                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "DocumentHeaders" & UserId & ".txt")
                                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "DocumentBodies" & UserId & ".txt")

#If DEBUG Then
                                ShareFileHelper.WaitDeleteFile(MySqlFilePath() & "DocumentHeaders" & UserId & ".txt")
                                ShareFileHelper.WaitDeleteFile(MySqlFilePath() & "DocumentBodies" & UserId & ".txt")
#End If
                            End If

                        End If


                    Catch ex As Exception
                        Log.Error("������ ��� ������������� ����������", ex)
                        MailHelper.MailErr(CCode, "������������� ����������", ex.Source & ": " & ex.Message)
                        Addition &= "������������� ����������" & ": " & ex.Message & "; "

                        If Documents Then ErrorFlag = True

                        ShareFileHelper.MySQLFileDelete(SevenZipTmpArchive)

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

                            MessageH = "����� ������ ���������� ���."
                            Addition &= " ��� ����� ����������"
                            ErrorFlag = True
                            PackFinished = True
                            Exit Sub

                        End If

                    End If




                    '���� �� ���������
                    If Not Documents Then

                        '������������� ���������� ���������
                        Try
                            If UpdateData.EnableUpdate Then

                                ef = UpdateData.GetUpdateFiles(ResultFileName)
                                If ef.Length > 0 Then
                                    Pr = System.Diagnostics.Process.Start(SevenZipExe, "a """ & SevenZipTmpArchive & """  """ & Path.GetDirectoryName(ef(0)) & """ " & SevenZipParam)

                                    '#If Not Debug Then
                                    '                                    Try
                                    '                                        Pr.ProcessorAffinity = New IntPtr(ZipProcessorAffinityMask)
                                    '                                    Catch
                                    '                                    End Try
                                    '#End If

                                    Pr.WaitForExit()

                                    If Pr.ExitCode <> 0 Then
                                        MailHelper.MailErr(CCode, "������������� EXE", "����� �� 7Z � ����� " & ": " & Pr.ExitCode)
                                        Addition &= "������������� ���������� ������, ����� �� 7Z � ����� " & ": " & Pr.ExitCode & "; "
                                        ShareFileHelper.MySQLFileDelete(SevenZipTmpArchive)
                                    Else

                                        Addition &= "���������� �������� � ���� ����� ������ ���������; "
                                    End If

                                End If

                            End If

                        Catch ex As ThreadAbortException
                            If Not Pr Is Nothing Then
                                If Not Pr.HasExited Then Pr.Kill()
                                Pr.WaitForExit()
                            End If
                            ShareFileHelper.MySQLFileDelete(SevenZipTmpArchive)
                        Catch ex As Exception
                            MailHelper.MailErr(CCode, "������������� Exe", ex.Source & ": " & ex.Message)
                            Addition &= " ������������� ���������� " & ": " & ex.Message & "; "
                            If Not Pr Is Nothing Then
                                If Not Pr.HasExited Then Pr.Kill()
                                Pr.WaitForExit()
                            End If
                            ShareFileHelper.MySQLFileDelete(SevenZipTmpArchive)
                        End Try

                        ArchTrans = Nothing
                        ArchCmd.Transaction = Nothing


                        '������������� FRF
                        Try
                            If UpdateData.EnableUpdate Then
                                ef = UpdateData.GetFrfUpdateFiles(ResultFileName)
                                If ef.Length > 0 Then
                                    For Each Name In ef
                                        FileInfo = New FileInfo(Name)
                                        If FileInfo.Extension = ".frf" And FileInfo.LastWriteTime.Subtract(OldUpTime).TotalSeconds > 0 Then
                                            Pr = System.Diagnostics.Process.Start(SevenZipExe, "a """ & SevenZipTmpArchive & """  """ & FileInfo.FullName & """  " & SevenZipParam)


                                            '#If Not Debug Then
                                            '                                            Try
                                            '                                                Pr.ProcessorAffinity = New IntPtr(ZipProcessorAffinityMask)
                                            '                                            Catch
                                            '                                            End Try
                                            '#End If

                                            Pr.WaitForExit()

                                            If Pr.ExitCode <> 0 Then
                                                MailHelper.MailErr(CCode, "������������� Frf", "����� �� 7Z � ����� " & ": " & Pr.ExitCode)
                                                Addition &= " ������������� Frf, ����� �� 7Z � ����� " & ": " & Pr.ExitCode & "; "
                                                ShareFileHelper.MySQLFileDelete(SevenZipTmpArchive)
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
                            ShareFileHelper.MySQLFileDelete(SevenZipTmpArchive)

                        Catch ex As Exception

                            Addition &= " ������������� Frf: " & ex.Message & "; "
                            MailHelper.MailErr(CCode, "������������� Frf", ex.Source & ": " & ex.Message)

                            If Not Pr Is Nothing Then
                                If Not Pr.HasExited Then Pr.Kill()
                                Pr.WaitForExit()
                            End If
                            ShareFileHelper.MySQLFileDelete(SevenZipTmpArchive)

                        End Try
                    End If
                End If


                '������������� ������, ��� �������
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
                            If GetHistory Then
                                File.Move(SevenZipTmpArchive, ResultFileName & "Orders" & UserId & ".zip")

                                FileInfo = New FileInfo(ResultFileName & "Orders" & UserId & ".zip")
                                ResultLenght = Convert.ToUInt32(FileInfo.Length)
                            ElseIf Reclame Then

                                'ArchCmd.CommandText &= "1"
                                File.Move(SevenZipTmpArchive, ResultFileName & "r" & UserId & ".zip")

                            Else

                                'ArchCmd.CommandText &= "0"
                                File.Move(SevenZipTmpArchive, ResultFileName & UserId & ".zip")
                                Log.DebugFormat("��������� ������������� �����: {0}", ResultFileName & UserId & ".zip")
                                If (UpdateType = RequestType.GetCumulative Or (UpdateType = RequestType.PostOrderBatch AndAlso GED)) Then
                                    File.SetAttributes(ResultFileName & UserId & ".zip", FileAttributes.Normal)
                                    Log.DebugFormat("��� ����� ��������� ������� Normal: {0}", ResultFileName & UserId & ".zip")
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
                                FileName = MySqlLocalFilePath() & FileForArchive.FileName & UserId & ".txt"
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
                        '                        If Not Pr.HasExited Then
                        '#If Not Debug Then
                        '                            Try
                        '                                Pr.ProcessorAffinity = New IntPtr(ZipProcessorAffinityMask)
                        '                            Catch
                        '                            End Try
                        '#End If
                        '                        End If

                        �����7Z = Pr.StandardOutput.ReadToEnd
                        ������7Z = Pr.StandardError.ReadToEnd

                        Pr.WaitForExit()

                        If Pr.ExitCode <> 0 Then
                            Addition &= String.Format(" SevenZip exit code : {0}, :" & Pr.StandardError.ReadToEnd, Pr.ExitCode)
                            ShareFileHelper.MySQLFileDelete(SevenZipTmpArchive)
                            Throw New Exception(String.Format("SevenZip exit code : {0}, {1}, {2}, {3}; ", Pr.ExitCode, startInfo.Arguments, �����7Z, ������7Z))
                        End If
                        Pr = Nothing
                        If Not Reclame Then ShareFileHelper.MySQLFileDelete(FileName)
                        zipfilecount += 1

                        'If zipfilecount >= FileCount Then

                        '    'ArchCmd.CommandText = "delete from ready_client_files where clientcode=" & CCode
                        '    'ArchCmd.CommandText &= " and reclame="

                        'Else

                        'End If

                        GoTo StartZipping

                    End If




                Catch ex As ThreadAbortException
                    ShareFileHelper.MySQLFileDelete(SevenZipTmpArchive)

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
                    ShareFileHelper.MySQLFileDelete(SevenZipTmpArchive)

                    If Not TypeOf ex.InnerException Is ThreadAbortException Then
                        ErrorFlag = True
                        UpdateType = RequestType.Error
                        MailHelper.MailErr(CCode, "�������������", ex.Source & ": " & ex.ToString())
                    End If
                    Addition &= " �������������: " & ex.ToString() & "; "

                Catch Unhandled As Exception

                    ErrorFlag = True
                    UpdateType = RequestType.Error
                    If Not Pr Is Nothing Then
                        If Not Pr.HasExited Then Pr.Kill()
                        Pr.WaitForExit()
                    End If
                    Addition &= " �������������: " & Unhandled.ToString()
                    ShareFileHelper.MySQLFileDelete(SevenZipTmpArchive)
                    MailHelper.MailErr(CCode, "�������������", Unhandled.Source & ": " & Unhandled.ToString())
                    Addition &= " �������������: " & Unhandled.ToString() & "; "
                    'If Not ArchTrans Is Nothing Then ArchTrans.Rollback()
                End Try
            End Using

        Catch tae As ThreadAbortException

        Catch Unhandled As Exception
            MailHelper.MailErr(CCode, "������������� general", Unhandled.Source & ": " & Unhandled.ToString())
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
            Counter.TryLock(UserId, "MaxSynonymCode")

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
                        Me.Log.Debug("MaxSynonymCode: ����, ��������� �� �������, ������, ��� ���� �� slave")
                    End If
                End If

            Catch ex As Exception
                MailHelper.MailErr(CCode, "������� ���� ���������� ", ex.Message & ex.Source)
                UpdateTime = Now().ToUniversalTime
            End Try

            MaxSynonymCode = UpdateTime.ToUniversalTime

            Try

                Cm.CommandText = "select SaveAFDataFiles from UserUpdateInfo  where UserId=" & UserId & "; "
                If Convert.ToBoolean(Cm.ExecuteScalar) Then
                    If Not Directory.Exists(ResultFileName & "\Archive\" & UserId) Then Directory.CreateDirectory(ResultFileName & "\Archive\" & UserId)
                    File.Copy(ResultFileName & UserId & ".zip", ResultFileName & "\Archive\" & UserId & "\" & UpdateId & ".zip")
                End If

                ShareFileHelper.MySQLFileDelete(ResultFileName & UserId & ".zip")
                Me.Log.DebugFormat("������� �������������� ������ ����� �������������: {0}", ResultFileName & UserId & ".zip")
                ShareFileHelper.MySQLFileDelete(ResultFileName & "r" & UserId & "Old.zip")

            Catch ex As Exception
                Me.Log.Error("������ ��� ���������� �������������� ������", ex)
            End Try
            ProtocolUpdatesThread.Start()
        Catch e As Exception
            LogRequestHelper.MailWithRequest(Log, String.Format("������ ��� ������������� ����������, ������ {0}, ������ ��", Now().ToUniversalTime), e)
            Return Now().ToUniversalTime
        Finally
            Counter.ReleaseLock(UserId, "MaxSynonymCode")
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
            Counter.TryLock(UserId, "CommitExchange")

            If Not WayBillsOnly Or Not File.GetAttributes(ResultFileName & UserId & ".zip") = FileAttributes.NotContentIndexed Then
                ' ����� ������������ ���� �����-������
                ProcessCommitExchange()
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
                    Me.Log.DebugFormat("CommitExchange: slave UncommitedUpdateDate {0}  master UncommitedUpdateDate {1}", UpdateTime, masterUpdateTime)
                    If IsDate(masterUpdateTime) And (CType(masterUpdateTime, DateTime) > UpdateTime) Then
                        UpdateTime = CType(masterUpdateTime, DateTime)
                        Me.Log.Debug("CommitExchange: ����, ��������� �� �������, ������, ��� ���� �� slave")
                    End If
                End If

            Catch ex As Exception
                MailHelper.MailErr(CCode, "������� ���� ���������� ", ex.Message & ex.Source)
                UpdateTime = Now().ToUniversalTime
            End Try

            CommitExchange = UpdateTime.ToUniversalTime

            Try

                Cm.CommandText = "select SaveAFDataFiles from UserUpdateInfo  where UserId=" & UserId & "; "
                If Convert.ToBoolean(Cm.ExecuteScalar) Then
                    If Not Directory.Exists(ResultFileName & "\Archive\" & UserId) Then Directory.CreateDirectory(ResultFileName & "\Archive\" & UserId)
                    File.Copy(ResultFileName & UserId & ".zip", ResultFileName & "\Archive\" & UserId & "\" & UpdateId & ".zip")
                End If

                ShareFileHelper.MySQLFileDelete(ResultFileName & UserId & ".zip")
                Me.Log.DebugFormat("������� �������������� ������ ����� �������������: {0}", ResultFileName & UserId & ".zip")
                ShareFileHelper.MySQLFileDelete(ResultFileName & "r" & UserId & "Old.zip")

            Catch ex As Exception
                'MailHelper.MailErr(CCode, "�������� ���������� ������;", ex.Message)
            End Try
            ProtocolUpdatesThread.Start()
        Catch e As Exception
            LogRequestHelper.MailWithRequest(Log, "������ ��� ������������� ����������", e)
            CommitExchange = Now().ToUniversalTime
        Finally
            DBDisconnect()
            Counter.ReleaseLock(UserId, "CommitExchange")
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
            Counter.TryLock(UserId, "SendClientLog")
            Try
                MySql.Data.MySqlClient.MySqlHelper.ExecuteNonQuery( _
                 readWriteConnection, _
                 "update logs.AnalitFUpdates set Log=?Log  where UpdateId=?UpdateId", _
                 New MySqlParameter("?Log", Log), _
                 New MySqlParameter("?UpdateId", UpdateId))
            Catch ex As Exception
                Me.Log.Error("������ ��� ���������� ���� �������", ex)
            End Try
            SendClientLog = "OK"
        Catch e As Exception
            LogRequestHelper.MailWithRequest(Log, "������ ��� ���������� ���� �������", e)
            SendClientLog = "Error"
        Finally
            DBDisconnect()
            Counter.ReleaseLock(UserId, "SendClientLog")
        End Try
    End Function

    Private Sub GetClientCode()
        UserName = ServiceContext.GetUserName()
        ThreadContext.Properties("user") = UserName
        If Left(UserName, 7) = "ANALIT\" Then
            UserName = Mid(UserName, 8)
        End If
        UpdateData = UpdateHelper.GetUpdateData(readWriteConnection, UserName)

        If UpdateData Is Nothing OrElse UpdateData.Disabled() Then
            Throw New UpdateException("������ ������.", "����������, ���������� � �� ��������.[1]", "��� ������ " & UserName & " ������ �� ���������������; ", RequestType.Forbidden)
        End If

        UpdateData.ClientHost = UserHost
        CCode = UpdateData.ClientId
        UserId = UpdateData.UserId
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

        Cm.Connection = readWriteConnection
        Cm.Transaction = Nothing
        Cm.CommandText = "" & _
         "UPDATE Logs.AuthorizationDates A " & _
         "SET     AFTime    =now() " & _
         "WHERE   UserId=" & UserId
        Dim AuthorizationDatesCounter As Integer = Cm.ExecuteNonQuery()

        If AuthorizationDatesCounter <> 1 Then
            Addition &= "��� ������ � AuthorizationDates (" & UserId & "); "
        End If
    End Sub

    Private Function DBConnect()
        UserHost = ServiceContext.GetUserHost()
        Try

            readWriteConnection = _simpleConnectionManager.GetConnection()
            readWriteConnection.Open()

            Return True
        Catch ex As Exception
            DBDisconnect()
            Throw
        End Try
    End Function

    Private Sub DBDisconnect()
        Try
            If Not readWriteConnection Is Nothing Then readWriteConnection.Dispose()
        Catch e As Exception
            Log.Error("������ ��� �������� ����������", e)
        End Try
    End Sub

    <WebMethod()> Public Function GetArchivedOrdersList() As String
        'If DBConnect("GetArchivedOrdersList") Then

        '    'TODO: �������� ����������� � prgdataex
        '    Try
        '        GetClientCode()

        '        '���� ������ �������� ��� �������
        '        If CCode > 0 Then
        '            Dim dsOrderList As DataSet = MySqlHelper.ExecuteDataset(Cm.Connection, "SELECT o.ClientOrderId FROM orders.ordershead o LEFT JOIN orders.orderslist ol ON ol.OrderID=o.RowID where ol.OrderID is null and o.WriteTime between '2007-09-16 20:34:02' and '2007-09-24 11:02:44' and o.ClientCode = ?ClientCode limit 50", New MySqlParameter("?ClientCode", CCode))
        '            Dim list As List(Of String) = New List(Of String)
        '            Dim drOrderId As DataRow

        '            For Each drOrderId In dsOrderList.Tables(0).Rows
        '                list.Add(drOrderId.Item("ClientOrderId").ToString())
        '            Next
        '            'MailHelper.MailErr(CCode, "��������� � ������� �������� ������", list.Count & " ��.")
        '            Return String.Join(";", list.ToArray())
        '        Else
        '            Return String.Empty
        '        End If

        '    Catch Exp As Exception
        '        MailHelper.MailErr(CCode, "������ ��� ��������� ������ �������� �������", Exp.Message & ": " & Exp.StackTrace)
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

        Try
            UpdateType = RequestType.SendOrder

            DBConnect()
            GetClientCode()
            Counter.TryLock(UserId, "PostOrder")

            UpdateHelper.CheckUniqueId(readWriteConnection, UpdateData, UniqueID, UpdateType)

            Dim helper = New ReorderHelper(UpdateData, readWriteConnection, True, ClientCode, False)

            helper.ParseOldOrder( _
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
                RequestRatio, _
                OrderCost, _
                MinOrderCount, _
                LeaderMinPriceCode)

            Return helper.PostOldOrder()

        Catch updateException As UpdateException
            Return ProcessUpdateException(updateException)
        Catch ex As NotEnoughElementsException
            Log.Warn("������ ��� �������� ������", ex)
            Return "Error=�������� ������� ����������� ��������.;Desc=��������� ������ �� ���� �����������."
        Catch ex As Exception
            LogRequestHelper.MailWithRequest(Log, "������ ��� �������� ������", ex)
            Return "Error=�������� ������� ����������� ��������.;Desc=����������, ��������� ������� ����� ��������� �����."
        Finally
            Counter.ReleaseLock(UserId, "PostOrder")
            DBDisconnect()
        End Try

    End Function

    '���������� ��������� ������� ������ � �� ��� ��� ��������� �����
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

        '���������� ������ ������� ���������� �������� � ����� ���-�� ������� � �������, �������� � ������� - ������ ������� ("")
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
          RetailMarkup, _
          ProducerCost, _
          NDS _
          )
    End Function

    '���������� ��������� ������� ������ � �� ��� ��� ��������� �����
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

        Dim ResStr As String = String.Empty

        Try
            UpdateType = RequestType.SendOrders
            DBConnect()
            GetClientCode()
            Counter.TryLock(UserId, "PostOrder")
            UpdateHelper.CheckUniqueId(readWriteConnection, UpdateData, UniqueID, UpdateType)
            If Not String.IsNullOrEmpty(EXEVersion) Then UpdateData.ParseBuildNumber(EXEVersion)

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
             NDS _
            )

            Return helper.PostSomeOrders()
        Catch updateException As UpdateException
            Return ProcessUpdateException(updateException)
        Catch ex As NotEnoughElementsException
            Log.Warn("������ ��� �������� ������", ex)
            Return "Error=�������� ������� ����������� ��������.;Desc=����������, ��������� ������� ����� ��������� �����."
        Catch ex As Exception
            LogRequestHelper.MailWithRequest(Log, "������ ��� �������� �������", ex)
            Return "Error=�������� ������� ����������� ��������.;Desc=����������, ��������� ������� ����� ��������� �����."
        Finally
            Counter.ReleaseLock(UserId, "PostOrder")
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
            UpdateType = RequestType.PostOrderBatch

            DBConnect()
            GetClientCode()
            Counter.TryLock(UserId, "PostOrderBatch")
            UpdateHelper.CheckUniqueId(readWriteConnection, UpdateData, UniqueID, UpdateType)
            UpdateData.ParseBuildNumber(EXEVersion)


            Dim helper = New SmartOrderHelper(UpdateData, ClientId, MaxOrderId, MaxOrderListId, MaxBatchId)

            Try
                helper.PrepareBatchFile(BatchFile)

                helper.ProcessBatchFile()

                AddFileToQueue(helper.BatchReportFileName)
                AddFileToQueue(helper.BatchOrderFileName)
                AddFileToQueue(helper.BatchOrderItemsFileName)

                ResStr = InternalGetUserData(AccessTime, GetEtalonData, EXEVersion, MDBVersion, UniqueID, WINVersion, WINDesc, False, Nothing, PriceCodes, True)

            Finally
                helper.DeleteTemporaryFiles()
            End Try


            Return ResStr
        Catch updateException As UpdateException
            Return ProcessUpdateException(updateException)
        Catch ex As Exception
            LogRequestHelper.MailWithRequest(Log, "������ ��� �������� ���������", ex)
            Return "Error=�������� ��������� ����������� ��������.;Desc=����������, ��������� ������� ����� ��������� �����."
        Finally
            Counter.ReleaseLock(UserId, "PostOrderBatch")
            DBDisconnect()
        End Try
    End Function

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
                    NoNeedProcessDocuments = True
                End If

                If (UpdateType = RequestType.GetData) _
                 Or (UpdateType = RequestType.GetCumulative) _
                 Or (UpdateType = RequestType.PostOrderBatch) _
                 Or (UpdateType = RequestType.Forbidden) _
                 Or (UpdateType = RequestType.Error) _
                 Or (UpdateType = RequestType.GetDocs) _
                 Or (UpdateType = RequestType.GetHistoryOrders) Then

PostLog:

                    transaction = connection.BeginTransaction(IsoLevel)

                    If CurUpdTime < Now().AddDays(-1) Then CurUpdTime = Now()

                    '���� ��� ����� ���������� �� � ������������� �� �����
                    '� � ��������� ���������������� ���������� ��� �������
                    '��� �� �� ���� ������
                    Dim commit = False
                    If MessageH = "����� ������ ���������� ���." Then
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
                        If (UpdateType = RequestType.GetData) And LimitedCumulative Then
                            .Parameters.Add(New MySqlParameter("?UpdateType", Convert.ToInt32(RequestType.GetCumulative)))
                        Else
                            .Parameters.Add(New MySqlParameter("?UpdateType", Convert.ToInt32(UpdateType)))
                        End If
                        .Parameters.Add(New MySqlParameter("?EXEVersion", UpdateData.BuildNumber))
                        .Parameters.Add(New MySqlParameter("?Size", ResultLenght))
                        .Parameters.Add(New MySqlParameter("?Addition", Addition))
                        .Parameters.Add(New MySqlParameter("?UpdateTime", CurUpdTime))
                        .Parameters.AddWithValue("?Commit", commit)
                        .Parameters.AddWithValue("?ClientHost", UserHost)
                    End With

                    GUpdateId = Convert.ToUInt32(LogCm.ExecuteScalar)


                    transaction.Commit()

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
                        If UpdateData.IsFutureClient Then
                            Dim command = New MySqlCommand("update Logs.DocumentSendLogs set UpdateId = ?UpdateId where UserId = ?UserId and DocumentId = ?DocumentId", connection)
                            command.Parameters.AddWithValue("?UserId", UpdateData.UserId)
                            command.Parameters.AddWithValue("?UpdateId", GUpdateId)
                            command.Parameters.Add("?DocumentId", MySqlDbType.UInt32)

                            For Each row As DataRow In DS.Tables("ProcessingDocuments").Rows
                                command.Parameters("?DocumentId").Value = row("DocumentId")
                                command.ExecuteNonQuery()
                            Next
                        Else
                            LogDA.Update(DS.Tables("ProcessingDocuments"))
                        End If
                        transaction.Commit()

                    End If

                    DS.Tables.Clear()

                End If

                If (UpdateType = RequestType.ResumeData) Then

                    transaction = connection.BeginTransaction(IsoLevel)

                    LogCm.CommandText = "" & _
                       "SELECT  MAX(UpdateId) " & _
                      "FROM    `logs`.AnalitFUpdates " & _
                      "WHERE   UpdateType IN (1, 2) " & _
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
                        Dim ������������() As String

                        transaction = connection.BeginTransaction(IsoLevel)
                        LogCm.CommandText = "update `logs`.`AnalitFUpdates` set Commit=1, Log=?Log, Addition=concat(Addition, ifnull(?Addition, ''))  where UpdateId=" & GUpdateId

                        LogCm.Parameters.Add(New MySqlParameter("?Log", MySqlDbType.VarString))
                        LogCm.Parameters("?Log").Value = ClientLog

                        LogCm.Parameters.Add(New MySqlParameter("?Addition", MySqlDbType.VarString))
                        LogCm.Parameters("?Addition").Value = Addition

                        LogCm.ExecuteNonQuery()

                        Dim helper = New UpdateHelper(UpdateData, readWriteConnection)

                        LogCm.CommandText = "delete from future.ClientToAddressMigrations where UserId = " & UpdateData.UserId
                        LogCm.ExecuteNonQuery()

                        If Not UpdateData.IsFutureClient Then
                            Dim processedDocuments = helper.GetProcessedDocuments(GUpdateId)

                            For Each DocumentsIdRow As DataRow In processedDocuments.Rows

                                ������������ = Directory.GetFiles(��������������� & _
                                   DocumentsIdRow.Item("ClientCode").ToString & _
                                   "\" & _
                                   CType(DocumentsIdRow.Item("DocumentType"), ������������).ToString, _
                                   DocumentsIdRow.Item("DocumentId").ToString & "_*")

                                If ������������.Length > 0 Then MySQLResultFile.Delete(������������(0))

                            Next
                        End If
                        LogCm.CommandText = helper.GetConfirmDocumentsCommnad(GUpdateId)
                        LogCm.ExecuteNonQuery()

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
                Log.Error("������ ����", ex)
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
        Me.readWriteConnection = New MySql.Data.MySqlClient.MySqlConnection
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
        Me.Cm.Connection = Me.readWriteConnection
        Me.Cm.Transaction = Nothing
        '
        'ReadOnlyCn
        '
        Me.readWriteConnection.ConnectionString = Nothing
        '
        'OrderInsertCm
        '
        Me.OrderInsertCm.Connection = Me.readWriteConnection
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
        Me.SelProc.Connection = Me.readWriteConnection
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
        Cm.Connection = readWriteConnection
        Cm.Transaction = Nothing
        Cm.CommandText = "" & _
           "SELECT  count(UpdateId) " & _
           "FROM    logs.AnalitFUpdates " & _
           "WHERE   UpdateType IN (1, 2) " & _
           "    AND Commit    =0 " & _
           "    AND RequestTime > curdate() - interval 1 DAY " & _
           "    AND UserId  =" & UserId

        If Convert.ToUInt32(Cm.ExecuteScalar) < 1 Then
            Log.DebugFormat("�� ������ ���������� ���������������� ������ ������: {0}", UserId)
            Return False
        Else
            Log.DebugFormat("������ ���������� ���������������� ������ ������: {0}", UserId)
        End If


        FileInfo = New FileInfo(ResultFileName & UserId & ".zip")

        If FileInfo.Exists Then

            Log.DebugFormat("���� � ��������������� ������� ����������: {0}", ResultFileName & UserId & ".zip")
            CkeckZipTimeAndExist = _
             (Date.UtcNow.Subtract(UncDT.ToUniversalTime).TotalHours < 1 And Not GetEtalonData) _
             Or (OldUpTime.Year = 2003 And DateTime.UtcNow.Subtract(UncDT.ToUniversalTime).TotalHours < 8) _
             Or (File.GetAttributes(ResultFileName & UserId & ".zip") = FileAttributes.Normal And GetEtalonData)

            Log.DebugFormat( _
             "��������� �������� CkeckZipTimeAndExist: {0}  " & vbCrLf & _
             "��������� " & vbCrLf & _
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

            Log.DebugFormat("���� � ��������������� ������� �� ����������: {0}", ResultFileName & UserId & ".zip")
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
            Dim helper As UpdateHelper = New UpdateHelper(UpdateData, readWriteConnection)
            Try
RestartTrans2:
                If ErrorFlag Then Exit Try

                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "Products" & UserId & ".txt")
                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "Catalog" & UserId & ".txt")
                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "CatalogCurrency" & UserId & ".txt")
                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "CatDel" & UserId & ".txt")
                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "Clients" & UserId & ".txt")
                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "ClientsDataN" & UserId & ".txt")
                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "Core" & UserId & ".txt")

                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "PriceAvg" & UserId & ".txt")
                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "PricesData" & UserId & ".txt")
                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "PricesRegionalData" & UserId & ".txt")
                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "RegionalData" & UserId & ".txt")
                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "Regions" & UserId & ".txt")
                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "Section" & UserId & ".txt")
                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "Synonym" & UserId & ".txt")
                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "SynonymFirmCr" & UserId & ".txt")
                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "Rejects" & UserId & ".txt")
                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "CatalogFarmGroups" & UserId & ".txt")
                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "CatalogNames" & UserId & ".txt")
                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "CatFarmGroupsDel" & UserId & ".txt")

                helper.MaintainReplicationInfo()

                If ThreadZipStream.IsAlive Then
                    ThreadZipStream.Abort()
                End If

                SelProc = New MySqlCommand
                SelProc.Connection = readWriteConnection
                helper.SetUpdateParameters(SelProc, GED, OldUpTime, CurUpdTime)

                Cm = New MySqlCommand
                Cm.Connection = readWriteConnection
                Cm.Parameters.AddWithValue("?UpdateTime", OldUpTime)

                Cm.Parameters.AddWithValue("?OfferRegionCode", UpdateData.OffersRegionCode)

                transaction = readWriteConnection.BeginTransaction(IsoLevel)
                SelProc.Transaction = transaction

                SelProc.CommandText = "drop temporary table IF EXISTS MaxCodesSynFirmCr, MinCosts, ActivePrices, Prices, Core, tmpprd, MaxCodesSyn, ParentCodes; "
                SelProc.ExecuteNonQuery()

                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "MinPrices" & UserId & ".txt")
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

                GetMySQLFile("Regions", SelProc, helper.GetRegionsCommand())

                helper.SelectPrices()

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


                If UpdateData.IsFutureClient Then
                    GetMySQLFile("PricesRegionalData", SelProc, _
                    "SELECT PriceCode           , " & _
                    "       RegionCode          , " & _
                    "       STORAGE             , " & _
                    "       0 as PublicUpCost   , " & _
                    "       MinReq              , " & _
                    "       MainFirm            , " & _
                    "       NOT disabledbyclient, " & _
                    "       0 as CostCorrByClient, " & _
                    "       ControlMinReq " & _
                    "FROM   Prices")
                Else
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
                End If

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

                helper.SelectReplicationInfo()

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


                If Not UpdateData.EnableImpersonalPrice Then

                    SelProc.CommandText = "" & _
                    "SELECT IFNULL(SUM(fresh), 0) " & _
                    "FROM   ActivePrices"
                    If CType(SelProc.ExecuteScalar, Integer) > 0 Or GED Then
                        helper.SelectOffers()
                        CostOptimizer.OptimizeCostIfNeeded(readWriteConnection, CCode)

                        SelProc.CommandText = "" & _
                        "UPDATE ActivePrices Prices, " & _
                        "       Core " & _
                        "SET    CryptCost       = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(AES_ENCRYPT(Cost, (SELECT BaseCostPassword FROM   retclientsset WHERE  clientcode=?ClientCode)), CHAR(37), '%25'), CHAR(32), '%20'), CHAR(159), '%9F'), CHAR(161), '%A1'), CHAR(0), '%00') " & _
                        "WHERE  Prices.PriceCode= Core.PriceCode " & _
                        "   AND IF(?Cumulative, 1, Fresh) " & _
                        "   AND Core.PriceCode != ?ImpersonalPriceId ; " & _
                        " " & _
                        "UPDATE Core " & _
                        "SET    CryptCost        =concat(LEFT(CryptCost, 1), CHAR(ROUND((rand()*110)+32,0)), SUBSTRING(CryptCost,2,LENGTH(CryptCost)-4), CHAR(ROUND((rand()*110)+32,0)), RIGHT(CryptCost, 3)) " & _
                        "WHERE  LENGTH(CryptCost)>0 " & _
                        "   AND Core.PriceCode != ?ImpersonalPriceId;"
                        SelProc.ExecuteNonQuery()


                        GetMySQLFile("MinPrices", SelProc, _
                         "SELECT RIGHT(MinCosts.ID, 9), " & _
                         "       MinCosts.ProductId   , " & _
                         "       MinCosts.RegionCode  , " & _
                         "       IF(PriceCode = ?ImpersonalPriceId, '', (99999900 ^ TRUNCATE((MinCost*100), 0))) " & _
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
                        " INTO OUTFILE '" & GetFileNameForMySql("Core" & UserId & ".txt") & "' FIELDS TERMINATED BY '" & Chr(159) & "' OPTIONALLY ENCLOSED BY '' ESCAPED BY '' LINES TERMINATED BY ''"
                        SelProc.ExecuteNonQuery()


                        SelProc.CommandText = "SELECT ''" & _
                          " INTO OUTFILE '" & GetFileNameForMySql("MinPrices" & UserId & ".txt") & "' FIELDS TERMINATED BY '" & Chr(159) & "' OPTIONALLY ENCLOSED BY '' ESCAPED BY '' LINES TERMINATED BY ''"
                        SelProc.ExecuteNonQuery()



                        SyncLock (FilesForArchive)

                            FilesForArchive.Enqueue(New FileForArchive("Core", False))
                            FilesForArchive.Enqueue(New FileForArchive("MinPrices", False))

                        End SyncLock


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
                ConnectionHelper.SafeRollback(transaction)
                If ExceptionHelper.IsDeadLockOrSimilarExceptionInChain(ex) Then
                    Thread.Sleep(2500)
                    GoTo RestartTrans2
                End If
                Throw
            End Try

        Catch ex As Exception
            Me.Log.Error("�������� ����� �������, general " & CCode, ex)
            ErrorFlag = True
            UpdateType = RequestType.Error
            Addition &= ex.Message
            If ThreadZipStream.IsAlive Then ThreadZipStream.Abort()
        End Try
    End Sub


    Private Sub MySqlProc()
        Dim StartTime As DateTime = Now()
        Dim TS As TimeSpan

        Dim transaction As MySqlTransaction
        Try
            ThreadContext.Properties("user") = UpdateData.UserName
            Dim helper As UpdateHelper = New UpdateHelper(UpdateData, readWriteConnection)
            Try

RestartTrans2:
                If ErrorFlag Then Exit Try

                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "Products" & UserId & ".txt")
                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "User" & UserId & ".txt")
                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "Client" & UserId & ".txt")
                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "Catalogs" & UserId & ".txt")
                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "CatDel" & UserId & ".txt")
                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "Clients" & UserId & ".txt")
                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "DelayOfPayments" & UserId & ".txt")
                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "Providers" & UserId & ".txt")
                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "Core" & UserId & ".txt")
                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "PricesData" & UserId & ".txt")
                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "PricesRegionalData" & UserId & ".txt")
                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "RegionalData" & UserId & ".txt")
                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "Regions" & UserId & ".txt")
                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "Synonyms" & UserId & ".txt")
                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "SynonymFirmCr" & UserId & ".txt")
                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "Rejects" & UserId & ".txt")
                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "CatalogNames" & UserId & ".txt")
                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "MNN" & UserId & ".txt")
                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "Descriptions" & UserId & ".txt")
                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "MaxProducerCosts" & UserId & ".txt")
                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "Producers" & UserId & ".txt")
                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "UpdateInfo" & UserId & ".txt")
                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "ClientToAddressMigrations" & UserId & ".txt")
                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "MinReqRules" & UserId & ".txt")
                'ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "CoreTest" & UserId & ".txt")

                helper.MaintainReplicationInfo()

                If ThreadZipStream.IsAlive Then
                    ThreadZipStream.Abort()
                End If

                SelProc = New MySqlCommand
                SelProc.Connection = readWriteConnection
                helper.SetUpdateParameters(SelProc, GED, OldUpTime, CurUpdTime)

                Dim debugHelper = New DebugReplicationHelper(UpdateData, readWriteConnection, SelProc)

                transaction = readWriteConnection.BeginTransaction(IsoLevel)
                SelProc.Transaction = transaction

                SelProc.CommandText = "drop temporary table IF EXISTS MaxCodesSynFirmCr, MinCosts, ActivePrices, Prices, Core, tmpprd, MaxCodesSyn, ParentCodes; "
                SelProc.ExecuteNonQuery()

                debugHelper.PrepareTmpReplicationInfo()

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

                If (UpdateData.BuildNumber > 945) Or (UpdateData.EnableUpdate And ((UpdateData.BuildNumber = 945) Or ((UpdateData.BuildNumber >= 705) And (UpdateData.BuildNumber <= 716)) Or ((UpdateData.BuildNumber >= 829) And (UpdateData.BuildNumber <= 837)))) _
                Then

                    If (UpdateData.BuildNumber >= 1150) Or (UpdateData.EnableUpdate And ((UpdateData.BuildNumber >= 1079) And (UpdateData.BuildNumber < 1150))) Then
                        '���������� ������ ��� ������ ��������� >= 1150 ��� ���������� �� ���
                        GetMySQLFileWithDefaultEx( _
                         "Catalogs", _
                         SelProc, _
                         helper.GetCatalogCommand(False, GED), _
                         UpdateData.NeedUpdateTo945(), _
                         True)

                        '��������� �� ����� ��������� MNN ��� RussianMNN = (UpdateData.BuildNumber > 1263) Or UpdateData.NeedUpdateToNewMNN)
                        GetMySQLFileWithDefaultEx( _
                         "MNN", _
                         SelProc, _
                         helper.GetMNNCommand( _
                             False, _
                             GED, _
                             (UpdateData.BuildNumber > 1263) Or UpdateData.NeedUpdateToNewMNN), _
                         ((UpdateData.BuildNumber = 945) Or ((UpdateData.BuildNumber >= 829) And (UpdateData.BuildNumber <= 837)) Or (UpdateData.BuildNumber <= 1035)) And UpdateData.EnableUpdate, _
                         True)

                        GetMySQLFileWithDefaultEx( _
                         "Descriptions", _
                         SelProc, _
                         helper.GetDescriptionCommand(False, GED), _
                         UpdateData.NeedUpdateTo945(), _
                         True)

                        If (UpdateData.EnableUpdate And ((UpdateData.BuildNumber >= 1079) And (UpdateData.BuildNumber < 1150))) Then
                            '���� ���������� ���������� �� ������ 1159 � ����, �� ���� ��������� ������ ������� ��������������
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
                             helper.GetProducerCommand(GED), _
                             UpdateData.NeedUpdateTo945(), _
                             True)
                        End If

                    Else
                        GetMySQLFileWithDefaultEx( _
                         "Catalogs", _
                         SelProc, _
                         helper.GetCatalogCommand(True, GED), _
                         UpdateData.NeedUpdateTo945(), _
                         True)

                        GetMySQLFileWithDefaultEx( _
                         "MNN", _
                         SelProc, _
                         helper.GetMNNCommand(True, GED, False), _
                         ((UpdateData.BuildNumber = 945) Or ((UpdateData.BuildNumber >= 829) And (UpdateData.BuildNumber <= 837)) Or (UpdateData.BuildNumber <= 1035)) And UpdateData.EnableUpdate, _
                         True)

                        GetMySQLFileWithDefaultEx( _
                         "Descriptions", _
                         SelProc, _
                         helper.GetDescriptionCommand(True, GED), _
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

                Try
                    '���������� ��������� ������� � ����������
                    helper.PrepareProviderContacts(SelProc)

                    GetMySQLFileWithDefault("Providers", SelProc, helper.GetProvidersCommand())
                Finally
                    helper.ClearProviderContacts(SelProc)
                End Try

                GetMySQLFileWithDefault("RegionalData", SelProc, helper.GetRegionalDataCommand())

                GetMySQLFileWithDefault("PricesRegionalData", SelProc, helper.GetPricesRegionalDataCommand())

                GetMySQLFileWithDefault("MinReqRules", SelProc, helper.GetMinReqRuleCommand())

                helper.PreparePricesData(SelProc)
                debugHelper.FillTmpReplicationInfo()
                debugHelper.FillTable("TmpReplicationInfo", "select * from TmpReplicationInfo")
                GetMySQLFileWithDefault("PricesData", SelProc, helper.GetPricesDataCommand())
                debugHelper.FillTable("PricesData", helper.GetPricesDataCommand())

                GetMySQLFileWithDefault("Rejects", SelProc, helper.GetRejectsCommand(GED))
                GetMySQLFileWithDefault("Clients", SelProc, helper.GetClientsCommand(False))

                GetMySQLFileWithDefault("DelayOfPayments", SelProc, helper.GetDelayOfPaymentsCommand())

                helper.SelectActivePrices()

                helper.SelectReplicationInfo()

                If UpdateData.EnableImpersonalPrice And (OldUpTime < New DateTime(2010, 8, 18, 5, 18, 0)) Then
                    GetMySQLFileWithDefault("SynonymFirmCr", SelProc, helper.GetSynonymFirmCrCommand(True))

                    GetMySQLFileWithDefault("Synonyms", SelProc, helper.GetSynonymCommand(True))
                Else
                    GetMySQLFileWithDefault("SynonymFirmCr", SelProc, helper.GetSynonymFirmCrCommand(GED))

                    GetMySQLFileWithDefault("Synonyms", SelProc, helper.GetSynonymCommand(GED))
                End If


                If Not UpdateData.EnableImpersonalPrice Then

                    SelProc.CommandText = "" & _
                    "SELECT IFNULL(SUM(fresh), 0) " & _
                    "FROM   ActivePrices"
                    If CType(SelProc.ExecuteScalar, Integer) > 0 Or GED Then

                        If debugHelper.NeedDebugInfo() Then
                            debugHelper.FillTable("ActivePrices", "select * from ActivePrices")
                            debugHelper.FillTable("AnalitFReplicationInfo", "select * from AnalitFReplicationInfo where UserId = ?UserId")
                            debugHelper.SendMail()
                        End If

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

                        'SelProc.CommandText = _
                        '    "UPDATE ActivePrices Prices, " & _
                        '    "       Core " & _
                        '    "SET    CryptCost       = AES_ENCRYPT(Cost, (SELECT BaseCostPassword FROM   retclientsset WHERE  clientcode=?ClientCode)) " & _
                        '    "WHERE  Prices.PriceCode= Core.PriceCode " & _
                        '    "   AND IF(?Cumulative, 1, Fresh) " & _
                        '    "   AND Core.PriceCode != ?ImpersonalPriceId ; "
                        'SelProc.ExecuteNonQuery()

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

                        GetMySQLFileWithDefaultEx( _
                         "Core", _
                         SelProc, _
                         helper.GetCoreCommand( _
                          False, _
                          (UpdateData.BuildNumber > 1027) Or (UpdateData.EnableUpdate And ((UpdateData.BuildNumber >= 945) Or ((UpdateData.BuildNumber >= 705) And (UpdateData.BuildNumber <= 716)) Or ((UpdateData.BuildNumber >= 829) And (UpdateData.BuildNumber <= 837)))), _
                          (UpdateData.BuildNumber >= 1249) Or UpdateData.NeedUpdateToBuyingMatrix _
                         ), _
                         (UpdateData.BuildNumber <= 1027) And UpdateData.EnableUpdate, _
                         True _
                        )
                    Else
                        '��������� ������ ������� Core
                        '������ ������ �� ����� ������� (� ������ ������ �� ActivePrices), ����� �������� 0 �������
                        GetMySQLFileWithDefault("Core", SelProc, "SELECT * from ActivePrices limit 0")
                    End If

                    If (UpdateData.BuildNumber > 945) Or (UpdateData.EnableUpdate And ((UpdateData.BuildNumber = 945) Or ((UpdateData.BuildNumber >= 705) And (UpdateData.BuildNumber <= 716)) Or ((UpdateData.BuildNumber >= 829) And (UpdateData.BuildNumber <= 837)))) Then
                        If helper.DefineMaxProducerCostsCostId() Then
                            If GED _
                             Or (UpdateData.EnableUpdate And ((UpdateData.BuildNumber < 1049) Or ((UpdateData.BuildNumber >= 1079) And (UpdateData.BuildNumber < 1150)))) _
                             Or helper.MaxProducerCostIsFresh() _
                            Then
                                GetMySQLFileWithDefault("MaxProducerCosts", SelProc, helper.GetMaxProducerCostsCommand())
                            Else
                                '���� �����-���� �� ��������, �� ������ ������ ����
                                GetMySQLFileWithDefault("MaxProducerCosts", SelProc, helper.GetMaxProducerCostsCommand() & " limit 0")
                            End If
                        Else
                            GetMySQLFileWithDefault("MaxProducerCosts", SelProc, helper.GetMaxProducerCostsCommand() & " limit 0")
                            Log.WarnFormat("�� �������� ���������� ������� ���� ��� �����-����� � ������������� ������ ��������������. ��� �����-�����: {0}", helper.MaxProducerCostsPriceId)
                        End If
                    Else
                        GetMySQLFileWithDefault("MaxProducerCosts", SelProc, helper.GetMaxProducerCostsCommand() & " limit 0")
                    End If
                Else

                    If Convert.ToInt32(SelProc.Parameters("?ImpersonalPriceFresh").Value) = 1 Then
                        helper.PrepareImpersonalOffres(SelProc)

                        '�������� ������ ��� ������������� �����-�����
                        GetMySQLFileWithDefault("Core", SelProc, helper.GetCoreCommand(True, True, (UpdateData.BuildNumber >= 1249) Or UpdateData.NeedUpdateToBuyingMatrix))
                    Else
                        '��������� ������ ������� Core
                        GetMySQLFileWithDefault("Core", SelProc, helper.GetMaxProducerCostsCommand() & " limit 0")
                    End If
                    '��������� ������ ������� MaxProducerCosts
                    GetMySQLFileWithDefault("MaxProducerCosts", SelProc, helper.GetMaxProducerCostsCommand() & " limit 0")
                End If

                AddEndOfFiles()

                helper.UpdateReplicationInfo()

                transaction.Commit()

                TS = Now().Subtract(StartTime)
                If Math.Round(TS.TotalSeconds, 0) > 30 Then
                    Addition &= "Sel: " & Math.Round(TS.TotalSeconds, 0) & "; "
                End If

            Catch ex As Exception
                ConnectionHelper.SafeRollback(transaction)
                If ExceptionHelper.IsDeadLockOrSimilarExceptionInChain(ex) Then
                    Thread.Sleep(500)
                    GoTo RestartTrans2
                End If
                Throw
            End Try

        Catch ex As Exception
            Me.Log.Error("�������� ����� ���������� ������, ��� ������� " & CCode, ex)
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
                    MailHelper.MailErr(CCode, "������������ Code", err.Message)
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
                    MailHelper.MailErr(CCode, "������������ CodeCr", err.Message)
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
                            ProblemStr &= "� ����� ������ �" & OrderID & " ������� ������������� ������ � ������� �" & Row.Item("OrderID").ToString & _
                             ", ������ �" & Row.Item("rowid").ToString & Chr(10) & Chr(13)

                        Else
                            Try
                                DS.Tables("OrdersL").Rows(i).Item("Quantity") = Convert.ToUInt16(DS.Tables("OrdersL").Rows(i).Item("Quantity")) - Convert.ToUInt16(Row.Item("Quantity"))
                                ProblemStr &= "� ����� ������ �" & OrderID & " �������� ����������� ������ � ������ � ������������� � ������� �" & Row.Item("OrderID").ToString & _
                                 ", ������ �" & Row.Item("rowid").ToString & Chr(10) & Chr(13)
                            Catch e As Exception
                                MailHelper.MailErr(CCode, "������������� �����", e.Message & ": " & e.StackTrace)
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
            ProblemStr = "����� �" & ClientOrderID & "(�� �������) �� ������ ��� ��������� �������������."
            Return False

        End If

        With OrderInsertCm
            .CommandText = String.Empty
            .Parameters.Clear()
            If ProblemStr <> String.Empty Then
                Addition = ProblemStr
                .CommandText = "update orders.ordershead set rowcount=" & DS.Tables("OrdersL").Rows.Count & " where rowid=" & OrderID & "; "
                'MailHelper.MailErr(CCode, "����� � ������", ProblemStr)
            End If

            .CommandText &= " insert into orders.orderslist (OrderID, ProductId, CodeFirmCr, SynonymCode, SynonymFirmCrCode, Code, CodeCr, Quantity, Junk, Await, Cost)" & _
             " select  " & OrderID & ", products.ID, if(Prod.Id is null, sfcr.codefirmcr, Prod.Id) , syn.synonymcode, sfcr.SynonymFirmCrCode, ?Code, ?CodeCr, ?Quantity, ?Junk, ?Await, ?Cost" & _
             " from catalogs.products" & _
             " left join farm.synonymarchive  syn on syn.synonymcode=?SynonymCode" & _
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

    '�������� ������ ������������� � ����� �������� Hex-�����
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
                Log.ErrorFormat("������ � SendUData ��� ������������ RSTUIN : {0}\n{1}", RSTUIN, err)
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
            LogRequestHelper.MailWithRequest(Log, "������ � SendUData", ex)
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
            End If

            Cm.CommandText = "select BaseCostPassword from retclientsset where clientcode=" & CCode
            Using SQLdr As MySqlDataReader = Cm.ExecuteReader
                SQLdr.Read()
                BasecostPassword = SQLdr.GetString(0)
            End Using

            '�������� ����� ����������� ��� ���������� ������
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
                MailHelper.MailErr(CCode, "������ ��� ��������� �������", "� ������� �� ������ ������ ��� �������� ������")
                Addition = "�� ������ ������ ��� �������� ������"
                ErrorFlag = True
            End If
        Catch updateException As UpdateException
            If UpdateData IsNot Nothing Then
                Log.Warn(updateException)
            Else
                Log.Error(updateException)
            End If
            Return "Error=��� ���������� ������ ������� ��������� ������.;Desc=����������, ��������� ������� ����� ��������� �����."
        Catch ex As Exception
            LogRequestHelper.MailWithRequest(Log, "������ ��� ��������� �������", ex)
            Return "Error=��� ���������� ������ ������� ��������� ������.;Desc=����������, ��������� ������� ����� ��������� �����."
        Finally
            DBDisconnect()
        End Try

        If ErrorFlag Then
            Return "Error=��� ���������� ������ ������� ��������� ������.;Desc=����������, ��������� ������� ����� ��������� �����."
        End If
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
            If Not String.IsNullOrEmpty(EXEVersion) Then UpdateData.ParseBuildNumber(EXEVersion)

            Dim helper = New UpdateHelper(UpdateData, readWriteConnection)
            helper.UpdatePriceSettings(PriceCodes, RegionCodes, INJobs)
            Return "Res=OK"

        Catch updateException As UpdateException
            Return ProcessUpdateException(updateException)
        Catch ex As Exception
            LogRequestHelper.MailWithRequest(Log, "������ ��� ���������� ���������� �������� �����-������", ex)
            ErrorFlag = True
        Finally
            DBDisconnect()
        End Try

        If ErrorFlag Then
            Return "Error=��� ���������� ������ ������� ��������� ������.;Desc=����������, ��������� ������� ����� ��������� �����."
        End If
    End Function

    <WebMethod()> Public Function GetReclame() As String
        Dim MaxReclameFileDate As Date
        Dim NewZip As Boolean = True

        If Log.IsDebugEnabled Then Log.Debug("������� GetReclame")

        Dim FileCount = 0
        Try
            DBConnect()
            GetClientCode()

            Dim updateHelpe = New UpdateHelper(UpdateData, readWriteConnection)

            Dim reclameData = updateHelpe.GetReclame()

            If Not reclameData.ShowAdvertising Then
                GetReclame = ""
                If Log.IsDebugEnabled Then Log.Debug("��������� GetReclame � ����������� (Not reclameData.ShowAdvertising)")
                Exit Function
            End If

            MaxReclameFileDate = reclameData.ReclameDate
            If Log.IsDebugEnabled Then Log.DebugFormat("��������� �� ���� reclameData.ReclameDate {0}", reclameData.ReclameDate)

            Reclame = True
            ReclamePath = ResultFileName & "Reclame\" & reclameData.Region & "\"
            If Log.IsDebugEnabled Then Log.DebugFormat("���� � ������� {0}", ReclamePath)

            ShareFileHelper.MySQLFileDelete(ResultFileName & "r" & UserId & ".zip")

            Dim FileList As String()
            Dim FileName As String

            If Not Directory.Exists(ReclamePath) Then Directory.CreateDirectory(ReclamePath)

            FileList = Directory.GetFiles(ReclamePath)
            If Log.IsDebugEnabled Then Log.DebugFormat("���-�� ������ � �������� � �������� {0}", FileList.Length)
            For Each FileName In FileList

                FileInfo = New FileInfo(FileName)

                If FileInfo.LastWriteTime.Subtract(reclameData.ReclameDate).TotalSeconds > 1 Then

                    If Log.IsDebugEnabled Then Log.DebugFormat("�������� ���� � ����� {0}", FileInfo.Name)
                    FileCount += 1

                    SyncLock (FilesForArchive)

                        FilesForArchive.Enqueue(New FileForArchive(FileInfo.Name, True))

                    End SyncLock

                    If FileInfo.LastWriteTime > MaxReclameFileDate Then MaxReclameFileDate = FileInfo.LastWriteTime

                End If

            Next

            If MaxReclameFileDate > Now() Then MaxReclameFileDate = Now()

            If Log.IsDebugEnabled Then Log.DebugFormat("����� ��������� ������ MaxReclameFileDate {0}", MaxReclameFileDate)

            If FileCount > 0 Then

                AddEndOfFiles()

                ZipStream()

                If Log.IsDebugEnabled Then Log.Debug("������� ��������� �������������")

                FileInfo = New FileInfo(ResultFileName & "r" & UserId & ".zip")
                FileInfo.CreationTime = MaxReclameFileDate

                If Log.IsDebugEnabled Then Log.Debug("���������� ���� �������� �����-������")
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
            LogRequestHelper.MailWithRequest(Log, "������ ��� �������� �������", ex)
            ErrorFlag = True
            Return ""
        Finally
            DBDisconnect()
        End Try

        If ErrorFlag Then
            GetReclame = ""
            If Log.IsDebugEnabled Then Log.Debug("��������� GetReclame � ����������� (ErrorFlag)")
        Else
            If FileCount > 0 Then

                GetReclame = "URL=" & UpdateHelper.GetDownloadUrl() & "/GetFileReclameHandler.ashx;New=" & True
                If Log.IsDebugEnabled Then Log.Debug("��������� GetReclame � ����������� (URL)")

            Else
                GetReclame = ""
                If Log.IsDebugEnabled Then Log.Debug("��������� GetReclame � ����������� (FileCount <= 0)")
            End If
        End If
    End Function

    <WebMethod()> Public Function ReclameComplete() As Boolean
        Dim transaction As MySqlTransaction
        If Log.IsDebugEnabled Then Log.Debug("������� ReclameComplete")
        Try
            DBConnect()
            GetClientCode()

            FileInfo = New FileInfo(ResultFileName & "r" & UserId & ".zip")

            If FileInfo.Exists Then

                If Log.IsDebugEnabled Then Log.DebugFormat("������������� ���� ������� FileInfo.CreationTime {0}", FileInfo.CreationTime)

                transaction = readWriteConnection.BeginTransaction(IsoLevel)
                Cm.CommandText = "update UserUpdateInfo set ReclameDate=?ReclameDate where UserId=" & UserId
                Cm.Parameters.AddWithValue("?ReclameDate", FileInfo.CreationTime)
                Cm.Connection = readWriteConnection
                Cm.ExecuteNonQuery()
                transaction.Commit()

                If Log.IsDebugEnabled Then Log.Debug("���� ������� ������� �����������")
            Else
                If Log.IsDebugEnabled Then Log.DebugFormat("����-����� � �������� �� ���������� {0}", ResultFileName & "r" & UserId & ".zip")
            End If

            Reclame = True
            ShareFileHelper.MySQLFileDelete(ResultFileName & "r" & UserId & ".zip")
            ReclameComplete = True
            If Log.IsDebugEnabled Then Log.Debug("������� ��������� ReclameComplete")
        Catch ex As Exception
            ConnectionHelper.SafeRollback(transaction)
            LogRequestHelper.MailWithRequest(Log, "������������� �������", ex)
            ReclameComplete = False
        Finally
            DBDisconnect()
        End Try
    End Function

    Private Sub SetCodesProc()
        Dim transaction As MySqlTransaction
        Try
            SelProc.Connection = readWriteConnection

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

            transaction = readWriteConnection.BeginTransaction(IsoLevel)
            SelProc.Transaction = transaction

            SelProc.ExecuteNonQuery()

            transaction.Commit()

        Catch ex As Exception
            ConnectionHelper.SafeRollback(transaction)
            If ExceptionHelper.IsDeadLockOrSimilarExceptionInChain(ex) Then
                Me.Log.Info("Deadlock ��������� �������")
                Thread.Sleep(1500)
                GoTo RestartMaxCodesSet
            End If
            Me.Log.Error("���������� �������� ������������ ���������", ex)
            Addition = ex.Message
            UpdateType = RequestType.Error
            ErrorFlag = True
        End Try

    End Sub

    Private Sub ProcessCommitExchange()
        Try
            Dim helper = New UpdateHelper(UpdateData, readWriteConnection)
            helper.CommitExchange()
        Catch err As Exception
            MailHelper.MailErr(CCode, "���������� �������� ������������ ���������", err.Message)
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
            MailHelper.MailErr(CCode, "���������� �������� ������������ ���������", err.Message)
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
            MailHelper.MailErr(CCode, "����� ���������� �� �����-������ � ������������ ����������", err.Message)
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
        Dim fullName = Path.Combine(MySqlFilePath(), outFileName)
        Return MySql.Data.MySqlClient.MySqlHelper.EscapeString(fullName)
    End Function


    Private Sub GetMySQLFile(ByVal FileName As String, ByVal MyCommand As MySqlCommand, ByVal SQLText As String)
        Dim SQL As String = SQLText


        SQL &= " INTO OUTFILE '" & GetFileNameForMySql(FileName & UserId & ".txt") & "' FIELDS TERMINATED BY '" & Chr(159) & "' OPTIONALLY ENCLOSED BY '' ESCAPED BY '' LINES TERMINATED BY '" & Chr(161) & "'"
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

    Private Function ProcessUpdateException(ByVal updateException As UpdateException) As String
        UpdateType = updateException.UpdateType
        Addition += updateException.Addition & "; IP:" & UserHost & "; "
        ErrorFlag = True
        If UpdateData IsNot Nothing Then
            Log.Warn(updateException)
            ProtocolUpdatesThread.Start()
        Else
            Log.Error(updateException)
        End If
        Return updateException.GetAnalitFMessage()
    End Function

    <WebMethod()> _
    Public Function GetHistoryOrders( _
        ByVal EXEVersion As String, _
        ByVal UniqueID As String, _
        ByVal ExistsServerOrderIds As UInt64(), _
        ByVal MaxOrderId As UInt64, _
        ByVal MaxOrderListId As UInt64 _
    ) As String

        Dim ResStr As String = String.Empty

        Try
            UpdateType = RequestType.GetHistoryOrders
            GetHistory = True

            DBConnect()
            GetClientCode()
            Counter.TryLock(UserId, "GetHistoryOrders")
            UpdateHelper.CheckUniqueId(readWriteConnection, UpdateData, UniqueID, UpdateType)
            UpdateData.ParseBuildNumber(EXEVersion)

            Dim historyIds As String = String.Empty
            If (ExistsServerOrderIds.Length > 0) AndAlso (ExistsServerOrderIds(0) <> 0) Then
                Dim d = ExistsServerOrderIds.Select(Function(item) item.ToString())
                If d.Count > 0 Then historyIds = String.Join(",", d.ToArray())
            End If

            SelProc = New MySqlCommand
            SelProc.Connection = readWriteConnection
            Dim transaction = readWriteConnection.BeginTransaction(IsoLevel)
            SelProc.Transaction = transaction

            Try

                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "PostedOrderHeads" & UserId & ".txt")
                ShareFileHelper.MySQLFileDelete(MySqlLocalFilePath() & "PostedOrderLists" & UserId & ".txt")

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
                    " where "

                Dim ClientIdAsField As String
                If UpdateData.IsFutureClient Then
                    SelProc.CommandText &= " OrdersHead.UserId = ?UserId "
                    ClientIdAsField = "OrdersHead.AddressId"
                Else
                    SelProc.CommandText &= " OrdersHead.ClientCode = ?ClientId "
                    ClientIdAsField = "OrdersHead.ClientCode"
                End If

                SelProc.CommandText &= " and OrdersHead.deleted = 0 and OrdersHead.processed = 1 "

                If Not String.IsNullOrEmpty(historyIds) Then
                    SelProc.CommandText &= " and OrdersHead.RowId not in (" & historyIds & ");"
                Else
                    SelProc.CommandText &= ";"
                End If

                SelProc.ExecuteNonQuery()

                SelProc.CommandText = "select count(*) from HistoryIds"
                Dim historyOrdersCount = Convert.ToInt32(SelProc.ExecuteScalar())
                If historyOrdersCount = 0 Then
                    UpdateHelper.InsertAnalitFUpdatesLog(readWriteConnection, UpdateData, UpdateType, "� ������� ��������� ��� ������� �������", UpdateData.BuildNumber)
                    Return "FullHistory=True"
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
                 "  OrdersHead.DelayOfPayment  " & _
                 "from " & _
                    " HistoryIds " & _
                    " inner join orders.OrdersHead on OrdersHead.RowId = HistoryIds.ServerOrderId ")

                '�������� �������������
                ThreadZipStream.Start()

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
                 "  OrdersList.Cost as Price, " & _
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
                 "  OrderedOffers.NDS " & _
                 "from " & _
                    " HistoryIds " & _
                    " inner join orders.OrdersHead on OrdersHead.RowId = HistoryIds.ServerOrderId " & _
                    " inner join orders.OrdersList on OrdersList.OrderId = HistoryIds.ServerOrderId " & _
                    " left join orders.OrderedOffers on OrderedOffers.Id = OrdersList.RowId ")

                AddEndOfFiles()

            Catch ex As Exception
                ConnectionHelper.SafeRollback(transaction)
                Me.Log.Error("���������� ������� �������, ��� ������� " & CCode, ex)
                If ThreadZipStream.IsAlive Then ThreadZipStream.Abort()
                ErrorFlag = True
                UpdateType = RequestType.Error
                Addition &= ex.Message
            End Try


endproc:
            If Not PackFinished And ThreadZipStream.IsAlive And Not ErrorFlag Then

                '���� ���� ������, ���������� ���������� ������
                If ErrorFlag Then

                    If ThreadZipStream.IsAlive Then ThreadZipStream.Abort()

                    PackFinished = True

                End If
                Thread.Sleep(1000)

                GoTo endproc

            ElseIf Not PackFinished And Not ErrorFlag And (UpdateType <> RequestType.Forbidden) Then

                Addition &= "; ��� ���������� �������, ������ c �������� ������� �� ������."
                UpdateType = RequestType.Forbidden

                ErrorFlag = True

            End If

            If Len(Addition) = 0 Then Addition = MessageH & " " & MessageD

            If Not ErrorFlag Then
                Dim ArhiveTS = Now().Subtract(ArhiveStartTime)

                If Math.Round(ArhiveTS.TotalSeconds, 0) > 30 Then
                    Addition &= "�������������: " & Math.Round(ArhiveTS.TotalSeconds, 0) & "; "
                End If
            End If


            ProtocolUpdatesThread.Start()

            If ErrorFlag Then

                If Len(MessageH) = 0 Then
                    ResStr = "Error=��� ���������� ������� ������� ��������� ������.;Desc=����������, ��������� ������ ������ ����� ��������� �����."
                Else
                    ResStr = "Error=" & MessageH & ";Desc=" & MessageD
                End If

            Else

                While GUpdateId = 0
                    Thread.Sleep(500)
                End While

                ResStr = "URL=" & UpdateHelper.GetDownloadUrl() & "/GetFileHistoryHandler.ashx?Id=" & GUpdateId

            End If

            Return ResStr
        Catch updateException As UpdateException
            Return ProcessUpdateException(updateException)
        Catch ex As Exception
            LogRequestHelper.MailWithRequest(Log, "������ ��� ������� ������� �������", ex)
            Return "Error=������ ������� ������� ���������� ��������.;Desc=����������, ��������� ������� ����� ��������� �����."
        Finally
            Counter.ReleaseLock(UserId, "GetHistoryOrders")
            DBDisconnect()
        End Try

    End Function

    <WebMethod()> _
    Public Function CommitHistoryOrders( _
        ByVal UpdateId As UInt64) As Boolean

        Dim transaction As MySqlTransaction
        If Log.IsDebugEnabled Then Log.Debug("������� CommitHistoryOrders")
        Try
            DBConnect()
            GetClientCode()

            FileInfo = New FileInfo(ResultFileName & "Orders" & UserId & ".zip")

            If FileInfo.Exists Then

                If Log.IsDebugEnabled Then Log.DebugFormat("������������� ���� ������� FileInfo.CreationTime {0}", FileInfo.CreationTime)

                transaction = readWriteConnection.BeginTransaction(IsoLevel)
                '                        LogCm.CommandText = "update `logs`.`AnalitFUpdates` set Commit=1, Log=?Log, Addition=concat(Addition, ifnull(?Addition, ''))  where UpdateId=" & GUpdateId

                Cm.CommandText = "update `logs`.`AnalitFUpdates` set Commit=1 where UpdateId = ?UpdateId"
                Cm.Parameters.AddWithValue("?UpdateId", UpdateId)
                Cm.Connection = readWriteConnection
                Cm.ExecuteNonQuery()
                transaction.Commit()

                If Log.IsDebugEnabled Then Log.Debug("����� � �������� ������� �����������")
            Else
                If Log.IsDebugEnabled Then Log.DebugFormat("����-����� � �������� ������� �� ���������� {0}", ResultFileName & "Orders" & UserId & ".zip")
            End If

            GetHistory = True
            ShareFileHelper.MySQLFileDelete(ResultFileName & "Orders" & UserId & ".zip")
            CommitHistoryOrders = True
            If Log.IsDebugEnabled Then Log.Debug("������� ��������� CommitHistoryOrders")
        Catch ex As Exception
            ConnectionHelper.SafeRollback(transaction)
            LogRequestHelper.MailWithRequest(Log, "������������� ������� �������", ex)
            CommitHistoryOrders = False
        Finally
            DBDisconnect()
        End Try
    End Function

End Class

