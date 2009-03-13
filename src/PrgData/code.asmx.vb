#Region " Imports "
Imports System.Web.Services
Imports System.Threading
Imports System.IO
Imports System.Web
Imports System.Text
Imports Counter
Imports MySql.Data.MySqlClient
Imports MySQLResultFile = System.IO.File
Imports Counter.Counter
Imports PrgData.Common
#End Region


    <WebService(Namespace:="IOS.Service")> _
    Public Class PrgDataEx
        Inherits System.Web.Services.WebService

#Region " Web Services Designer Generated Code "

        Public Sub New()
            MyBase.New()

            'This call is required by the Web Services Designer.
            InitializeComponent()

            'Add your own initialization code after the InitializeComponent() call
            ResultFileName = Server.MapPath("/Results") & "\"

        End Sub
        Private WithEvents SelProc As MySql.Data.MySqlClient.MySqlCommand
        Private WithEvents ArchDA As MySql.Data.MySqlClient.MySqlDataAdapter
        Private WithEvents dataTable4 As System.Data.DataTable
        Private WithEvents ArchCmd As MySql.Data.MySqlClient.MySqlCommand
        Private WithEvents ArchCn As MySql.Data.MySqlClient.MySqlConnection
    Private WithEvents DA As MySql.Data.MySqlClient.MySqlDataAdapter
    Friend WithEvents DataTable3 As System.Data.DataTable
    Friend WithEvents DataTable5 As System.Data.DataTable
    Friend WithEvents DataTable6 As System.Data.DataTable

    'Required by the Web Services Designer
    Private components As System.ComponentModel.IContainer

    'NOTE: The following procedure is required by the Web Services Designer
    'It can be modified using the Web Services Designer.  
    'Do not modify it using the code editor.
    Protected Overloads Overrides Sub Dispose(ByVal disposing As Boolean)
        'CODEGEN: This procedure is required by the Web Services Designer
        'Do not modify it using the code editor.
        If disposing Then
            If Not (components Is Nothing) Then
                components.Dispose()
            End If
        End If
        MyBase.Dispose(disposing)
    End Sub

#End Region
#Region " Variables "
    Private Const MySqlFileStr1 As String = "results/"

#If DEBUG Then

    '   Const MySqlFileStr2 As String = "\\offdc.adc.analit.net\SQLRes\"
    'Const ��������������� As String = "\\acdcserv\FTP\OptBox\"

    ReadOnly ��������������� As String = System.Configuration.ConfigurationManager.AppSettings("DocumentsPath")
    ReadOnly MySqlFileStr2 As String = System.Configuration.ConfigurationManager.AppSettings("MySqlFileStr2")


#Else

    ReadOnly ��������������� As String = System.Configuration.ConfigurationManager.AppSettings("DocumentsPath")
    ReadOnly MySqlFileStr2 As String = System.Configuration.ConfigurationManager.AppSettings("MySqlFileStr2")

#End If

    ReadOnly ZipProcessorAffinityMask As Integer = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings("ZipProcessorAffinity"))

    Private Const IsoLevel As System.Data.IsolationLevel = IsolationLevel.ReadCommitted
    Dim zipfilecount As Int32
    Private FileInfo As System.IO.FileInfo
    Private FileCount As Integer = 16
    Private tspan As New TimeSpan()
    Private ������, UserName, MessageD, MailMessage As String
    '������ � ������ �����-������, � ������� ����������� �������� �� �������
    Private AbsentPriceCodes As String
    Private MessageH As String
    Private SQLdr As MySqlDataReader
    Private rowcount, i, NewMDBVer As Integer
    Private MinCount As UInt32
    Private ErrorFlag, Documents As Boolean
    Private Addition, ClientLog As String
    Private Reclame As Boolean
    Private ResultFileName As String
    Dim ArhiveStartTime As DateTime
    Dim ArhiveTS As TimeSpan

    '������
    Private ThreadZipStream As New Thread(AddressOf ZipStream)
    Private BaseThread As New Thread(AddressOf BaseProc)
    Private ProtocolUpdatesThread As New Thread(AddressOf ProtocolUpdates)
    Private SetResultCodes As New Thread(AddressOf SetCodesProc)

    Private CurUpdTime, OldUpTime As Date
    Private ResultRow As Data.DataRow
    Private FirmSegment, BuildNo, AllowBuildNo, UpdateType, MDBVer As Integer
    Private ResultLenght, OrderId As UInt32
    Dim CCode As UInt32
    Private UserHost, UniqueCID, UID, Message, ReclamePath As String
    Private myTrans As MySqlTransaction
    Private UncDT As Date
    Private Active, AlowRegister, EnableUpdate, CheckID, NotUpdActive, GED, PackFinished, CalculateLeader As Boolean
    Private NewZip As Boolean = True
    Dim GUpdateId As UInt32 = 0
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
    'Public WithEvents Cm_ As MySql.Data.MySqlClient.MySqlCommand
    Public WithEvents OrdersL As System.Data.DataTable
    Private WithEvents OrderInsertCm As MySql.Data.MySqlClient.MySqlCommand
    Private WithEvents OrderInsertDA As MySql.Data.MySqlClient.MySqlDataAdapter
    Private WithEvents ReadOnlyCn As MySql.Data.MySqlClient.MySqlConnection
    Private ReadWriteCn As MySql.Data.MySqlClient.MySqlConnection

#End Region
    'UpdateType: 1 - �������, 2 - �������������, 3 - �������, 4 - ������, 5 - ������, 6 - ������, 7 - ������������� ���������, 8 - ������ ���������


    '�������� ������ � ���������� ���
    <WebMethod()> _
    Public Function SendLetter(ByVal subject As String, ByVal body As String, ByVal attachment() As Byte) As String
        Try

            Dim UserName As String = HttpContext.Current.User.Identity.Name
            If Left(UserName, 7) = "ANALIT\" Then
                UserName = Mid(UserName, 8)
            End If
            Dim GetQuery As String = " SELECT osuseraccessright.clientcode, clientsdata.ShortName" & _
                        " FROM clientsdata, osuseraccessright" & _
                        " where osuseraccessright.clientcode=clientsdata.firmcode" & _
                        " and allowGetData=1" & _
                        " and OSUserName='" & UserName & "'"
            Dim dr As Data.DataRow = MySqlHelper.ExecuteDataRow(Settings.ReadOnlyConnectionString(), GetQuery, Nothing)
            If dr IsNot Nothing Then
                Dim mess As System.Net.Mail.MailMessage = New System.Net.Mail.MailMessage(New System.Net.Mail.MailAddress("farm@analit.net", dr.Item("ShortName").ToString & " (" & dr.Item("ClientCode").ToString & ")"), New System.Net.Mail.MailAddress("tech@analit.net"))
                mess.Body = body
                mess.IsBodyHtml = False
                mess.BodyEncoding = Encoding.UTF8
                mess.Subject = "������ �� " & dr.Item("ShortName").ToString & ": " & subject
                If (Not IsNothing(attachment)) Then
                    mess.Attachments.Add(New System.Net.Mail.Attachment(New System.IO.MemoryStream(attachment), "Attach.7z"))
                End If
                Dim sc As System.Net.Mail.SmtpClient = New System.Net.Mail.SmtpClient("mail.adc.analit.net")
                sc.Send(mess)
            Else
                Throw New Exception("������ �� ������")
            End If

            Return "Res=OK"
        Catch ex As Exception
            Return "Error=" & ex.Message
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
        '            '    MailMessage &= "Hash ���������� �� ������: " & LibraryName(i) & ", � �������: Hash: " & LibraryHash(i) & ", ������: " & LibraryVersion(i) & "; "
        '            'End If
        '        Else
        '            'MailMessage &= "�� ��������� �� ������� ����������: " & LibraryName(i) & ", ������: " & LibraryVersion(i) & ", hash: " & LibraryHash(i) & "; "
        '        End If

        '    Next
        '    If MailMessage.Length > 0 Then
        '        'Addition &= MailMessage
        '        'MailErr("������ �������� ������ ���������", MailMessage)
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
        'Context.Request.SaveAs(ResultFileName & "res.txt", True)
        Dim LockCount As Int32
        Dim ResStr As String = String.Empty
        'StringDataSet
        Addition = " ��: " & WINVersion & " " & WINDesc & "; "



        Try


            If DBConnect("GetUserData") Then

                '�������� ������� ����������
                UpdateType = 1

                '��� ����������� ������
                ErrorFlag = False

                '������ ���������
                Documents = WayBillsOnly

                '����������� ������ ���������� � ����
                MDBVer = MDBVersion
                GED = GetEtalonData
                For i = 2 To 4
                    If Left(Right(EXEVersion, i), 1) = "." Then Exit For
                Next
                BuildNo = CInt(Right(EXEVersion, i - 1))


                CCode = 0

                '�������� ��� � ��������� ������� �������
                GetClientCode()


                'MessageH = "������ ������."
                'MessageD = "���������� � ������ ������ ����������.[5]"
                'UpdateType = 5
                'ErrorFlag = True
                'GoTo endproc


                'CCode = 3699

                If CCode < 1 Then
                    MessageH = "������ ������."
                    MessageD = "����������, ���������� � �� ��������.[1]"
                    Addition &= "��� ������ " & UserName & " ������ �� ���������������; "
                    UpdateType = 5
                    ErrorFlag = True
                    'NeedCloseCn = True
                    GoTo endproc
                End If


#If Not Debug Then

                Cm.Transaction = Nothing
                Cm.CommandText = "SELECT CurrentEXEVersion FROM usersettings.ret_update_info where clientcode=" & CCode
                AllowBuildNo = CType(Cm.ExecuteScalar, Int16)

                If BuildNo < AllowBuildNo Then
                    MessageH = "������ ������."
                    MessageD = "������������ ������ ��������� �� ���������, ���������� ���������� �� ������ �" & AllowBuildNo & ".[5]"
                    Addition &= "������� �������� ���������� ������; "
                    UpdateType = 5
                    ErrorFlag = True
                    'NeedCloseCn = True
                    GoTo endproc
                End If
#End If
                '���� � ������� ���������� ���������� ����� �������������� �������
                If Not Documents Then

#If Not Debug Then
                    MinCount = CheckUpdatePeriod()
                    If (MinCount <= 5 And UpdateType < 3) Then
                        MessageH = "��� �������. ���������� �������� �� ���� 1 ���� � 5 �����."
                        MessageD = "������ ����� ������ ����� " & CInt(5 - MinCount) + 1 & " ���.[4]"
                        Addition &= "5-�������� �����������, �������� " & CInt(5 - MinCount) + 1 & " ���."
                        UpdateType = 5
                        ErrorFlag = True
                        'NeedCloseCn = True
                        GoTo endproc
                    End If


                    If AllowBuildNo < BuildNo Then
                        Cm.Transaction = myTrans
                        Cm.CommandText = "update usersettings.ret_update_info set CurrentEXEVersion=" & BuildNo & " where clientcode=" & CCode
RestartInsertTrans:
                        Try

                            myTrans = ReadOnlyCn.BeginTransaction(IsoLevel)
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

                        End Try


                    End If

#End If
                    '���� ����������� ����� ���������� ���������� �� ������ � �������
                    If Not CheckUpdateTime(AccessTime.ToLocalTime, GED) Then
                        GED = True
                        Addition &= "����� ���������� �� ������� �� ������� � �������, ������� ��; "
                    End If


                    '������� ������������
                    If GED Then
                        UpdateType = 2
                        FileCount -= 1
                    End If

                End If



                If CheckID Then
                    UID = UniqueID
                    If Not FnCheckID() Then
                        MessageH = "���������� ��������� �� ������ ���������� ���������."
                        MessageD = "����������, ���������� � �� ��������.[2]"
                        Addition &= "�������������� UIN; "
                        UpdateType = 5
                        ErrorFlag = True
                        'NeedCloseCn = True
                        GoTo endproc
                    End If
                End If

                CurUpdTime = Now()
                If Documents Then

                    UpdateType = 8
                    Try
                        MySQLFileDelete(ResultFileName & CCode & ".zip")
                    Catch ex As Exception
                        Addition &= "�� ������� ������� ���������� ������ (��������� ������ ����������): " & ex.Message & "; "
                        UpdateType = 5
                        ErrorFlag = True
                        'NeedCloseCn = True
                        GoTo endproc
                    End Try

                Else

                    PackFinished = False

                    If CkeckZipTimeAndExist(GetEtalonData) Then


                        If Not File.GetAttributes(ResultFileName & CCode & ".zip") = FileAttributes.NotContentIndexed Then
                            'NeedCloseCn = True
                            UpdateType = 3
                            NewZip = False
                            PackFinished = True
                            GoTo endproc
                        End If

                    Else

                        Try
                            MySQLFileDelete(ResultFileName & CCode & ".zip")
                        Catch ex As Exception
                            Addition &= "�� ������� ������� ���������� ������: " & ex.Message & "; "
                            UpdateType = 5
                            ErrorFlag = True
                            ' NeedCloseCn = True
                            GoTo endproc
                        End Try

                    End If


                End If



                If Not Counter.Counter.TryLock(CCode, "GetUserData") Then
                    MessageH = "���������� ������ � ��������� ����� ����������."
                    MessageD = "����������, ��������� ������� ����� ��������� �����.[6]"
                    Addition &= "����������; "
                    UpdateType = 5
                    ErrorFlag = True
                    GoTo endproc
                End If

                If Documents Then

                    '�������� �������������
                    ThreadZipStream.Start()

                Else

                    '�������� �������� ������
                    BaseThread.Start()
                    System.Threading.Thread.Sleep(500)

                End If

                LockCount = 0
endproc:

                If Not PackFinished And (BaseThread.IsAlive Or ThreadZipStream.IsAlive) And Not ErrorFlag Then

                    '���� ���� ������, ���������� ���������� ������
                    If ErrorFlag Then

                        If ThreadZipStream.IsAlive Then ThreadZipStream.Abort()
                        If BaseThread.IsAlive Then BaseThread.Abort()
                        PackFinished = True

                    End If
                    Thread.Sleep(1000)
                    LockCount += 1


                    GoTo endproc

                ElseIf Not PackFinished And Not ErrorFlag And UpdateType <> 5 And Not WayBillsOnly Then

                    Addition &= "; ��� ���������� �������, ������ �� ������."
                    UpdateType = 5

                    ErrorFlag = True
                    ' NeedCloseCn = True

                End If
            End If


            If Len(Addition) = 0 Then Addition = MessageH & " " & MessageD

            If NewZip And Not ErrorFlag Then
                ArhiveTS = Now().Subtract(ArhiveStartTime)

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

                'ResStr = "URL=" & Context.Request.Url.Scheme & Uri.SchemeDelimiter & Context.Request.Url.Authority & "/FileHandlerService/GetFileHandler.ashx?Id=" & GUpdateId & ";New=" & NewZip & ";Cumulative=" & (UpdateType = 2)
                ResStr = "URL=" & Context.Request.Url.Scheme & Uri.SchemeDelimiter & Context.Request.Url.Authority & Context.Request.ApplicationPath & "/GetFileHandler.ashx?Id=" & GUpdateId & ";New=" & NewZip & ";Cumulative=" & (UpdateType = 2)

                If Message.Length > 0 Then ResStr &= ";Addition=" & Message

            End If

            GetUserData = ResStr

        Catch ErrorTXT As Exception
            Utils.Mail("����������� �������: " & LockCount & "; ������� �����: ", ErrorTXT.Message & ": " & ErrorTXT.StackTrace)
            UpdateType = 6
            GetUserData = "Error=��� ���������� ���������� ��������� ������.;Desc=����������, ��������� ������ ������ ����� ��������� �����."
        Finally
            ReleaseLock(CCode, "GetUserData")
        End Try
        DBDisconnect()
        GC.Collect()
    End Function


    Private Sub MySQLFileDelete(ByVal FileName As String)
        If MySQLResultFile.Exists(FileName) Then MySQLResultFile.Delete(FileName)
    End Sub


    Enum ������������ As Integer
        WayBills = 1
        Rejects = 2
        Docs = 3
    End Enum



    Public Sub ZipStream() ' � ������ ThreadZipStream
        Try


            ArhiveStartTime = Now()
            Const SevenZipExe As String = "C:\Program Files\7-Zip\7z.exe"
            Dim SevenZipParam As String = " -mx9 -bd -slp -mmt=6 -w" & Path.GetTempPath
            Dim SevenZipTmpArchive, Name As String
            Dim xRow As DataRow
            Dim FileName, �����7Z, ������7Z As String
            zipfilecount = 0
            Dim xset As New DataTable
            Dim ArchTrans As MySqlTransaction
            Dim ef(), ������������() As String


            Const ������������ As String = "runer"
            Const ������ As String = "zcxvcb"
            Dim ���������������� As New System.Security.SecureString
            For Each character As Char In ������.ToCharArray
                ����������������.AppendChar(character)
            Next
            ����������������.MakeReadOnly()



            If ArchCn.State = ConnectionState.Closed Then ArchCn.ConnectionString = Settings.ReadOnlyConnectionString
            If ArchCn.State = ConnectionState.Closed Then ArchCn.Open()

            Dim Pr As Process
            Dim startInfo As ProcessStartInfo



            If Reclame Then
                SevenZipTmpArchive = ResultFileName & "r" & CCode
            Else
                SevenZipTmpArchive = ResultFileName & CCode
            End If

            SevenZipTmpArchive &= "T.zip"
            MySQLFileDelete(SevenZipTmpArchive)


            '���� �� �������
            If Not Reclame Then

                Try

                    ArchCmd.CommandText = "SELECT  RCS.ClientCode, " & _
                 "        RowId, " & _
                 "        DocumentType " & _
                 "FROM    logs.document_logs d, " & _
                 "        retclientsset RCS " & _
                 "WHERE   RCS.ClientCode=" & CCode & _
                 "    AND RCS.ClientCode=d.ClientCode " & _
                 "    AND UpdateId     IS NULL " & _
                 "    AND FirmCode           IS NOT NULL " & _
                 "    AND AllowDocuments=1 " & _
                 "    AND d.Addition IS NULL " & _
                 " " & _
                 "UNION " & _
                 " " & _
                 "SELECT  ir.IncludeClientCode, RowId, " & _
                 "        DocumentType " & _
                 "FROM    logs.document_logs d, " & _
                 "        retclientsset RCS   , " & _
                 "        includeregulation ir " & _
                 "WHERE   ir.PrimaryClientCode=" & CCode & _
                 "    AND RCS.ClientCode      =ir.IncludeClientCode " & _
                 "    AND RCS.ClientCode      =d.ClientCode " & _
                 "    AND UpdateId           IS NULL " & _
                 "    AND FirmCode           IS NOT NULL " & _
                 "    AND AllowDocuments      =1 " & _
                 "    AND d.Addition IS NULL " & _
                 "    AND IncludeType        IN (0,3)" & _
                 "    Order by 3"


                    ArchDA.SelectCommand = ArchCmd
                    ArchDA.Fill(DS, "DocumentsToClient")

                    If DS.Tables("DocumentsToClient").Rows.Count > 0 Then

                        ArchCmd.CommandText = "" & _
                             "SELECT  * " & _
                             "FROM    AnalitFDocumentsProcessing limit 0"
                        ArchDA.FillSchema(DS, SchemaType.Source, "ProcessingDocuments")


                        Dim Row As DataRow

                        For Each Row In DS.Tables("DocumentsToClient").Rows

                            ������������ = Directory.GetFiles(��������������� & _
                                   Row.Item("ClientCode").ToString & _
                                   "\" & _
                                   CType(Row.Item("DocumentType"), ������������).ToString, _
                                   Row.Item("RowId").ToString & "_*")

                            If ������������.Length = 1 Then

                                xRow = DS.Tables("ProcessingDocuments").NewRow

                                startInfo = New ProcessStartInfo(SevenZipExe)
                                startInfo.CreateNoWindow = True
                                startInfo.RedirectStandardOutput = True
                                startInfo.RedirectStandardError = True
                                startInfo.UseShellExecute = False
                                startInfo.StandardOutputEncoding = System.Text.Encoding.GetEncoding(866)
                                'startInfo.UserName = ������������
                                'startInfo.Password = ����������������

                                startInfo.Arguments = "a " & _
                                   SevenZipTmpArchive & _
                                   " -i!""" & _
                                   CType(Row.Item("DocumentType"), ������������).ToString & "\" & _
                                   Path.GetFileName(������������(0)) & _
                                   """ " & _
                                   SevenZipParam

                                startInfo.WorkingDirectory = ��������������� & _
                                      Row.Item("ClientCode").ToString

                                xRow.Item("DocumentId") = Row.Item("RowId").ToString

                                Pr = New Process
                                Pr.StartInfo = startInfo
                                Pr = Process.Start(startInfo)
                                Pr.ProcessorAffinity = New IntPtr(ZipProcessorAffinityMask)

                                Pr.WaitForExit()


                                �����7Z = Pr.StandardOutput.ReadToEnd
                                ������7Z = Pr.StandardError.ReadToEnd

                                If Pr.ExitCode <> 0 Then

                                    MySQLFileDelete(SevenZipTmpArchive)
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

                                        Utils.Mail("������������� ����������", "����� �� 7Z � �������: " & ": " & _
                                          �����7Z & _
                                            "-" & _
                                          ������7Z)


                                    End If

                                End If

                                DS.Tables("ProcessingDocuments").Rows.Add(xRow)

                            ElseIf ������������.Length = 0 Then
                                Addition &= "��� ���������� ���������� � �����: " & _
                                    ��������������� & _
                                      Row.Item("ClientCode").ToString & _
                                      "\" & _
                                      CType(Row.Item("DocumentType"), ������������).ToString & _
                                      " �� ������ �������� � " & _
                                      Row.Item("RowId").ToString & _
                                      " ; "

                            Else


                            End If


                        Next

                    End If


                Catch ex As Exception
                    MailErr("������������� ����������", ex.Source & ": " & ex.Message)
                    Addition &= "������������� ����������" & ": " & ex.Message & "; "

                    If Documents Then ErrorFlag = True

                    MySQLFileDelete(SevenZipTmpArchive)

                End Try

                If Documents Then
                    If File.Exists(SevenZipTmpArchive) Then

                        File.Move(SevenZipTmpArchive, ResultFileName & CCode & ".zip")
                        File.SetAttributes(ResultFileName & CCode & ".zip", FileAttributes.NotContentIndexed)
                        PackFinished = True
                        FileInfo = New FileInfo(ResultFileName & CCode & ".zip")
                        ResultLenght = Convert.ToUInt32(FileInfo.Length)
                        If ArchCn.State = ConnectionState.Open Then ArchCn.Close()
                        Exit Sub

                    Else

                        MessageH = "����� ������ ���������� ���."
                        Addition &= " ��� ����� ����������"
                        ErrorFlag = True
                        PackFinished = True
                        If ArchCn.State = ConnectionState.Open Then ArchCn.Close()
                        Exit Sub

                    End If

                End If




                '���� �� ���������
                If Not Documents Then

                    '������������� ���������� ���������
                    Try
                        ArchTrans = ArchCn.BeginTransaction(IsoLevel)
                        ArchCmd.CommandText = "select EnableUpdate from retclientsset where clientcode=" & CCode
                        EnableUpdate = Convert.ToBoolean(ArchCmd.ExecuteScalar)
                        ArchTrans.Commit()

                        If EnableUpdate And Directory.Exists(ResultFileName & "Updates\" & BuildNo & "\EXE") Then

                            ef = Directory.GetFiles(ResultFileName & "Updates\" & BuildNo & "\EXE")

                            If ef.Length > 0 Then
                                'Pr.StartInfo.UserName = ������������
                                'Pr.StartInfo.Password = ����������������
                                Pr = System.Diagnostics.Process.Start(SevenZipExe, "a " & SevenZipTmpArchive & " " & ResultFileName & "Updates\" & BuildNo & "\EXE" & SevenZipParam)
                                Pr.ProcessorAffinity = New IntPtr(ZipProcessorAffinityMask)
                                Pr.WaitForExit()

                                If Pr.ExitCode <> 0 Then
                                    MailErr("������������� EXE", "����� �� 7Z � ����� " & ": " & Pr.ExitCode)
                                    Addition &= "������������� ���������� ������, ����� �� 7Z � ����� " & ": " & Pr.ExitCode & "; "
                                    'Try
                                    '    If Not pr Is Nothing Then pr.Kill()
                                    '    System.Threading.Thread.Sleep(2000)
                                    'Catch
                                    'End Try
                                    MySQLFileDelete(SevenZipTmpArchive)
                                Else

                                    'Mail("service@analit.net", "���������� ��������� � ������ " & BuildNo, MailFormat.Text, "��� �������: " & CCode, "service@analit.net", System.Text.Encoding.UTF8)
                                    Addition &= "���������� �������� � ���� ����� ������ ���������; "
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
                        MailErr("������������� Exe", ex.Source & ": " & ex.Message)
                        Addition &= " ������������� ���������� " & ": " & ex.Message & "; "
                        If Not Pr Is Nothing Then
                            If Not Pr.HasExited Then Pr.Kill()
                            Pr.WaitForExit()
                        End If
                        MySQLFileDelete(SevenZipTmpArchive)
                    End Try

                    ArchTrans = Nothing
                    ArchCmd.Transaction = Nothing


                    '������������� FRF
                    Try
                        If EnableUpdate And Directory.Exists(ResultFileName & "Updates\" & BuildNo & "\FRF") Then
                            ef = Directory.GetFiles(ResultFileName & "Updates\" & BuildNo & "\FRF")
                            If ef.Length > 0 Then
                                For Each Name In ef
                                    FileInfo = New FileInfo(Name)
                                    If FileInfo.Extension = ".frf" And FileInfo.LastWriteTime.Subtract(OldUpTime).TotalSeconds > 0 Then
                                        'Pr.StartInfo.UserName = ������������
                                        'Pr.StartInfo.Password = ����������������
                                        Pr = System.Diagnostics.Process.Start(SevenZipExe, "a " & SevenZipTmpArchive & " " & FileInfo.FullName & SevenZipParam)
                                        Pr.ProcessorAffinity = New IntPtr(ZipProcessorAffinityMask)
                                        Pr.WaitForExit()

                                        If Pr.ExitCode <> 0 Then
                                            MailErr("������������� Frf", "����� �� 7Z � ����� " & ": " & Pr.ExitCode)
                                            Addition &= " ������������� Frf, ����� �� 7Z � ����� " & ": " & Pr.ExitCode & "; "
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

                        Addition &= " ������������� Frf: " & ex.Message & "; "
                        MailErr("������������� Frf", ex.Source & ": " & ex.Message)

                        If Not Pr Is Nothing Then
                            If Not Pr.HasExited Then Pr.Kill()
                            Pr.WaitForExit()
                        End If
                        MySQLFileDelete(SevenZipTmpArchive)

                    End Try
                End If
            End If


            '������������� ������, ��� �������
            Try
                If Not Documents Then
                    ArchCmd.CommandText = "select id, name from ready_client_files where clientcode=" & CCode
                    ArchCmd.CommandText &= " and reclame="
                    If Reclame Then
                        ArchCmd.CommandText &= "1"
                    Else
                        ArchCmd.CommandText &= "0"
                    End If

                    ArchDA.MissingSchemaAction = MissingSchemaAction.AddWithKey
                    ArchDA.AcceptChangesDuringFill = False
StartZipping:
                    If ErrorFlag Then Exit Sub
                    ArchDA.Fill(DS, "results")

                    If Not DS.Tables("results").GetChanges(DataRowState.Added) Is Nothing Then

                        DS.Tables("results").BeginInit()
                        xset = DS.Tables("results").GetChanges(DataRowState.Added)
                        DS.Tables("results").AcceptChanges()
                        DS.Tables("results").EndInit()


                        For Each xRow In xset.Rows
                            If Reclame Then
                                FileName = ReclamePath & xRow.Item(1).ToString
                            Else

                                FileName = MySqlFileStr2 & xRow.Item(1).ToString & CCode & ".txt"

                                While Not File.Exists(FileName)
                                    i = +1
                                    Thread.Sleep(50)
                                    If i > 50 Then Exit While
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
                            'startInfo.WorkingDirectory = Path.GetTempPath
                            'startInfo.UserName = ������������
                            'startInfo.Password = ����������������

                            'startInfo.WindowStyle = ProcessWindowStyle.Hidden
                            'startInfo.Domain = "adc.analit.net"

                            Pr.StartInfo = startInfo
                            Pr.Start()
                            If Not Pr.HasExited Then
                                Pr.ProcessorAffinity = New IntPtr(ZipProcessorAffinityMask)
                            End If

                            �����7Z = Pr.StandardOutput.ReadToEnd
                            ������7Z = Pr.StandardError.ReadToEnd

                            Pr.WaitForExit()

                            If Pr.ExitCode <> 0 Then
                                Addition &= String.Format(" SevenZip exit code : {0}, :" & Pr.StandardError.ReadToEnd, Pr.ExitCode)
                                MySQLFileDelete(SevenZipTmpArchive)
                                Throw New Exception(String.Format("SevenZip exit code : {0}, {1}, {2}, {3}; ", Pr.ExitCode, startInfo.Arguments, �����7Z, ������7Z))
                            End If
                            Pr = Nothing
                            If Not Reclame Then MySQLFileDelete(FileName)
                            zipfilecount += 1

                            If zipfilecount >= FileCount Then
                                ArchCmd.CommandText = "delete from ready_client_files where clientcode=" & CCode
                                ArchCmd.CommandText &= " and reclame="

                                If Reclame Then

                                    ArchCmd.CommandText &= "1"
                                    File.Move(SevenZipTmpArchive, ResultFileName & "r" & CCode & ".zip")

                                Else

                                    ArchCmd.CommandText &= "0"
                                    File.Move(SevenZipTmpArchive, ResultFileName & CCode & ".zip")
                                    If UpdateType = 2 Then File.SetAttributes(ResultFileName & CCode & ".zip", FileAttributes.Normal)

                                    FileInfo = New FileInfo(ResultFileName & CCode & ".zip")
                                    ResultLenght = Convert.ToUInt32(FileInfo.Length)

                                End If
                                ArchCmd.ExecuteNonQuery()

                                PackFinished = True
                                Exit Sub

                            End If

                        Next xRow

                    Else
                        Thread.Sleep(500)


                    End If


                Else

                    Exit Sub

                End If

                Thread.Sleep(500)
                GoTo StartZipping

            Catch ex As ThreadAbortException

                If Not Pr Is Nothing Then
                    If Not Pr.HasExited Then Pr.Kill()
                    Pr.WaitForExit()
                End If
                MySQLFileDelete(SevenZipTmpArchive)
                'if Not ArchTrans Is Nothing Then ArchTrans.Rollback()

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
                    MailErr("�������������", ex.Source & ": " & ex.ToString())
                End If
                Addition &= " �������������: " & ex.ToString() & "; "

            Catch Unhandled As Exception

                ErrorFlag = True
                UpdateType = 6
                If Not Pr Is Nothing Then
                    If Not Pr.HasExited Then Pr.Kill()
                    Pr.WaitForExit()
                End If
                Addition &= " �������������: " & Unhandled.ToString()
                MySQLFileDelete(SevenZipTmpArchive)
                MailErr("�������������", Unhandled.Source & ": " & Unhandled.ToString())
                Addition &= " �������������: " & Unhandled.ToString() & "; "
                'If Not ArchTrans Is Nothing Then ArchTrans.Rollback()

            Finally
                If ArchCn.State = ConnectionState.Open Then ArchCn.Close()
            End Try

        Catch tae As ThreadAbortException

        Catch Unhandled As Exception
            MailErr("������������� general", Unhandled.Source & ": " & Unhandled.ToString())
            ErrorFlag = True
        End Try
    End Sub





    <WebMethod()> _
    Public Function MaxSynonymCode(ByVal Log As String, _
       ByVal PriceCode As UInt32(), _
       ByVal UpdateId As UInt32, _
       ByVal WayBillsOnly As Boolean) As Date
        'Dim ef() As String
        'Dim FileName As String
        Dim UpdateTime As Date
        Cm.Transaction = Nothing
        ClientLog = Log
        GUpdateId = UpdateId

        If DBConnect("MaxSynonymCode") Then
            GetClientCode()
            UpdateType = 7

            If Not Counter.Counter.TryLock(CCode, "MaxSynonymCode") Then
                Return DateTime.Now
            End If

            If Not WayBillsOnly Or Not File.GetAttributes(ResultFileName & CCode & ".zip") = FileAttributes.NotContentIndexed Then

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
                    ������ = "select UncommittedUpdateTime from ret_update_info  where clientcode=" & CCode & "; "
                    Cm.CommandText = ������
                    SQLdr = Cm.ExecuteReader
                    SQLdr.Read()
                    UpdateTime = SQLdr.GetDateTime(0)
                    SQLdr.Close()
                End If

            Catch ex As Exception
                MailErr("������� ���� ���������� ", ex.Message & ex.Source)
                UpdateTime = Now().ToUniversalTime
            End Try

            If SetResultCodes.IsAlive Then SetResultCodes.Join()

            'ProtocolUpdatesThread.Start()

            MaxSynonymCode = UpdateTime.ToUniversalTime
        Else
            MaxSynonymCode = Now().ToUniversalTime
        End If

        Try

            MySQLFileDelete(ResultFileName & CCode & ".zip")
            MySQLFileDelete(ResultFileName & "r" & CCode & "Old.zip")

        Catch ex As Exception
            'MailErr("�������� ���������� ������;", ex.Message)
        End Try

        DBDisconnect()
        ProtocolUpdatesThread.Start()
        ReleaseLock(CCode, "MaxSynonymCode")
    End Function

    Private Sub GetClientCode()
        UserName = HttpContext.Current.User.Identity.Name
        If Left(UserName, 7) = "ANALIT\" Then
            UserName = Mid(UserName, 8)
        End If
        Try

            ������ = "" & _
         "SELECT  ouar.clientcode                            , " & _
         "        firmsegment                                , " & _
         "        rui.Updatetime                             , " & _
         "        rui.UncommittedUpdateTime                  , " & _
         "        AlowRegister                               , " & _
         "        IF(ShowMessageCount<1, '', MESSAGE) MESSAGE, " & _
         "        CheckCopyID " & _
         "FROM    clientsdata           , " & _
         "        retclientsset         , " & _
         "        ret_update_info rui   , " & _
         "        UserPermissions up    , " & _
         "        AssignedPermissions ap, " & _
         "        osuseraccessright ouar " & _
         "        LEFT JOIN IncludeRegulation ir " & _
         "        ON      IncludeClientCode=ouar.ClientCode " & _
         "WHERE   ouar.clientcode          =clientsdata.firmcode " & _
         "    AND ouar.rowid               = ap.userid " & _
         "    AND up.id                    = ap.permissionid " & _
         "    AND up.Shortcut              = 'AF' " & _
         "    AND IF(ir.id                IS NULL, 1, ir.IncludeType IN (1,2,3)) " & _
         "    AND retclientsset.clientcode =ouar.clientcode " & _
         "    AND rui.clientcode           =ouar.clientcode " & _
         "    AND firmstatus               =1 " & _
         "    AND OSUserName               ='" & UserName & "'"

            Cm.Connection = ReadOnlyCn
            Cm.Transaction = Nothing
            Cm.CommandText = ������
            SQLdr = Cm.ExecuteReader



            While SQLdr.Read()

                CCode = SQLdr.GetUInt32("clientcode")
                FirmSegment = SQLdr.GetUInt16("firmsegment")
                OldUpTime = SQLdr.GetDateTime("Updatetime")
                UncDT = SQLdr.GetDateTime("UncommittedUpdateTime")
                AlowRegister = SQLdr.GetBoolean("alowregister")
                Message = SQLdr.GetString("message")
                CheckID = SQLdr.GetBoolean("CheckCopyID")
            End While
            SQLdr.Close()

            With Cm
                .Parameters.Add(New MySqlParameter("?UserName", MySqlDbType.VarString))
                .Parameters("?UserName").Value = UserName

                .Parameters.Add(New MySqlParameter("?ClientCode", MySqlDbType.Int32))
                .Parameters("?ClientCode").Value = CCode
            End With

            If CCode > 0 Then
                myTrans = ReadOnlyCn.BeginTransaction
                Cm.Connection = ReadWriteCn
                Cm.Transaction = Nothing
                Cm.CommandText = "" & _
                    "UPDATE Logs.AuthorizationDates A " & _
                    "SET     AFTime    =now() " & _
                    "WHERE   clientcode=?ClientCode; "
                Cm.ExecuteNonQuery()
                myTrans.Commit()
            End If


            'LogWrite("����� ���� �������", "", 2, rowcount)
        Catch ErrorTXT As Exception
            UpdateType = 6
            'If Not myTrans Is Nothing Then
            '	myTrans.Rollback()
            'End If
            MailErr("��������� ���������� � �������", ErrorTXT.Message)
        Finally
            If Not SQLdr.IsClosed Then SQLdr.Close()
        End Try
    End Sub



    Private Function DBConnect(ByVal FromProcess As String) As Boolean
        UserHost = HttpContext.Current.Request.UserHostAddress
        Try
            ReadOnlyCn = New MySqlConnection
            ReadOnlyCn.ConnectionString = Settings.ReadOnlyConnectionString()
            ReadOnlyCn.Open()
            ReadWriteCn = New MySqlConnection
            ReadWriteCn.ConnectionString = Settings.ReadWriteConnectionString()
            ReadWriteCn.Open()
            'MailErr("���������� � ��, " & FromProcess, "����� �" & ReadOnlyCn.ServerThread)
            Return True
        Catch err As Exception
            MailErr("���������� � ��", err.Message)
            ErrorFlag = True
            UpdateType = 6
            Return False
        End Try
    End Function





    Private Sub DBDisconnect()

        Dim ResultCode As Int16 = 2
        Try
            'MailErr("������������ �� ��", "����� �" & ReadOnlyCn.ServerThread)
            ReadOnlyCn.Close()
            Cm.Dispose()
            ReadOnlyCn.Dispose()
        Catch err As Exception
            MailErr("�������� ����������", err.Message)
            ErrorFlag = True
            UpdateType = 6
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
        '            'MailErr("��������� � ������� �������� ������", list.Count & " ��.")
        '            Return String.Join(";", list.ToArray())
        '        Else
        '            Return String.Empty
        '        End If

        '    Catch Exp As Exception
        '        MailErr("������ ��� ��������� ������ �������� �������", Exp.Message & ": " & Exp.StackTrace)
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
        Dim ControlMinReq As Boolean
        Dim MinReq As UInt32
        Dim SumOrder As UInt32
        Dim it As Integer

        Dim IntMinCost As Decimal()
        Dim IntMinPriceCode As UInt32()
        Dim IntLeaderMinCost As Decimal()
        Dim IntRequestRatio As UInt16()
        Dim IntOrderCost As Decimal()
        Dim IntMinOrderCount As UInt32()
        Dim IntLeaderMinPriceCode As UInt32()




        If DBConnect("PostOrder") Then

            'Context.Request.SaveAs(ResultFileName & "res.txt", True)

            GetClientCode()
            'Dim ID As String = Now().ToFileTimeUtc

            If CCode = Nothing Then
                CCode = ClientCode
                UpdateType = 5
                MessageH = "������ ������."
                MessageD = "����������, ���������� � �� ""�������""."
                ErrorFlag = True

                GoTo ItsEnd
            End If



            If Not Counter.Counter.TryLock(CCode, "PostOrder") Then
                MessageH = "�������� ������� � ��������� ����� ����������."
                MessageD = "����������, ��������� ������� ����� ��������� �����.[7]"
                Addition &= "����������; "
                UpdateType = 5
                ErrorFlag = True
                GoTo ItsEnd
            End If

            If ServerOrderId = 0 Then

                '��������� ���������� ����������� ��������������
                If CheckID Then
                    UID = UniqueID
                    If Not FnCheckID() Then
                        MessageH = "�������� ������� �� ������ ���������� ���������."
                        MessageD = "����������, ���������� � �� ��������.[2]"
                        Addition = "�������������� UIN."
                        UpdateType = 5
                        ErrorFlag = True

                        GoTo ItsEnd
                    End If
                End If

                With Cm
                    .Connection = ReadOnlyCn
                    .CommandText = "select ifnull(Max(IncludeClientCode=?ClientCode or ClientCode=?ClientCode), 0) as A from osuseraccessright" & _
                    " left join  includeregulation on PrimaryClientCode=ClientCode and IncludeType in (0,3)" & _
                    " where osusername=?UserName"
                    .Transaction = Nothing
                End With

                If Not Convert.ToBoolean(Cm.ExecuteScalar) Then
                    UpdateType = 5
                    MessageH = "�������� ������� ���������."
                    MessageD = "���������� ���������� � �� ""�������""."
                    ErrorFlag = True
                    'MailErr("������������ ��� ������� ��� �������� �������.", "��� �������: " & CCode)
                End If

                If ErrorFlag Then GoTo ItsEnd

                '�������� ��������� ����������� �����
                Try
                    Cm.CommandText = "SELECT i.ControlMinReq, if(i.MinReq>0, i.MinReq, prd.MinReq) FROM usersettings.intersection i " & _
                     " inner join usersettings.pricesregionaldata prd on prd.pricecode = i.priceCode and prd.RegionCode = i.regionCode " & _
                     " where i.ClientCode = ?ClientCode and prd.PriceCode = ?PriceCode and prd.RegionCode = ?RegionCode"
                    Cm.Parameters.Clear()

                    Cm.Parameters.Add(New MySqlParameter("?ClientCode", MySqlDbType.UInt32))
                    Cm.Parameters("?ClientCode").Value = ClientCode
                    Cm.Parameters.Add(New MySqlParameter("?PriceCode", MySqlDbType.UInt32))
                    Cm.Parameters("?PriceCode").Value = PriceCode
                    Cm.Parameters.Add(New MySqlParameter("?RegionCode", MySqlDbType.UInt64))
                    Cm.Parameters("?RegionCode").Value = RegionCode

                    SQLdr = Cm.ExecuteReader
                    SQLdr.Read()
                    ControlMinReq = SQLdr.GetBoolean(0)
                    MinReq = SQLdr.GetUInt32(1)
                    SQLdr.Close()

                    If ControlMinReq And MinReq > 0 Then
                        SumOrder = 0
                        For it = 0 To Cost.Length - 1
                            SumOrder += Convert.ToUInt32(Math.Round(Quantity(it) * Cost(it), 0))
                        Next
                        If SumOrder < MinReq Then
                            MessageD = "��������� ������� � ������ ������."
                            MessageH = "����� ������ ������ ���������� ����������."
                            UpdateType = 5
                            ErrorFlag = True
                            GoTo ItsEnd
                        End If
                    End If

                Catch err As Exception
                    UpdateType = 6
                    ErrorFlag = True
                    MailErr("���� ����������� ����. ������: " & CCode, "������: " & err.Source & ": " & err.StackTrace)
                End Try



                If ErrorFlag Then GoTo ItsEnd


                With Cm
                    .Transaction = myTrans
                    '.Parameters.Add(New MySqlParameter("?PriceCode", MySqlDbType.UInt32))
                    '.Parameters("?PriceCode").Value = PriceCode

                    .Parameters.Add(New MySqlParameter("?ClientOrderID", MySqlDbType.UInt32))
                    .Parameters("?ClientOrderID").Value = ClientOrderID

                    .Parameters.Add(New MySqlParameter("?PriceDate", MySqlDbType.DateTime))
                    .Parameters("?PriceDate").Value = PriceDate.ToLocalTime

                    ResStr = String.Empty
                    Try
                        For i = 1 To Len(ClientAddition) Step 3
                            ResStr &= Chr(Convert.ToInt16(Left(Mid(ClientAddition, i), 3)))
                        Next
                    Catch err As Exception
                        'MailErr("������������ ��������� ���������� ", ResStr & "; C�����: " & Left(Mid(ClientAddition, i), 3) & "; �������� ������:" & ClientAddition & "; ������:" & err.Message)
                    End Try

                    .Parameters.Add(New MySqlParameter("?ClientAddition", MySqlDbType.Blob))
                    .Parameters("?ClientAddition").Value = ResStr

                    .Parameters.Add(New MySqlParameter("?RowCount", MySqlDbType.UInt16))
                    .Parameters("?RowCount").Value = RowCount

                End With
            End If
RestartInsertTrans:

            Try

                myTrans = ReadOnlyCn.BeginTransaction(IsoLevel)

                If ServerOrderId = 0 Then
                    Cm.CommandText = "" & _
                     "INSERT " & _
                     "INTO    orders.ordershead " & _
                     "        ( " & _
                     "                ClientCode    , " & _
                     "                PriceCode     , " & _
                     "                RegionCOde    , " & _
                     "                PriceDate     , " & _
                     "                ClientAddition, " & _
                     "                RowCount      , " & _
                     "                ClientOrderID , " & _
                     "                Submited, " & _
                     "                SubmitDate " & _
                     "        ) " & _
                     "SELECT  ?ClientCode    , " & _
                     "        ?PriceCode     , " & _
                     "        ?RegionCode    , " & _
                     "        ?PriceDate     , " & _
                     "        ?ClientAddition, " & _
                     "        ?RowCount      , " & _
                     "        ?ClientOrderID , " & _
                     "        NOT (SubmitOrders and AllowSubmitOrders), " & _
                     "        IF(NOT(SubmitOrders and AllowSubmitOrders), NOW(), NULL) " & _
                     "FROM    RetClientsSet RCS " & _
                     "WHERE   RCS.ClientCode=?ClientCode;"
                    Cm.CommandText &= "select LAST_INSERT_ID()"
                    OID = Convert.ToUInt64(Cm.ExecuteScalar)
                    Cm.CommandText = "select CalculateLeader from retclientsset where clientcode=?ClientCode"
                    CalculateLeader = Convert.ToBoolean(Cm.ExecuteScalar)

                Else

                    Cm.CommandText = "SELECT RowId FROM orders.ordershead where ClientOrderid=" & ServerOrderId
                    OID = Convert.ToUInt64(Cm.ExecuteScalar)
                    'MailErr("������� �������� �����", "����� �" & ServerOrderId)

                End If

                OrderInsertCm.CommandText = "SELECT " & _
                 "        `MinCost`          , " & _
                 "        `LeaderMinCost`    , " & _
                 "        `CostCode`         , " & _
                 "        `LeaderCostCode`   , " & _
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

                MailErr("������� ������. ������: " & CCode, "������ MySQL(" & MySQLErr.Number & "): " & MySQLErr.Message & " � " & MySQLErr.Source & ": " & MySQLErr.StackTrace)

                UpdateType = 6
                ErrorFlag = True

                'Context.Request.SaveAs(ResultFileName & ID & ".txt", True)

            Catch err As Exception

                MailErr("������� ������. ������: " & CCode, "������: " & err.Message & ": " & err.Source & ": " & err.StackTrace)

                Try
                    myTrans.Rollback()
                Catch
                End Try

                UpdateType = 6
                ErrorFlag = True




                'Context.Request.SaveAs(ResultFileName & ID & ".txt", True)

            End Try

ItsEnd:

            DBDisconnect()


        Else
            MessageH = "�������� ������� ����������� ��������."
            MessageD = "���������� ��������� ������� ����� ��������� �����."
            ErrorFlag = True
        End If

        If ErrorFlag Or UpdateType > 4 Then
            If Len(MessageH) = 0 Then
                PostOrder = "Error=�������� ������� ����������� ��������.;Desc=��������� ������ �� ���� �����������."
            Else
                Addition = MessageH & " " & MessageD
                PostOrder = "Error=" & MessageH & ";Desc=" & MessageD
            End If
        Else
            PostOrder = "OrderID=" & OID
        End If

        ReleaseLock(CCode, "PostOrder")

    End Function

    Public Sub MailErr(ByVal ErrSource As String, ByVal ErrDesc As String)
        Utils.Mail("������: " & CCode & Chr(10) & Chr(13) & "�������: " & ErrSource & Chr(10) & Chr(13) & "��������: " & ErrDesc, "AF �����c: " & ErrSource)
    End Sub

    ' Private Sub MailUpdate(ByVal OldMDBVersion As Int32, ByVal NewMDBVersion As Int32, ByVal OldEXEVersion As Int32, ByVal NewEXEVersion As Int32)
    '        Cm.CommandText = " insert into logs.programmupgrade values(null, now(), " & CCode & ", " & OldMDBVersion & ", " & NewMDBVersion & ", " & OldEXEVersion & ", " & NewEXEVersion & "); SELECT email," & _
    '" ShortName FROM clientsdata, accessright.regionaladmins" & _
    '" where SendAlert=1 and RegionCode & regionaladmins.regionmask>0 and firmcode=" & CCode
    '        SQLdr = Cm.ExecuteReader
    '        While SQLdr.Read
    '            Mail("service@analit.net", "���������� ��������� - " & SQLdr.Item(1), MailFormat.Text, "��� �������: ", SQLdr.Item(0), System.Text.Encoding.UTF8)
    '        End While
    '        SQLdr.Close()

    '    End Sub

    Private Function FnCheckID() As Boolean
#If DEBUG Then
        Return True
#Else

        Cm.Transaction = myTrans
RePost:

        Cm.CommandText = "select UniqueCopyID from ret_update_info where clientcode=?ClientCode"
        'Cm.Parameters.Add(New MySqlParameter("?ClientCode", MySqlDbType.Int32))
        'Cm.Parameters("?ClientCode").Value = CCode
        Cm.Transaction = myTrans

        myTrans = ReadOnlyCn.BeginTransaction(IsoLevel)
        ' Cm.Parameters.
        SQLdr = Cm.ExecuteReader
        SQLdr.Read()
        UniqueCID = SQLdr.GetString(0)
        SQLdr.Close()

        If UniqueCID.Length < 1 Then
            Try
                Cm.CommandText = "update  ret_update_info set UniqueCopyID='" & UID & "' where clientcode=?ClientCode"

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
                'MailErr("�������������� UIN", UniqueCID & " " & UID)
                FnCheckID = False
            Else
                FnCheckID = True
            End If

        End If

        myTrans.Commit()
#End If
    End Function

    Private Sub ProtocolUpdates()
        Dim LogTrans As MySqlTransaction
        Dim LogCn As New MySqlConnection
        Dim LogCb As New MySqlCommandBuilder
        Dim LogDA As New MySqlDataAdapter
        Dim NoNeedProcessDocuments As Boolean = False


        LogCn.ConnectionString = Settings.ReadWriteConnectionString
        Try
            LogCn.Open()

            LogCm.Connection = LogCn
            LogCb.DataAdapter = DA

            If BaseThread.IsAlive Then BaseThread.Join()
            If ThreadZipStream.IsAlive Then ThreadZipStream.Join()

            If CCode < 1 Then
                LogCm.CommandText = "SELECT clientcode FROM osuseraccessright o " & _
                                    "where osusername='" & UserName & "'"
                CCode = Convert.ToUInt32(LogCm.ExecuteScalar)
                NoNeedProcessDocuments = True
            End If

            Try
                If UpdateType = 1 _
                Or UpdateType = 2 _
                Or UpdateType = 5 _
                Or UpdateType = 6 _
                Or UpdateType = 8 Then

                    LogTrans = LogCn.BeginTransaction(IsoLevel)
                    LogCm.CommandText = "insert into `logs`.`AnalitFUpdates`(`RequestTime`, `UpdateType`, `ClientCode`, `UserName`, `AppVersion`, `DBVersion`, `ResultSize`, `Addition`) values(?UpdateTime, ?UpdateType, ?ClientCode, ?UserName, ?exeversion, ?mdbversion, ?Size, ?Addition); "
                    LogCm.CommandText &= "select last_insert_id()"

                    With LogCm

                        .Transaction = LogTrans
                        .Parameters.Add(New MySqlParameter("?ClientCode", CCode))

                        .Parameters.Add(New MySqlParameter("?ClientHost", UserHost))

                        .Parameters.Add(New MySqlParameter("?UserName", UserName))

                        .Parameters.Add(New MySqlParameter("?UpdateType", UpdateType))

                        .Parameters.Add(New MySqlParameter("?EXEVersion", BuildNo))

                        .Parameters.Add(New MySqlParameter("?MDBVersion", MDBVer))

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
                                            "    AND clientcode  =" & CCode

                    GUpdateId = Convert.ToUInt32(LogCm.ExecuteScalar)

                End If

                If Not NoNeedProcessDocuments Then

                    If UpdateType = 7 Then
                        Dim ������������() As String

                        LogTrans = LogCn.BeginTransaction(IsoLevel)
                        LogCm.CommandText = "update `logs`.`AnalitFUpdates` set Commit=1, Log=?Log, Addition=concat(Addition, ifnull(?Addition, ''))  where UpdateId=" & GUpdateId

                        LogCm.Parameters.Add(New MySqlParameter("?Log", MySqlDbType.VarString))
                        LogCm.Parameters("?Log").Value = ClientLog

                        LogCm.Parameters.Add(New MySqlParameter("?Addition", MySqlDbType.VarString))
                        LogCm.Parameters("?Addition").Value = Addition

                        LogCm.ExecuteNonQuery()


                        LogCm.CommandText = "" & _
                                            "SELECT  DocumentId, " & _
                                            "        DocumentType, " & _
                                            "        ClientCode " & _
                                            "FROM    AnalitFDocumentsProcessing AFDP, " & _
                                            "        `logs`.document_logs DL " & _
                                            "WHERE   DL.RowId=AFDP.DocumentId " & _
                                            "AND     AFDP.UpdateId=" & GUpdateId

                        LogDA.SelectCommand = LogCm
                        LogDA.Fill(DS, "ProcessingDocuments")

                        If DS.Tables("ProcessingDocuments").Rows.Count > 0 Then
                            Dim DocumentsIdRow As DataRow

                            For Each DocumentsIdRow In DS.Tables("ProcessingDocuments").Rows

                                ������������ = Directory.GetFiles(��������������� & _
                                               DocumentsIdRow.Item("ClientCode").ToString & _
                                               "\" & _
                                               CType(DocumentsIdRow.Item("DocumentType"), ������������).ToString, _
                                               DocumentsIdRow.Item("DocumentId").ToString & "_*")

                                MySQLResultFile.Delete(������������(0))

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

                        'If DS.Tables("Documents").Rows.Count > 0 Then

                        '    LogCm.CommandText = "SELECT * FROM logs.document_logs"
                        '    DA.SelectCommand = LogCm
                        '    LogCb.ConflictOption = ConflictOption.CompareRowVersion
                        '    DA.UpdateCommand = LogCb.GetUpdateCommand
                        '    For i = 0 To DS.Tables("Documents").Rows.Count - 1
                        '        DS.Tables("Documents").Rows(i).Item("UpdateId") = GUpdateId
                        '    Next
                        '    Dim rc As Integer = DA.Update(DS, "Documents")

                        'End If

                        LogTrans.Commit()
                    End If

                End If

            Catch MySQLErr As MySqlException
                If MySQLErr.Number = 1213 Or MySQLErr.Number = 1205 Then
                    LogTrans.Rollback()
                    MailErr("Log " & CCode, "Deadlock")
                    System.Threading.Thread.Sleep(500)
                    GoTo PostLog
                End If
                MailErr("������ ����", MySQLErr.Message & ": " & MySQLErr.StackTrace)
                LogTrans.Rollback()

            Catch err As Exception
                MailErr("������ ����", err.Message & ": " & err.StackTrace)
                LogTrans.Rollback()

            Finally

                Try

                    LogCn.Close()
                    LogCm.Dispose()
                    LogCn.Dispose()

                Catch err As Exception
                    MailErr("�������� ���������� Log ", err.Message)
                End Try
            End Try
        Catch err As Exception
            MailErr("general Log ", err.Message)
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
        Me.DataTable6 = New System.Data.DataTable
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
        Me.SelProc.CommandType = System.Data.CommandType.StoredProcedure
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
        '
        'DataTable6
        '
        Me.DataTable6.RemotingFormat = System.Data.SerializationFormat.Binary
        Me.DataTable6.TableName = "ProcessingDocuments"
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
        FileInfo = New FileInfo(ResultFileName & CCode & ".zip")
        If FileInfo.Exists Then
            CkeckZipTimeAndExist = (((Date.UtcNow.Subtract(UncDT.ToUniversalTime).TotalHours < 1 And Not GetEtalonData) _
        Or (OldUpTime.Year = 2003 And DateTime.UtcNow.Subtract(UncDT.ToUniversalTime).TotalHours < 6))) Or (File.GetAttributes(ResultFileName & CCode & ".zip") = FileAttributes.Normal And GetEtalonData)
        Else
            CkeckZipTimeAndExist = False
        End If
        'CkeckZipTimeAndExist = True
    End Function

    Private Function CheckUpdateTime(ByVal AccessTime As DateTime, ByVal GetEtalonData As Boolean) As Boolean
        Dim LCheckUpdateTime As Boolean
        LCheckUpdateTime = (OldUpTime = AccessTime Or _
                                        GetEtalonData)
        'If Not LCheckUpdateTime Then Return False

        Try
            With Cm
                .Connection = ReadWriteCn
                .CommandText = "update ret_update_info set UncommittedUpdateTime=now() where clientcode=?ClientCode"
                .CommandText &= "; select UncommittedUpdateTime from ret_update_info where clientcode=?ClientCode"
                .Transaction = myTrans

            End With

Restart:
            myTrans = ReadWriteCn.BeginTransaction(IsoLevel)
            Cm.Transaction = myTrans
            SQLdr = Cm.ExecuteReader()
            SQLdr.Read()
            CurUpdTime = SQLdr.GetDateTime(0)
            SQLdr.Close()
            myTrans.Commit()

        Catch MySQLErr As MySqlException
            If Not (ReadOnlyCn.State = ConnectionState.Closed Or ReadOnlyCn.State = ConnectionState.Broken) Then myTrans.Rollback()
            If MySQLErr.Number = 1213 Or MySQLErr.Number = 1205 Then
                System.Threading.Thread.Sleep(500)
                GoTo Restart
            End If
            MailErr("���������� ����������������� �������, ������: " & CCode, MySQLErr.Message)
            ErrorFlag = True
        Catch ex As Exception
            MailErr("���������� ����������������� �������, ������: " & CCode, ex.Message)
            ErrorFlag = True
        End Try

        Return LCheckUpdateTime



    End Function

    Private Function CheckUpdatePeriod() As UInt32
        Dim Min1, Min2, MinRes As UInt32
        Min1 = CType(Math.Round(Now().Subtract(OldUpTime).TotalMinutes), UInt32)
        Min2 = CType(Math.Round(Now().Subtract(UncDT).TotalMinutes), UInt32)

        Cm.Transaction = Nothing

        Cm.CommandText = "select not exists(SELECT * FROM ret_update_info rui, logs.AnalitFUpdates p " & _
        "where p.requesttime = rui.UncommittedUpdateTime " & _
        "and rui.clientcode=p.clientcode " & _
        "and rui.clientcode=" & CCode & ")"

        If CType(Cm.ExecuteScalar, UInt16) = 1 Then

            MinRes = Min1
            If Min2 < MinRes Then MinRes = Min2

        Else
            MinRes = 10

        End If


        Return MinRes
    End Function


    Private Sub BaseProc()
        Dim StartTime As DateTime = Now()
        Dim TS As TimeSpan
        Dim ReadSelTrans As MySqlTransaction
        Dim WriteSelTrans As MySqlTransaction
        Try
            Try
                'If NeedCloseCn Then Exit Try
                SelProc.Parameters.AddWithValue("?ClientCodeParam", CCode)
                SelProc.Parameters.AddWithValue("?FieldsTerminatedParam", "")
                SelProc.Parameters.AddWithValue("?LinesTerminatedParam", "")
                SelProc.Parameters.AddWithValue("?Cumulative", GED)

                SelProc.Connection = ReadOnlyCn
                Cm.Connection = ReadWriteCn
                Cm.Transaction = Nothing

RestartTrans2:
                If ErrorFlag Then Exit Try

                MySQLFileDelete(MySqlFileStr2 & "Products" & CCode & ".txt")
                MySQLFileDelete(MySqlFileStr2 & "Catalog" & CCode & ".txt")
                MySQLFileDelete(MySqlFileStr2 & "CatalogCurrency" & CCode & ".txt")
                MySQLFileDelete(MySqlFileStr2 & "CatDel" & CCode & ".txt")
                MySQLFileDelete(MySqlFileStr2 & "Clients" & CCode & ".txt")
                MySQLFileDelete(MySqlFileStr2 & "ClientsDataN" & CCode & ".txt")
                MySQLFileDelete(MySqlFileStr2 & "Core" & CCode & ".txt")
                MySQLFileDelete(MySqlFileStr2 & "MinPrices" & CCode & ".txt")
                MySQLFileDelete(MySqlFileStr2 & "PriceAvg" & CCode & ".txt")
                MySQLFileDelete(MySqlFileStr2 & "PricesData" & CCode & ".txt")
                MySQLFileDelete(MySqlFileStr2 & "PricesRegionalData" & CCode & ".txt")
                MySQLFileDelete(MySqlFileStr2 & "RegionalData" & CCode & ".txt")
                MySQLFileDelete(MySqlFileStr2 & "Regions" & CCode & ".txt")
                MySQLFileDelete(MySqlFileStr2 & "Section" & CCode & ".txt")
                MySQLFileDelete(MySqlFileStr2 & "Synonym" & CCode & ".txt")
                MySQLFileDelete(MySqlFileStr2 & "SynonymFirmCr" & CCode & ".txt")
                MySQLFileDelete(MySqlFileStr2 & "Rejects" & CCode & ".txt")
                MySQLFileDelete(MySqlFileStr2 & "CatalogFarmGroups" & CCode & ".txt")
                MySQLFileDelete(MySqlFileStr2 & "CatalogNames" & CCode & ".txt")
                MySQLFileDelete(MySqlFileStr2 & "CatFarmGroupsDel" & CCode & ".txt")


                'Cm.Transaction = myTrans
               
                Cm.CommandText = "delete from ready_client_files where clientcode=?ClientCode and not reclame;"
                Cm.ExecuteNonQuery()

                

                SelProc.CommandText = "PrgData1"


                If ThreadZipStream.IsAlive Then
                    ThreadZipStream.Abort()
                    System.Threading.Thread.Sleep(500)
                End If


                ThreadZipStream = New Thread(AddressOf ZipStream)
                ThreadZipStream.Start()

                myTrans = ReadOnlyCn.BeginTransaction(IsoLevel)
                SelProc.Transaction = myTrans
                SelProc.ExecuteNonQuery()
                myTrans.Commit()

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

                MailErr("�������� ����� �������: " & MySQLErr.Message, MySQLErr.StackTrace)
                ErrorFlag = True
                UpdateType = 6
                'NeedCloseCn = True
                Addition &= MySQLErr.Message


            Catch ErrorTXT As Exception
                ErrorFlag = True
                'NeedCloseCn = True
                UpdateType = 6
                Addition &= ErrorTXT.Message
                MailErr("�������� ����� �������, ������: " & CCode, ErrorTXT.Message)
                If ThreadZipStream.IsAlive Then ThreadZipStream.Abort()
                If Not (ReadOnlyCn.State = ConnectionState.Closed Or ReadOnlyCn.State = ConnectionState.Broken) Then myTrans.Rollback()
            End Try
        Catch err As Exception
            MailErr("�������� ����� �������, general " & CCode, err.Message & ": " & err.StackTrace)
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
        Dim MinCostP As Decimal = 0
        Dim LaederMinCostP As Decimal = 0
        Dim ProblemStr As String = String.Empty

        DS.Tables("OrdersL").Clear()
        DS.Tables("OrdersDouble").Clear()


        For i = 0 To ProductID.Length - 1
            newrow = DS.Tables("OrdersL").NewRow
            newrow.Item("ProductID") = ProductID(i)
            newrow.Item("SynonymCode") = SynonymCode(i)

            If SynonymFirmCrCode(i).Length < 1 Then
                newrow.Item("SynonymFirmCrCode") = DBNull.Value
            Else
                newrow.Item("SynonymFirmCrCode") = SynonymFirmCrCode(i)
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
                    MailErr("������������ Code", err.Message)
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
                    MailErr("������������ CodeCr", err.Message)
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

                If UInt32.TryParse(MinPriceCode(i), MinPriceCodeP) Then newrow.Item("CostCode") = MinPriceCodeP
                If UInt32.TryParse(LeaderMinPriceCode(i), LeaderMinPriceCodeP) Then newrow.Item("LeaderCostCode") = LeaderMinPriceCodeP

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
"                LEFT JOIN includeregulation ir " & _
"                ON      ir.includeclientcode=" & ClientCode & _
"                    AND includetype        IN (0,3) " & _
"        WHERE   updatetype                  =2 " & _
"            AND px.clientcode               =ifnull(ir.primaryclientcode, oh.clientcode) " & _
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
                                MailErr("������������� �����", e.Message & ": " & e.StackTrace)
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
            'MailErr("������������� �����", ProblemStr)
            Return False
            'Exit Function
        End If

        With OrderInsertCm
            .CommandText = String.Empty
            .Parameters.Clear()
            If ProblemStr <> String.Empty Then
                Addition = ProblemStr
                .CommandText = "update orders.ordershead set rowcount=" & DS.Tables("OrdersL").Rows.Count & " where rowid=" & OrderID & "; "
                'MailErr("����� � ������", ProblemStr)
            End If

            .CommandText &= " insert into orders.orderslist (OrderID, ProductId, CodeFirmCr, SynonymCode, SynonymFirmCrCode, Code, CodeCr, Quantity, Junk, Await, Cost)" & _
                                        " select  " & OrderID & ", products.ID, if(cfcr.codefirmcr is null, sfcr.codefirmcr, cfcr.codefirmcr) , syn.synonymcode, sfcr.SynonymFirmCrCode, ?Code, ?CodeCr, ?Quantity, ?Junk, ?Await, ?Cost" & _
                                        " from catalogs.products" & _
                                        " left join farm.synonym  syn on syn.synonymcode=?SynonymCode" & _
                                        " left join farm.synonymfirmcr sfcr on sfcr.SynonymFirmCrCode=?SynonymFirmCrCode" & _
                                        " left join  farm.catalogfirmcr cfcr on cfcr.codefirmcr=?CodeFirmCr" & _
                                        " where products.ID=?ProductID; "

            If CalculateLeader And (MinPriceCodeP > 0 Or LeaderMinPriceCodeP > 0) And (LaederMinCostP > 0 Or MinCostP > 0) Then

                .CommandText &= " insert into orders.leaders " & _
                                "values(last_insert_id(), nullif(?MinCost, 0), nullif(?LeaderMinCost, 0), nullif(?CostCode, 0), nullif(?LeaderCostCode, 0))"
                .Parameters.Add(New MySql.Data.MySqlClient.MySqlParameter("?CostCode", MySqlDbType.UInt32, 0, "CostCode"))
                .Parameters.Add(New MySql.Data.MySqlClient.MySqlParameter("?LeaderCostCode", MySqlDbType.UInt32, 0, "LeaderCostCode"))
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
    Public Function GetPasswords(ByVal UniqueID As String) As String
        Dim ErrorFlag As Boolean = False
        Dim BasecostPassword As String

        If DBConnect("GetPasswords") Then

            'TODO: �������� ����������� � prgdataex
            Try
                GetClientCode()

                '��������� ���������� ����������� ��������������
                If CheckID Then
                    UID = UniqueID
                    If Not FnCheckID() Then
                        MessageH = "���������� ��������� �� ������ ���������� ���������."
                        MessageD = "����������, ���������� � �� ��������.[2]"
                        Addition = "�������������� UIN."
                        'MailErr("�������������� ����������� �������������� ��� ��������� �������", "")
                        UpdateType = 5
                        ErrorFlag = True
                        'ProtocolThread.Start()
                        Return "Error=" & MessageH & ";Desc=" & MessageD
                    End If
                End If

                Cm.CommandText = "select BaseCostPassword from retclientsset where clientcode=" & CCode
                SQLdr = Cm.ExecuteReader
                SQLdr.Read()
                BasecostPassword = SQLdr.GetString(0)
                SQLdr.Close()

                '�������� ����� ����������� ��� ���������� ������
                Cm.CommandText = "SELECT ifnull(sum(SaveGridID), 0) FROM ret_save_grids r where ClientCode = " & CCode
                Dim SaveGridMask As UInt64 = Convert.ToUInt64(Cm.ExecuteScalar())

                If (BasecostPassword <> Nothing) Then
                    Dim S As String = "Basecost=" & ToHex(BasecostPassword) & ";SaveGridMask=" & SaveGridMask.ToString("X7") & ";"
                    Return S
                Else
                    MailErr("������ ��� ��������� �������", "� ������� �� ������ ������ ��� �������� ������")
                    Addition = "�� ������ ������ ��� �������� ������"
                    ErrorFlag = True
                    UpdateType = 5
                    'ProtocolThread.Start()
                End If
            Catch Exp As Exception
                MailErr("������ ��� ��������� �������", Exp.Message & ": " & Exp.StackTrace)
                Addition = Exp.Message
                ErrorFlag = True
                UpdateType = 5
                'ProtocolThread.Start()
            Finally
                DBDisconnect()
            End Try
        End If

        If ErrorFlag Then
            Return "Error=��� ���������� ������ ������� ��������� ������.;Desc=���������� ��������� ������� ����� ��������� �����."
        End If
    End Function

    <WebMethod()> Public Function PostPriceDataSettings(ByVal UniqueID As String, ByVal PriceCodes As Int32(), ByVal RegionCodes As Int32(), ByVal INJobs As Boolean()) As String
        Dim ErrorFlag As Boolean = False
        Dim tran As MySqlTransaction = Nothing

        If DBConnect("PostPriceDataSettings") Then
            Try
                GetClientCode()

                '��������� ���������� ����������� ��������������
                If CheckID Then
                    UID = UniqueID
                    If Not FnCheckID() Then
                        MessageH = "���������� ��������� �� ������ ���������� ���������."
                        MessageD = "����������, ���������� � �� ��������.[2]"
                        Addition = "�������������� UIN."
                        'MailErr("�������������� ����������� �������������� ��� ���������� �������� �����-������", "")
                        UpdateType = 5
                        ErrorFlag = True
                        'ProtocolThread.Start()
                        Return "Error=" & MessageH & ";Desc=" & MessageD
                    End If
                End If

                '��������� ����� ��������
                If ((PriceCodes.Length > 0) And (PriceCodes.Length = INJobs.Length) And (RegionCodes.Length = PriceCodes.Length)) Then
                    Dim dtIntersection As DataTable = New DataTable
                    '������� �� ������� ������ �� Intersection
                    Dim cmdSel As MySqlCommand = New MySqlCommand("SELECT i.Id, i.RegionCode, i.PriceCode, i.DisabledByClient FROM usersettings.intersection i where ClientCode = ?ClientCode", ReadOnlyCn)
                    cmdSel.Parameters.AddWithValue("?ClientCode", CCode)
                    Dim daSel As MySqlDataAdapter = New MySqlDataAdapter(cmdSel)
                    Dim cmdUp As MySqlCommand = New MySqlCommand

                    '��������� ������� �� ����������
                    daSel.UpdateCommand = cmdUp
                    cmdUp.Connection = ReadOnlyCn
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

                    '��������� ������� �����������
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
                                tran = ReadOnlyCn.BeginTransaction()
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
                                    'MailErr("Deadlock ��� ���������� ���������� �������� �����-������", MySQLError.Message & " - ���: " & MySQLError.Number)
                                    ErrCount += 1
                                    System.Threading.Thread.Sleep(300)
                                Else
                                    Quit = True
                                    ErrorFlag = True
                                    MailErr("������ ��� ���������� ���������� �������� �����-������", MySQLError.ToString())
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
                                MailErr("������ ��� ���������� ���������� �������� �����-������", ex.ToString())
                            End Try
                        Loop Until Quit
                    End If

                Else
                    MailErr("������ ��� ���������� �������� �����-������", "�� ��������� ����� ���������� ��������")
                    ErrorFlag = True
                End If

            Catch Exp As Exception
                MailErr("������ ��� ���������� �������� �����-������", Exp.Message)
                ErrorFlag = True
            Finally
                DBDisconnect()
            End Try
        End If

        If ErrorFlag Then
            Return "Error=��� ���������� ������ ������� ��������� ������.;Desc=���������� ��������� ������� ����� ��������� �����."
        End If
    End Function


    <WebMethod()> Public Function GetReclame() As String
        Dim RegionName As String
        Dim ReclameDate, MaxReclameFileDate As Date
        Dim NewZip As Boolean = True

        'Dim FileSize As Integer
        If DBConnect("GetReclame") Then
            Try
                GetClientCode()


                ������ = "SELECT Region, ReclameTime " & _
                            "FROM clientsdata cd, farm.regions r, ret_update_info rcs " & _
                            "where r.regioncode = cd.regioncode " & _
                               "and rcs.clientcode = cd.firmcode " & _
                            "and cd.firmcode=?ClientCode"
                Cm.Connection = ReadOnlyCn
                Cm.Transaction = Nothing
                Cm.CommandText = ������
                SQLdr = Cm.ExecuteReader
                SQLdr.Read()
                RegionName = SQLdr.GetString(0)
                ReclameDate = SQLdr.GetDateTime(1)
                SQLdr.Close()
                Reclame = True
                ReclamePath = ResultFileName & "Reclame\" & RegionName & "\"

                MySQLFileDelete(ResultFileName & "r" & CCode & ".zip")

                Dim FileList As String()
                Dim FileName As String

                If Not Directory.Exists(ReclamePath) Then Directory.CreateDirectory(ReclamePath)

                FileList = Directory.GetFiles(ReclamePath)
                FileCount = 0

                For Each FileName In FileList

                    FileInfo = New FileInfo(FileName)

                    If FileInfo.LastWriteTime.Subtract(ReclameDate).TotalSeconds > 1 Then
                        Cm.CommandText = "insert into ready_client_files values(null, ?ClientCode, '" & FileInfo.Name & " ', 1);"
                        Cm.ExecuteNonQuery()
                        FileCount += 1
                        If FileInfo.LastWriteTime > MaxReclameFileDate Then MaxReclameFileDate = FileInfo.LastWriteTime
                    End If

                Next

                If FileCount > 0 Then

                    ZipStream()

                    FileInfo = New FileInfo(ResultFileName & "r" & CCode & ".zip")
                    FileInfo.LastWriteTime = MaxReclameFileDate

                End If

                'Err.Raise("EMK")


            Catch Err As Exception
                ErrorFlag = True
                Cm.CommandText = "delete from ready_client_files  where clientcode=?ClientCode and Reclame;"
                Cm.ExecuteNonQuery()
            Finally
                DBDisconnect()
            End Try
        Else

        End If

        If ErrorFlag Then
            GetReclame = ""
        Else
            If FileCount > 0 Then
                'GetReclame = "URL=" & Context.Request.Url.Scheme & Uri.SchemeDelimiter & Context.Request.Url.Authority & Context.Request.ApplicationPath & "/GetFileReclameHandler.ashx?Id=" & Now.ToFileTimeUtc & ";New=" & True
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
            DBDisconnect()
            Reclame = True
            MySQLFileDelete(ResultFileName & "r" & CCode & "Old.zip")
            File.Move(ResultFileName & "r" & CCode & ".zip", ResultFileName & "r" & CCode & "Old.zip")
            ReclameComplete = True
        Catch ex As Exception
            MailErr("������������� �������", ex.Message)
            ReclameComplete = False
        End Try
    End Function



    Private Sub SetCodesProc()
        Dim CommitReclame As Boolean
        Dim ReclameDate As DateTime

        ' "update ret_update_info set ReclameTime=?ReclameTime where  clientcode=?ClientCode"

        Try

            FileInfo = New FileInfo(ResultFileName & "r" & CCode & "Old.zip")
            If FileInfo.Exists Then
                CommitReclame = True
                ReclameDate = FileInfo.LastWriteTime
            End If
            SelProc.Parameters.Add(New MySqlParameter("?CommitReclameParam", CommitReclame))
            SelProc.Parameters.Add(New MySqlParameter("?ReclameDateParam", ReclameDate))
            SelProc.Parameters.Add(New MySqlParameter("?ClientCodeParam", CCode))
            SelProc.Parameters.Add(New MySqlParameter("?AbsentPriceCodesParam", AbsentPriceCodes))

            SelProc.CommandText = "CommitClientUpdate2"
            SelProc.Transaction = myTrans

RestartMaxCodesSet:
            myTrans = ReadOnlyCn.BeginTransaction(IsoLevel)

            SelProc.ExecuteNonQuery()


            myTrans.Commit()

            If Len(AbsentPriceCodes) > 2 Then Addition &= "!!!����������� �������������� �����������"

        Catch MySQLErr As MySqlException
            Try
                If Not (ReadOnlyCn.State = ConnectionState.Closed Or ReadOnlyCn.State = ConnectionState.Broken) Then myTrans.Rollback()
            Catch
            End Try
            If MySQLErr.Number = 1213 Or MySQLErr.Number = 1205 Then
                'MailErr("���������� �������� ������������ ���������", "Deadlock ")
                System.Threading.Thread.Sleep(1500)
                GoTo RestartMaxCodesSet
            End If
            MailErr("���������� �������� ������������ ���������", MySQLErr.Message)
            Addition = MySQLErr.Message
            UpdateType = 6
            ErrorFlag = True

        Catch err As Exception
            MailErr("���������� �������� ������������ ���������", err.Message)
            Addition = err.Message
            If Not (ReadOnlyCn.State = ConnectionState.Closed Or ReadOnlyCn.State = ConnectionState.Broken) Then myTrans.Rollback()
            UpdateType = 6
            ErrorFlag = True
        Finally

        End Try

    End Sub

    Private Function TestDocument(ByVal File As String) As Boolean
        Dim DocumentNo As UInt32

        Try
            If Path.GetFileName(File).IndexOf("_") > 0 Then
                If Not UInt32.TryParse(Left(Path.GetFileName(File), Path.GetFileName(File).IndexOf("_")), DocumentNo) Then _
                Err.Raise(1, "���������� ����������", "������� �������� ��������, ���������� �� �������������:" & Path.GetFileName(File))
            Else
                Err.Raise(1, "���������� ����������", "������� �������� ��������, ���������� �� �������������:" & Path.GetFileName(File))
            End If


            ArchCmd.CommandText = "select exists(SELECT null FROM logs.document_logs where RowId=" & DocumentNo & ")"
            If Not Convert.ToBoolean(ArchCmd.ExecuteScalar) Then
                '  Err.Raise(1, "�������� ������ ���������", "������� ��������� ��������, �� ���������� �� ����������: " & Path.GetFileName(File) & " (" & _
                'DocumentNo & ")")
                Addition &= "��������� ��������, ���������� �� �������������: " & Path.GetFileName(File)
            End If


            Return True

        Catch Ex As Exception
            Addition &= "���� ��������� " & File & " �� ������� ������� � ������ �� �������: " & Ex.Source & ": " & Ex.Message
            Dim message As New System.Net.Mail.MailMessage
            Dim SMTPClient As New System.Net.Mail.SmtpClient("mail.adc.analit.net")
            message.From = New System.Net.Mail.MailAddress("service@analit.net")
            message.Subject = "�������������� ���� ����������"
            message.BodyEncoding = System.Text.Encoding.UTF8
            message.IsBodyHtml = True
            message.Body = "���� ��������� " & File & " �� ������� ������� � ������ �� �������. ���� ��������� �� �������� ����� ���������. �������: " & Ex.Source & ": " & Ex.Message
            message.To.Add(New System.Net.Mail.MailAddress("service@analit.net"))
            message.Attachments.Add(New System.Net.Mail.Attachment(File))
            SMTPClient.Send(message)
            message.Dispose()


            FileInfo = New FileInfo(File)
            FileInfo.Delete()
            'Err.Raise(1, Ex.Source, Ex.Message)
            Return False
        End Try

    End Function

    Private Sub LogDocumentSend(ByVal File As String, ByVal FileSize As UInt32)

        Dim DocumentNo As UInt32

        DocumentNo = Convert.ToUInt32(Left(Path.GetFileName(File), Path.GetFileName(File).IndexOf("_")))

        Cm.CommandText = "select * from logs.document_logs where rowid=" & DocumentNo

        DA.Fill(DS.Tables("Documents"))

        DS.Tables("Documents").Select("RowId=" & DocumentNo)(0).Item("DocumentSize") = FileSize
        'Row.Item("DocumentID") = DocumentNo
        'Row.Item("DocumentFileSize") = FileSize
        ' DS.Tables("Documents").Rows.Add(Row)


    End Sub

End Class

