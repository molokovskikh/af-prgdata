
Imports System.Runtime.Serialization
Imports System.Configuration
Imports log4net
Imports log4net.Config

<DataContract()> _
Public Class ClientStatus

    <DataMember()> _
    Public _UserId As UInt32
	<DataMember()> _
	Public _MethodName As String
	<DataMember()> _
	Public _StartTime As DateTime

	Public Id As Integer

    Public Sub New(ByVal UserId As UInt32, ByVal MethodName As String, ByVal StartTime As DateTime)
        _UserId = UserId
        _MethodName = MethodName
        _StartTime = StartTime
    End Sub

    Public Sub New(ByVal id As Integer, ByVal UserId As UInt32, ByVal MethodName As String, ByVal StartTime As DateTime)
        _UserId = UserId
        _MethodName = MethodName
        _StartTime = StartTime
        Me.Id = id
    End Sub

	Public Function IsWaitToLong() As Boolean
		Return Now.Subtract(_StartTime).TotalMinutes > 30
	End Function

End Class

Public Class Counter

	Private Shared ReadOnly MaxSessionCount As Integer = Convert.ToInt32(ConfigurationManager.AppSettings("MaxGetUserDataSession"))
	Private Shared ReadOnly Log As ILog = LogManager.GetLogger(GetType(Counter))

	Public Shared Function GetClients() As ClientStatus()
		Return FindAll().ToArray()
	End Function

    Public Shared Function TryLock(ByVal UserId As UInt32, ByVal Method As String) As Boolean

        If Method = "GetUserData" Then
            If TotalUpdatingClientCount() > MaxSessionCount Then
                Utils.Mail("Клиент №" & UserId & " получил отказ в обновлении.", "Отказ в обновлении")
                Return False
            End If
        End If

        If Not (Method = "ReclameFileHandler" Or Method = "FileHandler") Then
            Dim ClientItems = FindLocks(UserId, Method)
            If Not CanLock(ClientItems) Then
                Return False
            End If
        End If

        Save(New ClientStatus(UserId, Method, Now()))
        Return True
    End Function

	Public Shared Sub Clear()
		Utils.Execute("delete from Logs.PrgDataLogs")
	End Sub

    Public Shared Sub ReleaseLock(ByVal UserId As UInt32, ByVal Method As String)
        Try
            Remove(UserId, Method)
        Catch ex As Exception
            Log.Error("Ошибка снятия блокировки", ex)
        End Try
    End Sub

	Private Shared Function CanLock(ByRef ClientItems As List(Of ClientStatus)) As Boolean
		Dim IsClientInProcess = False
        Dim UserId As Integer
		For Each Client In ClientItems
			If Client.IsWaitToLong() Then
				Remove(Client)
			Else
                UserId = Client._UserId
				IsClientInProcess = True
			End If
		Next
		If IsClientInProcess Then
            Utils.Mail("Клиент №" & UserId & ".", "запрет обновления")
		End If
		Return Not IsClientInProcess
	End Function

    Private Shared Sub Remove(ByVal UserId As UInteger, ByVal Method As String)
        Utils.Execute("delete from Logs.PrgDataLogs where UserId = ?UserId and MethodName = ?Method", _
   New With {.UserId = UserId, .Method = Method})
    End Sub

	Private Shared Sub Remove(ByRef Status As ClientStatus)
		Utils.Execute("delete from Logs.PrgDataLogs where Id = ?Id", _
		   New With {.Id = Status.Id})
	End Sub

	Public Shared Function TotalUpdatingClientCount() As Integer
		Return Utils.RequestScalar(Of Integer)("select count(*) from Logs.PrgDataLogs where MethodName = 'GetUserData'")
	End Function

    Private Shared Function FindLocks(ByVal UserId As UInteger, ByVal Method As String) As IList(Of ClientStatus)
        If Method = "GetUserData" Then
            Return FindUpdateLocks(UserId)
        End If
        Return Utils.Request("select * from Logs.PrgDataLogs where UserId = ?UserId and MethodName = ?Method", _
         New With {.UserId = UserId, .Method = Method})
    End Function

    Private Shared Function FindUpdateLocks(ByVal UserId As UInteger) As IList(Of ClientStatus)
        Return Utils.Request("select * from Logs.PrgDataLogs where UserId = ?UserId and (MethodName = 'MaxSynonymCode' or MethodName = 'GetUserData')", _
         New With {.UserId = UserId})
    End Function

	Private Shared Function FindAll() As IList(Of ClientStatus)
		Return Utils.Request("select * from Logs.PrgDataLogs")
	End Function

	Private Shared Sub Save(ByRef Status As ClientStatus)
        Utils.Execute("insert into Logs.PrgDataLogs(UserId, MethodName, StartTime) Values(?UserId, ?MethodName, now())", _
   New With {.UserId = Status._UserId, .MethodName = Status._MethodName, .StartTime = Status._StartTime})
	End Sub

	Public Sub New()
		XmlConfigurator.Configure()
	End Sub

End Class
