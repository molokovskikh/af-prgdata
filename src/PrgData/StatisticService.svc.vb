Imports System.ServiceModel
Imports PrgData.Common.Counters

<ServiceContract()> _
Public Interface IStatisticService

	<OperationContract()> _
	Function GetUpdateInfo() As ClientStatus()

	<OperationContract()> _
	Function GetUpdatingClientCount() As Integer

End Interface

Public Class StatisticService
	Implements IStatisticService

	Public Function GetUpdateInfo() As ClientStatus() Implements IStatisticService.GetUpdateInfo
        Return Counter.GetClients()
	End Function

	Public Function GetUpdatingClientCount() As Integer Implements IStatisticService.GetUpdatingClientCount
        Return Counter.TotalUpdatingClientCount()
	End Function

End Class
