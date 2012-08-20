using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Configuration;
using System.Threading;
using PrgData.Common;
using log4net;
using log4net.Config;

namespace PrgData.Common.Counters
{
	[DataContract]
	public class ClientStatus
	{
		[DataMember] public uint _UserId;
		[DataMember] public string _MethodName;
		[DataMember] public DateTime _StartTime;

		public int Id;

		public ClientStatus(uint UserId, string MethodName, DateTime StartTime)
		{
			_UserId = UserId;
			_MethodName = MethodName;
			_StartTime = StartTime;
		}

		public ClientStatus(int id, uint UserId, string MethodName, DateTime StartTime)
		{
			_UserId = UserId;
			_MethodName = MethodName;
			_StartTime = StartTime;
			Id = id;
		}

		public bool IsWaitToLong()
		{
			return DateTime.Now.Subtract(_StartTime).TotalMinutes > 30;
		}

		public override string ToString()
		{
			return String.Format(
				"ClientStatus  Id = {0}  UserId = {1}  MethodName = {2}  StartTime = {3}",
				Id,
				_UserId,
				_MethodName,
				_StartTime);
		}
	}

	public class Counter
	{
		private static readonly int MaxSessionCount = Convert.ToInt32(ConfigurationManager.AppSettings["MaxGetUserDataSession"]);
		private static readonly ILog Log = LogManager.GetLogger(typeof(Counter));
		private static readonly string[] requestMethods = new string[] { "GetUserData", "PostOrderBatch" };
		private static readonly string[] updateMethods = new string[] { "GetUserData", "MaxSynonymCode", "CommitExchange", "PostOrderBatch" };
		private static readonly string[] historyMethods = new string[] { "GetHistoryOrders", "HistoryFileHandler" };

		public static ClientStatus[] GetClients()
		{
			return FindAll().ToArray();
		}

		public Counter()
		{
			XmlConfigurator.Configure();
		}

		public static uint TryLock(uint UserId, string Method)
		{
			uint id = 0;

			if (IsRequestMethods(Method)) {
				if (TotalUpdatingClientCount() > MaxSessionCount) {
					throw new UpdateException("Обновление данных в настоящее время невозможно.",
						"Пожалуйста, повторите попытку через несколько минут.[6]",
						"Перегрузка; ",
						RequestType.Forbidden);
				}
			}

			if (!(Method == "ReclameFileHandler" || Method == "FileHandler")) {
				var ClientItems = FindLocks(UserId, Method);
				if (!CanLock(ClientItems.ToList())) {
					var messageHeader = "Обновление данных в настоящее время невозможно.";

					if (Method == "PostOrder")
						messageHeader = "Отправка заказов в настоящее время невозможна.";
					else if (IsHistoryMethods(Method))
						messageHeader = "Загрузка истории заказов в настоящее время невозможна.";

					throw new UpdateException(messageHeader,
						"Пожалуйста, повторите попытку через несколько минут.[6]",
						"Перегрузка; ",
						RequestType.Forbidden);
				}
			}

			id = Save(new ClientStatus(UserId, Method, DateTime.Now));

			return id;
		}

		public static void ReleaseLock(uint userId, string method, UpdateData updateData)
		{
			if (updateData != null)
				ReleaseLock(userId, method, updateData.LastLockId);
		}

		public static void ReleaseLock(uint UserId, string Method, uint lockId)
		{
			try {
				Remove(UserId, Method, lockId);
			}
			catch (Exception ex) {
				Log.Error("Ошибка снятия блокировки", ex);
			}
		}

		private static bool CanLock(List<ClientStatus> ClientItems)
		{
			var isClientInProcess = false;
			foreach (var Client in ClientItems) {
				if (Client.IsWaitToLong()) {
					Log.DebugFormat("В логе присутствует запись, подлежащая удалению: {0}", Client);
				}
				else
					isClientInProcess = true;
			}
			return !isClientInProcess;
		}


		private static void Remove(uint UserId, string Method, uint lockId)
		{
			Utils.Execute(
				"delete from Logs.PrgDataLogs where UserId = ?UserId and MethodName = ?Method and Id = ?Id",
				new { UserId, Method, Id = lockId });
		}

		public static int TotalUpdatingClientCount()
		{
			return Utils.RequestScalar<int>("select count(*) from Logs.PrgDataLogs where MethodName in (" + GetRequestMethods() + ")");
		}


		private static IList<ClientStatus> FindLocks(uint UserId, string Method)
		{
			if (IsUpdateMethods(Method))
				return FindUpdateLocks(UserId);
			if (IsHistoryMethods(Method))
				return FindHistoryLocks(UserId);
			return Utils.Request(
				"select * from Logs.PrgDataLogs where UserId = ?UserId and MethodName = ?Method",
				new { UserId = UserId, Method = Method });
		}

		private static IList<ClientStatus> FindMultiplyLocks(uint UserId, string methods)
		{
			return Utils.Request(
				"select * from Logs.PrgDataLogs where UserId = ?UserId and MethodName in (" + methods + ")",
				new { UserId = UserId });
		}

		private static IList<ClientStatus> FindUpdateLocks(uint UserId)
		{
			return FindMultiplyLocks(UserId, GetUpdateMethods());
		}

		private static IList<ClientStatus> FindHistoryLocks(uint UserId)
		{
			return FindMultiplyLocks(UserId, GetHistoryMethods());
		}

		private static IList<ClientStatus> FindAll()
		{
			return Utils.Request("select * from Logs.PrgDataLogs");
		}

		private static uint Save(ClientStatus Status)
		{
			return
				Utils.RequestScalar<uint>(
					"insert into Logs.PrgDataLogs(UserId, MethodName, StartTime) Values(?UserId, ?MethodName, now()); select last_insert_id();",
					new { UserId = Status._UserId, MethodName = Status._MethodName, StartTime = Status._StartTime });
		}

		private static bool IsRequestMethods(string method)
		{
			return requestMethods.Any(item => item.Equals(method, StringComparison.OrdinalIgnoreCase));
		}

		private static bool IsUpdateMethods(string method)
		{
			return updateMethods.Any(item => item.Equals(method, StringComparison.OrdinalIgnoreCase));
		}

		private static bool IsHistoryMethods(string method)
		{
			return historyMethods.Any(item => item.Equals(method, StringComparison.OrdinalIgnoreCase));
		}

		private static string GetConcat(string[] methods)
		{
			return String.Join(", ", methods.Select(item => "'" + item + "'").ToArray());
		}

		private static string GetRequestMethods()
		{
			return GetConcat(requestMethods);
		}

		private static string GetUpdateMethods()
		{
			return GetConcat(updateMethods);
		}

		private static string GetHistoryMethods()
		{
			return GetConcat(historyMethods);
		}
	}
}