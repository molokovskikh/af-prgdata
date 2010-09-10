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

		[DataMember]
		public uint _UserId;
		[DataMember]
		public string _MethodName;
		[DataMember]
		public DateTime _StartTime;

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

		public static ClientStatus[] GetClients()
		{
			return FindAll().ToArray();
		}

		public Counter()
		{
			XmlConfigurator.Configure();
		}

		public static bool TryLock(uint UserId, string Method)
		{

			if (Method == "GetUserData")
			{
				if (TotalUpdatingClientCount() > MaxSessionCount)
				{
					throw new UpdateException("Обновление данных в настоящее время невозможно.",
					  "Пожалуйста, повторите попытку через несколько минут.[6]",
					  "Перегрузка; ",
					  RequestType.Forbidden);
				}
			}

			if (!(Method == "ReclameFileHandler" || Method == "FileHandler"))
			{
				var ClientItems = FindLocks(UserId, Method);
				if (!CanLock(ClientItems.ToList()))
				{
					var messageHeader = "Обновление данных в настоящее время невозможно.";

					if (Method == "PostOrder")
					{
						messageHeader = "Отправка заказов в настоящее время невозможна.";
					}

					throw new UpdateException(messageHeader,
					 "Пожалуйста, повторите попытку через несколько минут.[6]",
					 "Перегрузка; ",
					 RequestType.Forbidden);
				}
			}

			Save(new ClientStatus(UserId, Method, DateTime.Now));

#if (!DEBUG)
			var locks = FindAllFromLocks();
			if (locks.Count > 0)
				MailHelper.Mail(
					"Список локов:" 
						+ Environment.NewLine
						+ String.Join(Environment.NewLine, locks.ToList().ConvertAll(item => item.ToString()).ToArray()), 
					"Сервис: создан lock в базе данных sql2.analit.net");
#endif
			return true;
		}

		public static void Clear()
		{
			Utils.Execute("delete from Logs.PrgDataLogs");
		}

		public static void ReleaseLock(uint UserId, string Method)
		{
			try
			{
				Remove(UserId, Method);
			}
			catch (Exception ex)
			{
				Log.Error("Ошибка снятия блокировки", ex);
			}
		}

		private static bool CanLock(List<ClientStatus> ClientItems)
		{
			var IsClientInProcess = false;
			uint UserId;
			foreach (var Client in ClientItems)
			{
				if (Client.IsWaitToLong())
					Remove(Client);
				else
				{
					UserId = Client._UserId;
					IsClientInProcess = true;
				}
			}
			return !IsClientInProcess;
		}


		private static void Remove(uint UserId, string Method)
		{
			Utils.Execute(
			"delete from Logs.PrgDataLogs where UserId = ?UserId and MethodName = ?Method",
			new { UserId = UserId, Method = Method });
		}

		private static void Remove(ClientStatus Status)
		{
			Utils.Execute("delete from Logs.PrgDataLogs where Id = ?Id",
			   new { Id = Status.Id });
		}

		public static int TotalUpdatingClientCount()
		{
			return Utils.RequestScalar<int>("select count(*) from Logs.PrgDataLogs where MethodName = 'GetUserData'");
		}


		private static IList<ClientStatus> FindLocks(uint UserId, string Method)
		{
			if (Method == "GetUserData")
				return FindUpdateLocks(UserId);
			return Utils.Request(
			"select * from Logs.PrgDataLogs where UserId = ?UserId and MethodName = ?Method",
				new { UserId = UserId, Method = Method });
		}

		private static IList<ClientStatus> FindUpdateLocks(uint UserId)
		{
			return Utils.Request(
		"select * from Logs.PrgDataLogs where UserId = ?UserId and (MethodName = 'MaxSynonymCode' or MethodName = 'GetUserData')",
			 new { UserId = UserId });
		}

		private static IList<ClientStatus> FindAll()
		{
			return Utils.Request("select * from Logs.PrgDataLogs");
		}

		private static IList<ClientStatus> FindAllFromLocks()
		{
			return Utils.RequestFromLocks("select * from Logs.PrgDataLogs", null);
		}

		private static void Save(ClientStatus Status)
		{
			Utils.Execute(
			"insert into Logs.PrgDataLogs(UserId, MethodName, StartTime) Values(?UserId, ?MethodName, now())",
		new { UserId = Status._UserId, MethodName = Status._MethodName, StartTime = Status._StartTime });
		}


	}
}