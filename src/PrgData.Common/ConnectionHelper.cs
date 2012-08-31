using System;
using System.Data;
using log4net;
using MySql.Data.MySqlClient;

namespace PrgData.Common
{
	public class ConnectionHelper
	{
		private static ILog log = LogManager.GetLogger(typeof(ConnectionHelper));

		public static void SafeRollback(MySqlTransaction transaction)
		{
			if (transaction == null)
				return;

			if (transaction.Connection.State == ConnectionState.Closed
				|| transaction.Connection.State == ConnectionState.Broken)
				return;

			try {
				transaction.Rollback();
			}
			catch (Exception e) {
				log.Error("Ошибка при откате транзакции", e);
			}
		}
	}
}