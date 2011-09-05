using System;
using System.Configuration;
using MySql.Data.MySqlClient;

namespace PrgData.Common
{
	public class Settings
	{
		private static string _connectionStringName;

#if (DEBUG)
		private static bool IsIntegration()
		{
			return String.Equals(System.Environment.MachineName, "devsrv", StringComparison.OrdinalIgnoreCase)
				&& ConfigurationManager.ConnectionStrings["integration"] != null;
		}
#endif

		public static string ConnectionName
		{
			get
			{
				if (_connectionStringName == null)
					_connectionStringName = InitConnectionStringName();
				return _connectionStringName;
			}
			set
			{
				_connectionStringName = value;
			}
		}

		private static string InitConnectionStringName()
		{
#if (DEBUG)
			if (IsIntegration())
				return "integration";
			else
				return "Local";
#else
			return "Main";
#endif
		}

		public static MySqlConnection GetConnection()
		{
			return new MySqlConnection(ConnectionString());
		}

		public static string ConnectionString()
		{
			return ConfigurationManager.ConnectionStrings[ConnectionName].ConnectionString;
		}
	}
}