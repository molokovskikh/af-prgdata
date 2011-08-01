using System;
using System.Configuration;
using MySql.Data.MySqlClient;

namespace PrgData.Common
{
	public class Settings
	{

#if (DEBUG)
		private static bool IsIntegration()
		{
			return String.Equals(System.Environment.MachineName, "devsrv", StringComparison.OrdinalIgnoreCase)
				&& ConfigurationManager.ConnectionStrings["integration"] != null;
		}
#endif

		public static string GetConnectionName()
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
			return ConfigurationManager.ConnectionStrings[GetConnectionName()].ConnectionString;
		}
	}
}