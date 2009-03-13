using System.Configuration;

namespace PrgData.Common
{
	public class Settings
	{
		public static string ReadOnlyConnectionString()
		{ 
#if DEBUG 	
			//return "Database=usersettings;Data Source=testsql.analit.net;User Id=system;Password=newpass;pooling=true";
			return ConfigurationManager.ConnectionStrings["ReadOnly"].ConnectionString;
#else		
			return ConfigurationManager.ConnectionStrings["DB"].ReadOnlyConnectionString;
#endif
		}


		public static string ReadWriteConnectionString()
		{
#if DEBUG
			//return "Database=usersettings;Data Source=testsql.analit.net;User Id=system;Password=newpass;pooling=true";
			return ConfigurationManager.ConnectionStrings["ReadWrite"].ConnectionString;
#else		
			return ConfigurationManager.ConnectionStrings["DB"].ReadWriteConnectionString;
#endif
		}

	}
}