using System.Configuration;

namespace PrgData.Common
{
	public class Settings
	{
		public static string ConnectionString()
		{ 
#if DEBUG 	
			//return "Database=usersettings;Data Source=testsql.analit.net;User Id=system;Password=newpass;pooling=true";
			return ConfigurationManager.ConnectionStrings["Main"].ConnectionString;
#else		
			return ConfigurationManager.ConnectionStrings["Main"].ConnectionString;
#endif
		}


		

	}
}