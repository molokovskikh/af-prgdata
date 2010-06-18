using System;
using System.IO;
using System.Web;

namespace PrgData.Common
{
	public class ServiceContext
	{
		public static Func<string> GetUserName = () => HttpContext.Current.User.Identity.Name;
		public static Func<string> GetUserHost = () => HttpContext.Current.Request.UserHostAddress;
		public static Func<String> GetResultPath = () => HttpContext.Current.Server.MapPath(@"/Results") + @"\";


		public static void SetupDebugContext()
		{
			if (false)
			{
				GetUserName = () => Environment.UserName;
				GetResultPath = () => Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Results") + @"\";
			}
		}
	}
}