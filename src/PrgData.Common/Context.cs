using System;
using System.Configuration;
using System.IO;
using System.Web;

namespace PrgData.Common
{
	public class ServiceContext
	{
		public static Func<string> GetUserName = () => HttpContext.Current.User.Identity.Name;
		public static Func<string> GetUserHost = () => HttpContext.Current.Request.UserHostAddress;
		public static Func<String> GetResultPath = () => HttpContext.Current.Server.MapPath(@"/Results") + @"\";
		public static Func<String> GetDocumentsPath = () => ConfigurationManager.AppSettings["DocumentsPath"];

		public static string GetShortUserName()
		{
			var userName = GetUserName();
			if (userName.StartsWith(@"ANALIT\", StringComparison.OrdinalIgnoreCase))
				userName = userName.Substring(7);
			return userName;
		}

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