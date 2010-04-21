using System;
using System.Web;

namespace PrgData.Common
{
	public class ServiceContext
	{
		public static Func<string> GetUserName = () => HttpContext.Current.User.Identity.Name;
		public static Func<string> GetUserHost = () => HttpContext.Current.Request.UserHostAddress;
	}
}