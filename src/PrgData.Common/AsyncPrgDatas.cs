using System.Collections.Generic;
using System.Web.Services;

namespace PrgData.Common
{
	public static class AsyncPrgDatas
	{
		private static List<WebService> _services = new List<WebService>();

		public static void AddToList(WebService service)
		{
			if (!_services.Contains(service))
				lock (_services)
				{
					_services.Add(service);
				}
		}

		public static void DeleteFromList(WebService service)
		{
			if (_services.Contains(service))
				lock (_services)
				{
					_services.Remove(service);
				}
		}

		public static bool Contains(WebService service)
		{
			return _services.Contains(service);
		}
	}
}