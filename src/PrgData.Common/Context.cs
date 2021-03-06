﻿using System;
using System.Configuration;
using System.IO;
using System.Text;
using System.Web;

namespace PrgData.Common
{
	public class ServiceContext
	{
		public static Func<string> GetUserName = () => HttpContext.Current.User.Identity.Name;
		public static Func<string> GetUserHost = () => HttpContext.Current.Request.UserHostAddress;
		public static Func<String> GetResultPath = () => {
			var path = ConfigurationManager.AppSettings["ResultsPath"];
			if (String.IsNullOrEmpty(path))
				return HttpContext.Current.Server.MapPath(@"/Results") + @"\";
			else
				return path;
		};
		public static Func<String> GetDocumentsPath = () => ConfigurationManager.AppSettings["DocumentsPath"];

		public static Func<String> MySqlSharedExportPath = () =>
#if DEBUG 
			ConfigurationManager.AppSettings["MySqlFilePath"];
#else
			Path.Combine("\\\\" + Environment.MachineName, ConfigurationManager.AppSettings["MySqlFilePath"]);
#endif
		public static Func<String> MySqlLocalImportPath = () => ConfigurationManager.AppSettings["MySqlLocalFilePath"];

		public static Func<String> DistributionPath = () => ConfigurationManager.AppSettings["DistributionPath"];

		public static string GetShortUserName()
		{
			var userName = GetUserName();
			if (userName.StartsWith(@"ANALIT\", StringComparison.OrdinalIgnoreCase))
				userName = userName.Substring(7);
			return userName;
		}

#if DEBUG
		public static string ExpandExportPath(string path)
		{
			var fullPath = Path.GetFullPath(path);
			if (fullPath.Equals(path, StringComparison.CurrentCultureIgnoreCase))
				return path;

			fullPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, path);
			if (!Directory.Exists(fullPath))
				Directory.CreateDirectory(fullPath);
			return fullPath;
		}

		public static void SetupMySqlPath()
		{
			var sharedExportPath = ExpandExportPath(MySqlSharedExportPath());
			var localImportPath = ExpandExportPath(MySqlLocalImportPath());
			var distributionPath = ExpandExportPath(DistributionPath());
			MySqlSharedExportPath = () => sharedExportPath;
			MySqlLocalImportPath = () => localImportPath;
			DistributionPath = () => distributionPath;
		}

		/// <summary>
		/// Возвращаем имя пользователя, переданное из AnalitF, при запуске под дебагом VisualStudio
		/// </summary>
		/// <returns></returns>
		public static string GetAnalitFUserName()
		{
			var userName = HttpContext.Current.User.Identity.Name;

			var request = HttpContext.Current.Request.Headers["Authorization"];
			if (!String.IsNullOrWhiteSpace(request) && request.StartsWith("Basic ") && request.Length > 6) {
				var userBasic = request.Substring(6);
				var decodedAuthentification = Encoding.ASCII.GetString(Convert.FromBase64String(userBasic));
				var colonIndex = decodedAuthentification.IndexOf(':');
				if (colonIndex > 0)
					userName = decodedAuthentification.Substring(0, colonIndex);
			}

			return userName;
		}

		public static void SetupBasicAuthentication()
		{
			GetUserName = GetAnalitFUserName;
		}

		public static void SetupDebugContext()
		{
			SetupMySqlPath();
			SetupBasicAuthentication();
		}
#endif

		public static string GetFileByLocal(string fileName)
		{
			return Path.Combine(MySqlLocalImportPath(), fileName);
		}

		public static string GetFileByShared(string fileName)
		{
			return Path.Combine(MySqlSharedExportPath(), fileName);
		}
	}
}