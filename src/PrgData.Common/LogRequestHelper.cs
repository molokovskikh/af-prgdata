using System;
using System.Web;
using log4net;
using System.IO;
using System.Net.Mail;
using System.Configuration;
using PrgData.Common;

namespace PrgData
{
	public class LogRequestHelper
	{
		protected static ILog _Logger = LogManager.GetLogger(typeof(LogRequestHelper));

		public static bool NeedLogged()
		{
			return Convert.ToBoolean(ConfigurationManager.AppSettings["MustLogHttpcontext"]);
		}

		public static void ForceMailWithRequest(string MessageText, Exception exception)
		{
			InternalMailWithRequest(MessageText, exception);
		}

		public static void MailWithRequest(ILog Logger, string MessageText, Exception exception)
		{
			//Если логгер не установлен, то будем использовать логгер LogRequestHelper
			var l = Logger ?? _Logger;
			//Если exception не установлен, то будем брать исключение от сервера
			var ex = exception ?? HttpContext.Current.Server.GetLastError();
			if (NeedLogged())
				InternalMailWithRequest(MessageText, ex);
			else
				l.Error(MessageText, ex);
		}

		private static void InternalMailWithRequest(string MessageText, Exception exception)
		{
			try {

				var tmpRequestFileName = Path.GetTempFileName();
				HttpContext.Current.Request.SaveAs(tmpRequestFileName, true);

				try {
					_Logger.Error(MessageText, exception);

					if (exception != null)
						MessageText = String.Format("{0} " + Environment.NewLine + "{1}", MessageText, exception);
					MessageText = String.Format(
						"Date: {0}" + Environment.NewLine +
						"User: {1}" + Environment.NewLine +
						"{2}",
						DateTime.Now,
						ServiceContext.GetUserName(),
						MessageText);

					var httpRequestContent = File.ReadAllText(tmpRequestFileName);

					MailHelper.Mail(MessageText, null, httpRequestContent, "HTTPReguest.txt");
				}
				finally {
					try {
						File.Delete(tmpRequestFileName);
					}
					catch (Exception ex) {
						_Logger.Error("Ошибка при удалении временного файла для хранения HTTP-запроса", ex);
					}
				}
			}
			catch (Exception err) {
				_Logger.Error("Ошибка в MailWithRequest", err);
			}
		}
	}

}