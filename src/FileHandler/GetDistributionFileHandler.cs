using System;
using System.IO;
using System.Threading;
using System.Web;
using PrgData.Common;

namespace PrgData.FileHandlers
{
	public class GetDistributionFileHandler : AbstractHttpHandler,  IHttpHandler
	{
#if DEBUG
		//Нужен для тестирования
		public Stream outputStream;
#endif
		public string RequestedVersion;
		public string RequestedFile;

		public string GetDistributionFileName(HttpContext context)
		{
			RequestedVersion = context.Request.QueryString["Version"];
			RequestedFile = context.Request.QueryString["File"];

			if (!String.IsNullOrWhiteSpace(RequestedVersion) && !String.IsNullOrWhiteSpace(RequestedFile))
				return Path.Combine(ServiceContext.DistributionPath(), RequestedVersion, "Exe", "AnalitF", RequestedFile);

			return null;
		}

		public void ProcessRequest(HttpContext context)
		{
			try {
				Log.DebugFormat("Начали обработку на запрос файла");
				SUserId = GetUserId(context);

				var distributionFileName = GetDistributionFileName(context);
				Log.DebugFormat("Попытка запроса файла {1} для пользователя: {0}", SUserId, distributionFileName);
				if (!String.IsNullOrEmpty(distributionFileName) && File.Exists(distributionFileName)) {
					context.Response.ContentType = "application/octet-stream";
					using (var stmFileStream = new FileStream(distributionFileName, FileMode.Open, FileAccess.Read, FileShare.Read)) {
						long rs;
						if (!string.IsNullOrEmpty(context.Request.QueryString["RangeStart"])
							&& (Int64.TryParse(context.Request.QueryString["RangeStart"], out rs))) {
							if (rs < stmFileStream.Length)
								stmFileStream.Position = rs;
						}
						context.Response.AppendHeader("INFileSize", stmFileStream.Length.ToString());
#if DEBUG
						CopyStreams(stmFileStream, outputStream, context);
#else
						CopyStreams(stmFileStream, context.Response.OutputStream, context);
#endif
						context.Response.Flush();
					}
				}
				else if (String.IsNullOrEmpty(distributionFileName)) {
					Log.ErrorFormat("Некорректны параметры для запроса файла дистрибутива : Version={0}, File={1}", RequestedVersion, RequestedFile);
					context.Response.StatusCode = 404;
				}
				else {
					Log.ErrorFormat("Не был найден файл дистрибутива : {0}", distributionFileName);
					context.Response.StatusCode = 404;
				}
			}
			catch (HttpException wex) {
				if (!wex.IsWellKnownException())
					Log.ErrorFormat("Запрос на получение файла дистрибутива\r\nErrCode : {0}\r\n{1}", wex.ErrorCode, wex);
			}
			catch (Exception ex) {
				if (!(ex is ThreadAbortException)) {
					context.AddError(ex);
					Log.Error("Запрос на получение файла дистрибутива", ex);
					context.Response.StatusCode = 500;
				}
			}
		}

		public bool IsReusable
		{
			get
			{
				return false;
			}
		}
	}
}