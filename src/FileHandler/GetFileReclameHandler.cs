using System;
using System.IO;
using System.Threading;
using System.Web;
using PrgData.Common;
using PrgData.Common.Counters;

namespace PrgData.FileHandlers
{
	public class GetFileReclameHandler : AbstractHttpHandler, IHttpHandler
	{
		public void ProcessRequest(HttpContext context)
		{
			try {
				SUserId = GetUserId(context);
				UInt32 UserId;
				if (!string.IsNullOrEmpty(SUserId) && (UInt32.TryParse(SUserId, out UserId))) {
					LastLockId = Counter.TryLock(UserId, "ReclameFileHandler");
					var fn = ServiceContext.GetResultPath() + "r" + UserId + ".zip";
					if (File.Exists(fn)) {
						context.Response.ContentType = "application/octet-stream";
						using (var stmFileStream = new FileStream(fn, FileMode.Open, FileAccess.Read, FileShare.Read)) {
							long rs;
							if (!string.IsNullOrEmpty(context.Request.QueryString["RangeStart"])
								&& (Int64.TryParse(context.Request.QueryString["RangeStart"], out rs))) {
								if (rs < stmFileStream.Length)
									stmFileStream.Position = rs;
							}
							context.Response.AppendHeader("INFileSize", stmFileStream.Length.ToString());
							CopyStreams(stmFileStream, context.Response.OutputStream);
							context.Response.Flush();
						}
					}
					else
						context.Response.StatusCode = 404;
				}
				else
					context.Response.StatusCode = 404;
			}
			catch (HttpException wex) {
				if (!wex.IsWellKnownException())
					Log.ErrorFormat("Запрос на получение файла с рекламой\r\nErrCode : {0}\r\n{1}", wex.ErrorCode, wex);
			}
			catch (Exception ex) {
				if (!(ex is ThreadAbortException)) {
					context.AddError(ex);
					Log.Error("Запрос на получение файла с рекламой", ex);
					context.Response.StatusCode = 500;
				}
			}
			finally {
				Counter.ReleaseLock(Convert.ToUInt32(SUserId), "ReclameFileHandler", LastLockId);
			}
		}

		public bool IsReusable
		{
			get { return false; }
		}
	}
}