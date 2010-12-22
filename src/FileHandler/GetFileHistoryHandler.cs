using System;
using System.IO;
using System.Threading;
using System.Web;
using PrgData.Common;
using PrgData.Common.Counters;

namespace PrgData.FileHandlers
{
	public class GetFileHistoryHandler : IHttpHandler
	{
		private string SUserId;

		static void CopyStreams(Stream input, Stream output)
		{
			const int size = 4096;
			var bytes = new byte[4096];
			int numBytes;
			while ((numBytes = input.Read(bytes, 0, size)) > 0)
				output.Write(bytes, 0, numBytes);
		}

		void MailErr(string ErrSource, string ErrDesc)
		{
			var sBody = "Код пользователя : " + SUserId + Environment.NewLine + "Процесс : " + ErrSource + Environment.NewLine +
						"Описание : " + ErrDesc;
			MailHelper.Mail(sBody, "Сервис: ошибка в GetHistoryHadler");
		}

		public void ProcessRequest(HttpContext context)
		{
			try
			{
				SUserId = GetFileHandler.GetUserId(context);
				UInt32 UserId;
				if (!string.IsNullOrEmpty(SUserId) && (UInt32.TryParse(SUserId, out UserId)))
				{
					Counter.TryLock(UserId, "HistoryFileHandler");
					var fn = ServiceContext.GetResultPath() + "Orders" + UserId + ".zip";
					if (File.Exists(fn))
					{
						context.Response.ContentType = "application/octet-stream";
						using (var stmFileStream = new FileStream(fn, FileMode.Open, FileAccess.Read, FileShare.Read))
						{
							long rs;
							if (!string.IsNullOrEmpty(context.Request.QueryString["RangeStart"])
								&& (Int64.TryParse(context.Request.QueryString["RangeStart"], out rs)))
							{
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

			catch (HttpException wex)
			{
				if (!wex.IsWellKnownException())
					MailErr("Запрос на получение файла с историей заказов, пользователь: " + SUserId, "ErrCode: " + wex.ErrorCode + " Message: " + wex.Message);
			}
			catch (Exception ex)
			{
				if (!(ex is ThreadAbortException))
				{
					context.AddError(ex);
					MailErr("Запрос на получение файла с историей заказов, пользователь: " + SUserId, ex.ToString());
					context.Response.StatusCode = 500;
				}
			}
			finally
			{
				Counter.ReleaseLock(Convert.ToUInt32(SUserId), "HistoryFileHandler");
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