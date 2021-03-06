﻿using System;
using System.Configuration;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using System.Web;
using Common.MySql;
using Common.Tools.Helpers;
using log4net;
using MySql.Data.MySqlClient;
using PrgData.Common;
using PrgData.Common.Counters;

namespace PrgData.FileHandlers
{
	public class GetFileHandler : AbstractHttpHandler, IHttpHandler
	{
		public static TimeSpan Filetimeout = TimeSpan.Zero;

		private string _userHost;
		private long _totalBytes;
		private long _updateId;
		private long _fromByte;

		public static void ReadConfig()
		{
			TimeSpan.TryParse(ConfigurationManager.AppSettings["Filetimeout"], out Filetimeout);
		}

		private void LogSend(Exception ex)
		{
			try {
				using (var connection = new MySqlConnection(ConnectionHelper.GetConnectionString())) {
					connection.Open();
					var command = connection.CreateCommand();
					command.CommandText =
						@"
insert into logs.UpdateDownloadLogs (UpdateId, ClientHost, FromByte, SendBytes, TotalBytes, Addition) 
values (?UpdateId, ?IP, ?FromByte, ?SendBytes, ?TotalBytes, ?Addition);";
					command.Parameters.AddWithValue("?UpdateId", _updateId);
					command.Parameters.AddWithValue("?FromByte", _fromByte);
					command.Parameters.AddWithValue("?SendBytes", ByteSent);
					command.Parameters.AddWithValue("?IP", _userHost);
					command.Parameters.AddWithValue("?TotalBytes", _totalBytes);
					command.Parameters.AddWithValue("?Addition", ex);
					command.ExecuteNonQuery();
				}
			}
			catch (Exception exception) {
				Log.Error("Ошибка логирования загрузки файла обновлений", exception);
			}
		}

		private void LogSend()
		{
			LogSend(null);
		}

		public void ProcessRequest(HttpContext context)
		{
			try {
				SUserId = GetUserId(context);
				UInt32 UserId;

				_userHost = context.Request.UserHostAddress;

				if (String.IsNullOrEmpty(SUserId) || !UInt32.TryParse(SUserId, out UserId))
					throw new Exception("Не удалось идентифицировать клиента.");

				if (!String.IsNullOrEmpty(context.Request.QueryString["Id"]))
					Int64.TryParse(context.Request.QueryString["Id"], out _updateId);

				if (!String.IsNullOrEmpty(context.Request.QueryString["RangeStart"]))
					Int64.TryParse(context.Request.QueryString["RangeStart"], out _fromByte);

				var fn = ServiceContext.GetResultPath() + UserId + "_" + _updateId + ".zip";

				WaitHelper.Wait(Filetimeout, () => File.Exists(fn));

				if (!File.Exists(fn)) {
					throw new Exception(String.Format("При вызове GetFileHandler не найден файл с подготовленными данными: {0}", fn));
				}

				LastLockId = Counter.TryLock(UserId, "FileHandler");
				Log.DebugFormat("Успешно наложена блокировка FileHandler для пользователя: {0}", UserId);

				context.Response.ContentType = "application/octet-stream";
				Log.DebugFormat("Начали передачу файла для пользователя: {0}", UserId);
				using (var stmFileStream = new FileStream(fn, FileMode.Open, FileAccess.Read, FileShare.Read)) {
					_totalBytes = stmFileStream.Length;
					if (_fromByte > 0 && _fromByte < stmFileStream.Length)
						stmFileStream.Position = _fromByte;
					context.Response.AppendHeader("INFileSize", stmFileStream.Length.ToString());
					CopyStreams(stmFileStream, context.Response.OutputStream, context);
				}

				Log.DebugFormat("Производим вызов Flush() для пользователя: {0}", UserId);
				context.Response.Flush();
				Log.DebugFormat("Производим протоколирование после передачи файла для пользователя: {0}", UserId);
				LogSend();
			}
			catch (ExternalException wex) {
				LogSend(wex);
				if (!wex.IsWellKnownException())
					Log.Error(String.Format("HttpException " + wex.ErrorCode + "  при запросе получения файла с данными, пользователь: {0}", SUserId), wex);
			}
			catch (Exception ex) {
				LogSend(ex);
				if (!(ex is ThreadAbortException)) {
					context.AddError(ex);
					Log.Error(String.Format("Exception при запросе получения файла с данными, пользователь: {0}", SUserId), ex);
					context.Response.StatusCode = 500;
				}
			}
			finally {
				Log.DebugFormat("Пытаемся снять блокировку FileHandler для пользователя: {0}", SUserId);
				Counter.ReleaseLock(Convert.ToUInt32(SUserId), "FileHandler", LastLockId);
				Log.DebugFormat("Успешно снята блокировка FileHandler для пользователя: {0}", SUserId);
			}
		}
	}
}