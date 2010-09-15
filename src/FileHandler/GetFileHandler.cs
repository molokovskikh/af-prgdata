using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using System.Web;
using log4net;
using MySql.Data.MySqlClient;
using PrgData.Common;
using PrgData.Common.Counters;

namespace FileHandler
{
	public class GetFileHandler : IHttpHandler
	{
		private static readonly ILog _log = LogManager.GetLogger(typeof (GetFileHandler));

		private string SUserId;
		private long _byteSent;
		private long _totalBytes;
		private long _updateId;
		private long _fromByte;
		private string _userHost;

		private const int _bufferSize = 4096;

		private void CopyStreams(Stream input, Stream output)
		{
            
			var bytes = new byte[4096];
			int numBytes;
			while ((numBytes = input.Read(bytes, 0, _bufferSize)) > 0 
			       && HttpContext.Current.Response.IsClientConnected)
			{
				output.Write(bytes, 0, numBytes);
				_byteSent += numBytes;
			}
		}

		private void LogSend(Exception ex)
		{
			try
			{
				using (var connection = new MySqlConnection(Settings.ConnectionString()))
				{
					connection.Open();
					var command = connection.CreateCommand();
					command.CommandText =
						@"
insert into logs.UpdateDownloadLogs (UpdateId, ClientHost, FromByte, SendBytes, TotalBytes, Addition) 
values (?UpdateId, ?IP, ?FromByte, ?SendBytes, ?TotalBytes, ?Addition);";
					command.Parameters.AddWithValue("?UpdateId", _updateId);
					command.Parameters.AddWithValue("?FromByte", _fromByte);
					command.Parameters.AddWithValue("?SendBytes", _byteSent);
					command.Parameters.AddWithValue("?IP", _userHost);
					command.Parameters.AddWithValue("?TotalBytes", _totalBytes);
					command.Parameters.AddWithValue("?Addition", ex);
					command.ExecuteNonQuery();
				}
			}
			catch(Exception exception)
			{
				_log.Error("Ошибка логирования загрузки файла обновлений", exception);
			}
		}

		private void LogSend()
		{
			LogSend(null);
		}

		public static string GetUserId(HttpContext context)
		{
			var UserName = ServiceContext.GetUserName();
			if (UserName.StartsWith(@"ANALIT\", StringComparison.OrdinalIgnoreCase))
				UserName = UserName.Substring(7);
			string userId = null;
			try
			{
				using (var connection = new Common.MySql.SimpleConnectionManager().GetConnection())
				{
					var command = new MySqlCommand(@"SELECT ouar.RowId
FROM clientsdata cd
	JOIN osuseraccessright ouar on ouar.clientcode = cd.firmcode
		JOIN AssignedPermissions ap on ap.UserId = ouar.RowId
			JOIN UserPermissions up on up.Id = ap.PermissionId
where cd.firmstatus = 1
	  and up.Shortcut = 'AF'
	  and ouar.OSUserName = ?Username", connection);
					command.Parameters.AddWithValue("?Username", UserName);
					connection.Open();
					using (var sqlr = command.ExecuteReader())
					{
						if (sqlr.Read())
							userId = sqlr["RowId"].ToString();
					}

					if (!String.IsNullOrEmpty(userId))
						return userId;

					command.CommandText = @"
SELECT u.Id
FROM Future.Clients c
	JOIN Future.Users u on u.ClientId = c.Id
		JOIN AssignedPermissions ap on ap.UserId = u.Id
			JOIN UserPermissions up on up.Id = ap.PermissionId
where c.Status = 1
	  and up.Shortcut = 'AF'
	  and u.Login = ?Username";
					using (var sqlr = command.ExecuteReader())
					{
						if (sqlr.Read())
							return sqlr["Id"].ToString();
					}
				}
			}
			catch (Exception e)
			{
				_log.Error("Ошибка при авторизации клиента", e);
			}
			return null;
		}

		public void ProcessRequest(HttpContext context)
		{
			try
			{
				SUserId = GetUserId(context);
				UInt32 UserId;

				_userHost = context.Request.UserHostAddress;

				if (String.IsNullOrEmpty(SUserId) || !UInt32.TryParse(SUserId, out UserId))
					throw new Exception("Не удалось идентифицировать клиента.");

				var fn = ServiceContext.GetResultPath() + UserId + ".zip";
				if (!File.Exists(fn))
				{
					_log.DebugFormat("При вызове GetFileHandler не найден файл: {0}", fn);
					throw new Exception(String.Format("При вызове GetFileHandler не найден файл с подготовленными данными: {0}", fn));
				}

				if (!String.IsNullOrEmpty(context.Request.QueryString["Id"]))
					Int64.TryParse(context.Request.QueryString["Id"], out _updateId);

				if (!String.IsNullOrEmpty(context.Request.QueryString["RangeStart"]))
					Int64.TryParse(context.Request.QueryString["RangeStart"], out _fromByte);


				Counter.TryLock(UserId, "FileHandler");
				_log.DebugFormat("Успешно наложена блокировка FileHandler для пользователя: {0}", UserId);

				context.Response.ContentType = "application/octet-stream";
				using (var stmFileStream = new FileStream(fn, FileMode.Open, FileAccess.Read, FileShare.Read))
				{
					_totalBytes = stmFileStream.Length;
					if (_fromByte > 0 && _fromByte < stmFileStream.Length)
						stmFileStream.Position = _fromByte;
					context.Response.AppendHeader("INFileSize", stmFileStream.Length.ToString());
					CopyStreams(stmFileStream, context.Response.OutputStream);
					context.Response.Flush();
					LogSend();
				}
			}
			catch (COMException comex)
			{
				//-2147024832 - (0x80070040): Указанное сетевое имя более недоступно. (Исключение из HRESULT: 0x80070040)
				//-2147024775 - (0x80070079): Превышен таймаут семафора. (Исключение из HRESULT: 0x80070079)
				if (comex.ErrorCode != -2147023901
					&& comex.ErrorCode != -2147024775
					&& comex.ErrorCode != -2147024832)
				{
					LogSend(comex);
					_log.Error(String.Format("COMException при запросе получения файла с данными, пользователь: {0}", SUserId), comex);
				}
			}
			catch (HttpException wex)
			{

				// 0x800703E3 -2147023901 Удаленный хост разорвал соединение.

				LogSend(wex);
				if (	wex.ErrorCode != -2147014842
					//
					&& wex.ErrorCode != -2147023901
					&& wex.ErrorCode != -2147467259
					&& wex.ErrorCode != -2147024832
					&& wex.ErrorCode != -2147024775
	)
					//
					_log.Error(String.Format("HttpException " + wex.ErrorCode + "  при запросе получения файла с данными, пользователь: {0}", SUserId), wex);
			}

			catch (Exception ex)
			{
				LogSend(ex);

				if (!(ex is ThreadAbortException))
				{
					context.AddError(ex);
					_log.Error(String.Format("Exception при запросе получения файла с данными, пользователь: {0}", SUserId), ex);
					context.Response.StatusCode = 500;
				}
			}
			finally
			{
				_log.DebugFormat("Пытаемся снять блокировку FileHandler для пользователя: {0}", SUserId);
				Counter.ReleaseLock(Convert.ToUInt32(SUserId), "FileHandler");
				_log.DebugFormat("Успешно снята блокировка FileHandler для пользователя: {0}", SUserId);
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