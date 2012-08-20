using System;
using System.IO;
using System.Web;
using log4net;
using MySql.Data.MySqlClient;
using PrgData.Common;

namespace PrgData.FileHandlers
{
	public class AbstractHttpHandler
	{
		protected readonly ILog Log = LogManager.GetLogger(typeof(GetFileHandler));

		public AbstractHttpHandler()
		{
			Log = LogManager.GetLogger(GetType());
		}

		protected const int BufferSize = 4096;

		protected long ByteSent;

		protected uint LastLockId;

		protected void CopyStreams(Stream input, Stream output)
		{
			var bytes = new byte[BufferSize];
			int numBytes;
			while ((numBytes = input.Read(bytes, 0, BufferSize)) > 0
				&& HttpContext.Current.Response.IsClientConnected) {
				output.Write(bytes, 0, numBytes);
				ByteSent += numBytes;
			}
		}

		protected string SUserId;

		protected string GetUserId(HttpContext context)
		{
			var userName = ServiceContext.GetShortUserName();
			ThreadContext.Properties["user"] = ServiceContext.GetUserName();
			try {
				using (var connection = Settings.GetConnection()) {
					connection.Open();
					var command = new MySqlCommand(@"
SELECT u.Id
FROM Customers.Clients c
	JOIN Customers.Users u on u.ClientId = c.Id
		JOIN AssignedPermissions ap on ap.UserId = u.Id
			JOIN UserPermissions up on up.Id = ap.PermissionId
where c.Status = 1
	  and up.Shortcut = 'AF'
	  and u.Login = ?Username", connection);
					command.Parameters.AddWithValue("?Username", userName);
					using (var sqlr = command.ExecuteReader()) {
						if (sqlr.Read())
							return sqlr["Id"].ToString();
					}
				}
			}
			catch (Exception e) {
				Log.Error("Ошибка при авторизации клиента", e);
			}
			return null;
		}
	}
}