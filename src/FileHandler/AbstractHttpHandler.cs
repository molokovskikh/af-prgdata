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
			Log = LogManager.GetLogger(this.GetType());
		}

		protected const int BufferSize = 4096;

		protected long ByteSent;

		protected uint LastLockId;

		protected void CopyStreams(Stream input, Stream output)
		{
			var bytes = new byte[BufferSize];
			int numBytes;
			while ((numBytes = input.Read(bytes, 0, BufferSize)) > 0
				   && HttpContext.Current.Response.IsClientConnected)
			{
				output.Write(bytes, 0, numBytes);
				ByteSent += numBytes;
			}
		}

		protected string SUserId;

		protected string GetUserId(HttpContext context)
		{
			var UserName = ServiceContext.GetShortUserName();
			ThreadContext.Properties["user"] = ServiceContext.GetUserName();
			string userId = null;
			try
			{
				using (var connection = Settings.GetConnection())
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
				Log.Error("Ошибка при авторизации клиента", e);
			}
			return null;
		}

	}
}