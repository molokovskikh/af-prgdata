using System;
using MySql.Data.MySqlClient;

namespace PrgData.Common.Model
{
	public class AnalitFUpdate
	{
		public static void UpdateLog(MySqlConnection connection, uint updateId, string log)
		{
			MySqlHelper.ExecuteNonQuery(
				connection,
				"update logs.AnalitFUpdates set Log=?Log  where UpdateId=?UpdateId",
				new MySqlParameter("?Log", log),
				new MySqlParameter("?UpdateId", updateId));
		}

		public static uint InsertAnalitFUpdatesLog(MySqlConnection connection, UpdateData updateData, RequestType request)
		{
			return InsertAnalitFUpdatesLog(connection, updateData, request, null);
		}

		public static uint InsertAnalitFUpdatesLog(MySqlConnection connection, UpdateData updateData, RequestType request,
			string addition,
			bool commit = false,
			uint? resultSize = null,
			string log = null)
		{
			var uid = Convert.ToUInt32(MySqlHelper.ExecuteScalar(
				connection,
				@"
insert into logs.AnalitFUpdates (RequestTime, UpdateType, UserId, Commit, Addition, AppVersion, ClientHost, ResultSize, Log)
values (now(), ?UpdateType, ?UserId, ?Commit, ?Addition, ?AppVersion, ?ClientHost, ?ResultSize, ?Log);
select last_insert_id()",
				new MySqlParameter("?UpdateType", (int)request),
				new MySqlParameter("?UserId", updateData.UserId),
				new MySqlParameter("?Addition", addition),
				new MySqlParameter("?AppVersion", updateData.BuildNumber),
				new MySqlParameter("?ClientHost", updateData.ClientHost),
				new MySqlParameter("?ResultSize", resultSize),
				new MySqlParameter("?Commit", commit),
				new MySqlParameter("?Log", log)
			));
			return uid;
		}
	}
}