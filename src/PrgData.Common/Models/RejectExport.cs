using System.Collections.Generic;
using MySql.Data.MySqlClient;

namespace PrgData.Common.Models
{
	public class RejectExport : BaseExport
	{
		public RejectExport(UpdateData updateData, MySqlConnection connection, Queue<FileForArchive> files)
			: base(updateData, connection, files)
		{
		}

		public override int RequiredVersion
		{
			get { return -1; }
		}

		public override RequestType[] AllowedArchiveRequests
		{
			get { return new RequestType[]{}; }
		}

		public override void Export()
		{
			string sql;
			if (updateData.CheckVersion(1833))
				sql = GetNewExportSql();
			else
				sql = GetOldExportSql();

			Process("Rejects", sql);
		}

		private string GetOldExportSql()
		{
			var sql = @"
SELECT 
	r.Id,
	r.Product,
	r.Producer,
	'',
	r.Series,
	r.LetterNo,
	r.LetterDate,
	'',
	r.CauseRejects
FROM Farm.Rejects r
WHERE 1 = 1
";

			if (!updateData.Cumulative)
				sql += "   AND r.UpdateTime > ?UpdateTime";

			return sql;
		}

		private string GetNewExportSql()
		{
			var sql =
				@"
select r.Id,
	r.Product,
	r.ProductId,
	r.Producer,
	r.ProducerId,
	r.Series,
	r.LetterNo,
	r.LetterDate,
	r.CauseRejects,
	0 as Hidden
from Farm.Rejects r
where r.CancelDate is null";

			if (!updateData.Cumulative && !updateData.NeedUpdateForMatchWaybillsToOrders())
				sql +=
					@" and r.UpdateTime > ?UpdateTime
union
select r.Id,
	r.Product,
	r.ProductId,
	r.Producer,
	r.ProducerId,
	r.Series,
	r.LetterNo,
	r.LetterDate,
	r.CauseRejects,
	1 as Hidden
from Farm.Rejects r
where r.CancelDate is not null
	and r.UpdateTime > ?UpdateTime
union
select
	l.RejectId,
	l.Product,
	l.ProductId,
	l.Producer,
	l.ProducerId,
	l.Series,
	l.LetterNo,
	l.LetterDate,
	l.CauseRejects,
	1 as Hidden
from
	Logs.RejectLogs l
where
	l.LogTime >= ?UpdateTime
and l.Operation = 2";
			return sql;
		}
	}
}