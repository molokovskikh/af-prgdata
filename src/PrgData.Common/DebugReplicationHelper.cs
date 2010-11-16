using System;
using System.Collections;
using System.Collections.Generic;
using Common.Tools;
using System.Linq;
using System.Data;
using log4net;
using MySql.Data.MySqlClient;
using System.IO;

namespace PrgData.Common
{
	public class DebugReplicationHelper
	{
		private MySqlConnection _connection;
		private MySqlCommand _command;
		private DataSet _dataSet;
		private UpdateData _updateData;
		private List<string> _reasons;

		public ILog Logger = LogManager.GetLogger(typeof (DebugReplicationHelper));

		public DebugReplicationHelper(UpdateData updateData, MySqlConnection connection, MySqlCommand command)
		{
			ThreadContext.Properties["user"] = updateData.UserName;
			_reasons = new List<string>();
			_updateData = updateData;
			_dataSet = new DataSet();
			_connection = connection;
			_command = command;
		}

		public void FillTmpReplicationInfo()
		{
			if (!_updateData.EnableImpersonalPrice)
			{
				_command.CommandText =
					@"
create temporary table TmpReplicationInfo engine=MEMORY
as
select
         Prices.FirmCode ,
         Prices.pricecode,
         IF(?OffersClientCode IS NULL, ((ForceReplication != 0) OR (actual = 0) OR ?Cumulative), 1)          as Fresh,
         ARI.ForceReplication,
         Prices.Actual
from
         clientsdata AS firm,
         PriceCounts        ,
         Prices             ,
         AnalitFReplicationInfo ARI
WHERE    PriceCounts.firmcode = firm.firmcode
AND      firm.firmcode   = Prices.FirmCode
AND      ARI.FirmCode    = Prices.FirmCode
AND      ARI.UserId      = ?UserId
GROUP BY Prices.FirmCode,
         Prices.pricecode;
";
				_command.ExecuteNonQuery();
			}
		}

		public void CopyActivePrices()
		{
			if (!_updateData.EnableImpersonalPrice)
			{
				_command.CommandText =
					@"
drop temporary table if exists CopyActivePrices;
create temporary table CopyActivePrices engine=MEMORY
as
select * from ActivePrices;
";
				_command.ExecuteNonQuery();
			}
		}

		public void FillTable(string tableName, string sql)
		{
			if (!_updateData.EnableImpersonalPrice)
			{
				var dataAdapter = new MySqlDataAdapter(_command);
				_command.CommandText = sql;
				var table = new DataTable(tableName);
				dataAdapter.Fill(table);
				_dataSet.Tables.Add(table);
			}
		}

		public bool NeedDebugInfo(int exportCoreCount)
		{
			_command.CommandText = @"
select
  count(*)
from
  CurrentReplicationInfo ri
  inner join ActivePrices p on p.FirmCode = ri.FirmCode
where
    (ri.ForceReplication = 0)
and (p.Fresh = 1)";
			var countFresh = Convert.ToInt32(_command.ExecuteScalar());
			if (countFresh > 0)
			{
				_reasons.Add("разница во Fresh между CurrentReplicationInfo и ActivePrices");
				FillTable(
					"DistinctReplicationInfo",
					@"
select
ri.ForceReplication as TmpForceReplication,
p.*
from
  CurrentReplicationInfo ri
  inner join ActivePrices p on p.FirmCode = ri.FirmCode
where
    (ri.ForceReplication = 0)
and (p.Fresh = 1)
");
			}

			_command.CommandText = @"
select
  count(*)
from
  ActivePrices p
  left join CurrentReplicationInfo ri on p.FirmCode = ri.FirmCode
where
    (ri.FirmCode is null)";
			var countNotReplicationInfo = Convert.ToInt32(_command.ExecuteScalar());
			if (countNotReplicationInfo > 0)
			{
				_reasons.Add("в ActivePrices существуют записи, которые не существуют в CurrentReplicationInfo");
				FillTable(
					"NotReplicationInfo",
					@"
select
  p.*
from
  ActivePrices p
  left join CurrentReplicationInfo ri on p.FirmCode = ri.FirmCode
where
    (ri.FirmCode is null)
");
			}

			_command.CommandText = @"
select
  *
from
  ActivePrices,
  CopyActivePrices
where
    CopyActivePrices.PriceCode = ActivePrices.PriceCode
and CopyActivePrices.RegionCode = ActivePrices.RegionCode
and CopyActivePrices.Fresh != ActivePrices.Fresh
";
			var countDistictActivePrices = Convert.ToInt32(_command.ExecuteScalar());
			if (countDistictActivePrices > 0)
			{
				_reasons.Add("ActivePrices после получени€ предложений была изменена");
				FillTable(
					"DistictActivePrices",
					@"

select
  ActivePrices.*
from
  ActivePrices,
  CopyActivePrices
where
    CopyActivePrices.PriceCode = ActivePrices.PriceCode
and CopyActivePrices.RegionCode = ActivePrices.RegionCode
and CopyActivePrices.Fresh != ActivePrices.Fresh
");
				FillTable(
					"CopyActivePrices",
					@"
select
  *
from
  CopyActivePrices
");
			}

			_command.CommandText = @"
select
  count(*)
from
  Core c
  left join farm.Core0 c0 on c0.id = c.Id
where
  c0.Id is null
";
			var countNotExistsCore = Convert.ToInt32(_command.ExecuteScalar());
			if (countNotExistsCore > 0)
			{
				_reasons.Add("во временной таблице Core существуют позиции, которых нет в Core0");
				FillTable(
					"NotExistsCore",
					@"
select
  c.PriceCode, c.RegionCode, count(*)
from
  Core c
  left join farm.Core0 c0 on c0.id = c.Id
where
  c0.Id is null
group by c.PriceCode, c.RegionCode
");
			}

			Logger.DebugFormat("ќбнаружены следующие проблемы:\r\n{0}", _reasons.Implode("\r\n"));

			return _reasons.Count > 0;
		}

		
		private string DumpTables()
		{
			var result = String.Empty;
			using (var writer = new StringWriter())
			{
				foreach (DataTable table in _dataSet.Tables)
					DumpTable(writer, table);

				result = writer.ToString();
			}

			return result;
		}

		private void DumpTable(StringWriter writer, DataTable table)
		{
			writer.WriteLine(table.TableName + ":");
			writer.WriteLine();
			var columnNames = table.Columns.Cast<DataColumn>().Implode(item => item.ColumnName, "\t"); ;
			writer.WriteLine(columnNames);
			writer.WriteLine("-".PadRight(columnNames.Length-1, '-'));

			foreach (DataRow row in table.Rows)
				writer.WriteLine(row.ItemArray.Implode("\t"));

			writer.WriteLine();
			writer.WriteLine();
		}

		public string TableToString(string tableName)
		{
			using (var writer = new StringWriter())
			{
				if (_dataSet.Tables.Contains(tableName))
				{
					DumpTable(writer, _dataSet.Tables[tableName]);
					return writer.ToString();
				}
				else
					return null;
			}
		}

		public void SendMail()
		{
			var body = String
				.Format(
					"ѕроблема возникла у пользовател€: {0}\r\nпри подготовке обновлени€ от : {1}\r\nѕричины:\r\n{2}", 
					_updateData.UserName,
					_updateData.OldUpdateTime,
					_reasons.Implode("\r\n"));
			var attachment = DumpTables();

			MailHelper.Mail(body, "ѕри подготовке данных возникли различи€ во Fresh между PricesData и Core", attachment);
		}
	}
}