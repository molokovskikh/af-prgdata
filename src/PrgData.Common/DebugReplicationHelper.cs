using System;
using System.Collections;
using System.Collections.Generic;
using Common.Tools;
using System.Linq;
using System.Data;
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

		public DebugReplicationHelper(UpdateData updateData, MySqlConnection connection, MySqlCommand command)
		{
			_updateData = updateData;
			_dataSet = new DataSet();
			_connection = connection;
			_command = command;
		}

		public void PrepareTmpReplicationInfo()
		{
			_command.CommandText = "drop temporary table IF EXISTS TmpReplicationInfo;";
			_command.ExecuteNonQuery();
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
         tmpprd             ,
         Prices             ,
         AnalitFReplicationInfo ARI
WHERE    tmpprd.firmcode = firm.firmcode
AND      firm.firmcode   = Prices.FirmCode
AND      ARI.FirmCode    = Prices.FirmCode
AND      ARI.UserId      = ?UserId
GROUP BY Prices.FirmCode,
         Prices.pricecode;
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

		public bool NeedDebugInfo()
		{
			_command.CommandText = @"
select
  count(*)
from
  TmpReplicationInfo ri
  inner join ActivePrices p on p.PriceCode = ri.PriceCode
where
    (ri.ForceReplication = 0)
and (p.Fresh = 1)";
			var countFresh = Convert.ToInt32(_command.ExecuteScalar());

			if (countFresh > 0)
				FillTable(
					"DistinctReplicationInfo",
					@"
select
ri.ForceReplication as TmpForceReplication,
ri.Actual as TmpActual,
p.*
from
  TmpReplicationInfo ri
  inner join ActivePrices p on p.PriceCode = ri.PriceCode
where
    (ri.ForceReplication = 0)
and (p.Fresh = 1)
");
			return countFresh > 0;
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

		public void SendMail()
		{
			var body = String
				.Format(
					"Проблема возникла у пользователя: {0}\r\nпри подготовке обновления от : {1}", 
					_updateData.UserName,
					_updateData.OldUpdateTime);
			var attachment = DumpTables();

			MailHelper.Mail(body, "При подготовке данных возникла разница во Fresh между PricesData и Core", attachment);
		}
	}
}