using System;
using System.Collections;
using System.Collections.Generic;
using Common.MySql;
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
		private DataSet _dataSet;
		private UpdateData _updateData;
		private List<string> _reasons;

		public ILog Logger = LogManager.GetLogger(typeof(DebugReplicationHelper));

		public int ExportCoreCount;

		public DebugReplicationHelper(UpdateData updateData, MySqlConnection connection)
		{
			ThreadContext.Properties["user"] = updateData.UserName;
			ExportCoreCount = 0;
			_reasons = new List<string>();
			_updateData = updateData;
			_dataSet = new DataSet();
			_connection = connection;
		}

		public void CopyActivePrices()
		{
			if (!_updateData.EnableImpersonalPrice) {
				_connection.Execute(@"
drop temporary table if exists CopyActivePrices;
create temporary table CopyActivePrices engine=MEMORY
as
select * from ActivePrices;");
			}
		}

		public void FillTable(string tableName, string sql)
		{
			if (!_updateData.EnableImpersonalPrice) {
				_dataSet.Tables.Add(_connection.Fill(sql));
			}
		}

		public bool NeedDebugInfo()
		{
			var cmd = new MySqlCommand(@"
select
  *
from
  ActivePrices,
  CopyActivePrices
where
	CopyActivePrices.PriceCode = ActivePrices.PriceCode
and CopyActivePrices.RegionCode = ActivePrices.RegionCode
and CopyActivePrices.Fresh != ActivePrices.Fresh", _connection);
			var countDistictActivePrices = Convert.ToInt32(cmd.ExecuteScalar());
			if (countDistictActivePrices > 0) {
				_reasons.Add("ActivePrices после получения предложений была изменена");
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

			if (ExportCoreCount > 0) {
				cmd = new MySqlCommand(@"
select
  count(*)
from
  Core c
  left join farm.Core0 c0 on c0.id = c.Id
where
  c0.Id is null", _connection);
				var countNotExistsCore = Convert.ToInt32(cmd.ExecuteScalar());
				if (countNotExistsCore > 0) {
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
			}

			if (_reasons.Count > 0)
				Logger.DebugFormat("Обнаружены следующие проблемы:\r\n{0}", _reasons.Implode("\r\n"));

			return _reasons.Count > 0;
		}


		private string DumpTables()
		{
			var result = String.Empty;
			using (var writer = new StringWriter()) {
				foreach (DataTable table in _dataSet.Tables)
					DumpTable(writer, table);

				result = writer.ToString();
			}

			return result;
		}

		private static void DumpTable(StringWriter writer, DataTable table)
		{
			writer.WriteLine(table.TableName + ":");
			writer.WriteLine();
			var columnNames = table.Columns.Cast<DataColumn>().Implode(item => item.ColumnName, "\t");

			writer.WriteLine(columnNames);
			writer.WriteLine("-".PadRight(columnNames.Length - 1, '-'));

			foreach (DataRow row in table.Rows)
				writer.WriteLine(row.ItemArray.Implode("\t"));

			writer.WriteLine();
			writer.WriteLine();
		}

		public static string TableToString(DataSet dataSet, string tableName)
		{
			using (var writer = new StringWriter()) {
				if (dataSet.Tables.Contains(tableName)) {
					DumpTable(writer, dataSet.Tables[tableName]);
					return writer.ToString();
				}
				else
					return null;
			}
		}

		public string TableToString(string tableName)
		{
			return TableToString(_dataSet, tableName);
		}

		public void SendMail()
		{
			var body = String
				.Format(
					"Проблема возникла у пользователя: {0}\r\nпри подготовке обновления от : {1}\r\nПричины:\r\n{2}",
					_updateData.UserName,
					_updateData.OldUpdateTime,
					_reasons.Implode("\r\n"));
			var attachment = DumpTables();

			MailHelper.Mail(body, "при подготовке данных возникли различия во Fresh между PricesData и Core", attachment, null);
		}
	}
}