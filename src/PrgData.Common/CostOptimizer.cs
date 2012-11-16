using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using log4net;
using MySql.Data.MySqlClient;

namespace PrgData.Common
{
	public class CostOptimizationLog
	{
		public uint ProductId { get; set; }
		public uint ProducerId { get; set; }
		public decimal? SelfCost { get; set; }
		public decimal? ConcurentCost { get; set; }
		public decimal? AllCost { get; set; }
		public decimal? ResultCost { get; set; }
	}

	public class CostOptimizer
	{
		private readonly ILog _log = LogManager.GetLogger(typeof(CostOptimizer));

		private readonly MySqlConnection _readWriteConnection;

		private readonly uint _clientId;
		private readonly uint _supplierId;
		private readonly uint _ruleId;
		private readonly ulong _homeRegionCode;

		public static void OptimizeCostIfNeeded(MySqlConnection readWriteConnection, uint clientCode)
		{
			var optimizer = new CostOptimizer(readWriteConnection, clientCode);
			if (optimizer.IsCostOptimizationNeeded())
				optimizer.Oprimize();
		}

		public CostOptimizer(MySqlConnection readWriteConnection, uint clientCode)
		{
			_clientId = clientCode;
			_readWriteConnection = readWriteConnection;

			var command = new MySqlCommand(@"
select cor.Id, cor.SupplierId
from usersettings.CostOptimizationClients coc 
	join usersettings.CostOptimizationRules cor on coc.RuleId = cor.Id
where coc.ClientId = ?ClientId
limit 1", _readWriteConnection);
			command.Parameters.AddWithValue("?ClientId", clientCode);
			using (var reader = command.ExecuteReader()) {
				if (reader.Read()) {
					_supplierId = reader.GetUInt32("SupplierId");
					_ruleId = reader.GetUInt32("Id");
				}
			}

			command.CommandText = @"
select RegionCode
from Customers.clients
where Id = ?ClientId";
			_homeRegionCode = Convert.ToUInt64(command.ExecuteScalar());
		}

		public bool IsCostOptimizationNeeded()
		{
			if (_supplierId == 0)
				return false;
			var command = new MySqlCommand(@"
select count(*)
from usersettings.ActivePrices
where firmcode = ?firmCode", _readWriteConnection);
			command.Parameters.AddWithValue("?firmCode", _supplierId);
			return Convert.ToUInt32(command.ExecuteScalar()) > 0;
		}

		public void Oprimize()
		{
			if (_log.IsDebugEnabled)
				_log.DebugFormat("Начало оптимизации цен, поставщик {0}", _supplierId);

			var concurents = new List<uint>();
			var command = new MySqlCommand(@"
select coc.SupplierId
from Usersettings.CostOptimizationConcurrents coc
where coc.RuleId = ?Id", _readWriteConnection);
			command.Parameters.AddWithValue("?Id", _ruleId);
			using (var reader = command.ExecuteReader())
				while (reader.Read())
					concurents.Add(reader.GetUInt32("SupplierId"));

			if (concurents.Count == 0)
				throw new Exception("Ни одного поставщика в конкурентной группе");

			PatchFresh(concurents);

			var optimizeCommand = new MySqlCommand(String.Format(@"
drop temporary table if exists ConcurentCosts;
create temporary table ConcurentCosts
(
	Cost decimal(11, 2),
	AllCost decimal(11, 2) not null,
	ProductId int not null,
	CodeFirmCr int not null,
	primary key (ProductId, CodeFirmCr)
) engine memory;

drop temporary table if exists CoreCopy;
create temporary table CoreCopy engine memory
select * from core;

insert into ConcurentCosts(AllCost, Cost, ProductId, CodeFirmCr)
select min(c.cost), min(if(ap.FirmCode in ({0}), c.Cost, null)), c0.ProductId, c0.CodeFirmCr
from core c
	join farm.core0 c0 on c.id = c0.id
	join ActivePrices ap on ap.PriceCode = c.PriceCode
where c.RegionCode = ?HomeRegionCode and c0.Junk = 0 and c0.CodeFirmCr is not null
group by c0.productid, c0.CodeFirmCr, c0.junk;

update core c
	join farm.Core0 c0 on c0.Id = c.Id
	join ActivePrices ap on c.PriceCode = ap.PriceCode
	join ConcurentCosts cc on cc.ProductId = c.ProductId and cc.CodeFirmCr = c0.CodeFirmCr
set c.Cost = MakeCostNoLess(c.Cost, cc.Cost)
where ap.FirmCode = ?FirmCode and c.RegionCode = ?HomeRegionCode and c0.Junk = 0 and c0.CodeFirmCr is not null;

select c0.ProductId, c0.CodeFirmCr, copy.Cost as SelfCost, cc.Cost as ConcurentCost, cc.AllCost, c.Cost as ResultCost
from core c
	join CoreCopy copy on copy.Id = c.id
	join farm.Core0 c0 on c0.Id = c.Id
	join ConcurentCosts cc on cc.ProductId = c.ProductId and cc.CodeFirmCr = c0.CodeFirmCr
where c.Cost <> copy.Cost and c.RegionCode = ?HomeRegionCode and copy.RegionCode = ?HomeRegionCode;

drop temporary table ConcurentCosts;
drop temporary table CoreCopy;
", String.Join(", ", concurents.Select(f => f.ToString()).ToArray())), _readWriteConnection);
			optimizeCommand.Parameters.AddWithValue("?FirmCode", _supplierId);
			optimizeCommand.Parameters.AddWithValue("?HomeRegionCode", _homeRegionCode);

			var logs = new List<CostOptimizationLog>();
			using (var reader = optimizeCommand.ExecuteReader()) {
				while (reader.Read()) {
					var log = new CostOptimizationLog {
						ProducerId = reader.GetUInt32("CodeFirmCr"),
						ProductId = reader.GetUInt32("ProductId"),
					};
					logs.Add(log);

					if (!reader.IsDBNull(reader.GetOrdinal("SelfCost")))
						log.SelfCost = reader.GetDecimal("SelfCost");
					if (!reader.IsDBNull(reader.GetOrdinal("ConcurentCost")))
						log.ConcurentCost = reader.GetDecimal("ConcurentCost");
					if (!reader.IsDBNull(reader.GetOrdinal("AllCost")))
						log.AllCost = reader.GetDecimal("AllCost");
					if (!reader.IsDBNull(reader.GetOrdinal("ResultCost")))
						log.ResultCost = reader.GetDecimal("ResultCost");
				}
			}

			var header = "insert into logs.CostOptimizationLogs(ClientId, SupplierId, ProductId, ProducerId, SelfCost, ConcurentCost, AllCost, ResultCost) values";
			var logCommand = new MySqlCommand(header, _readWriteConnection);

			var begin = 0;

			while (begin < logs.Count) {
				var commandText = new StringBuilder();
				commandText.Append(header);

				for (var i = 0; i < 100; i++) {
					if (begin + i >= logs.Count)
						break;

					var log = logs[begin + i];
					commandText.Append(String.Format(" ({6}, {7}, {0}, {1}, {2}, {3}, {4}, {5})", log.ProductId, log.ProducerId, ForMySql(log.SelfCost), ForMySql(log.ConcurentCost), ForMySql(log.AllCost), ForMySql(log.ResultCost), _clientId, _supplierId));
					if (i < 99 && begin + i < logs.Count - 1)
						commandText.AppendLine(", ");
				}
				logCommand.CommandText = commandText.ToString();
				logCommand.ExecuteNonQuery();
				begin += 100;
			}

			if (_log.IsDebugEnabled)
				_log.DebugFormat("Оптимизация цен завершена, поставщик {0}", _supplierId);
		}

		public string ForMySql(decimal? value)
		{
			if (value == null)
				return "null";
			return value.Value.ToString(CultureInfo.InvariantCulture);
		}

		//если кто то из конкурентов обновил прайс то нам тоже нужно
		//ставим флаг что наш прайc тоже обновлен
		private void PatchFresh(IEnumerable<uint> concurents)
		{
			var command = new MySqlCommand(String.Format(@"
select exists(
select *
from activeprices
where firmcode in ({0}) and fresh = 1)", String.Join(", ", concurents.Select(c => c.ToString()).ToArray())), _readWriteConnection);
			var isConcurentUpdated = Convert.ToBoolean(command.ExecuteScalar());
			if (isConcurentUpdated) {
				var updateCommand = new MySqlCommand(@"
update usersettings.ActivePrices ap
set fresh = 1
where ap.FirmCode = ?FirmCode and ap.RegionCode = ?HomeRegion", _readWriteConnection);
				updateCommand.Parameters.AddWithValue("?HomeRegion", _homeRegionCode);
				updateCommand.Parameters.AddWithValue("?FirmCode", _supplierId);
				updateCommand.ExecuteNonQuery();
			}
		}
	}
}