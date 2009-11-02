using System;
using System.Collections.Generic;
using System.Linq;
using log4net;
using MySql.Data.MySqlClient;

namespace PrgData.Common
{
	public class CostOptimizer
	{
		private readonly ILog _log = LogManager.GetLogger(typeof (CostOptimizer));

		private readonly MySqlConnection _connection;

		private readonly uint _firmCode;
		private readonly ulong _homeRegionCode;

		public CostOptimizer(MySqlConnection connection, uint clientCode)
		{
			_connection = connection;
			var command = new MySqlCommand(@"
select FirmCode 
from usersettings.CostCorrectorSettings
where clientCode = ?ClientCode
limit 1", _connection);
			command.Parameters.AddWithValue("?ClientCode", clientCode);
			var value = command.ExecuteScalar();
			if (!(value == DBNull.Value))
				_firmCode = Convert.ToUInt32(value);

			command.CommandText = @"select regionCode 
from usersettings.clientsdata
where firmcode = ?clientcode";
			_homeRegionCode = Convert.ToUInt64(command.ExecuteScalar());
		}

		public bool IsCostOptimizationNeeded()
		{
			if (_firmCode == 0)
				return false;
			var command = new MySqlCommand(@"
select count(*)
from usersettings.ActivePrices
where firmcode = ?firmCode", _connection);
			command.Parameters.AddWithValue("?firmCode", _firmCode);
			return Convert.ToUInt32(command.ExecuteScalar()) > 0;
		}

		public void Oprimize()
		{
			if (_log.IsDebugEnabled)
				_log.DebugFormat("Начало оптимизации цен, поставщик {0}", _firmCode);

			var concurents = new List<uint>();
			var command = new MySqlCommand(@"
select FirmCode
from ConcurentGroup
where SettingsId = ?FirmCode", _connection);
			command.Parameters.AddWithValue("?FirmCode", _firmCode);
			using(var reader = command.ExecuteReader())
				while (reader.Read())
					concurents.Add(reader.GetUInt32("FirmCode"));

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
	Junk tinyint(1) not null,
	primary key (ProductId, Junk, CodeFirmCr)
) engine memory;

drop temporary table if exists CoreCopy;
create temporary table CoreCopy
select * from core;

insert into ConcurentCosts(AllCost, Cost, ProductId, Junk, CodeFirmCr)
select min(c.cost), min(if(ap.FirmCode in ({0}), c.Cost, null)), c0.ProductId, c0.Junk, c0.CodeFirmCr
from core c
	join farm.core0 c0 on c.id = c0.id
	join ActivePrices ap on ap.PriceCode = c.PriceCode
where c0.CodeFirmCr is not null and c.RegionCode = ?HomeRegionCode
group by c0.productid, c0.CodeFirmCr, c0.junk;

update core c
	join farm.Core0 c0 on c0.Id = c.Id
	join ActivePrices ap on c.PriceCode = ap.PriceCode
	join ConcurentCosts cc on cc.ProductId = c.ProductId and cc.Junk = c0.Junk and cc.CodeFirmCr = c0.CodeFirmCr
set c.Cost = MakeCost(c.Cost, cc.Cost)
where ap.FirmCode = ?FirmCode and c.RegionCode = ?HomeRegionCode and c0.CodeFirmCr is not null;

insert into logs.CostOptimizationLogs(ProductId, ProducerId, Junk, SelfCost, ConcurentCost, AllCost, ResultCost)
select c0.ProductId, c0.CodeFirmCr, c0.Junk, copy.Cost, cc.Cost, cc.AllCost, c.Cost
from core c
	join CoreCopy copy on copy.Id = c.id
	join farm.Core0 c0 on c0.Id = c.Id
	join ConcurentCosts cc on cc.ProductId = c.ProductId and cc.Junk = c0.Junk and cc.CodeFirmCr = c0.CodeFirmCr
where c.Cost <> copy.Cost and c.RegionCode = ?HomeRegionCode and copy.RegionCode = ?HomeRegionCode;

drop temporary table ConcurentCosts;
drop temporary table CoreCopy;
", String.Join(", ", concurents.Select(f => f.ToString()).ToArray())), _connection);
			optimizeCommand.Parameters.AddWithValue("?FirmCode", _firmCode);
			optimizeCommand.Parameters.AddWithValue("?HomeRegionCode", _homeRegionCode);

			optimizeCommand.ExecuteNonQuery();

			if (_log.IsDebugEnabled)
				_log.DebugFormat("Оптимизация цен завершена, поставщик {0}", _firmCode);
		}

		//если кто то из конкурентов обновил прайс то нам тоже нужно
		//ставим флаг что наш прайc тоже обновлен
		private void PatchFresh(IEnumerable<uint> concurents)
		{
			var command = new MySqlCommand(String.Format(@"
select exists(
select *
from activeprices
where firmcode in ({0}) and fresh = 1)", String.Join(", ", concurents.Select(c => c.ToString()).ToArray())), _connection);
			var isConcurentUpdated = Convert.ToBoolean(command.ExecuteScalar());
			if (isConcurentUpdated)
			{
				var updateCommand = new MySqlCommand(@"
update usersettings.ActivePrices ap
set fresh = 1
where ap.FirmCode = ?FirmCode and ap.RegionCode = ?HomeRegion", _connection);
				updateCommand.Parameters.AddWithValue("?HomeRegion", _homeRegionCode);
				updateCommand.Parameters.AddWithValue("?FirmCode", _firmCode);
				updateCommand.ExecuteNonQuery();
			}
		}
	}
}
