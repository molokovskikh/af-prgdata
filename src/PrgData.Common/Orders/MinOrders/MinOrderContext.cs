using System;
using System.Collections.Generic;
using System.Linq;
using MySql.Data.MySqlClient;
using NHibernate;
using PrgData.Common.Models;

namespace PrgData.Common.Orders.MinOrders
{
	public class MinOrderContext : IMinOrderContext
	{
		public bool MinReqEnabled { get; private set; }
		public uint ClientId { get; private set; }
		public uint AddressId { get; private set; }
		public uint UserId { get; private set; }
		public uint SupplierId { get; private set; }
		public uint PriceCode { get; private set; }
		public ulong RegionCode { get; private set; }
		public string SupplierName { get; private set; }
		public string RegionName { get; private set; }
		public bool ControlMinReq { get; private set; }
		public uint MinReq { get; private set; }
		public uint MinReordering { get; private set; }
		public DateTime CurrentDateTime { get; private set; }

		private MySqlConnection _connection;
		private ISession _session;

		public MinOrderContext(MySqlConnection connection, ISession session, uint clientId,
			uint addressId, uint userId, uint pricdeCode, ulong regionCode)
		{
			_connection = connection;
			_session = session;

			CurrentDateTime = DateTime.Now;
			ClientId = clientId;
			AddressId = addressId;
			UserId = userId;
			PriceCode = pricdeCode;
			RegionCode = regionCode;

			MinReqEnabled = false;

			var command = new MySqlCommand(@"
SELECT
ai.ControlMinReq,
if(ifnull(ai.MinReq, 0) > 0, ai.MinReq, if(ifnull(i.MinReq, 0) > 0, i.MinReq, prd.MinReq)) as MinReqValue,
ifnull(if(ifnull(ai.MinReordering, 0) > 0, ai.MinReordering, prd.MinReqReorder), 0) as MinReorderingValue,
r.Region,
s.Id,
s.Name
FROM
Customers.Intersection i
join Customers.AddressIntersection ai on (ai.IntersectionId = i.Id)
join usersettings.pricesregionaldata prd on prd.pricecode = i.PriceId and prd.RegionCode = i.RegionId
join usersettings.pricesData pd on pd.PriceCode = prd.PriceCode
join farm.regions r on r.RegionCode = prd.RegionCode
join customers.suppliers s on s.Id = pd.FirmCode
where
	(i.ClientId = ?ClientCode)
and (prd.PriceCode =  ?PriceCode)
and (prd.RegionCode = ?RegionCode)
and (ai.AddressId = ?AddressId)
",
				_connection);
			command.Parameters.AddWithValue("?ClientCode", ClientId);
			command.Parameters.AddWithValue("?RegionCode", RegionCode);
			command.Parameters.AddWithValue("?PriceCode", PriceCode);
			command.Parameters.AddWithValue("?AddressId", AddressId);

			using (var reader = command.ExecuteReader())
				if (reader.Read()) {
					MinReqEnabled = true;
					ControlMinReq = reader.GetBoolean(0);
					MinReq = reader.GetUInt32(1);
					MinReordering = reader.GetUInt32(2);
					RegionName = reader.GetString(3);
					SupplierId = reader.GetUInt32(4);
					SupplierName = reader.GetString(5);
				}
		}

		public List<ReorderingRule> GetRules()
		{
			return _session.CreateSQLQuery(@"
SELECT
	r.Id as {ReorderingRule.Id},
	r.RegionalDataId as {ReorderingRule.RegionalDataId},
	r.DayOfWeek as {ReorderingRule.DayOfWeek},
	r.TimeOfStopsOrders as {ReorderingRule.TimeOfStopsOrders}
from
	UserSettings.regionalData rd
	inner join UserSettings.ReorderingRules r on r.RegionalDataId = rd.RowId
where
	rd.FirmCode = :supplierId
and rd.RegionCode = :regionId
order by r.DayOfWeek")
				.AddEntity("ReorderingRule", typeof(ReorderingRule))
				.SetParameter("supplierId", SupplierId)
				.SetParameter("regionId", RegionCode)
				.List<ReorderingRule>()
				.ToList();
		}

		public bool OrdersExists(IReorderingPeriod period)
		{
			var result = _session.CreateSQLQuery(@"
select
	count(*)
from
	orders.ordershead
where
	clientcode = :clientId
and addressId = :addressId
and userId = :userId
and :startDate <= writetime
and writetime < :endDate
and processed = 1
and deleted = 0")
				.SetParameter("clientId", ClientId)
				.SetParameter("addressId", AddressId)
				.SetParameter("userId", UserId)
				.SetParameter("startDate", period.StartTime)
				.SetParameter("endDate", period.EndTime)
				.UniqueResult<long>();

			return result > 0;
		}
	}
}