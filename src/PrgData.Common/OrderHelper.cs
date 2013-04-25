using System;
using MySql.Data.MySqlClient;

namespace PrgData.Common
{
	public class MinReqController
	{
		public bool ControlMinReq;
		public uint MinReq;

		public MinReqController(bool controlMinReq, uint minReq)
		{
			ControlMinReq = controlMinReq;
			MinReq = minReq;
		}
	}

	public class OrderHelper
	{
		protected UpdateData _data;
		protected MySqlConnection _readWriteConnection;

		public OrderHelper(UpdateData data, MySqlConnection readWriteConnection)
		{
			_data = data;
			_readWriteConnection = readWriteConnection;
		}

		public MinReqController GetMinReq(uint clientCode, ulong regionCode, uint priceCode)
		{
			var command = new MySqlCommand(@"
SELECT
ai.ControlMinReq,
if(ifnull(ai.MinReq, 0) > 0, ai.MinReq, if(ifnull(i.MinReq, 0) > 0, i.MinReq, prd.MinReq))
FROM
Customers.Intersection i
join Customers.AddressIntersection ai on (ai.IntersectionId = i.Id)
join usersettings.pricesregionaldata prd on prd.pricecode = i.PriceId and prd.RegionCode = i.RegionId
where
	(i.ClientId = ?ClientCode)
and (prd.PriceCode =  ?PriceCode)
and (prd.RegionCode = ?RegionCode)
and (ai.AddressId = ?AddressId)
",
				_readWriteConnection);
			command.Parameters.AddWithValue("?ClientCode", _data.ClientId);
			command.Parameters.AddWithValue("?RegionCode", regionCode);
			command.Parameters.AddWithValue("?PriceCode", priceCode);
			command.Parameters.AddWithValue("?AddressId", clientCode);

			using (var reader = command.ExecuteReader())
				if (reader.Read())
					return new MinReqController(reader.GetBoolean(0), reader.GetUInt32(1));

			return null;
		}

		public void CheckCanPostOrder(uint clientCode)
		{
			var command = new MySqlCommand(@"
select if(ua.UserId is null, 0, 1)
from Customers.Users u
join Customers.UserAddresses ua on ua.UserId = u.Id and ua.AddressId = ?AddressId
where u.Id = ?UserId", _readWriteConnection);
			command.Parameters.AddWithValue("?UserId", _data.UserId);
			command.Parameters.AddWithValue("?AddressId", clientCode);
			var canPostOrder = Convert.ToBoolean(command.ExecuteScalar());

			if (!canPostOrder)
				throw new UpdateException("Отправка заказов запрещена", "Пожалуйста, обратитесь в АК \"Инфорум\".", RequestType.Forbidden);
		}
	}
}