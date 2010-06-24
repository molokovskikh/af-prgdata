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
		protected MySqlConnection _connection;
		protected MySqlConnection _readWriteConnection;

		public OrderHelper(UpdateData data, MySqlConnection readOnlyConnection, MySqlConnection readWriteConnection)
		{
			_data = data;
			_connection = readOnlyConnection;
			_readWriteConnection = readWriteConnection;
		}

		public MinReqController GetMinReq(uint clientCode, ulong regionCode, uint priceCode)
		{
			if (_data.IsFutureClient)
			{
				var command = new MySqlCommand(@"
SELECT i.ControlMinReq, if(ifnull(i.MinReq, 0) > 0, i.MinReq, prd.MinReq)
FROM Future.Intersection i
join usersettings.pricesregionaldata prd on prd.pricecode = i.PriceId and prd.RegionCode = i.RegionId
where i.ClientId = ?ClientCode and prd.PriceCode = ?PriceCode and prd.RegionCode = ?RegionCode", _connection);
				command.Parameters.AddWithValue("?ClientCode", _data.ClientId);
				command.Parameters.AddWithValue("?RegionCode", regionCode);
				command.Parameters.AddWithValue("?PriceCode", priceCode);

				using (var reader = command.ExecuteReader())
					if (reader.Read())
						return new MinReqController(reader.GetBoolean(0), reader.GetUInt32(1));

				return null;
			}
			else
			{
				var command = new MySqlCommand(@"
SELECT i.ControlMinReq, if(ifnull(i.MinReq, 0) > 0, i.MinReq, prd.MinReq)
FROM usersettings.intersection i
join usersettings.pricesregionaldata prd on prd.pricecode = i.priceCode and prd.RegionCode = i.regionCode
where i.ClientCode = ?ClientCode and prd.PriceCode = ?PriceCode and prd.RegionCode = ?RegionCode", _connection);
				command.Parameters.AddWithValue("?ClientCode", clientCode);
				command.Parameters.AddWithValue("?RegionCode", regionCode);
				command.Parameters.AddWithValue("?PriceCode", priceCode);

				using (var reader = command.ExecuteReader())
					if (reader.Read())
						return new MinReqController(reader.GetBoolean(0), reader.GetUInt32(1));

				return null;
			}
		}

		public void CheckCanPostOrder(uint clientCode)
		{
			bool canPostOrder;
			if (_data.IsFutureClient)
			{
				var command = new MySqlCommand(@"
select if(ua.UserId is null, 0, 1)
from Future.Users u
	join Future.UserAddresses ua on ua.UserId = u.Id and ua.AddressId = ?AddressId
where u.Id = ?UserId", _connection);
				command.Parameters.AddWithValue("?UserId", _data.UserId);
				command.Parameters.AddWithValue("?AddressId", clientCode);
				canPostOrder = Convert.ToBoolean(command.ExecuteScalar());
			}
			else
			{
				var command = new MySqlCommand(@"
select ifnull(Max(IncludeClientCode=?ClientCode or ClientCode=?ClientCode), 0) as A 
from osuseraccessright
left join includeregulation on PrimaryClientCode=ClientCode and IncludeType in (0,3)
where osuseraccessright.RowId = ?UserId", _connection);
				command.Parameters.AddWithValue("?UserId", _data.UserId);
				command.Parameters.AddWithValue("?clientcode", clientCode);
				canPostOrder = Convert.ToBoolean(command.ExecuteScalar());
			}

			if (!canPostOrder)
				throw new UpdateException("Отправка заказов запрещена", "Пожалуйста обратитесь в АК \"Инфорум\".", RequestType.Forbidden);
		}

		public ulong SaveOrder(uint clientId, uint priceId, ulong regionId, DateTime priceDate, uint rowCount, uint clientOrderId, string clientAddition)
		{
			return SaveOrder(clientId, priceId, regionId, priceDate, rowCount, clientOrderId, clientAddition, null);
		}

		public ulong SaveOrder(uint clientId, uint priceId, ulong regionId, DateTime priceDate, uint rowCount, uint clientOrderId, string clientAddition, decimal? delayOfPayment)
		{
			priceDate = priceDate.ToLocalTime();
			if (_data.IsFutureClient)
			{
				var command = new MySqlCommand(@"
INSERT
INTO orders.ordershead (
	ClientCode,
	AddressId,
	UserId,
	PriceCode,
	RegionCode,
	PriceDate,
	ClientAddition,
	RowCount,
	ClientOrderId,
	Submited,
	SubmitDate,
    DelayOfPayment
)
SELECT ?ClientCode,
	?AddressId,
	?UserId,
	?PriceCode,
	?RegionCode,
	?PriceDate,
	?ClientAddition,
	?RowCount,
	?ClientOrderID,
	NOT (u.SubmitOrders),
	IF(NOT(u.SubmitOrders), NOW(), NULL),
    ?DelayOfPayment
FROM Future.Users u
WHERE u.Id = ?UserId;

select LAST_INSERT_ID();", _readWriteConnection);
				command.Parameters.AddWithValue("?ClientCode", _data.ClientId);
				command.Parameters.AddWithValue("?AddressId", clientId);
				command.Parameters.AddWithValue("?UserId", _data.UserId);
				command.Parameters.AddWithValue("?PriceCode", priceId);
				command.Parameters.AddWithValue("?RegionCode", regionId);
				command.Parameters.AddWithValue("?PriceDate", priceDate);
				command.Parameters.AddWithValue("?RowCount", rowCount);
				command.Parameters.AddWithValue("?ClientOrderId", clientOrderId);
				command.Parameters.AddWithValue("?ClientAddition", clientAddition);
				command.Parameters.AddWithValue("?DelayOfPayment", delayOfPayment);
				return Convert.ToUInt32(command.ExecuteScalar());
			}
			else
			{
				var command = new MySqlCommand(@"
INSERT
INTO orders.ordershead (
	ClientCode,
	PriceCode,
	RegionCode,
	PriceDate,
	ClientAddition,
	RowCount,
	ClientOrderID ,
	Submited,
	SubmitDate,
    DelayOfPayment
)
SELECT ?ClientCode,
		?PriceCode,
		?RegionCode,
		?PriceDate,
		?ClientAddition,
		?RowCount,
		?ClientOrderID ,
		NOT (SubmitOrders and AllowSubmitOrders),
		IF(NOT(SubmitOrders and AllowSubmitOrders), NOW(), NULL),
        ?DelayOfPayment
FROM RetClientsSet RCS
WHERE RCS.ClientCode=?ClientCode;

select LAST_INSERT_ID();", _readWriteConnection);
				command.Parameters.AddWithValue("?ClientCode", clientId);
				command.Parameters.AddWithValue("?PriceCode", priceId);
				command.Parameters.AddWithValue("?RegionCode", regionId);
				command.Parameters.AddWithValue("?PriceDate", priceDate);
				command.Parameters.AddWithValue("?RowCount", rowCount);
				command.Parameters.AddWithValue("?ClientOrderId", clientOrderId);
				command.Parameters.AddWithValue("?ClientAddition", clientAddition);
				command.Parameters.AddWithValue("?DelayOfPayment", delayOfPayment);
				return Convert.ToUInt64(command.ExecuteScalar());
			}
		}
	}
}