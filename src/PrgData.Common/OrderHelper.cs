using System;
using MySql.Data.MySqlClient;

namespace PrgData.Common
{
	public class OrderHelper
	{
		protected UpdateData _data;
		protected MySqlConnection _readWriteConnection;

		public OrderHelper(UpdateData data, MySqlConnection readWriteConnection)
		{
			_data = data;
			_readWriteConnection = readWriteConnection;
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