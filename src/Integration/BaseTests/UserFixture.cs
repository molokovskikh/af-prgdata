using System.Collections.Generic;
using System.Linq;
using Castle.ActiveRecord;
using Common.Tools;
using NUnit.Framework;
using Test.Support;
using Test.Support.Helpers;

namespace Integration.BaseTests
{
	public class UserFixture
	{
		public TestUser CreateUser()
		{
			var _client = TestClient.Create();
			TestUser _user;
			using (var transaction = new TransactionScope())
			{
				_user = _client.Users[0];

				_client.Users.Each(u =>
				{
					u.SendRejects = true;
					u.SendWaybills = true;
				});
				_user.Update();
			}

			return _user;
		}

		public TestUser CreateUserWithMinimumPrices()
		{
			var _user = CreateUser();

			SessionHelper.WithSession(
				s =>
				{
					var prices = _user.GetActivePricesList().Where(p => p.PositionCount > 800).OrderBy(p => p.PositionCount);
					var newPrices = new List<uint>();
					foreach (var testActivePrice in prices)
					{
						if (testActivePrice.CoreCount() > 0)
							newPrices.Add(testActivePrice.Id.PriceId);
						if (newPrices.Count == 4)
							break;
					}

					Assert.That(newPrices.Count, Is.EqualTo(4), "Не нашли достаточное кол-во прайс-листов для тестов");

					s.CreateSQLQuery(
						"delete from future.UserPrices where UserId = :userId and PriceId not in (:priceIds);")
						.SetParameter("userId", _user.Id)
						.SetParameterList("priceIds", newPrices.ToArray())
						.ExecuteUpdate();
				});

			return _user;
		}
	}
}