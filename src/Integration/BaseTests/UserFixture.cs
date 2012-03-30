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
			var client = TestClient.Create();
			TestUser user;
			using (var transaction = new TransactionScope())
			{
				user = client.Users[0];

				client.Users.Each(u =>
				{
					u.SendRejects = true;
					u.SendWaybills = true;
				});
				user.Update();
			}

			return user;
		}

		public TestUser CreateUserWithMinimumPrices()
		{
			var user = CreateUser();

			SessionHelper.WithSession(
				s =>
				{
					var prices = user.GetActivePricesList().Where(p => p.PositionCount > 800).OrderBy(p => p.PositionCount);
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
						"delete from Customers.UserPrices where UserId = :userId and PriceId not in (:priceIds);")
						.SetParameter("userId", user.Id)
						.SetParameterList("priceIds", newPrices.ToArray())
						.ExecuteUpdate();
				});

			return user;
		}
	}
}