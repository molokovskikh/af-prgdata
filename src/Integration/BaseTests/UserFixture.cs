using System.Collections.Generic;
using System.Linq;
using Castle.ActiveRecord;
using Common.Tools;
using NUnit.Framework;
using Test.Support;
using Test.Support.Helpers;

namespace Integration.BaseTests
{
	public class UserFixture : IntegrationFixture
	{
		public static TestUser CreateUser()
		{
			var client = TestClient.Create();
			TestUser user;
			using (var transaction = new TransactionScope()) {
				user = client.Users[0];

				client.Users.Each(u => {
					u.SendRejects = true;
					u.SendWaybills = true;
				});
				user.Update();
			}

			return user;
		}
	}
}