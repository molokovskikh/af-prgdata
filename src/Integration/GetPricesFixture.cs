using NUnit.Framework;
using Test.Support;
using Test.Support.Suppliers;

namespace Integration
{
	[TestFixture]
	public class GetPricesFixture : IntegrationFixture
	{
		[Test]
		public void Hidden_copy_should_respect_regional_base_cost()
		{
			var supplier = TestSupplier.CreateNaked();
			var price = supplier.Prices[0];
			var cost = price.NewPriceCost();
			price.RegionalData[0].BaseCost = cost;

			var client = TestClient.CreateNaked();
			var user = client.Users[0];
			client.Settings.InvisibleOnFirm = 2;
			session.Flush();

			var result = session.CreateSQLQuery(@"call Customers.GetPrices(:userId);
select CostCode
from Usersettings.Prices
where PriceCode = :priceId")
				.SetParameter("userId", user.Id)
				.SetParameter("priceId", price.Id)
				.List();
			Assert.That(result.Count, Is.EqualTo(1));
			Assert.That(result[0], Is.EqualTo(cost.Id), "userid = {0}, priceid = {1}", user.Id, price.Id);
		}
	}
}