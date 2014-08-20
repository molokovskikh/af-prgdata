using System;
using System.Collections.Generic;
using System.Linq;
using Common.Models.Helpers;
using Common.Tools;
using Common.Models;
using NUnit.Framework;

namespace Unit
{
	[TestFixture]
	public class CostOptimizer2Fixture
	{
		[Test]
		public void Optimize()
		{
			var config = new[] {
				new CostOptimizationRule {
					Supplier = new Supplier { Id = 1 },
					Concurrents = new [] { new Supplier { Id = 2u },  new Supplier { Id = 3u } }
				},
				new CostOptimizationRule {
					Supplier = new Supplier { Id = 3 },
					Concurrents = new [] { new Supplier { Id = 4u }, new Supplier { Id = 5u } }
				}
			};

			var offers = new[] {
				//монопольное предложение по продукту 10 производителю 2
				new Offer2 {
					SupplierId = 1,
					ProductId = 10,
					ProducerId = 2,
					Cost = 51.20m,
					MaxBoundCost = 150,
				},
				new Offer2 {
					SupplierId = 4,
					ProductId = 10,
					ProducerId = 2,
					Cost = 70.34m,
				},
				new Offer2 {
					SupplierId = 4,
					ProductId = 10,
					ProducerId = 3,
					Cost = 67.34m,
				},
				//не монопольное предложение по продукту 10 производителю 4
				new Offer2 {
					SupplierId = 1,
					ProductId = 10,
					ProducerId = 4,
					Cost = 55.20m,
					MaxBoundCost = 200,
				},
				new Offer2 {
					SupplierId = 2,
					ProductId = 10,
					ProducerId = 4,
					Cost = 71.34m,
				},
			};

			CostOptimizer.MonopolisticsOptimize(offers, config);
			Assert.AreEqual(150, offers[0].Cost);
			Assert.AreEqual(55.20, offers[3].Cost);
		}

		[Test]
		public void Ignore_unknown_producers()
		{
			var config = new[] {
				new CostOptimizationRule {
					Supplier = new Supplier { Id = 1 },
					Concurrents = new [] { new Supplier { Id = 2u },  new Supplier { Id = 3u } }
				},
			};

			var offers = new[] {
				//монопольное предложение по продукту 10
				new Offer2 {
					SupplierId = 1,
					ProductId = 10,
					Cost = 51.20m,
					MaxBoundCost = 150,
				},
				new Offer2 {
					SupplierId = 4,
					ProductId = 10,
					Cost = 70.34m,
				},
				new Offer2 {
					SupplierId = 4,
					ProductId = 10,
					Cost = 67.34m,
				},
			};
			CostOptimizer.MonopolisticsOptimize(offers, config);
			Assert.AreEqual(51.20, offers[0].Cost);
		}

		[Test]
		public void Use_cost_diapason()
		{
			var config = new[] {
				new CostOptimizationRule {
					Supplier = new Supplier { Id = 1 },
					Concurrents = new[] { new Supplier { Id = 2u },  new Supplier { Id = 3u } },
					Diapasons = new[] { new CostOptimizationDiapason(0, 1000, 20),  }
				},
			};

			var offers = new[] {
				//монопольное предложение по продукту 10
				new Offer2 {
					SupplierId = 1,
					ProductId = 10,
					ProducerId = 2,
					Cost = 51.20m,
					MaxBoundCost = 150,
				},
				new Offer2 {
					SupplierId = 4,
					ProductId = 10,
					ProducerId = 2,
					Cost = 70.34m,
				},
				new Offer2 {
					SupplierId = 4,
					ProductId = 10,
					ProducerId = 2,
					Cost = 67.34m,
				},
			};
			CostOptimizer.MonopolisticsOptimize(offers, config);
			Assert.AreEqual(61.44, offers[0].Cost);
		}
	}
}