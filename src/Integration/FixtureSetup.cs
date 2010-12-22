using System.Reflection;
using Castle.MicroKernel.Registration;
using Common.Models;
using Common.Models.Tests.Repositories;
using NUnit.Framework;
using PrgData.Common.Model;
using PrgData.Common.Repositories;
using SmartOrderFactory;
using SmartOrderFactory.Domain;
using SmartOrderFactory.Repositories;

namespace Integration
{
	[SetUpFixture]
	public class FixtureSetup
	{
		[SetUp]
		public void Setup()
		{
			Test.Support.Setup.Initialize();

			ContainerInitializer.InitializerContainerForTests(new Assembly[] { typeof(SmartOrderRule).Assembly, typeof(AnalitFVersionRule).Assembly });

			IoC.Container.Register(
				Component.For<ISmartOrderFactoryRepository>().ImplementedBy<SmartOrderFactoryRepository>(),
				Component.For<ISmartOfferRepository>().ImplementedBy<SmartOfferRepository>(),
				Component.For<IOrderFactory>().ImplementedBy<SmartOrderFactory.SmartOrderFactory>(),
				Component.For<IVersionRuleRepository>().ImplementedBy<VersionRuleRepository>()
				);
		}
	}
}