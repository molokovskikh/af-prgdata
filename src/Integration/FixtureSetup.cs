using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using Castle.ActiveRecord;
using Castle.MicroKernel.Registration;
using Common.Models;
using Common.Models.Tests.Repositories;
using Common.MySql;
using Common.Tools;
using Common.Tools.Helpers;
using Inforoom.Common;
using NHibernate.Mapping.Attributes;
using NUnit.Framework;
using PrgData.Common;
using PrgData.Common.Models;
using PrgData.Common.Repositories;
using SmartOrderFactory;
using SmartOrderFactory.Domain;
using SmartOrderFactory.Repositories;
using log4net.Config;
using MySqlHelper = MySql.Data.MySqlClient.MySqlHelper;

namespace Integration
{
	[SetUpFixture]
	public class FixtureSetup
	{
		/// <summary>
		/// Список поставщиков, для которых существуют предложения в farm.Core0
		/// Команда bake PrepareLocal для PrgData загружает в Core0 только данные избранных прайс-листов
		/// Список этих поставщиков используется в методе UserFixture.CreateUserWithMinimumPrices
		/// </summary>
		public static List<uint> FilledSuppliers = new List<uint>();

		[SetUp]
		public void Setup()
		{
			XmlConfigurator.Configure();

			ArchiveHelper.SevenZipExePath = @"7zip\7z.exe";
			Common.MySql.With.DefaultConnectionStringName = ConnectionHelper.GetConnectionName();

			PrepareMySqlPaths();

			CreateResultsDir();

			CheckLocalDB();

			Test.Support.Setup.BuildConfiguration();
			var holder = ActiveRecordMediator.GetSessionFactoryHolder();
			var cfg = holder.GetConfiguration(typeof(ActiveRecordBase));
			cfg.AddInputStream(HbmSerializer.Default.Serialize(Assembly.Load("Common.Models")));
			var factory = holder.GetSessionFactory(typeof(ActiveRecordBase));
			Test.Support.Setup.SessionFactory = factory;

			ContainerInitializer.InitializerContainerForTests(new[] { typeof(SmartOrderRule).Assembly, typeof(AnalitFVersionRule).Assembly });

			IoC.Container.Register(
				Component.For<ISmartOrderFactoryRepository>().ImplementedBy<SmartOrderFactoryRepository>(),
				Component.For<ISmartOfferRepository>().ImplementedBy<SmartOfferRepository>(),
				Component.For<IOrderFactory>().ImplementedBy<SmartOrderFactory.SmartOrderFactory>(),
				Component.For<IVersionRuleRepository>().ImplementedBy<VersionRuleRepository>());
		}

		private void PrepareMySqlPaths()
		{
			ServiceContext.SetupMySqlPath();
		}

		private void CheckLocalDB()
		{
			using (var connection = ConnectionHelper.GetConnection()) {
				connection.Open();
				var coreCount = Convert.ToUInt32(MySqlHelper.ExecuteScalar(
					connection,
					"select count(*) from farm.Core0"));

				Assert.That(coreCount, Is.GreaterThan(30000), "Локальная база данных не готова к тестам. Выполните в корне проекта: bake PrepareLocal");

				var distinctSuppliers = MySqlHelper.ExecuteDataset(
					connection,
					"select distinct pd.FirmCode as DistinctSupplier from farm.Core0 c join usersettings.PricesData pd on " +
						"pd.PriceCode = c.PriceCode");

				foreach (DataRow row in distinctSuppliers.Tables[0].Rows) {
					FilledSuppliers.Add(Convert.ToUInt32(row["DistinctSupplier"]));
				}
			}
		}

		private void CreateResultsDir()
		{
			if (Directory.Exists("results"))
				FileHelper.DeleteDir("results");

			Directory.CreateDirectory("results");
		}

		private void CheckNetworkFile(string testFile)
		{
			if (File.Exists(testFile))
				File.Delete(testFile);

			File.WriteAllText(testFile, "this is file");

			if (File.Exists(testFile)) {
				var contentFile = File.ReadAllText(testFile);
				Assert.That(contentFile, Is.EqualTo("this is file"));
				File.Delete(testFile);
			}
			else
				throw new Exception("Не найден файл: " + testFile);
		}

		private void CheckDBFiles()
		{
			var dbFilesPath = ServiceContext.MySqlLocalImportPath();
			var testFile = ServiceContext.GetFileByLocal(DateTime.Now.ToString("yyyyMMddHHmmss") + ".txt");

			var accessExists = false;
			try {
				CheckNetworkFile(testFile);

				accessExists = true;
			}
			catch (Exception) {
				//C:\Users\tester>net use \\fms.adc.analit.net\affiles newpassword /user:analit\PrgDataTester
				ProcessHelper.Cmd("net use {0} {1} /user:{2}", dbFilesPath, "newpassword", "analit\\PrgDataTester");
			}

			if (!accessExists)
				CheckNetworkFile(testFile);
		}
	}
}