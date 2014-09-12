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
		[SetUp]
		public void Setup()
		{
			XmlConfigurator.Configure();

			ArchiveHelper.SevenZipExePath = @"7zip\7z.exe";
			Common.MySql.With.DefaultConnectionStringName = ConnectionHelper.GetConnectionName();

			PrepareMySqlPaths();

			CreateResultsDir();

			Test.Support.Setup.BuildConfiguration();
			var holder = ActiveRecordMediator.GetSessionFactoryHolder();
			var cfg = holder.GetConfiguration(typeof(ActiveRecordBase));
			cfg.AddInputStream(HbmSerializer.Default.Serialize(Assembly.Load("Common.Models")));
			cfg.AddInputStream(HbmSerializer.Default.Serialize(Assembly.Load("PrgData.Common")));

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
