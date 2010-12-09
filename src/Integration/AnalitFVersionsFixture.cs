using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Reflection;
using Castle.ActiveRecord;
using Castle.MicroKernel.Registration;
using Common.Models;
using Common.Models.Repositories;
using Common.Models.Tests.Repositories;
using Common.Tools;
using Inforoom.Common;
using MySql.Data.MySqlClient;
using NUnit.Framework;
using PrgData.Common;
using PrgData.Common.AnalitFVersions;
using PrgData.Common.Model;
using PrgData.Common.Repositories;
using SmartOrderFactory.Domain;
using Test.Support;

namespace Integration
{
	[TestFixture]
	public class AnalitFVersionsFixture
	{
		private TestClient client;
		private TestUser user;
		private TestAddress address;

		[SetUp]
		public void Setup()
		{
			Test.Support.Setup.Initialize();

			ServiceContext.GetUserHost = () => "127.0.0.1";
			UpdateHelper.GetDownloadUrl = () => "http://localhost/";
			ServiceContext.GetResultPath = () => "results\\";

			ContainerInitializer.InitializerContainerForTests(new Assembly[]{typeof(SmartOrderRule).Assembly, typeof(AnalitFVersionRule).Assembly});
			//IoC.Container.Register(
			//    Component.For<ISmartOfferRepository>().ImplementedBy<SmartOfferRepository>()
			//    );
			IoC.Container.Register(
				Component.For<IVersionRuleRepository>().ImplementedBy<VersionRuleRepository>()
			    );

			//ConfigurationManager.AppSettings["WaybillPath"] = "FtpRoot\\";
			//if (Directory.Exists("FtpRoot"))
			//    FileHelper.DeleteDir("FtpRoot");

			using (var transaction = new TransactionScope())
			{
				var permission = TestUserPermission.ByShortcut("AF");

				client = TestClient.CreateSimple();
				user = client.Users[0];

				client.Users.Each(u =>
				{
					u.AssignedPermissions.Add(permission);
					u.SendRejects = true;
					u.SendWaybills = true;
				});
				user.Update();

				address = user.AvaliableAddresses[0];
			}

			//IoC.Resolve<IRepository<AnalitFVersionRule>>().

			//Directory.CreateDirectory("FtpRoot");
			//CreateFolders(address.Id.ToString());

			InsertEtalonVersions();
		}

		[TestFixtureTearDown]
		public void FixtureTearDown()
		{
			InsertEtalonVersions();
		}

		private void DeleteVersions()
		{
			MySqlHelper.ExecuteNonQuery(
				Settings.ConnectionString(),
				"delete from usersettings.AnalitFVersionRules");
		}

		private void InsertEtalonVersions()
		{
			DeleteVersions();

			MySqlHelper.ExecuteNonQuery(
				Settings.ConnectionString(),
				@"
insert into usersettings.AnalitFVersionRules
(SourceVersion, DestinationVersion)
values
(705, 716),
(705, 1101),
(716, 1101),
(1011, 1065),
(1065, 1106),
(1101, 1161),
(1106, 1161),
(1161, 1183),
(1183, 1317),
(1229, 1317),
(1259, 1317),
(1263, 1317),
(1269, 1317),
(1271, 1317),
(1285, 1317),
(1289, 1317),
(1295, 1317),
(1299, 1317),
(1315, 1317);
");
		}

		[Test(Description = "Тестируем чтение версий из базы")]
		public void TestVersionRepository()
		{
			var realVersionCount = Convert.ToInt32(
				MySqlHelper.ExecuteScalar(
				Settings.ConnectionString(),
				"select count(*) from UserSettings.AnalitFVersionRules"));

			var rulesRepository = IoC.Resolve<IVersionRuleRepository>();

			var rules = rulesRepository.FindAllRules().ToList();

			Assert.That(rules.Count, Is.GreaterThan(0));
			Assert.That(rules.Count, Is.EqualTo(realVersionCount));
			Assert.That(rules.TrueForAll(rule => rule.Id > 0 && rule.SourceVersion > 0 && rule.SourceVersion < rule.DestinationVersion), "Не все правила корректно созданы");
		}

		[Test(Description = "Проверка создания VersionInfo")]
		public void ReadVersionInfos()
		{
			var dirInfo = new DirectoryInfo("..\\..\\EtalonUpdates\\Updates");
			var releaseInfos = dirInfo.GetDirectories("Release*");
			Assert.That(releaseInfos.Length, Is.EqualTo(7));
			var infos = new List<VersionInfo>();

			foreach (var releaseInfo in releaseInfos)
			{
				var info = new VersionInfo(releaseInfo.FullName);

				Assert.That(info.VersionNumber, Is.GreaterThan(0));
				Assert.That(info.ExeVersionInfo, Is.Not.Null);
				Assert.That(info.ExeVersionNumber(), Is.GreaterThan(0));
				Assert.That(info.Folder, Is.Not.Null.And.Not.Empty);
				Assert.That(info.ExeFolder(), Is.Not.Null.And.Not.Empty);
				Assert.That(info.ExeFolder(), Is.StringEnding("Exe").IgnoreCase);

				infos.Add(info);
			}

			Assert.That(infos.Count, Is.EqualTo(releaseInfos.Length));

			ServiceContext.GetResultPath = () => "..\\..\\EtalonUpdates\\";
			var factoryInfos = VersionUpdaterFactory.GetVersionInfos();
			Assert.That(factoryInfos.Count, Is.EqualTo(infos.Count));
		}

		[Test(Description = "проверяем работу ExeVersionUpdater")]
		public void TestVersionUpdater()
		{
			ServiceContext.GetResultPath = () => "..\\..\\EtalonUpdates\\";
			var updater = VersionUpdaterFactory.GetUpdater();

			Assert.That(updater, Is.Not.Null);

			Assert.That(updater.GetVersionInfo(705, null), Is.Not.Null);
			Assert.That(updater.GetVersionInfo(705, null).VersionNumber, Is.EqualTo(1101));

			Assert.That(updater.GetVersionInfo(705, 716), Is.Not.Null);
			Assert.That(updater.GetVersionInfo(705, 716).VersionNumber, Is.EqualTo(716));

			Assert.That(updater.GetVersionInfo(705, 1317), Is.Not.Null);
			Assert.That(updater.GetVersionInfo(705, 1317).VersionNumber, Is.EqualTo(1101));

			Assert.That(updater.GetVersionInfo(1101, null), Is.Not.Null);
			Assert.That(updater.GetVersionInfo(1101, null).VersionNumber, Is.EqualTo(1161));

			Assert.That(updater.GetVersionInfo(1101, 1317), Is.Not.Null);
			Assert.That(updater.GetVersionInfo(1101, 1317).VersionNumber, Is.EqualTo(1161));

			Assert.That(updater.GetVersionInfo(1317, null), Is.Null);

			Assert.That(updater.GetVersionInfo(1317, 1318), Is.Null);

			Assert.That(updater.GetVersionInfo(705, 705), Is.Null);
			Assert.That(updater.GetVersionInfo(1299, 1299), Is.Null);
			Assert.That(updater.GetVersionInfo(1317, 1317), Is.Null);
		}

	}
}