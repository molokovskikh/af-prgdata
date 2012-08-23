﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Common.Models;
using Common.Tools;
using Inforoom.Common;
using Integration.BaseTests;
using MySql.Data.MySqlClient;
using NUnit.Framework;
using PrgData;
using PrgData.Common;
using PrgData.Common.AnalitFVersions;
using PrgData.Common.Repositories;
using Test.Support;

namespace Integration
{
	[TestFixture]
	public class AnalitFVersionsFixture : PrepareDataFixture
	{
		private TestUser _user;

		[SetUp]
		public void Setup()
		{
			_user = CreateUser();

			InsertEtalonVersions();

			RegisterLogger();
		}

		[TearDown]
		public void TearDown()
		{
			CheckForErrors();
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
(1315, 1317),
(1809, 1823);
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
			var dirInfo = new DirectoryInfo("..\\..\\Data\\EtalonUpdates\\Updates");
			var releaseInfos = dirInfo.GetDirectories("Release*");
			Assert.That(releaseInfos.Length, Is.EqualTo(8));
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

			ServiceContext.GetResultPath = () => "..\\..\\Data\\EtalonUpdates\\";
			var factoryInfos = VersionUpdaterFactory.GetVersionInfos();
			Assert.That(factoryInfos.Count, Is.EqualTo(infos.Count));
		}

		[Test(Description = "проверяем работу ExeVersionUpdater")]
		public void TestVersionUpdater()
		{
			ServiceContext.GetResultPath = () => "..\\..\\Data\\EtalonUpdates\\";
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

		[Test(Description = "проверяем работу ExeVersionUpdater для 'сетевой' версии")]
		public void TestVersionUpdaterForNetwork()
		{
			ServiceContext.GetResultPath = () => "..\\..\\Data\\NetworkUpdates\\";
			var updater = VersionUpdaterFactory.GetUpdater();

			Assert.That(updater, Is.Not.Null);

			Assert.That(updater.VersionInfos, Is.Not.Null);
			Assert.That(updater.VersionInfos.Count, Is.EqualTo(1));

			var info = updater.VersionInfos[0];
			Assert.That(info.VersionNumber, Is.EqualTo(1380));
			Assert.That(info.ExeVersionNumber(), Is.EqualTo(1380));
			Assert.That(info.Folder, Is.StringEnding("Release1380"));
		}

		private void ResetUpdatesFolder()
		{
			var files = Directory.GetFiles(ServiceContext.GetResultPath());
			files.Each(file => File.Delete(file));

			var dirs = Directory.GetDirectories(ServiceContext.GetResultPath());
			dirs.Each(dir =>
						{
							var info = new DirectoryInfo(dir);
							if (!info.Name.Equals("Updates", StringComparison.OrdinalIgnoreCase))
								Directory.Delete(dir, true);
						});
		}

		[Test(Description = "Проверка подготовки данных с обновлением exe для сетевых версий")]
		public void CheckGetUserDataWithExeUpdateOnNetworkVersion()
		{
			ServiceContext.GetResultPath = () => "..\\..\\Data\\NetworkUpdates\\";
			
			var extractFolder = Path.Combine(Path.GetFullPath(ServiceContext.GetResultPath()), "ExtractZip");
			if (Directory.Exists(extractFolder))
				Directory.Delete(extractFolder, true);
			Directory.CreateDirectory(extractFolder);

			var appVersion = "1.1.1.1378";

			MySqlHelper.ExecuteNonQuery(
				Settings.ConnectionString(),
				@"
delete from usersettings.AnalitFVersionRules
where
	SourceVersion = 1378
and DestinationVersion = 1380;
insert into usersettings.AnalitFVersionRules
(SourceVersion, DestinationVersion)
values
(1378, 1380);
update Customers.Users
set
  TargetVersion = null
where
  Id = ?UserId;
"
				,
				new MySqlParameter("?UserId", _user.Id));

			try
			{
				SetCurrentUser(_user.Login);

				var service = new PrgDataEx();
				var responce = service.GetUserDataWithPriceCodes(DateTime.Now, true, appVersion, 50, UniqueId, "", "", false, null, null);

				Assert.That(responce, Is.StringStarting("URL=").IgnoreCase);
				var updateId = ParseUpdateId(responce);

				var updateFile = Path.Combine(ServiceContext.GetResultPath(), "{0}_{1}.zip".Format(_user.Id, updateId));
				Assert.That(File.Exists(updateFile), Is.True, "Не найден файл с подготовленными данными");

				ArchiveHelper.Extract(updateFile, "*.*", extractFolder);

				var exeFolder = Path.Combine(extractFolder, "Exe");
				Assert.That(Directory.Exists(exeFolder), Is.True, "На найден каталог с обновлением exe");

				var rootFiles = Directory.GetFiles(exeFolder);
				Assert.That(rootFiles.Length, Is.EqualTo(2));
				Assert.That(
					rootFiles.Any(file => file.EndsWith("AnalitFService.exe", StringComparison.OrdinalIgnoreCase)),
					Is.True,
					"Не найден файл с сервисом");
				Assert.That(
					rootFiles.Any(file => file.EndsWith("testRoot.txt", StringComparison.OrdinalIgnoreCase)),
					Is.True,
					"Не найден текстовый файл");

				var analitFFolder = Path.Combine(exeFolder, "AnalitF");
				Assert.That(Directory.Exists(analitFFolder), Is.True, "На найден каталог с обновлением AnalitF");

				var analitFFiles = Directory.GetFiles(analitFFolder);
				Assert.That(analitFFiles.Length, Is.EqualTo(2));
				Assert.That(
					analitFFiles.Any(file => file.EndsWith("AnalitF.exe", StringComparison.OrdinalIgnoreCase)),
					Is.True,
					"Не найден файл с AnalitF");
				Assert.That(
					analitFFiles.Any(file => file.EndsWith("testSub.txt", StringComparison.OrdinalIgnoreCase)),
					Is.True,
					"Не найден текстовый файл");

			}
			finally
			{
				ResetUpdatesFolder();
			}
		}

		private void InternalUpdateOnVersion(uint fromVersion, uint toVersion)
		{
			ServiceContext.GetResultPath = () => "..\\..\\Data\\EtalonUpdates\\";
			
			var extractFolder = Path.Combine(Path.GetFullPath(ServiceContext.GetResultPath()), "ExtractZip");
			if (Directory.Exists(extractFolder))
				Directory.Delete(extractFolder, true);
			Directory.CreateDirectory(extractFolder);

			var appVersion = "1.1.1." + fromVersion;

			MySqlHelper.ExecuteNonQuery(
				Settings.ConnectionString(),
				@"
update Customers.Users
set
  TargetVersion = ?toVersion
where
  Id = ?UserId;
"
				,
				new MySqlParameter("?UserId", _user.Id),
				new MySqlParameter("?toVersion", toVersion));

			try
			{
				SetCurrentUser(_user.Login);

				var service = new PrgDataEx();
				var responce = service.GetUserDataWithPriceCodes(DateTime.Now, true, appVersion, 50, UniqueId, "", "", false, null, null);

				var updateId = ShouldBeSuccessfull(responce);

				var updateFile = Path.Combine(ServiceContext.GetResultPath(), "{0}_{1}.zip".Format(_user.Id, updateId));
				Assert.That(File.Exists(updateFile), Is.True, "Не найден файл с подготовленными данными");

				ArchiveHelper.Extract(updateFile, "*.*", extractFolder);

				var exeFolder = Path.Combine(extractFolder, "Exe");
				Assert.That(Directory.Exists(exeFolder), Is.True, "На найден каталог с обновлением exe");

				var rootFiles = Directory.GetFiles(exeFolder);
				Assert.That(rootFiles.Length, Is.GreaterThan(0), "В директории Exe не найдены файлы");
				Assert.That(
					rootFiles.Any(file => file.EndsWith("AnalitF.exe", StringComparison.OrdinalIgnoreCase)),
					Is.True,
					"Не найден файл с обновлением программы");
			}
			finally
			{
				ResetUpdatesFolder();
			}
		}

		[Test(Description = "Проверка подготовки данных с обновлением exe на версию 1317")]
		public void CheckGetUserDataWithExeUpdateOn1317()
		{
			InternalUpdateOnVersion(1315, 1317);
		}

		[Test(Description = "Проверка подготовки данных с обновлением exe на версию 1823")]
		public void CheckGetUserDataWithExeUpdateOn1823()
		{
			InternalUpdateOnVersion(1809, 1823);
		}

		[Test(Description = "получаем ошибку при несуществовании папки Exe")]
		public void GetErrorOnNonExistsExeFolder()
		{
			var testFolder = Path.Combine(ServiceContext.GetResultPath(), "Release1800");

			if (!Directory.Exists(testFolder))
				Directory.CreateDirectory(testFolder);

			try {

				var exception = Assert.Throws<Exception>(() => {var info = new VersionInfo(testFolder);});
				Assert.That(exception.Message, Is.StringStarting("Не найдена вложенная директория Exe"));

			}
			finally {
				if (Directory.Exists(testFolder))
					Directory.Delete(testFolder);
			}
		}

		private void CheckExeFolder(Action<string, string> action)
		{
			var testFolder = Path.Combine(ServiceContext.GetResultPath(), "Release1800");

			if (!Directory.Exists(testFolder))
				Directory.CreateDirectory(testFolder);

			var exeFolder = Path.Combine(testFolder, "Exe");
			if (!Directory.Exists(exeFolder))
				Directory.CreateDirectory(exeFolder);

			try {

				action(testFolder, exeFolder);

			}
			finally {
				if (Directory.Exists(testFolder))
					Directory.Delete(testFolder, true);
			}
			
		}

		[Test(Description = "получаем ошибку при несуществовании файла с exe в папке Exe")]
		public void GetErrorOnNonExistsExeFile()
		{
			CheckExeFolder((testFolder, exeFolder) => {
				var exception = Assert.Throws<Exception>(() => {var info = new VersionInfo(testFolder);});
				Assert.That(exception.Message, Is.StringStarting("Во вложенной директории Exe не найден файл с расширением"));
			});
		}

		[Test(Description = "получаем ошибку при нескольких файлов с exe в папке Exe")]
		public void GetErrorOnSameExistsExeFile()
		{
			CheckExeFolder((testFolder, exeFolder) => {
				File.WriteAllText(Path.Combine(exeFolder, "1.exe"), "1.exe");
				File.WriteAllText(Path.Combine(exeFolder, "2.exe"), "2.exe");

				var exception = Assert.Throws<Exception>(() => {var info = new VersionInfo(testFolder);});
				Assert.That(exception.Message, Is.StringStarting("Во вложенной директории Exe найдено более одного файла с расширением .exe"));
			});
		}

		[Test(Description = "получаем ошибку при чтении версии файла с exe в папке Exe")]
		public void GetErrorOnReadVersionExeFile()
		{
			CheckExeFolder((testFolder, exeFolder) => {
				File.WriteAllText(Path.Combine(exeFolder, "1.exe"), "1.exe");

				var exception = Assert.Throws<Exception>(() => {var info = new VersionInfo(testFolder);});
				//исключение при чтении версии файла происходит, только если файла не существует
				//Если в файле нет информации о версии, то вернется структура с версией = 0
				Assert.That(exception.Message, Is.StringStarting("Не совпадают номера версий в названии папки"));
			});
		}

	}
}