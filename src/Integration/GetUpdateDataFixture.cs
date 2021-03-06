﻿using System;
using Castle.ActiveRecord;
using Common.MySql;
using Common.Tools;
using MySql.Data.MySqlClient;
using NUnit.Framework;
using PrgData.Common;
using PrgData.Common.AnalitFVersions;
using Test.Support;
using Test.Support.Logs;
using MySqlHelper = MySql.Data.MySqlClient.MySqlHelper;

namespace Integration
{
	[TestFixture]
	public class GetUpdateDataFixture : IntegrationFixture
	{
		private TestClient _client;
		private TestUser _user;

		[SetUp]
		public void Setup()
		{
			_client = TestClient.CreateNaked(session);

			_user = _client.Users[0];

			_client.Users.Each(u => {
				u.SendRejects = true;
				u.SendWaybills = true;
			});
			_user.Update();
			session.Flush();
		}

		[Test]
		public void Get_update_data_for_enabled_future_client()
		{
			var updateData = GetUpdateData(_user);
			Assert.That(updateData, Is.Not.Null);
			Assert.That(updateData.UserId, Is.EqualTo(_user.Id));
			Assert.That(updateData.ClientId, Is.EqualTo(_client.Id));
			Assert.That(updateData.ShortName, Is.Not.Null);
			Assert.That(updateData.ShortName, Is.Not.Empty);
			Assert.That(updateData.ShortName, Is.EqualTo(_client.Name));
			Assert.That(updateData.Disabled(), Is.False, "Пользователь отключен");
			Assert.That(updateData.ClientEnabled, Is.True);
			Assert.That(updateData.UserEnabled, Is.True);
			Assert.That(updateData.AFPermissionExists, Is.True);
		}

		[Test]
		public void Get_update_data_for_future_client_for_user_without_AF()
		{
			var _userWithoutAF = _client.CreateUser();
			var permissionAF = TestUserPermission.ByShortcut("AF");
			var afIndex = _userWithoutAF.AssignedPermissions.IndexOf(item => item.Id == permissionAF.Id);
			if (afIndex > -1) {
				_userWithoutAF.AssignedPermissions.RemoveAt(afIndex);
				_userWithoutAF.Update();
			}
			session.Flush();

			var updateData = GetUpdateData(_userWithoutAF);
			Assert.That(updateData, Is.Not.Null);
			Assert.That(updateData.UserId, Is.EqualTo(_userWithoutAF.Id));
			Assert.That(updateData.ClientId, Is.EqualTo(_client.Id));
			Assert.That(updateData.ShortName, Is.Not.Null);
			Assert.That(updateData.ShortName, Is.Not.Empty);
			Assert.That(updateData.ShortName, Is.EqualTo(_client.Name));
			Assert.That(updateData.Disabled(), Is.True, "Пользователь включен");
			Assert.That(updateData.ClientEnabled, Is.True);
			Assert.That(updateData.UserEnabled, Is.True);
			Assert.That(updateData.AFPermissionExists, Is.False);
			Assert.That(updateData.DisabledMessage(), Is.EqualTo("пользователю не разрешено обновлять AnalitF"));
		}

		[Test]
		public void Get_update_data_for_future_client_for_disabled_user()
		{
			TestUser disabledUser;
			disabledUser = _client.CreateUser(session);
			disabledUser.SendRejects = true;
			disabledUser.SendWaybills = true;
			disabledUser.Enabled = false;
			disabledUser.Update();
			session.Flush();

			var updateData = GetUpdateData(disabledUser);
			Assert.That(updateData, Is.Not.Null);
			Assert.That(updateData.UserId, Is.EqualTo(disabledUser.Id));
			Assert.That(updateData.ClientId, Is.EqualTo(_client.Id));
			Assert.That(updateData.ShortName, Is.Not.Null);
			Assert.That(updateData.ShortName, Is.Not.Empty);
			Assert.That(updateData.ShortName, Is.EqualTo(_client.Name));
			Assert.That(updateData.Disabled(), Is.True, "Пользователь включен");
			Assert.That(updateData.ClientEnabled, Is.True);
			Assert.That(updateData.UserEnabled, Is.False);
			Assert.That(updateData.AFPermissionExists, Is.True);
			Assert.That(updateData.DisabledMessage(), Is.EqualTo("пользователь отключен"));
		}

		[Test]
		public void Get_update_data_for_disabled_future_client()
		{
			var disabledClient = TestClient.CreateNaked(session);
			var disabledUser = disabledClient.Users[0];
			disabledUser.SendRejects = true;
			disabledUser.SendWaybills = true;
			disabledUser.Update();

			disabledClient.Status = ClientStatus.Off;
			disabledClient.Update();

			var updateData = GetUpdateData(disabledUser);
			Assert.That(updateData, Is.Not.Null);
			Assert.That(updateData.UserId, Is.EqualTo(disabledUser.Id));
			Assert.That(updateData.ClientId, Is.EqualTo(disabledClient.Id));
			Assert.That(updateData.ShortName, Is.Not.Null);
			Assert.That(updateData.ShortName, Is.Not.Empty);
			Assert.That(updateData.ShortName, Is.EqualTo(disabledClient.Name));
			Assert.That(updateData.Disabled(), Is.True, "Пользователь включен");
			Assert.That(updateData.ClientEnabled, Is.False);
			Assert.That(updateData.UserEnabled, Is.True);
			Assert.That(updateData.AFPermissionExists, Is.True);
			Assert.That(updateData.DisabledMessage(), Is.EqualTo("клиент отключен"));
		}

		private UpdateData GetUpdateData(TestUser user)
		{
			session.Flush();
			return UpdateHelper.GetUpdateData((MySqlConnection)session.Connection, user.Login);
		}

		[Test]
		public void Check_ON_flags_for_BuyingMatrix_and_MNN_for_future_client()
		{
			ServiceContext.GetResultPath = () => "..\\..\\Data\\EtalonUpdates\\";
			var updateData = GetUpdateData(_user);
			Assert.That(updateData, Is.Not.Null);
			updateData.ParseBuildNumber("6.0.0.1183");
			Assert.IsTrue(updateData.NeedUpdateToBuyingMatrix, "Неправильно сработало условие обновления на матрицу закупок");
			Assert.IsTrue(updateData.NeedUpdateToNewMNN, "Неправильно сработало условие обновления на МНН");

			updateData = GetUpdateData(_user);
			Assert.That(updateData, Is.Not.Null);
			updateData.ParseBuildNumber("6.0.0.1269");
			//Значения будут false, т.к. версия 1269 не требует обновления МНН и матрицы закупок
			Assert.IsFalse(updateData.NeedUpdateToBuyingMatrix, "Неправильно сработало условие обновления на матрицу закупок");
			Assert.IsFalse(updateData.NeedUpdateToNewMNN, "Неправильно сработало условие обновления на МНН");
		}

		[Test]
		public void Check_OFF_flags_for_BuyingMatrix_and_MNN_for_future_client()
		{
			ServiceContext.GetResultPath = () => "..\\..\\Data\\EtalonUpdates\\";
			var updateData = GetUpdateData(_user);
			Assert.That(updateData, Is.Not.Null);
			updateData.ParseBuildNumber("6.0.0.1269");
			Assert.IsFalse(updateData.NeedUpdateToBuyingMatrix, "Неправильно сработало условие обновления на матрицу закупок");
			Assert.IsFalse(updateData.NeedUpdateToNewMNN, "Неправильно сработало условие обновления на МНН");
		}

		[Test]
		public void Check_ON_flags_for_NewClients_for_future_client()
		{
			ServiceContext.GetResultPath = () => "..\\..\\Data\\EtalonUpdates\\";
			var updateData = GetUpdateData(_user);
			Assert.That(updateData, Is.Not.Null);
			updateData.ParseBuildNumber("6.0.0.1271");
			Assert.IsTrue(updateData.NeedUpdateToNewClientsWithLegalEntity, "Неправильно сработало условие обновления новых клиентов с юр лицами");
		}

		[Test]
		public void Check_OFF_flags_for_NewClients_for_future_client()
		{
			ServiceContext.GetResultPath = () => "..\\..\\Data\\EtalonUpdates\\";
			var updateData = GetUpdateData(_user);
			Assert.That(updateData, Is.Not.Null);
			updateData.ParseBuildNumber("6.0.0.1279");
			Assert.IsFalse(updateData.NeedUpdateToNewClientsWithLegalEntity, "Неправильно сработало условие обновления новых клиентов с юр лицами");
		}

		[Test]
		public void Check_need_update_if_client_updated_matrix()
		{
			ServiceContext.GetResultPath = () => "..\\..\\Data\\EtalonUpdates\\";
			var matrix = new TestMatrix();
			session.Save(matrix);
			_client.Settings.BuyingMatrix = matrix;
			session.Update(_client.Settings);
			session.Flush();

			var updateData = GetUpdateData(_user);
			updateData.ParseBuildNumber("6.0.0.1279");
			Assert.IsFalse(updateData.NeedUpdateToBuyingMatrix);
			matrix.MatrixUpdateTime = DateTime.Now.AddHours(-1);
			session.Update(matrix);

			updateData = GetUpdateData(_user);
			updateData.OldUpdateTime = DateTime.Now;
			updateData.ParseBuildNumber("6.0.0.1279");
			Assert.IsFalse(updateData.NeedUpdateToBuyingMatrix);
			updateData.OldUpdateTime = DateTime.Now.AddHours(-2);
			updateData.ParseBuildNumber("6.0.0.1279");
			Assert.IsTrue(updateData.NeedUpdateToBuyingMatrix);
			session.CreateSQLQuery("update farm.matrices set MatrixUpdateTime = null").ExecuteUpdate();
		}

		[Test(Description = "В общем случае нельзя запрашивать обновление с устаревшей версией")]
		public void Check_old_version_after_new_version()
		{
			ServiceContext.GetResultPath = () => "..\\..\\Data\\EtalonUpdates\\";
			var updateData = GetUpdateData(_user);
			updateData.KnownBuildNumber = 1279;
			try {
				updateData.ParseBuildNumber("6.0.0.1261");
				Assert.Fail("Не сработало исключение об актуальности версии");
			}
			catch (UpdateException updateException) {
				Assert.That(updateException.Addition, Is.StringStarting("Попытка обновить устаревшую версию").IgnoreCase);
				Assert.That(updateException.Message, Is.StringStarting("Доступ закрыт").IgnoreCase);
				Assert.That(updateException.Error, Is.StringStarting("Используемая версия программы не актуальна, необходимо обновление до версии №1279").IgnoreCase);
			}
		}

		[Test(Description = "Версия программы не проверяется, если установлен параметр NetworkPriceId")]
		public void Check_old_version_after_new_version_with_NetworkPriceId()
		{
			ServiceContext.GetResultPath = () => "..\\..\\Data\\EtalonUpdates\\";
			var updateData = GetUpdateData(_user);
			//Устанавливаем любой прайс-лист, в данном случае это прайс поставщика Инфорум
			updateData.NetworkPriceId = 2647;
			updateData.KnownBuildNumber = 1279;

			updateData.ParseBuildNumber("6.0.0.1261");
			Assert.That(updateData.BuildNumber, Is.EqualTo(1261));
		}

		private void DeleteAllLogs(uint userId)
		{
			TestAnalitFUpdateLog.DeleteAll("UserId = {0}".Format(userId));
		}

		private void CheckPreviousRequestOnFirst(string userName, uint userId)
		{
			try {
				var updateData = UpdateHelper.GetUpdateData((MySqlConnection)session.Connection, userName);
				Assert.That(updateData, Is.Not.Null);
				Assert.That(updateData.PreviousRequest, Is.Not.Null);
				Assert.That(updateData.PreviousRequest.UpdateId, Is.Null);
			}
			finally {
				DeleteAllLogs(userId);
			}
		}

		[Test(Description = "Проверяем установку свойства PreviousRequest при первом обращении для Future")]
		public void CheckPreviousRequestOnFirstByFuture()
		{
			CheckPreviousRequestOnFirst(_user.Login, _user.Id);
		}

		[Test(Description = "Проверяем установку свойства PreviousRequest при существовании старых записей в AnalitFUpdates для Future")]
		public void CheckPreviousRequestWithOldRequestByFuture()
		{
			var userId = _user.Id;
			var log = new TestAnalitFUpdateLog {
				UserId = userId,
				RequestTime = DateTime.Now.AddDays(-2),
				UpdateType = (TestRequestType)RequestType.GetData,
				Commit = false
			};
			log.Save();
			log = new TestAnalitFUpdateLog {
				UserId = userId,
				RequestTime = DateTime.Now.AddDays(-2),
				UpdateType = (TestRequestType)RequestType.GetCumulative,
				Commit = true
			};
			log.Save();
			log = new TestAnalitFUpdateLog {
				UserId = userId,
				RequestTime = DateTime.Now,
				UpdateType = (TestRequestType)RequestType.SendWaybills,
				Commit = true
			};
			log.Save();

			try {
				var updateData = GetUpdateData(_user);
				Assert.That(updateData, Is.Not.Null);
				Assert.That(updateData.PreviousRequest, Is.Not.Null);
				Assert.That(updateData.PreviousRequest.UpdateId, Is.Null);
			}
			finally {
				DeleteAllLogs(userId);
			}
		}

		[Test(Description = "Проверяем установку свойства PreviousRequest при существовании старых записей в AnalitFUpdates для Future")]
		public void CheckPreviousRequestWithOldRequestExistsByFuture()
		{
			uint userId = _user.Id;
			TestAnalitFUpdateLog log;
			log = new TestAnalitFUpdateLog {
				UserId = userId,
				RequestTime = DateTime.Now.AddDays(-2),
				UpdateType = (TestRequestType)RequestType.GetData,
				Commit = false
			};
			log.Save();
			log = new TestAnalitFUpdateLog {
				UserId = userId,
				RequestTime = DateTime.Now.AddDays(-2),
				UpdateType = (TestRequestType)RequestType.GetCumulative,
				Commit = true
			};
			log.Save();
			log = new TestAnalitFUpdateLog {
				UserId = userId,
				RequestTime = DateTime.Now,
				UpdateType = (TestRequestType)RequestType.GetCumulative,
				Commit = true
			};
			log.Save();
			var last = new TestAnalitFUpdateLog {
				UserId = userId,
				RequestTime = DateTime.Now,
				UpdateType = (TestRequestType)RequestType.GetDocs,
				Commit = true
			};
			last.Save();

			try {
				var updateData = GetUpdateData(_user);
				Assert.That(updateData, Is.Not.Null);
				Assert.That(updateData.PreviousRequest, Is.Not.Null);
				Assert.That(updateData.PreviousRequest.UpdateId, Is.Not.Null);
				Assert.That(updateData.PreviousRequest.UpdateId.Value, Is.EqualTo(log.Id));
				Assert.That(updateData.PreviousRequest.RequestType, Is.EqualTo((RequestType)log.UpdateType));
				Assert.That(log.RequestTime.Subtract(updateData.PreviousRequest.RequestTime).TotalSeconds, Is.LessThan(1));
				Assert.That(updateData.PreviousRequest.Commit, Is.EqualTo(log.Commit));
			}
			finally {
				DeleteAllLogs(userId);
			}
		}

		[Test(Description = "проверяем методы для работы с именами подготовленными файлами")]
		public void CheckResultPaths()
		{
			var updateData = UpdateHelper.GetUpdateData((MySqlConnection)session.Connection, _user.Login);
			Assert.That(updateData, Is.Not.Null);

			Assert.That(
				() => updateData.GetReclameFile(),
				Throws.InstanceOf<Exception>()
					.And.Property("Message").EqualTo("Не установлено свойство ResultPath"));
			Assert.That(
				() => updateData.GetOrdersFile(),
				Throws.InstanceOf<Exception>()
					.And.Property("Message").EqualTo("Не установлено свойство ResultPath"));
			Assert.That(
				() => updateData.GetPreviousFile(),
				Throws.InstanceOf<Exception>()
					.And.Property("Message").EqualTo("Не установлено свойство ResultPath"));
			Assert.That(
				() => updateData.GetCurrentTempFile(),
				Throws.InstanceOf<Exception>()
					.And.Property("Message").EqualTo("Не установлено свойство ResultPath"));
			Assert.That(
				() => updateData.GetCurrentFile(3),
				Throws.InstanceOf<Exception>()
					.And.Property("Message").EqualTo("Не установлено свойство ResultPath"));
			Assert.That(updateData.GetOldFileMask(), Is.EqualTo(String.Format("{0}_*.zip", _user.Id)));

			updateData.ResultPath = "result\\";

			Assert.That(updateData.GetReclameFile(), Is.EqualTo(String.Format("{0}r{1}.zip", updateData.ResultPath, _user.Id)));
			Assert.That(updateData.GetOrdersFile(), Is.EqualTo(String.Format("{0}Orders{1}.zip", updateData.ResultPath, _user.Id)));
			Assert.That(updateData.GetCurrentFile(3), Is.EqualTo(String.Format("{0}{1}_{2}.zip", updateData.ResultPath, _user.Id, 3)));
			Assert.That(updateData.GetCurrentTempFile(), Is.StringStarting(String.Format("{0}{1}_{2}", updateData.ResultPath, _user.Id, DateTime.Now.ToString("yyyyMMddHHmm"))));
			Assert.That(updateData.GetOldFileMask(), Is.EqualTo(String.Format("{0}_*.zip", _user.Id)));

			Assert.That(
				() => updateData.GetPreviousFile(),
				Throws.InstanceOf<Exception>()
					.And.Property("Message").EqualTo("Отсутствует предыдущее неподтвержденное обновление"));

			updateData.PreviousRequest.UpdateId = 333;
			Assert.That(updateData.GetPreviousFile(), Is.EqualTo(String.Format("{0}{1}_{2}.zip", updateData.ResultPath, _user.Id, 333)));
		}

		[Test(Description = "Проверяем корректность доступности механизма подтверждения пользовательского сообщения")]
		public void CheckFlagIsConfrimUserMessage()
		{
			var updateData = GetUpdateData(_user);

			Assert.That(updateData.IsConfirmUserMessage(), Is.EqualTo(false), "Для неопределенной версии доступен механизм");

			updateData.KnownBuildNumber = 1299;
			Assert.That(updateData.IsConfirmUserMessage(), Is.EqualTo(false), "Для версии 1299 доступен механизм");
			updateData.BuildNumber = 1299;
			Assert.That(updateData.IsConfirmUserMessage(), Is.EqualTo(false), "Для версии 1299 доступен механизм");

			updateData.BuildNumber = null;
			updateData.KnownBuildNumber = 1300;
			Assert.That(updateData.IsConfirmUserMessage(), Is.EqualTo(true), "Не доступен механизм для версии > 1299");

			updateData.BuildNumber = 1300;
			updateData.KnownBuildNumber = null;
			Assert.That(updateData.IsConfirmUserMessage(), Is.EqualTo(true), "Не доступен механизм для версии > 1299");

			updateData.BuildNumber = 1301;
			updateData.KnownBuildNumber = 1300;
			Assert.That(updateData.IsConfirmUserMessage(), Is.EqualTo(true), "Не доступен механизм для версии > 1299");
		}

		[Test(Description = "проверяем корректное чтение TargetVersion")]
		public void CheckReadTargetVersion()
		{
			MySqlHelper.ExecuteNonQuery(
				(MySqlConnection)session.Connection,
				"update Customers.Users set TargetVersion = null where Id = ?UserId",
				new MySqlParameter("?UserId", _user.Id));

			var updateData = GetUpdateData(_user);
			Assert.That(updateData.TargetVersion, Is.Null);

			const int targetVersion = 1300;
			MySqlHelper.ExecuteNonQuery(
				(MySqlConnection)session.Connection,
				"update Customers.Users set TargetVersion = ?TargetVersion where Id = ?UserId",
				new MySqlParameter("?UserId", _user.Id),
				new MySqlParameter("?TargetVersion", targetVersion));
			updateData = GetUpdateData(_user);
			Assert.That(updateData.TargetVersion, Is.EqualTo(targetVersion));
		}

		[Test(Description = "Проверяем результат функции EnableUpdate при различных значениях TargetVersion")]
		public void CheckSetEnableUpdate()
		{
			ServiceContext.GetResultPath = () => "..\\..\\Data\\EtalonUpdates\\";

			var updateData = GetUpdateData(_user);

			//Если все в null и BuildNumber не установлен, то неизвестно, что на что обновлять, поэтому обновление запрещено
			updateData.TargetVersion = null;
			updateData.BuildNumber = null;
			Assert.That(updateData.EnableUpdate(), Is.EqualTo(false));

			//Если BuildNumber установлен в 1271, то для нее выложено автообновление в Data\EtalonUpdates
			//т.к. TargetVersion = null, то автообновление разрешено
			updateData.ParseBuildNumber("6.0.0.1271");
			Assert.That(updateData.EnableUpdate(), Is.EqualTo(true));

			//Если TargetVersion равно BuildNumber, то автообновлять не на что
			updateData.TargetVersion = 1271;
			Assert.That(updateData.EnableUpdate(), Is.EqualTo(false));

			//TargetVersion соответствует той версии, что выложена в автообновление,
			//поэтому автообновление разрешено
			updateData.TargetVersion = 1317;
			Assert.That(updateData.EnableUpdate(), Is.EqualTo(true));

			//TargetVersion меньше той версии, что выложена в автообновление,
			//поэтому автообновление не разрешено
			updateData.TargetVersion = 1275;
			Assert.That(updateData.EnableUpdate(), Is.EqualTo(false));
		}

		[Test(Description = "Проверяем условие обновление на версию, поддерживающую столбец RetailVitallyImportant в Core")]
		public void CheckNeedUpdateForRetailVitallyImportant()
		{
			ServiceContext.GetResultPath = () => "..\\..\\Data\\EtalonUpdates\\";
			var updateData = GetUpdateData(_user);
			Assert.That(updateData, Is.Not.Null);
			updateData.ParseBuildNumber("6.0.0.1183");
			Assert.That(updateData.NeedUpdateForRetailVitallyImportant(), Is.False, "Если обновление не выложено, то обновлять нельзя");

			updateData = GetUpdateData(_user);
			Assert.That(updateData, Is.Not.Null);
			updateData.ParseBuildNumber("6.0.0.1403");
			Assert.That(updateData.NeedUpdateForRetailVitallyImportant(), Is.False, "Если обновление не выложено, то обновлять нельзя");

			updateData = GetUpdateData(_user);
			Assert.That(updateData, Is.Not.Null);
			updateData.ParseBuildNumber("6.0.0.1385");
			updateData.UpdateExeVersionInfo = new VersionInfo(1403);
			Assert.That(updateData.NeedUpdateForRetailVitallyImportant(), Is.False, "Если обновление не на версию старше 1403, то обновлять нельзя");

			updateData = GetUpdateData(_user);
			Assert.That(updateData, Is.Not.Null);
			updateData.ParseBuildNumber("6.0.0.1405");
			Assert.That(updateData.NeedUpdateForRetailVitallyImportant(), Is.False, "Если обновление текущая версия > 1403, то незачем обновлять");

			updateData = GetUpdateData(_user);
			Assert.That(updateData, Is.Not.Null);
			updateData.ParseBuildNumber("6.0.0.1405");
			updateData.UpdateExeVersionInfo = new VersionInfo(1407);
			Assert.That(updateData.NeedUpdateForRetailVitallyImportant(), Is.False, "Если обновление текущая версия > 1403, то незачем обновлять");

			updateData = GetUpdateData(_user);
			Assert.That(updateData, Is.Not.Null);
			updateData.ParseBuildNumber("6.0.0.1385");
			updateData.UpdateExeVersionInfo = new VersionInfo(1405);
			Assert.That(updateData.NeedUpdateForRetailVitallyImportant(), Is.True, "Должны обновиться с версии 1385 на 1405 и выше");

			updateData = GetUpdateData(_user);
			Assert.That(updateData, Is.Not.Null);
			updateData.ParseBuildNumber("6.0.0.1403");
			updateData.UpdateExeVersionInfo = new VersionInfo(1405);
			Assert.That(updateData.NeedUpdateForRetailVitallyImportant(), Is.True, "Должны обновиться с версии 1403 на 1405 и выше");
		}

		[Test(Description = "Проверяем корректность доступности механизма расписания обновлений AnalitF")]
		public void CheckFlagAllowAnalitFSchedule()
		{
			var updateData = GetUpdateData(_user);

			Assert.That(updateData.AllowAnalitFSchedule, Is.EqualTo(false), "В базе флаг по умолчанию должен быть сброшен");
			Assert.That(updateData.SupportAnalitFSchedule, Is.EqualTo(false), "Для неопределенной версии доступен механизм");

			updateData.KnownBuildNumber = 1506;
			Assert.That(updateData.SupportAnalitFSchedule, Is.EqualTo(true), "Должен быть доступен механизм");

			updateData.KnownBuildNumber = null;
			updateData.BuildNumber = 1506;
			Assert.That(updateData.SupportAnalitFSchedule, Is.EqualTo(true), "Должен быть доступен механизм");

			MySqlHelper.ExecuteNonQuery(
				(MySqlConnection)session.Connection,
				"update UserSettings.RetClientsSet set AllowAnalitFSchedule = 1 where ClientCode = ?clientId",
				new MySqlParameter("?clientId", _user.Client.Id));
			updateData = GetUpdateData(_user);
			Assert.That(updateData.AllowAnalitFSchedule, Is.EqualTo(true), "Флаг должен быть поднят");
		}
	}
}