using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using Castle.ActiveRecord;
using Castle.MicroKernel.Registration;
using Common.Models;
using Common.Models.Repositories;
using Common.Models.Tests.Repositories;
using Common.Tools;
using MySql.Data.MySqlClient;
using NHibernate;
using NUnit.Framework;
using PrgData.Common;
using PrgData.Common.Model;
using PrgData.Common.Repositories;
using SmartOrderFactory.Domain;
using Test.Support;
using Test.Support.Logs;

namespace Integration
{
	[TestFixture]
	public class GetUpdateDataFixture
	{

		TestClient _client;
		TestUser _user;
		TestUser _userWithoutAF;

		TestOldClient _oldClient;
		TestOldClient _oldClientWithoutAF;

		[TestFixtureSetUp]
		public void FixtureSetUp()
		{
			Test.Support.Setup.Initialize();
			ContainerInitializer.InitializerContainerForTests(new Assembly[] { typeof(SmartOrderRule).Assembly, typeof(AnalitFVersionRule).Assembly });
			IoC.Container.Register(
				Component.For<IVersionRuleRepository>().ImplementedBy<VersionRuleRepository>()
				);

			using (var transaction = new TransactionScope())
			{
				_client = TestClient.CreateSimple();
				_user = _client.Users[0];

				var permission = TestUserPermission.ByShortcut("AF");
				_client.Users.Each(u =>
				{
					u.AssignedPermissions.Add(permission);
					u.SendRejects = true;
					u.SendWaybills = true;
				});
				_user.Update();

				_userWithoutAF = _client.CreateUser();

				_oldClient = TestOldClient.CreateTestClient();
				_oldClientWithoutAF = TestOldClient.CreateTestClient();

				var session = ActiveRecordMediator.GetSessionFactoryHolder().CreateSession(typeof (ActiveRecordBase));
				try
				{
					session.CreateSQLQuery(@"
insert into usersettings.AssignedPermissions (PermissionId, UserId) values (:permissionid, :userid)")
						.SetParameter("permissionid", permission.Id)
						.SetParameter("userid", _oldClient.Users[0].Id)
						.ExecuteUpdate();
				}
				finally
				{
					ActiveRecordMediator.GetSessionFactoryHolder().ReleaseSession(session);
				}

			}
		}
	
		[Test]
		public void Get_update_data_for_enabled_old_client()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				var updateData = UpdateHelper.GetUpdateData(connection, _oldClient.Users[0].OSUserName);
				Assert.That(updateData, Is.Not.Null);
				Assert.That(updateData.UserId, Is.EqualTo(_oldClient.Users[0].Id));
				Assert.That(updateData.ClientId, Is.EqualTo(_oldClient.Id));
				Assert.That(updateData.ShortName, Is.Not.Null);
				Assert.That(updateData.ShortName, Is.Not.Empty);
				Assert.That(updateData.ShortName, Is.EqualTo(_oldClient.ShortName));
				Assert.IsFalse(updateData.Disabled(), "Пользотель отключен");
			}
		}

		[Test]
		public void Get_update_data_for_disabled_old_client()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				var updateData = UpdateHelper.GetUpdateData(connection, _oldClientWithoutAF.Users[0].OSUserName);
				Assert.That(updateData, Is.Not.Null);
				Assert.That(updateData.UserId, Is.EqualTo(_oldClientWithoutAF.Users[0].Id));
				Assert.That(updateData.ClientId, Is.EqualTo(_oldClientWithoutAF.Id));
				Assert.That(updateData.ShortName, Is.Not.Null);
				Assert.That(updateData.ShortName, Is.Not.Empty);
				Assert.That(updateData.ShortName, Is.EqualTo(_oldClientWithoutAF.ShortName));
				Assert.IsTrue(updateData.Disabled(), "Пользотель включен");
			}
		}

		[Test]
		public void Get_update_data_for_enabled_future_client()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				Assert.That(updateData, Is.Not.Null);
				Assert.That(updateData.UserId, Is.EqualTo(_user.Id));
				Assert.That(updateData.ClientId, Is.EqualTo(_client.Id));
				Assert.That(updateData.ShortName, Is.Not.Null);
				Assert.That(updateData.ShortName, Is.Not.Empty);
				Assert.That(updateData.ShortName, Is.EqualTo(_client.Name));
				Assert.IsFalse(updateData.Disabled(), "Пользотель отключен");
			}
		}

		[Test]
		public void Get_update_data_for_disabled_future_client()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				var updateData = UpdateHelper.GetUpdateData(connection, _userWithoutAF.Login);
				Assert.That(updateData, Is.Not.Null);
				Assert.That(updateData.UserId, Is.EqualTo(_userWithoutAF.Id));
				Assert.That(updateData.ClientId, Is.EqualTo(_client.Id));
				Assert.That(updateData.ShortName, Is.Not.Null);
				Assert.That(updateData.ShortName, Is.Not.Empty);
				Assert.That(updateData.ShortName, Is.EqualTo(_client.Name));
				Assert.IsTrue(updateData.Disabled(), "Пользотель включен");
			}
		}

		[Test]
		public void Check_ON_flags_for_BuyingMatrix_and_MNN_for_old_client()
		{
			Check_ON_flags_for_BuyingMatrix_and_MNN(_oldClient.Users[0].OSUserName);
		}

		[Test]
		public void Check_ON_flags_for_BuyingMatrix_and_MNN_for_future_client()
		{
			Check_ON_flags_for_BuyingMatrix_and_MNN(_user.Login);
		}

		[Test]
		public void Check_OFF_flags_for_BuyingMatrix_and_MNN_for_old_client()
		{
			Check_OFF_flags_for_BuyingMatrix_and_MNN(_oldClient.Users[0].OSUserName);
		}

		[Test]
		public void Check_OFF_flags_for_BuyingMatrix_and_MNN_for_future_client()
		{
			Check_OFF_flags_for_BuyingMatrix_and_MNN(_user.Login);
		}

		[Test]
		public void Check_ON_flags_for_NewClients_for_future_client()
		{
			Check_ON_flags_for_NewClients(_user.Login);
		}

		[Test]
		public void Check_OFF_flags_for_NewClients_for_future_client()
		{
			Check_OFF_flags_for_NewClients(_user.Login);
		}

		private void Check_ON_flags_for_BuyingMatrix_and_MNN(string login)
		{
			ServiceContext.GetResultPath = () => "..\\..\\Data\\EtalonUpdates\\";
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				var updateData = UpdateHelper.GetUpdateData(connection, login);
				Assert.That(updateData, Is.Not.Null);
				updateData.ParseBuildNumber("6.0.0.1183");
				Assert.IsTrue(updateData.NeedUpdateToBuyingMatrix, "Неправильно сработало условие обновления на матрицу закупок");
				Assert.IsTrue(updateData.NeedUpdateToNewMNN, "Неправильно сработало условие обновления на МНН");

				updateData = UpdateHelper.GetUpdateData(connection, login);
				Assert.That(updateData, Is.Not.Null);
				updateData.ParseBuildNumber("6.0.0.1269");
				//Значения будут false, т.к. версия 1269 не требует обновления МНН и матрицы закупок
				Assert.IsFalse(updateData.NeedUpdateToBuyingMatrix, "Неправильно сработало условие обновления на матрицу закупок");
				Assert.IsFalse(updateData.NeedUpdateToNewMNN, "Неправильно сработало условие обновления на МНН");
			}
		}

		private void Check_OFF_flags_for_BuyingMatrix_and_MNN(string login)
		{
			ServiceContext.GetResultPath = () => "..\\..\\Data\\EtalonUpdates\\";
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				var updateData = UpdateHelper.GetUpdateData(connection, login);
				Assert.That(updateData, Is.Not.Null);
				updateData.ParseBuildNumber("6.0.0.1269");
				Assert.IsFalse(updateData.NeedUpdateToBuyingMatrix, "Неправильно сработало условие обновления на матрицу закупок");
				Assert.IsFalse(updateData.NeedUpdateToNewMNN, "Неправильно сработало условие обновления на МНН");
			}
		}

		private void Check_ON_flags_for_NewClients(string login)
		{
			ServiceContext.GetResultPath = () => "..\\..\\Data\\EtalonUpdates\\";
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				var updateData = UpdateHelper.GetUpdateData(connection, login);
				Assert.That(updateData, Is.Not.Null);
				updateData.ParseBuildNumber("6.0.0.1271");
				Assert.IsTrue(updateData.NeedUpdateToNewClientsWithLegalEntity, "Неправильно сработало условие обновления новых клиентов с юр лицами");
			}
		}

		private void Check_OFF_flags_for_NewClients(string login)
		{
			ServiceContext.GetResultPath = () => "..\\..\\Data\\EtalonUpdates\\";
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				var updateData = UpdateHelper.GetUpdateData(connection, login);
				Assert.That(updateData, Is.Not.Null);
				updateData.ParseBuildNumber("6.0.0.1279");
				Assert.IsFalse(updateData.NeedUpdateToNewClientsWithLegalEntity, "Неправильно сработало условие обновления новых клиентов с юр лицами");
			}
		}

		[Test(Description = "В общем случае нельзя запрашивать обновление с устаревшей версией")]
		public void Check_old_version_after_new_version()
		{
			var login = _user.Login;

			ServiceContext.GetResultPath = () => "..\\..\\Data\\EtalonUpdates\\";
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				var updateData = UpdateHelper.GetUpdateData(connection, login);
				updateData.KnownBuildNumber = 1279;
				try
				{
					updateData.ParseBuildNumber("6.0.0.1261");
					Assert.Fail("Не сработало исключение об актуальности версии");
				}
				catch (UpdateException updateException)
				{
					Assert.That(updateException.Addition, Is.StringStarting("Попытка обновить устаревшую версию").IgnoreCase);
					Assert.That(updateException.Message, Is.StringStarting("Доступ закрыт").IgnoreCase);
					Assert.That(updateException.Error, Is.StringStarting("Используемая версия программы не актуальна, необходимо обновление до версии №1279").IgnoreCase);
				}
			}
		}

		[Test(Description = "Версия программы не проверяется, если установлен параметр NetworkPriceId")]
		public void Check_old_version_after_new_version_with_NetworkPriceId()
		{
			var login = _user.Login;

			ServiceContext.GetResultPath = () => "..\\..\\Data\\EtalonUpdates\\";
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				var updateData = UpdateHelper.GetUpdateData(connection, login);
				//Устанавливаем любой прайс-лист, в данном случае это прайс поставщика Инфорум
				updateData.NetworkPriceId = 2647;
				updateData.KnownBuildNumber = 1279;

				updateData.ParseBuildNumber("6.0.0.1261");
				Assert.That(updateData.BuildNumber, Is.EqualTo(1261));
			}
		}

		private void DeleteAllLogs(uint userId)
		{
			using (var transaction = new TransactionScope())
			{
				TestAnalitFUpdateLog.DeleteAll("UserId = {0}".Format(userId));
			}
		}

		private void CheckPreviousRequestOnFirst(string userName, uint userId)
		{
			try
			{
				using (var connection = new MySqlConnection(Settings.ConnectionString()))
				{
					connection.Open();

					var updateData = UpdateHelper.GetUpdateData(connection, userName);
					Assert.That(updateData, Is.Not.Null);
					Assert.That(updateData.PreviousRequest, Is.Not.Null);
					Assert.That(updateData.PreviousRequest.UpdateId, Is.Null);
				}
			}
			finally
			{
				DeleteAllLogs(userId);
			}
		}

		[Test(Description = "Проверяем установку свойства PreviousRequest при первом обращении для Future")]
		public void CheckPreviousRequestOnFirstByFuture()
		{
			CheckPreviousRequestOnFirst(_user.Login, _user.Id);
		}

		[Test(Description = "Проверяем установку свойства PreviousRequest при первом обращении для клиента из старой реальности")]
		public void CheckPreviousRequestOnFirstByOldClient()
		{
			CheckPreviousRequestOnFirst(_oldClient.Users[0].OSUserName, _oldClient.Users[0].Id);
		}

		private void CheckPreviousRequestWithOldRequest(string userName, uint userId)
		{
			TestAnalitFUpdateLog log;
			using (var transaction = new TransactionScope())
			{
				log = new TestAnalitFUpdateLog
				{
					UserId = userId,
				    RequestTime = DateTime.Now.AddDays(-2),
				    UpdateType = (uint) RequestType.GetData,
				    Commit = false
				};
				log.Save();
				log = new TestAnalitFUpdateLog
				{
					UserId = userId,
					RequestTime = DateTime.Now.AddDays(-2),
					UpdateType = (uint)RequestType.GetCumulative,
					Commit = true
				};
				log.Save();
				log = new TestAnalitFUpdateLog
				{
					UserId = userId,
					RequestTime = DateTime.Now,
					UpdateType = (uint)RequestType.SendWaybills,
					Commit = true
				};
				log.Save();
			}

			try
			{
				using (var connection = new MySqlConnection(Settings.ConnectionString()))
				{
					connection.Open();

					var updateData = UpdateHelper.GetUpdateData(connection, userName);
					Assert.That(updateData, Is.Not.Null);
					Assert.That(updateData.PreviousRequest, Is.Not.Null);
					Assert.That(updateData.PreviousRequest.UpdateId, Is.Null);
				}
			}
			finally
			{
				DeleteAllLogs(userId);
			}
		}

		[Test(Description = "Проверяем установку свойства PreviousRequest при существовании старых записей в AnalitFUpdates для Future")]
		public void CheckPreviousRequestWithOldRequestByFuture()
		{
			CheckPreviousRequestWithOldRequest(_user.Login, _user.Id);
		}

		[Test(Description = "Проверяем установку свойства PreviousRequest при существовании старых записей в AnalitFUpdates для клиента из старой реальности")]
		public void CheckPreviousRequestWithOldRequestByOldClient()
		{
			CheckPreviousRequestWithOldRequest(_oldClient.Users[0].OSUserName, _oldClient.Users[0].Id);
		}

		private void CheckPreviousRequestWithOldRequestExists(string userName, uint userId)
		{
			TestAnalitFUpdateLog log;
			using (var transaction = new TransactionScope())
			{
				log = new TestAnalitFUpdateLog
				{
					UserId = userId,
					RequestTime = DateTime.Now.AddDays(-2),
					UpdateType = (uint)RequestType.GetData,
					Commit = false
				};
				log.Save();
				log = new TestAnalitFUpdateLog
				{
					UserId = userId,
					RequestTime = DateTime.Now.AddDays(-2),
					UpdateType = (uint)RequestType.GetCumulative,
					Commit = true
				};
				log.Save();
				log = new TestAnalitFUpdateLog
				{
					UserId = userId,
					RequestTime = DateTime.Now,
					UpdateType = (uint)RequestType.GetCumulative,
					Commit = true
				};
				log.Save();
				var last = new TestAnalitFUpdateLog
				{
					UserId = userId,
					RequestTime = DateTime.Now,
					UpdateType = (uint)RequestType.GetDocs,
					Commit = true
				};
				last.Save();
			}

			try
			{
				using (var connection = new MySqlConnection(Settings.ConnectionString()))
				{
					connection.Open();

					var updateData = UpdateHelper.GetUpdateData(connection, userName);
					Assert.That(updateData, Is.Not.Null);
					Assert.That(updateData.PreviousRequest, Is.Not.Null);
					Assert.That(updateData.PreviousRequest.UpdateId, Is.Not.Null);
					Assert.That(updateData.PreviousRequest.UpdateId.Value, Is.EqualTo(log.Id));
					Assert.That(updateData.PreviousRequest.RequestType, Is.EqualTo((RequestType)log.UpdateType));
					Assert.That(log.RequestTime.Subtract(updateData.PreviousRequest.RequestTime).TotalSeconds, Is.LessThan(1));
					Assert.That(updateData.PreviousRequest.Commit, Is.EqualTo(log.Commit));
				}
			}
			finally
			{
				DeleteAllLogs(userId);
			}
		}

		[Test(Description = "Проверяем установку свойства PreviousRequest при существовании старых записей в AnalitFUpdates для Future")]
		public void CheckPreviousRequestWithOldRequestExistsByFuture()
		{
			CheckPreviousRequestWithOldRequestExists(_user.Login, _user.Id);
		}

		[Test(Description = "Проверяем установку свойства PreviousRequest при существовании старых записей в AnalitFUpdates для клиента из старой реальности")]
		public void CheckPreviousRequestWithOldRequestExistsByOldClient()
		{
			CheckPreviousRequestWithOldRequestExists(_oldClient.Users[0].OSUserName, _oldClient.Users[0].Id);
		}

		[Test(Description = "проверяем методы для работы с именами подготовленными файлами")]
		public void CheckResultPaths()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();

				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
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
		}

		[Test(Description = "Проверяем корректность доступности механизма подтверждения пользовательского сообщения")]
		public void CheckFlagIsConfrimUserMessage()
		{
			var login = _user.Login;

			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				var updateData = UpdateHelper.GetUpdateData(connection, login);

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
		}

		[Test(Description = "проверяем корректное чтение TargetVersion")]
		public void CheckReadTargetVersion()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();

				MySqlHelper.ExecuteNonQuery(
					connection,
					"update usersettings.UserUpdateInfo set TargetVersion = null where UserId = ?UserId",
					new MySqlParameter("?UserId", _user.Id));

				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				Assert.That(updateData.TargetVersion, Is.Null);

				const int targetVersion = 1300;
				MySqlHelper.ExecuteNonQuery(
					connection,
					"update usersettings.UserUpdateInfo set TargetVersion = ?TargetVersion where UserId = ?UserId",
					new MySqlParameter("?UserId", _user.Id),
					new MySqlParameter("?TargetVersion", targetVersion));
				updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				Assert.That(updateData.TargetVersion, Is.EqualTo(targetVersion));
			}
		}

		[Test(Description = "Проверяем результат функции EnableUpdate при различных значениях TargetVersion")]
		public void CheckSetEnableUpdate()
		{
			ServiceContext.GetResultPath = () => "..\\..\\Data\\EtalonUpdates\\";

			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();

				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);

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
		}

	}
}
