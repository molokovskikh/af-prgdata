using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Castle.ActiveRecord;
using Common.Models;
using Common.Models.Repositories;
using Common.Models.Tests.Repositories;
using Common.Tools;
using MySql.Data.MySqlClient;
using NHibernate;
using NUnit.Framework;
using PrgData.Common;
using Test.Support;

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
			ContainerInitializer.InitializerContainerForTests();

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
	}
}
