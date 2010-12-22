using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using Castle.ActiveRecord;
using Common.Models.Tests.Repositories;
using Common.Tools;
using log4net;
using log4net.Config;
using NUnit.Framework;
using MySql.Data.MySqlClient;
using PrgData.Common;
using System.Data.Common;
using System.Data;
using Test.Support;

namespace Integration
{
	[TestFixture]
	public class CostOptimizationFixture
	{
		private uint _optimizationSupplierId = 5;
		private uint _concurentSupplierId = 14;
		private uint _optimizationPriceId;

		TestClient _client;
		TestUser _user;

		TestOldClient _oldClient;
		TestOldUser _oldUser;

		[SetUp]
		public void SetUp()
		{
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


				_oldClient = TestOldClient.CreateTestClient();
				_oldUser = _oldClient.Users[0];

				var session = ActiveRecordMediator.GetSessionFactoryHolder().CreateSession(typeof(ActiveRecordBase));
				try
				{
					session.CreateSQLQuery(@"
insert into usersettings.AssignedPermissions (PermissionId, UserId) values (:permissionid, :userid)")
						.SetParameter("permissionid", permission.Id)
						.SetParameter("userid", _oldUser.Id)
						.ExecuteUpdate();
				}
				finally
				{
					ActiveRecordMediator.GetSessionFactoryHolder().ReleaseSession(session);
				}
			}

			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();

				MySqlHelper.ExecuteNonQuery(
					connection,
					"call future.GetPrices(?UserId)",
					new MySqlParameter("?UserId", _user.Id));

				_optimizationPriceId = Convert.ToUInt32(MySqlHelper.ExecuteScalar(
					connection,
					@"
select 
	Prices.PriceCode, count(*)
from
	Prices
	inner join farm.Core0 c on c.PriceCode = Prices.PriceCode
where
	Prices.FirmCode = ?OptimizationSupplierId
group by Prices.PriceCode
order by 2 desc",
				new MySqlParameter("?OptimizationSupplierId", _optimizationSupplierId)));

				var ruleId = Convert.ToUInt64(
					MySqlHelper.ExecuteScalar(
						connection,
						@"
insert into usersettings.costoptimizationrules (SupplierId) values (?OptimizationSupplierId);
set @LastRuleId = last_insert_id();
insert into usersettings.costoptimizationconcurrents (RuleId, SupplierId) values (@LastRuleId, ?ConcurentSupplierId);
insert into usersettings.costoptimizationclients (RuleId, ClientId) values (@LastRuleId, ?OldClientId);
insert into usersettings.costoptimizationclients (RuleId, ClientId) values (@LastRuleId, ?NewClientId);
select @LastRuleId;
"
						,
						new MySqlParameter("?OptimizationSupplierId", _optimizationSupplierId),
						new MySqlParameter("?ConcurentSupplierId", _concurentSupplierId),
						new MySqlParameter("?OldClientId", _oldClient.Id),
						new MySqlParameter("?NewClientId", _client.Id)));
			}
		}

		private void CostOptimizerShouldCreateLogsWithSupplier(uint clientId, Action<MySqlCommand> getOffers)
		{
			using (var conn = new MySqlConnection(Settings.ConnectionString()))
			{
				conn.Open();
				var command = new MySqlCommand("select Now()", conn);
				var startTime = Convert.ToDateTime(command.ExecuteScalar());

				command = new MySqlCommand(
@"select cs.Id, min(ccc.Cost) Cost
  from farm.Core0 cs
       join farm.Core0 cc on cc.ProductId = cs.ProductId and cs.Id <> cc.Id and cs.ProductId = cc.ProductId
       join farm.CoreCosts ccc on ccc.Core_Id = cc.Id
       join usersettings.PricesData pds on pds.PriceCode = cs.PriceCode
       join usersettings.PricesData pdc on pdc.PriceCode = cc.PriceCode
 where pds.PriceCode = ?priceId
   and pdc.FirmCode = ?concurentId
group by cs.Id
limit 0, 50", conn);
				command.Parameters.AddWithValue("?priceId", _optimizationPriceId);
				command.Parameters.AddWithValue("?concurentId", _concurentSupplierId);

				var cores = command.ExecuteReader();
				var update = new StringBuilder();
				foreach (var row in cores.Cast<DbDataRecord>())
					update.Append("update farm.CoreCosts set Cost=").Append(row["Cost"]).Append("+1 where Core_Id=").Append(row["Id"]).Append(";");
				cores.Close();

				command.CommandText = update.ToString().Replace(',', '.');
				command.ExecuteNonQuery();

				command.Parameters.Clear();
				getOffers(command);
				command.Parameters.Clear();

				command.CommandType = CommandType.Text;

				var optimizer = new CostOptimizer(conn, clientId);
				optimizer.Oprimize();

				command.CommandText = "select * from logs.CostOptimizationLogs where LoggedOn > ?startTime and ClientId = ?clientId";
				command.Parameters.AddWithValue("?startTime", startTime);
				command.Parameters.AddWithValue("?clientId", clientId);
				var reader = command.ExecuteReader();

				foreach (var row in reader.Cast<DbDataRecord>())
				{
					Assert.AreEqual(5, row["SupplierId"]);
				}
			}
		}

		[Test(Description = "проверяем создание записей в логах оптимизации для клиента из старой реальности")]
		public void CostOptimizerShouldCreateLogsWithSupplierForOldClient()
		{
			CostOptimizerShouldCreateLogsWithSupplier(
				_oldClient.Id, 
				command =>
					{
						command.CommandText = "call usersettings.GetOffers(?ClientCodeParam, ?FreshOnly);";
						command.Parameters.AddWithValue("?ClientCodeParam", _oldClient.Id);
						command.Parameters.AddWithValue("?FreshOnly", false);
						command.ExecuteNonQuery();
					});
		}

		[Test(Description = "проверяем создание записей в логах оптимизации для клиента из новой реальности")]
		public void CostOptimizerShouldCreateLogsWithSupplierForFuture()
		{
			CostOptimizerShouldCreateLogsWithSupplier(
				_client.Id,
				command =>
				{
					command.CommandText = "call future.GetOffers(?UserId);";
					command.Parameters.AddWithValue("?UserId", _user.Id);
					command.ExecuteNonQuery();
				});
		}

		public class CostLogInsert
		{
			public uint ClientId;

			private ILog _log;

			public Thread Thread;

			//общее кол-во вставляемых записей
			private int _insertCount = 8000;
			//кол-во записей вставляемых одной командой
			private int _packCount = 300;

			public bool Error { get; private set; }

			public CostLogInsert(uint clientId)
			{
				_log = LogManager.GetLogger(typeof (CostLogInsert));
				Thread = new Thread(ThreadMethod);
				Thread.Start();
				ClientId = clientId;
			}

			public string ForMySql(decimal? value)
			{
				if (value == null)
					return "null";
				return value.Value.ToString(CultureInfo.InvariantCulture);
			}

			public void ThreadMethod()
			{
				try
				{
					using (var connection = new MySqlConnection(Settings.ConnectionString()))
					{
						connection.Open();
						var transaction = connection.BeginTransaction(IsolationLevel.RepeatableRead);
						try
						{
							var header = "insert into logs.CostOptimizationLogs(ClientId, SupplierId, ProductId, ProducerId, SelfCost, ConcurentCost, AllCost, ResultCost) values";
							var logCommand = new MySqlCommand(header, connection);

							var begin = 0;

							while (begin < _insertCount)
							{
								var commandText = new StringBuilder();
								commandText.Append(header);

								for (var i = 0; i < _packCount; i++)
								{
									if (begin + i >= _insertCount)
										break;

									commandText.Append(String.Format(" ({6}, {7}, {0}, {1}, {2}, {3}, {4}, {5})", 1, 1, ForMySql(1.1m), ForMySql(1.2m), ForMySql(1.3m), ForMySql(1.0m), ClientId, 2));
									if (i < (_packCount-1) && begin + i < _insertCount - 1)
										commandText.AppendLine(", ");
								}
								logCommand.CommandText = commandText.ToString();
								logCommand.ExecuteNonQuery();
								begin += _packCount;
							}

							transaction.Commit();
						}
						catch
						{
							ConnectionHelper.SafeRollback(transaction);
							throw;
						}
					}
				}
				catch (Exception exception)
				{
					Error = true;
					_log.ErrorFormat("Ошибка для клиента {0}: {1}", ClientId, exception);
				}
			}

		}

		[Test(Description = "Проверка множественных вставок в CostOptimizationLogs для воспроизведения ошибки Duplicate entry")]
		public void TestMultiInsertWithSomeThreads()
		{
			BasicConfigurator.Configure();
			try
			{
				var log = LogManager.GetLogger(typeof(CostOptimizationFixture));

				log.Debug("Начали работу теста");

				var list = new List<CostLogInsert>();
				var clientCount = 10;
				for (int i = 0; i < clientCount; i++)
				{
					list.Add(new CostLogInsert((uint)i+1));
				}

				do
				{
					Thread.Sleep(500);
				} while (!list.TrueForAll(item => item.Thread.ThreadState == ThreadState.Stopped));

				Assert.That(list.TrueForAll(item => !item.Error), "В одной из ниток возникла ошибка");

				log.Debug("Закончили работу теста");
			}
			finally
			{
				LogManager.ResetConfiguration();
			}
		}

	}
}
