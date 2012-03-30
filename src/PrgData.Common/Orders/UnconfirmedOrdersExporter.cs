using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.IO;
using System.Text;
using Common.Models;
using Common.Tools;
using log4net;
using MySql.Data.MySqlClient;
using NHibernate.Criterion;
using MySqlHelper = Common.MySql.MySqlHelper;
using Order = Common.Models.Order;
using With = Common.MySql.With;

namespace PrgData.Common.Orders
{
	public class UnconfirmedOrdersExporter
	{
		public UpdateData Data { get; private set; }
		public UpdateHelper Helper { get; private set; }
		public string ExportFolder { get; private set; }

		private Queue<FileForArchive> _filesForArchive;

		public string OrdersHeadFileName { get; private set; }
		public string OrdersListFileName { get; private set; }

		public List<Order> LoadedOrders { get; private set; }

		public List<Order> ExportedOrders { get; private set; }

		public UnconfirmedOrdersExporter(UpdateData updateData, UpdateHelper helper, string exportFolder, Queue<FileForArchive> filesForArchive)
		{
			Data = updateData;
			Helper = helper;
			ExportFolder = exportFolder;
			_filesForArchive = filesForArchive;

			OrdersHeadFileName = Path.Combine(ExportFolder, "CurrentOrderHeads" + Data.UserId + ".txt");
			OrdersListFileName = Path.Combine(ExportFolder, "CurrentOrderLists" + Data.UserId + ".txt");

			ShareFileHelper.MySQLFileDelete(OrdersHeadFileName);
			ShareFileHelper.MySQLFileDelete(OrdersListFileName);
		}

		public void Export()
		{
			LoadOrders();
			if (LoadedOrders.Count > 0)
			{
				UnionOrders();
				ExportOrders();
			}
		}

		public void ExportOrders()
		{
			ShareFileHelper.WaitDeleteFile(OrdersHeadFileName);
			ShareFileHelper.WaitDeleteFile(OrdersListFileName);

			var converter = new Orders2StringConverter(ExportedOrders, Data.MaxOrderId, Data.MaxOrderListId, Data.AllowExportSendDate);

			File.WriteAllText(OrdersHeadFileName, converter.OrderHead.ToString(), Encoding.GetEncoding(1251));
			File.WriteAllText(OrdersListFileName, converter.OrderItems.ToString(), Encoding.GetEncoding(1251));

			lock (_filesForArchive)
			{
				_filesForArchive.Enqueue(new FileForArchive(OrdersHeadFileName, true));
				_filesForArchive.Enqueue(new FileForArchive(OrdersListFileName, true));
			}
		}

		public void UnionOrders()
		{
			ExportedOrders = LoadedOrders
				.GroupBy(o => new { o.AddressId, o.PriceList.PriceCode, o.RegionCode })
				.Select(
					g => {
							var firstOrder = g.OrderBy(o => o.WriteTime).First();
							if (g.Count() > 1)
							{
								foreach (var order in g)
								{
									if (order != firstOrder)
										for (int i = order.OrderItems.Count - 1; i >= 0; i--)
										{
											var item = order.OrderItems[i];
											order.RemoveItem(item);
											item.Order = firstOrder;
											firstOrder.OrderItems.Add(item);
										}
								}

								firstOrder.RowCount = (uint)firstOrder.OrderItems.Count;
							}

							return firstOrder;
					}).ToList();
		}

		public void LoadOrders()
		{
			using (var session = IoC.Resolve<ISessionFactoryHolder>().SessionFactory.OpenSession(Helper.ReadWriteConnection))
			{
				var addressList = session
					.CreateSQLQuery("select AddressId from Customers.UserAddresses where UserId = :userId")
					.SetParameter("userId", Data.UserId)
					.List<uint>();
				var criteria = DetachedCriteria.For<Order>()
					.Add(Restrictions.Eq("Submited", false))
					.Add(Restrictions.Eq("Deleted", false))
					.Add(Restrictions.Eq("Processed", false))
					.Add(Restrictions.In("AddressId", addressList.ToArray()));
				LoadedOrders = criteria.GetExecutableCriteria(session).List<Order>().ToList();

				Data.UnconfirmedOrders.Clear();
				LoadedOrders.ForEach(o => Data.UnconfirmedOrders.Add(o.RowId));
			}
		}

		public static string DeleteUnconfirmedOrders(UpdateData updateData, MySqlConnection connection, uint updateId)
		{
			var list = new List<string>();

			if (updateData.AllowDeleteUnconfirmedOrders)
				With.DeadlockWraper(() =>
				{
					var transaction = connection.BeginTransaction(IsolationLevel.RepeatableRead);
					try
					{
						var orders = MySql.Data.MySqlClient.MySqlHelper.ExecuteDataset(
							connection,
							@"
select 
  OrderId as RowId 
from 
  logs.UnconfirmedOrdersSendLogs
where
	UserId = ?UserId
and UpdateId = ?UpdateId
and Committed = 0
order by OrderId"
							,
							new MySqlParameter("?UserId", updateData.UserId),
							new MySqlParameter("?UpdateId", updateId));

						if (orders.Tables.Count == 1)
						{
							var logger = LogManager.GetLogger(typeof (UnconfirmedOrdersExporter));
							foreach (DataRow row in orders.Tables[0].Rows)
							{
								var orderId = row["RowId"];
								logger.DebugFormat("Удаляем неподтвержденный заказ: {0}", orderId);
								MySql.Data.MySqlClient.MySqlHelper.ExecuteNonQuery(
									connection,
									@"
update orders.OrdersHead set Deleted = 1 where RowId = ?orderId;
update logs.UnconfirmedOrdersSendLogs
set
  Committed = 1  
where
  UserId = ?UserId 
and OrderId = ?OrderId
and UpdateId = ?UpdateId;
",
									new MySqlParameter("?orderId", orderId),
									new MySqlParameter("?UserId", updateData.UserId),
									new MySqlParameter("?UpdateId", updateId));
								list.Add(orderId.ToString());
							}
						}

						transaction.Commit();
					}
					catch
					{
						ConnectionHelper.SafeRollback(transaction);
						throw;
					}
				});

			return list.Implode();
		}

		public static void InsertUnconfirmedOrdersLogs(UpdateData updateData, MySqlConnection connection, uint? updateId)
		{
			if (updateData.AllowDeleteUnconfirmedOrders && updateId.HasValue && updateData.UnconfirmedOrders.Count > 0)
			{
				var transaction = connection.BeginTransaction(IsolationLevel.RepeatableRead);
				try
				{
					foreach (var unconfirmedOrder in updateData.UnconfirmedOrders)
					{
						MySql.Data.MySqlClient.MySqlHelper.ExecuteNonQuery(
							connection,
							@"
insert into logs.UnconfirmedOrdersSendLogs 
  (UserId, OrderId) 
select 
	?UserId, 
	?OrderId 
from 
	Customers.Users u
where 
	u.Id = ?UserId
and not exists(select * from logs.UnconfirmedOrdersSendLogs where UserId = ?UserId and OrderId = ?OrderId);
update logs.UnconfirmedOrdersSendLogs
set
  Committed = 0,
  UpdateId = ?UpdateId
where
  UserId = ?UserId 
and OrderId = ?OrderId;
"
							,
							new MySqlParameter("?UserId", updateData.UserId),
							new MySqlParameter("?OrderId", unconfirmedOrder),
							new MySqlParameter("?UpdateId", updateId));
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
	}
}