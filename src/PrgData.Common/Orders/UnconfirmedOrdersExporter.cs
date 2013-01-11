using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.IO;
using System.Text;
using Common.Models;
using Common.Tools;
using NHibernate.Linq;
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

		public Queue<FileForArchive> FilesForArchive;

		public string OrdersHeadFileName { get; private set; }
		public string OrdersListFileName { get; private set; }

		public List<UnconfirmedOrderInfo> ExportedOrders { get; private set; }

		public UnconfirmedOrdersExporter(UpdateData updateData, UpdateHelper helper, string exportFolder, Queue<FileForArchive> filesForArchive)
		{
			Data = updateData;
			Helper = helper;
			ExportFolder = exportFolder;
			FilesForArchive = filesForArchive;

			OrdersHeadFileName = Path.Combine(ExportFolder, "CurrentOrderHeads" + Data.UserId + ".txt");
			OrdersListFileName = Path.Combine(ExportFolder, "CurrentOrderLists" + Data.UserId + ".txt");

			ShareFileHelper.MySQLFileDelete(OrdersHeadFileName);
			ShareFileHelper.MySQLFileDelete(OrdersListFileName);
		}

		public void Export()
		{
			LoadOrders();
			if (Data.UnconfirmedOrders.Count > 0) {
				UnionOrders();
				ExportOrders();
			}
		}

		public void ExportOrders()
		{
			ShareFileHelper.WaitDeleteFile(OrdersHeadFileName);
			ShareFileHelper.WaitDeleteFile(OrdersListFileName);

			var converter = new Orders2StringConverter(ExportedOrders, Data.MaxOrderListId, Data.AllowExportSendDate);

			File.WriteAllText(OrdersHeadFileName, converter.OrderHead.ToString(), Encoding.GetEncoding(1251));
			File.WriteAllText(OrdersListFileName, converter.OrderItems.ToString(), Encoding.GetEncoding(1251));

			lock (FilesForArchive) {
				FilesForArchive.Enqueue(new FileForArchive(OrdersHeadFileName, true));
				FilesForArchive.Enqueue(new FileForArchive(OrdersListFileName, true));
			}
		}

		public void UnionOrders()
		{
			var maxOrderId = Data.MaxOrderId;
			ExportedOrders = Data.UnconfirmedOrders
				.GroupBy(info => new { info.Order.AddressId, info.Order.PriceList.PriceCode, info.Order.RegionCode })
				.Select(
				g => {
					var firstOrderInfo = g.OrderBy(o => o.Order.WriteTime).First();
					firstOrderInfo.ClientOrderId = maxOrderId;
					maxOrderId++;
					if (g.Count() > 1) {
						foreach (var orderInfo in g) {
							//в группировке g содержится и первый заказ, поэтому при обработке его исключаем
							if (orderInfo != firstOrderInfo) {
								orderInfo.ClientOrderId = firstOrderInfo.ClientOrderId;
								//переносим все позиции из заказов из группировки в первый заказ в группировке
								for (int i = orderInfo.Order.OrderItems.Count - 1; i >= 0; i--) {
										var item = orderInfo.Order.OrderItems[i];
										orderInfo.Order.RemoveItem(item);
										item.Order = firstOrderInfo.Order;
										firstOrderInfo.Order.OrderItems.Add(item);
									}
							}
						}

						firstOrderInfo.Order.RowCount = (uint)firstOrderInfo.Order.OrderItems.Count;
						var unionClientAddition = g
							.Where(i => !String.IsNullOrWhiteSpace(i.Order.ClientAddition))
							.Select(i => i.Order.UserId + ": " + i.Order.ClientAddition)
							.Implode(" | ");
						firstOrderInfo.Order.ClientAddition = unionClientAddition;
					}

					return firstOrderInfo;
				}).ToList();
		}

		public void LoadOrders()
		{
			using (var session = IoC.Resolve<ISessionFactoryHolder>().SessionFactory.OpenSession(Helper.ReadWriteConnection)) {
				var addressList = session
					.CreateSQLQuery("select AddressId from Customers.UserAddresses where UserId = :userId")
					.SetParameter("userId", Data.UserId)
					.List<uint>();
				var criteria = DetachedCriteria.For<Order>()
					.Add(Restrictions.Eq("Submited", false))
					.Add(Restrictions.Eq("Deleted", false))
					.Add(Restrictions.Eq("Processed", false))
					.Add(Restrictions.In("AddressId", addressList.ToArray()));
				var loadedOrders = criteria.GetExecutableCriteria(session).List<Order>().ToList();
				var activePrices = session
					.CreateSQLQuery("select RegionCode, PriceCode from usersettings.ActivePrices")
					.List();

				loadedOrders = loadedOrders
					.Where(o => activePrices.Cast<object[]>().Any(p => Convert.ToUInt32(p[1]) == o.PriceList.PriceCode && Convert.ToUInt64(p[0]) == o.RegionCode))
					.ToList();

				Data.UnconfirmedOrders.Clear();
				loadedOrders.ForEach(o => Data.UnconfirmedOrders.Add(new UnconfirmedOrderInfo(o)));
			}
		}

		public static string UnconfirmedOrderInfosToString(List<UnconfirmedOrderInfo> list)
		{
			if (list == null || list.Count == 0)
				return String.Empty;

			var exportInfo = new List<string>();
			list.GroupBy(g => g.ClientOrderId)
				.Each(g => {
					if (g.Key.HasValue) {
						exportInfo.Add(g.Select(i => i.OrderId).Implode("+") + "->" + g.Key.Value);
					}
					else
						foreach (var unconfirmedOrderInfo in g) {
							exportInfo.Add(unconfirmedOrderInfo.OrderId + "->(неизвестно)");
						}
				});
			return exportInfo.Implode();
		}

		public static string DeleteUnconfirmedOrders(UpdateData updateData, MySqlConnection connection, uint updateId)
		{
			var orderInfos = new List<UnconfirmedOrderInfo>();

			if (updateData.AllowDeleteUnconfirmedOrders)
				With.DeadlockWraper(() => {
					var transaction = connection.BeginTransaction(IsolationLevel.RepeatableRead);
					try {
						var orders = MySql.Data.MySqlClient.MySqlHelper.ExecuteDataset(
							connection,
							@"
select 
  OrderId as RowId,
  ExportedClientOrderId
from 
  logs.UnconfirmedOrdersSendLogs
where
	UserId = ?UserId
and UpdateId = ?UpdateId
and Committed = 0
order by OrderId",
							new MySqlParameter("?UserId", updateData.UserId),
							new MySqlParameter("?UpdateId", updateId));

						if (orders.Tables.Count == 1) {
							var logger = LogManager.GetLogger(typeof(UnconfirmedOrdersExporter));
							foreach (DataRow row in orders.Tables[0].Rows) {
								var orderId = Convert.ToUInt32(row["RowId"]);
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

								orderInfos.Add(
									new UnconfirmedOrderInfo(
										orderId,
										Convert.IsDBNull(row["ExportedClientOrderId"]) ? null : (uint?)Convert.ToUInt32(row["ExportedClientOrderId"])));
							}
						}

						transaction.Commit();
					}
					catch {
						With.SafeRollback(transaction);
						throw;
					}
				});

			return UnconfirmedOrderInfosToString(orderInfos);
		}

		public static void InsertUnconfirmedOrdersLogs(UpdateData updateData, MySqlConnection connection, uint? updateId)
		{
			if (updateData.AllowDeleteUnconfirmedOrders && updateId.HasValue && updateData.UnconfirmedOrders.Count > 0) {
				var transaction = connection.BeginTransaction(IsolationLevel.RepeatableRead);
				try {
					foreach (var unconfirmedOrder in updateData.UnconfirmedOrders) {
						MySql.Data.MySqlClient.MySqlHelper.ExecuteNonQuery(
							connection,
							@"
insert into logs.UnconfirmedOrdersSendLogs 
  (UserId, OrderId, ExportedClientOrderId)
select 
	?UserId, 
	?OrderId,
	?ExportedClientOrderId
from 
	Customers.Users u
where 
	u.Id = ?UserId
and not exists(select * from logs.UnconfirmedOrdersSendLogs where UserId = ?UserId and OrderId = ?OrderId);
update logs.UnconfirmedOrdersSendLogs
set
  Committed = 0,
  UpdateId = ?UpdateId,
  ExportedClientOrderId = ?ExportedClientOrderId
where
  UserId = ?UserId 
and OrderId = ?OrderId;
",
							new MySqlParameter("?UserId", updateData.UserId),
							new MySqlParameter("?OrderId", unconfirmedOrder.OrderId),
							new MySqlParameter("?ExportedClientOrderId", unconfirmedOrder.ClientOrderId),
							new MySqlParameter("?UpdateId", updateId));
					}

					transaction.Commit();
				}
				catch {
					With.SafeRollback(transaction);
					throw;
				}
			}
		}
	}
}