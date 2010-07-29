﻿using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using MySql.Data.MySqlClient;
using Common.MySql;
using Common.Models;
using System.IO;
using log4net;
using Inforoom.Common;

using Castle.Windsor;
using Castle.MicroKernel.Registration;
using Common.Models.Repositories;
using NHibernate;
using SmartOrderFactory;
using SmartOrderFactory.Domain;
using SmartOrderFactory.Repositories;
using NHibernate.Mapping.Attributes;



namespace PrgData.Common
{
	public class SmartOrderHelper
	{
		private UpdateData _updateData;
		private MySqlConnection _readWriteConnection;
		private MySqlConnection _readOnlyConnection;

		public uint OrderedClientCode { get; private set; }
		public IOrderable Orderable { get; private set; }
		public Address Address { get; private set; }

		private string _tmpBatchFolder;
		private string _tmpBatchArchive;
		private string _tmpBatchFileName;

		public string BatchReportFileName;
		public string BatchOrderFileName;
		public string BatchOrderItemsFileName;

		private SmartOrderBatchHandler _handler;

		private SmartOrderRule _smartOrderRule;
		private OrderRules _orderRule;

		private readonly ILog _log = LogManager.GetLogger(typeof(SmartOrderHelper));

		private uint _maxOrderId;
		private uint _maxOrderListId;
		private uint _maxBatchId;


		public SmartOrderHelper(UpdateData updateData, MySqlConnection readOnlyConnection, MySqlConnection readWriteConnection, uint orderedClientCode, uint maxOrderId, uint maxOrderListId, uint maxBatchId)
		{
			_updateData = updateData;
			_readOnlyConnection = readOnlyConnection;
			_readWriteConnection = readWriteConnection;
			OrderedClientCode = orderedClientCode;
			_maxOrderId = maxOrderId;
			_maxOrderListId = maxOrderListId;
			_maxBatchId = maxBatchId;

			_orderRule = IoC.Resolve<IRepository<OrderRules>>().Get(updateData.ClientId);
			if (!_orderRule.EnableSmartOrder)
				throw new UpdateException("Услуга 'АвтоЗаказ' не предоставляется", "Пожалуйста обратитесь в АК \"Инфорум\".", "Услуга 'АвтоЗаказ' не предоставляется; ", RequestType.Forbidden);

			_smartOrderRule = IoC.Resolve<ISmartOrderFactoryRepository>().GetSmartOrderRule(updateData.ClientId);

			if (_smartOrderRule == null)
				throw new UpdateException("Не настроены правила для автоматического формирования заказа", "Пожалуйста обратитесь в АК \"Инфорум\".", "Не настроены правила для автоматического формирования заказа; ", RequestType.Forbidden);

			using(var unitOfWork = new UnitOfWork())
			{
				if (_updateData.IsFutureClient)
				{
					Orderable = IoC.Resolve<IRepository<User>>().Get(_updateData.UserId);
					NHibernateUtil.Initialize(((User)Orderable).AvaliableAddresses);
					Address = IoC.Resolve<IRepository<Address>>().Get(orderedClientCode);
					NHibernateUtil.Initialize(Address.Users);
				}
				else
					Orderable = IoC.Resolve<IRepository<Client>>().Get(orderedClientCode);
			}

			_tmpBatchFolder = Path.GetTempPath() + Path.GetFileNameWithoutExtension(Path.GetTempFileName());
			_tmpBatchArchive = _tmpBatchFolder + @"\batch.7z";
			BatchReportFileName = _tmpBatchFolder + @"\BatchReport.txt";
			BatchOrderFileName = _tmpBatchFolder + @"\BatchOrder.txt";
			BatchOrderItemsFileName = _tmpBatchFolder + @"\BatchOrderItems.txt";
			Directory.CreateDirectory(_tmpBatchFolder);
		}

		public void PrepareBatchFile(string batchFile)
		{
			var batchFileBytes = Convert.FromBase64String(batchFile);
			using (var fileBatch = new FileStream(_tmpBatchArchive, FileMode.CreateNew))
				fileBatch.Write(batchFileBytes, 0, batchFileBytes.Length);

			if (!ArchiveHelper.TestArchive(_tmpBatchArchive))
				throw new Exception("Полученный архив поврежден.");

			var extractDir = Path.GetDirectoryName(_tmpBatchArchive) + "\\BatchExtract";
			if (!Directory.Exists(extractDir))
				Directory.CreateDirectory(extractDir);

			ArchiveHelper.Extract(_tmpBatchArchive, "*.*", extractDir);
			var files = Directory.GetFiles(extractDir);
			if (files.Length == 0)
				throw new Exception("Полученный архив не содержит файлов.");

			_tmpBatchFileName = files[0];
		}


		public void ProcessBatchFile()
		{
			using (var stream = new FileStream(_tmpBatchFileName, FileMode.Open))
			{
				_handler = new SmartOrderBatchHandler(Orderable, Address, stream);
				var orders = _handler.ProcessOrderBatch();
				SaveToFile(_handler.OrderBatchItems, orders);
			}
		}

		private void SaveToFile(List<OrderBatchItem> list, List<Order> orders)
		{
			var buildOrder = new StringBuilder();
			var buildItems = new StringBuilder();
			var buildReport = new StringBuilder();
			//uint id = 1;
			foreach (var order in orders)
			{
				order.RowId = _maxOrderId;
				_maxOrderId++;
				buildOrder.AppendFormat(
					"{0}\t{1}\t{2}\t{3}\n",
					order.RowId,
					OrderedClientCode,
					order.PriceList.PriceCode,
					order.RegionCode);
				foreach (var item in order.OrderItems)
				{
					item.RowId = _maxOrderListId;
					_maxOrderListId++;

					var report = list.Find(reportItem => { return reportItem.Item != null && reportItem.Item.OrderItem == item; });

					buildItems.AppendFormat(
						"{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\t{8}\t{9}\t{10}\t{11}\t{12}\t{13}\t{14}\t{15}\t{16}\t{17}\t{18}\t{19}\n",
						item.RowId,
						item.Order.RowId,
						OrderedClientCode,
						item.CoreId,
						item.ProductId,
						item.CodeFirmCr.HasValue ? item.CodeFirmCr.Value.ToString() : "\\N",
						item.SynonymCode.HasValue ? item.SynonymCode.Value.ToString() : "\\N",
						item.SynonymFirmCrCode.HasValue ? item.SynonymFirmCrCode.Value.ToString() : "\\N",
						item.Code,
						item.CodeCr,
						report.Item.Offer.CostWithoutDelayOfPayment.ToString(CultureInfo.InvariantCulture.NumberFormat),
						report.Item.Offer.Cost.ToString(CultureInfo.InvariantCulture.NumberFormat),
						item.Await ? "1" : "0",
						item.Junk ? "1" : "0",
						item.Quantity,
						item.RequestRatio.HasValue ? item.RequestRatio.Value.ToString() : "\\N",
						item.OrderCost.HasValue ? item.OrderCost.Value.ToString(CultureInfo.InvariantCulture.NumberFormat) : "\\N",
						item.MinOrderCount.HasValue ? item.MinOrderCount.Value.ToString() : "\\N",
						item.OfferInfo.Period,
						item.OfferInfo.ProducerCost.HasValue ? item.OfferInfo.ProducerCost.Value.ToString(CultureInfo.InvariantCulture.NumberFormat) : "\\N");
				}
			}

			foreach (var report in list)
			{
				if (report.Item != null)
				{
					var comments = new List<string>();
					if (!String.IsNullOrEmpty(report.Comment))
						comments.Add(report.Comment);
					comments.AddRange(report.Item.Comments);
					comments = comments.Distinct().ToList();
					buildReport.AppendFormat(
						//"{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\t{8}\t{9}\n",
						"{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\t{8}\t{9}\n",
						_maxBatchId,
						OrderedClientCode,
						//!String.IsNullOrEmpty(report.Code) ? report.Code : "\\N",
						report.ProductName,
						report.ProducerName,
						report.Quantity,
						String.Join("\r\\\n", comments.ToArray()),
						report.Item.OrderItem != null ? report.Item.OrderItem.RowId.ToString() : "\\N",
						(int)report.Item.Status,
						report.Item.ProductId,
						report.Item.CodeFirmCr.HasValue ? report.Item.CodeFirmCr.Value.ToString() : "\\N");
				}
				else
					buildReport.AppendFormat(
						//"{0}\t{1}\t{2}\t{3}\t{4}\t{5}\n",
						"{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t\\N\t\\N\t\\N\t\\N\n",
						_maxBatchId,
						OrderedClientCode,
						//report.Code,
						report.ProductName,
						report.ProducerName,
						report.Quantity,
						report.Comment);
				_maxBatchId++;
			}

			File.WriteAllText(BatchReportFileName, buildReport.ToString(), Encoding.GetEncoding(1251));
			File.WriteAllText(BatchOrderFileName, buildOrder.ToString(), Encoding.GetEncoding(1251));
			File.WriteAllText(BatchOrderItemsFileName, buildItems.ToString(), Encoding.GetEncoding(1251));
		}

		public void DeleteTemporaryFiles()
		{ 
			if (Directory.Exists(_tmpBatchFolder))
				try
				{
					Directory.Delete(_tmpBatchFolder, true);
				}
				catch (Exception exception)
				{
					_log.Error("Ошибка при удалении временнной директории при обработке дефектуры", exception);
				}
		}

		public static void InitializeIoC()
		{
			var sessionFactoryHolder = new SessionFactoryHolder();
			sessionFactoryHolder
				.Configuration
				.AddInputStream(HbmSerializer.Default.Serialize(typeof(Client).Assembly))
				.AddInputStream(HbmSerializer.Default.Serialize(typeof(SmartOrderRule).Assembly));
			IoC.Initialize(new WindsorContainer()
			               .Register(
							Component.For<ISessionFactoryHolder>().Instance(sessionFactoryHolder),
							Component.For<RepositoryInterceptor>(),
							Component.For(typeof(IRepository<>)).ImplementedBy(typeof(Repository<>)),
							Component.For<IOfferRepository>().ImplementedBy<OfferRepository>(),
							Component.For<ISmartOrderFactoryRepository>().ImplementedBy<SmartOrderFactoryRepository>(),
							Component.For<ISmartOfferRepository>().ImplementedBy<SmartOfferRepository>(),
							Component.For<IOrderFactory>().ImplementedBy<SmartOrderFactory.SmartOrderFactory>()
			               	));
		}
	}
}