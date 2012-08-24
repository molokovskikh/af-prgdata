using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using Common.Tools;
using MySql.Data.MySqlClient;
using PrgData.Common.SevenZip;
using log4net;

namespace PrgData.Common.Models
{
	public class PromotionsExport : BaseExport
	{
		public PromotionsExport(UpdateData updateData, MySqlConnection connection, Queue<FileForArchive> files)
			: base(updateData, connection, files)
		{
		}

		public override int RequiredVersion
		{
			get { return UpdateData._versionBeforeSupplierPromotions; }
		}

		private string GetAbstractPromotionsCommand()
		{
			var sql = "";
			if (!SendAllData) {
				sql = @"select
	log.PromotionId as Id,
	0 as Status,
	log.SupplierId,
	log.Name,
	log.Annotation,
	log.PromoFile,
	log.Begin,
	log.End
from
	logs.SupplierPromotionLogs log
where
	log.LogTime > '2011-03-01'
	and log.Operation = 2
union";
			}
			sql += @"
select
	sp.Id,
	sp.Status and (sp.RegionMask & ?RegionMask > 0) as Status,
	sp.SupplierId,
	sp.Name,
	sp.Annotation,
	sp.PromoFile,
	sp.Begin,
	sp.End
from usersettings.SupplierPromotions sp
";

			return sql;
		}

		public string GetPromotionsCommand()
		{
			var sql = GetAbstractPromotionsCommand();
			//если обновление камулитивное то нет нужды отдавать отключенный акции
			if (SendAllData) {
				sql += "where (sp.Status and (sp.RegionMask & ?RegionMask > 0)) = 1";
			}
			return sql;
		}

		private bool SendAllData
		{
			get
			{
				return updateData.Cumulative || updateData.NeedUpdateToSupplierPromotions;
			}
		}

		public string GetPromotionsCommandById(List<uint> promotionIds)
		{
			return
				GetAbstractPromotionsCommand() +
					string.Format("where sp.Id in ({0})", promotionIds.Implode());
		}

		public string GetPromotionCatalogsCommandById(List<uint> promotionIds)
		{
			var sql = "";
			if (!SendAllData) {
				sql = @"select
  CatalogId,
  PromotionId,
  1 as Hidden
from
  logs.PromotionCatalogLogs
where
	LogTime > ?UpdateTime
and Operation = 2
union";
			}

			sql += String.Format(@"
select
  CatalogId,
  PromotionId,
  0 as Hidden
from
  usersettings.PromotionCatalogs
where
  PromotionId in ({0})
", promotionIds.Implode());
			return sql;
		}

		private List<SupplierPromotion> GetPromotionsList()
		{
			var command = new MySqlCommand("", connection);
			var dataAdapter = new MySqlDataAdapter(command);
			var dataTable = new DataTable();
			command.CommandText = GetPromotionsCommand();
			SetParameters(command);
			dataAdapter.Fill(dataTable);

			var list = new List<SupplierPromotion>();
			foreach (DataRow row in dataTable.Rows) {
				list.Add(
					new SupplierPromotion {
						Id = Convert.ToUInt32(row["Id"]),
						Status = Convert.ToBoolean(row["Status"])
					});
			}
			return list;
		}

		public override void Export()
		{
			if (!updateData.ShowAdvertising)
				return;

			updateData.SupplierPromotions = GetPromotionsList();

			var ids = updateData.SupplierPromotions.Select(promotion => promotion.Id).ToList();
			Process("SupplierPromotions", GetPromotionsCommandById(ids));
			Process("PromotionCatalogs", GetPromotionCatalogsCommandById(ids));
		}

		public override void ArchiveFiles(string archiveFile)
		{
			var promotionsFolder = "Promotions";
			var promotionsPath = Path.Combine(updateData.ResultPath, promotionsFolder);
			if (!Directory.Exists(promotionsPath))
				Directory.CreateDirectory(promotionsPath);

			foreach (var supplierPromotion in updateData.SupplierPromotions) {
				if (supplierPromotion.Status) {
					var files1 = Directory.GetFiles(promotionsPath, supplierPromotion.Id + "*");
					if (files1.Length > 0) {
						SevenZipHelper.ArchiveFilesWithNames(
							archiveFile,
							Path.Combine(promotionsFolder, supplierPromotion.Id + "*"),
							updateData.ResultPath);
					}
				}
			}
		}
	}
}