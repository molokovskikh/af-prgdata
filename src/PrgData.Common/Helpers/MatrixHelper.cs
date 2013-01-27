using System;
using System.Collections.Generic;
using System.ComponentModel;
using Common.Models;

namespace PrgData.Common.Helpers
{
	public class SqlParts
	{
		public string Select;
		public string Join;
		public string JoinWithoutProducer;
		public string Having;
	}

	public class MatrixDeclaretion
	{
		public uint MatrixId;
		public MatrixType Type;
		public MatrixAction Action;
		public string Alias;
		public string Alias2;
		public string JoinAlias = "Core";

		public string AdditionalJoin;

		public bool SupportExcludedSuppliers;

		public MatrixDeclaretion(bool supportExcludedSuppliers, string @alias, MatrixAction action, MatrixType type, uint matrixId)
		{
			SupportExcludedSuppliers = supportExcludedSuppliers;
			Alias = alias;
			Alias2 = Alias + "_1";
			Action = action;
			Type = type;
			MatrixId = matrixId;
		}

		public string Join()
		{
			//для того что бы при объединение исользовались индексы делаем два объедиения 1 по продукту\производителя, второе по коду окп
			var nullCondition = Type == MatrixType.WhiteList ? " 0 " : " 1 ";
			var sql = @"left join farm.BuyingMatrix {0} on {0}.ProductId = Products.Id and if({0}.ProducerId is null, 1, if({1}.CodeFirmCr is null, " + nullCondition + ", {0}.ProducerId = {1}.CodeFirmCr)) and {0}.MatrixId = " + MatrixId;
			sql += Environment.NewLine + @"left join farm.BuyingMatrix {2} on {2}.CodeOKP = {1}.CodeOKP and {2}.MatrixId = " + MatrixId;
			return ResultJoin(sql);
		}

		public string JoinWithoutProducer()
		{
			var nullCondition = Type == MatrixType.WhiteList ? " 0 " : " 1 ";
			var sql = @"left join farm.BuyingMatrix {0} on {0}.ProductId = Products.Id and if({0}.ProducerId is null, 1, " + nullCondition + ") and {0}.MatrixId = " + MatrixId;
			sql += Environment.NewLine + @"left join farm.BuyingMatrix {2} on {2}.CodeOKP = {1}.CodeOKP and {2}.MatrixId = " + MatrixId;
			return ResultJoin(sql);
		}

		private string ResultJoin(string sql)
		{
			sql = String.Format(sql, Alias, JoinAlias, Alias2) + Environment.NewLine;
			if (SupportExcludedSuppliers) {
				sql += "left join UserSettings.OfferMatrixSuppliers oms on oms.SupplierId = at.FirmCode and oms.ClientId = ?ClientCode" + Environment.NewLine;
			}
			if (!String.IsNullOrEmpty(AdditionalJoin)) {
				sql = Environment.NewLine + AdditionalJoin + Environment.NewLine + sql;
			}
			return sql;
		}

		public string Select(string external)
		{
			var result = "0";
			if (Action == MatrixAction.Warning)
				result = "2";
			else if (Action == MatrixAction.Block)
				result = "1";
			else if (Action == MatrixAction.Delete)
				result = "3";

			var additionalCondition = "";
			if (SupportExcludedSuppliers)
				additionalCondition = "or oms.Id is not null";
			if (Type == MatrixType.WhiteList)
				return String.Format("if({0}.Id is not null or {4}.Id is not null {2}, {3}, {1})", Alias, result, additionalCondition, external, Alias2);
			else
				return String.Format("if({0}.Id is null and {4}.Id is null {2}, {3}, {1})", Alias, result, additionalCondition, external, Alias2);
		}
	}

	public class MatrixHelper
	{
		private OrderRules _settings;

		public MatrixHelper(UpdateData updateData)
		{
			_settings = updateData.Settings;
		}

		public SqlParts BuyingMatrixCondition(bool exportInforoomPrice)
		{
			var declaretions = new List<MatrixDeclaretion>();

			if (exportInforoomPrice) {
				if (_settings.BuyingMatrix.HasValue) {
					declaretions.Add(new MatrixDeclaretion(false, "bol",
						_settings.BuyingMatrixAction,
						_settings.BuyingMatrixType,
						_settings.BuyingMatrix.Value) {
							JoinAlias = "a",
							AdditionalJoin = "left join catalogs.Products on Products.Id = A.ProductId"
						});
				}
			}
			else {
				if (_settings.BuyingMatrix.HasValue) {
					declaretions.Add(new MatrixDeclaretion(false, "bol",
						_settings.BuyingMatrixAction,
						_settings.BuyingMatrixType,
						_settings.BuyingMatrix.Value));
				}

				if (_settings.OfferMatrix.HasValue) {
					declaretions.Add(new MatrixDeclaretion(true, "mol",
						_settings.OfferMatrixAction,
						_settings.OfferMatrixType,
						_settings.OfferMatrix.Value));
				}
			}

			var parts = new SqlParts {
				Select = "0"
			};
			if (declaretions.Count > 0) {
				parts.Having = "having BuyingMatrixType <> 3";
			}
			foreach (var declaretion in declaretions) {
				parts.Join += declaretion.Join();
				parts.JoinWithoutProducer += declaretion.JoinWithoutProducer();
				parts.Select = declaretion.Select(parts.Select);
			}
			parts.Select = ", " + parts.Select + " as BuyingMatrixType";
			return parts;
		}
	}
}