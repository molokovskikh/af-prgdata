using System;
using System.Collections.Generic;
using PrgData.Common.Models;

namespace PrgData.Common.Orders.MinOrders
{
	public interface IMinOrderContext
	{
		/// <summary>
		/// Если значение true, то запись с правилами минимального заказа найдена
		/// и будет произведена проверка на минимальный заказ
		/// </summary>
		bool MinReqEnabled { get; }

		// Информация о клиенте
		uint ClientId { get; }
		uint AddressId { get; }
		uint UserId { get; }

		//Информация по поставщике
		uint SupplierId { get; }
		uint PriceCode { get; }
		ulong RegionCode { get; }

		/// <summary>
		/// Наименование поставщика
		/// </summary>
		string SupplierName { get; }
		/// <summary>
		/// Регион
		/// </summary>
		string RegionName { get; }

		/// <summary>
		/// Требуется ли выполнять проверки минимального заказа и дозаказа
		/// </summary>
		bool ControlMinReq { get; }
		/// <summary>
		/// Сумма минимального заказа
		/// </summary>
		uint MinReq { get; }
		/// <summary>
		/// Сумма минимального дозаказа
		/// </summary>
		uint MinReordering { get; }

		/// <summary>
		/// Текущая дата и время относительно региона заказа для проверки расписания дозаказа
		/// </summary>
		DateTime CurrentRegionDateTime { get; }

		/// <summary>
		/// Расписание дозаказа поставщика относительно региона
		/// </summary>
		/// <returns></returns>
		List<ReorderingRule> GetRules();

		/// <summary>
		/// Существуют ли обработанные заказы в указанный период?
		/// </summary>
		/// <param name="period"></param>
		/// <returns></returns>
		bool OrdersExists(IReorderingPeriod period);

		/// <summary>
		/// Поддерживает ли текущая версия AnalitF работу с дозаказом?
		/// </summary>
		bool SupportedMinReordering { get; }
	}
}