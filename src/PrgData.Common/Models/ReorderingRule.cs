using System;
using NHibernate.Mapping.Attributes;

namespace PrgData.Common.Models
{
	[
		Class(Table = "ReorderingRules", Schema = "UserSettings", Lazy = false),
		Serializable
	]
	public class ReorderingRule
	{
		public ReorderingRule()
		{
		}

		public ReorderingRule(DayOfWeek dayOfWeek, int hour)
		{
			DayOfWeek = dayOfWeek;
			TimeOfStopsOrders = new TimeSpan(hour, 0, 0);
		}

		[Id(0, Name = "Id", UnsavedValue = "0")]
		[Generator(1, Class = "native")]
		public uint Id { get; set; }

		[Property]
		public uint RegionalDataId { get; set; }

		[Property(Type = "NHibernate.Type.EnumStringType`1[[System.DayOfWeek]], NHibernate")]
		public DayOfWeek DayOfWeek { get; set; }

		[Property]
		public TimeSpan? TimeOfStopsOrders { get; set; }

		//Можно ли рассчитать время окончания приема заявок относительно текущего правила?
		public bool AllowCalcTimeOfStopsOrders {
			get { return TimeOfStopsOrders.HasValue; }
		}

		public DateTime GetTimeOfStopsOrders(DateTime currentOrderTime)
		{
			if (!AllowCalcTimeOfStopsOrders)
				throw new InvalidOperationException("Для данного правила нельзя рассчитать дату окончания приема заявок, т.к. не опеределено время окончания приема заявок.");

			//Если указано время окончания у правила, то добавляем интервал к началу дня
			if (TimeOfStopsOrders.Value.Ticks > 0)
				return currentOrderTime.Date.Add(TimeOfStopsOrders.Value);

			//Если ничего не сработало, то возвращаем начало следующего дня
			return currentOrderTime.Date.AddDays(1);
		}
	}
}