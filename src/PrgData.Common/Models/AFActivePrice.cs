using System;
using Common.Models;
using NHibernate.Mapping.Attributes;

namespace PrgData.Common.Models
{
	[Class(Table = "activeprices", Schema = "usersettings", SchemaAction = "none")]
	public class AFActivePrice
	{
		[CompositeId(0, Name = "Id", ClassType = typeof(PriceKey))]
		[KeyProperty(1, Name = "RegionCode", Column = "RegionCode")]
		[KeyManyToOne(2, Name = "Price", Column = "PriceCode")]
		public virtual PriceKey Id { get; set; }

		[Property]
		public virtual bool Fresh { get; set; }

		[Property]
		public virtual DateTime PriceDate { get; set; }
	}
}