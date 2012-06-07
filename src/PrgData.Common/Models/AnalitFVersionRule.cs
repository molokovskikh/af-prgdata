using System;
using NHibernate.Mapping.Attributes;

namespace PrgData.Common.Models
{
	[
		Class(Table = "UserSettings.AnalitFVersionRules", Lazy = false),
		Serializable
	]
	public class AnalitFVersionRule
	{
		protected AnalitFVersionRule()
		{}

		[Id(0, Name = "Id", UnsavedValue = "0")]
		[Generator(1, Class = "native")]
		public uint Id { get; set; }

		[Property]
		public uint SourceVersion { get; set; }

		[Property]
		public uint DestinationVersion { get; set; }
	}
}