using System;
using Common.Models;
using Common.MySql;
using NHibernate;
using NHibernate.Cfg;
using NHibernate.Mapping.Attributes;
using PrgData.Common.Models;
using SmartOrderFactory.Domain;

namespace PrgData.Common.Config.Initializers
{
	public class NHibernate
	{
		public Configuration Configuration { get; set; }
		public ISessionFactory Factory { get; set; }
		public SessionFactoryHolder Holder {get; set; }

		public NHibernate()
		{
			Holder = new SessionFactoryHolder(ConnectionHelper.GetConnectionName());
			Holder
				.Configuration
				.AddInputStream(HbmSerializer.Default.Serialize(typeof(Client).Assembly))
				.AddInputStream(HbmSerializer.Default.Serialize(typeof(SmartOrderRule).Assembly))
				.AddInputStream(HbmSerializer.Default.Serialize(typeof(AnalitFVersionRule).Assembly));
			Configuration = Holder.Configuration;
		}

		public void Init()
		{
			Factory = Holder.SessionFactory;
		}
	}
}