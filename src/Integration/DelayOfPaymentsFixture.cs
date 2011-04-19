using System;
using System.Linq;
using Castle.ActiveRecord;
using MySql.Data.MySqlClient;
using NUnit.Framework;
using PrgData.Common;
using Test.Support;

namespace Integration
{
	[TestFixture]
	public class DelayOfPaymentsFixture
	{

		private string UniqueId;

		[SetUp]
		public void SetUp()
		{
			UniqueId = "123";
			ServiceContext.GetUserHost = () => "127.0.0.1";
			UpdateHelper.GetDownloadUrl = () => "http://localhost/";
			ServiceContext.GetResultPath = () => "results\\";
		}

		[Test(Description = "Получаем значения DayOfWeek")]
		public void CheckDayOfWeek()
		{
			Assert.That((int)DayOfWeek.Sunday, Is.EqualTo(0));
			Assert.That((int)DayOfWeek.Monday, Is.EqualTo(1));
			Assert.That((int)DayOfWeek.Tuesday, Is.EqualTo(2));
			Assert.That((int)DayOfWeek.Wednesday, Is.EqualTo(3));
			Assert.That((int)DayOfWeek.Thursday, Is.EqualTo(4));
			Assert.That((int)DayOfWeek.Friday, Is.EqualTo(5));
			Assert.That((int)DayOfWeek.Saturday, Is.EqualTo(6));
		}

		[Test(Description = "Пробуем загрузить отсрочку платежа из базы")]
		public void TestActiveRecordDelayOfPayment()
		{
			TestDelayOfPayment delay;
			using (new SessionScope())
			{
				delay = TestDelayOfPayment.Queryable.First();
			}
			Assert.That(delay, Is.Not.Null);
			Assert.That(delay.Id, Is.GreaterThan(0));
			var oldValue = delay.DayOfWeek;

			try
			{
				MySqlHelper.ExecuteNonQuery(
					Settings.ConnectionString(),
					"update usersettings.DelayOfPayments set DayOfWeek = 'Sunday' where Id = ?Id",
					new MySqlParameter("?Id", delay.Id));

				using (new SessionScope())
				{
					delay.Refresh();
				}
				Assert.That(delay.DayOfWeek, Is.EqualTo(DayOfWeek.Sunday));

				using (var transaction = new TransactionScope())
				{
					delay.DayOfWeek = DayOfWeek.Friday;
					delay.SaveAndFlush();
					transaction.VoteCommit();
				}

				var dayOfWeek = MySqlHelper.ExecuteScalar(
					Settings.ConnectionString(),
					"select DayOfWeek from usersettings.DelayOfPayments where Id = ?Id",
					new MySqlParameter("?Id", delay.Id));

				Assert.That(dayOfWeek, Is.Not.Null);
				Assert.That(dayOfWeek, Is.TypeOf<string>());
				Assert.That(dayOfWeek, Is.EqualTo(Enum.GetName(typeof(DayOfWeek), DayOfWeek.Friday)));
			}
			finally
			{
				using (var transaction = new TransactionScope())
				{
					delay.DayOfWeek = oldValue;
					delay.SaveAndFlush();
					transaction.VoteCommit();
				}
			}

		}
	
	}
}