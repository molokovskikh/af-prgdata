using Common.MySql;
using Integration.BaseTests;
using NUnit.Framework;
using PrgData.Common;
using PrgData.Common.Models;

namespace Integration.Models
{
	[TestFixture]
	public class MailsExportFixture : BaseExportFixture
	{
		[Test]
		public void CheckAllowedArchiveRequest()
		{
			With.Connection(c => {
				var mailsExport = new MailsExport(updateData, c, files);

				Assert.That(mailsExport.AllowArchiveFiles(RequestType.GetData), Is.True);
				Assert.That(mailsExport.AllowArchiveFiles(RequestType.GetCumulative), Is.True);
				Assert.That(mailsExport.AllowArchiveFiles(RequestType.PostOrderBatch), Is.True);
				Assert.That(mailsExport.AllowArchiveFiles(RequestType.GetDataAsync), Is.True);
				Assert.That(mailsExport.AllowArchiveFiles(RequestType.GetCumulativeAsync), Is.True);
				Assert.That(mailsExport.AllowArchiveFiles(RequestType.GetLimitedCumulative), Is.True);
				Assert.That(mailsExport.AllowArchiveFiles(RequestType.GetLimitedCumulativeAsync), Is.True);
				Assert.That(mailsExport.AllowArchiveFiles(RequestType.RequestAttachments), Is.True);

				Assert.That(mailsExport.AllowArchiveFiles(RequestType.ResumeData), Is.False);
				Assert.That(mailsExport.AllowArchiveFiles(RequestType.SendOrder), Is.False);
				Assert.That(mailsExport.AllowArchiveFiles(RequestType.Forbidden), Is.False);
				Assert.That(mailsExport.AllowArchiveFiles(RequestType.Error), Is.False);
				Assert.That(mailsExport.AllowArchiveFiles(RequestType.CommitExchange), Is.False);
				Assert.That(mailsExport.AllowArchiveFiles(RequestType.GetDocs), Is.False);
				Assert.That(mailsExport.AllowArchiveFiles(RequestType.SendWaybills), Is.False);
				Assert.That(mailsExport.AllowArchiveFiles(RequestType.SendOrders), Is.False);
				Assert.That(mailsExport.AllowArchiveFiles(RequestType.PostPriceDataSettings), Is.False);
				Assert.That(mailsExport.AllowArchiveFiles(RequestType.GetHistoryOrders), Is.False);
				Assert.That(mailsExport.AllowArchiveFiles(RequestType.ConfirmUserMessage), Is.False);
				Assert.That(mailsExport.AllowArchiveFiles(RequestType.SendUserActions), Is.False);
			});
		}
	}
}