using System.Data;
using NUnit.Framework;
using PrgData.Common;

namespace Unit
{
	[TestFixture]
	public class UpdateDataFixture
	{
		[Test]
		public void Fill_attachment_ids()
		{
			var data = new UpdateData();
			data.FillAttachmentIds(new uint[] { 0, 11 });
			Assert.That(data.AttachmentRequests.Count, Is.EqualTo(1));
			Assert.That(data.AttachmentRequests[0].AttachmentId, Is.EqualTo(11));
		}
	}
}