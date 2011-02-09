using System;
using System.Net.Mail;
using System.Configuration;
using System.Text;
using log4net;
using System.IO;

namespace PrgData.Common
{
	public class MailHelper
	{
		private static ILog logger = LogManager.GetLogger(typeof(MailHelper));

		public static void Mail(string messageText, string subject, string attachment, string attachmentName)
		{
			try
			{
				var MailAddress = new MailAddress("service@analit.net", "Сервис AF", Encoding.UTF8);
				var message = new MailMessage("service@analit.net", ConfigurationManager.AppSettings["ErrorMail"]);
				var SC = new SmtpClient("box.analit.net");
				message.From = MailAddress;
				if (!String.IsNullOrEmpty(subject))
					message.Subject = "Ошибка в сервисе подготовки данных: " + subject;
				else
					message.Subject = "Ошибка в сервисе подготовки данных";
				message.SubjectEncoding = Encoding.UTF8;
				message.BodyEncoding = Encoding.UTF8;
				message.Body = messageText;

				if (!String.IsNullOrEmpty(attachment))
				{
					var stream = new MemoryStream();
					var writer = new StreamWriter(stream);
					writer.Write(attachment);
					writer.Flush();
					stream.Position = 0;
					if (String.IsNullOrEmpty(attachmentName))
						attachmentName = "Attachment.txt";
					message.Attachments.Add(new Attachment(stream, attachmentName));
				}
				SC.Send(message);
			}
			catch (Exception exception)
			{
				logger
					.ErrorFormat(
						"Ошибка при отправке письма:{0}\r\nТема:{1}\r\nТело письма:{2}",
						exception,
						subject,
						messageText);
			}
		}

		public static void Mail(string messageText, string subject)
		{
			Mail(messageText, subject, null, null);
		}

	}
}