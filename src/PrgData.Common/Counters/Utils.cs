using System;
using System.Collections.Generic;
using System.Text;
using System.Reflection;
using System.Configuration;
using System.Net.Mail;
using Common.MySql;
using PrgData.Common;
using MySql.Data.MySqlClient;
using System.ComponentModel;

namespace PrgData.Common.Counters
{
	public class Utils
	{
		public static void Execute(string CommandText)
		{
			Execute(CommandText, null);
		}

		public static int Execute(string CommandText, object ParametersAsAnonymousObject)
		{
			using (var connection = new MySqlConnection(ConnectionHelper.GetConnectionString())) {
				connection.Open();
				var command = new MySqlCommand(CommandText, connection);
				BindParameters(command, ParametersAsAnonymousObject);
				return command.ExecuteNonQuery();
			}
		}

		public static IList<ClientStatus> Request(string CommandText, object ParametersAsAnonymousObject)
		{
			using (var connection = new MySqlConnection(ConnectionHelper.GetConnectionString())) {
				connection.Open();
				var command = new MySqlCommand(CommandText, connection);
				BindParameters(command, ParametersAsAnonymousObject);
				var statuses = new List<ClientStatus>();
				using (var Reader = command.ExecuteReader()) {
					while (Reader.Read()) {
						statuses.Add(new ClientStatus(Reader.GetInt32("Id"), Reader.GetUInt32("UserId"), Reader.GetString("MethodName"), Reader.GetDateTime("StartTime")));
					}
				}
				return statuses;
			}
		}

		public static T RequestScalar<T>(string CommandText)
		{
			using (var connection = new MySqlConnection(ConnectionHelper.GetConnectionString())) {
				connection.Open();
				var command = new MySqlCommand(CommandText, connection);

				var result = command.ExecuteScalar();
				return (T)TypeDescriptor.GetConverter(result.GetType()).ConvertTo(result, typeof(T));
			}
		}

		public static T RequestScalar<T>(string CommandText, object ParametersAsAnonymousObject)
		{
			using (var connection = new MySqlConnection(ConnectionHelper.GetConnectionString())) {
				connection.Open();
				var command = new MySqlCommand(CommandText, connection);
				BindParameters(command, ParametersAsAnonymousObject);

				var result = command.ExecuteScalar();
				return (T)TypeDescriptor.GetConverter(result.GetType()).ConvertTo(result, typeof(T));
			}
		}

		public static IList<ClientStatus> Request(string CommandText)
		{
			return Request(CommandText, null);
		}

		public static void BindParameters(MySqlCommand Command, object ParametersAsAnonymousObject)
		{
			if (ParametersAsAnonymousObject != null) {
				foreach (var PropertyInfo in ParametersAsAnonymousObject.GetType().GetProperties(BindingFlags.GetProperty | BindingFlags.Public | BindingFlags.Instance)) {
					var Value = PropertyInfo.GetValue(ParametersAsAnonymousObject, null);
					Command.Parameters.AddWithValue("?" + PropertyInfo.Name, Value);
				}
			}
		}
	}
}