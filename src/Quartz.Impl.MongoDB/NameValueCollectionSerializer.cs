using System;
using System.Collections.Specialized;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;

namespace Elmah
{
	public class NameValueCollectionSerializer : BsonBaseSerializer
	{
		private static readonly NameValueCollectionSerializer instance = new NameValueCollectionSerializer();
		public static NameValueCollectionSerializer Instance
		{
			get { return instance; }
		}

    public override object Deserialize(BsonReader bsonReader, Type nominalType, Type actualType, IBsonSerializationOptions options)
    {
      return Deserialize(bsonReader, nominalType, options);
    }

		public override object Deserialize(
			BsonReader bsonReader,
			Type nominalType,
			IBsonSerializationOptions options
			)
		{
			var nvc = new NameValueCollection();
			bsonReader.ReadStartDocument();
			while (bsonReader.ReadBsonType() != BsonType.EndOfDocument)
			{
				var name = bsonReader.ReadName().Replace("__period__", ".");
				var value = bsonReader.ReadString();
				nvc.Add(name, value);
			}
			bsonReader.ReadEndDocument();
			return nvc;
		}

		public override void Serialize(
			BsonWriter bsonWriter,
			Type nominalType,
			object value,
			IBsonSerializationOptions options
			)
		{
			var nvc = (NameValueCollection)value;
			bsonWriter.WriteStartDocument();
			if (nvc != null && nvc.Count > 0)
			{
				foreach (var key in nvc.AllKeys)
				{
					bsonWriter.WriteString(key.Replace(".", "__period__"), nvc[key]);
				}
			}
			bsonWriter.WriteEndDocument();
		}
	}
}