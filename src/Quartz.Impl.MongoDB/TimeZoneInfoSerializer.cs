using System;

using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;

namespace Quartz.Impl.MongoDB
{
    public class TimeZoneInfoSerializer : IBsonSerializer
    {
        public object Deserialize(BsonReader bsonReader, Type nominalType, IBsonSerializationOptions options)
        {
            return Deserialize(bsonReader);
        }

        public object Deserialize(BsonReader bsonReader, Type nominalType, Type actualType, IBsonSerializationOptions options)
        {
            return Deserialize(bsonReader);
        }

        private static object Deserialize(BsonReader bsonReader)
        {
            var timeZoneId = bsonReader.ReadString();
            return TimeZoneInfo.FindSystemTimeZoneById(timeZoneId);
        }

        public IBsonSerializationOptions GetDefaultSerializationOptions()
        {
            return null;
        }

        public void Serialize(BsonWriter bsonWriter, Type nominalType, object value, IBsonSerializationOptions options)
        {
            var timeZoneInfo = (TimeZoneInfo)value;
            bsonWriter.WriteString(timeZoneInfo.Id);
        }
    }
}