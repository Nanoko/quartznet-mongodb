using System;

using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Options;

using Quartz;

namespace AutoScout24.SMP.BackgroundServices.SmpJobs.Infrastructure.Quartz
{
    public class TimeOfDaySerializer : IBsonSerializer
    {
        public object Deserialize(BsonReader bsonReader, Type nominalType, IBsonSerializationOptions options)
        {
            return Deserialize(bsonReader);
        }

        public object Deserialize(BsonReader bsonReader, Type nominalType, Type actualType, IBsonSerializationOptions options)
        {
            return Deserialize(bsonReader);
        }

        static object Deserialize(BsonReader bsonReader)
        {
            var currentBsonType = bsonReader.GetCurrentBsonType();
            switch (currentBsonType)
            {
                case BsonType.Null:
                    return null;
                case BsonType.Document:
                    bsonReader.ReadStartDocument();
                    var hour = bsonReader.ReadInt32("Hour");
                    var minute = bsonReader.ReadInt32("Minute");
                    var second = bsonReader.ReadInt32("Second");
                    bsonReader.ReadEndDocument();
                    return new TimeOfDay(hour, minute, second);
                default:
                    throw new NotSupportedException(
                        string.Format("Bson type : {0} is not supported for TimeOfDay type property", currentBsonType));
            }
        }

        public IBsonSerializationOptions GetDefaultSerializationOptions()
        {
            return new DocumentSerializationOptions();
        }

        public void Serialize(BsonWriter bsonWriter, Type nominalType, object value, IBsonSerializationOptions options)
        {
            var timeOfDay = (TimeOfDay)value;
            bsonWriter.WriteStartDocument();
            bsonWriter.WriteInt32("Hour", timeOfDay.Hour);
            bsonWriter.WriteInt32("Minute", timeOfDay.Minute);
            bsonWriter.WriteInt32("Second", timeOfDay.Second);
            bsonWriter.WriteEndDocument();
        }
    }
}