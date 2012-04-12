using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MongoDB.Bson.Serialization;
using MongoDB.Bson;
using System.Reflection;
using Quartz.Impl.Triggers;

namespace Quartz.Impl.MongoDB
{
    public class SimpleTriggerImplSerializer : IBsonSerializer
    {
        public object Deserialize(global::MongoDB.Bson.IO.BsonReader bsonReader, Type nominalType, Type actualType, IBsonSerializationOptions options)
        {
            if (nominalType != typeof(SimpleTriggerImpl) || actualType != typeof(SimpleTriggerImpl))
            {
                var message = string.Format("Can't deserialize a {0} with {1}.", nominalType.FullName, this.GetType().Name);
                throw new BsonSerializationException(message);
            }

            var bsonType = bsonReader.GetCurrentBsonType();
            if (bsonType == BsonType.Document)
            {
                bsonReader.ReadStartDocument();

                // Ignore _id
                bsonReader.ReadString("_id");

                // Ignore _t
                bsonReader.ReadString("_t");

                Spi.IOperableTrigger trigger = new SimpleTriggerImpl(
                    bsonReader.ReadString("Name"),
                    bsonReader.ReadString("Group"),
                    bsonReader.ReadString("JobName"),
                    bsonReader.ReadString("JobGroup"),
                    new DateTimeOffset(new DateTime(bsonReader.ReadInt64("StartDate"))),
                    ReadNullableDateTimeOffset(bsonReader, "EndDate"),
                    bsonReader.ReadInt32("RepeatCount"),
                    new TimeSpan(bsonReader.ReadInt64("RepeatInterval")));

                bsonReader.ReadBsonType();
                trigger.JobDataMap = (JobDataMap)BsonSerializer.Deserialize(bsonReader, typeof(JobDataMap));

                bsonReader.ReadName("Description");
                if (bsonReader.GetCurrentBsonType() == BsonType.String)
                {
                    trigger.Description = bsonReader.ReadString();
                }
                else
                {
                    bsonReader.ReadNull();
                }

                trigger.Priority = bsonReader.ReadInt32("Priority");

                // Ignore State
                bsonReader.ReadString("State");

                bsonReader.ReadEndDocument();

                return trigger;
            }
            else if (bsonType == BsonType.Null)
            {
                bsonReader.ReadNull();
                return null;
            }
            else
            {
                var message = string.Format("Can't deserialize a {0} from BsonType {1}.", nominalType.FullName, bsonType);
                throw new BsonSerializationException(message);
            }
        }

        private DateTimeOffset? ReadNullableDateTimeOffset(global::MongoDB.Bson.IO.BsonReader bsonReader, string name)
        {
            bsonReader.ReadName(name);
            if (bsonReader.GetCurrentBsonType() == BsonType.DateTime)
            {
                return new DateTimeOffset(new DateTime(bsonReader.ReadInt64()));
            }

            bsonReader.ReadNull();
            return null;
        }

        public object Deserialize(global::MongoDB.Bson.IO.BsonReader bsonReader, Type nominalType, IBsonSerializationOptions options)
        {
            return this.Deserialize(bsonReader, nominalType, nominalType, options);
        }

        public IBsonSerializationOptions GetDefaultSerializationOptions()
        {
            throw new NotImplementedException();
        }

        public bool GetDocumentId(object document, out object id, out Type idNominalType, out IIdGenerator idGenerator)
        {
            throw new NotImplementedException();
        }

        public BsonSerializationInfo GetItemSerializationInfo()
        {
            throw new NotImplementedException();
        }

        public BsonSerializationInfo GetMemberSerializationInfo(string memberName)
        {
            throw new NotImplementedException();
        }

        public void Serialize(global::MongoDB.Bson.IO.BsonWriter bsonWriter, Type nominalType, object value, IBsonSerializationOptions options)
        {
            SimpleTriggerImpl item = (SimpleTriggerImpl)value;
            bsonWriter.WriteStartDocument();

            bsonWriter.WriteString("_id", item.Key.ToString());

            bsonWriter.WriteString("_t", "SimpleTriggerImpl");

            bsonWriter.WriteString("Name", item.Name);
            bsonWriter.WriteString("Group", item.Group);
            bsonWriter.WriteString("JobName", item.JobName);
            bsonWriter.WriteString("JobGroup", item.JobGroup);
            bsonWriter.WriteInt64("StartDate", item.StartTimeUtc.UtcTicks);
            if (item.EndTimeUtc != null)
            {
                bsonWriter.WriteInt64("EndDate", ((DateTimeOffset)item.EndTimeUtc).UtcTicks);
            }
            else
            {
                bsonWriter.WriteNull("EndDate");
            }

            bsonWriter.WriteInt32("RepeatCount", item.RepeatCount);
            bsonWriter.WriteInt64("RepeatInterval", item.RepeatInterval.Ticks);

            bsonWriter.WriteName("JobDataMap");
            BsonSerializer.Serialize(bsonWriter, item.JobDataMap);

            if (item.Description != null)
            {
                bsonWriter.WriteString("Description", item.Description);
            }
            else
            {
                bsonWriter.WriteNull("Description");
            }

            bsonWriter.WriteInt32("Priority", item.Priority);
            bsonWriter.WriteString("State", TriggerState.Normal.ToString());

            bsonWriter.WriteEndDocument();
        }

        public void SetDocumentId(object document, object id)
        {
            throw new NotImplementedException();
        }
    }
}
