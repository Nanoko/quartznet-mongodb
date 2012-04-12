using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MongoDB.Bson.Serialization;
using MongoDB.Bson;
using System.Reflection;

namespace Quartz.Impl.MongoDB
{
    public class JobDetailImplSerializer : IBsonSerializer
    {
        public object Deserialize(global::MongoDB.Bson.IO.BsonReader bsonReader, Type nominalType, Type actualType, IBsonSerializationOptions options)
        {
            if (nominalType != typeof(JobDetailImpl) || actualType != typeof(JobDetailImpl))
            {
                var message = string.Format("Can't deserialize a {0} from {1}.", nominalType.FullName, this.GetType().Name);
                throw new BsonSerializationException(message);
            }

            var bsonType = bsonReader.GetCurrentBsonType();
            if (bsonType == BsonType.Document)
            {
                bsonReader.ReadStartDocument();

                // Ignore _id
                BsonSerializer.Deserialize(bsonReader, typeof(JobKey));

                Assembly assembly = Assembly.Load(bsonReader.ReadString("_assembly"));
                Type type = assembly.GetType(bsonReader.ReadString("_class"));

                IJobDetail jobDetail = new JobDetailImpl(
                    bsonReader.ReadString("Name"),
                    bsonReader.ReadString("Group"),
                    type,
                    bsonReader.ReadBoolean("RequestRecovery"),
                    bsonReader.ReadBoolean("Durable"));

                bsonReader.ReadBsonType();
                jobDetail = jobDetail.GetJobBuilder()
                    .UsingJobData((JobDataMap)BsonSerializer.Deserialize(bsonReader, typeof(JobDataMap))).Build();

                bsonReader.ReadEndDocument();

                return jobDetail;
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
            JobDetailImpl item = (JobDetailImpl)value;
            bsonWriter.WriteStartDocument();

            bsonWriter.WriteName("_id");
            BsonSerializer.Serialize(bsonWriter, item.Key);

            bsonWriter.WriteString("_assembly", item.JobType.Assembly.FullName);
            bsonWriter.WriteString("_class", item.JobType.FullName);

            bsonWriter.WriteString("Name", item.Name);
            bsonWriter.WriteString("Group", item.Name);
            bsonWriter.WriteBoolean("RequestRecovery", item.RequestsRecovery);
            bsonWriter.WriteBoolean("Durable", item.Durable);

            bsonWriter.WriteName("JobDataMap");
            BsonSerializer.Serialize(bsonWriter, item.JobDataMap);

            bsonWriter.WriteEndDocument();
        }

        public void SetDocumentId(object document, object id)
        {
            throw new NotImplementedException();
        }
    }
}
