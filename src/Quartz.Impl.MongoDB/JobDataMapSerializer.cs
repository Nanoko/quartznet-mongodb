using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MongoDB.Bson.Serialization;
using MongoDB.Bson;

namespace Quartz.Impl.MongoDB
{
    public class JobDataMapSerializer : IBsonSerializer
    {
        public object Deserialize(global::MongoDB.Bson.IO.BsonReader bsonReader, Type nominalType, Type actualType, IBsonSerializationOptions options)
        {
            if (nominalType != typeof(JobDataMap) || actualType != typeof(JobDataMap))
            {
                var message = string.Format("Can't deserialize a {0} with {1}.", nominalType.FullName, this.GetType().Name);
                throw new BsonSerializationException(message);
            }

            var bsonType = bsonReader.CurrentBsonType;
            if (bsonType == BsonType.Document)
            {
                JobDataMap item = new JobDataMap();
                bsonReader.ReadStartDocument();

                while (bsonReader.ReadBsonType() != BsonType.EndOfDocument)
                {
                    string key = bsonReader.ReadName();
                    object value = BsonSerializer.Deserialize<object>(bsonReader);
                    item.Add(key, value);
                }

                bsonReader.ReadEndDocument();

                return item;
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
            JobDataMap item = (JobDataMap)value;
            bsonWriter.WriteStartDocument();

            foreach (KeyValuePair<string, object> data in item.Values)
            {
                bsonWriter.WriteName(data.Key);
                BsonSerializer.Serialize(bsonWriter, data.Value);
            }
            
            bsonWriter.WriteEndDocument();
        }

        public void SetDocumentId(object document, object id)
        {
            throw new NotImplementedException();
        }
    }
}
