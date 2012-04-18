using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using Quartz.Impl.Calendar;

namespace Quartz.Impl.MongoDB
{
    public class CalendarWrapper : IBsonSerializable
    {
        
        public string Name { get; set; }
        public ICalendar Calendar { get; set; }

        public object Deserialize(global::MongoDB.Bson.IO.BsonReader bsonReader, Type nominalType, IBsonSerializationOptions options)
        {
            CalendarWrapper item = new CalendarWrapper();
            
            bsonReader.ReadStartDocument();
            item.Name = bsonReader.ReadString("_id");
            byte[] byteArray;
            BsonBinarySubType binarySubType;
            bsonReader.ReadBinaryData("ContentStream", out byteArray, out binarySubType);
            item.Calendar = (ICalendar)new BinaryFormatter().Deserialize(new MemoryStream(byteArray));
            bsonReader.ReadEndDocument();
            
            return item;
        }

        public bool GetDocumentId(out object id, out Type idNominalType, out IIdGenerator idGenerator)
        {
            id = this.Name;
            idNominalType = typeof(string);
            idGenerator = null;

            return true;
        }

        public void Serialize(global::MongoDB.Bson.IO.BsonWriter bsonWriter, Type nominalType, IBsonSerializationOptions options)
        {
            bsonWriter.WriteStartDocument();
            bsonWriter.WriteString("_id", this.Name);
            MemoryStream stream = new MemoryStream();
            new BinaryFormatter().Serialize(stream, this.Calendar);
            bsonWriter.WriteBinaryData("ContentStream", stream.ToArray(), BsonBinarySubType.Binary);
            bsonWriter.WriteEndDocument();
        }

        public void SetDocumentId(object id)
        {
            throw new NotImplementedException();
        }
    }
}
