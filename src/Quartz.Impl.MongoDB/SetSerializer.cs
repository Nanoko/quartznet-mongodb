using System;
using System.Collections.Generic;

using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;

namespace Quartz.Impl.MongoDB
{
    public class SetSerializer<T> : IBsonSerializer
    {
        private IBsonSerializer enumerableSerializer;

        public SetSerializer()
        {
            enumerableSerializer = BsonSerializer.LookupSerializer(typeof(IEnumerable<T>));
        }

        public object Deserialize(BsonReader bsonReader, Type nominalType, IBsonSerializationOptions options)
        {
            return Deserialize(bsonReader, options);
        }

        public object Deserialize(BsonReader bsonReader, Type nominalType, Type actualType, IBsonSerializationOptions options)
        {
            return Deserialize(bsonReader, options);
        }

        private object Deserialize(BsonReader bsonReader, IBsonSerializationOptions options)
        {
            var enumerable = (IEnumerable<T>)enumerableSerializer.Deserialize(bsonReader, typeof(ISet<T>), typeof(HashSet<T>), options);
            return new Quartz.Collection.HashSet<T>(enumerable);
        }

        public IBsonSerializationOptions GetDefaultSerializationOptions()
        {
            return enumerableSerializer.GetDefaultSerializationOptions();
        }

        public void Serialize(BsonWriter bsonWriter, Type nominalType, object value, IBsonSerializationOptions options)
        {
            var hashSet = (Collection.HashSet<T>)value;
            enumerableSerializer.Serialize(bsonWriter, typeof(ISet<T>), new HashSet<T>(hashSet), options);
        }
    }
}