using Izenda.BI.CacheProvider.RedisCache.Serializer;
using Izenda.BI.CacheProvider.RedisCache.Utilities;
using Izenda.BI.Framework.Converters;
using log4net;
using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Izenda.BI.CacheProvider.RedisCache
{
    public class RedisCache
    {
        private static RedisCache instance;
        private readonly IDatabase cache;
        private readonly IServer server;
        private readonly JsonSerializerSettings serializerSettings;
        private readonly JsonSerializer serializer;
        private readonly ILog logger;

        private RedisCache()
        {
            cache = RedisHelper.Database;
            server = RedisHelper.Server;
            serializerSettings = new JsonSerializerSettings
            {
                DateTimeZoneHandling = DateTimeZoneHandling.Unspecified,
                NullValueHandling = NullValueHandling.Ignore,
                ObjectCreationHandling = ObjectCreationHandling.Replace,
                ReferenceLoopHandling = ReferenceLoopHandling.Ignore,
                Converters = new List<JsonConverter> { new ReportPartContentConverter(), new DBServerTypeSupportingConverter() }
            };
            serializer = JsonSerializer.Create(serializerSettings);
            logger = LogManager.GetLogger(this.GetType());
        }

        public static RedisCache Instance
        {
            get { return instance ?? (instance = new RedisCache()); }
        }

        public T Get<T>(string key)
        {
            try
            {
                var result = cache.StringGet(key);
                if (result.IsNullOrEmpty)
                    return default;

                return this.Deserialize<T>(result);
            }
            catch (Exception ex)
            {
                logger.Error($"Redis Cache error occured during GET of key, {key}", ex);
                return default;
            }
        }

        public object Get(string key, Type type)
        {
            try
            {
                var result = cache.StringGet(key);
                if (result.IsNullOrEmpty)
                    return default;

                return this.Deserialize(result, type);
            }
            catch (Exception ex)
            {
                logger.Error($"Redis Cache error occured during GET of key, {key}", ex);
                return default;
            }
        }

        public void Set<T>(string key, T value)
        {
            try
            {
                var json = this.Serialize(value);
                cache.StringSet(key, json);
            }
            catch (Exception ex)
            {
                logger.Error($"Redis Cache error occured during SET of key, {key}", ex);
            }
        }

        public void SetWithLifetime(string key, object value, TimeSpan expiration)
        {
            try
            {
                var json = this.Serialize(value);
                cache.StringSet(key, json, expiration);
            }
            catch (Exception ex)
            {
                logger.Error($"Redis Cache error occured during SET with lifetime of key, {key}", ex);
            }
        }

        public bool Contains(string key)
        {
            try
            {
                return cache.KeyExists(key);
            }
            catch (Exception ex)
            {
                logger.Error($"Redis Cache error occured during CONTAINS of key, {key}", ex);
                return false;
            }
        }

        public void Remove(string key)
        {
            try
            {
                cache.KeyDelete(key);
            }
            catch (Exception ex)
            {
                logger.Error($"Redis Cache error occured during REMOVE of key, {key}", ex);
            }
        }

        public void RemoveWithPattern(string pattern)
        {
            try
            {
                var keysToRemove = server.Keys(cache.Database, $"*{pattern}*").ToArray();
                foreach (var key in keysToRemove)
                {
                    cache.KeyDelete(key);
                }
            }
            catch (Exception ex)
            {
                logger.Error($"Redis Cache error occured during REMOVE of keys with pattern, {pattern}", ex);
            }
        }

        private string Serialize(object value)
        {
            var json = JsonConvert.SerializeObject(value, serializerSettings);
            return json;
        }

        private T Deserialize<T>(string serialized)
        {
            return JsonConvert.DeserializeObject<T>(serialized, serializerSettings);
        }

        private object Deserialize(string serialized, Type type)
        {
            return JsonConvert.DeserializeObject(serialized, type, serializerSettings);
        }
    }
}
