using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Composition;
using Izenda.BI.Cache.Contracts;
using Izenda.BI.Cache.Metadata.Constants;
using Izenda.BI.Core;

namespace Izenda.BI.CacheProvider.RedisCache
{
    [Export(typeof(ICacheStore))]
    [ExportMetadata("CacheStore", "RedisCacheStore")]
    [ExportMetadata("CacheStoreType", CacheType.SystemCache)]
    public class RedisCacheSystemStore : RedisCacheStore
    {
        public RedisCacheSystemStore()
            : base(true)
        {
            this.SetTimeToLive(CacheConfiguration.Instance.CurrentSetting.SystemCacheTTL);
        }

        public override CacheType CacheType => CacheType.SystemCache;

        public override string chaceDirectory => throw new NotImplementedException();

        public override Task ExecuteReloadCacheData(Dictionary<Guid, object> datasourceAdaptors, int loadDuration)
        {
            return Task.CompletedTask;
        }

        public override Task ExecuteRestoreFromMetadata(Dictionary<Guid, object> datasourceAdaptors)
        {
            return Task.CompletedTask;
        }

        protected override void SetTimeToLive(int timeToLive)
        {
            base.TimeToLive = Math.Max(timeToLive, MinSystemCacheTTL);
        }
    }
}
