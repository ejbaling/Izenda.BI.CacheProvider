using Izenda.BI.Cache;
using Izenda.BI.Cache.Contracts;
using Izenda.BI.Cache.Schedule.Job;
using Izenda.BI.Core;
using Izenda.BI.Core.Factories;
using Izenda.BI.Framework.Contracts;
using Izenda.BI.Framework.Models.Setting;
using Izenda.BI.Logic.CustomConfiguration;
using Izenda.BI.Logic.Job;
using Izenda.BI.SystemRepository;
using Izenda.BI.Utility;
using Quartz;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Izenda.BI.CacheProvider.RedisCache
{
    public abstract class RedisCacheStore : CacheStore, ICacheStore
    {
        protected ISystemRepository Repository { get; }

        protected RedisCacheStore(bool isEnabled)
            : base(isEnabled)
        {
            var systemRepositories = ServiceLocator.GetMany<Lazy<ISystemRepository, ServerTypeMetaData>>().ToList();
            Repository = systemRepositories.GetLazyItem();
            CacheConfiguration.Instance.OnDataCacheSettingChanged += OnDataCacheConfigurationChanged;
            if (isEnabled)
                ExecuteRestoreCacheDataJob();
        }

        public CacheItemContainer Get<T>(string key)
        {
            return RedisCache.Instance.Get<CacheItemContainer<T>>(key);
        }

        public void Remove(string key)
        {
            RedisCache.Instance.Remove(key);
        }

        public void Set(string key, CacheItemContainer data)
        {
            if (isEnabled)
                RedisCache.Instance.Set(key, data);

            UpdateCacheItems(key, data);
        }

        public bool TryGetValue<T>(string key, out CacheItemContainer data)
        {
            data = null;
            if (RedisCache.Instance.Contains(key))
            {
                data = RedisCache.Instance.Get<CacheItemContainer<T>>(key);
                if (data == null || data.IsExpired(TimeToLive))
                {
                    data = null;
                    return false;
                }

                return true;
            }

            return false;
        }

        public bool TryGetValue(string key, Type type, out CacheItemContainer data)
        {
            data = null;
            if (RedisCache.Instance.Contains(key))
            {
                var genericContainerType = genericCacheItemContainer.GetOrAdd(type, t =>
                {
                    return typeof(CacheItemContainer<>).MakeGenericType(t);
                });

                data = RedisCache.Instance.Get(key, genericContainerType) as CacheItemContainer;
                if (data == null || data.IsExpired(TimeToLive))
                {
                    data = null;
                    return false;
                }

                return true;
            }

            return false;
        }

        public override void Enable()
        {
            base.Enable();
        }

        public override void Disable()
        {
            base.Disable();
            this.ClearAll();
        }

        public override void ClearAll()
        {
            var metadataItems = Repository.GetIzendaCacheMetadata((int)this.CacheType);
            foreach (var items in metadataItems)
            {
                Remove(items.CacheKey);
            }

            if (Repository != null)
            {
                Repository.DeleteAllIzendaCacheMetadata();
            }
        }

        public override Task ExecuteEviction()
        {
            return Task.Factory.StartNew(() =>
            {
                var expiredItems = cacheMetadataItems
                    .Where(x => x.IsExpired(TimeToLive, DateTime.UtcNow) && !x.IsRemoved)
                    .ToArray();

                lock (lockCacheStore)
                {
                    foreach (var cacheMeta in expiredItems)
                    {
                        RedisCache.Instance.Remove(cacheMeta.CacheKey);
                        cacheMeta.IsRemoved = true;
                    }
                }
            });
        }

        public override Task BackupCacheMetadata()
        {
            return Task.CompletedTask;
        }

        private void ExecuteRestoreCacheDataJob()
        {
            var jobName = CacheType.ToString() + "RestoreCacheDataJob";
            var jobGroup = "RestoreCacheDataGroup";

            var job = JobBuilder.Create<RestoreCacheDataJob>()
            .WithIdentity(jobName, jobGroup)
            .UsingJobData("CacheType", (int)CacheType)
            .Build();

            var triggerName = CacheType.ToString() + "RestoreCacheDataTrigger";
            var trigger = TriggerBuilder.Create()
                .WithIdentity(triggerName, jobGroup)
                .StartNow()
                .Build();

            var jobManager = Container.Resolve<IJobManager>();
            jobManager.ScheduleJob(job, trigger);
        }

        public abstract Task ExecuteRestoreFromMetadata(Dictionary<Guid, object> datasourceAdaptors);

        public abstract Task ExecuteReloadCacheData(Dictionary<Guid, object> datasourceAdaptors, int loadDuration);

        protected abstract void SetTimeToLive(int timeToLive);

        protected void OnDataCacheConfigurationChanged(object sender, SettingChangedEventArgs<DataCacheSetting> setting)
        {
            if (setting.NewSetting == null) return;

            if (TimeToLive != setting.NewSetting.TimeToLive)
            {
                SetTimeToLive(setting.NewSetting.TimeToLive);
            }
        }
    }
}