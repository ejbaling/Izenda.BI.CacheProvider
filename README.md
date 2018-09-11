# Izenda RedisCacheProvider (v2.0.0)

## Overview
This is a custom cache provider that utilizes the Redis cache.  

:warning: The current version of this project will only work with Izenda versions 2.4.4+


## Breaking Changes in v2.0.0 </h1>  
v2.0.0 of the RedisCacheProvider utilizes compression of the Redis cache values. If you are upgrading to this version from a lesser version, please ensure that you flush all values from the Redis cache before deploying this version.

## Installation

1. Build the project and copy the following dlls to the bin folder of your Izenda API :

   * Izenda.BI.CacheProvider.RedisCache.dll
  
   * StackExchange.Redis.dll
   
   
   
2. Remove the following dll from the bin folder of your Izenda API:
   
   * Izenda.BI.CacheProvider.Memcache.dll

   :warning: If you do not remove the default Memcache provider referenced in this step, caching may not work properly.



3. Add the RedisCache configuration to the Web.config of the API instance as detailed below.
```
<configuration>
  <configSections>
    <section name="evoPdfSettings" type="Izenda.BI.Framework.Models.Exporting.EvopdfConfiguration" />
    
    <!--Redis cache-->
    <section name="redisCacheSettings" type="Izenda.BI.CacheProvider.RedisCache.RedisCacheConfiguration" />
     
  </configSections>
  <evoPdfSettings cloudEnable="false">
      <azureCloudService server="" port="" servicePassword="" />
  </evoPdfSettings>

  <!--Redis Cache Provider-->
  <redisCacheSettings connectionString="127.0.0.1:6379" additionalOptions="abortConnect=false"/>
  
```
4. Restart the API instance
