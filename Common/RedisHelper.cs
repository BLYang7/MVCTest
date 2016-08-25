using ServiceStack.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace MVCTest.Common
{
    public class RedisHelper
    {
        private static string RedisPath = "127.0.0.1";

        /// <summary>
        /// Redis连接池
        /// </summary>
        private static PooledRedisClientManager _redisClientPool;

        //锁
        private static readonly object _lockObj = new object();

        /// <summary>
        /// 单例模式下初始化Redis连接池
        /// </summary>
        private static PooledRedisClientManager RedisClientPool
        {
            get
            {
                if (_redisClientPool == null)
                {
                    lock (_lockObj)
                    {
                        if (_redisClientPool == null)
                        {
                            try
                            {
                                _redisClientPool = new PooledRedisClientManager(RedisPath);
                            }
                            catch (Exception ex)
                            {
                                _redisClientPool = null;
                            }
                        }
                    }
                }
                return _redisClientPool;
            }
        }


        #region 查询
        /// <summary>
        /// 获取对象
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <returns></returns>
        public T Get<T>(string key)
        {
            try
            {
                using (var client = RedisClientPool.GetClient())
                {
                    return client.Get<T>(key);
                }
            }
            catch (Exception ex)
            {
                //LogTool.WriteErrorLocal("redis读取失败", "KeyValueRedisHelper.Get<T>", ex);
                return default(T);
            }
        }
        #endregion

        #region 增/改
        /// <summary>
        /// 设置对象
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="item"></param>
        /// <returns></returns>
        public bool Set<T>(string key, T item)
        {
            try
            {
                using (var client = RedisClientPool.GetClient())
                {
                    return client.Set(key, item);
                }
            }
            catch (Exception ex)
            {
                //LogTool.WriteErrorLocal("redis设置失败", "KeyValueRedisHelper.Set<T>", ex);
                return false;
            }
        }
        #endregion

        #region 删除
        /// <summary>
        /// 删除指定key
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public bool Delete(string key)
        {
            try
            {
                using (var client = RedisClientPool.GetClient())
                {
                    return client.Remove(key);
                }
            }
            catch (Exception)
            {
                return false;
            }
        }
        #endregion

        #region 设置过期
        /// <summary>
        /// 设置过期
        /// </summary>
        /// <param name="key"></param>
        /// <param name="expirein">时间跨度</param>
        /// <returns></returns>
        public bool SetExpire(string key, TimeSpan expirein)
        {
            try
            {
                using (var client = RedisClientPool.GetClient())
                {
                    return client.ExpireEntryIn(key, expirein);
                }
            }
            catch
            {
                return false;
            }
        }
        #endregion

    }
}