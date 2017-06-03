using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SignalR.Utils.Redis
{
    /// <summary>
    /// Redis base class
    /// </summary>
    public class RedisService : RedisConfig
    {


        /// <summary>
        /// 取得DB
        /// </summary>
        public static IDatabase GetDatabase(int dbIndex)
        {
            return Multiplexer.GetDatabase(dbIndex);

        }

        /// <summary>
        /// 刪除key
        /// </summary>
        /// <param name="dbIndex"></param>
        /// <param name="key"></param>
        public static bool KeyDelete(int dbIndex, string key)
        {
            var conn = GetDatabase(dbIndex);
            var task = conn.KeyDeleteAsync(key);
            Multiplexer.Wait(task);
            return task.Result;
        }


        /// <summary>
        /// 查詢 key 是否存在
        /// </summary>
        /// <param name="dbIndex"></param>
        /// <param name="key"></param>
        public static bool KeyExist(int dbIndex, string key)
        {
            var conn = GetDatabase(dbIndex);
            var task = conn.KeyExistsAsync(key);
            Multiplexer.Wait(task);
            return task.Result;
        }

        /// <summary>
        /// 查詢所有 key 
        /// </summary>
        /// <param name="dbIndex"></param>
        public static List<string> KeySelect(int dbIndex)
        {
            var list = Multiplexer.GetServer(Config.EndPoints.First(endpoint =>
                                                                    !Multiplexer.GetServer(endpoint).IsSlave)).Keys(dbIndex);
            IEnumerable<RedisKey> redisKeys = list as RedisKey[] ?? list.ToArray();

            return redisKeys.Select(item => item.ToString()).ToList();
        }

        /// <summary>
        /// flushDB
        /// </summary>
        /// <param name="dbIndex"></param>
        public static void FlushDb(int dbIndex)
        {

            var server = Multiplexer.GetServer(Config.EndPoints.First(endpoint =>
                                                                      !Multiplexer.GetServer(endpoint).IsSlave));

            //it's not working
            //server.FlushDatabase(dbIndex);

            IDatabase cache = Multiplexer.GetDatabase(dbIndex);

            var keys = server.Keys(dbIndex);
            foreach (var key in keys)
            {
                cache.KeyDeleteAsync(key);
            }
        }

    }

}
  