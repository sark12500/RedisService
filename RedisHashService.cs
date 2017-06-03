

using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Threading.Tasks;
using System.Xml.Schema;
using StackExchange.Redis;

namespace SignalR.Utils.Redis
{

    /// <summary>
    /// 用以存取Redis Table(Hash)
    /// </summary>
    public class RedisHashService : RedisConfig
    {

        /// <summary>
        /// 取得DB
        /// </summary>
        public static IDatabase GetDatabase(int dbIndex)
        {
            return Multiplexer.GetDatabase(dbIndex);
        }

        /// <summary>
        /// 取得長度
        /// </summary>
        /// <param name="dbIndex"></param>
        /// <param name="key"></param>
        /// <returns></returns>
        public static long HashLenth(int dbIndex, string key)
        {
            var conn = GetDatabase(dbIndex);
            var task = conn.HashLengthAsync(key);
            Multiplexer.Wait(task);
            return task.Result;
        }

        /// <summary>
        /// 取得總長度 未讀+已讀
        /// </summary>
        /// <param name="dbIndex"></param>
        /// <param name="key"></param>
        /// <returns></returns>
        public static long HashLenthForBuild(string key)
        {
            var conn1 = GetDatabase(8);
            var length1 = conn1.HashLength(key);
            var conn2 = GetDatabase(9);
            var length2 = conn2.HashLength(key);
            return length1 + length2 + 1;
        }

#region 檢查是否存在
        /// <summary>
        /// 檢查是否存在 - 單筆
        /// </summary>
        /// <param name="dbIndex"></param>
        /// <param name="key"></param>
        /// <param name="hashKey"></param>
        /// <returns></returns>
        public static bool HashExist(int dbIndex, string key, string hashKey)
        {
            var conn = GetDatabase(dbIndex);
            var task = conn.HashExistsAsync(key, hashKey);
            Multiplexer.Wait(task);
            return task.Result;
        }

        /// <summary>
        /// 檢查是否存在 - 多筆
        /// </summary>
        /// <param name="dbIndex"></param>
        /// <param name="key"></param>
        /// <param name="dic"></param>
        /// <returns></returns>
        public static bool HashExist(int dbIndex, string key, Dictionary<string, string> dic)
        {

            var conn = GetDatabase(dbIndex);
            // LINQ 寫法 非同步
            //return (dic.Any(item => conn.HashExistsAsync(key, item.Key).Result));

            // LINQ 寫法 同步
            foreach (var task in dic.Select(item => conn.HashExistsAsync(key, item.Key)))
            {
                Multiplexer.Wait(task);
                if (task.Result)
                {
                    return true;
                }
            }


            return false;

        }
#endregion

#region 新增
        /// <summary>
        /// 新增hash資料 - 單筆
        /// </summary>
        /// <param name="dbIndex">資料庫索引</param>
        /// <param name="key">資料key</param>
        /// <param name="hashKey">hashKey</param>
        /// <param name="hashValue">hashValue</param>
        public static void HashInsert(int dbIndex, string key, string hashKey, string hashValue)
        {
            if (HashExist(dbIndex, key, hashKey))
                return;

            var conn = GetDatabase(dbIndex);

            var hashEntries = new[]
            {
                new HashEntry(hashKey, hashValue)
            };

            var task = conn.HashSetAsync(key, hashEntries);
            Multiplexer.Wait(task);
        }

        /// <summary>
        /// 新增hash資料 - 多筆
        /// </summary>
        /// <param name="dbIndex">資料庫索引</param>
        /// <param name="key">資料key</param>
        /// <param name="dic">資料value</param>
        public static void HashInsert(int dbIndex, string key, Dictionary<string, string> dic)
        {
            if (HashExist(dbIndex, key, dic))
                return;

            var conn = GetDatabase(dbIndex);

            var hashEntries = new HashEntry[dic.Count];
            var index = 0;

            foreach (var item in dic)
            {
                hashEntries[index] = new HashEntry(item.Key, item.Value);
                ++index;
            }

            var task = conn.HashSetAsync(key, hashEntries);
            Multiplexer.Wait(task);
        }

        /// <summary>
        /// 新增hash資料 - 多筆
        /// </summary>
        /// <param name="dbIndex">資料庫索引</param>
        /// <param name="key">資料key</param>
        /// <param name="dic">資料value</param>
        public static void HashInsertPartition(int dbIndex, string key, Dictionary<string, string> dic)
        {
   
            //if (HashExist(dbIndex, key, dic))
            //    return;

            var conn = GetDatabase(dbIndex);

            var partition = 250;
            var partitionNum = dic.Count / partition;
            var partitionLeft = dic.Count % partition;

            for (int n = 0; n <= partitionNum; ++n)
            {
                var length = 0;
                if (n == partitionNum)
                {
                    length = partitionLeft;
                }
                else
                {
                    length = partition;
                }

                var hashEntries = new HashEntry[length];
                var hashCounter = 0;
                var total = 0;
                foreach (var item in dic)
                {
                    if (total >= partition * n && total < partition * (n + 1))
                    {
                        hashEntries[hashCounter] = new HashEntry(item.Key, item.Value);
             
                        ++hashCounter;                  
                    }
                    ++total;
                }
                var task = conn.HashSetAsync(key, hashEntries);
                Multiplexer.Wait(task);   
            }
        }

#endregion

#region 刪除
        /// <summary>
        /// 刪除hash資料 - 單筆
        /// </summary>
        /// <param name="dbIndex">資料庫索引</param>
        /// <param name="key">資料key</param>
        /// <param name="hashKey">資料hashKey</param>
        public static void HashDelete(int dbIndex, string key, string hashKey)
        {

            var conn = GetDatabase(dbIndex);
            {
                var task = conn.HashDeleteAsync(key, hashKey);
                Multiplexer.Wait(task);
            }

        }

        /// <summary>
        /// 刪除hash資料 - 多筆
        /// </summary>
        /// <param name="dbIndex">資料庫索引</param>
        /// <param name="key">資料key</param>
        /// <param name="hashKeyList">欲刪除hashKey列表</param>
        public static void HashDelete(int dbIndex, string key, List<string> hashKeyList)
        {       

            var conn = GetDatabase(dbIndex);
            {
                var redisValuAry = new RedisValue[hashKeyList.Count];
                var index = 0;
             
                foreach (var item in hashKeyList)
                {
                    redisValuAry[index] = item;
                    ++index;
                }

                var task = conn.HashDeleteAsync(key, redisValuAry);
                Multiplexer.Wait(task);
            }
            
        }
#endregion

#region 修改

        /// <summary>
        /// 更新hash資料 - 單筆
        /// </summary>
        /// <param name="dbIndex">資料庫索引</param>
        /// <param name="key">資料key</param>
        /// <param name="hashKey">hashKey</param>
        /// <param name="hashValue">hashValue</param>
        public static void HashUpdate(int dbIndex, string key, string hashKey, string hashValue)
        {
            if (HashExist(dbIndex, key, hashKey) == false)
            {
                //var errMsg = string.Format("key : {0} ; hashKey : {1} ; Exist !!", key, hashKey);
                return;
            }

            var conn = GetDatabase(dbIndex);

            var hashEntries = new[]
            {
                new HashEntry(hashKey, hashValue)
            };

            var task = conn.HashSetAsync(key, hashEntries);
            Multiplexer.Wait(task);
        }

        /// <summary>
        /// 更新hash資料 多筆
        /// </summary>
        /// <param name="dbIndex">資料庫索引</param>
        /// <param name="key">資料key</param>
        /// <param name="dic">資料value</param>
        public static void HashUpdate(int dbIndex, string key, Dictionary<string, string> dic)
        {
            if (HashExist(dbIndex, key, dic) == false)
                return;

            var conn = GetDatabase(dbIndex);

            var hashEntries = new HashEntry[dic.Count];
            var index = 0;

            foreach (var item in dic)
            {
                hashEntries[index] = new HashEntry(item.Key, item.Value);
                ++index;
            }

            var task = conn.HashSetAsync(key, hashEntries);
            Multiplexer.Wait(task);
        }
#endregion

#region 查詢

        /// <summary>
        /// 查詢hash value - 單筆
        /// </summary>
        /// <param name="dbIndex">資料庫索引</param>
        /// <param name="key">資料key</param>
        /// <param name="hashKey">hashKey</param>
        public static string HashSelect(int dbIndex, string key, string hashKey)
        {
            var conn = GetDatabase(dbIndex);
            var task = conn.HashGetAsync(key, hashKey);
            Multiplexer.Wait(task);
            return task.Result;
        }

        /// <summary>
        /// 查詢hash key and value all 
        /// </summary>
        /// <param name="dbIndex">資料庫索引</param>
        /// <param name="key">資料key</param>
        public static Dictionary<string, string> HashSelectAllReturnDic(int dbIndex, string key)
        {
            var conn = GetDatabase(dbIndex);
            var task = conn.HashGetAllAsync(key);
            Multiplexer.Wait(task);
            var dic = task.Result.ToStringDictionary();
            return dic;
        }


        /// <summary>
        /// 查詢db and value all 
        /// </summary>
        /// <param name="dbIndex">資料庫索引</param>
        /// <param name="key">資料key</param>
        public static HashSet<string> HashSelectAllReturnHashSet(int dbIndex, string key)
        {
            var conn = GetDatabase(dbIndex);
            var task = conn.HashGetAllAsync(key);
            Multiplexer.Wait(task);
            var dic = task.Result.ToStringDictionary();
            var set = new HashSet<string>();

            foreach (var item in dic)
            {
                set.Add(item.Value);
            }
            return set;
        }

        /// <summary>
        /// 查詢db and key all 
        /// </summary>
        /// <param name="dbIndex">資料庫索引</param>
        /// <param name="key">資料key</param>
        public static HashSet<string> HashSelectAllReturnHashSetKey(int dbIndex, string key)
        {
            var conn = GetDatabase(dbIndex);
            var task = conn.HashGetAllAsync(key);
            Multiplexer.Wait(task);
            var dic = task.Result.ToStringDictionary();
            var set = new HashSet<string>();

            foreach (var item in dic)
            {
                set.Add(item.Key);
            }
            return set;
        }

       
#endregion
    }

}