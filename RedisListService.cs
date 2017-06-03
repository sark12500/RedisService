using System.Configuration;
using StackExchange.Redis;

namespace SignalR.Utils.Redis
{
    
    /// <summary>
    /// 用以存取Redis Table(List)
    /// </summary>
    public class RedisListService : RedisConfig
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
        /// <param name="dbIndex">資料庫索引</param>
        /// <param name="key">key</param>
        /// <returns></returns>
        public static long ListLength(int dbIndex, string key)
        {
            var conn = GetDatabase(dbIndex);
            var task = conn.ListLengthAsync(key);
            Multiplexer.Wait(task);
            return task.Result;

        }

        #region 新增
        /// <summary>
        /// 新增資料 - 單筆
        /// </summary>
        /// <param name="dbIndex">資料庫索引</param>
        /// <param name="key">key</param>
        /// <param name="value">Value</param>
        public static void ListInsert(int dbIndex, string key, string value)
        {
            var conn = GetDatabase(dbIndex);
            var task = conn.ListRightPushAsync(key, value);
            Multiplexer.Wait(task);
        }

        /// <summary>
        /// 新增資料 - 多筆
        /// </summary>
        public static void ListInsert(int dbIndex, string key, string[] values)
        {
            var conn = GetDatabase(dbIndex);

            var index = 0;
            var list = new RedisValue[values.Length];
            foreach (var v in values)
            {
                list[index] = values[index];
                ++index;
            }

            var task = conn.ListRightPushAsync(key, list);
            Multiplexer.Wait(task);
        }

        #endregion

        #region 刪除
        /// <summary>
        /// 刪除資料
        /// </summary>
        /// <param name="dbIndex">資料庫索引</param>
        /// <param name="key">key</param>
        /// <param name="value">Index</param>
        public static void ListDelete(int dbIndex, string key, string value)
        {
            var conn = GetDatabase(dbIndex);
            var task = conn.ListRemoveAsync(key, value);
            Multiplexer.Wait(task);
        }

        #endregion

        #region 修改

        /// <summary>
        /// 更新List資料 - 單筆
        /// </summary>
        /// <param name="dbIndex">資料庫索引</param>
        /// <param name="key">key</param>
        /// <param name="index">Index</param>
        /// <param name="value">Value</param>
        public static void ListUpdate(int dbIndex, string key, int index, string value)
        {
            var conn = GetDatabase(dbIndex);
            var task = conn.ListSetByIndexAsync(key, index, value);
            Multiplexer.Wait(task);
        }


        #endregion

        #region 查詢

        /// <summary>
        /// 查詢List value - 單筆
        /// </summary>
        /// <param name="dbIndex">資料庫索引</param>
        /// <param name="key">key</param>
        /// <param name="index">Index</param>
        public static string ListSelect(int dbIndex, string key, int index)
        {
            var conn = GetDatabase(dbIndex);
            var task = conn.ListGetByIndexAsync(key, index);
            Multiplexer.Wait(task);
            return task.Result;
        }

        /// <summary>
        /// 查詢List value - 全部
        /// </summary>
        /// <param name="dbIndex"></param>
        /// <param name="key"></param>
        /// <returns></returns>
        public static string[] ListSelectAll(int dbIndex, string key)
        {
            var conn = GetDatabase(dbIndex);
            var task = conn.ListRangeAsync(key);
            Multiplexer.Wait(task);
            return task.Result.ToStringArray();
        }
        

        #endregion
 
    }
}