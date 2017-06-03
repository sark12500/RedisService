
using System.Configuration;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace SignalR.Utils.Redis
{

    /// <summary>
    /// 用以存取Redis Table(key:value)
    /// </summary>
    public class RedisStringService : RedisConfig
    {

        /// <summary>
        /// 取得DB
        /// </summary>
        public static IDatabase GetDatabase(int dbIndex)
        {
            return Multiplexer.GetDatabase(dbIndex);
        }

        /// <summary>
        /// 取得文字長度
        /// </summary>
        /// <param name="dbIndex"></param>
        /// <param name="key"></param>
        /// <returns></returns>
        public static long StringLenth(int dbIndex, string key)
        {
            var conn = GetDatabase(dbIndex);
            var task = conn.StringLengthAsync(key);
            Multiplexer.Wait(task);
            return task.Result;
        }


#region 新增

        /// <summary>
        /// insert value
        /// </summary>
        /// <param name="dbIndex">資料庫索引</param>
        /// <param name="key">資料key</param>
        /// <param name="value"></param>
        public static void StringInsert(int dbIndex, string key, string value)
        {
            var conn = GetDatabase(dbIndex);
            var task = conn.StringSetAsync(key, value, null, When.NotExists);
            Multiplexer.Wait(task);
        }

#endregion

#region 修改

        /// <summary>
        /// update value
        /// </summary>
        /// <param name="dbIndex">資料庫索引</param>
        /// <param name="key">資料key</param>
        /// <param name="value">StringValue</param>
        public static void StringUpdate(int dbIndex, string key, string value)
        {
            var conn = GetDatabase(dbIndex);
            var task = conn.StringSetAsync(key, value, null, When.Exists);
            Multiplexer.Wait(task);

        }

        /// <summary>
        /// Append value
        /// </summary>
        /// <param name="dbIndex">資料庫索引</param>
        /// <param name="key">資料key</param>
        /// <param name="value"></param>
        public static void StringAppend(int dbIndex, string key, string value)
        {
            var conn = GetDatabase(dbIndex);
            var task = conn.StringAppendAsync(key, value);
            Multiplexer.Wait(task);
        }

#endregion

#region 查詢

        /// <summary>
        /// 查詢value 
        /// </summary>
        /// <param name="dbIndex">資料庫索引</param>
        /// <param name="key">資料key</param>
        public static string StringSelect(int dbIndex, string key)
        {
            var conn = GetDatabase(dbIndex);
            var task = conn.StringGetAsync(key);
            Multiplexer.Wait(task);
            return task.Result;
        }

#endregion
    }

}