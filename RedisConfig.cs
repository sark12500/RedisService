using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SignalR.Utils.Redis
{
    public class RedisConfig
    {
        public static ConfigurationOptions Config = new ConfigurationOptions
        {
            EndPoints =
            {
                {"0.0.0.0", 12345},
                {"0.0.0.1", 12345}
            },
            AllowAdmin = true,
            Password = "xxxxxxxx",
            AbortOnConnectFail = false, 
        };

        //http://stackoverflow.com/a/28821322
        private static Lazy<ConnectionMultiplexer> lazyConnection = new Lazy<ConnectionMultiplexer>(() =>
        {
            return ConnectionMultiplexer.Connect(Config);
        });

        public static ConnectionMultiplexer Multiplexer
        {
            get
            {
                return lazyConnection.Value;
            }
        }

        //private static ConnectionMultiplexer RedisConnection;


        //public static ConnectionMultiplexer Multiplexer
        //{
        //    get
        //    {
        //        if (RedisConnection == null || !RedisConnection.IsConnected)
        //        {
        //            RedisConnection = ConnectionMultiplexer.Connect(Config);
        //            return RedisConnection;
        //        }
        //        else
        //        {
        //            return RedisConnection;
        //        }
        //    }
        //}
    }
}
