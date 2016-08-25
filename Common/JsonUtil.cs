using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace MVCTest.Common
{
    /// <summary>
    /// Json 序列化与反序列化
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class JsonUtil<T> where T : class
    {
        public static string Serialize(T fromObj)
        {
            string result = string.Empty;
            result = JsonConvert.SerializeObject(fromObj);
            return result;
        }

        public static string Serialize(T fromObj, JsonSerializerSettings settings = null)
        {
            string result = string.Empty;


            if (null == settings)
            {
                result = JsonConvert.SerializeObject(fromObj);
            }
            else
            {
                result = JsonConvert.SerializeObject(fromObj, settings);
            }

            return result;
        }

        public static T Deserialize(string jsonStr, JsonSerializerSettings settings = null)
        {
            T result = default(T);
            if (null == settings)
            {
                result = JsonConvert.DeserializeObject<T>(jsonStr);
            }
            else
            {
                result = JsonConvert.DeserializeObject<T>(jsonStr, settings);
            }

            return result;
        }
    }
}