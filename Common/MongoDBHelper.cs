using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace MVCTest.Common
{
    #region MongoDB基础类
    internal class MongoDB
    {
        //数据库所在主机的端口
        private readonly int MONGO_CONN_PORT = 27017;
        //设置连接超时15秒
        private readonly int CONNECT_TIME_OUT = 15;
        //设置最大连接数
        private readonly int MAXConnectionPoolSize = 99;
        //设置最小连接数
        private readonly int MINConnectionPoolSize = 1;

        /// <summary>
        /// 获得数据库实例
        /// </summary>
        /// <param name="MONGO_CONN_HOST">数据库主机链接</param>
        /// <param name="DB_Name">数据库名称</param>
        /// <returns>数据库实例</returns>
        public MongoDatabase GetDataBase(string MONGO_CONN_HOST, string DB_Name)
        {
            MongoClientSettings mongoSetting = new MongoClientSettings();

            mongoSetting.ConnectTimeout = new TimeSpan(CONNECT_TIME_OUT * TimeSpan.TicksPerSecond);  //设置超时连接

            mongoSetting.Server = new MongoServerAddress(MONGO_CONN_HOST, MONGO_CONN_PORT);  //设置数据库服务器

            mongoSetting.MaxConnectionPoolSize = MAXConnectionPoolSize;  //设置最大连接数
            mongoSetting.MinConnectionPoolSize = MINConnectionPoolSize;  //设置最小连接数

            MongoClient client = new MongoClient(mongoSetting);  //创建Mongo客户端

            return client.GetServer().GetDatabase(DB_Name);  //得到服务器端并生成数据库实例
        }


        /// <summary>
        /// 得到数据库服务器
        /// </summary>
        /// <param name="MONGO_CONN_HOST">数据库主机链接</param>
        /// <returns>数据库服务器实例</returns>
        public MongoServer GetDataBaseServer(string MONGO_CONN_HOST)
        {
            MongoClientSettings mongoSetting = new MongoClientSettings();

            mongoSetting.ConnectTimeout = new TimeSpan(CONNECT_TIME_OUT * TimeSpan.TicksPerSecond);  //设置超时连接

            mongoSetting.Server = new MongoServerAddress(MONGO_CONN_HOST, MONGO_CONN_PORT);  //设置数据库服务器

            mongoSetting.MaxConnectionPoolSize = MAXConnectionPoolSize;  //设置最大连接数
            mongoSetting.MinConnectionPoolSize = MINConnectionPoolSize;  //设置最小连接数

            MongoClient client = new MongoClient(mongoSetting);  //创建MongoDB客户端

            return client.GetServer();
        }
    }
    #endregion


    /// <summary>
    /// Mongo db的数据库帮助类 还未投入到生产环境中 
    /// </summary>
    internal sealed class MongoDBHelper : IDisposable
    {
        #region 创建实例

        /// <summary>
        /// 数据库的实例
        /// </summary>
        public MongoDatabase _db;

        /// <summary>
        /// 得到数据库服务器
        /// </summary>
        private MongoServer _dbServer;

        /// <summary>
        /// ObjectId的键
        /// </summary>
        private readonly string OBJECTID_KEY = "_id";

        //初始化构造函数
        public MongoDBHelper(string MONGO_CONN_HOST, string DB_Name)
        {
            this._db = new MongoDB().GetDataBase(MONGO_CONN_HOST, DB_Name);
            this._dbServer = new MongoDB().GetDataBaseServer(MONGO_CONN_HOST);
        }

        #endregion

        #region 删除数据库
        /// <summary>
        /// 删除数据库
        /// </summary>
        /// <param name="DBName">数据库名称</param>
        public void DropDataBase(string DBName)
        {
            this._dbServer.DropDatabase(DBName);
        }

        #endregion

        #region 插入数据

        /// <summary>
        /// 将数据插入进数据库
        /// </summary>
        /// <typeparam name="T">需要插入数据库的实体类型</typeparam>
        /// <param name="t">需要插入数据库的具体实体</param>
        /// <param name="collectionName">指定插入的集合</param>
        public void Insert<T>(T t, string collectionName)
        {
            MongoCollection<BsonDocument> mc = this._db.GetCollection<BsonDocument>(collectionName);

            //将实体转换为bson文档
            BsonDocument bd = t.ToBsonDocument();

            //进行插入操作
            WriteConcernResult result = mc.Insert(bd);
            if (!string.IsNullOrEmpty(result.ErrorMessage))
            {
                throw new Exception(result.ErrorMessage);
            }

        }

        /// <summary>
        /// 批量插入数据
        /// </summary>
        /// <typeparam name="T">需要插入数据库的实体类型</typeparam>
        /// <param name="list">需要插入数据的列表</param>
        /// <param name="collectionName">指定要插入的集合</param>
        public void Insert<T>(List<T> list, string collectionName)
        {
            MongoCollection<BsonDocument> mc = this._db.GetCollection<BsonDocument>(collectionName);

            //创建一个空间bson集合
            List<BsonDocument> bsonList = new List<BsonDocument>();

            //批量将数据转为bson格式 并且放进bson文档,lambda表达式
            list.ForEach(t => bsonList.Add(t.ToBsonDocument()));

            //批量插入数据
            mc.InsertBatch(bsonList);
        }

        #endregion

        #region 查询


        /// <summary>
        /// 查询一个集合中的所有数据
        /// </summary>
        /// <typeparam name="T">该集合数据的所属类型</typeparam>
        /// <param name="collectionName">指定集合的名称</param>
        /// <returns>返回一个List列表</returns>
        public List<T> FindAll<T>(string collectionName)
        {
            MongoCollection<T> mc = this._db.GetCollection<T>(collectionName);
            //以实体方式取出其数据集合
            MongoCursor<T> mongoCursor = mc.FindAll();
            //直接转化为List返回
            return mongoCursor.ToList<T>();
        }



        /// <summary>
        /// 查询指定字段的所有数据
        /// </summary>
        /// <typeparam name="T">数据类型</typeparam>
        /// <param name="collectionName">数据表名称</param>
        /// <param name="fd">字段区间定义</param>
        /// <returns></returns>
        public List<T> FindAll<T>(string collectionName, FieldsDocument fd)
        {
            MongoCollection<T> mc = this._db.GetCollection<T>(collectionName);
            //以实体方式取出其数据集合
            MongoCursor<T> mongoCursor = mc.FindAll().SetFields(fd);
            //直接转化为List返回
            return mongoCursor.ToList<T>();
        }




        /// <summary>
        /// 查询一条记录
        /// </summary>
        /// <typeparam name="T">该数据所属的类型</typeparam>
        /// <param name="query">查询的条件 可以为空</param>
        /// <param name="collectionName">去指定查询的集合</param>
        /// <returns>返回一个实体类型</returns>
        public T FindOne<T>(IMongoQuery query, string collectionName)
        {
            MongoCollection<T> mc = this._db.GetCollection<T>(collectionName);
            query = this.InitQuery(query);
            T t = mc.FindOne(query);
            return t;
        }

        /// <summary>
        /// 根据指定条件查询集合中的多条数据记录
        /// </summary>
        /// <typeparam name="T">该集合数据的所属类型</typeparam>
        /// <param name="query">指定的查询条件 比如Query.And(Query.EQ("username","admin"),Query.EQ("password":"admin"))</param>
        /// <param name="collectionName">指定的集合的名称</param>
        /// <returns>返回一个List列表</returns>
        public List<T> Find<T>(IMongoQuery query, string collectionName)
        {
            MongoCollection<T> mc = this._db.GetCollection<T>(collectionName);
            query = this.InitQuery(query);

            MongoCursor<T> mongoCursor = mc.Find(query);

            return mongoCursor.ToList<T>();
        }


        /// <summary>
        /// 根据指定条件查询集合中的多条数据记录
        /// </summary>
        /// <typeparam name="T">该集合数据的所属类型</typeparam>
        /// <param name="query">指定的查询条件 比如Query.And(Query.EQ("username","admin"),Query.EQ("password":"admin"))</param>
        /// <param name="collectionName">指定的集合的名称</param>
        /// <returns>返回一个List列表</returns>
        public List<T> Find<T>(IMongoQuery query, string collectionName, FieldsDocument fd)
        {
            MongoCollection<T> mc = this._db.GetCollection<T>(collectionName);
            query = this.InitQuery(query);

            MongoCursor<T> mongoCursor = mc.Find(query).SetFields(fd);

            return mongoCursor.ToList<T>();
        }

        #endregion

        #region 更新数据

        /// <summary>
        /// 更新数据
        /// </summary>
        /// <typeparam name="T">更新的数据 所属的类型</typeparam>
        /// <param name="query">更新数据的查询</param>
        /// <param name="update">需要更新的文档</param>
        /// <param name="collectionName">指定更新集合的名称</param>
        public void Update<T>(IMongoQuery query, BsonDocument bd, string collectionName)
        {

            MongoCollection<T> mc = this._db.GetCollection<T>(collectionName);
            query = this.InitQuery(query);

            mc.Update(query, new UpdateDocument(bd));
        }


        #endregion

        #region 移除/删除数据
        /// <summary>
        /// 移除指定的数据
        /// </summary>
        /// <typeparam name="T">移除的数据类型</typeparam>
        /// <param name="query">移除的数据条件</param>
        /// <param name="collectionName">指定的集合名词</param>
        public void Remove<T>(IMongoQuery query, string collectionName)
        {
            MongoCollection<T> mc = this._db.GetCollection<T>(collectionName);

            query = this.InitQuery(query);
            //根据指定查询移除数据
            mc.Remove(query);
        }

        /// <summary>
        /// 移除实体里面所有的数据
        /// </summary>
        /// <typeparam name="T">移除的数据类型</typeparam>
        /// <param name="collectionName">指定的集合名称</param>
        public void RemoveAll<T>(string collectionName)
        {
            this.Remove<T>(null, collectionName);
        }

        #endregion

        #region 创建索引
        /// <summary>
        /// 创建索引
        /// </summary>
        /// <param name="collname">表名</param>
        /// <param name="IndexName">索引名</param>
        /// <param name="isdec">是否倒序</param>
        public void CreateIndex(string collname, string IndexName, string isdes)
        {
            MongoCollection<BsonDocument> mc = this._db.GetCollection<BsonDocument>(collname);
            if (isdes == "1")
            {
                mc.CreateIndex(IndexKeys.Descending(IndexName));
            }
            else
            {
                mc.CreateIndex(IndexKeys.Ascending(IndexName));
            }
        }
        #endregion

        #region 获取集合的存储大小

        /// <summary>
        /// 获取集合的存储大小
        /// </summary>
        /// <param name="collectionName">该集合对应的名称</param>
        /// <returns>返回一个long型</returns>
        public long GetDataSize(string collectionName)
        {
            MongoCollection<BsonDocument> mc = this._db.GetCollection<BsonDocument>(collectionName);
            return mc.GetTotalStorageSize();
        }


        #endregion

        #region 私有的一些辅助方法
        /// <summary>
        /// 初始化查询记录 主要当该查询条件为空时 会附加一个恒真的查询条件，防止空查询报错
        /// </summary>
        /// <param name="query">查询的条件</param>
        /// <returns></returns>
        private IMongoQuery InitQuery(IMongoQuery query)
        {
            if (query == null)
            {
                //当查询为空时 附加恒真的条件 类似SQL：1=1的语法
                query = Query.Exists(OBJECTID_KEY);
            }
            return query;
        }

        /// <summary>
        /// 初始化排序条件  主要当条件为空时 会默认以ObjectId递增的一个排序
        /// </summary>
        /// <param name="sortBy"></param>
        /// <returns></returns>
        private SortByDocument InitSortBy(SortByDocument sortBy)
        {
            if (sortBy == null)
            {
                //默认ObjectId 递增
                sortBy = new SortByDocument(OBJECTID_KEY, 1);
            }
            return sortBy;
        }

        public MongoServerInstance GetServerInstance()
        {
            return _dbServer.Instance;
        }
        public MongoServerSettings GetServerSettings()
        {
            return _dbServer.Settings;
        }
        public List<string> GetDataBaseNames()
        {
            return _dbServer.GetDatabaseNames().ToList();
        }
        public long GetTableNameDataSize(string dbname, string tablename)
        {
            return _dbServer.GetDatabase(dbname).GetCollection(tablename).GetTotalDataSize();
        }
        public long GetTableNameDataCount(string dbname, string tablename)
        {
            return _dbServer.GetDatabase(dbname).GetCollection(tablename).Count();
        }

        public List<IndexInfo> GetIndexNames(string collname, string IndexName)
        {
            MongoCollection<BsonDocument> mc = this._db.GetCollection<BsonDocument>(collname);
            return mc.GetIndexes().ToList();
        }

        #endregion

        #region IDisposable 成员
        //可以被客户直接调用 
        public void Dispose()
        {
            if (_db != null)
            {
                _db = null;
            }
            if (_dbServer != null)
            {
                _dbServer = null;
            }
            // GC.SuppressFinalize(this); // 告诉垃圾回收器从Finalization队列中清除自己,从而阻止垃圾回收器调用Finalize方法. 
        }
        #endregion
    }
}