using MongoDB.Driver;
using MongoDB.Driver.Builders;
using MVCTest.Common;
using MVCTest.Models;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace MVCTest.Controllers
{
    public class TestController : Controller
    {
        //
        // GET: /Test/
        public string GetString() 
        {
            return "hello world!";
        }

        public string GetTest(int kind = 1) 
        {
            switch (kind)
            {
                case 1: return "1"; 
                case 2: return "2"; 
                case 3: return "3"; 
                default: return "-1";
            }
        }

        public ActionResult GetView() 
        {
            Employee emp = new Employee();
            emp.FirstName = "_FirstName_";
            emp.LastName = "_LastName_";
            emp.Salary = 20000;

            //ViewData["Employee"] = emp;
            ViewBag.Html = ".html";
            ViewBag.Employee = emp;

            return View("MyView", emp);
        }


        public string GetModel()
        {
            Employee emp = new Employee();
            emp.FirstName = "1";
            emp.LastName = "0";
            emp.Salary = 40000;
            return JsonUtil<Employee>.Serialize(emp);
        }

        public string GetRedis()
        {
            RedisHelper helper = new RedisHelper();
            helper.Set<string>("name", "hhh");
            return helper.Get<string>("name");
        }


        public string GetMongo(string val = "1")
        {
            string result = "";

            string host = "localhost";
            string DBName = "DB_Name";  //创建数据库名称
            string collectionName = "CollectionName";　//数据库中的表单名称
            MongoDBHelper mongo = new MongoDBHelper(host, DBName);

            //流的方式插入数据
            List<Dictionary<string, object>> insertList = new List<Dictionary<string, object>>();
            for (int i = 0; i < 10; i++)
            {
                Dictionary<string, object> dic = new Dictionary<string, object>();
                dic.Add("val", val);
                insertList.Add(dic);
            }
            mongo.Insert(insertList, collectionName);

            //查找
            FieldsDocument fd = new FieldsDocument();
            fd.Add("val", 1);
            fd.Add("_id", 0);
            List<Dictionary<string, object>> tempA = mongo.Find<Dictionary<string, object>>( Query.EQ("val", "1"), collectionName,fd);
            result = JsonConvert.SerializeObject(tempA); 

            return result;
        }


	}
}