using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Amazon.SimpleDB;
using Amazon.SimpleDB.Model;
using System.Reflection;
using System.Diagnostics;
using Amazon;

namespace Org.Ochilab.AWS
{
    class OSimpleMapper
    {

        static AmazonSimpleDBClient sdb;

        IDictionary<String, String> map = new Dictionary<String, String>();


        public OSimpleMapper(string endpoint)
        {
            AmazonSimpleDBConfig config = new AmazonSimpleDBConfig { ServiceURL = endpoint };
            sdb = new AmazonSimpleDBClient(config);

        }

        public OSimpleMapper()
            : this("https://sdb.amazonaws.com") { }


        /**
        *  Domainの作成
        * */
        public void createDomain(String DomainName)
        {

            bool found = false;

            //同名のドメインを探す
            ListDomainsResponse response = sdb.ListDomains(new ListDomainsRequest());
            //foreach (string domain in response.ListDomainsResult.DomainName)
            foreach (string domain in response.DomainNames)
            {
                if (domain == DomainName)
                {
                    found = true;
                    break;
                }
            }
            //同名のドメインがなければ作成
            if (!found)
            {
                sdb.CreateDomain(
                  new CreateDomainRequest() { DomainName = DomainName });
            }

        }

        /**
         * id（ItemName）による検索
         * */
        public Object getItem(Object obj, string id)
        {
            GetAttributesResponse response = sdb.GetAttributes(new GetAttributesRequest() { DomainName = obj.GetType().Name, ItemName = id });
            Type type = obj.GetType();
            //foreach (Amazon.SimpleDB.Model.Attribute attribute in response.GetAttributesResult.Attribute)
            foreach (Amazon.SimpleDB.Model.Attribute attribute in response.Attributes)
            {
                foreach (PropertyInfo prop in type.GetProperties())
                {
                    if (prop.Name.Equals(attribute.Name))
                    {
                        Debug.WriteLine("!" + attribute.Name);
                        prop.SetValue(obj, attribute.Value, null);
                        break;
                    }
                }
            }
            return obj;

        }


        /**
         * クエリー検索
         * */
        public List<object> query(string clazz, string query)
        {

            Type t = Type.GetType(clazz);
            List<object> list = new List<object>();
            SelectResponse response = sdb.Select(new SelectRequest() { SelectExpression = query });
            //foreach (Item item in response.SelectResult.Item)
            foreach (Item item in response.Items)
            {
                object obj = System.Activator.CreateInstance(t);
                Type type = obj.GetType();
                //foreach (Amazon.SimpleDB.Model.Attribute attribute in item.Attribute)
                foreach (Amazon.SimpleDB.Model.Attribute attribute in item.Attributes)
                {
                    foreach (PropertyInfo prop in type.GetProperties())
                    {
                        if (prop.Name.Equals(attribute.Name))
                        {
                            Debug.WriteLine("!" + attribute.Name);
                            prop.SetValue(obj, attribute.Value, null);
                            break;
                        }
                    }

                }
                list.Add(obj);
            }
            return list;

        }

        /**
         * アイテムの登録
         * */
        public void putItem(Object obj)
        {

            string id = "dummy";

            Type type = obj.GetType();

            List<ReplaceableAttribute> listReplaceAttribute = new List<ReplaceableAttribute>();
            foreach (PropertyInfo prop in type.GetProperties())
            {
                string name = prop.Name;
                string value = (string)prop.GetValue(obj, null);
                if (name.Equals("Id"))
                {
                    id = value;
                }
                else
                {
                    ReplaceableAttribute replaceAttribute = new ReplaceableAttribute() { Name = name, Replace = true, Value = value };
                    listReplaceAttribute.Add(replaceAttribute);
                }

                Debug.WriteLine(name + "=" + value);
            }
            sdb.PutAttributes(new PutAttributesRequest()
            {
                //Attribute = listReplaceAttribute,
                Attributes = listReplaceAttribute,
                DomainName = obj.GetType().Name,
                ItemName = id
            });
        }

        /**
         * アイテムの削除
         * */
        public void deleteItem(string domainName, string id)
        {
            sdb.DeleteAttributes(new DeleteAttributesRequest()
            {
                DomainName = domainName,
                ItemName = id
            });
        }

    }
}
