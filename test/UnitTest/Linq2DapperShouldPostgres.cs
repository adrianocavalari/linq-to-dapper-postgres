using System;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using Dapper.Contrib.Linq2Dapper.Postgre.Extensions;
using Dapper.Contrib.Linq2Dapper.Test.Data.POCO;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Npgsql;

namespace Dapper.Contrib.Linq2Dapper.Test.UnitTest
{
    [TestClass]
    public class Linq2DapperShouldPostgres
    {
        private static string ConnectionString => "Host=localhost;Port=5433;Database = pocdb; User ID = postgres; password = postgres; ";

        [TestMethod]
        public void SelectAllRecords()
        {
            using (var cn = new NpgsqlConnection(ConnectionString))
            {
                cn.Open();
                var results = cn.Query<DataType>().ToList();
                Assert.AreEqual(5, results.Count);
            }
        }

        [TestMethod]
        public void SelectAllRecords2()
        {
            var cntx = new DataContextPostgres(ConnectionString);

            var results = cntx.DataTypes.Where(x => x.Name == "text").ToList();
            
            Assert.AreEqual(1, results.Count);
        }

        [TestMethod]
        public void JoinWhere()
        {
            var cntx = new DataContextPostgres(ConnectionString);

            var results = (from d in cntx.DataTypes
                           join a in cntx.Fields on d.DataTypeId equals a.DataTypeId
                           where a.DataTypeId == 1
                           select d).ToList();

            Assert.AreEqual(3, results.Count);
        }

        [TestMethod]
        public void JoinWhereProjection()
        {
            var cntx = new DataContextPostgres(ConnectionString);

            var results = (from d in cntx.DataTypes
                           join a in cntx.Fields on d.DataTypeId equals a.DataTypeId
                           where a.DataTypeId == 1
                           select d).ToList();

            Assert.AreEqual(3, results.Count);
        }

        [TestMethod]
        public void MultiJoinWhere()
        {
            var cntx = new DataContextPostgres(ConnectionString);

            var results = (from d in cntx.DataTypes
                           join a in cntx.Fields on d.DataTypeId equals a.DataTypeId
                           join b in cntx.Documents on a.FieldId equals b.FieldId
                           where a.DataTypeId == 1 && b.FieldId == 1
                           select d).ToList();

            Assert.AreEqual(1, results.Count);
        }

        [TestMethod]
        public void WhereContains()
        {
            using (var cn = new NpgsqlConnection(ConnectionString))
            {
                var r = (from a in cn.Query<DataType>()
                        where new[] { "text", "int", "random" }.Contains(a.Name)
                        orderby a.Name
                        select a).ToList();
                    
                Assert.AreEqual(2, r.Count);
            }
        }

        [TestMethod]
        public void WhereEquals()
        {
            using (var cn = new NpgsqlConnection(ConnectionString))
            {
                foreach (var item in new[] { "text", "int" })
                {
                    var results = cn.Query<DataType>(x => x.Name == item).ToList();
                    Assert.AreEqual(1, results.Count);
                }
            }
        }


        [TestMethod]
        public void Top1Statement()
        {
            using (var cn = new NpgsqlConnection(ConnectionString))
            {
                cn.Open();
                var results = cn.Query<DataType>().FirstOrDefault(m => !m.IsActive);
                Assert.IsNotNull(results);
            }
        }


        [TestMethod]
        public void Top10A()
        {
            using (var cn = new NpgsqlConnection(ConnectionString))
            {
                cn.Open();
                var results = cn.Query<DataType>().Take(5).ToList();
                Assert.AreEqual(5, results.Count);
            }
        }

        [TestMethod]
        public void Top10B()
        {
            const int topCount = 10;
            using (var cn = new NpgsqlConnection(ConnectionString))
            {
                cn.Open();
                var results = cn.Query<DataType>().Take(topCount).ToList();
                Assert.AreEqual(5, results.Count);
            }
        }

        [TestMethod]
        public void Top10C()
        {
            using (var cn = new NpgsqlConnection(ConnectionString))
            {
                cn.Open();
                for (int topCount = 1; topCount < 5; topCount++)
                {
                    var results = cn.Query<DataType>().Take(topCount).ToList();
                    Assert.AreEqual(topCount, results.Count);
                }
            }
        }

        [TestMethod]
        public void Distinct()
        {
            using (var cn = new NpgsqlConnection(ConnectionString))
            {
                cn.Open();
                var results = cn.Query<DataType>().Distinct().ToList();
                Assert.AreEqual(5, results.Count);
            }
        }

        [TestMethod]
        public void OrderBy()
        {
            using (var cn = new NpgsqlConnection(ConnectionString))
            {
                cn.Open();
                var results = cn.Query<DataType>().OrderBy(m => m.Name).ToList();
                Assert.AreEqual(5, results.Count);
            }
        }

        [TestMethod]
        public void OrderByAndThenBy()
        {
            using (var cn = new NpgsqlConnection(ConnectionString))
            {
                cn.Open();
                var results = cn.Query<DataType>().OrderBy(m => m.Name).ThenBy(m => m.DataTypeId).ToList();
                Assert.AreEqual(5, results.Count);
            }
        }

        [TestMethod]
        public void OrderByWithTop()
        {
            using (var cn = new NpgsqlConnection(ConnectionString))
            {
                cn.Open();
                var results = cn.Query<DataType>().OrderBy(m => m.Name).ThenBy(m => m.DataTypeId).Take(5).ToList();
                Assert.AreEqual(5, results.Count);
            }
        }

        [TestMethod]
        public void WhereSimpleEqual()
        {
            using (var cn = new NpgsqlConnection(ConnectionString))
            {
                cn.Open();
                var results = cn.Query<DataType>(m => m.Name == "text").ToList();
                Assert.AreEqual(1, results.Count);
                Assert.AreEqual("text", results[0].Name);
            }
        }

        [TestMethod]
        public void WhereSimpleEqualWithoutParameter()
        {
            using (var cn = new NpgsqlConnection(ConnectionString))
            {
                cn.Open();
                var results = cn.Query<DataType>(m => m.DataTypeId == m.DataTypeId).ToList();
                Assert.AreEqual(5, results.Count);
            }
        }

        [TestMethod]
        public void WhereIsNullOrEmpty()
        {
            using (var cn = new NpgsqlConnection(ConnectionString))
            {
                cn.Open();
                var results = cn.Query<DataType>(m => String.IsNullOrEmpty(m.Name)).ToList();
                Assert.AreEqual(0, results.Count);
            }
        }

        [TestMethod]
        public void WhereNotIsNullOrEmpty()
        {
            using (var cn = new NpgsqlConnection(ConnectionString))
            {
                cn.Open();
                var results = cn.Query<DataType>(m => !String.IsNullOrEmpty(m.Name)).ToList();
                Assert.AreEqual(5, results.Count);
            }
        }

        [TestMethod]
        public void WhereHasValue()
        {
            using (var cn = new NpgsqlConnection(ConnectionString))
            {
                cn.Open();
                var results = cn.Query<DataType>(m => m.Created.HasValue).ToList();
                Assert.AreEqual(5, results.Count);
            }
        }

        [TestMethod]
        public void WhereNotHasValue()
        {
            using (var cn = new NpgsqlConnection(ConnectionString))
            {
                cn.Open();
                var results = cn.Query<DataType>(m => !m.Created.HasValue).ToList();
                Assert.AreEqual(0, results.Count);
            }
        }

        [TestMethod]
        public void WhereLike()
        {
            using (var cn = new NpgsqlConnection(ConnectionString))
            {
                cn.Open();
                var results = cn.Query<DataType>(m => m.Name.Contains("te")).ToList();
                Assert.AreEqual(2, results.Count);
            }
        }

        [TestMethod]
        public void WhereEndsWith()
        {
            using (var cn = new NpgsqlConnection(ConnectionString))
            {
                cn.Open();
                var results = cn.Query<DataType>(m => m.Name.StartsWith("te")).ToList();
                Assert.AreEqual(1, results.Count);
            }
        }

        [TestMethod]
        public void WhereStartsWith()
        {
            using (var cn = new NpgsqlConnection(ConnectionString))
            {
                cn.Open();
                var results = cn.Query<DataType>(m => m.Name.EndsWith("xt")).ToList();
                Assert.AreEqual(1, results.Count);
            }
        }

        [TestMethod]
        public void WhereEndsWithAndComparison()
        {
            using (var cn = new NpgsqlConnection(ConnectionString))
            {
                cn.Open();
                var results = cn.Query<DataType>(m => m.Name.StartsWith("te", StringComparison.OrdinalIgnoreCase)).ToList();
                Assert.AreEqual(1, results.Count);
            }
        }

        [TestMethod]
        public void WhereStartsWithAndComparison()
        {
            using (var cn = new NpgsqlConnection(ConnectionString))
            {
                cn.Open();
                var results = cn.Query<DataType>(m => m.Name.EndsWith("xt", StringComparison.OrdinalIgnoreCase)).ToList();
                Assert.AreEqual(1, results.Count);
            }
        }

        [TestMethod]
        public void WhereNotLike()
        {
            using (var cn = new NpgsqlConnection(ConnectionString))
            {
                cn.Open();
                var results = cn.Query<DataType>(m => !m.Name.Contains("te")).ToList();
                Assert.AreEqual(3, results.Count);
            }
        }

        [TestMethod]
        public void WhereNotEndsWith()
        {
            using (var cn = new NpgsqlConnection(ConnectionString))
            {
                cn.Open();
                var results = cn.Query<DataType>(m => !m.Name.StartsWith("te")).ToList();
                Assert.AreEqual(4, results.Count);
            }
        }

        [TestMethod]
        public void WhereNotStartsWith()
        {
            using (var cn = new NpgsqlConnection(ConnectionString))
            {
                cn.Open();
                var results = cn.Query<DataType>(m => !m.Name.EndsWith("xt")).ToList();
                Assert.AreEqual(4, results.Count);
            }
        }

        [TestMethod]
        public void TwoPartWhereAnd()
        {
            using (var cn = new NpgsqlConnection(ConnectionString))
            {
                cn.Open();
                var results = cn.Query<DataType>(m => m.Name == "text" && m.Created.HasValue).ToList();
                Assert.AreEqual(1, results.Count);
            }
        }

        [TestMethod]
        public void TwoPartWhereOr()
        {
            using (var cn = new NpgsqlConnection(ConnectionString))
            {
                cn.Open();
                var results = cn.Query<DataType>(m => m.Name == "text" || m.Name == "int").ToList();
                Assert.AreEqual(2, results.Count);
            }
        }

        [TestMethod]
        public void MultiPartWhereAndOr()
        {
            using (var cn = new NpgsqlConnection(ConnectionString))
            {
                cn.Open();
                var results = cn.Query<DataType>(m => m.Name == "text" && (m.Name == "int" || m.Created.HasValue)).ToList();
                Assert.AreEqual(1, results.Count);
            }
        }

        [TestMethod]
        public void MultiPartWhereAndOr2()
        {
            using (var cn = new NpgsqlConnection(ConnectionString))
            {
                cn.Open();
                var results = cn.Query<DataType>(m => m.Name != "text" && (m.Name == "int" || m.Created.HasValue)).ToList();
                Assert.AreEqual(4, results.Count);
            }
        }

        [TestMethod]
        public void Single()
        {
            using (var cn = new NpgsqlConnection(ConnectionString))
            {
                cn.Open();
                var result2 = cn.Query<DataType>();
                var result = result2.Single(m => m.Name == "text");
                Assert.AreEqual("text", result.Name);
            }
        }

        [TestMethod]
        public void Select()
        {
            using (var cn = new NpgsqlConnection(ConnectionString))
            {
                cn.Open();
                var result2 = cn.Query<DataType>();
                var result = result2.Select(s => s.Name);
                var a = result.ToList();
                //Assert.AreEqual(5, results.Count);
            }
        }
    }

    public class DataContextPostgres : IDisposable
    {
        private readonly IDbConnection _connection;

        private Linq2Dapper<DataType> _dataTypes;
        public Linq2Dapper<DataType> DataTypes => _dataTypes ?? (_dataTypes = CreateObject<DataType>());

        private Linq2Dapper<Field> _fields;
        public Linq2Dapper<Field> Fields => _fields ?? (_fields = CreateObject<Field>());

        private Linq2Dapper<Document> _documents;
        public Linq2Dapper<Document> Documents => _documents ?? (_documents = CreateObject<Document>());


        public DataContextPostgres(string connectionString) : this(new NpgsqlConnection(connectionString)) { }

        public DataContextPostgres(IDbConnection connection)
        {
            _connection = connection;
        }

        private Linq2Dapper<T> CreateObject<T>()
        {
            return new Linq2Dapper<T>(_connection);
        }

        public void Dispose()
        {
            _connection.Dispose();
        }
    }
}
