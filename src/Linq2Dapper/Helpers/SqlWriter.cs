using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace Dapper.Contrib.Linq2Dapper.Postgre.Helpers
{
    internal class SqlWriter<TData>
    {
        private StringBuilder _selectStatement;
        private readonly StringBuilder _joinTable;
        private readonly StringBuilder _whereClause;
        private readonly StringBuilder _orderBy;

        private int _nextParameter;

        private string _parameter => $"ld__{_nextParameter += 1}";

        internal Type SelectType;
        internal Dictionary<string, string> SelectColumns;
        internal bool NotOperater;
        internal int TopCount;
        internal bool IsDistinct;

        internal DynamicParameters Parameters { get; private set; }

        internal string Sql
        {
            get
            {
                SelectStatement();
                return _selectStatement.ToString();
            }
        }

        internal SqlWriter()
        {
            Parameters = new DynamicParameters();
            _joinTable = new StringBuilder();
            _whereClause = new StringBuilder();
            _orderBy = new StringBuilder();
            SelectType = typeof(TData);
            SelectColumns = new Dictionary<string, string>();
            GetTypeProperties();
        }

        private void GetTypeProperties()
        {
            QueryHelper.GetTypeProperties(typeof(TData));
        }

        private void SelectStatement()
        {
            var primaryTable = CacheHelper.TryGetTable<TData>();
            var selectTable = (SelectType != typeof(TData)) ? CacheHelper.TryGetTable(SelectType) : primaryTable;

            _selectStatement = new StringBuilder();

            _selectStatement.Append("SELECT ");

            if (IsDistinct)
                _selectStatement.Append("DISTINCT ");

            primaryTable.Columns = SelectColumns;

            for (var i = 0; i < primaryTable.Columns.Count; i++)
            {
                var x = primaryTable.Columns.ElementAt(i);
                _selectStatement.Append($"{primaryTable.Identifier}.{x.Value} ");

                if ((i + 1) != primaryTable.Columns.Count)
                    _selectStatement.Append(",");

                _selectStatement.Append(" ");
            }

            _selectStatement.Append($"FROM {primaryTable.Schema + "."}{primaryTable.Name} {primaryTable.Identifier} ");
            _selectStatement.Append(WriteClause());

            if (TopCount > 0)
                _selectStatement.Append(" LIMIT(" + TopCount + ") ");
        }

        private string WriteClause()
        {
            var clause = string.Empty;

            // JOIN
            if (!string.IsNullOrEmpty(_joinTable.ToString()))
                clause += _joinTable;

            // WHERE
            if (!string.IsNullOrEmpty(_whereClause.ToString()))
                clause += " WHERE " + _whereClause;

            //ORDER BY
            if (!string.IsNullOrEmpty(_orderBy.ToString()))
                clause += " ORDER BY " + _orderBy;

            return clause;
        }

        internal void WriteOrder(string name, bool descending)
        {
            var order = new StringBuilder();
            order.Append(name);
            if (descending) order.Append(" DESC");
            if (!string.IsNullOrEmpty(_orderBy.ToString())) order.Append(", ");
            _orderBy.Insert(0, order);
        }

        internal void WriteJoin(string joinToTableName, string joinToTableIdentifier, string primaryJoinColumn, string secondaryJoinColumn)
        {
            _joinTable.Append(
                $" JOIN {joinToTableName} {joinToTableIdentifier} ON {primaryJoinColumn} = {secondaryJoinColumn}");
        }

        internal void Write(object value)
        {
            _whereClause.Append(value);
        }

        internal void Parameter(object val)
        {
            if (val == null)
            {
                Write("NULL");
                return;
            }

            var param = _parameter;
            Parameters.Add(param, val);

            Write("@" + param);
        }

        internal void AliasName(string aliasName)
        {
            Write(aliasName);
        }

        internal void ColumnName(string columnName)
        {
            Write(columnName);
        }

        internal void IsNull()
        {
            Write(NotOperater ? " IS" : " NOT");
            Write("NULL");
            NotOperater = false;
        }

        internal void IsNullFunction()
        {
            Write(NotOperater ? " NOT" : " IS");
            Write("NULL");
            NotOperater = false;
        }

        internal void Like()
        {
            if (NotOperater)
                Write(" NOT");
            Write(" LIKE ");
            NotOperater = false;
        }

        internal void In()
        {
            if (NotOperater)
                Write(" NOT");
            Write(" IN ");
            NotOperater = false;
        }

        internal void Operator()
        {
            Write(QueryHelper.GetOperator((NotOperater) ? ExpressionType.NotEqual : ExpressionType.Equal));
            NotOperater = false;
        }

        internal void Boolean(bool op)
        {
            Write("");
        }

        internal void OpenBrace()
        {
            Write("(");
        }

        internal void CloseBrace()
        {
            Write(")");
        }

        internal void WhiteSpace()
        {
            Write(" ");
        }

        internal void Delimiter()
        {
            Write(", ");
        }

        internal void LikePrefix()
        {
            Write("'%' || ");
        }

        internal void LikeSuffix()
        {
            Write("|| '%'");
        }

        internal void EmptyString()
        {
            Write("''");
        }
    }
}
