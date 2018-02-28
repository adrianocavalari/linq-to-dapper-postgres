using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace Dapper.Contrib.Linq2Dapper.Helpers
{
    internal class SqlWriterPostgres<TData> : SqlWriter<TData>
    {
        protected override void SelectStatement()
        {
            var primaryTable = CacheHelper.TryGetTable<TData>();
            var selectTable = (SelectType != typeof(TData)) ? CacheHelper.TryGetTable(SelectType) : primaryTable;

            _selectStatement = new StringBuilder();

            _selectStatement.Append("SELECT ");

            if (TopCount > 0)
                _selectStatement.Append("TOP(" + TopCount + ") ");

            if (IsDistinct)
                _selectStatement.Append("DISTINCT ");

            for (int i = 0; i < selectTable.Columns.Count; i++)
            {
                var x = selectTable.Columns.ElementAt(i);
                _selectStatement.Append(string.Format("{0}.{1}", selectTable.Identifier, x.Value));

                if ((i + 1) != selectTable.Columns.Count)
                    _selectStatement.Append(",");

                _selectStatement.Append(" ");
            }

            _selectStatement.Append(string.Format("FROM {0} {1}", primaryTable.Name, primaryTable.Identifier));
            _selectStatement.Append(WriteClause());

            _selectStatement.ToString();
        }

        protected override void WriteJoin(string joinToTableName, string joinToTableIdentifier, string primaryJoinColumn, string secondaryJoinColumn)
        {
            _joinTable.Append(string.Format(" JOIN {0} {1} ON {2} = {3}", joinToTableName, joinToTableIdentifier, primaryJoinColumn, secondaryJoinColumn));
        }

    }
}
