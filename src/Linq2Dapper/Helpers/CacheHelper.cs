﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Dapper.Contrib.Linq2Dapper.Postgre.Exceptions;

namespace Dapper.Contrib.Linq2Dapper.Postgre.Helpers
{
    internal static class CacheHelper
    {
        internal static int Size => _typeList.Count;

        private static readonly ConcurrentDictionary<Type, TableHelper> _typeList;

        static CacheHelper()
        {
            if (_typeList == null)
                _typeList = new ConcurrentDictionary<Type, TableHelper>();
        }

        internal static bool HasCache<T>()
        {
            return HasCache(typeof (T));
        }

        internal static bool HasCache(Type type)
        {
            return _typeList.TryGetValue(type, out var table);
        }

        internal static bool TryAddTable<T>(TableHelper table)
        {
            return TryAddTable(typeof(T), table);
        }

        internal static bool TryAddTable(Type type, TableHelper table)
        {
            return _typeList.TryAdd(type, table);
        }

        internal static TableHelper TryGetTable<T>()
        {
            return TryGetTable(typeof(T));
        }

        internal static TableHelper TryGetTable(Type type)
        {
            return !_typeList.TryGetValue(type, out var table) ? new TableHelper() : table;
        }

        internal static string TryGetIdentifier<T>()
        {
            return TryGetIdentifier(typeof(T));
        }

        internal static string TryGetIdentifier(Type type)
        {
            return TryGetTable(type).Identifier;
        }

        internal static Dictionary<string, string> TryGetPropertyList<T>()
        {
            return TryGetPropertyList(typeof(T));
        }

        internal static Dictionary<string, string> TryGetPropertyList(Type type)
        {
            return TryGetTable(type).Columns;
        }

        internal static string TryGetTableName<T>()
        {
            return TryGetTableName(typeof(T));
        }

        internal static string TryGetTableName(Type type)
        {
            return TryGetTable(type).Name;
        }

    }
}
