using System;
using System.Reflection;
using System.Reflection.Emit;

namespace Ksql.Linq.Query.Analysis;

/// <summary>
/// Centralized dynamic type factory for runtime-derived entities (e.g., *_1s_rows).
/// Keeps dynamic module and cache localized to avoid scattering IL generation logic.
/// </summary>
internal static class DerivedTypeFactory
{
    private static readonly ModuleBuilder Module = AssemblyBuilder
        .DefineDynamicAssembly(new AssemblyName("KafkaKsqlLinq.Derived"), AssemblyBuilderAccess.Run)
        .DefineDynamicModule("Main");

    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, Type> Cache
        = new(System.StringComparer.Ordinal);
    private static readonly object Sync = new();

    public static Type GetDerivedType(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("name is required", nameof(name));

        if (Cache.TryGetValue(name, out var t)) return t;
        lock (Sync)
        {
            if (Cache.TryGetValue(name, out t)) return t;
            var tb = Module.DefineType(name, TypeAttributes.Public | TypeAttributes.Class);
            t = tb.CreateType()!;
            Cache[name] = t;
            return t;
        }
    }

    /// <summary>
    /// Metadata-driven overload: defines properties based on keys + projection.
    /// Use for hub rows (e.g., *_1s_rows) where column names/types are determined by the query.
    /// </summary>
    public static Type GetDerivedType(string name, string[] keyNames, string[] valueNames, Type[] valueTypes)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("name is required", nameof(name));

        lock (Sync)
        {
            if (Cache.TryGetValue(name, out var tCached)) return tCached;

            var tb = Module.DefineType(name, TypeAttributes.Public | TypeAttributes.Class);

            void AddProp(string propName, Type type)
            {
                if (string.IsNullOrWhiteSpace(propName)) return;
                var underlying = Nullable.GetUnderlyingType(type) ?? type;
                DefineAutoProperty(tb, propName, underlying);
            }

            // Keys first (default to string; BucketStart prefers DateTime)
            if (keyNames != null)
            {
                foreach (var k in keyNames)
                {
                    var tKey = string.Equals(k, "BucketStart", StringComparison.OrdinalIgnoreCase) ? typeof(DateTime) : typeof(string);
                    AddProp(k, tKey);
                }
            }

            // Values follow projection order and types
            if (valueNames != null && valueTypes != null)
            {
                var len = Math.Min(valueNames.Length, valueTypes.Length);
                for (int i = 0; i < len; i++)
                {
                    AddProp(valueNames[i], valueTypes[i] ?? typeof(object));
                }
            }

            var t = tb.CreateType()!;
            Cache[name] = t;
            return t;
        }
    }

    private static void DefineAutoProperty(TypeBuilder tb, string name, Type type)
    {
        var field = tb.DefineField("_" + name, type, FieldAttributes.Private);
        var prop = tb.DefineProperty(name, PropertyAttributes.HasDefault, type, null);

        var getter = tb.DefineMethod("get_" + name,
            MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig,
            type, Type.EmptyTypes);
        var getIl = getter.GetILGenerator();
        getIl.Emit(OpCodes.Ldarg_0);
        getIl.Emit(OpCodes.Ldfld, field);
        getIl.Emit(OpCodes.Ret);

        var setter = tb.DefineMethod("set_" + name,
            MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig,
            typeof(void), new[] { type });
        var setIl = setter.GetILGenerator();
        setIl.Emit(OpCodes.Ldarg_0);
        setIl.Emit(OpCodes.Ldarg_1);
        setIl.Emit(OpCodes.Stfld, field);
        setIl.Emit(OpCodes.Ret);

        prop.SetGetMethod(getter);
        prop.SetSetMethod(setter);
    }
}