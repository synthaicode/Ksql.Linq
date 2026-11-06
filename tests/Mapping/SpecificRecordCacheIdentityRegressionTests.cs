using Avro;
using Avro.Specific;
using Ksql.Linq.Mapping;
using System;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading.Tasks;
using Xunit;

namespace Ksql.Linq.Tests.Mapping;

[Trait("Level", "L3")]
public class SpecificRecordCacheIdentityRegressionTests
{
    [Theory]
    [InlineData("Pinned", null, false)]
    [InlineData(null, "pinned.records", false)]
    [InlineData("Pinned", "pinned.records", false)]
    [InlineData("Pinned", null, true)]
    [InlineData(null, "pinned.records", true)]
    [InlineData("Pinned", "pinned.records", true)]
    public void Generate_DifferentSchemaIdentity_IsIndependentInEitherOrder(
        string? name, string? ns, bool overrideFirst)
    {
        var source = CreateSource("records.Case" + Guid.NewGuid().ToString("N"), typeof(int));
        Type defaultType;
        Type overrideType;
        if (overrideFirst)
        {
            overrideType = SpecificRecordGenerator.Generate(source, name, ns);
            defaultType = SpecificRecordGenerator.Generate(source);
        }
        else
        {
            defaultType = SpecificRecordGenerator.Generate(source);
            overrideType = SpecificRecordGenerator.Generate(source, name, ns);
        }

        AssertRecord(defaultType, source.Name + "Avro", source.Namespace!, typeof(int), 17);
        AssertRecord(overrideType, (name ?? source.Name) + "Avro", ns ?? source.Namespace!, typeof(int), 23);
        Assert.NotSame(defaultType, overrideType);
        Assert.NotEqual(defaultType.FullName, overrideType.FullName);
        Assert.Same(defaultType, SpecificRecordGenerator.Generate(source));
        Assert.Same(overrideType, SpecificRecordGenerator.Generate(source, name, ns));
    }

    [Fact]
    public void Generate_EquivalentDefaultOverrides_ReuseImplementation()
    {
        var source = CreateSource("records.Defaults" + Guid.NewGuid().ToString("N"), typeof(int));
        var expected = SpecificRecordGenerator.Generate(source);
        foreach (var name in new string?[] { null, "", " \t", source.Name })
        foreach (var ns in new string?[] { null, "", " \t", source.Namespace })
            Assert.Same(expected, SpecificRecordGenerator.Generate(source, name, ns));
        AssertRecord(expected, source.Name + "Avro", source.Namespace!, typeof(int), 31);
    }

    [Fact]
    public void Generate_SameFullNameFromDifferentAssemblies_PreservesSourceTypeIdentity()
    {
        var fullName = "records.Shared" + Guid.NewGuid().ToString("N");
        var intSource = CreateSource(fullName, typeof(int));
        var longSource = CreateSource(fullName, typeof(long));
        Assert.Equal(intSource.FullName, longSource.FullName);
        Assert.NotEqual(intSource.Assembly, longSource.Assembly);

        var intRecord = SpecificRecordGenerator.Generate(intSource);
        var longRecord = SpecificRecordGenerator.Generate(longSource);

        AssertRecord(intRecord, intSource.Name + "Avro", intSource.Namespace!, typeof(int), 41);
        AssertRecord(longRecord, longSource.Name + "Avro", longSource.Namespace!, typeof(long), 43L);
        Assert.NotSame(intRecord, longRecord);
        Assert.Same(intRecord, SpecificRecordGenerator.Generate(intSource));
        Assert.Same(longRecord, SpecificRecordGenerator.Generate(longSource));
    }

    [Fact]
    public async Task Generate_ParallelEquivalentAndDifferentIdentities_ReuseOnlyMatchingImplementation()
    {
        var source = CreateSource("records.Parallel" + Guid.NewGuid().ToString("N"), typeof(int));
        var requests = Enumerable.Range(0, 64).Select(i => Task.Run(() =>
        {
            var variant = i % 4;
            var name = variant == 0 ? null : "Variant" + variant;
            var ns = variant < 2 ? null : "parallel.variant" + variant;
            return (Variant: variant, Type: SpecificRecordGenerator.Generate(source, name, ns));
        })).ToArray();
        var results = await Task.WhenAll(requests);
        var implementations = results.GroupBy(r => r.Variant).OrderBy(g => g.Key)
            .Select(group =>
            {
                var expected = group.First().Type;
                Assert.All(group, result => Assert.Same(expected, result.Type));
                var variant = group.Key;
                AssertRecord(expected, (variant == 0 ? source.Name : "Variant" + variant) + "Avro",
                    variant < 2 ? source.Namespace! : "parallel.variant" + variant, typeof(int), 47);
                return expected;
            }).ToArray();
        Assert.Equal(4, implementations.Distinct().Count());
        Assert.Equal(4, implementations.Select(t => t.FullName).Distinct().Count());
    }

    [Fact]
    public void Generate_SourceNameMatchingEmittedVariant_DoesNotCollide()
    {
        var source = CreateSource("records.Collision" + Guid.NewGuid().ToString("N"), typeof(int));
        var original = SpecificRecordGenerator.Generate(source);
        var variant = SpecificRecordGenerator.Generate(source, "Pinned", "collision.variant");
        Assert.NotEqual(original.FullName, variant.FullName);
        Assert.EndsWith("Avro", variant.FullName);
        var collidingSource = CreateSource(variant.FullName![..^4], typeof(long));
        Assert.Equal(variant.FullName, collidingSource.FullName + "Avro");

        var collisionRecord = SpecificRecordGenerator.Generate(collidingSource);

        Assert.NotEqual(variant.FullName, collisionRecord.FullName);
        AssertRecord(variant, "PinnedAvro", "collision.variant", typeof(int), 53);
        AssertRecord(collisionRecord, collidingSource.Name + "Avro", collidingSource.Namespace!, typeof(long), 59L);
        Assert.Same(collisionRecord, SpecificRecordGenerator.Generate(collidingSource));
    }

    private static void AssertRecord(Type implementation, string name, string ns, Type valueType, object value)
    {
        var record = Assert.IsAssignableFrom<ISpecificRecord>(Activator.CreateInstance(implementation));
        var schema = Assert.IsType<RecordSchema>(record.Schema);
        Assert.Equal(name, schema.Name);
        Assert.Equal(ns, schema.Namespace);
        var field = Assert.Single(schema.Fields);
        Assert.Equal("Value", field.Name);
        Assert.Equal(valueType == typeof(int) ? Schema.Type.Int : Schema.Type.Long, field.Schema.Tag);
        Assert.Equal(valueType, implementation.GetProperty("Value")!.PropertyType);
        record.Put(0, value);
        Assert.Equal(value, record.Get(0));
        Assert.Equal(value, implementation.GetProperty("Value")!.GetValue(record));
    }

    private static Type CreateSource(string fullName, Type valueType)
    {
        var assembly = AssemblyBuilder.DefineDynamicAssembly(
            new AssemblyName("SchemaIdentitySource" + Guid.NewGuid().ToString("N")), AssemblyBuilderAccess.Run);
        var builder = assembly.DefineDynamicModule("Main").DefineType(fullName, TypeAttributes.Public);
        var field = builder.DefineField("_value", valueType, FieldAttributes.Private);
        var property = builder.DefineProperty("Value", PropertyAttributes.None, valueType, null);
        var getter = builder.DefineMethod("get_Value", MethodAttributes.Public | MethodAttributes.SpecialName,
            valueType, Type.EmptyTypes);
        var getIl = getter.GetILGenerator();
        getIl.Emit(OpCodes.Ldarg_0);
        getIl.Emit(OpCodes.Ldfld, field);
        getIl.Emit(OpCodes.Ret);
        var setter = builder.DefineMethod("set_Value", MethodAttributes.Public | MethodAttributes.SpecialName,
            typeof(void), new[] { valueType });
        var setIl = setter.GetILGenerator();
        setIl.Emit(OpCodes.Ldarg_0);
        setIl.Emit(OpCodes.Ldarg_1);
        setIl.Emit(OpCodes.Stfld, field);
        setIl.Emit(OpCodes.Ret);
        property.SetGetMethod(getter);
        property.SetSetMethod(setter);
        return builder.CreateType()!;
    }
}
