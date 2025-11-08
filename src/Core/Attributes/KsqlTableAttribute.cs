using System;

namespace Ksql.Linq.Core.Attributes;

[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = true)]
public sealed class KsqlTableAttribute : Attribute
{
}
