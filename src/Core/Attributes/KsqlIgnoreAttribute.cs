using System;

namespace Ksql.Linq.Core.Attributes;

[AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
public sealed class KsqlIgnoreAttribute : Attribute
{
    public KsqlIgnoreAttribute()
    {
    }
}
