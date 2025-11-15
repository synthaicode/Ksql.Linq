using System;
using System.Globalization;

namespace Ksql.Linq.Core.Conversion;

internal static class ValueConverter
{
    public static bool TryChangeType(object? value, Type targetType, out object? result)
    {
        result = null;
        if (targetType == null) return false;

        if (value == null)
        {
            result = targetType.IsValueType && Nullable.GetUnderlyingType(targetType) == null
                ? Activator.CreateInstance(targetType)
                : null;
            return true;
        }

        var nonNullableTarget = Nullable.GetUnderlyingType(targetType) ?? targetType;

        try
        {
            if (nonNullableTarget.IsInstanceOfType(value))
            {
                result = value;
                return true;
            }

            if (nonNullableTarget.IsEnum)
            {
                if (value is string s && Enum.TryParse(nonNullableTarget, s, true, out var enumVal))
                {
                    result = enumVal;
                    return true;
                }
                var underlying = Convert.ChangeType(value, Enum.GetUnderlyingType(nonNullableTarget), CultureInfo.InvariantCulture);
                result = Enum.ToObject(nonNullableTarget, underlying!);
                return true;
            }

            result = Convert.ChangeType(value, nonNullableTarget, CultureInfo.InvariantCulture);
            return true;
        }
        catch
        {
            result = null;
            return false;
        }
    }

    public static object? ChangeTypeOrDefault(object? value, Type targetType, object? @default = null)
        => TryChangeType(value, targetType, out var converted) ? converted : @default;
}

