//! The standard normal distribution, for statistical tests' p-values.

const std = @import("std");

const TWO_OVER_SQRT_PI: f128 = 1.12837916709551257389615890312154517168810125865799771368817144342128493688298;
const ONE_OVER_SQRT_PI: f128 = 0.56418958354775628694807945156077258584405062932899885684408572171064246844149;

/// P(Z ≤ z) for a standard normal Z: erfc(−z/√2) / 2.
pub fn normalCdf(z: f64) f64 {
    return erfc(-(z / std.math.sqrt2)) / 2;
}

/// The complementary error function. Its halves, erf below ½ and erfc
/// above, are evaluated in 128-bit precision and rounded, then composed
/// as double-precision erfc is (1 − erf near zero, 2 − erfc(−x) below −½),
/// so a p-value comes out as other engines print it, to the last digit.
pub fn erfc(x: f64) f64 {
    if (std.math.isNan(x)) return x;
    if (x < -0.5) return 2 - erfcAbove(-x);
    if (x < 0) return 1 + erfBelow(-x);
    if (x < 0.5) return 1 - erfBelow(x);
    return erfcAbove(x);
}

/// erf(x) for 0 ≤ x < ½.
fn erfBelow(x: f64) f64 {
    return @floatCast(erfSeries(x));
}

/// erfc(x) for x ≥ ½: 1 − erf below 3, where the series still has bits to
/// spare, and the continued fraction beyond.
fn erfcAbove(x: f64) f64 {
    // erfc(x) rounds to zero past here.
    if (x > 27.3) return 0;
    if (x < 3) return @floatCast(1 - erfSeries(x));
    return @floatCast(erfcFraction(x));
}

/// erf's Taylor series, (2/√π) Σ (−1)ⁿ x²ⁿ⁺¹ / (n! (2n + 1)).
fn erfSeries(x: f128) f128 {
    const xx = x * x;
    var term: f128 = x;
    var sum: f128 = x;
    var n: u32 = 1;
    while (n < 400) : (n += 1) {
        term *= -xx / @as(f128, @floatFromInt(n));
        const add = term / @as(f128, @floatFromInt(2 * n + 1));
        sum += add;
        if (@abs(add) < @abs(sum) * 1e-36) break;
    }
    return sum * TWO_OVER_SQRT_PI;
}

/// erfc(x) = e^(−x²) / √π · 1 / (x + (1/2) / (x + 1 / (x + (3/2) / (x + …)))),
/// by Lentz's method. e^(−x²) is taken in double precision: past 3, erfc
/// is below 2.3·10⁻⁵, so a p-value of 1 − erfc or 2 − erfc keeps none of
/// the digits more precision would add.
fn erfcFraction(x: f128) f128 {
    const tiny: f128 = 1e-300;
    var f: f128 = x;
    var c: f128 = x;
    var d: f128 = 0;
    var k: u32 = 1;
    while (k < 2000) : (k += 1) {
        const a: f128 = @as(f128, @floatFromInt(k)) / 2;
        d = x + a * d;
        if (d == 0) d = tiny;
        c = x + a / c;
        if (c == 0) c = tiny;
        d = 1 / d;
        const delta = c * d;
        f *= delta;
        if (@abs(delta - 1) < 1e-34) break;
    }
    const decay: f128 = @exp(-@as(f64, @floatCast(x * x)));
    return decay * ONE_OVER_SQRT_PI / f;
}

test "erfc matches its known values" {
    const cases = .{
        .{ 0.0, 1.0 },
        .{ 0.25, 0.7236736098317630 },
        .{ 1.0, 0.15729920705028513 },
        .{ -1.0, 1.8427007929497148 },
        .{ 2.5, 4.069520174449590e-4 },
        .{ 3.0, 2.209049699858544e-5 },
        .{ 5.0, 1.5374597944280349e-12 },
        .{ 10.0, 2.088487583762545e-45 },
        .{ 30.0, 0.0 },
        .{ -30.0, 2.0 },
    };
    inline for (cases) |c| try std.testing.expectApproxEqRel(@as(f64, c[1]), erfc(c[0]), 1e-15);
    try std.testing.expect(std.math.isNan(erfc(std.math.nan(f64))));
}

test "normalCdf is symmetric and gives StarRocks' p-values" {
    try std.testing.expectEqual(@as(f64, 0.5), normalCdf(0));
    try std.testing.expectEqual(@as(f64, 0.9772498680518208), 1 - normalCdf(-2));
    inline for (.{ 0.1, 0.7, 1.96, 3.5, 6.0 }) |z| {
        try std.testing.expectApproxEqAbs(@as(f64, 1), normalCdf(z) + normalCdf(-z), 1e-15);
    }
}
