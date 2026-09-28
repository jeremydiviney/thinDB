//! MySQL miscellaneous scalar functions (issue #245): ELT, INSERT, QUOTE,
//! SOUNDEX / SOUNDS LIKE, the INET family, INTERVAL(N, ...), SLEEP, the
//! REGEXP_* match-type and position arguments, the JSON constructors and
//! aggregates, LAST_INSERT_ID() and ROW_COUNT(); then text read where a
//! number is expected, HEX of numbers, regex line anchors, JSON read as text
//! by string functions, and the information functions (#276-#282); then
//! doubles as text, fractional integer arguments, JSON_QUOTE, COLLATION and
//! BENCHMARK (#295-#299); then numbers and booleans as text, oversized
//! string results, MAKE_SET, UUID_SHORT, CURRENT_ROLE and system variables
//! in expressions (#316, #318). Expected values are MySQL 8.4 output for the
//! same statements.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");

const Case = struct { sql: []const u8, want: ?[]const u8 };

const scalar_cases = [_]Case{
    .{ .sql = "ELT(1, 'a', 'b')", .want = "a" },
    .{ .sql = "ELT(2, 'a', 'b')", .want = "b" },
    .{ .sql = "ELT(3, 'a', 'b')", .want = null },
    .{ .sql = "ELT(0, 'a')", .want = null },
    .{ .sql = "ELT(-1, 'a')", .want = null },
    .{ .sql = "ELT(NULL, 'a')", .want = null },
    .{ .sql = "ELT(1.5, 'a', 'b')", .want = "b" },
    .{ .sql = "ELT(2, 'a', NULL)", .want = null },
    .{ .sql = "ELT(1, 'héllo', 'x')", .want = "héllo" },
    .{ .sql = "INSERT('abcdef', 2, 3, 'XY')", .want = "aXYef" },
    .{ .sql = "INSERT('abcdef', 0, 3, 'XY')", .want = "abcdef" },
    .{ .sql = "INSERT('abcdef', 7, 3, 'XY')", .want = "abcdef" },
    .{ .sql = "INSERT('abcdef', 6, 3, 'XY')", .want = "abcdeXY" },
    .{ .sql = "INSERT('abcdef', 2, -1, 'XY')", .want = "aXY" },
    .{ .sql = "INSERT('abcdef', 1, 100, '')", .want = "" },
    .{ .sql = "INSERT('héllo', 2, 2, 'ü')", .want = "hülo" },
    .{ .sql = "INSERT('', 1, 1, 'x')", .want = "" },
    .{ .sql = "INSERT(NULL, 1, 1, 'x')", .want = null },
    .{ .sql = "INSERT('abc', NULL, 1, 'x')", .want = null },
    .{ .sql = "INSERT('abc', 1, 1, NULL)", .want = null },
    .{ .sql = "QUOTE('it''s')", .want = "'it\\'s'" },
    .{ .sql = "CONCAT('[', QUOTE(NULL), ']')", .want = "[NULL]" },
    .{ .sql = "QUOTE('')", .want = "''" },
    .{ .sql = "QUOTE('a\\\\b')", .want = "'a\\\\b'" },
    .{ .sql = "QUOTE(CONCAT('a', CHAR(0), 'b', CHAR(26)))", .want = "'a\\0b\\Z'" },
    .{ .sql = "QUOTE('héllo')", .want = "'héllo'" },
    .{ .sql = "SOUNDEX('Tymczak')", .want = "T520" },
    .{ .sql = "SOUNDEX('Robert')", .want = "R163" },
    .{ .sql = "SOUNDEX('Ashcraft')", .want = "A2613" },
    .{ .sql = "SOUNDEX('Quadratically')", .want = "Q36324" },
    .{ .sql = "SOUNDEX('Hello World')", .want = "H4643" },
    .{ .sql = "SOUNDEX('')", .want = "" },
    .{ .sql = "SOUNDEX('123')", .want = "" },
    .{ .sql = "SOUNDEX('éab')", .want = "é100" },
    .{ .sql = "SOUNDEX(NULL)", .want = null },
    .{ .sql = "'Robert' SOUNDS LIKE 'Rupert'", .want = "1" },
    .{ .sql = "'Robert' SOUNDS LIKE 'Tymczak'", .want = "0" },
    .{ .sql = "INET_ATON('10.0.5.9')", .want = "167773449" },
    .{ .sql = "INET_ATON('127.1')", .want = "2130706433" },
    .{ .sql = "INET_ATON('255.255.255.255')", .want = "4294967295" },
    .{ .sql = "INET_ATON('1.2.3.256')", .want = null },
    .{ .sql = "INET_ATON('1.2.3.4.5')", .want = null },
    .{ .sql = "INET_ATON('')", .want = null },
    .{ .sql = "INET_ATON(NULL)", .want = null },
    .{ .sql = "INET_NTOA(167773449)", .want = "10.0.5.9" },
    .{ .sql = "INET_NTOA(0)", .want = "0.0.0.0" },
    .{ .sql = "INET_NTOA(4294967295)", .want = "255.255.255.255" },
    .{ .sql = "INET_NTOA(4294967296)", .want = null },
    .{ .sql = "INET_NTOA(-1)", .want = null },
    .{ .sql = "INET_NTOA(1.5e0)", .want = "0.0.0.2" },
    .{ .sql = "INET_NTOA(NULL)", .want = null },
    .{ .sql = "LOWER(HEX(INET6_ATON('fdfe::5a55:caff:fefa:9089')))", .want = "fdfe0000000000005a55cafffefa9089" },
    .{ .sql = "LOWER(HEX(INET6_ATON('10.0.5.9')))", .want = "0a000509" },
    .{ .sql = "LOWER(HEX(INET6_ATON('::ffff:10.0.5.9')))", .want = "00000000000000000000ffff0a000509" },
    .{ .sql = "INET6_ATON('x')", .want = null },
    .{ .sql = "INET6_ATON(NULL)", .want = null },
    .{ .sql = "INET6_NTOA(INET6_ATON('fdfe::5a55:caff:fefa:9089'))", .want = "fdfe::5a55:caff:fefa:9089" },
    .{ .sql = "INET6_NTOA(INET6_ATON('::ffff:10.0.5.9'))", .want = "::ffff:10.0.5.9" },
    .{ .sql = "INET6_NTOA(INET6_ATON('10.0.5.9'))", .want = "10.0.5.9" },
    .{ .sql = "INET6_NTOA(INET6_ATON('::'))", .want = "::" },
    .{ .sql = "INET6_NTOA(INET6_ATON('1:0:0:2:0:0:0:3'))", .want = "1:0:0:2::3" },
    .{ .sql = "INET6_NTOA(INET6_ATON('::10.0.5.9'))", .want = "::10.0.5.9" },
    .{ .sql = "INET6_NTOA('abc')", .want = null },
    .{ .sql = "INET6_NTOA(NULL)", .want = null },
    .{ .sql = "IS_IPV4('10.0.5.9')", .want = "1" },
    .{ .sql = "IS_IPV4('10.0.5.256')", .want = "0" },
    .{ .sql = "IS_IPV4('::1')", .want = "0" },
    .{ .sql = "IS_IPV4(NULL)", .want = null },
    .{ .sql = "IS_IPV6('::1')", .want = "1" },
    .{ .sql = "IS_IPV6('10.0.5.9')", .want = "0" },
    .{ .sql = "IS_IPV6('fdfe::5a55:caff:fefa:9089')", .want = "1" },
    .{ .sql = "IS_IPV4_COMPAT(INET6_ATON('::10.0.5.9'))", .want = "1" },
    .{ .sql = "IS_IPV4_COMPAT(INET6_ATON('::ffff:10.0.5.9'))", .want = "0" },
    .{ .sql = "IS_IPV4_MAPPED(INET6_ATON('::ffff:10.0.5.9'))", .want = "1" },
    .{ .sql = "IS_IPV4_MAPPED(INET6_ATON('::10.0.5.9'))", .want = "0" },
    .{ .sql = "INTERVAL(5, 1, 3, 7)", .want = "2" },
    .{ .sql = "INTERVAL(0, 1, 2)", .want = "0" },
    .{ .sql = "INTERVAL(99, 1, 2)", .want = "2" },
    .{ .sql = "INTERVAL(3, 1, 3, 7)", .want = "2" },
    .{ .sql = "INTERVAL(NULL, 1, 2)", .want = "-1" },
    .{ .sql = "INTERVAL(5, NULL, 3, NULL, 7)", .want = "3" },
    .{ .sql = "INTERVAL(2.5, 1, 2.5, 3)", .want = "2" },
    .{ .sql = "1 + INTERVAL(5, 1, 3, 7)", .want = "3" },
    .{ .sql = "SLEEP(0)", .want = "0" },
    .{ .sql = "SLEEP(0.01)", .want = "0" },
    .{ .sql = "REGEXP_INSTR('dog cat dog', 'dog')", .want = "1" },
    .{ .sql = "REGEXP_INSTR('dog cat dog', 'dog', 2)", .want = "9" },
    .{ .sql = "REGEXP_INSTR('dog cat dog', 'dog', 1, 2)", .want = "9" },
    .{ .sql = "REGEXP_INSTR('dog cat dog', 'dog', 1, 3)", .want = "0" },
    .{ .sql = "REGEXP_INSTR('dog cat dog', 'dog', 1, 1, 1)", .want = "4" },
    .{ .sql = "REGEXP_INSTR('dog cat dog', 'dog', 1, 2, 1)", .want = "12" },
    .{ .sql = "REGEXP_INSTR('héllo wörld', 'w')", .want = "7" },
    .{ .sql = "REGEXP_INSTR('héllo wörld', 'ö', 1, 1, 1)", .want = "9" },
    .{ .sql = "REGEXP_INSTR('abc', 'x')", .want = "0" },
    .{ .sql = "REGEXP_INSTR(NULL, 'a')", .want = null },
    .{ .sql = "REGEXP_INSTR('a', 'a', NULL)", .want = null },
    .{ .sql = "REGEXP_INSTR('abc', 'c', 3)", .want = "3" },
    .{ .sql = "REGEXP_INSTR('abc', 'c', 1, 0)", .want = "3" },
    .{ .sql = "REGEXP_INSTR('aaa', 'a', 1, 4)", .want = "0" },
    .{ .sql = "REGEXP_INSTR('', 'a')", .want = "0" },
    .{ .sql = "REGEXP_INSTR('', 'a', 2)", .want = "0" },
    .{ .sql = "REGEXP_INSTR('abc', 'x*', 1, 2)", .want = "2" },
    .{ .sql = "REGEXP_INSTR('ABC', 'b', 1, 1, 0, 'i')", .want = "2" },
    .{ .sql = "REGEXP_INSTR('ABC', 'b', 1, 1, 0, 'c')", .want = "0" },
    .{ .sql = "REGEXP_INSTR('ABC', 'b', 1, 1, 0, 'ci')", .want = "2" },
    .{ .sql = "REGEXP_INSTR('ABC', 'b', 1, 1, 0, 'ic')", .want = "0" },
    .{ .sql = "REGEXP_LIKE('abc', 'b')", .want = "1" },
    .{ .sql = "REGEXP_LIKE('ABC', 'b', 'c')", .want = "0" },
    .{ .sql = "REGEXP_LIKE('ABC', 'b', 'i')", .want = "1" },
    .{ .sql = "REGEXP_LIKE('a\\nb', 'a.b')", .want = "0" },
    .{ .sql = "REGEXP_LIKE('a\\nb', 'a.b', 'n')", .want = "1" },
    .{ .sql = "REGEXP_LIKE('B', '[^b]', 'i')", .want = "0" },
    .{ .sql = "REGEXP_LIKE('B', '[a-c]', 'i')", .want = "1" },
    .{ .sql = "REGEXP_LIKE('a', 'a', NULL)", .want = null },
    .{ .sql = "REGEXP_REPLACE('a b c', 'b', 'X')", .want = "a X c" },
    .{ .sql = "REGEXP_REPLACE('abc abc', 'b', 'X', 5)", .want = "abc aXc" },
    .{ .sql = "REGEXP_REPLACE('abc abc abc', 'b', 'X', 1, 2)", .want = "abc aXc abc" },
    .{ .sql = "REGEXP_REPLACE('abc abc abc', 'b', 'X', 1, 0)", .want = "aXc aXc aXc" },
    .{ .sql = "REGEXP_REPLACE('abc abc abc', 'b', 'X', 3, 1)", .want = "abc aXc abc" },
    .{ .sql = "REGEXP_REPLACE('ABC', 'b', 'X', 1, 0, 'i')", .want = "AXC" },
    .{ .sql = "REGEXP_REPLACE('abc abc', 'b', 'X', 1, 5)", .want = "abc abc" },
    .{ .sql = "REGEXP_REPLACE('héllo wörld', 'l', 'L', 4)", .want = "hélLo wörLd" },
    .{ .sql = "REGEXP_REPLACE('aaa', 'a*', 'X')", .want = "XX" },
    .{ .sql = "REGEXP_REPLACE('abc', 'x*', '-')", .want = "-a-b-c-" },
    .{ .sql = "REGEXP_REPLACE('héllo', 'x*', '-')", .want = "-h-é-l-l-o-" },
    .{ .sql = "REGEXP_REPLACE('abc', 'b', 'X', 1, NULL)", .want = null },
    .{ .sql = "REGEXP_SUBSTR('abc abd abe', 'ab.')", .want = "abc" },
    .{ .sql = "REGEXP_SUBSTR('abc abd abe', 'ab.', 2)", .want = "abd" },
    .{ .sql = "REGEXP_SUBSTR('abc abd abe', 'ab.', 1, 3)", .want = "abe" },
    .{ .sql = "REGEXP_SUBSTR('abc abd abe', 'ab.', 1, 4)", .want = null },
    .{ .sql = "REGEXP_SUBSTR('ABC', 'b', 1, 1, 'i')", .want = "B" },
    .{ .sql = "REGEXP_SUBSTR('héllo', 'l+', 2)", .want = "ll" },
    .{ .sql = "REGEXP_SUBSTR('abc', 'b', 4)", .want = null },
    .{ .sql = "REGEXP_SUBSTR('abc', 'b', 1, NULL)", .want = null },
};

/// Text passed where a number is expected reads its leading number, as
/// MySQL does with no CAST: whitespace, sign, digits, fraction, exponent;
/// nothing numeric reads as 0. An integer parameter stops at the '.'.
const text_number_cases = [_]Case{
    .{ .sql = "REPEAT('a', '3')", .want = "aaa" },
    .{ .sql = "REPEAT('a', '2.7')", .want = "aa" },
    .{ .sql = "REPEAT('a', '3abc')", .want = "aaa" },
    .{ .sql = "REPEAT('a', 'abc')", .want = "" },
    .{ .sql = "REPEAT('a', '1e1')", .want = "a" },
    .{ .sql = "REPEAT('a', '\\t2')", .want = "aa" },
    .{ .sql = "REPEAT('a', '\\n2')", .want = "" },
    .{ .sql = "LEFT('abcdef', '2')", .want = "ab" },
    .{ .sql = "SUBSTRING('abcdef', '2', '3')", .want = "bcd" },
    .{ .sql = "ELT('2', 'x', 'y')", .want = "y" },
    .{ .sql = "'3abc' + 0", .want = "3" },
    .{ .sql = "'1.5' + 1", .want = "2.5" },
    .{ .sql = "'1e2' + 0", .want = "100" },
    .{ .sql = "'abc' + 0", .want = "0" },
    .{ .sql = "'' + 0", .want = "0" },
    .{ .sql = "' 12 ' + 0", .want = "12" },
    .{ .sql = "'-.5e1x' + 0", .want = "-5" },
    .{ .sql = "'1e+' + 0", .want = "1" },
    .{ .sql = "'0x10' + 0", .want = "0" },
    .{ .sql = "'\\n2' + 0", .want = "2" },
    .{ .sql = "'2' * '3'", .want = "6" },
    .{ .sql = "'10' / '4'", .want = "2.5" },
    .{ .sql = "'7' DIV '2'", .want = "3" },
    .{ .sql = "'7' % '3'", .want = "1" },
    .{ .sql = "-'3'", .want = "-3" },
    .{ .sql = "ABS('-3')", .want = "3" },
    .{ .sql = "ROUND('2.567', 1)", .want = "2.6" },
    .{ .sql = "SQRT('16')", .want = "4" },
    .{ .sql = "POW('2', '3')", .want = "8" },
    .{ .sql = "JSON_EXTRACT('{\"a\": \"7x\"}', '$.a') + 1", .want = "8" },
    .{ .sql = "JSON_EXTRACT('{\"a\": true}', '$.a') + 1", .want = "2" },
    .{ .sql = "JSON_EXTRACT('{\"a\": 2.5}', '$.a') * 2", .want = "5" },
    .{ .sql = "REPEAT('a', JSON_EXTRACT('{\"a\": 3}', '$.a'))", .want = "aaa" },
    .{ .sql = "CAST(JSON_EXTRACT('{\"a\": 5}', '$.a') AS SIGNED)", .want = "5" },
};

/// HEX of a number is the hex of its integer value, two's complement for a
/// negative one; a decimal rounds half away from zero, a double to even.
const hex_cases = [_]Case{
    .{ .sql = "HEX(255)", .want = "FF" },
    .{ .sql = "HEX(0)", .want = "0" },
    .{ .sql = "HEX(-1)", .want = "FFFFFFFFFFFFFFFF" },
    .{ .sql = "HEX(-255)", .want = "FFFFFFFFFFFFFF01" },
    .{ .sql = "HEX(CAST(3000000000 AS SIGNED))", .want = "B2D05E00" },
    .{ .sql = "HEX(TRUE)", .want = "1" },
    .{ .sql = "HEX(1.5)", .want = "2" },
    .{ .sql = "HEX(2.5)", .want = "3" },
    .{ .sql = "HEX(-1.5)", .want = "FFFFFFFFFFFFFFFE" },
    .{ .sql = "HEX(-0.4)", .want = "0" },
    .{ .sql = "HEX(123.456)", .want = "7B" },
    .{ .sql = "HEX(2.5e0)", .want = "2" },
    .{ .sql = "HEX(3.5e0)", .want = "4" },
    .{ .sql = "HEX(1e19)", .want = "7FFFFFFFFFFFFFFF" },
    .{ .sql = "HEX(NULL)", .want = null },
    .{ .sql = "HEX('z')", .want = "7A" },
    .{ .sql = "HEX('abc')", .want = "616263" },
    .{ .sql = "HEX('')", .want = "" },
    .{ .sql = "HEX(CAST('2024-01-02' AS DATE))", .want = "323032342D30312D3032" },
    .{ .sql = "LOWER(HEX(255))", .want = "ff" },
};

/// `^` and `$` hold only at the ends of the text unless the `m` match type
/// asks for line anchors too.
const regex_anchor_cases = [_]Case{
    .{ .sql = "'a\\nb' REGEXP '^b'", .want = "0" },
    .{ .sql = "'a\\nb' REGEXP 'a$'", .want = "0" },
    .{ .sql = "'a\\nb' REGEXP '^a'", .want = "1" },
    .{ .sql = "'a\\nb' REGEXP 'b$'", .want = "1" },
    .{ .sql = "REGEXP_LIKE('a\\nb', '^b', 'm')", .want = "1" },
    .{ .sql = "REGEXP_LIKE('a\\nb', 'a$', 'm')", .want = "1" },
    .{ .sql = "REGEXP_LIKE('a\\nb', '^b', 'mc')", .want = "1" },
    .{ .sql = "REGEXP_LIKE('a\\nb\\nc', '^b$')", .want = "0" },
    .{ .sql = "REGEXP_LIKE('a\\nb\\nc', '^b$', 'm')", .want = "1" },
    .{ .sql = "REGEXP_REPLACE('a\\nb', '^', '>')", .want = ">a\nb" },
    .{ .sql = "REGEXP_REPLACE('a\\nb', '^', '>', 1, 0, 'm')", .want = ">a\n>b" },
    .{ .sql = "REGEXP_REPLACE('a\\nb', '$', '<', 1, 0, 'm')", .want = "a<\nb<" },
    .{ .sql = "REGEXP_REPLACE('ab\\ncd', '^.', 'X', 1, 0, 'm')", .want = "Xb\nXd" },
    .{ .sql = "REGEXP_INSTR('a\\nb', '^b')", .want = "0" },
    .{ .sql = "REGEXP_INSTR('a\\nb', '^b', 1, 1, 0, 'm')", .want = "3" },
    .{ .sql = "REGEXP_SUBSTR('x1\\ny2', '^y.')", .want = null },
    .{ .sql = "REGEXP_SUBSTR('x1\\ny2', '^y.', 1, 1, 'm')", .want = "y2" },
};

/// A string function reads a JSON argument as its text, as does a CASE,
/// IF or COALESCE that mixes JSON with text.
const json_text_cases = [_]Case{
    .{ .sql = "LOWER(CAST('{\"a\": \"Xy\", \"b\": 5}' AS JSON))", .want = "{\"a\": \"xy\", \"b\": 5}" },
    .{ .sql = "UPPER(CAST('{\"a\": \"Xy\", \"b\": 5}' AS JSON))", .want = "{\"A\": \"XY\", \"B\": 5}" },
    .{ .sql = "LENGTH(CAST('{\"a\": \"Xy\", \"b\": 5}' AS JSON))", .want = "19" },
    .{ .sql = "CHAR_LENGTH(JSON_OBJECT('k', 'héllo'))", .want = "14" },
    .{ .sql = "CONCAT(CAST('[1, 2]' AS JSON), '!')", .want = "[1, 2]!" },
    .{ .sql = "CONCAT(JSON_OBJECT('a', 1), JSON_ARRAY(2))", .want = "{\"a\": 1}[2]" },
    .{ .sql = "REPLACE(CAST('{\"a\": \"Xy\", \"b\": 5}' AS JSON), '\"', '')", .want = "{a: Xy, b: 5}" },
    .{ .sql = "HEX(CAST('\"q\"' AS JSON))", .want = "227122" },
    .{ .sql = "MD5(CAST('[1, 2]' AS JSON)) = MD5('[1, 2]')", .want = "1" },
    .{ .sql = "CAST('{\"a\": \"Xy\"}' AS JSON) LIKE '%Xy%'", .want = "1" },
    .{ .sql = "SUBSTRING(CAST('[1, 2]' AS JSON), 2, 1)", .want = "1" },
    .{ .sql = "TRIM(CAST('\"q\"' AS JSON))", .want = "\"q\"" },
    .{ .sql = "LOCATE('b', JSON_OBJECT('b', 1))", .want = "3" },
    .{ .sql = "REVERSE(JSON_ARRAY(1, 2))", .want = "]2 ,1[" },
    .{ .sql = "COALESCE(CAST(NULL AS JSON), 'none')", .want = "none" },
    .{ .sql = "IFNULL(CAST('\"q\"' AS JSON), 'x')", .want = "\"q\"" },
    .{ .sql = "CASE WHEN 1 = 1 THEN CAST('[1, 2]' AS JSON) ELSE 'x' END", .want = "[1, 2]" },
    .{ .sql = "CASE WHEN 1 = 0 THEN CAST('[1, 2]' AS JSON) ELSE 'x' END", .want = "x" },
    .{ .sql = "IF(1 = 1, JSON_OBJECT('a', 1), 'x')", .want = "{\"a\": 1}" },
};

/// CHARSET names the character set of its argument's type.
const charset_cases = [_]Case{
    .{ .sql = "CHARSET('a')", .want = "utf8mb4" },
    .{ .sql = "CHARSET(CONCAT('a', 1))", .want = "utf8mb4" },
    .{ .sql = "CHARSET(CAST(NULL AS CHAR))", .want = "utf8mb4" },
    .{ .sql = "CHARSET(JSON_ARRAY(1))", .want = "utf8mb4" },
    .{ .sql = "CHARSET(1)", .want = "binary" },
    .{ .sql = "CHARSET(1.5)", .want = "binary" },
    .{ .sql = "CHARSET(1e0)", .want = "binary" },
    .{ .sql = "CHARSET(TRUE)", .want = "binary" },
    .{ .sql = "CHARSET(DATE '2020-01-01')", .want = "binary" },
    .{ .sql = "CHARSET(NOW())", .want = "binary" },
    .{ .sql = "CONCAT(CHARSET('a'), '!')", .want = "utf8mb4!" },
    .{ .sql = "CHARSET('a') = 'utf8mb4'", .want = "1" },
};

/// Each is wrapped in `CAST(... AS CHAR)`, which MySQL renders the same as
/// its JSON result.
const json_cases = [_]Case{
    .{ .sql = "JSON_ARRAY()", .want = "[]" },
    .{ .sql = "JSON_ARRAY(NULL)", .want = "[null]" },
    .{ .sql = "JSON_ARRAY(1, 'a', NULL, TRUE, FALSE, 1.50, -3)", .want = "[1, \"a\", null, true, false, 1.50, -3]" },
    .{ .sql = "JSON_ARRAY(1.5e-15, 1.5e-16, 1e15, 100.0e0, 0.1, 2.5)", .want = "[0.0000000000000015, 1.5e-16, 1e15, 100.0, 0.1, 2.5]" },
    .{ .sql = "JSON_ARRAY('a\"b', 'c\\\\d', 'e\\nf', 'héllo', '[1,2]')", .want = "[\"a\\\"b\", \"c\\\\d\", \"e\\nf\", \"héllo\", \"[1,2]\"]" },
    .{ .sql = "JSON_ARRAY(CAST('[1,2]' AS JSON), CAST(1.25 AS DECIMAL(10,4)))", .want = "[[1, 2], 1.2500]" },
    .{ .sql = "JSON_ARRAY(DATE '2024-01-02', TIMESTAMP '2024-01-02 03:04:05', TIMESTAMP '2024-01-02 03:04:05.123456')", .want = "[\"2024-01-02\", \"2024-01-02 03:04:05.000000\", \"2024-01-02 03:04:05.123456\"]" },
    .{ .sql = "JSON_OBJECT()", .want = "{}" },
    .{ .sql = "JSON_OBJECT('b', 1, 'a', 'x', 'aa', 2, 'c', NULL)", .want = "{\"a\": \"x\", \"b\": 1, \"c\": null, \"aa\": 2}" },
    .{ .sql = "JSON_OBJECT('a', 1, 'a', 2)", .want = "{\"a\": 2}" },
    .{ .sql = "JSON_OBJECT(1, 2, 1.5, 3)", .want = "{\"1\": 2, \"1.5\": 3}" },
    .{ .sql = "JSON_OBJECT('k', JSON_ARRAY(1, 2), 'o', JSON_OBJECT('z', 1, 'y', 2))", .want = "{\"k\": [1, 2], \"o\": {\"y\": 2, \"z\": 1}}" },
    .{ .sql = "JSON_KEYS(JSON_OBJECT('bb', 1, 'a', 2, 'ccc', 3, 'b', 4))", .want = "[\"a\", \"b\", \"bb\", \"ccc\"]" },
    .{ .sql = "JSON_TYPE(JSON_EXTRACT(JSON_ARRAY(CAST(1.5 AS DECIMAL(4,2))), '$[0]'))", .want = "DECIMAL" },
    .{ .sql = "JSON_EXTRACT(JSON_OBJECT('a', CAST(2.50 AS DECIMAL(5,2))), '$.a')", .want = "2.50" },
    .{ .sql = "JSON_UNQUOTE(JSON_EXTRACT(JSON_OBJECT('a', CAST(2.50 AS DECIMAL(5,2))), '$.a'))", .want = "2.50" },
    .{ .sql = "CAST(JSON_ARRAY(1, 2) AS CHAR)", .want = "[1, 2]" },
    .{ .sql = "CAST('{\"b\": [1, 2.5], \"a\": {\"y\": null}}' AS JSON)", .want = "{\"a\": {\"y\": null}, \"b\": [1, 2.5]}" },
};

/// A double or float becomes text as MySQL writes it, wherever it does:
/// shortest round-trip digits, positional for a decimal exponent in
/// -15..14, `d.ddde[-]x` otherwise.
const double_text_cases = [_]Case{
    .{ .sql = "CAST(1e100 AS CHAR)", .want = "1e100" },
    .{ .sql = "CAST(1e15 AS CHAR)", .want = "1e15" },
    .{ .sql = "CAST(1e14 AS CHAR)", .want = "100000000000000" },
    .{ .sql = "CAST(1e16 AS CHAR)", .want = "1e16" },
    .{ .sql = "CAST(1.5e17 AS CHAR)", .want = "1.5e17" },
    .{ .sql = "CAST(100e0 AS CHAR)", .want = "100" },
    .{ .sql = "CAST(-0e0 AS CHAR)", .want = "-0" },
    .{ .sql = "CAST(0e0 AS CHAR)", .want = "0" },
    .{ .sql = "CAST(1e-15 AS CHAR)", .want = "0.000000000000001" },
    .{ .sql = "CAST(1e-16 AS CHAR)", .want = "1e-16" },
    .{ .sql = "CAST(1.5e-15 AS CHAR)", .want = "0.0000000000000015" },
    .{ .sql = "CAST(-1.5e0 AS CHAR)", .want = "-1.5" },
    .{ .sql = "CAST(1e15 + 0.5 AS CHAR)", .want = "1000000000000000.5" },
    .{ .sql = "CAST(1.7976931348623157e308 AS CHAR)", .want = "1.7976931348623157e308" },
    .{ .sql = "CAST(5e-324 AS CHAR)", .want = "5e-324" },
    .{ .sql = "CONCAT(1e100, '')", .want = "1e100" },
    .{ .sql = "CONCAT(100e0, 'x')", .want = "100x" },
    .{ .sql = "CONCAT(0.1e0 + 0.2e0, '')", .want = "0.30000000000000004" },
    .{ .sql = "CONCAT(-1.5e-16, '')", .want = "-1.5e-16" },
    .{ .sql = "CONCAT(POW(2, 70), '')", .want = "1.1805916207174113e21" },
    .{ .sql = "CONCAT(1 / 3e0, '')", .want = "0.3333333333333333" },
    .{ .sql = "CONCAT(SQRT(2), '')", .want = "1.4142135623730951" },
    .{ .sql = "CONCAT_WS(',', 1e20, 2.5e0)", .want = "1e20,2.5" },
    .{ .sql = "LENGTH(1e100)", .want = "5" },
    .{ .sql = "LENGTH(1e15)", .want = "4" },
    .{ .sql = "REPLACE(1e20, 'e', 'E')", .want = "1E20" },
    .{ .sql = "UPPER(1e20)", .want = "1E20" },
    .{ .sql = "LPAD(1.5e0, 5, '0')", .want = "001.5" },
    .{ .sql = "CONCAT(CAST(1.1 AS FLOAT), '')", .want = "1.1" },
    .{ .sql = "CONCAT(CAST(3.4e38 AS FLOAT), '')", .want = "3.4e38" },
    .{ .sql = "CONCAT(CAST(1e-10 AS FLOAT), '')", .want = "0.0000000001" },
    .{ .sql = "CONCAT(CAST(123.456 AS FLOAT), '')", .want = "123.456" },
    .{ .sql = "CAST(JSON_ARRAY(1e20, 100e0, 0e0) AS CHAR)", .want = "[1e20, 100.0, 0.0]" },
};

/// A double or decimal meets an integer parameter as MySQL reads it: a
/// double rounds half to even, a decimal half away from zero, and text reads
/// its leading integer. A boolean widens to a double for text arithmetic.
const integer_arg_cases = [_]Case{
    .{ .sql = "ELT(1.5e0, 'x', 'y', 'z')", .want = "y" },
    .{ .sql = "ELT(2.5e0, 'x', 'y', 'z')", .want = "y" },
    .{ .sql = "ELT(1.5, 'x', 'y', 'z')", .want = "y" },
    .{ .sql = "ELT('1.5', 'x', 'y', 'z')", .want = "x" },
    .{ .sql = "ELT('2.9', 'x', 'y', 'z')", .want = "y" },
    .{ .sql = "ELT('2e0', 'x', 'y', 'z')", .want = "y" },
    .{ .sql = "ELT('abc', 'x', 'y', 'z')", .want = null },
    .{ .sql = "ELT(-1.5e0, 'x')", .want = null },
    .{ .sql = "ELT(CAST(NULL AS DECIMAL(5,2)), 'a')", .want = null },
    .{ .sql = "REPEAT('a', 2.5e0)", .want = "aa" },
    .{ .sql = "REPEAT('a', 3.5e0)", .want = "aaaa" },
    .{ .sql = "REPEAT('a', 2.5)", .want = "aaa" },
    .{ .sql = "REPEAT('a', 1.49)", .want = "a" },
    .{ .sql = "REPEAT('ab', 1e0 + 1.4e0)", .want = "abab" },
    .{ .sql = "LEFT('abcdef', 2.7e0)", .want = "abc" },
    .{ .sql = "LEFT('abcdef', -0.5)", .want = "" },
    .{ .sql = "RIGHT('abcdef', 1.5e0)", .want = "ef" },
    .{ .sql = "SUBSTRING('abcdef', 1.5, 2.5)", .want = "bcd" },
    .{ .sql = "SUBSTRING('abcdef', -1.5)", .want = "ef" },
    .{ .sql = "MID('abcdef', 2.5e0, 2)", .want = "bc" },
    .{ .sql = "INSERT('abcdef', 2.5, 1.5, 'X')", .want = "abXef" },
    .{ .sql = "LPAD('a', 2.5e0, 'x')", .want = "xa" },
    .{ .sql = "LOCATE('b', 'abcabc', 2.5)", .want = "5" },
    .{ .sql = "CHAR_LENGTH(SPACE(1.5))", .want = "2" },
    .{ .sql = "INET_NTOA(2.5e0)", .want = "0.0.0.2" },
    .{ .sql = "TRUE + '1'", .want = "2" },
    .{ .sql = "TRUE + '1.5'", .want = "2.5" },
    .{ .sql = "FALSE - '2'", .want = "-2" },
    .{ .sql = "TRUE * 'abc'", .want = "0" },
    .{ .sql = "'1' + TRUE", .want = "2" },
    .{ .sql = "TRUE / '2'", .want = "0.5" },
    .{ .sql = "TRUE DIV '2'", .want = "0" },
    .{ .sql = "TRUE + 1e0", .want = "2" },
    .{ .sql = "CAST(TRUE + 2.5 AS CHAR)", .want = "3.5" },
    .{ .sql = "CASE WHEN 1 = 1 THEN TRUE ELSE 1.5e0 END", .want = "1" },
    .{ .sql = "GREATEST(TRUE, 0.5e0)", .want = "1" },
    .{ .sql = "COALESCE(NULL, TRUE, 2.5e0)", .want = "1" },
};

const json_quote_cases = [_]Case{
    .{ .sql = "JSON_QUOTE('a')", .want = "\"a\"" },
    .{ .sql = "JSON_QUOTE('a\"b')", .want = "\"a\\\"b\"" },
    .{ .sql = "JSON_QUOTE('a\\\\b')", .want = "\"a\\\\b\"" },
    .{ .sql = "JSON_QUOTE(NULL)", .want = null },
    .{ .sql = "JSON_QUOTE('')", .want = "\"\"" },
    .{ .sql = "JSON_QUOTE('line1\\nline2\\ttab')", .want = "\"line1\\nline2\\ttab\"" },
    .{ .sql = "JSON_QUOTE('é/ü')", .want = "\"é/ü\"" },
    .{ .sql = "JSON_QUOTE('[1, 2]')", .want = "\"[1, 2]\"" },
    .{ .sql = "JSON_QUOTE(JSON_QUOTE('a'))", .want = "\"\\\"a\\\"\"" },
    .{ .sql = "CHAR_LENGTH(JSON_QUOTE('ab'))", .want = "4" },
    .{ .sql = "JSON_TYPE(JSON_QUOTE('a'))", .want = "STRING" },
    .{ .sql = "JSON_VALID(JSON_QUOTE('a\"b'))", .want = "1" },
    .{ .sql = "CAST(JSON_EXTRACT(JSON_QUOTE('a'), '$') AS CHAR)", .want = "\"a\"" },
    .{ .sql = "JSON_UNQUOTE(JSON_QUOTE('a\"b'))", .want = "a\"b" },
    .{ .sql = "CHARSET(JSON_QUOTE('a'))", .want = "utf8mb4" },
};

/// COLLATION names its argument type's collation as CHARSET names its
/// character set; a bare NULL has neither, and a system function's text is
/// utf8mb3. BENCHMARK is 0, or NULL for a NULL or negative count.
/// A number, boolean or date is its text where a string is expected: LIKE,
/// one-argument CONCAT, a boolean as 1 or 0. A string result past the
/// max_allowed_packet thinDB reports (16 MiB) is NULL, a huge double count
/// included. Then MAKE_SET, CURRENT_ROLE, UUID_SHORT and `@@name` inside an
/// expression.
const text_leftover_cases = [_]Case{
    .{ .sql = "12 LIKE '1%'", .want = "1" },
    .{ .sql = "12 NOT LIKE '1%'", .want = "0" },
    .{ .sql = "1.5e0 LIKE '1.5'", .want = "1" },
    .{ .sql = "1e100 LIKE '1e1%'", .want = "1" },
    .{ .sql = "2.50 LIKE '2.5_'", .want = "1" },
    .{ .sql = "(1 = 1) LIKE '1'", .want = "1" },
    .{ .sql = "DATE '2020-01-02' LIKE '2020%'", .want = "1" },
    .{ .sql = "CONCAT(2)", .want = "2" },
    .{ .sql = "CONCAT(1.5e0)", .want = "1.5" },
    .{ .sql = "CONCAT('a')", .want = "a" },
    .{ .sql = "CONCAT(NULL)", .want = null },
    .{ .sql = "CAST(TRUE AS CHAR)", .want = "1" },
    .{ .sql = "CAST(FALSE AS CHAR)", .want = "0" },
    .{ .sql = "CONCAT(TRUE, 'x')", .want = "1x" },
    .{ .sql = "CONCAT(1 = 1, '')", .want = "1" },
    .{ .sql = "LENGTH(TRUE)", .want = "1" },
    .{ .sql = "REPLACE(TRUE, '1', 'y')", .want = "y" },
    .{ .sql = "REPEAT('b', 9223372036854775807) IS NULL", .want = "1" },
    .{ .sql = "LENGTH(REPEAT('b', 9223372036854775807))", .want = null },
    .{ .sql = "REPEAT('b', 3.4e38) IS NULL", .want = "1" },
    .{ .sql = "LENGTH(REPEAT('ab', 8388608))", .want = "16777216" },
    .{ .sql = "REPEAT('ab', 8388609) IS NULL", .want = "1" },
    .{ .sql = "LPAD('a', 16777217, 'x') IS NULL", .want = "1" },
    .{ .sql = "RPAD('a', 1e300, 'xy') IS NULL", .want = "1" },
    .{ .sql = "LENGTH(RPAD('a', 16777216, 'xy'))", .want = "16777216" },
    .{ .sql = "SPACE(16777217) IS NULL", .want = "1" },
    .{ .sql = "LENGTH(SPACE(16777216))", .want = "16777216" },
    .{ .sql = "LPAD('a', 3, 'xy')", .want = "xya" },
    .{ .sql = "RPAD('abc', 2, 'x')", .want = "ab" },
    .{ .sql = "REPEAT(NULL, 2)", .want = null },
    .{ .sql = "LPAD('a', NULL, 'x')", .want = null },
    .{ .sql = "SPACE(NULL)", .want = null },
    .{ .sql = "MAKE_SET(3, 'a', 'b')", .want = "a,b" },
    .{ .sql = "MAKE_SET('3.9', 'a', 'b', 'c')", .want = "a,b" },
    .{ .sql = "MAKE_SET(3.9, 'a', 'b', 'c')", .want = "c" },
    .{ .sql = "MAKE_SET(5, 'a', NULL, 'c')", .want = "a,c" },
    .{ .sql = "MAKE_SET(3, '', 'b')", .want = ",b" },
    .{ .sql = "MAKE_SET(NULL, 'a')", .want = null },
    .{ .sql = "MAKE_SET(0, 'a')", .want = "" },
    .{ .sql = "MAKE_SET(-1, 'a', 'b')", .want = "a,b" },
    .{ .sql = "CURRENT_ROLE()", .want = "NONE" },
    .{ .sql = "CHARSET(CURRENT_ROLE())", .want = "utf8mb3" },
    .{ .sql = "UUID_SHORT() > 0", .want = "1" },
    .{ .sql = "1 + @@auto_increment_increment", .want = "2" },
    .{ .sql = "@@SESSION.auto_increment_increment * 3", .want = "3" },
    .{ .sql = "CHARSET(@@version)", .want = "utf8mb3" },
    .{ .sql = "CHARSET(@@auto_increment_increment)", .want = "binary" },
    .{ .sql = "@@version_comment IS NOT NULL", .want = "1" },
};

const type_name_cases = [_]Case{
    .{ .sql = "COLLATION(1)", .want = "binary" },
    .{ .sql = "COLLATION(NULL)", .want = "binary" },
    .{ .sql = "COLLATION((NULL))", .want = "binary" },
    .{ .sql = "COLLATION(1.5)", .want = "binary" },
    .{ .sql = "COLLATION(1.5e0)", .want = "binary" },
    .{ .sql = "COLLATION(TRUE)", .want = "binary" },
    .{ .sql = "COLLATION(DATE '2024-01-02')", .want = "binary" },
    .{ .sql = "COLLATION(NOW())", .want = "binary" },
    .{ .sql = "COLLATION(JSON_ARRAY(1))", .want = "utf8mb4_bin" },
    .{ .sql = "COLLATION(UUID())", .want = "utf8mb3_general_ci" },
    .{ .sql = "COLLATION(VERSION())", .want = "utf8mb3_general_ci" },
    .{ .sql = "COLLATION(CHARSET(1))", .want = "utf8mb3_general_ci" },
    .{ .sql = "COLLATION(COLLATION(1))", .want = "utf8mb3_general_ci" },
    .{ .sql = "CHARSET(NULL)", .want = "binary" },
    .{ .sql = "CHARSET((NULL))", .want = "binary" },
    .{ .sql = "CHARSET(NULL + 1)", .want = "binary" },
    .{ .sql = "CHARSET(UUID())", .want = "utf8mb3" },
    .{ .sql = "CHARSET(VERSION())", .want = "utf8mb3" },
    .{ .sql = "CHARSET(CHARSET(1))", .want = "utf8mb3" },
    .{ .sql = "CHARSET(COLLATION(1))", .want = "utf8mb3" },
    .{ .sql = "CHARSET(IFNULL(NULL, 'a'))", .want = "utf8mb4" },
    .{ .sql = "BENCHMARK(1, 1)", .want = "0" },
    .{ .sql = "BENCHMARK(0, 1)", .want = "0" },
    .{ .sql = "BENCHMARK(3, 'a')", .want = "0" },
    .{ .sql = "BENCHMARK(2, NULL)", .want = "0" },
    .{ .sql = "BENCHMARK(NULL, 1)", .want = null },
    .{ .sql = "BENCHMARK(-1, 1)", .want = null },
    .{ .sql = "BENCHMARK(1.5, 1)", .want = "0" },
    .{ .sql = "BENCHMARK(-0.4, 1)", .want = "0" },
    .{ .sql = "BENCHMARK(-1.5e0, 1)", .want = null },
    .{ .sql = "BENCHMARK('3', 1)", .want = "0" },
    .{ .sql = "BENCHMARK('-2', 1)", .want = null },
    .{ .sql = "BENCHMARK(1, 1) + 1", .want = "1" },
};

const error_cases = [_][]const u8{
    "SELECT SLEEP(NULL)",
    "SELECT SLEEP(-1)",
    "SELECT REGEXP_INSTR('abc', 'c', 4)",
    "SELECT REGEXP_INSTR('abc', 'c', 0)",
    "SELECT REGEXP_INSTR('abc', 'c', 1, 1, 2)",
    "SELECT REGEXP_INSTR('ABC', 'b', 1, 1, 0, 'x')",
    "SELECT REGEXP_LIKE('abc', 'b', 'q')",
    "SELECT REGEXP_REPLACE('abc', 'b', 'X', 5)",
    "SELECT REGEXP_REPLACE('abc', 'b', 'X', 0)",
    "SELECT REGEXP_SUBSTR('abc', 'b', 5)",
    "SELECT JSON_OBJECT(NULL, 1)",
    "SELECT FOUND_ROWS()",
};

fn renderCell(allocator: std.mem.Allocator, col: thindb.storage.ColumnView, row: usize) !?[]u8 {
    if (!col.isValid(row)) return null;
    return switch (col.data) {
        .boolean => |s| try allocator.dupe(u8, if (s[row] != 0) "1" else "0"),
        inline .tinyint, .smallint, .int, .bigint, .largeint, .float, .double => |s| try std.fmt.allocPrint(allocator, "{d}", .{s[row]}),
        .varchar, .string, .char => |sv| try allocator.dupe(u8, sv.rowBytes(row)),
        else => error.UnexpectedResultType,
    };
}

/// Every row of column `column` in `sql`'s result, rendered as MySQL text.
fn collectCells(allocator: std.mem.Allocator, db: *thindb.Database, sql: []const u8, column: usize) ![]?[]u8 {
    var q = try helpers.runSqlMysql(allocator, db, sql);
    defer q.deinit();
    var out: std.ArrayList(?[]u8) = .empty;
    errdefer {
        for (out.items) |v| if (v) |x| allocator.free(x);
        out.deinit(allocator);
    }
    while (try q.next()) |batch| {
        for (0..batch.row_count) |row| {
            const cell = try renderCell(allocator, batch.values[column], row);
            errdefer if (cell) |x| allocator.free(x);
            try out.append(allocator, cell);
        }
    }
    return out.toOwnedSlice(allocator);
}

fn expectCells(allocator: std.mem.Allocator, db: *thindb.Database, sql: []const u8, column: usize, want: []const ?[]const u8) !void {
    errdefer std.debug.print("query: {s}\n", .{sql});
    const got = try collectCells(allocator, db, sql, column);
    defer helpers.freeStrings(allocator, got);
    try std.testing.expectEqual(want.len, got.len);
    for (want, got) |w, g| {
        if (w) |text| {
            try std.testing.expect(g != null);
            try std.testing.expectEqualStrings(text, g.?);
        } else try std.testing.expect(g == null);
    }
}

fn expectFailure(allocator: std.mem.Allocator, db: *thindb.Database, sql: []const u8) !void {
    const got = collectCells(allocator, db, sql, 0) catch return;
    helpers.freeStrings(allocator, got);
    std.debug.print("query succeeded but MySQL rejects it: {s}\n", .{sql});
    return error.TestUnexpectedSuccess;
}

fn openDb(allocator: std.mem.Allocator, dir: std.Io.Dir) !*thindb.Database {
    return thindb.Database.open(allocator, std.testing.io, dir, .{});
}

test "MySQL misc functions: scalar values match MySQL 8.4" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try openDb(allocator, tmp.dir);
    defer db.close();

    var sql_buf: std.ArrayList(u8) = .empty;
    defer sql_buf.deinit(allocator);
    for (scalar_cases) |c| {
        sql_buf.clearRetainingCapacity();
        try sql_buf.print(allocator, "SELECT {s}", .{c.sql});
        try expectCells(allocator, db, sql_buf.items, 0, &.{c.want});
    }
    for (json_cases) |c| {
        sql_buf.clearRetainingCapacity();
        try sql_buf.print(allocator, "SELECT CAST({s} AS CHAR)", .{c.sql});
        try expectCells(allocator, db, sql_buf.items, 0, &.{c.want});
    }
}

test "MySQL misc functions: implicit conversions, HEX, regex anchors and CHARSET match MySQL 8.4" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try openDb(allocator, tmp.dir);
    defer db.close();

    var sql_buf: std.ArrayList(u8) = .empty;
    defer sql_buf.deinit(allocator);
    inline for (.{ text_number_cases, hex_cases, regex_anchor_cases, json_text_cases, charset_cases }) |cases| {
        for (cases) |c| {
            sql_buf.clearRetainingCapacity();
            try sql_buf.print(allocator, "SELECT {s}", .{c.sql});
            try expectCells(allocator, db, sql_buf.items, 0, &.{c.want});
        }
    }
}

test "MySQL misc functions: doubles as text, integer arguments, JSON_QUOTE, COLLATION and BENCHMARK match MySQL 8.4" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try openDb(allocator, tmp.dir);
    defer db.close();

    var sql_buf: std.ArrayList(u8) = .empty;
    defer sql_buf.deinit(allocator);
    inline for (.{ double_text_cases, integer_arg_cases, json_quote_cases, type_name_cases, text_leftover_cases }) |cases| {
        for (cases) |c| {
            sql_buf.clearRetainingCapacity();
            try sql_buf.print(allocator, "SELECT {s}", .{c.sql});
            try expectCells(allocator, db, sql_buf.items, 0, &.{c.want});
        }
    }
    try expectCells(allocator, db, "SELECT COLLATION('a')", 0, &.{"utf8mb4_general_ci"});
    try expectCells(allocator, db, "SELECT GROUP_CONCAT(x ORDER BY x) FROM (SELECT 1e100 x UNION ALL SELECT 1.5e0 UNION ALL SELECT 1e15) t", 0, &.{"1.5,1e15,1e100"});
    try expectCells(allocator, db, "SELECT GROUP_CONCAT(REPEAT('a', x) ORDER BY x) FROM (SELECT 2.5 x UNION ALL SELECT 3.5 UNION ALL SELECT 1.49) t", 0, &.{"a,aaa,aaaa"});
    try expectCells(allocator, db, "SELECT GROUP_CONCAT(ELT(x, 'p', 'q', 'r') ORDER BY x) FROM (SELECT 1.5e0 x UNION ALL SELECT 0.4e0 UNION ALL SELECT 2.5e0) t", 0, &.{"q,q"});
}

test "MySQL misc functions: FLOAT, DOUBLE and DECIMAL columns as text and as integer arguments" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try openDb(allocator, tmp.dir);
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE fd (id BIGINT PRIMARY KEY, x FLOAT, y DOUBLE, d DECIMAL(10,2), s VARCHAR(40))");
    try helpers.exec(allocator, db,
        \\INSERT INTO fd (id, x, y, d, s) VALUES
        \\ (1, 0.1, 1e100, 2.5, 'a'), (2, 3.4e38, 1.5e-16, 1.49, 'b'), (3, 1234567, 1e15, -1.5, 'c'), (4, NULL, NULL, NULL, NULL)
    );

    try expectCells(allocator, db, "SELECT CONCAT(x, '|', y) FROM fd ORDER BY id", 0, &.{ "0.1|1e100", "3.4e38|1.5e-16", "1234567|1e15", null });
    try expectCells(allocator, db, "SELECT CAST(y AS CHAR) FROM fd ORDER BY id", 0, &.{ "1e100", "1.5e-16", "1e15", null });
    try expectCells(allocator, db, "SELECT GROUP_CONCAT(y ORDER BY id) FROM fd", 0, &.{"1e100,1.5e-16,1e15"});
    try expectCells(allocator, db, "SELECT JSON_QUOTE(CAST(x AS CHAR)) FROM fd ORDER BY id", 0, &.{ "\"0.1\"", "\"3.4e38\"", "\"1234567\"", null });
    try expectCells(allocator, db, "SELECT REPEAT('a', d) FROM fd ORDER BY id", 0, &.{ "aaa", "a", "", null });
    try expectCells(allocator, db, "SELECT ELT(d, 'p', 'q', 'r') FROM fd ORDER BY id", 0, &.{ "r", "p", null, null });
    try expectCells(allocator, db, "SELECT LEFT('abcdef', y) FROM fd ORDER BY id", 0, &.{ "abcdef", "", "abcdef", null });
    try expectCells(allocator, db, "SELECT BENCHMARK(d, y) FROM fd ORDER BY id", 0, &.{ "0", "0", null, null });
    try expectCells(allocator, db, "SELECT COLLATION(s) FROM fd ORDER BY id", 0, &.{ "utf8mb4_general_ci", "utf8mb4_general_ci", "utf8mb4_general_ci", "utf8mb4_general_ci" });
    try expectCells(allocator, db, "SELECT CHARSET(x) FROM fd WHERE id = 4", 0, &.{"binary"});

    try helpers.exec(allocator, db, "INSERT INTO fd (id, s) VALUES (5, 1e100), (6, 2.5e0)");
    try helpers.exec(allocator, db, "UPDATE fd SET s = y WHERE id = 3");
    try expectCells(allocator, db, "SELECT s FROM fd WHERE id >= 3 ORDER BY id", 0, &.{ "1e15", null, "1e100", "2.5" });
}

test "MySQL misc functions: text and JSON columns convert per row" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try openDb(allocator, tmp.dir);
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE cv (id BIGINT PRIMARY KEY, j JSON, s VARCHAR(20), n VARCHAR(10))");
    try helpers.exec(allocator, db,
        \\INSERT INTO cv (id, j, s, n) VALUES
        \\ (1, '{"a": "Xy", "b": 5}', 'abc', '3abc'), (2, '[1, 2]', 'de', '2.7'), (3, NULL, NULL, NULL), (4, '"q"', 'f', 'zz')
    );

    try expectCells(allocator, db, "SELECT REPEAT('a', n) FROM cv ORDER BY id", 0, &.{ "aaa", "aa", null, "" });
    try expectCells(allocator, db, "SELECT n + 0 FROM cv ORDER BY id", 0, &.{ "3", "2.7", null, "0" });
    try expectCells(allocator, db, "SELECT LEFT(s, n) FROM cv ORDER BY id", 0, &.{ "abc", "de", null, "" });
    try expectCells(allocator, db, "SELECT LEFT(s, id + 1) FROM cv ORDER BY id", 0, &.{ "ab", "de", null, "f" });
    try expectCells(allocator, db, "SELECT HEX(id * 100) FROM cv ORDER BY id", 0, &.{ "64", "C8", "12C", "190" });
    try expectCells(allocator, db, "SELECT LOWER(j) FROM cv ORDER BY id", 0, &.{ "{\"a\": \"xy\", \"b\": 5}", "[1, 2]", null, "\"q\"" });
    try expectCells(allocator, db, "SELECT LENGTH(j) FROM cv ORDER BY id", 0, &.{ "19", "6", null, "3" });
    try expectCells(allocator, db, "SELECT COALESCE(j, 'none') FROM cv ORDER BY id", 0, &.{ "{\"a\": \"Xy\", \"b\": 5}", "[1, 2]", "none", "\"q\"" });
    try expectCells(allocator, db, "SELECT CASE WHEN id = 1 THEN j ELSE s END FROM cv ORDER BY id", 0, &.{ "{\"a\": \"Xy\", \"b\": 5}", "de", null, "f" });
    try expectCells(allocator, db, "SELECT CAST(JSON_EXTRACT(j, '$.b') AS SIGNED) FROM cv ORDER BY id", 0, &.{ "5", null, null, null });
    try expectCells(allocator, db, "SELECT JSON_EXTRACT(j, '$.b') + 1 FROM cv ORDER BY id", 0, &.{ "6", null, null, null });
    try expectCells(allocator, db, "SELECT id FROM cv WHERE LOWER(j) LIKE '%xy%'", 0, &.{"1"});
    try expectCells(allocator, db, "SELECT CHARSET(j) FROM cv WHERE id = 3", 0, &.{"utf8mb4"});
}

test "MySQL misc functions: invalid arguments raise errors as in MySQL" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try openDb(allocator, tmp.dir);
    defer db.close();
    for (error_cases) |sql| try expectFailure(allocator, db, sql);
}

test "MySQL misc functions: SLEEP runs once per row and returns 0" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try openDb(allocator, tmp.dir);
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE s (id BIGINT PRIMARY KEY)");
    try helpers.exec(allocator, db, "INSERT INTO s (id) VALUES (1), (2), (3)");
    try expectCells(allocator, db, "SELECT SLEEP(0) FROM s ORDER BY id", 0, &.{ "0", "0", "0" });
}

test "MySQL misc functions: JSON_ARRAYAGG and JSON_OBJECTAGG" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try openDb(allocator, tmp.dir);
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE ja (id BIGINT PRIMARY KEY, g BIGINT, k VARCHAR(8), v BIGINT, d DECIMAL(6,2))");
    try helpers.exec(allocator, db,
        \\INSERT INTO ja (id, g, k, v, d) VALUES
        \\ (1, 1, 'x', 10, 1.50), (2, 1, 'y', NULL, 2.00), (3, 2, 'z', 30, NULL), (4, 1, 'x', 40, 0.25)
    );

    const grouped = "SELECT g, CAST(JSON_ARRAYAGG(v) AS CHAR) a, CAST(JSON_OBJECTAGG(k, v) AS CHAR) o FROM ja GROUP BY g ORDER BY g";
    try expectCells(allocator, db, grouped, 1, &.{ "[10, null, 40]", "[30]" });
    try expectCells(allocator, db, grouped, 2, &.{ "{\"x\": 40, \"y\": null}", "{\"z\": 30}" });
    try expectCells(allocator, db, "SELECT CAST(JSON_ARRAYAGG(d) AS CHAR) FROM ja", 0, &.{"[1.50, 2.00, null, 0.25]"});
    try expectCells(allocator, db, "SELECT CAST(JSON_ARRAYAGG(JSON_OBJECT('a', v)) AS CHAR) FROM ja WHERE id < 3", 0, &.{"[{\"a\": 10}, {\"a\": null}]"});
    try expectCells(allocator, db, "SELECT CAST(JSON_ARRAYAGG(v) AS CHAR) FROM ja WHERE id > 10", 0, &.{null});
    try expectCells(allocator, db, "SELECT CAST(JSON_OBJECTAGG(k, v) AS CHAR) FROM ja WHERE id > 10", 0, &.{null});
    try expectCells(allocator, db, "SELECT g FROM ja GROUP BY g HAVING JSON_LENGTH(JSON_ARRAYAGG(v)) > 1", 0, &.{"1"});
    try expectFailure(allocator, db, "SELECT JSON_OBJECTAGG(CASE WHEN id = 3 THEN NULL ELSE k END, v) FROM ja");

    var q = try helpers.runSqlMysql(allocator, db, "SELECT JSON_ARRAYAGG(v), JSON_OBJECTAGG(k, v) FROM ja");
    defer q.deinit();
    const schema = q.outputSchema();
    try std.testing.expectEqualStrings("JSON_ARRAYAGG(v)", schema[0].name);
    try std.testing.expectEqualStrings("JSON_OBJECTAGG(k, v)", schema[1].name);
    while (try q.next()) |_| {}
}

fn expectSessionCells(allocator: std.mem.Allocator, db: *thindb.Database, session: thindb.Session, sql: []const u8, want: []const ?[]const u8) !void {
    errdefer std.debug.print("query: {s}\n", .{sql});
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const root = try thindb.sql.parseDialect(arena.allocator(), sql, .mysql);
    var cq = try thindb.net.compileWithSession(allocator, db, session, root);
    defer cq.deinit();
    var got: usize = 0;
    while (try cq.next()) |batch| {
        for (0..batch.row_count) |row| {
            try std.testing.expect(got < want.len);
            const cell = try renderCell(allocator, batch.values[0], row);
            defer if (cell) |x| allocator.free(x);
            if (want[got]) |text| {
                try std.testing.expect(cell != null);
                try std.testing.expectEqualStrings(text, cell.?);
            } else try std.testing.expect(cell == null);
            got += 1;
        }
    }
    try std.testing.expectEqual(want.len, got);
}

test "MySQL misc functions: LAST_INSERT_ID and ROW_COUNT read the session" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try openDb(allocator, tmp.dir);
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE ai (id BIGINT PRIMARY KEY AUTO_INCREMENT, s VARCHAR(10))");

    try expectSessionCells(allocator, db, .{}, "SELECT LAST_INSERT_ID()", &.{"0"});
    try expectSessionCells(allocator, db, .{}, "SELECT ROW_COUNT()", &.{"-1"});
    try expectSessionCells(allocator, db, .{ .last_insert_id = 2, .row_count = 3 }, "SELECT LAST_INSERT_ID()", &.{"2"});
    try expectSessionCells(allocator, db, .{ .last_insert_id = 2, .row_count = 3 }, "SELECT ROW_COUNT()", &.{"3"});

    {
        var q = try helpers.runSqlMysql(allocator, db, "INSERT INTO ai (s) VALUES ('a'), ('b'), ('c')");
        defer q.deinit();
        while (try q.next()) |_| {}
        try std.testing.expectEqual(@as(?u64, 1), q.cq.lastInsertId());
        try std.testing.expectEqual(@as(u64, 3), q.affectedRows());
    }
    {
        var q = try helpers.runSqlMysql(allocator, db, "INSERT INTO ai (id, s) VALUES (100, 'x')");
        defer q.deinit();
        while (try q.next()) |_| {}
        try std.testing.expectEqual(@as(?u64, null), q.cq.lastInsertId());
    }
    {
        var q = try helpers.runSqlMysql(allocator, db, "INSERT INTO ai (s) VALUES ('d')");
        defer q.deinit();
        while (try q.next()) |_| {}
        try std.testing.expectEqual(@as(?u64, 101), q.cq.lastInsertId());
    }
    try expectSessionCells(allocator, db, .{ .last_insert_id = 2 }, "SELECT s FROM ai WHERE id = LAST_INSERT_ID()", &.{"b"});
}

test "MySQL misc functions: information functions read the session" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try openDb(allocator, tmp.dir);
    defer db.close();

    const session: thindb.Session = .{
        .dialect = .mysql,
        .current_schema = "sales",
        .connection_id = 42,
        .user = "app@localhost",
        .server_version = "8.0.32-thinDB",
    };
    const cases = [_]Case{
        .{ .sql = "SELECT CONNECTION_ID() AS id, 7 AS n", .want = "42" },
        .{ .sql = "SELECT CONNECTION_ID() + 1", .want = "43" },
        .{ .sql = "SELECT USER() u", .want = "app@localhost" },
        .{ .sql = "SELECT CURRENT_USER() AS u, 1", .want = "app@localhost" },
        .{ .sql = "SELECT SESSION_USER()", .want = "app@localhost" },
        .{ .sql = "SELECT SYSTEM_USER()", .want = "app@localhost" },
        .{ .sql = "SELECT CONCAT(VERSION(), '!') v", .want = "8.0.32-thinDB!" },
        .{ .sql = "SELECT DATABASE() AS db, 2", .want = "sales" },
        .{ .sql = "SELECT UPPER(SCHEMA())", .want = "SALES" },
        .{ .sql = "SELECT 'hit' WHERE CONNECTION_ID() = 42 AND DATABASE() = 'sales'", .want = "hit" },
        .{ .sql = "SELECT CHARSET(DATABASE())", .want = "utf8mb3" },
        .{ .sql = "SELECT COLLATION(USER())", .want = "utf8mb3_general_ci" },
    };
    for (cases) |c| try expectSessionCells(allocator, db, session, c.sql, &.{c.want});

    var no_schema = session;
    no_schema.current_schema = "";
    try expectSessionCells(allocator, db, no_schema, "SELECT DATABASE()", &.{null});

    try expectFailure(allocator, db, "SELECT CONNECTION_ID()");
    try expectFailure(allocator, db, "SELECT USER()");
}

test "MySQL misc functions: a variadic call takes any number of arguments (issue #322)" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try openDb(allocator, tmp.dir);
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, s VARCHAR(10))");
    try helpers.exec(allocator, db, "INSERT INTO t VALUES (1, 'a'), (2, NULL)");

    const letters = "abcdefghijklmnopqrstuvw";
    var args: std.ArrayList(u8) = .empty;
    defer args.deinit(allocator);
    for (letters, 0..) |ch, i| {
        if (i > 0) try args.appendSlice(allocator, ", ");
        try args.print(allocator, "'{c}'", .{ch});
    }
    var nulls: std.ArrayList(u8) = .empty;
    defer nulls.deinit(allocator);
    for (0..40) |_| try nulls.appendSlice(allocator, "NULL, ");
    var sql: std.ArrayList(u8) = .empty;
    defer sql.deinit(allocator);

    sql.clearRetainingCapacity();
    try sql.print(allocator, "SELECT CONCAT({s})", .{args.items});
    try expectCells(allocator, db, sql.items, 0, &.{letters});
    sql.clearRetainingCapacity();
    try sql.print(allocator, "SELECT CONCAT_WS('', {s}, {s})", .{ args.items, args.items });
    try expectCells(allocator, db, sql.items, 0, &.{letters ++ letters});
    sql.clearRetainingCapacity();
    try sql.print(allocator, "SELECT ELT(23, {s})", .{args.items});
    try expectCells(allocator, db, sql.items, 0, &.{"w"});
    sql.clearRetainingCapacity();
    try sql.print(allocator, "SELECT FIELD('w', {s})", .{args.items});
    try expectCells(allocator, db, sql.items, 0, &.{"23"});
    sql.clearRetainingCapacity();
    try sql.print(allocator, "SELECT COALESCE({s}'x')", .{nulls.items});
    try expectCells(allocator, db, sql.items, 0, &.{"x"});
    sql.clearRetainingCapacity();
    try sql.print(allocator, "SELECT COALESCE({s}s, 'none') FROM t ORDER BY id", .{nulls.items});
    try expectCells(allocator, db, sql.items, 0, &.{ "a", "none" });
    sql.clearRetainingCapacity();
    try sql.print(allocator, "SELECT CONCAT(s, {s}) FROM t ORDER BY id", .{args.items});
    try expectCells(allocator, db, sql.items, 0, &.{ "a" ++ letters, null });
}

test "MySQL misc functions: LIKE reads a number, boolean or date column as its text" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try openDb(allocator, tmp.dir);
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE lk (id BIGINT PRIMARY KEY, n INT, d DOUBLE, m DECIMAL(10,2), b BOOLEAN, dt DATE)");
    try helpers.exec(allocator, db, "INSERT INTO lk VALUES (1, 12, 1.5, 2.5, TRUE, '2020-01-02'), (2, 21, 1e100, -3, FALSE, '2021-05-06'), (3, NULL, NULL, NULL, NULL, NULL)");

    for (0..2) |pass| {
        if (pass == 1) try (try db.openTable("lk", .{})).flush();
        try expectCells(allocator, db, "SELECT id FROM lk WHERE n LIKE '1%'", 0, &.{"1"});
        try expectCells(allocator, db, "SELECT id FROM lk WHERE n NOT LIKE '1%'", 0, &.{"2"});
        try expectCells(allocator, db, "SELECT id FROM lk WHERE d LIKE '1e%'", 0, &.{"2"});
        try expectCells(allocator, db, "SELECT id FROM lk WHERE m LIKE '%.50'", 0, &.{"1"});
        try expectCells(allocator, db, "SELECT id FROM lk WHERE b LIKE '0'", 0, &.{"2"});
        try expectCells(allocator, db, "SELECT id FROM lk WHERE dt LIKE '2021%' AND n > 0", 0, &.{"2"});
        try expectCells(allocator, db, "SELECT n LIKE '%1' FROM lk ORDER BY id", 0, &.{ "0", "1", null });
        try expectCells(allocator, db, "SELECT id FROM lk WHERE @@auto_increment_increment = 1 AND n > @@auto_increment_increment * 20", 0, &.{"2"});
    }
}
