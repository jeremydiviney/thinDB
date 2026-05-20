//! ClickBench `hits` table schema, mapped to thinDB types.
//!
//! Source: ClickHouse-flavored CREATE TABLE from
//! https://github.com/ClickHouse/ClickBench/blob/main/clickhouse/create.sql
//!
//! Type mapping decisions:
//!   ClickBench TEXT          → thinDB .string   (unbounded utf-8)
//!   ClickBench VARCHAR(N)    → thinDB .varchar(N)
//!   ClickBench INTEGER       → thinDB .int      (i32)
//!   ClickBench BIGINT        → thinDB .bigint   (i64)
//!   ClickBench SMALLINT      → thinDB .smallint (i16)
//!   ClickBench DATE          → thinDB .date     (i32 days since epoch)
//!   ClickBench TIMESTAMP     → thinDB .datetime (i64 micros)
//!   ClickBench CHAR(1)       → thinDB .char(1)
//!
//! ClickBench's primary key is the multi-column
//! (CounterID, EventDate, UserID, EventTime, WatchID) — we use it
//! verbatim as the order key.

const std = @import("std");
const thindb = @import("thindb");

const types = thindb.types;
const Column = thindb.Column;

pub const columns = [_]Column{
    .{ .name = "WatchID", .type = .bigint },
    .{ .name = "JavaEnable", .type = .smallint },
    .{ .name = "Title", .type = .string },
    .{ .name = "GoodEvent", .type = .smallint },
    .{ .name = "EventTime", .type = .datetime },
    .{ .name = "EventDate", .type = .date },
    .{ .name = "CounterID", .type = .int },
    .{ .name = "ClientIP", .type = .int },
    .{ .name = "RegionID", .type = .int },
    .{ .name = "UserID", .type = .bigint },
    .{ .name = "CounterClass", .type = .smallint },
    .{ .name = "OS", .type = .smallint },
    .{ .name = "UserAgent", .type = .smallint },
    .{ .name = "URL", .type = .string },
    .{ .name = "Referer", .type = .string },
    .{ .name = "IsRefresh", .type = .smallint },
    .{ .name = "RefererCategoryID", .type = .smallint },
    .{ .name = "RefererRegionID", .type = .int },
    .{ .name = "URLCategoryID", .type = .smallint },
    .{ .name = "URLRegionID", .type = .int },
    .{ .name = "ResolutionWidth", .type = .smallint },
    .{ .name = "ResolutionHeight", .type = .smallint },
    .{ .name = "ResolutionDepth", .type = .smallint },
    .{ .name = "FlashMajor", .type = .smallint },
    .{ .name = "FlashMinor", .type = .smallint },
    .{ .name = "FlashMinor2", .type = .string },
    .{ .name = "NetMajor", .type = .smallint },
    .{ .name = "NetMinor", .type = .smallint },
    .{ .name = "UserAgentMajor", .type = .smallint },
    .{ .name = "UserAgentMinor", .type = .{ .varchar = 255 } },
    .{ .name = "CookieEnable", .type = .smallint },
    .{ .name = "JavascriptEnable", .type = .smallint },
    .{ .name = "IsMobile", .type = .smallint },
    .{ .name = "MobilePhone", .type = .smallint },
    .{ .name = "MobilePhoneModel", .type = .string },
    .{ .name = "Params", .type = .string },
    .{ .name = "IPNetworkID", .type = .int },
    .{ .name = "TraficSourceID", .type = .smallint },
    .{ .name = "SearchEngineID", .type = .smallint },
    .{ .name = "SearchPhrase", .type = .string },
    .{ .name = "AdvEngineID", .type = .smallint },
    .{ .name = "IsArtifical", .type = .smallint },
    .{ .name = "WindowClientWidth", .type = .smallint },
    .{ .name = "WindowClientHeight", .type = .smallint },
    .{ .name = "ClientTimeZone", .type = .smallint },
    .{ .name = "ClientEventTime", .type = .datetime },
    .{ .name = "SilverlightVersion1", .type = .smallint },
    .{ .name = "SilverlightVersion2", .type = .smallint },
    .{ .name = "SilverlightVersion3", .type = .int },
    .{ .name = "SilverlightVersion4", .type = .smallint },
    .{ .name = "PageCharset", .type = .string },
    .{ .name = "CodeVersion", .type = .int },
    .{ .name = "IsLink", .type = .smallint },
    .{ .name = "IsDownload", .type = .smallint },
    .{ .name = "IsNotBounce", .type = .smallint },
    .{ .name = "FUniqID", .type = .bigint },
    .{ .name = "OriginalURL", .type = .string },
    .{ .name = "HID", .type = .int },
    .{ .name = "IsOldCounter", .type = .smallint },
    .{ .name = "IsEvent", .type = .smallint },
    .{ .name = "IsParameter", .type = .smallint },
    .{ .name = "DontCountHits", .type = .smallint },
    .{ .name = "WithHash", .type = .smallint },
    .{ .name = "HitColor", .type = .{ .char = 1 } },
    .{ .name = "LocalEventTime", .type = .datetime },
    .{ .name = "Age", .type = .smallint },
    .{ .name = "Sex", .type = .smallint },
    .{ .name = "Income", .type = .smallint },
    .{ .name = "Interests", .type = .smallint },
    .{ .name = "Robotness", .type = .smallint },
    .{ .name = "RemoteIP", .type = .int },
    .{ .name = "WindowName", .type = .int },
    .{ .name = "OpenerName", .type = .int },
    .{ .name = "HistoryLength", .type = .smallint },
    .{ .name = "BrowserLanguage", .type = .string },
    .{ .name = "BrowserCountry", .type = .string },
    .{ .name = "SocialNetwork", .type = .string },
    .{ .name = "SocialAction", .type = .string },
    .{ .name = "HTTPError", .type = .smallint },
    .{ .name = "SendTiming", .type = .int },
    .{ .name = "DNSTiming", .type = .int },
    .{ .name = "ConnectTiming", .type = .int },
    .{ .name = "ResponseStartTiming", .type = .int },
    .{ .name = "ResponseEndTiming", .type = .int },
    .{ .name = "FetchTiming", .type = .int },
    .{ .name = "SocialSourceNetworkID", .type = .smallint },
    .{ .name = "SocialSourcePage", .type = .string },
    .{ .name = "ParamPrice", .type = .bigint },
    .{ .name = "ParamOrderID", .type = .string },
    .{ .name = "ParamCurrency", .type = .string },
    .{ .name = "ParamCurrencyID", .type = .smallint },
    .{ .name = "OpenstatServiceName", .type = .string },
    .{ .name = "OpenstatCampaignID", .type = .string },
    .{ .name = "OpenstatAdID", .type = .string },
    .{ .name = "OpenstatSourceID", .type = .string },
    .{ .name = "UTMSource", .type = .string },
    .{ .name = "UTMMedium", .type = .string },
    .{ .name = "UTMCampaign", .type = .string },
    .{ .name = "UTMContent", .type = .string },
    .{ .name = "UTMTerm", .type = .string },
    .{ .name = "FromTag", .type = .string },
    .{ .name = "HasGCLID", .type = .smallint },
    .{ .name = "RefererHash", .type = .bigint },
    .{ .name = "URLHash", .type = .bigint },
    .{ .name = "CLID", .type = .int },
};

/// ClickBench's canonical order — multi-column PK on the hot
/// dimensions. thinDB uses this as the segment-level order key,
/// which lets predicates on these cols prune row groups via
/// min/max stats.
pub const order_key = [_][]const u8{
    "CounterID",
    "EventDate",
    "UserID",
    "EventTime",
    "WatchID",
};

pub const table_schema = thindb.TableSchema{
    .columns = &columns,
    .order_key = &order_key,
    .unique = false,
};

pub const table_options = thindb.TableOptions{
    .order_key = &order_key,
    .unique = false,
    .row_group_size = 65_536,
};
