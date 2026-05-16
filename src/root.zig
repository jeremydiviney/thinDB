//! thinDB — public library entry point.
//! Public names re-exported here form the v0.1 API surface.

const std = @import("std");

pub const version = "0.1.0-dev";

pub const types = @import("types.zig");
pub const Type = types.Type;
pub const Value = types.Value;
pub const Column = types.Column;
pub const Schema = types.Schema;
pub const decimal = types.decimal;
pub const DecimalSpec = types.DecimalSpec;

pub const storage = @import("storage/storage.zig");
pub const engine = @import("engine/engine.zig");
pub const api = @import("api/api.zig");
pub const exec = @import("exec/exec.zig");

pub const Database = api.Database;
pub const Table = api.Table;
pub const Config = api.Config;
pub const SyncMode = api.SyncMode;
pub const TableOptions = api.TableOptions;
pub const OpenOptions = api.OpenOptions;
pub const Error = api.Error;
pub const Query = exec.Query;
pub const Batch = exec.Batch;
pub const Predicate = exec.Predicate;
pub const PredicateExpr = exec.PredicateExpr;
pub const leafExpr = exec.leafExpr;
pub const isNullExpr = exec.isNullExpr;
pub const isNotNullExpr = exec.isNotNullExpr;
pub const AggFunc = exec.AggFunc;
pub const AggSpec = exec.AggSpec;
pub const SortSpec = exec.SortSpec;
pub const scan = exec.scan;

test {
    // Pull in tests from sub-modules.
    std.testing.refAllDecls(@This());
    _ = types;
    _ = storage;
    _ = engine;
    _ = api;
    _ = exec;
}
