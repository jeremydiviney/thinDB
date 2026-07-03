//! thinDB — public library entry point.
//! Public names re-exported here form the v0.1 API surface.

const std = @import("std");

pub const version = "0.1.0-dev";

pub const types = @import("types.zig");

pub const memory = @import("memory.zig");
pub const MemoryAccountant = memory.MemoryAccountant;
pub const Type = types.Type;
pub const Value = types.Value;
pub const Column = types.Column;
pub const TableSchema = types.TableSchema;
pub const decimal = types.decimal;
pub const DecimalSpec = types.DecimalSpec;

pub const storage = @import("storage/storage.zig");
pub const engine = @import("engine/engine.zig");
pub const api = @import("api/api.zig");
pub const exec = @import("exec/exec.zig");
pub const regex = @import("util/regex.zig");
pub const udf = @import("udf.zig");

pub const Database = api.Database;
pub const Catalog = api.Catalog;
pub const TempNamespace = api.TempNamespace;
pub const Session = api.Session;
pub const SessionVars = api.SessionVars;
pub const Dialect = api.Dialect;
/// Namespace type in the Catalog → Database → Schema → Table hierarchy.
/// Distinct from the table column-schema type `thindb.TableSchema`.
pub const Schema = api.Schema;
/// Deprecated alias for `Schema`. Will be removed in a future release.
pub const DbSchema = api.Schema;
pub const Table = api.Table;
pub const Config = api.Config;
pub const FileScanAccess = api.FileScanAccess;
pub const SyncMode = api.SyncMode;
pub const TableOptions = api.TableOptions;
pub const OpenOptions = api.OpenOptions;
pub const Error = api.Error;
pub const AlterOp = api.AlterOp;
pub const UdfRegistry = api.UdfRegistry;
pub const ScalarUdf = api.ScalarUdf;
pub const AggregateUdf = api.AggregateUdf;
pub const UdfVolatility = api.UdfVolatility;
pub const UdfNullStrategy = api.UdfNullStrategy;
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

// Plan-tree builder for multi-source / multi-branched pipelines.
pub const plan = @import("api/plan.zig");
pub const PlanBuilder = plan.PlanBuilder;
pub const JoinSpec = plan.JoinSpec;

// SQL parser — produces ir.Op trees from SQL text.
pub const sql = @import("sql/sql.zig");

// ---------------------------------------------------------------------------
// Client/server surface (new, going forward — see DESIGN.md §16).
// `thindb.local(...)` returns a Connection that wraps an in-process server.
// Future TCP transport will use the same Connection type. The pre-existing
// Database / Table API above remains usable for tests + the server's own
// internals; new user code should go through a Connection.
// ---------------------------------------------------------------------------
pub const net = @import("net/local.zig");
pub const Connection = net.Connection;
pub const ClientQuery = net.ClientQuery;
pub const local = net.local;
pub const connect = net.connect;
pub const tcp_server = @import("net/tcp_server.zig");
pub const serveTcp = tcp_server.serveTcp;
pub const TcpServer = tcp_server.Server;
pub const ir = @import("ir/ir.zig");
pub const wire = @import("net/wire.zig");

pub const mysql = @import("net/mysql.zig");
pub const serveMysql = mysql.serveMysql;
pub const MysqlServer = mysql.Server;
pub const mysql_default_port: u16 = mysql.default_port;

pub const pg = @import("net/pg.zig");
pub const servePg = pg.servePg;
pub const PgServer = pg.Server;
pub const pg_default_port: u16 = pg.default_port;

pub const serveTcpCatalog = tcp_server.serveTcpCatalog;

pub const conn_limit = @import("net/conn_limit.zig");
pub const ConnectionLimiter = conn_limit.ConnectionLimiter;

pub const conn_registry = @import("net/conn_registry.zig");
pub const ConnectionRegistry = conn_registry.Registry;
pub const ConnectionState = conn_registry.ConnectionState;

/// Default TCP port for a thinDB server. Servers and clients can pick
/// any port, but examples / docs use this.
pub const default_port: u16 = wire.default_port;

test {
    // Pull in tests from sub-modules.
    std.testing.refAllDecls(@This());
    _ = types;
    _ = storage;
    _ = engine;
    _ = api;
    _ = exec;
    _ = sql;
    _ = @import("net/wire_format.zig");
    _ = @import("net/error_map.zig");
    _ = @import("net/sql_text.zig");
    _ = @import("net/random_seed.zig");
    _ = @import("util/snapshot.zig");
    _ = @import("util/hll.zig");
    _ = @import("util/affinity.zig");
    _ = @import("util/core_scheduler.zig");
    _ = @import("util/huge_page.zig");
    _ = @import("util/buffer_pool.zig");
    _ = @import("net/const_fold.zig");
    _ = @import("exec/affine_agg.zig");
    _ = @import("exec/mat_stage.zig");
    _ = @import("exec/parallel_scan.zig");
    _ = @import("exec/partitioned_aggregate.zig");
}
