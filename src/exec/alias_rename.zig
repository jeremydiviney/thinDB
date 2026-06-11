//! AliasRename — zero-copy column-name rewriter. Wraps an upstream
//! Query and reports its output schema with every column renamed to
//! `alias.col`. Data views pass through unchanged; only the Column
//! `.name` fields are rewritten in an owned schema slice.
//!
//! Used by `compileOp` to implement `FROM table AS alias` semantics so
//! that downstream operators (filter / project / join-on) can refer
//! to columns by their qualified `alias.col` names — required for
//! disambiguating self-joins.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Column = types.Column;

const exec = @import("exec.zig");
const Query = exec.Query;
const Batch = exec.Batch;
const makeQuery = exec.makeQuery;

const predicate = @import("predicate.zig");
const Predicate = predicate.Predicate;

pub const AliasRename = struct {
    allocator: Allocator,
    upstream: Query,
    output_schema: []Column,
    name_storage: []u8,
    /// A join probe fused below: upstream batches are already joined (join
    /// output schema, not this wrapper's), so next() passes them through.
    probe_fused: bool = false,

    pub fn create(allocator: Allocator, upstream: Query, alias: []const u8) !Query {
        const up_schema = upstream.outputSchema();

        var total_bytes: usize = 0;
        for (up_schema) |c| total_bytes += alias.len + 1 + c.name.len;
        const name_storage = try allocator.alloc(u8, total_bytes);
        errdefer allocator.free(name_storage);

        const out_schema = try allocator.alloc(Column, up_schema.len);
        errdefer allocator.free(out_schema);

        var off: usize = 0;
        for (up_schema, 0..) |c, i| {
            const start = off;
            @memcpy(name_storage[off..][0..alias.len], alias);
            off += alias.len;
            name_storage[off] = '.';
            off += 1;
            @memcpy(name_storage[off..][0..c.name.len], c.name);
            off += c.name.len;
            out_schema[i] = c;
            out_schema[i].name = name_storage[start..off];
        }

        const self = try allocator.create(AliasRename);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .upstream = upstream,
            .output_schema = out_schema,
            .name_storage = name_storage,
        };
        return makeQuery(allocator, self);
    }

    pub fn deinit(self: *AliasRename) void {
        var up = self.upstream;
        up.deinit();
        self.allocator.free(self.output_schema);
        self.allocator.free(self.name_storage);
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn outputSchema(self: *AliasRename) []const Column {
        return self.output_schema;
    }

    pub fn addPrune(self: *AliasRename, pred: Predicate) !void {
        // Translate the qualified name back to the upstream's bare
        // column name before forwarding. The predicate stays the
        // operator's IR object — we shallow-copy and adjust the col
        // pointer to view into our upstream's name space.
        const dot = std.mem.lastIndexOfScalar(u8, pred.col, '.') orelse {
            return self.upstream.addPrune(pred);
        };
        var rewritten = pred;
        rewritten.col = pred.col[dot + 1 ..];
        return self.upstream.addPrune(rewritten);
    }

    /// Forward fusion offers: the scan resolves qualified `alias.col`
    /// names against its bare schema via findColumn's tail matching, so
    /// the expression passes through unchanged. A fused upstream emits
    /// the same column order/types; only this wrapper's names differ.
    pub fn tryFuseFilter(self: *AliasRename, expr: predicate.PredicateExpr) !bool {
        return self.upstream.tryFuseFilter(expr);
    }

    pub fn tryFuseProbe(self: *AliasRename, sink: exec.ProbeSink) !bool {
        const ok = try self.upstream.tryFuseProbe(sink);
        if (ok) self.probe_fused = true;
        return ok;
    }

    pub fn stats(self: *AliasRename) exec.PipelineStats {
        return self.upstream.stats();
    }

    pub fn accountant(self: *AliasRename) ?*exec.memory.MemoryAccountant {
        return self.upstream.accountant();
    }

    pub fn explain(self: *AliasRename, out: *std.ArrayList(u8), allocator: std.mem.Allocator, depth: usize) !void {
        try exec.explainLine(out, allocator, depth, "AliasRename");
        try self.upstream.explain(out, allocator, depth + 1);
    }

    pub fn next(self: *AliasRename) !?Batch {
        const batch = (try self.upstream.next()) orelse return null;
        if (self.probe_fused) return batch;
        return Batch{
            .schema = self.output_schema,
            .values = batch.values,
            .row_count = batch.row_count,
        };
    }
};
