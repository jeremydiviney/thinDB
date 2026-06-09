//! Engine V2 pipeline composer.
//!
//! This module is the narrow dispatch surface between the IR shape matcher and
//! concrete physical pipelines. New query shapes should be added here as small
//! sibling builders, not as branches inside a single operator.

const std = @import("std");

const api = @import("../api/api.zig");
const exec = @import("exec.zig");
const v2_group_topn = @import("v2_shape_group_topn.zig");
const v2_lowcard_group = @import("v2_lowcard_group.zig");

pub const GroupTopNRequest = v2_group_topn.Request;

pub fn tryBuildGroupTopN(
    allocator: std.mem.Allocator,
    table: *api.Table,
    request: GroupTopNRequest,
) !?exec.Query {
    return v2_group_topn.tryBuild(allocator, table, request);
}

/// Direct (scatter-free) grouped aggregate for PROVABLY low-cardinality group
/// keys. Returns null when the cardinality bound is unknown/too high or the
/// shape isn't covered — the silo grid is the fallback.
pub fn tryBuildLowCardGroup(
    allocator: std.mem.Allocator,
    table: *api.Table,
    request: GroupTopNRequest,
) !?exec.Query {
    return v2_lowcard_group.tryBuild(allocator, table, request);
}

