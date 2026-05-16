//! Shared fixtures used by the integration tests. Keeps each test file
//! focused on its topic rather than re-declaring the same schema in every
//! file.

const thindb = @import("thindb");

pub const schema_v1 = thindb.Schema{
    .columns = &.{
        .{ .name = "id", .type = .bigint },
        .{ .name = "qty", .type = .int },
        .{ .name = "active", .type = .boolean },
        .{ .name = "tag", .type = .string },
    },
    .order_key = &.{"id"},
    .unique = true,
};

const order_key_v1 = [_][]const u8{"id"};
pub const opts_v1 = thindb.TableOptions{
    .order_key = &order_key_v1,
    .unique = true,
    .row_group_size = 4,
};
