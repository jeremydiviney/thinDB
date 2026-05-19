import { describe } from "bun:test";
import { todo } from "../helpers/todo.ts";

// Two layers block table-shaped tests over the MySQL wire:
//   1. The SQL parser does not support CREATE TABLE / INSERT yet —
//      the only DDL it accepts is CREATE/DROP DATABASE | SCHEMA.
//      Any seeding step would therefore need to happen out-of-band.
//   2. Even SELECT result-sets are unreachable because mysql2's
//      result-set parser is incompatible with the server's
//      CLIENT_DEPRECATE_EOF-only output format. See mysql/basic.test.ts
//      for the deeper explanation.
//
// Once either side is fixed these will graduate to real tests.

describe("mysql table — todo (parser + wire-format gaps)", () => {
  todo("CREATE TABLE via SQL (parser does not support yet)");
  todo("INSERT INTO via SQL (parser does not support yet)");
  todo("SELECT round-trips inserted rows (blocked at both layers)");
});
