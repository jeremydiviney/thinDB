import { describe } from "bun:test";
import { todo } from "../helpers/todo.ts";

// Type round-tripping requires INSERT support (parser gap) AND
// result-set delivery (wire-format gap with mysql2). Both are
// documented in mysql/basic.test.ts and mysql/table.test.ts.

describe("mysql types — todo", () => {
  todo("INT type round-trips");
  todo("BIGINT type round-trips");
  todo("VARCHAR type round-trips");
  todo("DATE type round-trips");
  todo("DATETIME type round-trips");
  todo("FLOAT type round-trips");
  todo("DOUBLE type round-trips");
});
