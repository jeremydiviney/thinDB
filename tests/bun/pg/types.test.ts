import { describe } from "bun:test";
import { todo } from "../helpers/todo.ts";

describe("pg types", () => {
  // Type round-tripping requires INSERT support via SQL. The parser
  // doesn't yet support INSERT, so we mark these as todo until it does.
  todo("INT type round-trips");
  todo("BIGINT type round-trips");
  todo("VARCHAR type round-trips");
  todo("DATE type round-trips");
  todo("DATETIME type round-trips");
  todo("FLOAT type round-trips");
  todo("DOUBLE type round-trips");
});
