import { test } from "bun:test";

/**
 * Bun's runtime accepts `test.todo("name")` with no body, but its
 * TypeScript types require a function argument. Passing a no-op
 * satisfies both: the runner still treats the test as TODO (it never
 * runs) and the type-checker is happy.
 */
export function todo(label: string): void {
  test.todo(label, () => undefined);
}
