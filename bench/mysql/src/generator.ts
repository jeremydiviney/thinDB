import type { ColumnConfig, GeneratorConfig } from "./config.ts";

export type BenchValue = string | number | boolean | Date;

const ALPHANUM = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
const WORDS = [
  "alpha",
  "bravo",
  "charlie",
  "delta",
  "echo",
  "foxtrot",
  "hotel",
  "india",
  "juliet",
  "kilo",
  "lima",
  "micro",
  "nova",
  "orbit",
  "pixel",
  "query",
  "river",
  "signal",
  "vector",
  "zenith",
];

export class Rng {
  private state: number;

  constructor(seed: number) {
    this.state = seed >>> 0;
  }

  next(): number {
    this.state = (this.state + 0x6d2b79f5) >>> 0;
    let t = this.state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  }

  int(min: number, max: number): number {
    return Math.floor(this.next() * (max - min + 1)) + min;
  }

  float(min: number, max: number): number {
    return this.next() * (max - min) + min;
  }

  pick<T>(values: readonly T[]): T {
    const value = values[this.int(0, values.length - 1)];
    if (value === undefined) throw new Error("cannot pick from an empty array");
    return value;
  }

  string(length: number): string {
    let out = "";
    for (let i = 0; i < length; i++) out += ALPHANUM[this.int(0, ALPHANUM.length - 1)];
    return out;
  }
}

export function generateRow(columns: ColumnConfig[], rowIndex: number, rng: Rng): BenchValue[] {
  return columns.map((column) => generateValue(column.generator, rowIndex, rng));
}

export function generateBatch(
  columns: ColumnConfig[],
  startRowIndex: number,
  rowCount: number,
  rng: Rng,
): BenchValue[] {
  const values: BenchValue[] = [];
  for (let i = 0; i < rowCount; i++) {
    values.push(...generateRow(columns, startRowIndex + i, rng));
  }
  return values;
}

function generateValue(generator: GeneratorConfig, rowIndex: number, rng: Rng): BenchValue {
  switch (generator.type) {
    case "sequence":
      return (generator.start ?? 1) + rowIndex * (generator.step ?? 1);
    case "int":
      return rng.int(generator.min, generator.max);
    case "float":
      return rng.float(generator.min, generator.max);
    case "string":
      return rng.string(generator.length);
    case "text":
      return randomText(rng, rng.int(generator.minLength, generator.maxLength));
    case "enum":
      return rng.pick(generator.values);
    case "bool":
      return rng.next() >= 0.5;
    case "datetime":
      return randomDate(generator.start, generator.end, rng);
    case "uuid":
      return randomUuid(rng);
  }
}

function randomText(rng: Rng, targetLength: number): string {
  let out = "";
  while (out.length < targetLength) {
    if (out.length > 0) out += " ";
    out += rng.pick(WORDS);
  }
  return out.slice(0, targetLength);
}

function randomDate(start: string | undefined, end: string | undefined, rng: Rng): Date {
  const startMs = start === undefined ? Date.parse("2020-01-01T00:00:00.000Z") : Date.parse(start);
  const endMs = end === undefined ? Date.parse("2030-01-01T00:00:00.000Z") : Date.parse(end);
  return new Date(Math.floor(rng.float(startMs, endMs)));
}

function randomUuid(rng: Rng): string {
  const bytes = Array.from({ length: 16 }, () => rng.int(0, 255));
  bytes[6] = (bytes[6]! & 0x0f) | 0x40;
  bytes[8] = (bytes[8]! & 0x3f) | 0x80;
  const hex = bytes.map((b) => b.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
