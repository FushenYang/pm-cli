import { Effect, Console } from "effect";
import { describe, it } from "vitest";

describe("Hello World", () => {
  it("should log hello world", () => {
    // 简单的测试示例
    const program = Console.log("Hello, World!");
    // 在测试中，我们可以运行它，但由于是副作用，我们可能需要 mock
    // 这里只是一个占位符
  });
});
