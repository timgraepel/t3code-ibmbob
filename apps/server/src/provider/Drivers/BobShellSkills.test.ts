import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import { BobShellSettings } from "@t3tools/contracts";

import { discoverBobShellSkills } from "./BobShellSkills.ts";

const BOB_BUILT_IN_NAMES = ["code", "plan", "ask", "advanced"];

const makeWorkspace = Effect.fn("makeWorkspace")(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const tmp = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-bob-skills-" });
  return {
    cwd: path.join(tmp, "workspace"),
    bobHome: path.join(tmp, "bob-home"),
  };
});

const makeSettings = (homePath: string): Pick<BobShellSettings, "binaryPath" | "homePath"> => ({
  binaryPath: "bob",
  homePath,
});

const writeSkill = Effect.fn("writeSkill")(function* (
  directory: string,
  skillName: string,
  contents: string,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const skillDir = path.join(directory, skillName);
  yield* fileSystem.makeDirectory(skillDir, { recursive: true });
  yield* fileSystem.writeFileString(path.join(skillDir, "SKILL.md"), contents);
  return path.join(skillDir, "SKILL.md");
});

it.layer(NodeServices.layer)("discoverBobShellSkills", (it) => {
  it.effect("returns only built-ins when no skill directories exist", () =>
    Effect.gen(function* () {
      const { cwd, bobHome } = yield* makeWorkspace();
      const skills = yield* discoverBobShellSkills(makeSettings(bobHome), cwd);
      assert.deepEqual(
        skills.map((s) => s.name),
        BOB_BUILT_IN_NAMES,
      );
      for (const s of skills) {
        assert.isUndefined(s.scope);
        assert.isTrue(s.enabled);
      }
    }).pipe(Effect.scoped),
  );

  it.effect("discovers user skills from <bobHome>/skills", () =>
    Effect.gen(function* () {
      const { cwd, bobHome } = yield* makeWorkspace();
      yield* writeSkill(
        `${bobHome}/skills`,
        "my-tool",
        "---\nname: my-tool\ndescription: My custom tool\n---\n# My Tool\n",
      );
      const skills = yield* discoverBobShellSkills(makeSettings(bobHome), cwd);
      const myTool = skills.find((s) => s.name === "my-tool");
      assert.ok(myTool, "should discover user skill");
      assert.equal(myTool.scope, "user");
      assert.equal(myTool.description, "My custom tool");
      assert.isTrue(myTool.enabled);
    }).pipe(Effect.scoped),
  );

  it.effect("discovers project skills from <cwd>/.bob/skills", () =>
    Effect.gen(function* () {
      const { cwd, bobHome } = yield* makeWorkspace();
      yield* writeSkill(
        `${cwd}/.bob/skills`,
        "project-helper",
        "---\ndescription: Project-level helper\n---\n# Helper\n",
      );
      const skills = yield* discoverBobShellSkills(makeSettings(bobHome), cwd);
      const helper = skills.find((s) => s.name === "project-helper");
      assert.ok(helper, "should discover project skill");
      assert.equal(helper.scope, "project");
      assert.equal(helper.description, "Project-level helper");
    }).pipe(Effect.scoped),
  );

  it.effect("user skill with same name as project skill wins (user first)", () =>
    Effect.gen(function* () {
      const { cwd, bobHome } = yield* makeWorkspace();
      yield* writeSkill(
        `${bobHome}/skills`,
        "shared",
        "---\ndescription: User version\n---\n",
      );
      yield* writeSkill(
        `${cwd}/.bob/skills`,
        "shared",
        "---\ndescription: Project version\n---\n",
      );
      const skills = yield* discoverBobShellSkills(makeSettings(bobHome), cwd);
      const shared = skills.filter((s) => s.name === "shared");
      assert.equal(shared.length, 1, "should not duplicate the skill");
      assert.equal(shared[0]!.scope, "user");
      assert.equal(shared[0]!.description, "User version");
    }).pipe(Effect.scoped),
  );

  it.effect("discovered skill named like a built-in suppresses the built-in", () =>
    Effect.gen(function* () {
      const { cwd, bobHome } = yield* makeWorkspace();
      yield* writeSkill(
        `${bobHome}/skills`,
        "code",
        "---\ndescription: My custom code mode\n---\n",
      );
      const skills = yield* discoverBobShellSkills(makeSettings(bobHome), cwd);
      const codeSkills = skills.filter((s) => s.name === "code");
      assert.equal(codeSkills.length, 1, "code should appear only once");
      assert.equal(codeSkills[0]!.scope, "user");
      assert.equal(codeSkills[0]!.description, "My custom code mode");
      for (const builtIn of ["plan", "ask", "advanced"]) {
        assert.ok(skills.some((s) => s.name === builtIn), `${builtIn} built-in should still be present`);
      }
    }).pipe(Effect.scoped),
  );

  it.effect("skips entries with malformed frontmatter", () =>
    Effect.gen(function* () {
      const { cwd, bobHome } = yield* makeWorkspace();
      yield* writeSkill(
        `${bobHome}/skills`,
        "bad-skill",
        "---\n: this is malformed: yaml: [\n---\n# Bad\n",
      );
      const skills = yield* discoverBobShellSkills(makeSettings(bobHome), cwd);
      assert.isUndefined(skills.find((s) => s.name === "bad-skill"), "malformed skill should be skipped");
    }).pipe(Effect.scoped),
  );

  it.effect("discovered skills are sorted before built-ins", () =>
    Effect.gen(function* () {
      const { cwd, bobHome } = yield* makeWorkspace();
      yield* writeSkill(`${bobHome}/skills`, "zebra", "---\n---\n");
      yield* writeSkill(`${cwd}/.bob/skills`, "alpha", "---\n---\n");
      const skills = yield* discoverBobShellSkills(makeSettings(bobHome), cwd);
      const names = skills.map((s) => s.name);
      const alphaIdx = names.indexOf("alpha");
      const zebraIdx = names.indexOf("zebra");
      const codeIdx = names.indexOf("code");
      assert.isTrue(alphaIdx < zebraIdx, "alpha before zebra");
      assert.isTrue(zebraIdx < codeIdx, "discovered skills before built-ins");
    }).pipe(Effect.scoped),
  );
});
