/**
 * BobShellSkills tests — covers skill discovery from disk.
 *
 * Exercises:
 *   - empty roots: only built-in skills returned in fixed order
 *   - user-level skills discovered from <bobHome>/skills
 *   - project-level skills discovered from <cwd>/.bob/skills
 *   - first-seen-wins (user overrides project for same name)
 *   - malformed SKILL.md (no frontmatter) is skipped gracefully
 *   - discovered skills sort alphabetically; built-ins follow
 *   - built-in names are not duplicated when also discovered
 */
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { BobShellSettings } from "@t3tools/contracts";

import { discoverBobShellSkills } from "../Drivers/BobShellSkills.ts";

const decodeSettings = Schema.decodeSync(BobShellSettings);

const BUILT_IN_NAMES = ["code", "plan", "ask", "advanced"];

/** Write a minimal SKILL.md with YAML frontmatter in the given directory. */
const writeSkillMd = (
  directory: string,
  skillName: string,
  frontmatter: string,
) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const skillDir = path.join(directory, skillName);
    yield* fs.makeDirectory(skillDir, { recursive: true });
    yield* fs.writeFileString(path.join(skillDir, "SKILL.md"), `---\n${frontmatter}\n---\n`);
  });

it.layer(NodeServices.layer)("discoverBobShellSkills", (it) => {
  it.effect("returns only built-in skills when no skill directories exist", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const dir = yield* fs.makeTempDirectoryScoped({ prefix: "t3code-skills-empty-" });
      const settings = decodeSettings({ enabled: true });

      const skills = yield* discoverBobShellSkills(settings, dir);

      const names = skills.map((s) => s.name);
      for (const builtin of BUILT_IN_NAMES) {
        expect(names).toContain(builtin);
      }
      // Only built-ins — nothing with a scope
      const withScope = skills.filter((s) => s.scope !== undefined);
      expect(withScope).toHaveLength(0);
    }),
  );

  it.effect("discovers user-level skills from <bobHome>/skills", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const dir = yield* fs.makeTempDirectoryScoped({ prefix: "t3code-skills-user-" });
      const path = yield* Path.Path;

      const userSkillsRoot = path.join(dir, "skills");
      yield* writeSkillMd(userSkillsRoot, "my-skill", "name: my-skill\ndescription: User skill");

      const settings = decodeSettings({ enabled: true, homePath: dir });
      const skills = yield* discoverBobShellSkills(settings, dir);

      const mySkill = skills.find((s) => s.name === "my-skill");
      expect(mySkill).toBeDefined();
      expect(mySkill?.scope).toBe("user");
      expect(mySkill?.description).toBe("User skill");
      expect(mySkill?.enabled).toBe(true);
    }),
  );

  it.effect("discovers project-level skills from <cwd>/.bob/skills", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const dir = yield* fs.makeTempDirectoryScoped({ prefix: "t3code-skills-project-" });
      const path = yield* Path.Path;

      const projectSkillsRoot = path.join(dir, ".bob", "skills");
      yield* writeSkillMd(projectSkillsRoot, "proj-skill", "name: proj-skill");

      const settings = decodeSettings({ enabled: true, homePath: "/nonexistent-home-for-test" });
      const skills = yield* discoverBobShellSkills(settings, dir);

      const projSkill = skills.find((s) => s.name === "proj-skill");
      expect(projSkill).toBeDefined();
      expect(projSkill?.scope).toBe("project");
    }),
  );

  it.effect("user-level skill wins when same name exists in both roots", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const dir = yield* fs.makeTempDirectoryScoped({ prefix: "t3code-skills-dedup-" });
      const path = yield* Path.Path;

      const userSkillsRoot = path.join(dir, "skills");
      const projectSkillsRoot = path.join(dir, ".bob", "skills");
      yield* writeSkillMd(userSkillsRoot, "shared", "name: shared\ndescription: from user");
      yield* writeSkillMd(projectSkillsRoot, "shared", "name: shared\ndescription: from project");

      const settings = decodeSettings({ enabled: true, homePath: dir });
      const skills = yield* discoverBobShellSkills(settings, dir);

      const allShared = skills.filter((s) => s.name === "shared");
      expect(allShared).toHaveLength(1);
      expect(allShared[0]?.description).toBe("from user");
      expect(allShared[0]?.scope).toBe("user");
    }),
  );

  it.effect("skill with no SKILL.md in subdirectory is skipped", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const dir = yield* fs.makeTempDirectoryScoped({ prefix: "t3code-skills-missing-md-" });
      const path = yield* Path.Path;

      // Create a subdirectory without SKILL.md
      const skillDir = path.join(dir, "skills", "orphan");
      yield* fs.makeDirectory(skillDir, { recursive: true });

      const settings = decodeSettings({ enabled: true, homePath: dir });
      const skills = yield* discoverBobShellSkills(settings, dir);

      const orphan = skills.find((s) => s.name === "orphan");
      expect(orphan).toBeUndefined();
    }),
  );

  it.effect("discovered skills are sorted alphabetically before built-ins", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const dir = yield* fs.makeTempDirectoryScoped({ prefix: "t3code-skills-sort-" });
      const path = yield* Path.Path;

      const userSkillsRoot = path.join(dir, "skills");
      yield* writeSkillMd(userSkillsRoot, "zebra-skill", "name: zebra-skill");
      yield* writeSkillMd(userSkillsRoot, "alpha-skill", "name: alpha-skill");

      const settings = decodeSettings({ enabled: true, homePath: dir });
      const skills = yield* discoverBobShellSkills(settings, dir);

      const discoveredNames = skills.filter((s) => s.scope !== undefined).map((s) => s.name);
      expect(discoveredNames).toEqual([...discoveredNames].sort());

      // All built-ins must appear after discovered skills
      const lastDiscoveredIdx = Math.max(
        ...discoveredNames.map((n) => skills.findIndex((s) => s.name === n)),
      );
      for (const builtin of BUILT_IN_NAMES) {
        const builtinIdx = skills.findIndex((s) => s.name === builtin);
        if (builtinIdx !== -1) {
          expect(builtinIdx).toBeGreaterThan(lastDiscoveredIdx);
        }
      }
    }),
  );

  it.effect("built-in skill name is not duplicated when discovered skill has same name", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const dir = yield* fs.makeTempDirectoryScoped({ prefix: "t3code-skills-builtin-dedup-" });
      const path = yield* Path.Path;

      // A user skill named "code" — same as the built-in
      const userSkillsRoot = path.join(dir, "skills");
      yield* writeSkillMd(userSkillsRoot, "code", "name: code\ndescription: Custom code skill");

      const settings = decodeSettings({ enabled: true, homePath: dir });
      const skills = yield* discoverBobShellSkills(settings, dir);

      const codeSkills = skills.filter((s) => s.name === "code");
      expect(codeSkills).toHaveLength(1);
      // User version should win (has a description and a scope)
      expect(codeSkills[0]?.scope).toBe("user");
      expect(codeSkills[0]?.description).toBe("Custom code skill");
    }),
  );
});
