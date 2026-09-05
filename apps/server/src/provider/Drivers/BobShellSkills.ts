/**
 * BobShellSkills — skill discovery for Bob Shell.
 *
 * Bob Shell loads user-level skills from `<bobHome>/skills/<name>/SKILL.md`
 * (where `bobHome` defaults to `~/.bob`) and project-level skills from
 * `<cwd>/.bob/skills/<name>/SKILL.md`. Each skill directory must contain a
 * `SKILL.md` with YAML frontmatter carrying at minimum a `name` field.
 *
 * Built-in Bob modes (`code`, `plan`, `ask`, `advanced`) are always appended
 * after discovered skills so they appear in the picker even when no skill
 * directories exist. A discovered skill whose name matches a built-in is
 * not duplicated — the user's or project version wins by appearing first.
 *
 * Discovery is best-effort: unreadable roots and malformed skill entries are
 * skipped so a broken skill never degrades the provider snapshot.
 *
 * @module provider/Drivers/BobShellSkills
 */
import type { BobShellSettings, ServerProviderSkill } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import { parse as parseYamlDocument } from "yaml";

import { resolveBobShellHomePath } from "./BobShellHome.ts";

const FRONTMATTER_PATTERN = /^---\r?\n([\s\S]*?)\r?\n---(?:\r?\n|$)/;

type SkillFrontmatter =
  | { readonly kind: "missing" | "malformed" }
  | { readonly kind: "parsed"; readonly name?: string; readonly description?: string };

function parseSkillFrontmatter(contents: string): SkillFrontmatter {
  const match = FRONTMATTER_PATTERN.exec(contents);
  if (!match) return { kind: "missing" };
  let parsed: unknown;
  try {
    parsed = parseYamlDocument(match[1] ?? "");
  } catch {
    return { kind: "malformed" };
  }
  if (typeof parsed !== "object" || parsed === null) return { kind: "malformed" };
  const record = parsed as Record<string, unknown>;
  const name = typeof record.name === "string" ? record.name.trim() : undefined;
  const description = typeof record.description === "string" ? record.description.trim() : undefined;
  return {
    kind: "parsed",
    ...(name ? { name } : {}),
    ...(description ? { description } : {}),
  };
}

/** Built-in Bob Shell modes, always available as skills. */
const BOB_BUILT_IN_SKILLS: ReadonlyArray<ServerProviderSkill> = [
  { name: "code", path: "code", enabled: true, description: "Code mode — default development assistant" },
  { name: "plan", path: "plan", enabled: true, description: "Plan mode — structured planning before coding" },
  { name: "ask", path: "ask", enabled: true, description: "Ask mode — read-only question-answering" },
  { name: "advanced", path: "advanced", enabled: true, description: "Advanced mode — extended capabilities" },
];

/**
 * Scan one skill root directory and collect skills into `skillsByName`.
 * Each direct subdirectory is treated as a skill by its directory name.
 * First-seen name wins.
 */
const scanSkillRoot = Effect.fn("scanBobSkillRoot")(function* (
  directory: string,
  scope: "user" | "project",
  skillsByName: Map<string, ServerProviderSkill>,
): Effect.fn.Return<void, never, FileSystem.FileSystem | Path.Path> {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;

  const entries = yield* fileSystem
    .readDirectory(directory)
    .pipe(Effect.orElseSucceed((): ReadonlyArray<string> => []));

  for (const entry of [...entries].sort()) {
    const skillMdPath = path.join(directory, entry, "SKILL.md");
    const contents = yield* fileSystem
      .readFileString(skillMdPath)
      .pipe(Effect.orElseSucceed(() => undefined));
    if (contents === undefined) continue;

    const frontmatter = parseSkillFrontmatter(contents);
    if (frontmatter.kind === "malformed") continue;

    // Use the directory entry name as the canonical skill name (same as Claude's
    // verified behaviour). A `name` in frontmatter is purely informational.
    const name = entry.trim();
    if (!name) continue;

    if (skillsByName.has(name)) continue;

    skillsByName.set(name, {
      name,
      path: skillMdPath,
      enabled: true,
      scope,
      ...(frontmatter.kind === "parsed" && frontmatter.description
        ? { description: frontmatter.description }
        : {}),
    });
  }
});

/**
 * Discover Bob Shell skills from the user home directory and the project
 * `.bob/skills` directory, then append the four built-in mode skills for
 * any names not already discovered.
 */
export const discoverBobShellSkills = Effect.fn("discoverBobShellSkills")(function* (
  settings: Pick<BobShellSettings, "binaryPath" | "homePath">,
  cwd: string,
  _environment: NodeJS.ProcessEnv = process.env,
): Effect.fn.Return<ReadonlyArray<ServerProviderSkill>, never, FileSystem.FileSystem | Path.Path> {
  const path = yield* Path.Path;
  const bobHome = yield* resolveBobShellHomePath(settings);

  const roots: ReadonlyArray<{ directory: string; scope: "user" | "project" }> = [
    { directory: path.join(bobHome, "skills"), scope: "user" },
    { directory: path.join(cwd, ".bob", "skills"), scope: "project" },
  ];

  const skillsByName = new Map<string, ServerProviderSkill>();
  for (const root of roots) {
    yield* scanSkillRoot(root.directory, root.scope, skillsByName);
  }

  // Append built-in modes for names not already discovered.
  for (const builtin of BOB_BUILT_IN_SKILLS) {
    if (!skillsByName.has(builtin.name)) {
      skillsByName.set(builtin.name, builtin);
    }
  }

  // Discovered (scoped) skills sort alphabetically; built-ins follow in their
  // fixed order at the end.
  const discovered = [...skillsByName.values()];
  const userAndProject = discovered.filter((s) => s.scope !== undefined);
  const builtins = discovered.filter((s) => s.scope === undefined);
  return [
    ...userAndProject.sort((a, b) => a.name.localeCompare(b.name)),
    ...builtins,
  ];
});
