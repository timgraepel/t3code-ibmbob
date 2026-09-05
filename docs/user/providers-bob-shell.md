# Bob Shell

T3 Code runs IBM Bob Shell as a coding agent. Bob Shell connects to IBM's hosted service,
which selects the model for each request. You cannot choose a model from within T3 Code — Bob
manages that internally.

The agent, files, and conversation state stay on the selected environment. Your Bob credentials
stay in the Bob Shell config directory on that machine.

## Set up Bob Shell

Bob Shell is off by default. On web or desktop, open **Settings** > **Providers**, select the
device that runs your project, then select **Bob Shell**. Enable it, then configure the fields
described below.

Bob Shell must already be installed on the environment. T3 Code does not install it.
Download the installer for your platform from the IBM Bob download page and follow the
official IBM installation guide.

## Settings

### Binary path

Path to the `bob` executable on the environment. Leave empty to use `bob` from `PATH`.

```text
Binary path: /usr/local/bin/bob
```

### BOB_HOME path

Custom Bob Shell config directory. When set, T3 Code passes this path as the `BOB_HOME`
environment variable so Bob Shell reads config, credentials, and trusted folders from that
directory.

Leave empty to use the default location (`~/.bob`).

```text
BOB_HOME path: ~/.bob-work
```

Use this when you want to run multiple Bob instances with separate credentials — for example,
a work account and a personal account. Bob Shell will open a browser to authenticate when you
first run it with a new home directory:

```bash
BOB_HOME=~/.bob-work bob
```

### Team ID

When set, T3 Code passes `--team-id <value>` on every `bob run` call. Use this if your
Bob subscription requires a team identifier. Leave empty if it does not.

### API key

Bob Shell API key for non-interactive authentication. This is for CI-style setups where
browser sign-in is not available.

When set, T3 Code:
- passes the key as the `BOBSHELL_API_KEY` environment variable, and
- passes `--auth-method api-key` on every `bob run` call.

Leave empty to use Bob Shell's normal browser-based SSO sign-in. If you have already signed in
interactively on the environment, you do not need an API key.

To create an API key, go to the Bob web portal and create a key with **Scope** set to
**Inference**. Store it securely; you cannot view it again after creation.

### Launch arguments

Extra flags passed to every `bob run` call. Use this to set a mode, cap cost, or pass any other
`bob run` flag.

Examples:

```text
--chat-mode=plan
--max-cost 2
--chat-mode=ask --max-cost 1
```

The default mode is `agent`. If you do not specify `--chat-mode`, T3 Code passes
`--chat-mode=agent` automatically so the full tool suite and configured MCP servers are available.

## Modes and skills

Bob Shell modes shape the agent's behavior for a session. The four built-in modes are:

| Mode       | Description                                          |
| ---------- | ---------------------------------------------------- |
| `code`     | Generate, modify, and refactor code.                 |
| `plan`     | Design and plan implementations before running them. |
| `ask`      | Read-only question answering. Does not modify files. |
| `advanced` | Extended capabilities including MCP tools.           |

You can define custom modes in `.bob/custom_modes.yaml` in your project or in
`~/.bob/custom_modes.yaml` for user-level modes. T3 Code discovers both locations and shows them
in the mode and skill picker.

To select a mode for a thread, use the skill picker in the message composer. Selecting a
built-in or custom mode sets `--chat-mode=<slug>` for that turn.

To force a mode for every turn on a provider, put it in **Launch arguments**:

```text
--chat-mode=plan
```

If a mode is set in **Launch arguments**, the skill picker selection for that thread is ignored.

## Authentication and trusted folders

Bob Shell uses browser-based SSO (IBMid or corporate SSO) by default. The first time you run
Bob Shell on a machine, or after your session expires, it opens `bob.ibm.com/login` in your
browser automatically. No explicit login command is needed.

Credentials are stored in the Bob config directory (`~/.bob` by default, or whatever
`BOB_HOME path` you set). T3 Code does not open a browser for you — run Bob Shell once
interactively in a terminal first to authenticate, then enable the provider.

```bash
bob
```

For non-interactive environments, use an API key (see [API key](#api-key) above).

### Trusted folders

Bob Shell requires the working directory to be trusted before loading project settings, MCP
servers, and custom commands. Non-interactive sessions (which is how T3 Code runs Bob) default
to trusted if no explicit decision is stored.

To pre-configure trust for a project directory:

```bash
# Run an interactive session in the directory once to set trust via the dialog
bob
```

Or add the path to `~/.bob/trustedFolders.json` directly. Untrusted folders run Bob in a
restricted mode where MCP servers do not connect and project settings are ignored.

## Model selection

Bob Shell does not expose model choice to the caller. IBM's service selects the model for each
request based on your subscription and the request type. The model picker in T3 Code shows no
models for this provider — this is expected behavior, not an error.

## Thread history and rollback

Bob Shell stores its own conversation history in an internal database (`~/.bob/db/bob.db`).
T3 Code cannot read that history directly. Thread continuity across turns uses Bob's `--resume`
flag with the task ID from the previous turn.

T3 Code's own checkpoint and rollback controls work by returning to a prior git checkpoint.
Bob Shell does not support conversation rewind through the CLI, so reverting a thread discards
the T3 Code checkpoint but does not rewind Bob's internal session state. Send a follow-up
message or start a new thread after a rollback.

## Multiple Bob accounts or configurations

Use separate `BOB_HOME path` values to run multiple Bob instances on the same environment.

```text
Display name: Bob Work
BOB_HOME path: ~/.bob-work
```

```text
Display name: Bob Personal
BOB_HOME path: ~/.bob-personal
```

Authenticate each home directory separately before using that provider instance in T3 Code.
Run Bob Shell once interactively with each home to trigger the browser sign-in:

```bash
BOB_HOME=~/.bob-work bob
BOB_HOME=~/.bob-personal bob
```

T3 Code keeps sessions from the same Bob home directory together. Threads started with one
instance can be resumed by another instance that uses the same `BOB_HOME path`.
