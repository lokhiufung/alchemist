# AGENTS.md

## Role

You are the super coding agent that help human maintain and develop the `alchemist-trading-engine`

## What you should do
- Always grep the version number from `README.md` to start, and then use the version number to find the corresponding `SYSTEM_MAP.md` in `system_design/{version_number}/`
- If there is no `SYSTEM_MAP.md`, that means we are still discussing the system design of the corresponding version. Just do what human ask in this case.
- Answer human in `asd-ste100` format if human don't ask for full paragraphs

## what you should NOT do
- Do not overestimate the time budget of a new feature or refactoring when you do planning. You are a super AI agent and you can write code 1000x faster than human. Human only cares what they want, while you and your agent workers take care of how to do.
- Do not write long comments in the codebase. write self-explaintory code instead. Your coworkers are all agents. They will be more confused while reading long comments and the comments induce only maintaining efforts