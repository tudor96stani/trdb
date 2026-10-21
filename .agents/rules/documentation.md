# Documentation instructions

These instructions apply when writing or editing technical documentation under `../../docs/technical_docs`. They describe the style used there; they do not prescribe implementation choices.

## Scope

- Work only under `../../docs/technical_docs` for documentation tasks unless the prompt explicitly requests changes elsewhere.
- Never edit, append to, rewrite or otherwise update `docs/decision log.md` or `docs/implementation notes.md` unless specifically asked to update that file in the prompt. A general request to document an implementation or explain a decision does not authorize updating either file.
- Use technical documentation to describe behavior, formats, constraints and relevant reasoning. Do not automatically add decision log entries or implementation notes as part of this work.
- Document how the component behaves, how it is implemented and why it is designed that way. Technical docs are not a code review or bug backlog: do not include implementation faults, missing checks, broken edge cases, correctness warnings or lists of shortcomings. Bugs belong in a task tracking tool, not in these documents. Do not investigate the code for new issues as part of a documentation review unless explicitly asked.
- Keep normal API errors, supported scope and caller responsibilities when they explain how to use the component. State them as behavior or contracts, without turning them into criticism of the implementation.
- Do not add notes about implementation status, such as "not yet implemented", "currently the code does X" or "still to do". They go stale as soon as the code catches up and make the doc drift. Describe the design as it is meant to work.
- Documentation should be self-contained - meaning do not reference documents outside of the `technical_docs` folder

## Voice and phrasing

- Write like a developer explaining the implementation to themselves or another developer. Keep the language plain, direct and conversational.
- Start with the concrete topic, problem or goal. Avoid generic introductions, polished summaries and promotional language.
- Use "we" when walking through behavior: "we go through the slots", "we can compute the offset". First person fits an explanation of a design choice when supported by the existing documentation. Do not invent personal experiences or attribute decisions to the author without evidence.
- Prefer concrete verbs and identifiers: "the buffer owns the pages", "the caller updates the header", "the slot is invalidated". Name the component responsible for each action.
- Explain consequences with natural transitions such as "this means that", "because", "so", "however", "instead" and "for now". Parenthetical clarifications and short asides are fine when they help.
- Keep uncertainty explicit. Use "I think", "could", "might", "still unsure" or "will need to revisit" for open questions; use definite wording for established behavior. Distinguish what exists now from proposals and future improvements.
- Keep the technical explanations conversational, but do not force slang or copy spelling and grammar mistakes. Match the surrounding document's capitalization and level of formality rather than imposing a uniform editorial style.
- Talk like a bored dev, using simple words and clear phrasing.

## Technical documentation structure

- Begin with a brief description of the component, format or behavior and its goal.
- Read the relevant code comments for explanations of the concept, its purpose and the current design. Use those explanations alongside the existing notes; reconcile them with the current implementation when needed rather than copying comments verbatim.
- Explain the high level concept before the implementation. Define terms when they first matter, including distinctions such as logical identity versus physical placement or ownership versus access.
- Present the implementation succinctly: identify the responsible components, their ownership boundaries and the main flow. Keep field layouts and concrete examples where they help explain the concept, but avoid a method-by-method inventory or repeating the code in prose.
- Organize around the subject, with simple headings such as `High level`, `Layout`, `process`, `Planning phase`, `Reasoning`, `Example`, `Possible improvements` and `See also`. Follow the local document's conventions.
- Explain an operation in execution order. Use numbered steps for ordered checks and actions, bullets for fields, responsibilities, scenarios or tradeoffs, and prose for reasoning.
- Use `TBD` for parts of the design that haven't been decided yet. Do not fill them with speculative behavior presented as decided.
- Match the high level folder structure to the crates and modules in the codebase.
- Not every struct and rs file requires a document - if multiple files aggregate to produce a single functionality, keep them together
- If a topic spans multiple files, create a subfolder and place all of them in there
- Do not jump from topic to topic and back again. Detail a topic until the end, then move to another one. Avoid talking about something, mentioning another topic, then going back to the first one.
- Do not document things that are obviously placeholders & testing purposes only.
- README.md from each folder/subfolder should not contain detailed technical concepts, only act as an index to that subfolder/module being documented there.
- For every module being documented, do a cross check in "decision log.md" and "implementation notes.md" for any notes and decisions around that module. Do not include/copy previously considered options, but if there are arguments for the current implementation choice, you can add them in the technical doc

## Examples and Markdown conventions

- Put identifiers, types, paths, constants and formulas in backticks. Use bold sparingly for decisions, important constraints or behavior that must stand out.
- Prefer small concrete examples: a schema and row, slot `(offset, length)` pairs, byte counts, offsets, or before/after state. Walk through how the values produce the result.
- Use fenced code blocks for code, ASCII layouts, state transitions and raw bytes. Add a language tag when it helps (`rust`, `sql`, `text`). Preserve alignment in diagrams.
- Check example arithmetic, units and offsets. Explain whether offsets are absolute or relative, and distinguish physical placement from logical identifiers when relevant.
- Link related notes using relative Markdown links and existing heading anchors; encode spaces in link targets. Check that new links resolve from the document's location.
- Keep existing `created: YYYY-MM-DD` front matter. For a new dated document, use its actual creation date; do not change creation dates during ordinary edits.
- Keep edits focused on the requested content. Preserve surrounding wording, historical reasoning and unfinished sections unless the task calls for revising them.