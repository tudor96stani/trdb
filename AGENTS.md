# Documentation instructions

These instructions apply when writing or editing technical documentation under `docs/technical_docs/`. They describe the style used there; they do not prescribe implementation choices.

## Scope

- Work only under `docs/technical_docs/` for documentation tasks unless the prompt explicitly requests changes elsewhere.
- Never edit, append to, rewrite or otherwise update `docs/decision log.md` or `docs/implementation notes.md` unless specifically asked to update that file in the prompt. A general request to document an implementation or explain a decision does not authorize updating either file.
- Use technical documentation to describe behavior, formats, constraints and relevant reasoning. Do not automatically add decision log entries or implementation notes as part of this work.

## Voice and phrasing

- Write like a developer explaining the implementation to themselves or another developer. Keep the language plain, direct and conversational.
- Start with the concrete topic, problem or goal. Avoid generic introductions, polished summaries and promotional language.
- Use "we" when walking through behavior: "we go through the slots", "we can compute the offset". First person fits an explanation of a design choice when supported by the existing documentation. Do not invent personal experiences or attribute decisions to the author without evidence.
- Prefer concrete verbs and identifiers: "the buffer owns the pages", "the caller updates the header", "the slot is invalidated". Name the component responsible for each action.
- Explain consequences with natural transitions such as "this means that", "because", "so", "however", "instead" and "for now". Parenthetical clarifications and short asides are fine when they help.
- Keep uncertainty explicit. Use "I think", "could", "might", "still unsure" or "will need to revisit" for open questions; use definite wording for established behavior. Distinguish what exists now from proposals and future improvements.
- Keep the technical explanations conversational, but do not force slang or copy spelling and grammar mistakes. Match the surrounding document's capitalization and level of formality rather than imposing a uniform editorial style.
- Talk like a bored dev

## Explain the reasoning

- Describe what happens and why. Include the constraint that made the approach necessary, not just the resulting API or structure.
- When a choice matters, show the alternatives, their practical costs and the chosen approach. Explain why an option was rejected without turning the discussion into a formal decision framework.
- State the scope and limitations directly: what is supported, what is not yet implemented and what is left for later.
- Keep tradeoffs grounded in this implementation: ownership, coupling, byte footprint, extra scans, locking, testability or complexity. Do not claim performance benefits without evidence; leave benchmarking as an open question when appropriate.
- Comparisons with the Java implementation are useful when they explain a change or a lesson learned. Include them only when relevant and supported by the existing notes or code.
- For edge cases, describe the setup, the operation and the resulting state. Explain why the case is reachable and which invariant or assumption it affects.

## Technical documentation structure

- Begin with a brief description of the component, format or behavior and its goal.
- Organize around the subject, with simple headings such as `High level`, `Layout`, `process`, `Planning phase`, `Reasoning`, `Example`, `Possible improvements` and `See also`. Follow the local document's conventions.
- Explain an operation in execution order. Use numbered steps for ordered checks and actions, bullets for fields, responsibilities, scenarios or tradeoffs, and prose for reasoning.
- State invariants and ownership boundaries where they affect behavior. Explain both the normal path and relevant exceptions.
- Use `TBD` or `Not yet implemented` for unfinished sections when appropriate. Do not fill them with speculative behavior presented as implemented.

## Examples and Markdown conventions

- Put identifiers, types, paths, constants and formulas in backticks. Use bold sparingly for decisions, important constraints or behavior that must stand out.
- Prefer small concrete examples: a schema and row, slot `(offset, length)` pairs, byte counts, offsets, or before/after state. Walk through how the values produce the result.
- Use fenced code blocks for code, ASCII layouts, state transitions and raw bytes. Add a language tag when it helps (`rust`, `sql`, `text`). Preserve alignment in diagrams.
- Check example arithmetic, units and offsets. Explain whether offsets are absolute or relative, and distinguish physical placement from logical identifiers when relevant.
- Link related notes using relative Markdown links and existing heading anchors; encode spaces in link targets. Check that new links resolve from the document's location.
- Keep existing `created: YYYY-MM-DD` front matter. For a new dated document, use its actual creation date; do not change creation dates during ordinary edits.
- Keep edits focused on the requested content. Preserve surrounding wording, historical reasoning and unfinished sections unless the task calls for revising them.
