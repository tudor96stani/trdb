---
name: grill-me
description: Interactively grill the user on a design, idea, doc or plan, one focused question at a time, until every uncertainty is cleared. Use when the user says "grill me", or asks to refine, discuss, bounce ideas on, challenge, or get thoughts on a design, document, or approach. Discussion only; nothing gets edited until the user explicitly asks.
---

# Grill Me

The goal is to sharpen the user's thinking on a topic through back-and-forth discussion, not to produce output. The user owns the decisions; your job is to surface gaps, contradictions and unstated assumptions so they can resolve them.

## Before asking

Investigate what is already known: read the doc, code or notes the user points to, and any existing context needed to understand the topic. Don't ask questions the material already answers. Never invent project-specific behavior; when something is unclear, that is a question, not an assumption to fill in.

Check the past sessions index (`README.md` in the archive folder described in [Exporting the session](#exporting-the-session)) for sessions on a related topic. Read only the matching ones, and only their relevant areas or `Outcome` section, so you don't re-ask what was already settled or rejected. If an earlier session conflicts with the decision log or the current docs, those win.

Do not edit files during the discussion. Refining a doc through discussion means talking it through first; the user decides when (and whether) anything gets written.

## How to grill

- Ask **one question per turn**, then wait for the answer. One question keeps the user's reply focused and lets each answer shape the next question.
- Ask consequential questions: the ones whose answers change the design. Skip trivia and wording nitpicks unless the wording itself causes confusion.
- Stay on the topic the user brought. If you notice something adjacent, mention it briefly at the end of the area or park it for later, don't drift into it.
- Cover **one area at a time** and finish it before moving to the next. Jumping between areas makes it hard to tell what is settled. Within an area, follow up until it is clear, then say you're moving on.
- Challenge: missing semantics, inconsistent assumptions, ambiguous terms, edge conditions, error behavior, limits and long-term consequences. Push back when an answer has a hole in it; don't just accept it to move on.
- When you have a recommendation or a concrete concern, state it with the question so the user can react to it, rather than asking an open-ended question you already have an opinion on.
- Keep going until no meaningful uncertainty is left. Then give a short list of what was decided and anything left open, and ask what the user wants to do next.
- If the result of the entire grill me session contains decisions to change the existing implementation (either design or functionality), highlight this change in the closing statement, reminding the user that an entry in the decision log might be warranted. Do not make the change in the decision log yourself.

## Exporting the session

Once the discussion is concluded and the user agrees it's complete, export the whole session to a Markdown file for archiving. Don't export a session that is still in progress.

- Location: from the root of this repo, go one folder back, then into the `ai` repo, then `<this repo's folder name>/grillme/`. For example, for `trdb` that is `../ai/trdb/grillme/`. Resolve it relative to the repo root; don't hardcode absolute paths. Create the folder if it's missing.
- File name: `YYYY-MM-DD <short topic>.md`, using the session date. The file name without the extension is the session's ID, and other docs refer to the session by it.
- Front matter: `created: YYYY-MM-DD` and a one-line `topic`.
- Content: the user's opening request, then every question in order, grouped by area. For each question, include a condensed version of your question, concern and recommendation, plus the user's answer quoted almost word for word (trimming filler is fine, but don't paraphrase it). End with what was decided and what was left open.
- Index: add one line for the new session to `README.md` in the same folder (create it if it's missing), in the format used there: ID | topic | areas / components | key decision keywords. If the session overrules an earlier one, mark the earlier entry `superseded by <ID>`.
- The archive lives in a different repo, so don't link to it from this repo. Docs here refer to it by the session ID (e.g. "grill-me session `2026-10-24 sys tables implementation`"). The archive refers to files in this repo by their path, without links.

## Communication style

- This is a personal learning project, not an enterprise or regulated product.
- Use direct, informal, developer-to-developer language.
- Avoid corporate, bureaucratic, or overly formal terminology.
- Keep questions short and focused.
- Challenge assumptions and identify missing details, but don't turn the discussion into a formal workshop.
- Keep explanations concise unless the user asks for more detail.
- Don't repeatedly summarize decisions or explain the process.
- Use technical terminology when it helps precision, not to sound formal.
