# Project Agent Config

## Code Review Agent

When asked to perform a code review, follow this process exactly:

### Step 1: Context Gathering
Read the following files from the project root before doing anything else:

- `README.md` — project overview and setup

- Any file whose name contains `prd` (case-insensitive), or ends in `_prd`, or starts with `prd`,
  in any of these extensions: `.md`, `.txt`, `.docx`, or no extension
  — product requirements and Recommended Build Order

- Any file whose name contains `architecture` or `arch` (case-insensitive),
  in any of these extensions: `.md`, `.txt`, `.docx`, or no extension
  — system architecture

- Any file whose name contains `plan` (case-insensitive),
  including names like `Step1Plan_PR01.md`, `Step4Plan_PR05_Consolidated.md`, `Step1AndStep2LeanTestPlan.md`,
  in any of these extensions: `.md`, `.txt`, `.docx`, or no extension
  — implementation plans and test plans

To find these files, run:
  `find . -maxdepth 2 -type f \( -iname "*prd*" -o -iname "*architecture*" -o -iname "*arch*" -o -iname "*plan*" \) \( -iname "*.md" -o -iname "*.txt" -o -iname "*.docx" -o ! -iname "*.*" \)`
and read all results before proceeding.

After reading them, confirm:
1. What the project does
2. The Recommended Build Order (from the PRD)
3. Determine the build step as follows:
   - If the env var `CI_REVIEW` is set to `true`, or `CI` is set to `true`, do not ask. Instead:
     - Inspect the changed files from `git diff origin/main...HEAD --name-only`
     - If the changes map clearly to a step in the Recommended Build Order, state which step and proceed
     - If the changes are outside the Recommended Build Order (e.g. CI/CD config, tooling,
       infrastructure, GitHub Actions workflows, documentation), state that explicitly:
       "This PR appears to be an infrastructure/tooling addition outside the Recommended Build Order.
       Reviewing against general engineering standards rather than a specific build step."
       Then proceed with the review using that framing.
   - If neither `CI_REVIEW=true` nor `CI=true`, ask the user which step of the Recommended Build Order
     this PR is for before proceeding.

Do not begin the review until step 3 is resolved.

---

### Step 2: Code Review
Review the diff of the current branch against main (run `git diff origin/main...HEAD` if not provided).
Act as a strict senior engineer. Skip minor style issues. Focus on:

- **Bugs** — logic errors, off-by-ones, incorrect assumptions
- **Correctness** — does the code match the spec for this build step?
- **Edge cases** — unhandled inputs, boundary conditions, null/empty states
- **Concurrency** — race conditions, deadlocks, unsafe shared state
- **Performance** — unnecessary computation, N+1 queries, blocking calls
- **Security** — injection, auth bypass, data exposure, input validation gaps
- **Maintainability** — complexity that will cause future bugs
- **Test gaps** — critical paths with no coverage

For each issue:
- **Severity**: Critical / High / Medium / Low
- **Location**: file, function, line number
- **Explanation**: what is wrong and why
- **Failure Scenario**: concrete example of how it breaks
- **Precise Fix**: corrected code snippet

---

### Step 3: Testability Audit
Flag any of the following:
- Tight coupling to external systems (DB, network, filesystem, time, randomness)
- Missing dependency injection
- Hidden side effects
- Functions doing too many things
- Hard-to-mock dependencies
- Non-deterministic behavior (direct use of `Date.now()`, `Math.random()`, etc.)
- Missing abstraction boundaries between business logic and I/O

For each issue:
1. Explain why it makes testing hard
2. Show a refactor that fixes it
3. Sketch the unit tests that become possible after

Prefer: pure functions, separated business logic and I/O, injectable dependencies, deterministic behavior.

---

### Step 4: Summary Report
- 🔴 Must-Fix Items (merge blockers)
- 🟡 Recommended Refactors
- 🟢 Optimization Opportunities
- 📝 Improved Code Snippets
- ⚖️ Final Verdict — is this PR ready to merge? Does it fulfill the scope of its Build Order step?
  This section must be the last section in the report. End it with exactly one of these two lines:
  `VERDICT: READY TO MERGE`
  `VERDICT: NOT READY TO MERGE`

---

### Output
After completing the full review, write the report to the **current working directory**:
- Use a relative path only (e.g. `./review_feature-auth_2026-04-24.md`)
- Never use an absolute path
- Use the filename provided by the caller if one is specified
- If no filename is provided, derive it by running `git rev-parse --abbrev-ref HEAD`
  for the branch name and `date +%Y-%m-%d` for today's date,
  then write to `./review_<branch-name>_<YYYY-MM-DD>.md`
- Do not truncate any section. Write the full report including all issues, snippets, and the final verdict.
- After writing, confirm the filename and its full resolved path to the user.
