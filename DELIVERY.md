# Delivery Method

How a change is planned, built and handed over in this repository and its siblings.
`ENGINEERING.md` says what the code looks like; this file says how a change is done.

The goal: deliver changes that someone else can understand, verify, operate and
revert. Writing more code is not the goal; avoiding rework is.

## Before coding

- What concrete problem is being solved, and which behaviour must stay the same?
- Does a service, component, API or policy already own this? Reuse it; do not copy it.
- Who may perform the action, on which record, customer, contract or resource?
  Access to a screen is not authorization over the record behind it.
- Who consumes the output, and what breaks if the contract changes?
- Which flags, rollout modes (pilot, shadow), public interfaces and owners decide the
  rule? Do not anticipate a contract that is still being decided.
- Map the whole flow once: user → UI → endpoint → service → storage → response → back
  to the client. Write down what must keep working before the first edit.

## While coding

- The simplest change that respects the existing architecture.
- Keep domain decisions, integration, storage and presentation separate.
- Test at least one allowed case, one denied case and one plausible failure.
- Encode output for its context: HTML, attribute, JavaScript, URL, SQL and file paths
  each have their own rules. Filtering input is not enough.
- One logical change per diff. Unrelated edits go in another change.
- Two similar implementations? Compare their meaning and dependencies, extract only what
  is truly equal, and keep adapters for the differences.
- Every route or action declares its permission explicitly, so access can be audited
  from configuration, not only by reading handlers.

## Before calling it done

- Run the real journey to the stored or displayed result, not only the unit test.
- A test that checks a rule exists (a config pattern, a file) is not proof the rule
  works. Pair a small deterministic test with an integrated check sized to the risk.
- Check contract, permissions, error messages, mobile and recovery where they apply.
- Tie the result to the environment and the exact commit that was tested.
- Say how to revert, and what was not verified.

## Evidence, not activity

Activity numbers do not prove delivery:

| Measure | Allowed conclusion | Not allowed |
|---|---|---|
| Commits | How much changed | How much was accepted or works |
| Commits that touch tests | Effort went into tests | The tests ran or passed |
| `revert` commits | Changes were undone | Each one was a defect (requirements change too) |
| `fix` commits | Incremental work | That many bugs |
| Duplicated files | A future maintenance cost | That the copy is useless or safe to delete |

Report what passed, what failed and what was not tested, with the command and the
output that show it.

## Using AI

AI is a tool for investigation, review and challenging hypotheses. Understanding the
problem, owning the effects and the final decision stay with the engineer.

- Ask for context, not only code: which services and patterns already exist, where the
  sources of truth are.
- Ask for risks and alternatives: which modules depend on this, which contract,
  permission and concurrency cases can fail.
- Ask for the smallest design and what would make it needlessly complex.
- Ask for the negative scenarios that would prove the change wrong.
- Before merging, be able to justify every check, query, dependency and state change
  without asking the AI to explain the code.

## Practice plan (8 weeks)

| Weeks | Exercise |
|---|---|
| 1–2 | Map one feature you know: consumers, input, authorization, data, states, failures, tests. No code before the map. |
| 3–4 | Fix a real defect with an explicit contract: behaviour before, negative case, minimal change, comparison after. |
| 5–6 | Responsible reuse: find two similar implementations, extract only what is equal, keep adapters. |
| 7–8 | One end-to-end improvement with focused tests, functional proof, risk review, short docs and rollback. |

Progress shows when you can explain a solution and its alternatives on your own, know
contracts and consumers before the first commit, and leave the next person able to
maintain or revert the change without rebuilding its history.
