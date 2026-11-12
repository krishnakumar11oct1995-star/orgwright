# Contributing to Orgwright

Thanks for taking a look. Small, focused pull requests get reviewed fastest.

## Getting set up

```bash
git clone --recurse-submodules https://github.com/krishnakumar11oct1995-star/orgwright.git
cd orgwright
npm run setup
npm run test:unit
```

`test:unit` builds the packages and runs the step parser and code generator tests. It doesn't need a Salesforce org, and it's what CI runs on every pull request.

If you're touching `utils/lightning-helpers.ts`, also run:

```bash
npm run test:helpers
```

That runs the helpers against saved Lightning pages in `tests-orgwright/fixtures/`, also without an org.

For Studio org-metadata endpoints, `npm run test:studio` starts the real server
on a temporary loopback port and puts a controlled fake `sf` command on its PATH.
It checks HTTP/WebSocket responsiveness, cache/refresh behavior, CLI arguments
and fallback responses without connecting to Salesforce. These tests also run
as part of `npm run test:unit`.

## Where things live

- **A new step phrase** ("Clone this record", "Verify the record is locked"...): the parser is `orgwright/packages/resolver/src/stepParser.ts`, the code it turns into is in `orgwright/packages/codegen/src/stepEmitter.ts`. Add a line to the golden list in `orgwright/packages/resolver/test/parser.test.mjs`, and if you add a new action type, give it a sample in `orgwright/packages/codegen/test/codegen.test.mjs` (that test fails until you do).
- **Something Lightning does that breaks a helper**: fix it in `utils/lightning-helpers.ts`. If you can, save the page with `npm run fixture:capture` and add a test. Check the captured HTML for real names or record data before committing it.
- **Studio UI or API**: `orgwright/apps/studio/`.

## A few house rules

- Generated tests must never pass without doing what the step says. If something can't be done, throw an error with a message a tester can act on.
- No fixed sleeps (`waitForTimeout`) in new code. Wait for the thing you actually need.
- Never commit org data: `.env`, `.auth/`, `.orgwright/` caches, screenshots or fixtures with real records.

## Reporting a bug

Include the step text exactly as you typed it, what you expected, what happened, and a screenshot of the page if you can share one. Blur anything private first.
