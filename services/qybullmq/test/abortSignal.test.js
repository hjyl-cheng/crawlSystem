import assert from "node:assert/strict";
import test from "node:test";
import { combineAbortSignals, throwIfAborted } from "../src/abortSignal.js";

test("combineAbortSignals preserves a sole explicit signal", () => {
  const controller = new AbortController();

  assert.equal(combineAbortSignals(controller.signal), controller.signal);
});

test("combineAbortSignals removes duplicate signal references", () => {
  const controller = new AbortController();

  assert.equal(
    combineAbortSignals(controller.signal, controller.signal),
    controller.signal,
  );
});

test("combineAbortSignals preserves the triggering reason for distinct signals", () => {
  const explicit = new AbortController();
  const ambient = new AbortController();
  const reason = new Error("lease lost");
  const signal = combineAbortSignals(explicit.signal, ambient.signal);

  assert.notEqual(signal, explicit.signal);
  assert.notEqual(signal, ambient.signal);
  ambient.abort(reason);
  assert.equal(signal.aborted, true);
  assert.equal(signal.reason, reason);
});

test("combineAbortSignals returns null when no signal is available", () => {
  assert.equal(combineAbortSignals(null, undefined), null);
});

test("throwIfAborted throws the original abort reason", () => {
  const controller = new AbortController();
  const reason = new Error("channel execution cancelled");
  controller.abort(reason);

  assert.throws(
    () => throwIfAborted(controller.signal),
    (error) => error === reason,
  );
});
