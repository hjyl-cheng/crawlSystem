import assert from "node:assert/strict";
import test from "node:test";
import {
  migrationCandidateStatusSql,
  migrationDoneSql,
  migrationIncompleteSql,
  migrationLifecycleState,
} from "./migrationCompletion.js";

const lifecycleCases = [
  {
    name: "unstarted",
    input: {},
    expected: {
      candidateStatus: "discovered",
      status: "discovered",
      migrationIncomplete: false,
      migrationDone: false,
    },
  },
  {
    name: "queued Target candidate",
    input: { candidateId: "11", candidateStatus: "queued" },
    expected: {
      candidateStatus: "queued",
      status: "queued",
      migrationIncomplete: false,
      migrationDone: false,
    },
  },
  {
    name: "active promoted candidate awaiting finalization",
    input: {
      candidateId: "12",
      candidateStatus: "accepted",
      channelStatus: "active",
      promotionCandidateId: "12",
      finalizedStatus: "pending",
    },
    expected: {
      candidateStatus: "finishing",
      status: "finishing",
      migrationIncomplete: true,
      migrationDone: false,
    },
  },
  {
    name: "active promoted candidate finalized automatically",
    input: {
      candidateId: "13",
      candidateStatus: "accepted",
      channelStatus: "active",
      promotionCandidateId: "13",
      finalizedStatus: "ready_auto",
    },
    expected: {
      candidateStatus: "accepted",
      status: "active",
      migrationIncomplete: false,
      migrationDone: true,
    },
  },
  {
    name: "dormant promoted candidate finalized partially",
    input: {
      candidateId: "14",
      candidateStatus: "accepted",
      channelStatus: "dormant",
      promotionCandidateId: "14",
      finalizedStatus: "ready_partial",
    },
    expected: {
      candidateStatus: "accepted",
      status: "dormant",
      migrationIncomplete: false,
      migrationDone: true,
    },
  },
  {
    name: "active candidate not selected for promotion",
    input: {
      candidateId: "15",
      candidateStatus: "accepted",
      channelStatus: "active",
      promotionCandidateId: "99",
      finalizedStatus: "pending",
    },
    expected: {
      candidateStatus: "accepted",
      status: "active",
      migrationIncomplete: false,
      migrationDone: false,
    },
  },
];

test("Migration lifecycle has one table-driven JavaScript contract", () => {
  for (const fixture of lifecycleCases) {
    assert.deepEqual(
      migrationLifecycleState(fixture.input),
      fixture.expected,
      fixture.name,
    );
  }
});

test("Migration lifecycle SQL builders accept the list query aliases", () => {
  const incomplete = migrationIncompleteSql("target", "registry", "profile");
  const done = migrationDoneSql("target", "registry", "profile");
  const candidateStatus = migrationCandidateStatusSql("target", "registry", "profile");

  assert.match(incomplete, /target\.status='accepted'/);
  assert.match(incomplete, /registry\.status='active'/);
  assert.match(incomplete, /profile\.status/);
  assert.match(done, /registry\.status IN \('active','dormant'\)/);
  assert.match(done, /IN \('ready_auto','ready_partial'\)/);
  assert.match(candidateStatus, /^CASE\s+WHEN/);
  assert.match(candidateStatus, /THEN 'finishing'/);
  assert.match(candidateStatus, /COALESCE\(target\.status,'discovered'\)/);
});
