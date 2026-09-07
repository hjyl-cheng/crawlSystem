import { classifyPublicationWindow } from "./publicationTimeEvidence.js";

export function createVideoActivityAccumulator({ observedAt, maxAgeDays }) {
  const evidence = {
    recentPublishedContentCount: 0,
    uncertainContentCount: 0,
    relationCounts: { inside: 0, outside: 0, after_as_of: 0, cutoff_overlap: 0, unresolved: 0 },
    unresolvedByStatusCounts: { relative: 0, estimated: 0, unavailable: 0, unresolved: 0 },
  };
  return {
    evidence,
    add(publication) {
      const window = classifyPublicationWindow(publication, { asOf: observedAt, maxAgeDays });
      evidence.relationCounts[window.relation] += 1;
      if (window.relation === "inside") evidence.recentPublishedContentCount += 1;
      else if (["after_as_of", "cutoff_overlap", "unresolved"].includes(window.relation)) {
        evidence.uncertainContentCount += 1;
        if (window.relation === "unresolved") {
          evidence.unresolvedByStatusCounts[publication.published_at_status] += 1;
        }
      }
    },
    addUnresolved() {
      evidence.uncertainContentCount += 1;
      evidence.relationCounts.unresolved += 1;
      evidence.unresolvedByStatusCounts.unresolved += 1;
    },
  };
}
