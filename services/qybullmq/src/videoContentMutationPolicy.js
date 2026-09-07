// Arguments are SQL expressions owned by the content store, never request values.
export function retainedCommentPageSql(current, incoming) {
  return `CASE
    WHEN COALESCE((${current}->>'returned_count')::integer,0)>0 THEN ${current}
    WHEN COALESCE((${incoming}->>'returned_count')::integer,0)>0 THEN ${incoming}
    ELSE COALESCE(${current},${incoming})
  END`;
}

export function observedListSql(current, incoming, observed) {
  return `CASE
    WHEN ${observed} AND (
      COALESCE(cardinality(${incoming}),0)>0 OR COALESCE(cardinality(${current}),0)=0
    ) THEN ${incoming} ELSE ${current} END`;
}

export function descriptionMutationSql({ current = "crawler.contents.", incoming = "EXCLUDED.",
  observed = "true", unresolved = "retain", value, status, source } = {}) {
  value ??= `${incoming}description`;
  status ??= `${incoming}description_status`;
  source ??= `${incoming}description_source`;
  const stored = `${current}description`;
  const accepts = `${observed} AND (${status}='exact' OR (${status}='empty'
    AND NULLIF(btrim(COALESCE(${stored},'')),'') IS NULL))`;
  const fallback = (field, candidate) => unresolved === "retain"
    ? `${current}${field}`
    : `CASE WHEN ${current}description_status IN ('exact','empty') THEN ${current}${field}
      ELSE ${field === "description_status" ? candidate : `COALESCE(${candidate},${current}${field})`} END`;
  return [
    ["description", value], ["description_status", status], ["description_source", source],
  ].map(([field, candidate]) => `${field}=CASE WHEN ${accepts} THEN ${candidate}
    ELSE ${fallback(field, candidate)} END`).join(",\n");
}
