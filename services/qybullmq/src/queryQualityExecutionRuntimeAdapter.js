import { DiscoverExecutionRuntimeAdapter } from "./discoverExecutionRuntimeAdapter.js";

export class QueryQualityExecutionRuntimeAdapter extends DiscoverExecutionRuntimeAdapter {
  constructor(options = {}) {
    super({ ...options, role: "query_quality" });
  }
}
