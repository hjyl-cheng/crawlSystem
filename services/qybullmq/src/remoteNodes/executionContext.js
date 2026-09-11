import { uploadsCountryCode } from '../youtubeUploadsCountry.js';
import { RemoteProtocolError } from './protocol.js';

export function remoteExecutionOptions({ egress_country = null, uploads_country_recheck = null, resume_mode = 'initial' } = {}) {
  if (egress_country !== null && uploadsCountryCode(egress_country) !== egress_country) throw new RemoteProtocolError('INVALID_EXECUTION_COUNTRY', 400);
  if (!['initial','bullmq_redelivery_resume','network_attempt_resume','api_continuation'].includes(resume_mode)) throw new RemoteProtocolError('INVALID_EXECUTION_MODE', 400);
  let recheck = null;
  if (uploads_country_recheck !== null) {
    const value = uploads_country_recheck;
    if (Object.keys(value).sort().join(',') !== 'country,status' || uploadsCountryCode(value.country) !== value.country
      || !['requested','checked','unavailable'].includes(value.status)) throw new RemoteProtocolError('INVALID_COUNTRY_RECHECK', 400);
    recheck = { country: value.country, status: value.status };
  }
  return { egress_country, uploads_country_recheck: recheck, resume_mode };
}

// Rota persists the recheck on the Job, separately from its immutable Plan.
// Only remove that known field, never arbitrary unrecognized payload keys.
export function remotePlanJob(job) {
  const { uploads_country_recheck, ...data } = job.data;
  if (uploads_country_recheck !== undefined) remoteExecutionOptions({ uploads_country_recheck });
  // BullMQ exposes queueName as a prototype getter, not an own property.
  // Preserve that identity explicitly in the read/validation view.
  return { ...job, queueName: job.queueName, data };
}
