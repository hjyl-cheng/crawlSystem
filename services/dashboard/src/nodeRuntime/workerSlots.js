import {nodeWorkerRole} from '../nodeWorkerTypes.js';

export const workerSlotPrefix = node => nodeWorkerRole(node)==='fullcrawl'?'full-crawl':'incremental';

export function deploymentSlots(deployment) {
  const prefix=deployment?.mode==='full_crawl_collect'?'full-crawl':'incremental';
  return deployment?.slots??Array.from({length:deployment?.appliedCount??0},(_,i)=>`${prefix}-${i+1}`);
}

export function plannedWorkerSlots(node,count) {
  const prior=node.deployment;
  const prefix=workerSlotPrefix(node);
  const pattern=new RegExp(`^${prefix}-[1-9][0-9]*$`);
  const slots=[...(prior?.allocationSlots??deploymentSlots(prior))];
  if(count<(prior?.appliedCount??0) || new Set(slots).size!==slots.length || slots.some(s=>!pattern.test(s)))
    throw Object.assign(new Error('已有 Worker 清单与部署数量不一致，请刷新后重试'),{statusCode:409});
  let sequence=Math.max(prior?.slotSequence??0,...slots.map(s=>Number(s.slice(prefix.length+1))));
  if(!Number.isSafeInteger(sequence)||sequence<0)throw new Error('Worker 编号无效');
  while(slots.length<count){if(!Number.isSafeInteger(++sequence))throw new Error('Worker 编号超出范围');slots.push(`${prefix}-${sequence}`);}
  return {slots:slots.slice(0,count),allocationSlots:slots,slotSequence:sequence};
}
