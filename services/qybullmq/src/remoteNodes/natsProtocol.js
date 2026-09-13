import {hash, RemoteProtocolError, uuid, MAX_GZIP_BYTES} from './protocol.js';
export const RESULT_STREAM='QY_REMOTE_RESULTS';
export const RESULT_CONSUMER='center-receipts';
export const MAX_NATS_BYTES=2*1024*1024;
export function natsEndpoint(value,{allowLoopback=false}={}) {
  const url=new URL(value);
  if(url.username||url.password||url.search||url.hash||(url.protocol==='wss:'?url.pathname!=='/node-messages':!['','/'].includes(url.pathname))
    ||(!['tls:','wss:'].includes(url.protocol)&&!(allowLoopback&&url.protocol==='nats:'&&['127.0.0.1','[::1]'].includes(url.hostname))))throw new TypeError('TLS NATS endpoint required');
  return url.href.replace(/\/$/,'');
}
export function resultEnvelope(nodeId,token,operation,taskId,payload){
  uuid(nodeId);uuid(taskId);
  if(!['channel_result','work_result','whole_channel_result'].includes(operation)||payload.length>MAX_GZIP_BYTES)throw new RemoteProtocolError('INVALID_RESULT',400);
  const value={version:1,nodeId,token,operation,taskId,payload:Buffer.from(payload).toString('base64')};
  value.receiptId=hash(`${nodeId}\n${operation}\n${taskId}\n${value.payload}`);
  return value;
}
export function encode(value){const bytes=Buffer.from(JSON.stringify(value));if(bytes.length>MAX_NATS_BYTES)throw new RemoteProtocolError('BODY_TOO_LARGE',413);return bytes;}
export function decode(bytes){
  if(bytes.length>MAX_NATS_BYTES)throw new RemoteProtocolError('BODY_TOO_LARGE',413);
  try{return JSON.parse(Buffer.from(bytes).toString());}catch{throw new RemoteProtocolError('INVALID_JSON',400);}
}
export function failure(error){return {ok:false,status:error instanceof RemoteProtocolError?error.status:503,error:error instanceof RemoteProtocolError?error.code:'GATEWAY_UNAVAILABLE'};}
export function unwrap(value){if(value?.ok!==true)throw new RemoteProtocolError(value?.error||'INVALID_GATEWAY_RESPONSE',value?.status||502);return value.value;}
