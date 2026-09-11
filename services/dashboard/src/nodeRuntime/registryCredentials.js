import {open} from 'node:fs/promises';
import {constants} from 'node:fs';

// Deployment-only credential. It is never included in page previews or node
// configuration, and its authority must exactly match the frozen image host.
export function registryCredentialsFromEnv(env=process.env){
  const path=env.SERVER_NODE_REGISTRY_CREDENTIALS_FILE;
  return async image=>{
    if(!path)return null;
    const file=await open(path,constants.O_RDONLY|constants.O_NOFOLLOW|constants.O_NONBLOCK);
    try{
      const stat=await file.stat();
      if(!stat.isFile() || stat.size>4096 || (stat.mode&0o077))throw new Error('镜像拉取凭据文件无效');
      const buffer=Buffer.alloc(4097);const {bytesRead}=await file.read(buffer,0,buffer.length,0);
      if(bytesRead>4096)throw new Error('镜像拉取凭据文件无效');
      const value=JSON.parse(buffer.subarray(0,bytesRead).toString());
      if(Object.keys(value).sort().join(',')!=='password,server,username' || value.server!==image.split('/')[0]
        || !/^[a-z0-9.-]+(?::[0-9]{1,5})?$/.test(value.server)
        || typeof value.username!=='string' || !/^[a-zA-Z0-9_-]{1,64}$/.test(value.username)
        || typeof value.password!=='string' || !/^[a-zA-Z0-9_-]{32,256}$/.test(value.password))throw new Error('镜像拉取凭据与镜像仓库不匹配');
      return value;
    }finally{await file.close();}
  };
}
