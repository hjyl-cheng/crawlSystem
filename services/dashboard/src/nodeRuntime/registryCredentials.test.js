import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,writeFile,chmod,symlink,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {registryCredentialsFromEnv} from './registryCredentials.js';
test('registry credentials stay deployment-only and reject host mismatch, public permissions and symlinks',async t=>{
 const root=await mkdtemp(join(tmpdir(),'registry-auth-test-'));t.after(()=>rm(root,{recursive:true,force:true}));
 const path=join(root,'credential');const value={server:'registry.example',username:'node-pull',password:'a'.repeat(64)};
 await writeFile(path,JSON.stringify(value),{mode:0o600});
 const load=registryCredentialsFromEnv({SERVER_NODE_REGISTRY_CREDENTIALS_FILE:path});
 assert.deepEqual(await load('registry.example/collect@sha256:'+'a'.repeat(64)),value);
 await assert.rejects(()=>load('different.example/collect@sha256:'+'a'.repeat(64)));
 await chmod(path,0o644);await assert.rejects(()=>load('registry.example/collect'));await chmod(path,0o600);
 await symlink(path,join(root,'link'));await assert.rejects(()=>registryCredentialsFromEnv({SERVER_NODE_REGISTRY_CREDENTIALS_FILE:join(root,'link')})('registry.example/collect'));
 assert.equal(await registryCredentialsFromEnv({})('registry.example/collect'),null);
});
