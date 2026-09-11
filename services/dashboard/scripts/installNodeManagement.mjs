import {readFile,writeFile} from 'node:fs/promises';
const path='/app/src/server.js';
let text=await readFile(path,'utf8');
const original='app.use(serverNodesRoutes({ store: serverNodeStore, layout, onboarding: nodeOnboardingFromEnv(serverNodeStore) }));';
const replacement='app.use(serverNodesRoutes({ store: serverNodeStore, layout, onboarding: nodeOnboardingFromEnv(serverNodeStore), runtime: nodeRuntimeFromEnv(serverNodeStore), workerDeployment: workerDeploymentFromEnv(serverNodeStore) }));';
if(text.split(original).length!==2)throw new Error('Dashboard node route differs from reviewed base image');
if(text.includes('workerDeploymentFromEnv')||text.includes('nodeRuntimeFromEnv'))throw new Error('Dashboard node runtime already installed');
text="import { nodeRuntimeFromEnv } from './serverNodeRuntime.js';\nimport { workerDeploymentFromEnv } from './serverNodeWorkerDeployment.js';\n"+text.replace(original,replacement);
await writeFile(path,text);
