import { runNodeIncremental, checkNodeIncrementalHealth } from '../src/remoteNodes/nodeIncrementalRuntime.js';

if(process.argv[2]==='--healthcheck'){
  try{await checkNodeIncrementalHealth();}catch{process.exitCode=1;}
}else if(process.argv.length!==2){process.exitCode=64;}
else{
  const abort=new AbortController();
  process.once('SIGTERM',()=>abort.abort());process.once('SIGINT',()=>abort.abort());
  try{
    let previous;
    await runNodeIncremental({signal:abort.signal,onStatus(status){
      if(previous!==status.state)console.log(JSON.stringify({event:'remote_node_incremental',...status}));
      previous=status.state;
    }});
  }catch{
    console.error(JSON.stringify({event:'remote_node_incremental_failed'}));process.exitCode=1;
  }
}
