export const statisticsClientScript=String.raw`<script>
(() => {
  const panel=document.querySelector('[data-statistics-url]');
  if(!panel)return;
  const content=panel.querySelector('[data-statistics-content]');
  const note=panel.querySelector('[data-statistics-note]');
  const button=panel.querySelector('button');
  let timer,busy=false,loaded=false;
  async function refresh(){
    if(busy)return;
    clearTimeout(timer);
    if(document.hidden){timer=setTimeout(refresh,30000);return;}
    busy=true;button.disabled=true;
    try {
      const response=await fetch(panel.dataset.statisticsUrl,{headers:{accept:'application/json'},cache:'no-store',signal:AbortSignal.timeout(75000)});
      if(!response.ok)throw Error('statistics unavailable');
      const data=await response.json();
      if(typeof data.html!=='string')throw Error('invalid statistics');
      content.innerHTML=data.html;loaded=true;
      const at=new Date(data.generatedAt).toLocaleString('zh-CN',{timeZone:'Asia/Shanghai',hour12:false});
      note.textContent=(data.stale?'显示上次统计 · ':'统计更新于 ')+at+' 北京时间';
      button.textContent='刷新统计';
      // Keep the existing Clock list refresh while a day's work is running.
      if(data.refreshPage){timer=setTimeout(()=>{if(!document.hidden)window.location.reload();else refresh();},30000);return;}
    }catch{
      if(!loaded)content.textContent='统计暂不可用，列表仍可浏览。';
      note.textContent=loaded?'统计刷新失败，保留上次结果；列表不受影响。':'';
      button.textContent='重试统计';
    }finally{busy=false;button.disabled=false;}
    timer=setTimeout(refresh,30000);
  }
  button.addEventListener('click',refresh);
  window.addEventListener('pagehide',()=>clearTimeout(timer),{once:true});
  refresh();
})();
</script>`;
