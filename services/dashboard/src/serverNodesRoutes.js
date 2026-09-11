import express from "express";
import { fileURLToPath } from "node:url";
import { renderServerNodesPage } from "./serverNodesPage.js";
import { connectionDeploymentPreview } from './nodeRuntime/connectionDeployment.js';
import { collectDeploymentPreview } from './nodeRuntime/collectDeployment.js';

export function serverNodesRoutes({ store, layout, onboarding = null, runtime = null, workerDeployment = null, deploymentEnvironment = process.env }) {
  const router = express.Router();
  router.get("/server-nodes", (_req, res) => {
    res.set("Cache-Control", "no-store").send(layout({ title: "服务器节点", active: "server-nodes", body: renderServerNodesPage() }));
  });
  router.get("/assets/server-nodes.js", (_req, res) => res.sendFile(fileURLToPath(new URL("./serverNodesClient.js", import.meta.url))));
  router.get("/assets/server-nodes.css", (_req, res) => res.sendFile(fileURLToPath(new URL("./serverNodes.css", import.meta.url))));
  router.get("/api/server-nodes", async (_req, res, next) => {
    try { res.set("Cache-Control", "no-store").json({ ...await store.load(), capabilities: { onboarding: !!onboarding, runtime: !!runtime, workerDeployment: !!workerDeployment } }); }
    catch (error) { next(error); }
  });
  const save = async (req, res, next) => {
    try {
      if (!req.is("application/json")) return res.status(415).json({ error: "请使用 JSON 格式提交服务器配置" });
      // Node registration changes metadata only. No SSH, Docker or network calls.
      res.set("Cache-Control", "no-store").json(await store.save({ id: req.params.id, version: req.body?.version, node: req.body?.node }));
    } catch (error) {
      if (error.statusCode) return res.status(error.statusCode).json({ error: error.message });
      next(error);
    }
  };
  router.post("/api/server-nodes", save);
  router.put("/api/server-nodes/:id", save);
  router.post("/api/server-nodes/:id/initialize", async (req, res, next) => {
    try {
      if (!req.is("application/json")) return res.status(415).json({ error: "请使用 JSON 提交初始化信息" });
      if (!onboarding) return res.status(503).json({ error: "中心初始化服务尚未配置" });
      const password = req.body?.password ?? "";
      if (req.body) delete req.body.password;
      res.set("Cache-Control", "no-store").status(202).json(await onboarding.start({ id: req.params.id, version: req.body?.version, password }));
    } catch (error) {
      if (error.statusCode) return res.status(error.statusCode).json({ error: error.message });
      // Do not delegate credential-bearing operations to generic error logging.
      res.status(503).json({ error: "初始化服务暂时不可用，请检查中心监控服务后重试" });
    }
  });
  router.get("/api/server-nodes/:id/monitoring", async (req, res) => {
    try {
      const node = (await store.load()).nodes.find(item => item.id === req.params.id);
      if (!node) return res.status(404).json({ error: "服务器不存在" });
      if (!onboarding || !node.provisioning?.systemId) return res.status(409).json({ error: "此节点尚未接入监控" });
      res.set("Cache-Control", "no-store").json(await onboarding.observe(node.provisioning.systemId));
    } catch { res.status(503).json({ error: "暂时无法读取监控数据" }); }
  });
  router.post('/api/server-nodes/:id/prepare-runtime', async (req, res) => {
    try {
      if (!req.is('application/json')) return res.status(415).json({ error: '请使用 JSON 提交环境准备信息' });
      if (!runtime) return res.status(503).json({ error: '中心运行环境准备服务尚未配置' });
      if (Object.keys(req.body ?? {}).some(key => !['version', 'password'].includes(key))) return res.status(400).json({ error: '环境准备不接受自定义脚本或部署参数' });
      const password = req.body?.password ?? '';
      if (req.body) delete req.body.password;
      res.set('Cache-Control', 'no-store').status(202).json(await runtime.start({ id: req.params.id, version: req.body?.version, password }));
    } catch (error) {
      if (error.statusCode) return res.status(error.statusCode).json({ error: error.message });
      res.status(503).json({ error: '环境准备暂时不可用，请检查节点连接后重试' });
    }
  });
  router.get("/api/server-nodes/:id/deletion-check", async (req, res, next) => {
    try { res.set("Cache-Control", "no-store").json(await store.deletionCheck(req.params.id)); }
    catch (error) {
      if (error.statusCode) return res.status(error.statusCode).json({ error: error.message });
      next(error);
    }
  });
  router.get('/api/server-nodes/:id/worker-deployment', async (req, res, next) => {
    try {
      const registry = await store.load();
      const node = registry.nodes.find(item => item.id === req.params.id);
      if (!node) return res.status(404).json({ error: '服务器不存在' });
      res.set('Cache-Control', 'no-store').json({ version: registry.version, ...(deploymentEnvironment.SERVER_NODE_COLLECT_IMAGE
        ?collectDeploymentPreview(node,deploymentEnvironment):connectionDeploymentPreview(node, deploymentEnvironment)) });
    } catch (error) { next(error); }
  });
  router.post('/api/server-nodes/:id/deploy-workers',async(req,res)=>{
    try{
      if(!req.is('application/json'))return res.status(415).json({error:'请使用 JSON 提交部署信息'});
      if(!workerDeployment)return res.status(503).json({error:'中心 Worker 部署服务尚未配置'});
      if(Object.keys(req.body??{}).some(key=>!['version','password'].includes(key)))return res.status(400).json({error:'部署只使用已保存计划，不接受自定义脚本或镜像'});
      const password=req.body?.password??'';if(req.body)delete req.body.password;
      res.set('Cache-Control','no-store').status(202).json(await workerDeployment.start({id:req.params.id,version:req.body?.version,password}));
    }catch(error){res.status(error.statusCode??503).json({error:error.statusCode?error.message:'Worker 部署暂时不可用，请检查中心配置'});}
  });
  router.delete("/api/server-nodes/:id", async (req, res, next) => {
    try {
      if (!req.is("application/json")) return res.status(415).json({ error: "请使用 JSON 格式确认删除" });
      res.set("Cache-Control", "no-store").json(await store.remove({ id: req.params.id, version: req.body?.version }));
    } catch (error) {
      if (error.statusCode) return res.status(error.statusCode).json({ error: error.message });
      next(error);
    }
  });
  return router;
}
