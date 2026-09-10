import express from "express";
import { fileURLToPath } from "node:url";
import { renderServerNodesPage } from "./serverNodesPage.js";

export function serverNodesRoutes({ store, layout }) {
  const router = express.Router();
  router.get("/server-nodes", (_req, res) => {
    res.set("Cache-Control", "no-store").send(layout({ title: "服务器节点", active: "server-nodes", body: renderServerNodesPage() }));
  });
  router.get("/assets/server-nodes.js", (_req, res) => res.sendFile(fileURLToPath(new URL("./serverNodesClient.js", import.meta.url))));
  router.get("/assets/server-nodes.css", (_req, res) => res.sendFile(fileURLToPath(new URL("./serverNodes.css", import.meta.url))));
  router.get("/api/server-nodes", async (_req, res, next) => {
    try { res.set("Cache-Control", "no-store").json(await store.load()); }
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
  router.get("/api/server-nodes/:id/deletion-check", async (req, res, next) => {
    try { res.set("Cache-Control", "no-store").json(await store.deletionCheck(req.params.id)); }
    catch (error) {
      if (error.statusCode) return res.status(error.statusCode).json({ error: error.message });
      next(error);
    }
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
