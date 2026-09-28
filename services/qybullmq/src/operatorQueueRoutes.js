import { operatorQueueNames } from './operatorQueuePolicy.js';
// Keep this before Bull Board's generic pause/resume handlers: operator intent
// must be durable before the controller next reconciles the queue.
export function installOperatorQueueRoutes(app, { basePath, control }) {
  for (const name of operatorQueueNames) {
    for (const action of ['pause', 'resume']) {
      app.put(`${basePath.replace(/\/$/, '')}/api/queues/${name}/${action}`, (req, res, next) => {
        control.setOperatorPaused(name, action === 'pause')
          .then(result => res.json(result)).catch(next);
      });
    }
  }
}
