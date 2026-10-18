import { Router } from 'express';
import type { Hub } from '../../dashboard/hub.js';

/** Server-Sent Events: one connection per dashboard, fed by the shared hub. */
export function eventRoutes(hub: Hub): Router {
  const r = Router();

  r.get('/', (req, res) => {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      Connection: 'keep-alive',
      'X-Accel-Buffering': 'no', // stop proxies from buffering events
    });
    const send = (topic: string, data: unknown) => {
      res.write(`event: ${topic}\ndata: ${JSON.stringify(data)}\n\n`);
    };
    const unsubscribe = hub.subscribe(send); // replays the latest snapshots first
    const ping = setInterval(() => res.write(': ping\n\n'), 20_000);
    req.on('close', () => {
      clearInterval(ping);
      unsubscribe();
    });
  });

  return r;
}
