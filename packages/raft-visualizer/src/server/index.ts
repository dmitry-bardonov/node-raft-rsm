import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { readFile } from 'node:fs/promises';
import { extname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import type { ClusterSnapshot, SimulationAction } from '../shared/protocol.js';
import { SimulationController } from './simulation-controller.js';

const host = '127.0.0.1';
const port = 3_000;
const publicDirectory = fileURLToPath(new URL('../../public/', import.meta.url));
let simulation = await SimulationController.create();
let operation: Promise<void> = Promise.resolve();
const eventClients = new Set<ServerResponse>();
let unsubscribeSimulation = simulation.subscribe(broadcast);

const server = createServer((request, response) => {
  void handleRequest(request, response).catch((error: unknown) => {
    sendJson(response, 400, { error: asError(error).message });
  });
});

server.listen(port, host, () => {
  console.log(`Raft visualizer running at http://${host}:${port.toString()}`);
});

for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.once(signal, () => {
    void shutdown();
  });
}

async function handleApi(request: IncomingMessage, response: ServerResponse): Promise<void> {
  const url = new URL(request.url ?? '/', `http://${host}:${port.toString()}`);
  if (request.method === 'GET' && url.pathname === '/api/snapshot') {
    sendJson(response, 200, simulation.snapshot());
    return;
  }
  if (request.method === 'GET' && url.pathname === '/api/events') {
    openEventStream(request, response);
    return;
  }
  if (request.method === 'POST' && url.pathname === '/api/action') {
    const action = validateAction(await readJson(request));
    await enqueue(async () => {
      if (action.type === 'reset') {
        unsubscribeSimulation();
        await simulation.stop();
        simulation = await SimulationController.create(action.nodeCount, action.seed);
        unsubscribeSimulation = simulation.subscribe(broadcast);
      } else {
        await simulation.execute(action);
      }
    });
    sendJson(response, 200, simulation.snapshot());
    return;
  }
  sendJson(response, 404, { error: 'API route not found' });
}

async function handleRequest(request: IncomingMessage, response: ServerResponse): Promise<void> {
  if (request.url?.startsWith('/api/') === true) {
    await handleApi(request, response);
    return;
  }
  if (request.method !== 'GET' && request.method !== 'HEAD') {
    sendJson(response, 405, { error: 'method not allowed' });
    return;
  }
  const url = new URL(request.url ?? '/', `http://${host}:${port.toString()}`);
  const relativePath = url.pathname === '/' ? 'index.html' : url.pathname.slice(1);
  if (relativePath.includes('..')) throw new Error('invalid asset path');
  let asset: Buffer;
  let servedPath = relativePath;
  try {
    asset = await readFile(join(publicDirectory, servedPath));
  } catch {
    servedPath = 'index.html';
    asset = await readFile(join(publicDirectory, servedPath));
  }
  response.writeHead(200, { 'Content-Type': contentType(servedPath) });
  response.end(request.method === 'HEAD' ? undefined : asset);
}

function openEventStream(request: IncomingMessage, response: ServerResponse): void {
  response.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache, no-transform',
    Connection: 'keep-alive',
  });
  eventClients.add(response);
  writeEvent(response, simulation.snapshot());
  const keepAlive = setInterval(() => response.write(': keep-alive\n\n'), 15_000);
  keepAlive.unref();
  request.once('close', () => {
    clearInterval(keepAlive);
    eventClients.delete(response);
  });
}

function broadcast(snapshot: ClusterSnapshot): void {
  for (const response of eventClients) writeEvent(response, snapshot);
}

function writeEvent(response: ServerResponse, snapshot: ClusterSnapshot): void {
  response.write(`data: ${JSON.stringify(snapshot)}\n\n`);
}

function enqueue(task: () => Promise<void>): Promise<void> {
  const result = operation.then(task);
  operation = result.catch(() => undefined);
  return result;
}

async function readJson(request: IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = [];
  let bytes = 0;
  for await (const chunk of request) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as Uint8Array);
    bytes += buffer.length;
    if (bytes > 64 * 1024) throw new Error('request body is too large');
    chunks.push(buffer);
  }
  try {
    return JSON.parse(Buffer.concat(chunks).toString('utf8')) as unknown;
  } catch {
    throw new Error('request body must be valid JSON');
  }
}

function validateAction(value: unknown): SimulationAction {
  if (typeof value !== 'object' || value === null || !('type' in value))
    throw new Error('action must be an object with a type');
  return value as SimulationAction;
}

function sendJson(response: ServerResponse, status: number, body: unknown): void {
  if (response.headersSent) return;
  response.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8' });
  response.end(JSON.stringify(body));
}

async function shutdown(): Promise<void> {
  unsubscribeSimulation();
  for (const response of eventClients) response.end();
  eventClients.clear();
  await Promise.all([
    new Promise<void>((resolve) => {
      server.close(() => {
        resolve();
      });
    }),
    simulation.stop(),
  ]);
  process.exitCode = 0;
}

function contentType(path: string): string {
  switch (extname(path)) {
    case '.html':
      return 'text/html; charset=utf-8';
    case '.js':
      return 'text/javascript; charset=utf-8';
    case '.css':
      return 'text/css; charset=utf-8';
    case '.svg':
      return 'image/svg+xml';
    default:
      return 'application/octet-stream';
  }
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}
