import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";

export interface Recorded {
  method: string;
  path: string;
  headers: IncomingMessage["headers"];
  body: any;
}

export type Handler = (req: Recorded, res: ServerResponse) => void | Promise<void>;

export interface FakeServer {
  url: string;
  requests: Recorded[];
  close(): Promise<void>;
}

/** A local HTTP server; `handler` answers each request after its body is read. */
export async function fakeServer(handler: Handler): Promise<FakeServer> {
  const requests: Recorded[] = [];
  const server = createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on("data", (c: Buffer) => chunks.push(c));
    req.on("end", () => {
      const text = Buffer.concat(chunks).toString("utf8");
      const rec: Recorded = {
        method: req.method ?? "",
        path: req.url ?? "",
        headers: req.headers,
        body: text ? JSON.parse(text) : undefined,
      };
      requests.push(rec);
      void Promise.resolve(handler(rec, res)).catch((err) => {
        res.statusCode = 500;
        res.end(String(err));
      });
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address() as AddressInfo;
  return {
    url: `http://127.0.0.1:${port}`,
    requests,
    close: () =>
      new Promise<void>((resolve) => {
        server.closeAllConnections();
        server.close(() => resolve());
      }),
  };
}

export function json(res: ServerResponse, status: number, body: unknown, headers: Record<string, string> = {}): void {
  res.writeHead(status, { "content-type": "application/json", ...headers });
  res.end(JSON.stringify(body));
}

export function errorBody(code: string, retryable: boolean, extra: Record<string, unknown> = {}) {
  return { error: `${code} happened`, code, retryable, request_id: "req-1", ...extra };
}
