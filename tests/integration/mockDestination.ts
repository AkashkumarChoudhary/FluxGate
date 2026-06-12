import { createServer, type Server } from 'node:http';

export interface ReceivedCall {
  method: string;
  url: string;
  body: string;
}

export interface MockDestination {
  port: number;
  received: ReceivedCall[];
  /** Next N responses use this status; then falls back to 200. */
  failNext(status: number, times: number): void;
  close(): Promise<void>;
}

export async function startMockDestination(port = 9999): Promise<MockDestination> {
  const received: ReceivedCall[] = [];
  let failStatus = 0;
  let failRemaining = 0;

  const server: Server = createServer((req, res) => {
    let body = '';
    req.on('data', (c) => (body += c));
    req.on('end', () => {
      received.push({ method: req.method!, url: req.url!, body });
      if (failRemaining > 0) {
        failRemaining--;
        res.writeHead(failStatus).end('forced failure');
      } else {
        res.writeHead(200, { 'content-type': 'application/json' }).end('{"ok":true}');
      }
    });
  });
  await new Promise<void>((resolve) => server.listen(port, resolve));
  return {
    port,
    received,
    failNext(status, times) {
      failStatus = status;
      failRemaining = times;
    },
    close: () => new Promise((resolve) => server.close(() => resolve())),
  };
}
