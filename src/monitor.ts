import { MongoServer } from "./mongoServer.ts";
import { IsMasterResponse } from "./topology.ts";
import * as logger from "./logger.ts";

async function timed<T>(op: () => Promise<T>): Promise<[T, number]> {
  const t1 = performance.now();
  const response = await op();
  const t2 = performance.now();
  return [response, t2 - t1];
}

export class Monitor {
  #server: MongoServer;
  #onPollComplete: (response: IsMasterResponse) => void;
  #intervalHandle: number | null = null;
  #rtts: number[] = [];

  constructor(
    server: MongoServer,
    onPollComplete: (response: IsMasterResponse) => void,
  ) {
    this.#server = server;
    this.#onPollComplete = onPollComplete;
  }

  async open() {
    logger.info(`Init ${this.server.hostAndPort}`);
    await this.#server.init();
    logger.info(`Initial poll ${this.server.hostAndPort}`);
    await this.poll({ force: true });
    logger.info(`Starting poller for ${this.server.hostAndPort}`);
    this.#intervalHandle = setInterval(this.poll, 10_000);
  }

  poll = async ({ force } = { force: false }) => {
    const [response, timeInMillis] = await timed<IsMasterResponse>(() =>
      this.#server.updateState()
    );
    if (!force && this.#intervalHandle === null) return; // Not polling anymore, bail

    this.#rtts.unshift(timeInMillis);
    this.#rtts = this.#rtts.slice(0, 10);
    logger.info(
      `Received { ok: ${response
        ?.ok} } from ${this.#server.hostAndPort} in ${
        timeInMillis.toFixed(2)
      }ms (avg: ${this.avgRtt().toFixed(2)}ms)`,
    );
    // TODO: Catch errors and dispatch to topology
    this.#onPollComplete(response);
  };

  avgRtt() {
    return this.#rtts.reduce((acc, item) => acc + item, 0) / this.#rtts.length;
  }

  close() {
    if (this.#intervalHandle) clearInterval(this.#intervalHandle);
    this.#intervalHandle = null;
    this.#server.close();
  }

  get server(): MongoServer {
    return this.#server;
  }

  toString() {
    return this.#server.toString();
  }
}
