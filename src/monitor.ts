import { MongoServer } from "./mongoServer.ts";
import { IsMasterResponse } from "./topology.ts";
import * as logger from "./logger.ts";

export class Monitor {
  #server: MongoServer;
  #onPollComplete: (response: IsMasterResponse) => void;
  #intervalHandle: number | null = null;

  constructor(
    server: MongoServer,
    onPollComplete: (response: IsMasterResponse) => void,
  ) {
    this.#server = server;
    this.#onPollComplete = onPollComplete;
  }

  async open() {
    await this.#server.init();
    await this.poll();
    this.#intervalHandle = setInterval(this.poll, 10_000);
  }

  poll = async () => {
    // TODO: Record RTT and maintain average
    const response = await this.#server.updateState();
    if (this.#intervalHandle === null) return; // Not polling anymore, bail

    logger.info(
      `Received { ok: ${response
        ?.ok} } from ${this.#server.hostAndPort}`,
    );
    this.#onPollComplete(response);
  };

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
