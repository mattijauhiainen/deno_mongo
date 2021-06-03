import { ConnectOptions } from "./types.ts";
import { assert } from "../deps.ts";
import { MongoServer } from "./mongoServer.ts";
import { Topology } from "./topology.ts";
import { Monitor } from "./monitor.ts";

export interface DenoConnectOptions {
  hostname: string;
  port: number;
  certFile?: string;
}

export class Cluster {
  #monitors: Map<string, Monitor> = new Map();
  #options: ConnectOptions;
  #topology: Topology;

  constructor(options: ConnectOptions) {
    this.#options = options;
    this.#topology = new Topology(
      options.servers.map((s) => ({
        host: s.host.replace(/\.$/, ""), // TODO: Why there is a . at the end of Atlas resolved URIs?
        port: s.port,
      })),
      options,
    );
    this.#topology.servers().forEach(([hostAndPort]) => {
      const host = hostAndPort.split(":")[0];
      const port = Number.parseInt(hostAndPort.split(":")[1]);
      this.#monitors.set(
        hostAndPort,
        new Monitor(
          new MongoServer(host, port, this.#options),
          (response) =>
            this.#topology.updateServerDescription(hostAndPort, response),
        ),
      );
    });
  }

  async connect() {
    await Promise.all(
      Array.from(this.#monitors.values()).map((monitor) => monitor.open()),
    );
  }

  getMaster() {
    const masterHostAndPort = this.#topology.getMaster()?.[0];
    assert(masterHostAndPort, "No writable primary found");
    return this.#monitors.get(masterHostAndPort)?.server;
  }

  get protocol() {
    const protocol = this.getMaster()?.protocol;
    assert(protocol, "No writable primary found");
    return protocol;
  }

  close() {
    for (const [_, monitor] of this.#monitors) {
      monitor.close();
    }
  }

  toString() {
    return Array.from(this.#monitors.values()).map((server) =>
      server.toString()
    )
      .join("\n");
  }
}
