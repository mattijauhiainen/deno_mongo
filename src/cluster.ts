import { ConnectOptions } from "./types.ts";
import { assert } from "../deps.ts";
import { MongoServer, ServerState } from "./mongoServer.ts";

export interface DenoConnectOptions {
  hostname: string;
  port: number;
  certFile?: string;
}

export class Cluster {
  #servers: Map<string, MongoServer> = new Map();
  #options: ConnectOptions;

  constructor(options: ConnectOptions) {
    this.#options = options;
    const serverConfigs = options.servers;
    serverConfigs.forEach((serverConfig) =>
      this.#servers.set(
        `${serverConfig.host}:${serverConfig.port}`,
        new MongoServer(
          serverConfig.host,
          serverConfig.port,
          options,
        ),
      )
    );
  }

  async connect() {
    await Promise.all(
      Array.from(this.#servers.values()).map((server) =>
        server.init((server) => this.onDiscovery(server))
      ),
    );
  }

  async onDiscovery(hostString: string) {
    const key = hostString;
    const host = hostString.split(":")[0];
    const port = Number.parseInt(hostString.split(":")[1]) || 27017;
    if (this.#servers.has(key)) return;
    const discoveredInstance = new MongoServer(
      host,
      port,
      this.#options,
    );
    this.#servers.set(key, discoveredInstance);
    await discoveredInstance.init();
  }

  getMaster() {
    const masterStates: ServerState[] = ["Standalone", "RSPrimary"];
    return Array.from(this.#servers.values()).find((server) =>
      masterStates.includes(server.state)
    );
  }

  get protocol() {
    const protocol = this.getMaster()?.protocol;
    assert(protocol, "No writable primary found");
    return protocol;
  }

  close() {
    this.#servers.forEach((server) => server.close());
  }

  toString() {
    return Array.from(this.#servers.values()).map((server) => server.toString())
      .join("\n");
  }
}
