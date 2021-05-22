import { ConnectOptions, Server } from "./types.ts";

// Get the seedlist
// connect and auth to each seed
// send hello
// based on hello responses, update the topology
// Topologies:
// - Single instance
// - ReplicasetWithPrimary
// - ReplicasetWithoutPrimary
// - Mongos (sharded)
// - Others

class Topology {
  #servers: MongoServer[];
  #options: ConnectOptions;

  constructor(seedlist: Server[], options: ConnectOptions) {
    this.#options = options;
    this.#servers = seedlist.map((serverDesc) => {
      return new MongoServer(
        serverDesc.host,
        serverDesc.port,
      );
    });
  }

  connect(): Promise<void[]> {
    return Promise.all(this.#servers.map((server) => {
      return server.connect(this.#options);
    }));
  }
}
