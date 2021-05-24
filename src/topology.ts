// TODO: Move the test code to test folder
import { assertEquals } from "../tests/test.deps.ts";
import { parse } from "./utils/uri.ts";
// TODO: Fix import
import { parse as parseYaml } from "https://deno.land/std/encoding/yaml.ts";

// Get the seedlist
// connect and auth to each seed
// send hello
// based on hello responses, update the topology
// Topologies:
// - Single instance
// - ReplicasetWithPrimary
// - ReplicasetWithoutPrimary
// - Mongos (sharded)

function topologyVersionIsStale(
  newVersion: TopologyVersion,
  oldVersion?: TopologyVersion,
) {
  if (!oldVersion) return false;
  if (newVersion.processId.$oid !== oldVersion.processId.$oid) {
    return false;
  }
  if (
    BigInt(newVersion.counter.$numberLong) >
      BigInt(oldVersion.counter.$numberLong)
  ) {
    return false;
  }
  return true;
}

export type ServerType =
  | "Unknown"
  | "Standalone"
  | "Mongos"
  | "PossiblePrimary"
  | "RSPrimary"
  | "RSSecondary"
  | "RSArbiter"
  | "RSOther"
  | "RSGhost"
  | "LoadBalanced";

export type TopologyType =
  | "Single"
  | "ReplicaSetNoPrimary"
  | "ReplicaSetWithPrimary"
  | "Sharded"
  | "Unknown";

interface ServerDescription {
  type: ServerType;
  minWireVersion: number;
  maxWireVersion: number;
  me?: string;
  hosts: string[];
  setName?: string | null;
  primary?: string;
  topologyVersion?: TopologyVersion;
  setVersion?: number | null;
  electionId?: ElectionId | null;
}

interface TopologyVersion {
  processId: { $oid: string };
  counter: { $numberLong: string };
}

interface ElectionId {
  $oid: string;
}

class Topology {
  #minWireVersion = 2; // TODO: What should be the supported range?
  #maxWireVersion = 9; // TODO: What should be the supported range?

  #serverDescriptions: Map<string, ServerDescription> = new Map();
  #type: TopologyType = "Unknown";
  #setName: string | null = null;
  #maxSetVersion?: number;
  #maxElectionId?: ElectionId;

  constructor(
    initialServerDescriptions: { host: string; port: number }[],
    options: Partial<{ replicaSet?: string }>,
  ) {
    initialServerDescriptions.forEach(({ host, port }) => {
      const key = `${host}:${port}`;
      this.addInitialServerDescription(key);
    });
    if (options.replicaSet) {
      this.#setName = options.replicaSet;
      this.#type = "ReplicaSetNoPrimary";
    }
  }

  private addInitialServerDescription(hostAndPort: string) {
    this.#serverDescriptions.set(hostAndPort, {
      type: "Unknown",
      minWireVersion: 0,
      maxWireVersion: 0,
      me: hostAndPort,
      hosts: [],
      setName: null,
    });
  }

  updateServerDescription(
    hostAndPort: string,
    response: IsMasterResponse,
  ) {
    function typeFromResponse(response: IsMasterResponse): ServerType {
      if (response.arbiterOnly === true && !!response.setName) {
        return "RSArbiter";
      }
      if (response.isreplicaset === true) return "RSGhost";
      if (response.ismaster === true && !!response.setName) return "RSPrimary";
      if (
        response.setName &&
        (response.hidden ||
          (!response.ismaster && !response.secondary && !response.arbiterOnly))
      ) {
        return "RSOther";
      }
      if (response.secondary && !!response.setName) return "RSSecondary";

      return "Unknown";
    }

    if (response.ok !== 1) {
      throw new Error(
        `Not implemented yet: ${JSON.stringify(response, null, 2)}`,
      );
    }

    const {
      hosts,
      passives,
      arbiters,
      setName,
      minWireVersion,
      maxWireVersion,
      topologyVersion,
    } = response;

    const oldServerDescription = this.#serverDescriptions.get(hostAndPort);
    const serverDescription: ServerDescription = {
      type: typeFromResponse(response),
      hosts: hosts || [],
      setName,
      minWireVersion,
      maxWireVersion,
    };
    if (response.setVersion) serverDescription.setVersion = response.setVersion;
    if (response.electionId) serverDescription.electionId = response.electionId;
    if (response.electionId && response.setVersion) {
      serverDescription.electionId = response.electionId;
      // Check if response is stale
      if (response.ismaster) {
        if (
          this.#maxElectionId &&
          this.#maxSetVersion &&
          (this.#maxSetVersion > response.setVersion ||
            (this.#maxSetVersion === response.setVersion &&
              BigInt(this.#maxElectionId?.$oid) >
                BigInt(response.electionId.$oid)))
        ) {
          serverDescription.type = "Unknown";
          serverDescription.electionId = null;
          serverDescription.setVersion = null;
          serverDescription.setName = null;
          this.#serverDescriptions.set(hostAndPort, serverDescription);
          return;
        } else {
          this.#maxElectionId = response.electionId;
          this.#maxSetVersion = response.setVersion;
          for (const desc of this.#serverDescriptions.values()) {
            if (
              desc !== serverDescription &&
              serverDescription.type === "RSPrimary"
            ) {
              desc.electionId = null;
              desc.setVersion = null;
              desc.setName = null;
              desc.type = "Unknown";
            }
          }
        }
      }
    }
    if (topologyVersion) {
      if (
        topologyVersionIsStale(
          topologyVersion,
          oldServerDescription?.topologyVersion,
        )
      ) {
        console.warn(`Received stale response...`);
        return;
      } else {
        serverDescription.topologyVersion = topologyVersion;
      }
    }

    // Bail if the server isn't from correct replicaSet and delete the server
    if (
      this.#setName &&
      this.#setName !== serverDescription.setName &&
      serverDescription.type !== "RSGhost"
    ) {
      this.#serverDescriptions.delete(hostAndPort);
      if (
        !Array.from(this.#serverDescriptions.values()).some((server) =>
          server.type === "RSPrimary"
        )
      ) {
        this.#type = "ReplicaSetNoPrimary";
      }
      return;
    }

    // If we discover any new servers add the initial descriptors
    hosts
      ?.filter((hostAndPort) => !this.#serverDescriptions.has(hostAndPort))
      .forEach((hostAndPort) => this.addInitialServerDescription(hostAndPort));
    arbiters
      ?.filter((hostAndPort) => !this.#serverDescriptions.has(hostAndPort))
      .forEach((hostAndPort) => this.addInitialServerDescription(hostAndPort));
    passives
      ?.filter((hostAndPort) => !this.#serverDescriptions.has(hostAndPort))
      .forEach((hostAndPort) => this.addInitialServerDescription(hostAndPort));

    if (
      response.primary
    ) {
      const possiblePrimary = this.#serverDescriptions.get(response.primary);
      if (possiblePrimary?.type === "Unknown") {
        possiblePrimary.type = "PossiblePrimary";
      }
    }

    // Bail if the 'me' field does not match the host and port we queried with.
    // According to spec we still need to use the hosts, arbiters and passives
    // from this response
    if (response.me && response.me !== hostAndPort) {
      this.#serverDescriptions.delete(hostAndPort);
      return;
    }

    this.#serverDescriptions.set(hostAndPort, serverDescription);
    if (serverDescription.type === "RSPrimary") {
      this.#setName = serverDescription.setName!;
      this.#type = "ReplicaSetWithPrimary";
    }
    if (
      this.#type === "Unknown" &&
      serverDescription.type === "RSSecondary"
    ) {
      // Actually can it be missing?
      this.#setName = serverDescription.setName!;
      this.#type = "ReplicaSetNoPrimary";
    }
    if (this.#type === "Unknown" && serverDescription.type === "RSOther") {
      // Actually can it be missing?
      this.#setName = serverDescription.setName!;
      this.#type = "ReplicaSetNoPrimary";
    }
    if (serverDescription.type === "RSGhost") serverDescription.setName = null;
  }

  describe() {
    type TopologyDescription = {
      servers: Record<string, TopologyServerDescription>;
      setName?: string | null;
      topologyType: TopologyType;
      logicalSessionTimeoutMinutes: null;
      compatible?: boolean;
      maxSetVersion?: number;
      maxElectionId?: ElectionId;
    };
    type TopologyServerDescription = {
      setName?: string | null;
      type: ServerType;
      topologyVersion?: TopologyVersion;
      electionId?: ElectionId | null;
      setVersion?: number | null;
    };
    const description: TopologyDescription = {
      servers: Array.from(this.#serverDescriptions).reduce(
        (acc, [hostAndPort, desc]) => {
          const serverDescription: TopologyServerDescription = {
            setName: desc.setName,
            type: desc.type,
          };
          if (desc.topologyVersion) {
            serverDescription.topologyVersion = desc.topologyVersion;
          }
          if (desc.setVersion !== undefined) {
            serverDescription.setVersion = desc.setVersion;
          }
          if (desc.electionId !== undefined) {
            serverDescription.electionId = desc.electionId;
          }
          return {
            ...acc,
            [hostAndPort]: serverDescription,
          };
        },
        {},
      ),
      setName: this.#setName,
      topologyType: this.#type,
      logicalSessionTimeoutMinutes: null,
    };
    if (!this.isCompatible()) description.compatible = false;
    if (this.#maxSetVersion) description.maxSetVersion = this.#maxSetVersion;
    if (this.#maxElectionId) {
      description.maxElectionId = this.#maxElectionId;
    }
    return description;
  }

  isCompatible() {
    const descs = Array.from(this.#serverDescriptions.values());
    if (
      descs.some((desc) =>
        desc.type !== "Unknown" && desc.type !== "PossiblePrimary" &&
        desc.maxWireVersion < this.#minWireVersion
      )
    ) {
      return false;
    }
    if (
      descs.some((desc) =>
        desc.type !== "Unknown" && desc.type !== "PossiblePrimary" &&
        desc.minWireVersion > this.#maxWireVersion
      )
    ) {
      return false;
    }
    return true;
  }
}

interface IsMasterResponse {
  ok: number;
  ismaster: boolean;
  isreplicaset?: boolean;
  secondary?: boolean;
  minWireVersion: number;
  maxWireVersion: number;
  setName?: string;
  hosts?: string[];
  arbiters?: string[];
  passives?: string[];
  hidden?: boolean;
  arbiterOnly?: boolean;
  topologyVersion?: TopologyVersion;
  me?: string;
  primary?: string;
  setVersion?: number;
  electionId?: ElectionId;
}

interface TestSample {
  description: string;
  uri: string;
  phases: Phase[];
}

interface Phase {
  responses: [string, IsMasterResponse][];
  outcome: unknown;
}

function getSpecs(
  dir: string,
  specsToRun: RegExp[] = [],
): TestSample[] {
  const samples = Array.from(Deno.readDirSync(dir)).filter((file) =>
    file.isFile && /.yml$/.test(file.name)
  )
    .map((file) => Deno.readTextFileSync(`${dir}/${file.name}`))
    .map((text) => parseYaml(text) as TestSample);
  console.log(samples.map((sample) => sample.description));

  return samples
    .filter((specDesc) =>
      specsToRun.length === 0 ||
      specsToRun.some((regex) => regex.test(specDesc.description))
    );
}

async function runSpec() {
  const samplesToRun = getSpecs(
    "/Users/matti/work/rebootramen/deno_scrapers/deno_mongo/tests/unified_specs/rs",
    [
      // /secondary/,
      // /Discover primary with directConnection URI option/,
      // /Discover primary with replicaSet URI option/,
      // /Discover ghost with replicaSet URI option/,
      // /Discover/,
      // /Incompatible/,
      // /primary/i,
      // /secondary with mis/i,
      /Primary becomes mongos/,
      /Discover ghost with replicaSet URI option/,
      /Incompatible ghost/,
      /Primary with older topologyVersion/,
      /Received stale response/,
      /Secondary wrong setName with primary/,
      /Secondary with mismatched 'me' tells us who the primary is/,
      /Primary changes setName/,
      /Discover RSOther with directConnection URI option/,
      /Primary wrong setName/,
      /Discover primary with directConnection URI option/,
      /Discover arbiters with replicaSet URI option/,
      /Discover passives with directConnection URI option/,
      /Primary mismatched me/,
      /Discover ghost with directConnection URI option/,
      /New primary with equal electionId/,
      /Primary mismatched me is not removed/,
    ],
  );
  for (const testSample of samplesToRun) {
    console.log(`${testSample.description}...`);
    const options = await parse(testSample.uri);
    const topology = new Topology(options.servers, options);
    testSample.phases.forEach((phase, phaseIndex) => {
      console.log(`\tPhase ${phaseIndex + 1}`);
      phase.responses.forEach((response) => {
        topology.updateServerDescription(
          response[0] as string,
          response[1] as IsMasterResponse,
        );
      });
      try {
        assertEquals(topology.describe(), phase.outcome);
      } catch (error) {
        console.log(JSON.stringify(testSample, null, 2));
        console.log(JSON.stringify(topology.describe(), null, 2));
        throw error;
      }
    });
    console.log("ok");
  }
}

runSpec();
