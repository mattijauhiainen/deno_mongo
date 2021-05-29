function unknownDefault(): ServerDescription {
  return {
    type: "Unknown",
    electionId: null,
    setVersion: null,
    setName: null,
    minWireVersion: 0,
    maxWireVersion: 0,
    hosts: [],
    arbiters: [],
    passives: [],
    topologyVersion: null,
    logicalSessionTimeoutMinutes: null,
    me: null,
  };
}

function normaliseResponse(response: IsMasterResponse) {
  return {
    ...response,
    hosts: response.hosts?.map((host) => host.toLowerCase()) || [],
    arbiters: response.arbiters?.map((host) => host.toLowerCase()) || [],
    passives: response.passives?.map((host) => host.toLowerCase()) || [],
    primary: response.primary?.toLowerCase(),
    setName: response.setName?.toLowerCase(),
    me: response.me?.toLowerCase(),
  };
}

function topologyVersionIsStale(
  newVersion: TopologyVersion,
  oldVersion?: TopologyVersion | null,
) {
  if (!oldVersion) return false;
  if (newVersion.processId.$oid !== oldVersion.processId.$oid) {
    return false;
  }
  if (
    BigInt(newVersion.counter.$numberLong) >=
      BigInt(oldVersion.counter.$numberLong)
  ) {
    return false;
  }
  return true;
}

function typeFromResponse(response: IsMasterResponse): ServerType {
  if (response.arbiterOnly === true && !!response.setName) {
    return "RSArbiter";
  }
  if (
    response.msg !== "isdbgrid" &&
    !response.setName &&
    response.isreplicaset !== true
  ) {
    return "Standalone";
  }
  if (response.isreplicaset === true) return "RSGhost";
  if (
    (response.ismaster === true || response.isWritablePrimary === true) &&
    !!response.setName
  ) {
    return "RSPrimary";
  }
  if (
    response.setName &&
    (response.hidden ||
      (!response.ismaster && !response.secondary && !response.arbiterOnly))
  ) {
    return "RSOther";
  }
  if (response.secondary && !!response.setName) return "RSSecondary";
  if (response.msg === "isdbgrid") return "Mongos";

  return "Unknown";
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

export interface IsMasterResponse {
  ok: number;
  ismaster: boolean;
  minWireVersion: number;
  maxWireVersion: number;

  isWritablePrimary?: boolean;
  isreplicaset?: boolean;
  secondary?: boolean;
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
  msg?: string;
  logicalSessionTimeoutMinutes?: number | null;
}

interface ServerDescription {
  type: ServerType;
  minWireVersion: number;
  maxWireVersion: number;
  hosts: string[];
  arbiters: string[];
  passives: string[];

  logicalSessionTimeoutMinutes: number | null;
  topologyVersion: TopologyVersion | null;
  setName: string | null;
  setVersion: number | null;
  electionId: ElectionId | null;

  me: string | null;
  primary?: string;
}

interface TopologyVersion {
  processId: { $oid: string };
  counter: { $numberLong: string };
}

interface ElectionId {
  $oid: string;
}

export class Topology {
  #minWireVersion = 2; // TODO: What should be the supported range?
  #maxWireVersion = 9; // TODO: What should be the supported range?

  #seeds: string[];
  #serverDescriptions: Map<string, ServerDescription> = new Map();
  #type: TopologyType = "Unknown";
  #setName: string | null = null;
  #maxSetVersion: number | null = null;
  #maxElectionId: ElectionId | null = null;
  #logicalSessionTimeoutMinutes: number | null = null;

  constructor(
    initialServerDescriptions: { host: string; port: number }[],
    options: Partial<{ replicaSet?: string }>,
  ) {
    this.#seeds = initialServerDescriptions.map(({ host, port }) =>
      `${host}:${port}`
    );
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
    hostAndPort = hostAndPort.toLowerCase();
    this.#serverDescriptions.set(hostAndPort, unknownDefault());
  }

  updateServerDescription(
    hostAndPort: string,
    response: IsMasterResponse,
  ) {
    hostAndPort = hostAndPort.toLowerCase();
    response = normaliseResponse(response);

    if (response.ok !== 1) {
      this.addInitialServerDescription(hostAndPort);
      this.checkIfHasPrimary();
      return;
    }

    const props: ServerDescription = {
      type: typeFromResponse(response),
      logicalSessionTimeoutMinutes: response.logicalSessionTimeoutMinutes ??
        null,
      topologyVersion: response.topologyVersion ?? null,
      setName: response.setName ?? null,
      electionId: response.electionId ?? null,
      setVersion: response.setVersion ?? null,
      minWireVersion: response.minWireVersion ?? 0,
      maxWireVersion: response.maxWireVersion ?? 0,
      hosts: response.hosts ?? [],
      arbiters: response.arbiters ?? [],
      passives: response.passives ?? [],
      me: response.me ?? null,
    };
    if (props.topologyVersion) {
      if (
        topologyVersionIsStale(
          props.topologyVersion,
          this.#serverDescriptions.get(hostAndPort)?.topologyVersion,
        )
      ) {
        console.warn(`Received stale response...`);
        return;
      }
    }

    switch (props.type) {
      case "RSPrimary":
        this.updateRSFromPrimary(hostAndPort, props);
        this.updateLogicalSessionTimeoutMinutes();
        break;
      case "RSArbiter":
      case "RSOther":
      case "RSSecondary":
        if (this.#type === "ReplicaSetWithPrimary") {
          this.updateRSWithPrimaryFromMember(hostAndPort, response, props);
        }
        if (this.#type === "ReplicaSetNoPrimary" || this.#type === "Unknown") {
          this.updateRSWithoutPrimary(hostAndPort, response, props);
        }
        this.updateLogicalSessionTimeoutMinutes();
        break;
      case "Mongos":
        this.#serverDescriptions.delete(hostAndPort);
        this.checkIfHasPrimary();
        this.updateLogicalSessionTimeoutMinutes();
        break;
      case "RSGhost":
        this.#serverDescriptions.set(hostAndPort, props);
        if (this.#type === "ReplicaSetWithPrimary") this.checkIfHasPrimary();
        break;
      case "Standalone":
        if (this.#type === "Unknown") {
          if (!this.#serverDescriptions.has(hostAndPort)) return;
          if (this.#seeds.length === 1) {
            this.#type = "Single";
            // TODO: Should add the desc?
          } else {
            this.#serverDescriptions.delete(hostAndPort);
          }
        } else if (this.#type === "ReplicaSetWithPrimary") {
          this.#serverDescriptions.delete(hostAndPort);
          this.checkIfHasPrimary();
        } else {
          this.#serverDescriptions.delete(hostAndPort);
        }
        break;
      case "Unknown":
        this.#serverDescriptions.delete(hostAndPort);
        this.checkIfHasPrimary();
        return;
      default:
        throw new Error(
          `Not handling implemented for response ${typeFromResponse(response)}`,
        );
    }
  }

  updateRSFromPrimary(
    hostAndPort: string,
    props: ServerDescription,
  ) {
    if (!this.#serverDescriptions.has(hostAndPort)) {
      console.warn(
        `Ignoring response for server which is no longer monitored (${hostAndPort})`,
      );
      return;
    }

    if (this.#setName === null) {
      this.#setName = props.setName!;
    } else if (this.#setName !== props.setName) {
      this.#serverDescriptions.delete(hostAndPort);
      this.checkIfHasPrimary();
      return;
    }

    if (props.setVersion && props.electionId) {
      if (
        this.#maxElectionId &&
        this.#maxSetVersion &&
        (this.#maxSetVersion > props.setVersion ||
          (this.#maxSetVersion === props.setVersion &&
            BigInt(this.#maxElectionId.$oid) >
              BigInt(props.electionId.$oid)))
      ) {
        this.#serverDescriptions.set(hostAndPort, unknownDefault());
        this.checkIfHasPrimary();
        return;
      }

      this.#maxElectionId = props.electionId;
    }
    if (
      props.setVersion &&
      (
        this.#maxSetVersion === null ||
        props.setVersion > this.#maxSetVersion
      )
    ) {
      this.#maxSetVersion = props.setVersion;
    }

    for (const peerHostAndPort of this.#serverDescriptions.keys()) {
      const peer = this.#serverDescriptions.get(peerHostAndPort);
      if (hostAndPort !== peerHostAndPort && peer?.type === "RSPrimary") {
        this.#serverDescriptions.set(peerHostAndPort, unknownDefault());
        // TODO:
        // # See note below about invalidating an old primary.
        // replace the server with a default ServerDescription of type "Unknown"
      }
    }

    // Only add if the 'me' matches
    if (
      !props.me ||
      hostAndPort === props.me ||
      // TODO: This propbably isn't right?
      !props.hosts?.includes(props.me)
    ) {
      this.#serverDescriptions.set(hostAndPort, props);
    }

    const peers = [
      ...props.hosts,
      ...props.arbiters,
      ...props.passives,
    ];

    peers
      .filter((hostAndPort) => !this.#serverDescriptions.has(hostAndPort))
      .forEach((hostAndPort) => this.addInitialServerDescription(hostAndPort));

    // Do this after we've added the primary. If the primary is not on it's
    // own hosts list we need to remove it
    Array.from(this.#serverDescriptions.keys()).forEach((hostAndPort) => {
      if (!peers.includes(hostAndPort)) {
        this.#serverDescriptions.delete(hostAndPort);
      }
    });

    this.checkIfHasPrimary();
  }

  updateRSWithPrimaryFromMember(
    hostAndPort: string,
    response: IsMasterResponse,
    props: ServerDescription,
  ) {
    if (!this.#serverDescriptions.has(hostAndPort)) {
      // While this request was on flight, another server has already
      // reported this server not to be part of the replica set. Remove
      // and bail
      // TODO: Should remove if it was added?
      // TODO: Should checkIfHasPrimary?
      return;
    }

    if (this.#setName !== props.setName) {
      this.#serverDescriptions.delete(hostAndPort);
      this.checkIfHasPrimary();
      return;
    }

    // TODO: Is this correct? Specs often omit "me"
    if (props.me && hostAndPort !== props.me) {
      this.#serverDescriptions.delete(hostAndPort);
      this.checkIfHasPrimary();
      return;
    }

    this.#serverDescriptions.set(hostAndPort, props);

    if (
      !Array.from(this.#serverDescriptions.values()).some((desc) =>
        desc.type === "RSPrimary"
      )
    ) {
      this.#type = "ReplicaSetNoPrimary";
      if (response.primary) {
        const possiblePrimary = this.#serverDescriptions.get(response.primary);
        if (possiblePrimary?.type === "Unknown") {
          possiblePrimary.type = "PossiblePrimary";
        }
      }
    }
  }

  updateRSWithoutPrimary(
    hostAndPort: string,
    response: IsMasterResponse,
    props: ServerDescription,
  ) {
    if (!this.#serverDescriptions.has(hostAndPort)) {
      // While this request was in flight, another server has already
      // reported this server not being part of the replica set
      // TODO: Should delete
      return;
    }

    if (!this.#setName) {
      this.#setName = props.setName;
    } else if (this.#setName !== props.setName) {
      this.#serverDescriptions.delete(hostAndPort);
      return;
    }

    this.#serverDescriptions.set(hostAndPort, props);

    const peers = [
      ...props.hosts,
      ...props.arbiters,
      ...props.passives,
    ];

    peers
      .filter((hostAndPort) => !this.#serverDescriptions.has(hostAndPort))
      .forEach((hostAndPort) => this.addInitialServerDescription(hostAndPort));

    if (
      !Array.from(this.#serverDescriptions.values()).some((desc) =>
        desc.type === "RSPrimary"
      )
    ) {
      this.#type = "ReplicaSetNoPrimary";
      if (response.primary) {
        const possiblePrimary = this.#serverDescriptions.get(response.primary);
        if (possiblePrimary?.type === "Unknown") {
          possiblePrimary.type = "PossiblePrimary";
        }
      }
    }

    // TODO: Is this right? Atleast RSOther doesn't have "me" prop
    // but it needs to be kept in set
    if (props.me && hostAndPort !== props.me) {
      this.#serverDescriptions.delete(hostAndPort);
      return;
    }
  }

  checkIfHasPrimary() {
    // const rsTypes: TopologyType[] = [
    //   "ReplicaSetWithPrimary",
    //   "ReplicaSetNoPrimary",
    // ];
    // if (!rsTypes.includes(this.#type)) {
    //   throw new Error(
    //     "Tried to update replica set primary status for non-replicaset topology",
    //   );
    // }
    for (const description of this.#serverDescriptions.values()) {
      if (description.type === "RSPrimary") {
        this.#type = "ReplicaSetWithPrimary";
        return;
      }
    }
    this.#type = "ReplicaSetNoPrimary";
  }

  private updateLogicalSessionTimeoutMinutes() {
    const descs = Array.from(this.#serverDescriptions.values()).filter(
      (desc) =>
        desc.type === "RSPrimary" ||
        desc.type === "RSSecondary" ||
        desc.type === "Mongos" ||
        desc.type === "Standalone",
    );
    const timeouts = descs.reduce<number[]>(
      (acc, { logicalSessionTimeoutMinutes }) =>
        logicalSessionTimeoutMinutes !== null
          ? [...acc, logicalSessionTimeoutMinutes]
          : acc,
      [],
    );
    if (timeouts.length === 0 || timeouts.length < descs.length) {
      this.#logicalSessionTimeoutMinutes = null;
      return;
    } else {
      this.#logicalSessionTimeoutMinutes = Math.min(...timeouts);
    }
  }

  isCompatible() {
    return Array
      .from(this.#serverDescriptions.values())
      .filter((desc) =>
        desc.type !== "Unknown" && desc.type !== "PossiblePrimary"
      )
      .every((desc) => {
        return desc.maxWireVersion >= this.#minWireVersion &&
          desc.minWireVersion <= this.#maxWireVersion;
      });
  }

  describe() {
    type TopologyDescription = {
      servers: Record<string, ServerDescription>;

      setName: string | null;
      topologyType: TopologyType;
      logicalSessionTimeoutMinutes: number | null;
      compatible: boolean;
      maxSetVersion: number | null;
      maxElectionId: ElectionId | null;
    };

    const description: TopologyDescription = {
      servers: Array.from(this.#serverDescriptions).reduce(
        (acc, [hostAndPort, desc]) => {
          return {
            ...acc,
            [hostAndPort]: {
              setName: desc.setName,
              type: desc.type,
              topologyVersion: desc.topologyVersion,
              setVersion: desc.setVersion,
              electionId: desc.electionId,
            },
          };
        },
        {},
      ),
      setName: this.#setName,
      topologyType: this.#type,
      logicalSessionTimeoutMinutes: this.#logicalSessionTimeoutMinutes,
      maxSetVersion: this.#maxSetVersion,
      compatible: this.isCompatible(),
      maxElectionId: this.#maxElectionId,
    };
    return description;
  }
}
