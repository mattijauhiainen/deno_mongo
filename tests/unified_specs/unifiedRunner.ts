import { assertEquals, existsSync, parseYaml } from "../test.deps.ts";
import { parse } from "../../src/utils/uri.ts";
import { ObjectId } from "../../src/utils/bson.ts";

import {
  ApplicationError,
  IsMasterResponse,
  Topology,
} from "../../src/topology.ts";

interface TestSample {
  description: string;
  uri: string;
  phases: Phase[];
}

interface Phase {
  description?: string;
  responses: [string, IsMasterResponse][];
  applicationErrors?: ApplicationError[];
  outcome: Record<string, unknown>;
}

function getSpecs(
  dir: string,
  specsToRun: RegExp[] = [],
): TestSample[] {
  if (!existsSync(dir)) {
    throw new Error(`Could not find the test samples in directory '${dir}'`);
  }
  const samples = Array.from(Deno.readDirSync(dir)).filter((file) =>
    file.isFile && /.yml$/.test(file.name)
  )
    .map((file) => Deno.readTextFileSync(`${dir}/${file.name}`))
    .map((text) => parseYaml(text) as TestSample);

  return samples
    .filter((specDesc) =>
      specsToRun.length === 0 ||
      specsToRun.some((regex) => regex.test(specDesc.description))
    );
}

export default function unifiedTests() {
  const specDir = "./tests/unified_specs";
  const rsSamples = getSpecs(`${specDir}/rs`);
  const shardedSamples = getSpecs(`${specDir}/sharded`);
  const singleSamples = getSpecs(`${specDir}/single`);

  const errorsSamples = getSpecs(
    `${specDir}/errors`,
    [
      /Non-stale network error/,
      /Post-4.2 ShutdownInProgress error/,
      /Pre-4.2 NotMasterNoSlaveOk error/,
      // /Stale generation InterruptedAtShutdown error afterHandshakeCompletes/,
      // /Stale generation network error afterHandshakeCompletes/,
      /Post-4.2 InterruptedDueToReplStateChange error/,
      /Non-stale topologyVersion missing NotMasterNoSlaveOk error/,
      // /Stale generation PrimarySteppedDown error/,
      // /Stale generation ShutdownInProgress error/,
      /Pre-4.2 InterruptedAtShutdown error/,
      // /Stale generation InterruptedDueToReplStateChange error beforeHandshakeCompletes/,
      /Post-4.2 PrimarySteppedDown error/,
      /writeErrors field is ignored/,
      /Network timeouts before and after the handshake completes/,
      // /Stale generation ShutdownInProgress error afterHandshakeCompletes/,
      // /Stale generation InterruptedDueToReplStateChange error afterHandshakeCompletes/,
      // /Stale generation NotMaster error beforeHandshakeCompletes/,
      // /Stale generation PrimarySteppedDown error afterHandshakeCompletes/,
      /Post-4.2 NotMaster error/,
      /Non-stale topologyVersion missing InterruptedDueToReplStateChange error/,
      /Non-stale topologyVersion greater NotMasterNoSlaveOk error/,
      // /Stale generation timeout error beforeHandshakeCompletes/,
      // /Stale generation ShutdownInProgress error beforeHandshakeCompletes/,
      // /Stale generation LegacyNotPrimary error beforeHandshakeCompletes/,
      /Post-4.2 NotMasterOrSecondary error/,
      // /Stale generation PrimarySteppedDown error beforeHandshakeCompletes/,
      /Stale topologyVersion NotMasterNoSlaveOk error/,
      /Non-stale topologyVersion proccessId changed InterruptedAtShutdown error/,
      // /Stale generation LegacyNotPrimary error afterHandshakeCompletes/,
      /Non-stale topologyVersion missing InterruptedAtShutdown error/,
      /Non-stale topologyVersion proccessId changed NotMasterOrSecondary error/,
      /Non-stale topologyVersion greater NotMaster error/,
      /Non-stale topologyVersion proccessId changed NotMasterNoSlaveOk error/,
      /Non-stale topologyVersion greater LegacyNotPrimary error/,
      // /Stale generation NotMasterOrSecondary error beforeHandshakeCompletes/,
      /Pre-4.2 InterruptedDueToReplStateChange error/,
      /Non-stale topologyVersion proccessId changed LegacyNotPrimary error/,
      /Non-stale network timeout error/,
      /Non-stale topologyVersion proccessId changed NotMaster error/,
      /Stale topologyVersion NotMasterOrSecondary error/,
      // /Stale generation NotMasterOrSecondary error/,
      /Non-stale topologyVersion proccessId changed InterruptedDueToReplStateChange error/,
      /Non-stale topologyVersion greater InterruptedDueToReplStateChange error/,
      // /Stale generation NotMasterNoSlaveOk error afterHandshakeCompletes/,
      /Stale topologyVersion InterruptedAtShutdown error/,
      /Pre-4.2 LegacyNotPrimary error/,
      /Pre-4.2 PrimarySteppedDown error/,
      /Post-4.2 LegacyNotPrimary error/,
      /Non-stale topologyVersion missing PrimarySteppedDown error/,
      // /Stale generation NotMasterNoSlaveOk error/,
      /Non-stale topologyVersion missing ShutdownInProgress error/,
      // /Stale generation network error beforeHandshakeCompletes/,
      /Pre-4.2 ShutdownInProgress error/,
      /Post-4.2 NotMasterNoSlaveOk error/,
      // /Stale generation NotMaster error afterHandshakeCompletes/,
      /Non-stale topologyVersion missing NotMaster error/,
      /Pre-4.2 NotMasterOrSecondary error/,
      /Non-stale topologyVersion greater InterruptedAtShutdown error/,
      /Non-stale topologyVersion proccessId changed PrimarySteppedDown error/,
      // /Stale generation InterruptedAtShutdown error beforeHandshakeCompletes/,
      // /Stale generation InterruptedDueToReplStateChange error/,
      /Non-stale topologyVersion proccessId changed ShutdownInProgress error/,
      // /Stale generation InterruptedAtShutdown error/,
      // /Stale generation NotMasterOrSecondary error afterHandshakeCompletes/,
      /Do not check errmsg when code exists/,
      /Pre-4.2 NotMaster error/,
      // /Stale generation NotMaster error/,
      /Non-stale topologyVersion greater PrimarySteppedDown error/,
      /Stale topologyVersion ShutdownInProgress error/,
      /Stale topologyVersion InterruptedDueToReplStateChange error/,
      /Stale topologyVersion NotMaster error/,
      /Non-stale topologyVersion greater NotMasterOrSecondary error/,
      /Non-stale topologyVersion missing LegacyNotPrimary error/,
      // /Stale generation NotMasterNoSlaveOk error beforeHandshakeCompletes/,
      /Stale topologyVersion LegacyNotPrimary error/,
      // /Stale generation timeout error afterHandshakeCompletes/,
      /Stale topologyVersion PrimarySteppedDown error/,
      /Post-4.2 InterruptedAtShutdown error/,
      /Non-stale topologyVersion greater ShutdownInProgress error/,
      /Non-stale topologyVersion missing NotMasterOrSecondary error/,
    ],
  );

  const samplesToRun = [
    ...rsSamples,
    ...shardedSamples,
    ...singleSamples,
    ...errorsSamples,
  ];

  for (const testSample of samplesToRun) {
    Deno.test({
      name: `UNIFIED: ${testSample.description}`,
      async fn() {
        const options = await parse(testSample.uri);
        const topology = new Topology(options.servers, options);
        const ranPhases: string[] = [];
        try {
          testSample.phases.forEach((phase, phaseIndex) => {
            ranPhases.push(phase.description || `Phase ${phaseIndex}`);
            if (phase.responses) {
              for (const response of phase.responses) {
                topology.updateServerDescription(
                  response[0] as string,
                  parseObjects(response[1]),
                );
              }
            } else if (phase.applicationErrors) {
              for (const error of phase.applicationErrors) {
                if (error.response) {
                  error.response = parseObjects(error.response);
                }
                topology.handleError(error);
              }
            }
            const expected = parseObjects(phase.outcome);
            if (expected?.maxElectionId?.$oid) {
              expected.maxElectionId = ObjectId(expected.maxElectionId.$oid);
            }
            for (const server of Object.values(expected.servers)) {
              const s = parseObjects(server);
              delete s.pool;
            }
            const actual = {};
            const pathsToCheck = getPaths(phase.outcome).filter((path) =>
              !/.*pool.generation$/.test(path)
            );
            pathsToCheck.forEach((path) =>
              pick(topology.describe(), actual, path)
            );

            assertEquals(actual, expected);
          });
        } catch (error) {
          const dbgMessage = `Test sample:
${JSON.stringify(testSample, null, 2)}

Failed sample "${testSample.description}"
  Phases:
  ${ranPhases.slice(-1).join(" ...ok \n")}
  ${ranPhases[ranPhases.length - 1]} FAILED
`;
          error.message = dbgMessage + error.message;
          throw error;
        }
      },
    });
  }
}

function pick(
  src: Record<string, unknown>,
  dest: Record<string, unknown>,
  path: string,
): Record<string, unknown> {
  const segments = path.split(".");
  for (const segment of segments) {
    const value = src[segment];
    if (value !== null && !Array.isArray(value) && typeof value === "object") {
      dest[segment] = dest[segment] || {};
      dest = dest[segment] as Record<string, unknown>;
      src = src[segment] as Record<string, unknown>;
    } else {
      dest[segment] = value;
    }
  }
  return dest;
}

function getPaths(
  obj: Record<string, unknown>,
  paths: string[] = [],
  basePath = "",
): string[] {
  const entries = Object.entries(obj);
  if (entries.length === 0) {
    return [...paths, basePath];
  }
  entries.forEach(([key, value]: [string, unknown]) => {
    const path = basePath ? `${basePath}.${key}` : key;
    if (Array.isArray(value)) {
      // TODO: Might need a way to map object arrays and nested arrays
      paths.push(path);
    } else if (value === null) {
      paths.push(path);
    } else if (typeof value === "object") {
      const nested = getPaths(value as Record<string, unknown>, paths, path);
      paths = [...paths, ...nested];
    } else {
      paths.push(path);
    }
  });
  return paths;
}

// TODO: Very naughty
function parseObjects(
  object: any,
): any {
  if (object?.topologyVersion?.processId.$oid) {
    object.topologyVersion.processId = ObjectId(
      object.topologyVersion.processId.$oid,
    );
  }
  if (object?.topologyVersion?.counter?.$numberLong) {
    object.topologyVersion.counter = Number.parseInt(
      object.topologyVersion.counter.$numberLong,
    );
  }
  if (object?.electionId?.$oid) {
    object.electionId = ObjectId(object.electionId.$oid);
  }
  return object;
}
