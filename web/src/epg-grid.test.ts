import { describe, expect, it } from "vitest";
import type { Program, Service } from "./api";
import {
  makeEpgColumns,
  makeEpgProgramBlocks,
  makeSharedProgramKeyResolver,
  programContentKey,
} from "./shared";

// Characterization tests for the EPG grid logic. These pin the current
// behaviour so the Phase 2 split of shared.tsx stays behaviour-invariant.

const program = (overrides: Partial<Program>): Program => ({
  id: 1,
  eventId: 100,
  serviceId: 10,
  networkId: 1,
  startAt: 0,
  duration: 3600,
  isFree: true,
  ...overrides,
});

// Terrestrial services share an epgServiceGroupKey when their network,
// transport stream and remote control key match, so they land in one group.
const terrestrialService = (overrides: Partial<Service>): Service => ({
  id: 1,
  serviceId: 10,
  networkId: 1,
  transportStreamId: 32736,
  name: "svc",
  type: 1,
  eitScheduleFlag: true,
  epgReady: true,
  remoteControlKeyId: 5,
  channel: { type: "GR", channel: "27" },
  ...overrides,
});

const byService = (...entries: Array<[Service, Program[]]>): Map<string, Program[]> => {
  const map = new Map<string, Program[]>();
  for (const [service, programs] of entries) {
    map.set(`${service.networkId}:${service.serviceId}`, programs);
  }
  return map;
};

describe("programContentKey", () => {
  it("joins timing and normalized text when there is no shared resolver", () => {
    const key = programContentKey(program({ startAt: 1000, duration: 60, name: "  a  b ", description: "x\ny" }));
    expect(key).toBe("1000:60:a b:x y");
  });

  it("prefers a shared key returned by the resolver", () => {
    const key = programContentKey(program({}), () => "shared:1:10:100");
    expect(key).toBe("shared:1:10:100");
  });

  it("falls back to timing text when the resolver returns undefined", () => {
    const key = programContentKey(program({ startAt: 5, duration: 10, name: "n", description: "d" }), () => undefined);
    expect(key).toBe("5:10:n:d");
  });
});

describe("makeSharedProgramKeyResolver", () => {
  it("unions programs linked by shared related items under the smallest endpoint", () => {
    const a = program({
      networkId: 1,
      serviceId: 10,
      eventId: 100,
      relatedItems: [{ type: "shared", serviceId: 20, eventId: 200 }],
    });
    const b = program({ networkId: 1, serviceId: 20, eventId: 200 });
    const resolve = makeSharedProgramKeyResolver([a, b]);
    expect(resolve(a)).toBe("shared:1:10:100");
    expect(resolve(b)).toBe("shared:1:10:100");
  });

  it("merges transitive shared links into a single canonical key", () => {
    const a = program({
      networkId: 1,
      serviceId: 30,
      eventId: 300,
      relatedItems: [{ type: "shared", serviceId: 20, eventId: 200 }],
    });
    const b = program({
      networkId: 1,
      serviceId: 20,
      eventId: 200,
      relatedItems: [{ type: "shared", serviceId: 10, eventId: 100 }],
    });
    const c = program({ networkId: 1, serviceId: 10, eventId: 100 });
    const resolve = makeSharedProgramKeyResolver([a, b, c]);
    expect(resolve(a)).toBe("shared:1:10:100");
    expect(resolve(b)).toBe("shared:1:10:100");
    expect(resolve(c)).toBe("shared:1:10:100");
  });

  it("returns undefined for programs with no shared links", () => {
    const a = program({ networkId: 1, serviceId: 10, eventId: 100 });
    const resolve = makeSharedProgramKeyResolver([a]);
    expect(resolve(a)).toBeUndefined();
  });
});

describe("makeEpgColumns", () => {
  it("returns a single primary column for one service", () => {
    const service = terrestrialService({ id: 1, serviceId: 10 });
    const columns = makeEpgColumns([service], byService([service, [program({ serviceId: 10 })]]));
    expect(columns).toHaveLength(1);
    expect(columns[0].isSubchannel).toBe(false);
    expect(columns[0].primaryService).toBe(service);
    expect(columns[0].services).toEqual([service]);
  });

  it("folds an unstable second service into the primary column", () => {
    const primary = terrestrialService({ id: 1, serviceId: 10 });
    const secondary = terrestrialService({ id: 2, serviceId: 11, epgReady: false });
    const columns = makeEpgColumns(
      [primary, secondary],
      byService(
        [primary, [program({ serviceId: 10, eventId: 100, startAt: 0 })]],
        [secondary, [program({ serviceId: 11, eventId: 200, startAt: 999 })]],
      ),
    );
    expect(columns).toHaveLength(1);
    expect(columns[0].services).toEqual([primary, secondary]);
    expect(columns[0].isSubchannel).toBe(false);
  });

  it("splits a stable subchannel with distinct content into its own column", () => {
    const primary = terrestrialService({ id: 1, serviceId: 10 });
    const secondary = terrestrialService({ id: 2, serviceId: 11 });
    const columns = makeEpgColumns(
      [primary, secondary],
      byService(
        [primary, [program({ serviceId: 10, eventId: 100, startAt: 0, name: "main" })]],
        [secondary, [program({ serviceId: 11, eventId: 200, startAt: 5000, name: "sub" })]],
      ),
    );
    expect(columns).toHaveLength(2);
    expect(columns[1].isSubchannel).toBe(true);
    expect(columns[1].primaryService).toBe(secondary);
  });
});

describe("makeEpgProgramBlocks", () => {
  it("dedupes by content key and sorts by startAt then id", () => {
    const service = terrestrialService({ id: 1, serviceId: 10 });
    const map = byService([
      service,
      [
        program({ id: 3, serviceId: 10, eventId: 3, startAt: 200, name: "c" }),
        program({ id: 1, serviceId: 10, eventId: 1, startAt: 100, name: "a" }),
        program({ id: 2, serviceId: 10, eventId: 2, startAt: 100, name: "a" }),
      ],
    ]);
    const column = makeEpgColumns([service], map)[0];
    const blocks = makeEpgProgramBlocks(column, map);
    expect(blocks.map((block) => block.program.id)).toEqual([1, 3]);
    expect(blocks.map((block) => block.key)).toEqual(["1:1", "1:3"]);
  });
});
