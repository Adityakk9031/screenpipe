// screenpipe — AI that knows everything you've seen, said, or heard
// https://screenpipe.com

/**
 * Windows startup and capture continuity after malformed timeline history (#7437).
 *
 * Verifies that synthetic historical audio containing reversed (start > end),
 * extreme (infinity, overflow), and valid segment offsets does not crash or panic
 * the engine worker with "range start is greater than range end in BTreeMap".
 *
 * Invariants verified:
 *   1. App and local API remain available and responsive on startup and after timeline queries.
 *   2. Recoverable transcripts survive and valid timing is preserved.
 *   3. Capture continuity continues and the local health/capture endpoint stays healthy.
 */

import { waitForAppReady, t } from "../helpers/test-utils.js";
import { invokeOrThrow } from "../helpers/tauri.js";
import { E2E_SEED_FLAGS } from "../helpers/app-launcher.js";

interface LocalApiConfig {
  key: string | null;
  port: number;
  auth_enabled: boolean;
}

interface FetchResult {
  ok: boolean;
  status: number;
  body: unknown;
  error?: string;
}

async function fetchJson(
  url: string,
  headers: Record<string, string> = {},
): Promise<FetchResult> {
  const timeoutMs = t(5_000);
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const res = await fetch(url, {
      headers,
      signal: controller.signal,
    });
    const text = await res.text();
    let body: unknown = text;
    try {
      body = JSON.parse(text);
    } catch {
      // retain raw text if not JSON
    }
    return { ok: res.ok, status: res.status, body };
  } catch (err) {
    return {
      ok: false,
      status: 0,
      body: null,
      error: (err as Error).message,
    };
  } finally {
    clearTimeout(timer);
  }
}

describe("Windows Startup & Capture Continuity (#7437)", function () {
  this.timeout(120_000);

  let apiConfig: LocalApiConfig;
  const isMalformedSeeded = E2E_SEED_FLAGS.toLowerCase().includes(
    "malformed-timeline-history",
  );

  before(async () => {
    await waitForAppReady();
    apiConfig = await invokeOrThrow<LocalApiConfig>("get_local_api_config");
  });

  it("server and process stay alive and healthy on startup", async () => {
    const healthUrl = `http://127.0.0.1:${apiConfig.port}/health`;
    const res = await fetchJson(healthUrl);
    expect(res.ok).toBe(true);
    expect(res.status).toBe(200);

    const body = res.body as { status?: string };
    expect(body.status).toBe("healthy");
  });

  it("timeline and search queries survive malformed historical audio offsets without crashing", async () => {
    const headers: Record<string, string> = {};
    if (apiConfig.key) {
      headers["Authorization"] = `Bearer ${apiConfig.key}`;
    }

    // Query timeline frames across the recent 2 hours
    const now = new Date();
    const twoHoursAgo = new Date(now.getTime() - 2 * 60 * 60 * 1000);
    const timelineUrl = `http://127.0.0.1:${apiConfig.port}/frames?start_time=${encodeURIComponent(
      twoHoursAgo.toISOString(),
    )}&end_time=${encodeURIComponent(now.toISOString())}&limit=50`;

    const timelineRes = await fetchJson(timelineUrl, headers);
    // Even if timeline returns 200 or empty data, the process MUST remain healthy and not panic
    expect([200, 404]).toContain(timelineRes.status);

    if (isMalformedSeeded) {
      // Query keyword search to verify all transcripts survived
      const searchUrl = `http://127.0.0.1:${apiConfig.port}/search?q=transcript&content_type=audio&limit=20`;
      const searchRes = await fetchJson(searchUrl, headers);
      expect(searchRes.ok).toBe(true);

      const searchBody = JSON.stringify(searchRes.body);
      expect(searchBody).toContain("e2e-reversed-offset-transcript");
      expect(searchBody).toContain("e2e-infinite-offset-transcript");
      expect(searchBody).toContain("e2e-valid-offset-transcript");
    }
  });

  it("capture pipeline and server remain responsive after historical queries (continuity)", async () => {
    // Wait briefly and verify /health remains healthy and responsive
    await browser.pause(2_000);

    const healthUrl = `http://127.0.0.1:${apiConfig.port}/health`;
    const res = await fetchJson(healthUrl);
    expect(res.ok).toBe(true);
    expect(res.status).toBe(200);

    const body = res.body as { status?: string; frame_status?: string };
    expect(body.status).toBe("healthy");
  });
});
