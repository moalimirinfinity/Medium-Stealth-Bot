#!/usr/bin/env node

const fs = require("fs");
const path = require("path");
const { chromium } = require("playwright");

const ROOT = process.cwd();
const ENV_PATH = path.join(ROOT, ".env");
const OUT_DIR = path.join(ROOT, "captures", "final");
const TODAY = new Date().toISOString().slice(0, 10);
const RAW_BASENAME = `live_capture_${TODAY}.json`;
const OPS_BASENAME = `live_ops_${TODAY}.json`;
const RAW_PATH = path.join(OUT_DIR, RAW_BASENAME);
const OPS_PATH = path.join(OUT_DIR, OPS_BASENAME);

function readEnv(filePath) {
  const env = {};
  const data = fs.readFileSync(filePath, "utf8");
  for (const rawLine of data.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#")) continue;
    const idx = line.indexOf("=");
    if (idx === -1) continue;
    const key = line.slice(0, idx).trim();
    const value = line.slice(idx + 1).trim();
    env[key] = value;
  }
  return env;
}

function parseCookieHeader(cookieHeader) {
  return cookieHeader
    .split(";")
    .map((c) => c.trim())
    .filter(Boolean)
    .map((pair) => {
      const idx = pair.indexOf("=");
      if (idx === -1) return null;
      return {
        name: pair.slice(0, idx).trim(),
        value: pair.slice(idx + 1).trim(),
      };
    })
    .filter(Boolean);
}

function pickHeaders(headers, keys) {
  const out = {};
  for (const key of keys) {
    if (headers[key] !== undefined) out[key] = headers[key];
  }
  return out;
}

function uniqueSorted(values) {
  return [...new Set(values)].sort();
}

function safeStringify(obj) {
  return JSON.stringify(obj, null, 2) + "\n";
}

function statusSummary(records) {
  return uniqueSorted(records.map((r) => r.status).filter((v) => v !== undefined));
}

function isSideEffectMutation(payload) {
  if (!payload || typeof payload !== "object") return false;
  const op = payload.operationName || "";
  const query = typeof payload.query === "string" ? payload.query.trim() : "";
  if (!query.startsWith("mutation")) return false;
  return !["LogClientEventsMutation", "LogExperimentExposure"].includes(op);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function navigateAndSettle(page, url, ms = 9000) {
  await page.goto(url, { waitUntil: "domcontentloaded", timeout: 90000 });
  await sleep(ms);
  const title = await page.title();
  if (title.includes("Just a moment")) {
    await sleep(15000);
  }
}

async function main() {
  if (!fs.existsSync(ENV_PATH)) {
    throw new Error(".env file not found");
  }
  fs.mkdirSync(OUT_DIR, { recursive: true });

  const env = readEnv(ENV_PATH);
  if (!env.MEDIUM_SESSION) {
    throw new Error("MEDIUM_SESSION missing in .env");
  }
  const activityUrl = (env.MEDIUM_ACTIVITY_URL || "").trim();
  const captureHeadless = String(env.PLAYWRIGHT_HEADLESS || "true").toLowerCase() !== "false";

  const cookiePairs = parseCookieHeader(env.MEDIUM_SESSION);
  const hasXsrfCookie = cookiePairs.some((c) => c.name === "xsrf");
  if (!hasXsrfCookie && env.MEDIUM_CSRF) {
    cookiePairs.push({ name: "xsrf", value: env.MEDIUM_CSRF });
  }

  const cookies = cookiePairs.map((c) => ({
    name: c.name,
    value: c.value,
    domain: ".medium.com",
    path: "/",
    secure: true,
  }));

  const browser = await chromium.launch({ headless: captureHeadless });
  const context = await browser.newContext({
    viewport: { width: 1440, height: 900 },
    userAgent:
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
  });
  await context.addCookies(cookies);

  const page = await context.newPage();
  const graphqlRecords = [];
  const visits = [];
  const actions = [];

  await page.route("**/_/graphql", async (route) => {
    const req = route.request();
    if (req.method() !== "POST") {
      await route.continue();
      return;
    }

    const raw = req.postData() || "";
    let parsed = [];
    try {
      const body = JSON.parse(raw);
      parsed = Array.isArray(body) ? body : [body];
    } catch {
      parsed = [];
    }

    const shouldStub = parsed.some((p) => isSideEffectMutation(p));
    if (!shouldStub) {
      await route.continue();
      return;
    }

    const body = JSON.stringify(parsed.map(() => ({ data: {} })));
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      headers: {
        "x-codex-stubbed": "1",
      },
      body,
    });
  });

  page.on("response", async (response) => {
    const url = response.url();
    const req = response.request();
    if (!url.includes("/_/graphql") || req.method() !== "POST") return;

    const requestHeaders = req.headers();
    const requestBody = req.postData() || "";

    let payloadItems = [];
    try {
      const parsed = JSON.parse(requestBody);
      if (Array.isArray(parsed)) {
        payloadItems = parsed;
      } else {
        payloadItems = [parsed || {}];
      }
    } catch {
      payloadItems = [{}];
    }
    for (const payload of payloadItems) {
      if (typeof payload.variables === "string") {
        try {
          payload.variables = JSON.parse(payload.variables);
        } catch {
          // keep original string value
        }
      }
    }

    let responseBody = null;
    let responseBodyIsArray = false;
    let responseBodyParseError = null;
    try {
      const ct = response.headers()["content-type"] || "";
      if (ct.includes("application/json")) {
        responseBody = await response.json();
        responseBodyIsArray = Array.isArray(responseBody);
      }
    } catch (err) {
      responseBodyParseError = String(err).slice(0, 180);
    }

    for (let idx = 0; idx < payloadItems.length; idx += 1) {
      const payload = payloadItems[idx] || {};
      const bodyPart =
        responseBodyIsArray && Array.isArray(responseBody) ? responseBody[idx] || {} : responseBody || {};

      const responseSummary = {
        hasData: !!bodyPart.data,
        hasErrors: Array.isArray(bodyPart.errors) && bodyPart.errors.length > 0,
        errorCount: Array.isArray(bodyPart.errors) ? bodyPart.errors.length : 0,
        topLevelDataKeys: bodyPart.data ? Object.keys(bodyPart.data) : [],
        nonJson: responseBody === null,
      };
      if (responseBodyParseError) {
        responseSummary.parseError = responseBodyParseError;
      }

      graphqlRecords.push({
        capturedAt: new Date().toISOString(),
        pageUrl: page.url(),
        requestUrl: url,
        status: response.status(),
        operationName: payload.operationName || "UnknownOperation",
        query: payload.query || null,
        variableKeys: Object.keys(payload.variables || {}).sort(),
        variables: payload.variables || {},
        requestBodyLength: requestBody.length,
        requestBodyPreview: requestBody.slice(0, 280),
        requestHeadersSubset: pickHeaders(requestHeaders, [
          "origin",
          "referer",
          "content-type",
          "x-xsrf-token",
          "apollographql-client-name",
          "apollographql-client-version",
        ]),
        responseSummary,
        stubbed: response.headers()["x-codex-stubbed"] === "1",
      });
    }
  });

  await navigateAndSettle(page, "https://medium.com/me/followers");
  visits.push("https://medium.com/me/followers");

  await navigateAndSettle(page, "https://medium.com/me/following");
  visits.push("https://medium.com/me/following");

  if (activityUrl) {
    await navigateAndSettle(page, activityUrl);
    visits.push(activityUrl);
  }

  await navigateAndSettle(page, "https://medium.com/tag/programming/latest");
  visits.push("https://medium.com/tag/programming/latest");

  try {
    await page.getByRole("button", { name: /^Follow$/i }).first().click({ timeout: 7000 });
    actions.push("tag-follow-clicked");
    await sleep(3000);
  } catch {
    actions.push("tag-follow-not-found");
  }

  await page.evaluate(async () => {
    const probes = [
      {
        operationName: "UnsubscribeNewsletterV3Mutation",
        query:
          "mutation UnsubscribeNewsletterV3Mutation($newsletterV3Id: ID!) { unsubscribeNewsletterV3(newsletterV3Id: $newsletterV3Id) }",
        variables: { newsletterV3Id: "f032ccfb578c" },
      },
      {
        operationName: "UnfollowUserMutation",
        query:
          "mutation UnfollowUserMutation($targetUserId: ID!) { unfollowUser(targetUserId: $targetUserId) { __typename id name viewerEdge { __typename id isFollowing } } }",
        variables: { targetUserId: "37b65ca33b67" },
      },
      {
        operationName: "ClapMutation",
        query:
          "mutation ClapMutation($targetPostId: ID!, $userId: ID!, $numClaps: Int!) { clap(targetPostId: $targetPostId, userId: $userId, numClaps: $numClaps) { __typename viewerEdge { __typename id clapCount } id clapCount } }",
        variables: {
          targetPostId: "7d66cfdfa301",
          userId: "cf6627889e92",
          numClaps: 1,
        },
      },
      {
        operationName: "PublishPostThreadedResponse",
        query:
          "mutation PublishPostThreadedResponse($inResponseToPostId: ID!, $deltas: [Delta!]!, $inResponseToQuoteId: ID) { publishPostThreadedResponse(inResponseToPostId: $inResponseToPostId, deltas: $deltas, inResponseToQuoteId: $inResponseToQuoteId) { __typename } }",
        variables: {
          inResponseToPostId: "7d66cfdfa301",
          deltas: [{ insert: "Probe response from capture harness." }],
          inResponseToQuoteId: null,
        },
      },
      {
        operationName: "ClapMutation",
        query:
          "mutation ClapMutation($targetPostId: ID!, $userId: ID!, $numClaps: Int!) { clap(targetPostId: $targetPostId, userId: $userId, numClaps: $numClaps) { __typename viewerEdge { __typename id clapCount } id clapCount } }",
        variables: {
          targetPostId: "7d66cfdfa301",
          userId: "cf6627889e92",
          numClaps: -13,
        },
      },
      {
        operationName: "DeleteResponseMutation",
        query:
          "mutation DeleteResponseMutation($responseId: ID!) { deletePost(targetPostId: $responseId) }",
        variables: {
          responseId: "9facd32dd4b0",
        },
      },
    ];

    for (const op of probes) {
      await fetch("/_/graphql", {
        method: "POST",
        credentials: "include",
        headers: {
          "content-type": "application/json",
          "apollographql-client-name": "lite",
          "apollographql-client-version": "main-20260223-200902-cb62c3b9f7",
        },
        body: JSON.stringify([op]),
      });
    }
  });
  actions.push("manual-mutation-probes-sent");

  const articleUrl =
    "https://thilo-hermann.medium.com/the-day-we-forgot-about-layers-and-components-d6222451c4e2";
  await navigateAndSettle(page, articleUrl, 12000);
  visits.push(articleUrl);

  await sleep(3000);

  try {
    await page.getByRole("button", { name: /clap/i }).first().click({ timeout: 7000 });
    actions.push("article-clap-clicked");
    await sleep(2000);
  } catch {
    actions.push("article-clap-not-found");
  }

  await context.close();
  await browser.close();

  const opNames = uniqueSorted(graphqlRecords.map((r) => r.operationName));
  const mutationNames = uniqueSorted(
    graphqlRecords
      .filter((r) => typeof r.query === "string" && r.query.trim().startsWith("mutation"))
      .map((r) => r.operationName),
  );

  const operationStats = opNames.map((name) => {
    const rows = graphqlRecords.filter((r) => r.operationName === name);
    const stubbedHits = rows.filter((r) => r.stubbed).length;
    return {
      operationName: name,
      hits: rows.length,
      stubbedHits,
      nonStubbedHits: rows.length - stubbedHits,
      evidence:
        stubbedHits === rows.length
          ? "probe_stubbed_only"
          : stubbedHits > 0
            ? "mixed"
            : "live_ui_observed",
      statusCodes: statusSummary(rows),
      variableKeySets: uniqueSorted(rows.map((r) => JSON.stringify(r.variableKeys))).map((s) =>
        JSON.parse(s),
      ),
      samplePageUrls: uniqueSorted(rows.map((r) => r.pageUrl)).slice(0, 5),
      sampleRequestUrls: uniqueSorted(rows.map((r) => r.requestUrl)).slice(0, 5),
    };
  });

  const practicalCritical = uniqueSorted(
    [
      "UserViewerEdge",
      "NewsletterV3ViewerEdge",
      "TopicLatestStorieQuery",
      "TopicWhoToFollowPubishersQuery",
      "WhoToFollowModuleQuery",
      "SubscribeNewsletterV3Mutation",
      "UnsubscribeNewsletterV3Mutation",
      "UnfollowUserMutation",
      "ClapMutation",
      "DeleteResponseMutation",
    ].filter((name) => opNames.includes(name)),
  );

  const rawDoc = {
    capturedAt: new Date().toISOString(),
    purpose: "live_graphql_capture",
    source: "playwright + .env session cookies",
    visits,
    actions,
    totalRequestsCaptured: graphqlRecords.length,
    totalUniqueOperationNames: opNames.length,
    operationNames: opNames,
    mutationNames,
    practicalCritical,
    requests: graphqlRecords,
  };

  const opsDoc = {
    capturedAt: rawDoc.capturedAt,
    purpose: "live_graphql_ops_summary",
    sourceCapture: `captures/final/${RAW_BASENAME}`,
    totalUniqueOperationNames: opNames.length,
    operationNames: opNames,
    mutationNames,
    practicalCritical,
    operationStats,
  };

  fs.writeFileSync(RAW_PATH, safeStringify(rawDoc), "utf8");
  fs.writeFileSync(OPS_PATH, safeStringify(opsDoc), "utf8");

  console.log(`Wrote ${RAW_PATH}`);
  console.log(`Wrote ${OPS_PATH}`);
  console.log(`Captured requests: ${graphqlRecords.length}`);
  console.log(`Unique operation names: ${opNames.length}`);
}

main().catch((err) => {
  console.error(err.stack || String(err));
  process.exit(1);
});
