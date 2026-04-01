// service.js — yekrial.com → Google Sheets (optimized for low usage on Railway)

import "dotenv/config";
import express from "express";
import { chromium } from "playwright";
import { GoogleSpreadsheet } from "google-spreadsheet";
import { JWT } from "google-auth-library";

const app = express();

// -----------------------------
// Config
// -----------------------------
const PORT = Number(process.env.PORT || 3000);

const SHEET_ID = process.env.SHEET_ID || "";
const WORKSHEET_TITLE = process.env.WORKSHEET_TITLE || "YekRialRates";
const SA_B64 = process.env.GOOGLE_SERVICE_ACCOUNT_JSON_BASE64 || "";

const CACHE_TTL_MS = Number(process.env.CACHE_TTL_MS || 15 * 60_000); // default 15 min
const YEKRIAL_URL = process.env.YEKRIAL_URL || "https://yekrial.com";
const YEKRIAL_HEADLESS = String(process.env.YEKRIAL_HEADLESS || "1") === "1";
const YEKRIAL_WAIT_MS = Number(process.env.YEKRIAL_WAIT_MS || 30_000);
const YEKRIAL_RENDER_WAIT_MS = Number(process.env.YEKRIAL_RENDER_WAIT_MS || 5_000);

// if RUN_ONCE=1, app runs a single scrape/write and exits.
// useful later if you want scheduled jobs instead of always-on server.
const RUN_ONCE = String(process.env.RUN_ONCE || "0") === "1";

// -----------------------------
// Helpers
// -----------------------------
function nowMs() {
  return Date.now();
}

function safeString(v) {
  return typeof v === "string" ? v : "";
}

function num(v) {
  if (v === null || v === undefined) return null;
  if (typeof v === "number") return Number.isFinite(v) ? v : null;
  const s = String(v).replace(/,/g, "").trim();
  if (!s) return null;
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

const CRYPTO_SYMBOLS = new Set([
  "BTC", "ETH", "USDT", "BNB", "XRP", "ADA", "DOGE", "SOL", "DOT",
  "TRX", "LTC", "BCH", "TON", "AVAX", "LINK", "MATIC", "SHIB", "ATOM",
  "ETC", "XLM", "EOS", "XAUT",
]);

const METAL_SYMBOLS = new Set(["XAU", "XAG", "GOLD", "SILVER"]);

// -----------------------------
// Google Sheets auth + write
// -----------------------------
function loadServiceAccountFromEnv() {
  if (!SA_B64) throw new Error("Missing GOOGLE_SERVICE_ACCOUNT_JSON_BASE64 in env");

  let jsonText = "";
  try {
    jsonText = Buffer.from(SA_B64, "base64").toString("utf8");
  } catch {
    throw new Error("GOOGLE_SERVICE_ACCOUNT_JSON_BASE64 is not valid base64");
  }

  let creds;
  try {
    creds = JSON.parse(jsonText);
  } catch {
    throw new Error("Decoded service account JSON is invalid");
  }

  if (!creds.client_email || !creds.private_key) {
    throw new Error("Service account JSON missing client_email/private_key");
  }

  return creds;
}

function makeJwtAuth(creds) {
  return new JWT({
    email: creds.client_email,
    key: String(creds.private_key).replace(/\\n/g, "\n"),
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });
}

async function getSheet() {
  if (!SHEET_ID) throw new Error("Missing SHEET_ID in env");

  const creds = loadServiceAccountFromEnv();
  const auth = makeJwtAuth(creds);

  const doc = new GoogleSpreadsheet(SHEET_ID, auth);
  await doc.loadInfo();

  const sheet = doc.sheetsByTitle[WORKSHEET_TITLE] || doc.sheetsByIndex[0];
  if (!sheet) throw new Error(`Worksheet not found: ${WORKSHEET_TITLE}`);

  return sheet;
}

async function writeRowsToSheet(rows) {
  const sheet = await getSheet();

  const wantedHeaders = [
    "group",
    "code",
    "name_fa",
    "price",
    "change",
    "low",
    "high",
    "ts",
    "source",
    "updated_at",
  ];

  await sheet.clear();
  await sheet.setHeaderRow(wantedHeaders);
  await sheet.loadHeaderRow();

  const updated_at = new Date().toISOString();
  const finalRows = rows.map((r) => ({ ...r, updated_at }));

  if (finalRows.length) {
    await sheet.addRows(finalRows);
  }

  return { count: finalRows.length, updated_at };
}

// -----------------------------
// Scraper
// -----------------------------
async function fetchYekRialRows() {
  const browser = await chromium.launch({
    headless: YEKRIAL_HEADLESS,
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-dev-shm-usage",
      "--disable-gpu",
      "--disable-blink-features=AutomationControlled",
    ],
  });

  const page = await browser.newPage({
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    viewport: { width: 1366, height: 900 },
  });

  try {
    await page.goto(YEKRIAL_URL, {
      waitUntil: "domcontentloaded",
      timeout: YEKRIAL_WAIT_MS,
    });

    await page.waitForTimeout(YEKRIAL_RENDER_WAIT_MS);

    // small interaction helps some lazy-loaded UIs
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(1200);
    await page.evaluate(() => window.scrollTo(0, 0));
    await page.waitForTimeout(800);

    const extracted = await page.evaluate(() => {
      const results = [];

      const cardNodes = [
        ...document.querySelectorAll("a.currency-card-link"),
        ...document.querySelectorAll("a[href*='/toman-rate/']"),
      ];

      cardNodes.forEach((card) => {
        const href = card.getAttribute("href") || "";
        const text = (card.innerText || card.textContent || "").trim();
        if (!href || !text) return;

        const codeMatch = href.match(/\/toman-rate\/([A-Z0-9_-]{2,15})/i);
        if (!codeMatch) return;
        const symbol = String(codeMatch[1]).toUpperCase();

        const nums = Array.from(
          text.matchAll(/\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d+(?:\.\d+)?/g)
        ).map((m) => m[0]);

        let priceText = nums.find((s) => s.includes(",")) || "";
        if (!priceText && nums.length) {
          priceText =
            nums
              .map((s) => ({ s, n: Number(s.replace(/,/g, "")) }))
              .filter((x) => Number.isFinite(x.n))
              .sort((a, b) => b.n - a.n)[0]?.s || "";
        }

        if (!priceText) return;

        const price = Number(priceText.replace(/,/g, ""));
        if (!Number.isFinite(price)) return;

        const faMatch = text.match(/[\u0600-\u06FF][\u0600-\u06FF\s‌]{2,}/);
        const name_fa = faMatch ? faMatch[0].trim() : symbol;

        const changeMatch = text.match(/[-+]\s*\d+(?:\.\d+)?\s*%/);
        const change = changeMatch ? changeMatch[0].replace(/\s+/g, "") : "0";

        results.push({ symbol, name_fa, price, change });
      });

      const seen = new Set();
      return results.filter((r) => {
        if (seen.has(r.symbol)) return false;
        seen.add(r.symbol);
        return true;
      });
    });

    if (!extracted.length) {
      const debug = await page.evaluate(() => ({
        title: document.title,
        sample: (document.body?.innerText || "").slice(0, 1200),
        links: Array.from(document.querySelectorAll("a"))
          .map((a) => a.getAttribute("href"))
          .filter(Boolean)
          .slice(0, 40),
      }));

      throw new Error(
        `No currency cards found. Title: ${debug.title}. Sample: ${debug.sample}`
      );
    }

    const ts = new Date().toISOString();

    const rows = extracted
      .map((x) => {
        const symbol = String(x.symbol || "").toUpperCase();
        const price = typeof x.price === "number" ? x.price : num(x.price);
        if (!symbol || price === null) return null;

        let group = "fiat";
        if (CRYPTO_SYMBOLS.has(symbol)) group = "crypto";
        else if (METAL_SYMBOLS.has(symbol)) group = "metal";

        return {
          group,
          code: symbol.toLowerCase(),
          name_fa: safeString(x.name_fa) || symbol,
          price,
          change: safeString(x.change) || "0",
          low: null,
          high: null,
          ts,
          source: "yekrial.com",
        };
      })
      .filter(Boolean);

    if (!rows.length) {
      throw new Error("Cards found, but no valid rows parsed");
    }

    const groupOrder = { fiat: 1, metal: 2, crypto: 3, unknown: 9 };
    rows.sort(
      (a, b) =>
        (groupOrder[a.group] ?? 9) - (groupOrder[b.group] ?? 9) ||
        a.code.localeCompare(b.code)
    );

    return rows;
  } finally {
    await page.close().catch(() => {});
    await browser.close().catch(() => {});
  }
}

// -----------------------------
// Runner + cache
// -----------------------------
let lastRun = { ok: false, error: "Not run yet", updated_at: null, count: 0 };
let lastFetchMs = 0;
let isRunning = false;

async function runOnce(force = false) {
  const age = nowMs() - lastFetchMs;

  if (!force && lastRun.ok && age < CACHE_TTL_MS) {
    return {
      ...lastRun,
      cached: true,
      cache_age_ms: age,
    };
  }

  if (isRunning) {
    return {
      ...lastRun,
      busy: true,
    };
  }

  isRunning = true;

  try {
    const rows = await fetchYekRialRows();
    const result = await writeRowsToSheet(rows);

    lastRun = {
      ok: true,
      error: null,
      updated_at: result.updated_at,
      count: result.count,
    };
    lastFetchMs = nowMs();

    return {
      ...lastRun,
      cached: false,
      cache_age_ms: 0,
    };
  } catch (e) {
    lastRun = {
      ok: false,
      error: e?.message || "unknown",
      updated_at: null,
      count: 0,
    };
    throw e;
  } finally {
    isRunning = false;
  }
}

// -----------------------------
// Routes
// -----------------------------
app.get("/", (_req, res) => {
  res.type("text/plain").send("yekrial-to-sheets running");
});

app.get("/health", (_req, res) => {
  res.json({
    ok: true,
    service: "yekrial-to-sheets",
    worksheet: WORKSHEET_TITLE,
    yekrial_url: YEKRIAL_URL,
    cache_ttl_ms: CACHE_TTL_MS,
    run_once: RUN_ONCE,
    lastRun,
  });
});

app.get("/run", async (req, res) => {
  const force = req.query.force === "1" || req.query.force === "true";

  try {
    const out = await runOnce(force);
    res.json({ ok: true, ...out });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || "unknown" });
  }
});

// -----------------------------
// Start / one-shot mode
// -----------------------------
async function main() {
  if (RUN_ONCE) {
    try {
      const out = await runOnce(true);
      console.log("Run-once completed:", out);
      process.exit(0);
    } catch (e) {
      console.error("Run-once failed:", e?.message || e);
      process.exit(1);
    }
    return;
  }

  app.listen(PORT, () => {
    console.log(`yekrial-to-sheets running on port ${PORT}`);
    console.log("Health: /health");
    console.log("Manual run: /run?force=1");
  });
}

main().catch((e) => {
  console.error("Startup failed:", e?.message || e);
  process.exit(1);
});
