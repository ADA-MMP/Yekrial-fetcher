/** 
 * service.js — yekrial.com → Google Sheets (Render-ready, ESM)
 *
 * What it does:
 * - Loads https://yekrial.com in a headless browser (Playwright)
 * - Extracts ALL rate cards from DOM: a.currency-card-link (symbol from href)
 * - Writes rows into Google Sheets (service account via JWT)
 *
 * ENV required (Render → Environment Variables):
 *   SHEET_ID
 *   GOOGLE_SERVICE_ACCOUNT_JSON_BASE64
 *
 * Optional:
 *   PORT
 *   WORKSHEET_TITLE
 *   CACHE_TTL_MS
 *   CRON                 (default: every 10 minutes)
 *   YEKRIAL_URL           (default: "https://yekrial.com")
 *   YEKRIAL_HEADLESS      ("1" or "0", default "1")
 *   YEKRIAL_WAIT_MS       (default: 20000)
 *   YEKRIAL_RENDER_WAIT_MS(default: 1800)
 *
 * Routes:
 *   GET /
 *   GET /health
 *   GET /run?force=1
 */

import "dotenv/config";
import express from "express";
import cron from "node-cron";
import { chromium } from "playwright";
import { GoogleSpreadsheet } from "google-spreadsheet";
import { JWT } from "google-auth-library";

const app = express();

// -----------------------------
// Config
// -----------------------------
const PORT = Number(process.env.PORT || 3000);

const SHEET_ID = process.env.SHEET_ID || "";
const WORKSHEET_TITLE = process.env.WORKSHEET_TITLE || "Rates";
const SA_B64 = process.env.GOOGLE_SERVICE_ACCOUNT_JSON_BASE64 || "";

const CACHE_TTL_MS = Number(process.env.CACHE_TTL_MS || 60_000);
const CRON_EXPR = process.env.CRON || "every 10 minutes";

const YEKRIAL_URL = process.env.YEKRIAL_URL || "https://yekrial.com";
const YEKRIAL_HEADLESS = String(process.env.YEKRIAL_HEADLESS || "1") === "1";
const YEKRIAL_WAIT_MS = Number(process.env.YEKRIAL_WAIT_MS || 20_000);
const YEKRIAL_RENDER_WAIT_MS = Number(process.env.YEKRIAL_RENDER_WAIT_MS || 1800);

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

// Symbol classification (future-proof for crypto/metals if added later)
const CRYPTO_SYMBOLS = new Set([
  "BTC","ETH","USDT","BNB","XRP","ADA","DOGE","SOL","DOT",
  "TRX","LTC","BCH","TON","AVAX","LINK","MATIC","SHIB","ATOM",
  "ETC","XLM","EOS","XAUT"
]);

const METAL_SYMBOLS = new Set(["XAU","XAG","GOLD","SILVER"]);

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

  // google-spreadsheet v4+ supports passing auth directly
  const doc = new GoogleSpreadsheet(SHEET_ID, auth);
  await doc.loadInfo();

  const sheet = doc.sheetsByTitle[WORKSHEET_TITLE] || doc.sheetsByIndex[0];
  if (!sheet) throw new Error("Worksheet not found");
  return sheet;
}

async function ensureHeaders(sheet, wantedHeaders) {
  try {
    await sheet.loadHeaderRow();
  } catch {
    // ignore
  }

  const hasHeaders =
    Array.isArray(sheet.headerValues) && sheet.headerValues.length > 0;

  if (!hasHeaders) {
    await sheet.setHeaderRow(wantedHeaders);
    await sheet.loadHeaderRow();
  }
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

  await ensureHeaders(sheet, wantedHeaders);

  // ✅ single clear call (avoid per-row deletes → quota-friendly)
  await sheet.clear();

  const updated_at = new Date().toISOString();
  const finalRows = rows.map((r) => ({ ...r, updated_at }));

  if (finalRows.length) {
    await sheet.addRows(finalRows);
  }

  return { count: finalRows.length, updated_at };
}

// -----------------------------
// YekRial scraper (DOM-based, stable)
// -----------------------------
async function fetchYekRialRows() {
  const browser = await chromium.launch({ headless: YEKRIAL_HEADLESS });
  const page = await browser.newPage({
    userAgent: "yekrial-to-sheets/1.0",
  });

  try {
    await page.goto(YEKRIAL_URL, {
      waitUntil: "networkidle",
      timeout: YEKRIAL_WAIT_MS,
    });

    // Let Blazor finish rendering cards
    await page.waitForTimeout(YEKRIAL_RENDER_WAIT_MS);

    const extracted = await page.evaluate(() => {
      const results = [];

      const cards = document.querySelectorAll("a.currency-card-link");
      cards.forEach((card) => {
        const href = card.getAttribute("href") || "";
        const text = (card.innerText || "").trim();
        if (!href || !text) return;

        // Extract symbol from href:
        // e.g. "/toman-rate/USD" (future: "/toman-rate/BTC")
        const codeMatch = href.match(/\/toman-rate\/([A-Z0-9_-]{2,15})/i);
        if (!codeMatch) return;

        const symbol = String(codeMatch[1]).toUpperCase();

        // Extract price:
        // Supports "166,340" and also plain numbers, optionally decimals
        const priceMatch = text.match(
          /\b\d{1,3}(?:,\d{3})+(?:\.\d+)?\b|\b\d+(?:\.\d+)?\b/
        );
        if (!priceMatch) return;

        // Persian name:
        // Take first Persian chunk found
        const faMatch = text.match(/[\u0600-\u06FF][\u0600-\u06FF\s‌]{2,}/);
        const name_fa = faMatch ? faMatch[0].trim() : symbol;

        // Change percent if present (optional)
        // e.g. "+0.42%" or "-0.12%"
        const changeMatch = text.match(/[-+]\s*\d+(?:\.\d+)?\s*%/);
        const change = changeMatch ? changeMatch[0].replace(/\s+/g, "") : null;

        results.push({
          symbol,
          name_fa,
          priceText: priceMatch[0],
          change,
        });
      });

      // De-dup by symbol (keep first)
      const seen = new Set();
      return results.filter((r) => {
        if (seen.has(r.symbol)) return false;
        seen.add(r.symbol);
        return true;
      });
    });

    if (!extracted.length) {
      throw new Error("No currency cards found using selector: a.currency-card-link");
    }

    const ts = new Date().toISOString();

    const rows = extracted
      .map((x) => {
        const symbol = String(x.symbol || "").toUpperCase();
        const price = num(x.priceText);

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
      throw new Error("Cards found, but no valid rows parsed (price/symbol issue)");
    }

    // Optional: stable ordering
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
  if (!force && lastRun.ok && age < CACHE_TTL_MS) return lastRun;

  if (isRunning) return lastRun;
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
    return lastRun;
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
  res.type("text/plain").send("yekrial-to-sheets running ✅");
});

app.get("/health", (_req, res) => {
  res.json({
    ok: true,
    service: "yekrial-to-sheets",
    worksheet: WORKSHEET_TITLE,
    yekrial_url: YEKRIAL_URL,
    cron: CRON_EXPR,
    cache_ttl_ms: CACHE_TTL_MS,
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
// Scheduler
// -----------------------------
cron.schedule(CRON_EXPR, async () => {
  try {
    await runOnce(false);
    console.log("✅ Sheet updated:", lastRun);
  } catch (e) {
    console.error("❌ Update failed:", e?.message || e);
  }
});

// -----------------------------
// Start server
// -----------------------------
app.listen(PORT, () => {
  console.log(`yekrial-to-sheets running on port ${PORT}`);
  console.log(`Manual run: /run?force=1`);
});
