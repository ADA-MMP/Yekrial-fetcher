// service.js — yekrial.com → Google Sheets
// Railway-hardened version:
// - prevents overlapping scraper runs
// - hard timeout for stuck runs
// - guaranteed Playwright cleanup
// - lower Chromium resource usage
// - memory/runtime diagnostics
// - graceful Railway SIGTERM handling

import "dotenv/config";
import express from "express";
import { chromium } from "playwright";
import { GoogleSpreadsheet } from "google-spreadsheet";
import { JWT } from "google-auth-library";

const app = express();

// ============================================================
// CONFIG
// ============================================================

const PORT = Number(process.env.PORT || 3000);

const SHEET_ID = process.env.SHEET_ID || "";

const WORKSHEET_TITLE =
  process.env.WORKSHEET_TITLE || "YekRialRates";

const SERVICE_ACCOUNT_JSON =
  process.env.GOOGLE_SERVICE_ACCOUNT_JSON || "";

const CACHE_TTL_MS = Number(
  process.env.CACHE_TTL_MS || 15 * 60_000
);

const YEKRIAL_URL =
  process.env.YEKRIAL_URL || "https://yekrial.com";

const YEKRIAL_HEADLESS =
  String(process.env.YEKRIAL_HEADLESS || "1") === "1";

const YEKRIAL_WAIT_MS = Number(
  process.env.YEKRIAL_WAIT_MS || 30_000
);

const YEKRIAL_RENDER_WAIT_MS = Number(
  process.env.YEKRIAL_RENDER_WAIT_MS || 5_000
);

// Maximum timeout for normal Playwright actions.
const PLAYWRIGHT_ACTION_TIMEOUT_MS = Number(
  process.env.PLAYWRIGHT_ACTION_TIMEOUT_MS || 15_000
);

// Maximum time allowed for the ENTIRE scrape + Sheets update.
// Default: 75 seconds.
const RUN_TIMEOUT_MS = Number(
  process.env.RUN_TIMEOUT_MS || 75_000
);

// Time Railway gets for graceful shutdown.
const SHUTDOWN_GRACE_MS = Number(
  process.env.SHUTDOWN_GRACE_MS || 10_000
);

const RUN_ONCE =
  String(process.env.RUN_ONCE || "0") === "1";


// ============================================================
// RUNTIME STATE
// ============================================================

let server = null;

let shuttingDown = false;

// Keeps a reference to the current Chromium browser.
// This allows the watchdog and SIGTERM handler to close it.
let activeBrowser = null;

let lastRun = {
  ok: false,
  error: "Not run yet",
  updated_at: null,
  count: 0,
};

let lastFetchMs = 0;

let isRunning = false;

let currentRunStartedAt = null;


// ============================================================
// GENERAL HELPERS
// ============================================================

function nowMs() {
  return Date.now();
}


function safeString(v) {
  return typeof v === "string" ? v : "";
}


function num(v) {
  if (v === null || v === undefined) {
    return null;
  }

  if (typeof v === "number") {
    return Number.isFinite(v) ? v : null;
  }

  const s = String(v)
    .replace(/,/g, "")
    .trim();

  if (!s) {
    return null;
  }

  const n = Number(s);

  return Number.isFinite(n) ? n : null;
}


// ============================================================
// MEMORY / RUNTIME DIAGNOSTICS
// ============================================================

function mb(bytes) {
  return Math.round(
    (bytes / 1024 / 1024) * 10
  ) / 10;
}


function runtimeStats() {
  const memory = process.memoryUsage();

  return {
    pid: process.pid,

    uptime_sec:
      Math.round(process.uptime()),

    memory_mb: {
      rss: mb(memory.rss),

      heap_used:
        mb(memory.heapUsed),

      heap_total:
        mb(memory.heapTotal),

      external:
        mb(memory.external),
    },
  };
}


function logRuntime(label) {
  console.log(
    `[runtime] ${label}`,
    runtimeStats()
  );
}


// ============================================================
// ACTIVE BROWSER CLEANUP
// ============================================================

async function closeActiveBrowser(
  reason = "cleanup"
) {
  const browser = activeBrowser;

  // Remove global reference first so another cleanup
  // attempt does not try to close the same browser twice.
  activeBrowser = null;

  if (!browser) {
    return;
  }

  console.log(
    `closing active browser (${reason})`
  );

  try {
    await browser.close();

    console.log(
      "active browser closed"
    );
  } catch (e) {
    console.error(
      "active browser close error:",
      e?.message || e
    );
  }
}


// ============================================================
// HARD RUN WATCHDOG
// ============================================================

async function withHardRunTimeout(fn) {
  let timeoutHandle = null;

  let forcedExitHandle = null;

  let timedOut = false;

  const timeoutPromise =
    new Promise((_, reject) => {

      timeoutHandle = setTimeout(
        async () => {

          timedOut = true;

          console.error(
            `HARD RUN TIMEOUT after ${RUN_TIMEOUT_MS} ms`
          );

          logRuntime(
            "hard-timeout"
          );

          // Attempt to kill Chromium first.
          await closeActiveBrowser(
            "hard timeout"
          );

          reject(
            new Error(
              `Run exceeded hard timeout of ${RUN_TIMEOUT_MS} ms`
            )
          );

          /*
           * Important:
           *
           * If Playwright, Chromium, Google APIs, or another
           * native operation is genuinely stuck, rejecting the
           * Promise alone may not free the process.
           *
           * Therefore, after giving cleanup a moment to finish,
           * terminate Node.
           *
           * Railway's restart policy can then start a fresh
           * container automatically instead of requiring a
           * manual redeploy.
           */

          forcedExitHandle = setTimeout(
            () => {

              console.error(
                "Forcing process exit after hard timeout so Railway can restart cleanly"
              );

              process.exit(1);

            },
            2000
          );

          forcedExitHandle.unref();

        },
        RUN_TIMEOUT_MS
      );

      timeoutHandle.unref();
    });


  try {

    return await Promise.race([
      fn(),
      timeoutPromise,
    ]);

  } finally {

    if (timeoutHandle) {
      clearTimeout(timeoutHandle);
    }

    /*
     * Don't cancel forced exit when the timeout actually fired.
     * In that situation we intentionally want Railway to
     * restart the process if cleanup doesn't recover it.
     */

    if (
      !timedOut &&
      forcedExitHandle
    ) {
      clearTimeout(
        forcedExitHandle
      );
    }
  }
}


// ============================================================
// CURRENCY GROUPS
// ============================================================

const CRYPTO_SYMBOLS = new Set([
  "BTC",
  "ETH",
  "USDT",
  "BNB",
  "XRP",
  "ADA",
  "DOGE",
  "SOL",
  "DOT",
  "TRX",
  "LTC",
  "BCH",
  "TON",
  "AVAX",
  "LINK",
  "MATIC",
  "SHIB",
  "ATOM",
  "ETC",
  "XLM",
  "EOS",
  "XAUT",
]);


const METAL_SYMBOLS = new Set([
  "XAU",
  "XAG",
  "GOLD",
  "SILVER",
]);


// ============================================================
// GOOGLE SHEETS AUTHENTICATION
// ============================================================

function loadServiceAccountFromEnv() {

  if (!SERVICE_ACCOUNT_JSON) {

    throw new Error(
      "Missing GOOGLE_SERVICE_ACCOUNT_JSON in env"
    );
  }


  let creds;


  try {

    creds =
      JSON.parse(
        SERVICE_ACCOUNT_JSON
      );

  } catch {

    throw new Error(
      "Service account JSON invalid"
    );
  }


  if (
    !creds.client_email ||
    !creds.private_key
  ) {

    throw new Error(
      "Service account JSON missing client_email/private_key"
    );
  }


  return creds;
}


function makeJwtAuth(creds) {

  return new JWT({

    email:
      creds.client_email,

    key:
      String(
        creds.private_key
      ).replace(
        /\\n/g,
        "\n"
      ),

    scopes: [
      "https://www.googleapis.com/auth/spreadsheets",
    ],
  });
}


// ============================================================
// GET GOOGLE SHEET
// ============================================================

async function getSheet() {

  if (!SHEET_ID) {

    throw new Error(
      "Missing SHEET_ID in env"
    );
  }


  const creds =
    loadServiceAccountFromEnv();


  const auth =
    makeJwtAuth(creds);


  console.log(
    "Google doc loadInfo started"
  );


  const doc =
    new GoogleSpreadsheet(
      SHEET_ID,
      auth
    );


  await doc.loadInfo();


  console.log(
    "Google doc loaded"
  );


  const sheet =
    doc.sheetsByTitle[
      WORKSHEET_TITLE
    ];


  if (!sheet) {

    throw new Error(
      `Worksheet not found: ${WORKSHEET_TITLE}`
    );
  }


  return sheet;
}


// ============================================================
// WRITE DATA TO GOOGLE SHEET
// ============================================================

async function writeRowsToSheet(
  rows
) {

  console.log(
    "writeRowsToSheet started. rows =",
    rows.length
  );


  const sheet =
    await getSheet();


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


  /*
   * IMPORTANT:
   *
   * clear() MUST happen before setHeaderRow().
   *
   * Otherwise we would create the headers and immediately
   * erase them.
   */

  console.log(
    "clearing sheet"
  );


  await sheet.clear();


  console.log(
    "setting header row"
  );


  await sheet.setHeaderRow(
    wantedHeaders
  );


  await sheet.loadHeaderRow();


  const updated_at =
    new Date().toISOString();


  const finalRows =
    rows.map(
      (r) => ({
        ...r,
        updated_at,
      })
    );


  if (finalRows.length) {

    console.log(
      "adding rows to sheet:",
      finalRows.length
    );


    await sheet.addRows(
      finalRows
    );
  }


  console.log(
    "writeRowsToSheet finished:",
    updated_at
  );


  return {

    count:
      finalRows.length,

    updated_at,
  };
}


// ============================================================
// YEKRIAL SCRAPER
// ============================================================

async function fetchYekRialRows() {

  console.log(
    "fetchYekRialRows started"
  );


  console.log(
    "launching browser"
  );


  logRuntime(
    "before-browser-launch"
  );


  let browser = null;

  let page = null;


  try {

    browser =
      await chromium.launch({

        headless:
          YEKRIAL_HEADLESS,

        args: [

          "--no-sandbox",

          "--disable-setuid-sandbox",

          "--disable-dev-shm-usage",

          "--disable-gpu",

          "--disable-blink-features=AutomationControlled",

          // Reduce unnecessary Chromium activity.

          "--disable-extensions",

          "--disable-background-networking",

          "--disable-default-apps",

          "--disable-sync",

          "--metrics-recording-only",

          "--no-first-run",
        ],
      });


    /*
     * Save the browser globally.
     *
     * If the run hangs, our watchdog can close it.
     */

    activeBrowser =
      browser;


    console.log(
      "browser launched"
    );


    logRuntime(
      "after-browser-launch"
    );


    page =
      await browser.newPage({

        userAgent:
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",

        viewport: {
          width: 1366,
          height: 900,
        },
      });


    /*
     * Default timeout for Playwright operations.
     */

    page.setDefaultTimeout(
      PLAYWRIGHT_ACTION_TIMEOUT_MS
    );


    page.setDefaultNavigationTimeout(
      YEKRIAL_WAIT_MS
    );


    /*
     * We don't need images, fonts or video to read
     * YekRial's currency values.
     *
     * Blocking these lowers network traffic and Chromium
     * memory/resource usage.
     *
     * JavaScript is NOT blocked because YekRial may need it
     * to render the rates.
     */

    await page.route(
      "**/*",
      async (route) => {

        const type =
          route
            .request()
            .resourceType();


        if (
          type === "image" ||
          type === "font" ||
          type === "media"
        ) {

          return route.abort();
        }


        return route.continue();
      }
    );


    console.log(
      "opening yekrial:",
      YEKRIAL_URL
    );


    await page.goto(
      YEKRIAL_URL,
      {

        waitUntil:
          "domcontentloaded",

        timeout:
          YEKRIAL_WAIT_MS,
      }
    );


    console.log(
      "page domcontentloaded"
    );


    await page.waitForTimeout(
      YEKRIAL_RENDER_WAIT_MS
    );


    console.log(
      "page render wait finished"
    );


    /*
     * Scroll down and back up.
     *
     * This preserves the behavior of your existing scraper
     * in case some YekRial cards are lazy-rendered.
     */

    await page.evaluate(
      () =>
        window.scrollTo(
          0,
          document.body.scrollHeight
        )
    );


    await page.waitForTimeout(
      1200
    );


    await page.evaluate(
      () =>
        window.scrollTo(
          0,
          0
        )
    );


    await page.waitForTimeout(
      800
    );


    console.log(
      "starting page.evaluate parser"
    );


    // ========================================================
    // PARSER CONTINUES IN PART 2
    // ========================================================
      const extracted =
      await page.evaluate(() => {

        const results = [];


        // ====================================================
        // METHOD 1:
        // Try YekRial currency-card links first
        // ====================================================

        const cardNodes = [

          ...document.querySelectorAll(
            "a.currency-card-link"
          ),

          ...document.querySelectorAll(
            "a[href*='/toman-rate/']"
          ),
        ];


        if (cardNodes.length) {

          cardNodes.forEach(
            (card) => {

              const href =
                card.getAttribute(
                  "href"
                ) || "";


              const text =
                (
                  card.innerText ||
                  card.textContent ||
                  ""
                ).trim();


              if (
                !href ||
                !text
              ) {
                return;
              }


              const codeMatch =
                href.match(
                  /\/toman-rate\/([A-Z0-9_-]{2,15})/i
                );


              if (!codeMatch) {
                return;
              }


              const symbol =
                String(
                  codeMatch[1]
                ).toUpperCase();


              /*
               * Extract numeric values from the card.
               */

              const nums =
                Array.from(
                  text.matchAll(
                    /\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d+(?:\.\d+)?/g
                  )
                ).map(
                  (m) => m[0]
                );


              /*
               * Prefer a comma-formatted value such as:
               *
               * 171,695
               */

              let priceText =
                nums.find(
                  (s) =>
                    s.includes(",")
                ) || "";


              /*
               * If no comma-formatted value exists,
               * choose the largest numeric value.
               */

              if (
                !priceText &&
                nums.length
              ) {

                priceText =
                  nums
                    .map(
                      (s) => ({
                        s,

                        n: Number(
                          s.replace(
                            /,/g,
                            ""
                          )
                        ),
                      })
                    )
                    .filter(
                      (x) =>
                        Number.isFinite(
                          x.n
                        )
                    )
                    .sort(
                      (a, b) =>
                        b.n - a.n
                    )[0]?.s || "";
              }


              if (!priceText) {
                return;
              }


              const price =
                Number(
                  priceText.replace(
                    /,/g,
                    ""
                  )
                );


              if (
                !Number.isFinite(
                  price
                )
              ) {
                return;
              }


              /*
               * Persian currency name.
               */

              const faMatch =
                text.match(
                  /[\u0600-\u06FF][\u0600-\u06FF\s‌]{2,}/
                );


              const name_fa =
                faMatch
                  ? faMatch[0].trim()
                  : symbol;


              /*
               * Percentage change.
               *
               * Examples:
               *
               * -3.27%
               * +1.25%
               */

              const changeMatch =
                text.match(
                  /[-+−]\s*\d+(?:\.\d+)?\s*%/
                );


              const change =
                changeMatch
                  ? changeMatch[0]
                      .replace(
                        /\s+/g,
                        ""
                      )
                      .replace(
                        "−",
                        "-"
                      )
                  : "0";


              results.push({
                symbol,
                name_fa,
                price,
                change,
              });
            }
          );
        }


        // ====================================================
        // METHOD 2:
        // Body-text fallback
        //
        // Used if YekRial changes/removes card selectors.
        //
        // Typical YekRial text:
        //
        // دلار آمریکا
        // USD
        // -3.27%
        // قیمت فعلی
        // 171,695 تومان
        // ====================================================

        if (!results.length) {

          const text =
            document.body
              ?.innerText || "";


          const lines =
            text
              .split(/\n+/)
              .map(
                (s) =>
                  s.trim()
              )
              .filter(Boolean);


          for (
            let i = 0;
            i < lines.length;
            i++
          ) {

            const name_fa =
              lines[i];


            const symbol =
              lines[i + 1];


            /*
             * Currency symbols normally look like:
             *
             * USD
             * CAD
             * EUR
             * USDT
             */

            if (
              !/^[A-Z]{3,6}$/.test(
                symbol || ""
              )
            ) {
              continue;
            }


            /*
             * Search several nearby lines for
             * the تومان price and percentage.
             */

            const nextLines =
              lines.slice(
                i,
                i + 8
              );


            const joined =
              nextLines.join(
                "\n"
              );


            const priceMatch =
              joined.match(
                /([\d,]+)\s*تومان/
              );


            if (!priceMatch) {
              continue;
            }


            const price =
              Number(
                priceMatch[1]
                  .replace(
                    /,/g,
                    ""
                  )
              );


            if (
              !Number.isFinite(
                price
              )
            ) {
              continue;
            }


            const changeMatch =
              joined.match(
                /[-+−]\s*\d+(?:\.\d+)?\s*%/
              );


            const change =
              changeMatch
                ? changeMatch[0]
                    .replace(
                      /\s+/g,
                      ""
                    )
                    .replace(
                      "−",
                      "-"
                    )
                : "0";


            results.push({

              symbol:
                symbol.toUpperCase(),

              name_fa,

              price,

              change,
            });
          }
        }


        // ====================================================
        // REMOVE DUPLICATES
        // ====================================================

        const seen =
          new Set();


        return results.filter(
          (r) => {

            if (
              !r.symbol ||
              seen.has(
                r.symbol
              )
            ) {
              return false;
            }


            seen.add(
              r.symbol
            );


            return true;
          }
        );
      });


    console.log(
      "page.evaluate finished. extracted =",
      extracted.length
    );


    // ========================================================
    // DEBUG INFORMATION IF NOTHING WAS FOUND
    // ========================================================

    if (!extracted.length) {

      const debug =
        await page.evaluate(
          () => ({

            title:
              document.title,

            sample:
              (
                document.body
                  ?.innerText || ""
              ).slice(
                0,
                2000
              ),

            links:
              Array.from(
                document.querySelectorAll(
                  "a"
                )
              )
                .map(
                  (a) =>
                    a.getAttribute(
                      "href"
                    )
                )
                .filter(Boolean)
                .slice(
                  0,
                  40
                ),
          })
        );


      throw new Error(
        `No rates parsed. Title: ${debug.title}. Sample: ${debug.sample}`
      );
    }


    // ========================================================
    // CONVERT EXTRACTED DATA TO SHEET ROWS
    // ========================================================

    const ts =
      new Date().toISOString();


    const rows =
      extracted
        .map(
          (x) => {

            const symbol =
              String(
                x.symbol || ""
              ).toUpperCase();


            const price =
              typeof x.price ===
              "number"

                ? x.price

                : num(
                    x.price
                  );


            if (
              !symbol ||
              price === null
            ) {
              return null;
            }


            let group =
              "fiat";


            if (
              CRYPTO_SYMBOLS.has(
                symbol
              )
            ) {

              group =
                "crypto";

            } else if (
              METAL_SYMBOLS.has(
                symbol
              )
            ) {

              group =
                "metal";
            }


            return {

              group,

              code:
                symbol.toLowerCase(),

              name_fa:
                safeString(
                  x.name_fa
                ) || symbol,

              price,

              change:
                safeString(
                  x.change
                ) || "0",

              low:
                null,

              high:
                null,

              ts,

              source:
                "yekrial.com",
            };
          }
        )
        .filter(
          Boolean
        );


    console.log(
      "parsed rows:",
      rows.length
    );


    if (!rows.length) {

      throw new Error(
        "Rates found, but no valid rows parsed"
      );
    }


    // ========================================================
    // SORT RESULTS
    // ========================================================

    const groupOrder = {

      fiat: 1,

      metal: 2,

      crypto: 3,

      unknown: 9,
    };


    rows.sort(
      (a, b) =>

        (
          groupOrder[
            a.group
          ] ?? 9
        ) -

        (
          groupOrder[
            b.group
          ] ?? 9
        )

        ||

        a.code.localeCompare(
          b.code
        )
    );


    return rows;


  } finally {

    // ========================================================
    // GUARANTEED PLAYWRIGHT CLEANUP
    // ========================================================

    console.log(
      "fetchYekRialRows cleanup started"
    );


    /*
     * Close the page first.
     */

    if (page) {

      try {

        await page.close();


        console.log(
          "page closed"
        );

      } catch (e) {

        console.error(
          "page close error:",
          e?.message || e
        );
      }
    }


    /*
     * Then close Chromium.
     */

    if (browser) {

      try {

        await browser.close();


        console.log(
          "browser closed"
        );

      } catch (e) {

        console.error(
          "browser close error:",
          e?.message || e
        );
      }
    }


    /*
     * Remove the global browser reference only
     * if it still points to this browser.
     *
     * The hard-timeout watchdog may already have
     * cleared it.
     */

    if (
      activeBrowser ===
      browser
    ) {

      activeBrowser =
        null;
    }


    logRuntime(
      "after-browser-cleanup"
    );
  }
}


// ============================================================
// RUNNER + CACHE
// ============================================================

async function runOnce(
  force = false
) {

  console.log(
    "runOnce started. force =",
    force
  );


  const age =
    nowMs() -
    lastFetchMs;


  // ==========================================================
  // RETURN CACHE WHEN APPROPRIATE
  // ==========================================================

  if (
    !force &&
    lastRun.ok &&
    age <
      CACHE_TTL_MS
  ) {

    console.log(
      "returning cached result. age =",
      age
    );


    return {

      ...lastRun,

      cached:
        true,

      cache_age_ms:
        age,

      busy:
        false,
    };
  }


  // ==========================================================
  // PREVENT OVERLAPPING SCRAPES
  // ==========================================================

  if (isRunning) {

    const runAge =
      currentRunStartedAt

        ? nowMs() -
          currentRunStartedAt

        : null;


    console.log(
      "run skipped because busy. current run age =",
      runAge
    );


    return {

      ...lastRun,

      busy:
        true,

      current_run_age_ms:
        runAge,
    };
  }


  isRunning =
    true;


  currentRunStartedAt =
    nowMs();


  logRuntime(
    "run-start"
  );


  try {

    // ========================================================
    // HARD TIMEOUT WRAPS THE ENTIRE OPERATION
    //
    // This includes:
    // - Chromium launch
    // - YekRial navigation
    // - parsing
    // - browser cleanup
    // - Google Sheets update
    // ========================================================

    return await withHardRunTimeout(
      async () => {

        console.log(
          "launching browser..."
        );


        const rows =
          await fetchYekRialRows();


        console.log(
          "rows fetched:",
          rows.length
        );


        console.log(
          "writing sheet..."
        );


        const result =
          await writeRowsToSheet(
            rows
          );


        console.log(
          "sheet written."
        );


        lastRun = {

          ok:
            true,

          error:
            null,

          updated_at:
            result.updated_at,

          count:
            result.count,
        };


        lastFetchMs =
          nowMs();


        console.log(
          "finished successfully"
        );


        logRuntime(
          "run-success"
        );


        return {

          ...lastRun,

          cached:
            false,

          cache_age_ms:
            0,

          busy:
            false,
        };
      }
    );


  } catch (e) {

    // ========================================================
    // RUN FAILED
    // ========================================================

    console.error(
      "runOnce error:",
      e?.message || e
    );


    lastRun = {

      ok:
        false,

      error:
        e?.message ||
        "unknown",

      updated_at:
        null,

      count:
        0,
    };


    logRuntime(
      "run-error"
    );


    throw e;


  } finally {

    // ========================================================
    // ALWAYS RELEASE THE RUN LOCK
    // ========================================================

    isRunning =
      false;


    currentRunStartedAt =
      null;


    console.log(
      "runOnce cleanup finished"
    );
  }
}


// ============================================================
// PART 3 CONTINUES WITH:
// - /
// - /health
// - HEAD /run
// - GET /run?force=1
// - graceful SIGTERM shutdown
// - server startup
// ============================================================
// ============================================================
// ROUTES
// ============================================================

app.get(
  "/",
  (_req, res) => {

    res
      .type("text/plain")
      .send(
        "yekrial-to-sheets running"
      );
  }
);


// ============================================================
// HEALTH ENDPOINT
// ============================================================

app.get(
  "/health",
  (_req, res) => {

    const runAge =
      isRunning &&
      currentRunStartedAt

        ? nowMs() -
          currentRunStartedAt

        : null;


    res.json({

      ok:
        true,

      service:
        "yekrial-to-sheets",

      worksheet:
        WORKSHEET_TITLE,

      yekrial_url:
        YEKRIAL_URL,

      cache_ttl_ms:
        CACHE_TTL_MS,

      run_timeout_ms:
        RUN_TIMEOUT_MS,

      run_once:
        RUN_ONCE,


      // Current scrape state

      running:
        isRunning,

      current_run_started_at:
        currentRunStartedAt

          ? new Date(
              currentRunStartedAt
            ).toISOString()

          : null,

      current_run_age_ms:
        runAge,


      // Last completed run

      lastRun,


      // Current Node memory/runtime information

      runtime:
        runtimeStats(),
    });
  }
);


// ============================================================
// IGNORE HEAD REQUESTS TO /run
//
// Some uptime services send HEAD requests.
// They must NOT launch Chromium.
// ============================================================

app.head(
  "/run",
  (_req, res) => {

    console.log(
      "HEAD /run ignored"
    );


    res
      .status(204)
      .end();
  }
);


// ============================================================
// MANUAL / SCHEDULED SCRAPE
//
// Required URL:
//
// /run?force=1
// ============================================================

app.get(
  "/run",
  async (req, res) => {

    console.log(
      "RUN REQUEST:",
      req.query
    );


    const force =
      req.query.force === "1" ||
      req.query.force === "true";


    // ========================================================
    // FORCE PARAMETER REQUIRED
    // ========================================================

    if (!force) {

      console.log(
        "RUN rejected: missing force=1"
      );


      return res
        .status(400)
        .json({

          ok:
            false,

          error:
            "Use /run?force=1",
        });
    }


    // ========================================================
    // DON'T START NEW WORK DURING RAILWAY SHUTDOWN
    // ========================================================

    if (shuttingDown) {

      return res
        .status(503)
        .json({

          ok:
            false,

          error:
            "Service is shutting down",
        });
    }


    try {

      const out =
        await runOnce(
          true
        );


      // ======================================================
      // SCRAPER ALREADY RUNNING
      // ======================================================

      if (out.busy) {

        return res
          .status(409)
          .json({

            ok:
              false,

            ...out,

            error:
              "A scrape is already running",
          });
      }


      // ======================================================
      // SUCCESS
      // ======================================================

      return res.json({

        ok:
          true,

        ...out,
      });


    } catch (e) {

      // ======================================================
      // FAILURE
      // ======================================================

      return res
        .status(500)
        .json({

          ok:
            false,

          error:
            e?.message ||
            "unknown",
        });
    }
  }
);


// ============================================================
// GRACEFUL SHUTDOWN
//
// Railway normally sends SIGTERM when:
// - redeploying
// - replacing a container
// - stopping the service
//
// This makes that shutdown clean instead of leaving Chromium
// or HTTP connections open.
// ============================================================

async function shutdown(
  signal
) {

  if (shuttingDown) {
    return;
  }


  shuttingDown =
    true;


  console.log(
    `${signal} received; shutting down cleanly`
  );


  /*
   * Safety timer:
   *
   * If something refuses to close, do not let the old
   * container remain stuck indefinitely.
   */

  const forceExitTimer =
    setTimeout(
      () => {

        console.error(
          "Forced shutdown after grace period"
        );


        process.exit(1);
      },

      SHUTDOWN_GRACE_MS
    );


  forceExitTimer.unref();


  // ==========================================================
  // CLOSE CHROMIUM FIRST
  // ==========================================================

  try {

    await closeActiveBrowser(
      signal
    );

  } catch {
    // Ignore browser cleanup failure during shutdown.
  }


  // ==========================================================
  // RUN_ONCE MODE MAY NOT HAVE AN HTTP SERVER
  // ==========================================================

  if (!server) {

    clearTimeout(
      forceExitTimer
    );


    process.exit(0);

    return;
  }


  // ==========================================================
  // STOP ACCEPTING NEW HTTP REQUESTS
  // ==========================================================

  server.close(
    () => {

      console.log(
        "HTTP server closed"
      );


      clearTimeout(
        forceExitTimer
      );


      process.exit(0);
    }
  );
}


// ============================================================
// SIGNAL HANDLERS
// ============================================================

process.on(
  "SIGTERM",
  () => {

    shutdown(
      "SIGTERM"
    );
  }
);


process.on(
  "SIGINT",
  () => {

    shutdown(
      "SIGINT"
    );
  }
);


// ============================================================
// UNHANDLED PROMISE REJECTIONS
// ============================================================

process.on(
  "unhandledRejection",
  (reason) => {

    console.error(
      "Unhandled rejection:",
      reason
    );
  }
);


// ============================================================
// UNCAUGHT EXCEPTIONS
//
// If Node reaches an unsafe state, exit and allow Railway
// to start a clean process.
// ============================================================

process.on(
  "uncaughtException",
  (error) => {

    console.error(
      "Uncaught exception:",
      error
    );


    process.exit(1);
  }
);


// ============================================================
// APPLICATION STARTUP
// ============================================================

async function main() {

  // ==========================================================
  // ONE-SHOT MODE
  //
  // RUN_ONCE=1
  //
  // Useful if the same project is ever executed as a job
  // instead of an always-running web service.
// ==========================================================

  if (RUN_ONCE) {

    try {

      const out =
        await runOnce(
          true
        );


      console.log(
        "Run-once completed:",
        out
      );


      process.exit(0);


    } catch (e) {

      console.error(
        "Run-once failed:",
        e?.message || e
      );


      process.exit(1);
    }


    return;
  }


  // ==========================================================
  // START EXPRESS SERVER
  // ==========================================================

  server =
    app.listen(
      PORT,
      "0.0.0.0",
      () => {

        console.log(
          `yekrial-to-sheets running on port ${PORT}`
        );


        console.log(
          "Health: /health"
        );


        console.log(
          "Manual run: /run?force=1"
        );


        console.log(
          `Hard run timeout: ${RUN_TIMEOUT_MS} ms`
        );


        logRuntime(
          "startup"
        );
      }
    );
}


// ============================================================
// START
// ============================================================

main().catch(
  (e) => {

    console.error(
      "Startup failed:",
      e?.message || e
    );


    process.exit(1);
  }
);
