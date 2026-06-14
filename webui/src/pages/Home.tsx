import { useMemo, type ReactNode } from "react";
import { usePollingJson } from "@/hooks/usePollingJson";
import type { AppStateResponse, CsvRowsResponse, ProviderState, TradeState } from "@/types";
import {
  formatClock,
  formatCountdown,
  formatMoney,
  formatPercent,
  formatRuntime,
  formatSigned,
  parseNumeric,
  toneClass,
} from "@/utils/formatters";

const emptyState: AppStateResponse = {
  app: { title: "muzz.world sandbox control room", subtitle: "", build_label: "-", build_modified_at: "-", runtime_seconds: 0 },
  quotes: { bid: 0, ask: 0, mid: 0, spread_bps: 0, last_tick_at: 0, feed_label: "Starting", feed_detail: "waiting", feed_style: "yellow" },
  account: { available: 0, equity: 0, live_pnl: 0, position_side: "FLAT", next_signal_at: 0, next_signal_in: 0, leverage: 0, stack_fraction: 0, stop_loss_usdc: 0 },
  signal: {
    last_signal: "PENDING",
    last_signal_why: "",
    last_signal_score: 0,
    last_signal_at: 0,
    last_signal_sources: [],
    last_error: "",
    background: {
      fear_greed: { value: "", classification: "", signal: "" },
      twitter_btc_15m: {
        available: false,
        tweet_count: 0,
        valid_tweet_count: 0,
        bullish_count: 0,
        bearish_count: 0,
        neutral_count: 0,
        bullish_pct: 0,
        bearish_pct: 0,
        neutral_pct: 0,
        avg_score: 0,
        signal: "",
        baseline_window_count: 0,
        delta_bullish_pct: null,
        delta_avg_score: null,
        unavailable_reason: "",
        summary: "",
        window_minutes: 15,
      },
    },
    providers: [],
  },
  position: null,
  trades: [],
  logs: [],
};

const emptyRows: CsvRowsResponse = { rows: [] };
const strategyNames = ["gemini", "openai", "claude", "perplexity", "grok", "consensus"];

function Panel({ title, kicker, children }: { title: string; kicker?: string; children: ReactNode }) {
  return (
    <section className="rounded-[28px] border border-white/10 bg-zinc-950/75 px-4 py-3 shadow-[0_18px_60px_rgba(0,0,0,0.35)] backdrop-blur">
      <div className="mb-3 flex items-end justify-between gap-4">
        <div>
          <p className="text-[10px] uppercase tracking-[0.35em] text-cyan-300/70">{kicker ?? "live module"}</p>
          <h2 className="mt-1.5 text-sm font-semibold uppercase tracking-[0.18em] text-zinc-100">{title}</h2>
        </div>
      </div>
      {children}
    </section>
  );
}

function Metric({ label, value, tone = "text-zinc-100" }: { label: string; value: string; tone?: string }) {
  return (
    <div className="rounded-3xl border border-white/10 bg-white/[0.03] px-4 py-3">
      <div className="text-[10px] uppercase tracking-[0.32em] text-zinc-500">{label}</div>
      <div className={`mt-1.5 font-mono text-2xl font-semibold ${tone}`}>{value}</div>
    </div>
  );
}

function ProviderCard({ provider }: { provider: ProviderState }) {
  const tone =
    provider.signal === "LONG" ? "text-emerald-300" : provider.signal === "SHORT" ? "text-rose-300" : "text-zinc-200";
  return (
    <div className="min-w-[210px] rounded-[24px] border border-white/10 bg-white/[0.03] px-3.5 py-3">
      <div className="flex items-center justify-between gap-3">
        <div className="text-sm font-bold uppercase tracking-[0.22em] text-zinc-100">{provider.provider}</div>
        <div className="font-mono text-xs text-zinc-500">{provider.elapsed_seconds.toFixed(2)}s</div>
      </div>
      <div className={`mt-1.5 font-mono text-lg font-semibold ${tone}`}>{provider.signal}</div>
      <div className="mt-1 text-xs text-zinc-500">{provider.model || "model unavailable"}</div>
      <p className="mt-2.5 line-clamp-4 min-h-[4.25rem] text-[13px] leading-5 text-zinc-300">{provider.error || provider.why || "Waiting for next cycle."}</p>
    </div>
  );
}

function TradesTable({ trades }: { trades: TradeState[] }) {
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-left text-sm">
        <thead className="text-[10px] uppercase tracking-[0.28em] text-zinc-500">
          <tr>
            {["Time", "Side", "Reason", "Entry Px", "Exit Px", "Entry USD", "Exit USD", "Raw PnL", "Fees", "Final PnL"].map((label) => (
              <th key={label} className="border-b border-white/10 px-3 py-2.5 font-medium">{label}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {trades.length === 0 ? (
            <tr><td colSpan={10} className="px-3 py-4 text-zinc-500">No trades yet.</td></tr>
          ) : (
            trades.map((trade) => (
              <tr key={`${trade.timestamp}-${trade.reason}`} className="border-b border-white/5 text-zinc-200">
                <td className="px-3 py-2.5 font-mono">{formatClock(trade.timestamp)}</td>
                <td className="px-3 py-2.5">{trade.side}</td>
                <td className="px-3 py-2.5 text-zinc-400">{trade.reason}</td>
                <td className="px-3 py-2.5 font-mono">{formatMoney(trade.entry_price)}</td>
                <td className="px-3 py-2.5 font-mono">{formatMoney(trade.exit_price)}</td>
                <td className="px-3 py-2.5 font-mono">{formatMoney(trade.entry_usdc)}</td>
                <td className="px-3 py-2.5 font-mono">{formatMoney(trade.exit_usdc)}</td>
                <td className={`px-3 py-2.5 font-mono font-semibold ${toneClass(trade.gross_pnl)}`}>{formatSigned(trade.gross_pnl, 4)}</td>
                <td className="px-3 py-2.5 font-mono text-amber-300">{formatMoney(trade.fees_paid, 4)}</td>
                <td className={`px-3 py-2.5 font-mono font-semibold ${toneClass(trade.net_pnl)}`}>{formatSigned(trade.net_pnl, 4)}</td>
              </tr>
            ))
          )}
        </tbody>
      </table>
    </div>
  );
}

function StrategyTable({ rows }: { rows: Array<Record<string, string>> }) {
  const totals = useMemo(
    () =>
      strategyNames.map((name) => ({
        name,
        total: rows.reduce((sum, row) => sum + parseNumeric(row[`${name}_return_pct`]), 0),
      })),
    [rows],
  );

  return (
    <div className="space-y-3">
      <div className="grid gap-3 md:grid-cols-3 xl:grid-cols-6">
        {totals.map((item) => (
          <div key={item.name} className="rounded-3xl border border-white/10 bg-white/[0.03] px-4 py-3">
            <div className="text-[10px] uppercase tracking-[0.28em] text-zinc-500">{item.name}</div>
            <div className={`mt-1.5 font-mono text-xl font-semibold ${toneClass(item.total)}`}>{formatPercent(item.total, 3)}</div>
          </div>
        ))}
      </div>
      <div className="overflow-x-auto">
        <table className="min-w-full text-left text-sm">
          <thead className="text-[10px] uppercase tracking-[0.28em] text-zinc-500">
            <tr>
              {["Start", "End", "Entry", "Exit", "BTC Move", ...strategyNames.map((name) => `${name} rtn`)].map((label) => (
                <th key={label} className="border-b border-white/10 px-3 py-2.5 font-medium">{label}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.length === 0 ? (
              <tr><td colSpan={11} className="px-3 py-4 text-zinc-500">The table fills after the next full 15-minute window closes.</td></tr>
            ) : (
              rows.map((row) => (
                <tr key={`${row.period_start_ts}-${row.period_end_ts}`} className="border-b border-white/5 text-zinc-200">
                  <td className="px-3 py-2.5 font-mono">{formatClock(parseNumeric(row.period_start_ts))}</td>
                  <td className="px-3 py-2.5 font-mono">{formatClock(parseNumeric(row.period_end_ts))}</td>
                  <td className="px-3 py-2.5 font-mono">{formatMoney(parseNumeric(row.entry_price))}</td>
                  <td className="px-3 py-2.5 font-mono">{formatMoney(parseNumeric(row.exit_price))}</td>
                  <td className={`px-3 py-2.5 font-mono ${toneClass(parseNumeric(row.btc_move_pct))}`}>{formatPercent(parseNumeric(row.btc_move_pct), 3)}</td>
                  {strategyNames.map((name) => {
                    const value = parseNumeric(row[`${name}_return_pct`]);
                    return <td key={name} className={`px-3 py-2.5 font-mono font-semibold ${toneClass(value)}`}>{formatPercent(value, 3)}</td>;
                  })}
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default function Home() {
  const state = usePollingJson("/api/state", emptyState, 1000);
  const strategyReturns = usePollingJson("/api/strategy-returns", emptyRows, 15000);
  const data = state.data;
  const consensusTone =
    data.signal.last_signal === "LONG" ? "text-emerald-300" : data.signal.last_signal === "SHORT" ? "text-rose-300" : "text-zinc-100";
  const fearGreed = data.signal.background.fear_greed;
  const twitter = data.signal.background.twitter_btc_15m;
  const twitterTone =
    twitter.signal === "LONG" ? "text-emerald-300" : twitter.signal === "SHORT" ? "text-rose-300" : "text-zinc-100";

  return (
    <main className="min-h-screen bg-[radial-gradient(circle_at_top,_rgba(34,211,238,0.12),_transparent_32%),linear-gradient(180deg,#0b0c0f_0%,#11131a_48%,#090a0d_100%)] px-4 py-4 text-zinc-100 md:px-6 xl:px-8">
      <div className="mx-auto max-w-[1700px] space-y-3">
        <header className="rounded-[32px] border border-white/10 bg-zinc-950/80 px-5 py-3.5">
          <div className="flex flex-col gap-3 xl:flex-row xl:items-center xl:justify-between">
            <h1 className="font-['Sora'] text-[28px] font-semibold tracking-[0.08em] text-zinc-50">muzz.world - sandbox</h1>
            <div className="grid gap-2 md:grid-cols-4">
              <Metric label="Bid" value={formatMoney(data.quotes.bid)} />
              <Metric label="Ask" value={formatMoney(data.quotes.ask)} />
              <Metric label="Mid" value={formatMoney(data.quotes.mid)} />
              <Metric label="Spread" value={formatPercent(data.quotes.spread_bps, 2).replace("%", " bps")} />
            </div>
          </div>
          <div className="mt-3 flex flex-wrap gap-x-3 gap-y-1.5 text-xs uppercase tracking-[0.22em] text-zinc-500">
            <span>{data.quotes.feed_label}</span>
            <span className="font-semibold text-zinc-100">{data.quotes.feed_detail}</span>
            <span>Build {data.app.build_label}</span>
            <span>{data.app.build_modified_at}</span>
            <span className="font-semibold text-zinc-100">Runtime {formatRuntime(data.app.runtime_seconds)}</span>
            {state.error ? <span className="text-rose-300">State fetch error: {state.error}</span> : null}
          </div>
        </header>

        <Panel title="Account Rail" kicker="risk + timing">
          <div className="grid gap-2 md:grid-cols-4 xl:grid-cols-7">
            <Metric label="Available" value={`${formatMoney(data.account.available)} USDC`} />
            <Metric label="Equity" value={`${formatMoney(data.account.equity)} USDC`} />
            <Metric label="Live PnL" value={`${formatSigned(data.account.live_pnl)} USDC`} tone={toneClass(data.account.live_pnl)} />
            <Metric label="Next Cycle" value={formatCountdown(data.account.next_signal_in)} />
            <Metric label="Position" value={data.account.position_side} />
            <Metric label="Lev / Stack" value={`${data.account.leverage.toFixed(1)}x / ${(data.account.stack_fraction * 100).toFixed(1)}%`} />
            <Metric label="Stop Loss" value={`${formatMoney(data.account.stop_loss_usdc)} USDC`} />
          </div>
        </Panel>

        <Panel title="Consensus Pulse" kicker="score driven">
          <div className="space-y-3">
            <div className="rounded-[26px] border border-cyan-400/10 bg-cyan-400/[0.04] px-4 py-3">
              <div className="flex flex-wrap items-baseline gap-x-4 gap-y-2">
                <div className="text-[10px] uppercase tracking-[0.32em] text-cyan-200/70">Consensus</div>
                <div className={`font-mono text-3xl font-semibold ${consensusTone}`}>{data.signal.last_signal}</div>
                <div className="font-mono text-sm text-zinc-400">
                  Score {data.signal.last_signal_score >= 0 ? "+" : ""}{data.signal.last_signal_score} | threshold +/-2
                </div>
                <div className="font-mono text-xs text-zinc-500">Last at {formatClock(data.signal.last_signal_at)}</div>
              </div>
              <p className="mt-2 text-sm leading-5 text-zinc-300">
                {data.signal.last_signal_why || "Waiting for the first complete model cycle."}
              </p>
              {data.signal.last_error ? <p className="mt-2 text-sm leading-5 text-rose-300">{data.signal.last_error}</p> : null}
            </div>
            <div className="grid gap-2 md:grid-cols-2">
              <div className="rounded-[24px] border border-white/10 bg-white/[0.03] px-4 py-3">
                <div className="text-[10px] uppercase tracking-[0.28em] text-zinc-500">Fear &amp; Greed</div>
                <div className="mt-1.5 flex items-baseline gap-2">
                  <div className="font-mono text-2xl font-semibold text-zinc-100">{fearGreed.value || "n/a"}</div>
                  <div className="text-sm text-zinc-400">{fearGreed.classification || "waiting"}</div>
                </div>
                <div className="mt-2 text-xs uppercase tracking-[0.24em] text-zinc-500">
                  Background {fearGreed.signal || "n/a"}
                </div>
              </div>
              <div className="rounded-[24px] border border-white/10 bg-white/[0.03] px-4 py-3">
                <div className="text-[10px] uppercase tracking-[0.28em] text-zinc-500">Twitter/X BTC 15m</div>
                {twitter.available ? (
                  <>
                    <div className="mt-1.5 flex flex-wrap items-baseline gap-x-3 gap-y-1">
                      <div className="font-mono text-2xl font-semibold text-zinc-100">{twitter.valid_tweet_count}</div>
                      <div className="text-sm text-zinc-400">valid tweets</div>
                      <div className={`font-mono text-sm ${twitterTone}`}>{twitter.signal || "NO_TRADE"}</div>
                    </div>
                    <div className="mt-2 text-sm text-zinc-300">
                      Bull {twitter.bullish_pct.toFixed(1)}% | Bear {twitter.bearish_pct.toFixed(1)}% | Avg {twitter.avg_score >= 0 ? "+" : ""}
                      {twitter.avg_score.toFixed(2)}
                    </div>
                    <div className="mt-1 text-xs text-zinc-500">
                      Baseline {twitter.baseline_window_count} windows
                      {twitter.delta_bullish_pct !== null ? ` | Bull delta ${twitter.delta_bullish_pct >= 0 ? "+" : ""}${twitter.delta_bullish_pct.toFixed(1)} pts` : ""}
                      {twitter.delta_avg_score !== null ? ` | Score delta ${twitter.delta_avg_score >= 0 ? "+" : ""}${twitter.delta_avg_score.toFixed(2)}` : ""}
                    </div>
                  </>
                ) : (
                  <div className="mt-2 text-sm text-zinc-500">{twitter.unavailable_reason || "Waiting for next cycle."}</div>
                )}
              </div>
            </div>
            <div className="text-[10px] uppercase tracking-[0.32em] text-cyan-200/70">Consensus</div>
            <div className="grid gap-2.5 xl:grid-cols-5">
              {data.signal.providers.map((provider) => <ProviderCard key={provider.provider} provider={provider} />)}
            </div>
          </div>
        </Panel>

        <Panel title="Open Position + Logs" kicker="live context">
          <div className="grid gap-3 xl:grid-cols-[300px_minmax(0,1fr)]">
            <div className="rounded-3xl border border-white/10 bg-white/[0.03] px-4 py-3">
              <div className="text-[10px] uppercase tracking-[0.28em] text-zinc-500">Open position</div>
              {data.position ? (
                <div className="mt-2 space-y-1.5 text-sm text-zinc-300">
                  <div className="font-mono text-xl text-zinc-100">{data.position.side} {data.position.coin}</div>
                  <div>Entry {formatMoney(data.position.entry_price)} | Size {formatMoney(data.position.size, 6)}</div>
                  <div>Notional {formatMoney(data.position.notional)} | Margin {formatMoney(data.position.initial_margin)}</div>
                  <div>Opened {formatClock(data.position.entry_time)}</div>
                </div>
              ) : <div className="mt-2 text-sm text-zinc-500">No active position.</div>}
            </div>
            <div className="rounded-3xl border border-white/10 bg-white/[0.03] px-4 py-3">
              <div className="mb-2 text-[10px] uppercase tracking-[0.28em] text-zinc-500">Action log</div>
              <div className="max-h-[220px] space-y-1.5 overflow-y-auto pr-2 font-mono text-sm text-zinc-300">
                {data.logs.length === 0 ? <div className="text-zinc-500">No logs yet.</div> : data.logs.map((line) => <div key={line} className="rounded-2xl border border-white/5 bg-black/20 px-3 py-1.5">{line}</div>)}
              </div>
            </div>
          </div>
        </Panel>

        <Panel title="Recent Trades" kicker="fills + fees">
          <TradesTable trades={data.trades} />
        </Panel>

        <Panel title="15-Minute Strategy Returns" kicker="after fees">
          <StrategyTable rows={strategyReturns.data.rows} />
        </Panel>
      </div>
    </main>
  );
}
