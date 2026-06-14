export interface QuoteState {
  bid: number;
  ask: number;
  mid: number;
  spread_bps: number;
  last_tick_at: number;
  feed_label: string;
  feed_detail: string;
  feed_style: string;
}

export interface AccountState {
  available: number;
  equity: number;
  live_pnl: number;
  position_side: string;
  next_signal_at: number;
  next_signal_in: number;
  leverage: number;
  stack_fraction: number;
  stop_loss_usdc: number;
}

export interface ProviderState {
  provider: string;
  signal: string;
  why: string;
  model: string;
  elapsed_seconds: number;
  error: string;
}

export interface FearGreedState {
  value: string;
  classification: string;
  signal: string;
}

export interface TwitterSentimentState {
  available: boolean;
  tweet_count: number;
  valid_tweet_count: number;
  bullish_count: number;
  bearish_count: number;
  neutral_count: number;
  bullish_pct: number;
  bearish_pct: number;
  neutral_pct: number;
  avg_score: number;
  signal: string;
  baseline_window_count: number;
  delta_bullish_pct: number | null;
  delta_avg_score: number | null;
  unavailable_reason: string;
  summary: string;
  window_minutes: number;
}

export interface SignalState {
  last_signal: string;
  last_signal_why: string;
  last_signal_score: number;
  last_signal_at: number;
  last_signal_sources: string[];
  last_error: string;
  background: {
    fear_greed: FearGreedState;
    twitter_btc_15m: TwitterSentimentState;
  };
  providers: ProviderState[];
}

export interface TradeState {
  timestamp: number;
  side: string;
  reason: string;
  entry_price: number;
  exit_price: number;
  entry_usdc: number;
  exit_usdc: number;
  gross_pnl: number;
  fees_paid: number;
  net_pnl: number;
  seconds_open: number;
}

export interface PositionState {
  coin: string;
  side: string;
  entry_price: number;
  size: number;
  notional: number;
  initial_margin: number;
  entry_fee: number;
  entry_time: number;
}

export interface AppStateResponse {
  app: {
    title: string;
    subtitle: string;
    build_label: string;
    build_modified_at: string;
    runtime_seconds: number;
  };
  quotes: QuoteState;
  account: AccountState;
  signal: SignalState;
  position: PositionState | null;
  trades: TradeState[];
  logs: string[];
}

export interface CsvRowsResponse {
  rows: Array<Record<string, string>>;
}
