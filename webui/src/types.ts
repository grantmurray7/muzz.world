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

export interface SignalState {
  last_signal: string;
  last_signal_why: string;
  last_signal_score: number;
  last_signal_at: number;
  last_signal_sources: string[];
  last_error: string;
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
