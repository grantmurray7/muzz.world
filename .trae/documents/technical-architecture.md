## 1. Architecture Design
```mermaid
flowchart LR
    A["Python Launcher"] --> B["Engine Coordinator"]
    B --> C["Market Feed Service"]
    B --> D["Signal Service"]
    B --> E["Sandbox Trade Service"]
    B --> F["Persistence Service"]
    B --> G["Web API Server"]
    G --> H["React Frontend"]
    C --> I["Hyperliquid APIs"]
    D --> J["AI Provider APIs"]
    D --> K["Fear & Greed API"]
    F --> L["CSV History Files"]
    F --> M["State JSON"]
    H --> N["Browser WebSocket Client"]
    N --> G
```

## 2. Technology Description
- Frontend: React@18 + Tailwind CSS@3 + Vite, desktop-first local dashboard.
- Initialization Tool: Vite.
- Backend: FastAPI + Uvicorn for HTTP routes, WebSocket updates, and local browser launch coordination.
- Engine runtime: Python standard-library-first services plus existing `websocket-client` usage for Hyperliquid streaming.
- Data storage: existing CSV files and JSON state files; no external database required.
- External services: Hyperliquid market APIs, five AI provider APIs, Fear & Greed API.

## 3. Route Definitions
| Route | Purpose |
|-------|---------|
| / | Main live dashboard with quotes, account state, model signals, trades, and logs |
| /analytics | Strategy analytics view with per-window returns and cumulative comparison |
| /api/state | Returns current engine state snapshot for initial page hydration |
| /api/trades | Returns recent trades from memory or CSV history |
| /api/strategy-returns | Returns completed 15-minute strategy return rows |
| /api/ai-responses | Returns recent AI decision history for deeper inspection |
| /ws/state | WebSocket endpoint for live state and incremental updates |

## 4. API Definitions
```ts
type SignalValue = "LONG" | "SHORT" | "NO_TRADE" | "PENDING";

interface QuoteState {
  bid: number;
  ask: number;
  mid: number;
  spreadBps: number;
  lastTickAt: number;
  lastBookAt: number;
  feedLabel: string;
}

interface ProviderSignal {
  provider: "gemini" | "openai" | "claude" | "perplexity" | "grok" | "consensus";
  signal: SignalValue;
  why: string;
  model?: string;
  error?: string;
}

interface TradeRow {
  timestamp: number;
  side: "LONG" | "SHORT";
  reason: string;
  entryPrice: number;
  exitPrice: number;
  entryUsd: number;
  exitUsd: number;
  grossPnl: number;
  feesPaid: number;
  netPnl: number;
  secondsOpen: number;
}

interface StrategyReturnRow {
  periodStartTs: number;
  periodEndTs: number;
  entryPrice: number;
  exitPrice: number;
  btcMovePct: number;
  roundTripFeePct: number;
  geminiSignal: SignalValue;
  geminiReturnPct: number;
  openaiSignal: SignalValue;
  openaiReturnPct: number;
  claudeSignal: SignalValue;
  claudeReturnPct: number;
  perplexitySignal: SignalValue;
  perplexityReturnPct: number;
  grokSignal: SignalValue;
  grokReturnPct: number;
  consensusSignal: SignalValue;
  consensusReturnPct: number;
}

interface AppStateResponse {
  quotes: QuoteState;
  available: number;
  equity: number;
  livePnl: number;
  position: Record<string, unknown> | null;
  nextSignalAt: number;
  lastSignal: SignalValue;
  providers: ProviderSignal[];
  trades: TradeRow[];
  logs: string[];
}
```

## 5. Server Architecture Diagram
```mermaid
flowchart TD
    A["FastAPI Router"] --> B["State Controller"]
    A --> C["History Controller"]
    B --> D["Engine State Store"]
    C --> E["Persistence Service"]
    D --> F["Market Feed Service"]
    D --> G["Signal Service"]
    D --> H["Sandbox Trade Service"]
    E --> I["CSV Files"]
    E --> J["State JSON"]
```

## 6. Data Model
### 6.1 Data Model Definition
```mermaid
erDiagram
    APP_STATE ||--o{ PROVIDER_SIGNAL : contains
    APP_STATE ||--o{ TRADE_ROW : shows
    APP_STATE ||--o{ LOG_ENTRY : streams
    APP_STATE ||--o{ STRATEGY_RETURN_ROW : summarizes
    PROVIDER_SIGNAL {
        string provider
        string signal
        string why
        string model
        string error
    }
    TRADE_ROW {
        float timestamp
        string side
        string reason
        float entry_price
        float exit_price
        float entry_usd
        float exit_usd
        float gross_pnl
        float fees_paid
        float net_pnl
    }
    STRATEGY_RETURN_ROW {
        float period_start_ts
        float period_end_ts
        float entry_price
        float exit_price
        float btc_move_pct
        float gemini_return_pct
        float openai_return_pct
        float claude_return_pct
        float perplexity_return_pct
        float grok_return_pct
        float consensus_return_pct
    }
```

### 6.2 Data Definition Language
```sql
-- CSV-backed logical schemas retained; no external database required.
-- strategy_returns.csv
period_start_utc TEXT,
period_start_ts REAL,
period_end_utc TEXT,
period_end_ts REAL,
entry_price REAL,
exit_price REAL,
btc_move_pct REAL,
round_trip_fee_pct REAL,
gemini_signal TEXT,
gemini_return_pct REAL,
openai_signal TEXT,
openai_return_pct REAL,
claude_signal TEXT,
claude_return_pct REAL,
perplexity_signal TEXT,
perplexity_return_pct REAL,
grok_signal TEXT,
grok_return_pct REAL,
consensus_signal TEXT,
consensus_return_pct REAL;

-- trades.csv and ai_responses.csv remain append-only and are exposed through API adapters.
```

