use serde::{Deserialize, Serialize};
use serde_json::Value;

// ── Orders ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrdersResponse {
    #[serde(default)]
    pub orders: Vec<Order>,
    #[serde(default)]
    pub length: i64,
    pub success: bool,
    #[serde(default)]
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Order {
    #[serde(default)]
    pub id: i64,
    #[serde(default)]
    pub contingent_id: Option<i64>,
    #[serde(default)]
    pub broker_id: Vec<String>,
    #[serde(default)]
    pub symbol: Option<Symbol>,
    #[serde(default)]
    pub limit_price: Option<f64>,
    #[serde(default)]
    pub stop_price: Option<f64>,
    #[serde(default)]
    pub stop_triggered: Option<bool>,
    #[serde(default)]
    pub price: f64,
    #[serde(default)]
    pub price_currency: Option<String>,
    #[serde(default)]
    pub time: Option<String>,
    #[serde(default)]
    pub created_time: Option<String>,
    #[serde(default)]
    pub last_fill_time: Option<String>,
    #[serde(default)]
    pub last_update_time: Option<String>,
    #[serde(default)]
    pub canceled_time: Option<String>,
    #[serde(default)]
    pub quantity: f64,
    /// 0=Market,1=Limit,2=StopMarket,3=StopLimit,4=MarketOnOpen,
    /// 5=MarketOnClose,6=OptionExercise,7=LimitIfTouched,
    /// 8=ComboMarket,9=ComboLimit,10=ComboLegLimit,11=TrailingStop
    #[serde(default, rename = "type")]
    pub order_type: i64,
    /// 0=New,1=Submitted,2=PartiallyFilled,3=Filled,5=Canceled,
    /// 6=None,7=Invalid,8=CancelPending,9=UpdateSubmitted
    #[serde(default)]
    pub status: i64,
    #[serde(default)]
    pub tag: Option<String>,
    /// 0=Base,1=Equity,2=Option,3=Commodity,4=Forex,5=Future,6=Cfd,
    /// 7=Crypto,8=FutureOption,9=Index,10=IndexOption,11=CryptoFuture
    #[serde(default)]
    pub security_type: Option<i64>,
    /// 0=Buy, 1=Sell, 2=Hold
    #[serde(default)]
    pub direction: i64,
    #[serde(default)]
    pub value: f64,
    #[serde(default)]
    pub is_marketable: Option<bool>,
    #[serde(default)]
    pub order_submission_data: Option<Value>,
    #[serde(default)]
    pub properties: Option<Value>,
    #[serde(default)]
    pub events: Vec<OrderEvent>,
    #[serde(default)]
    pub trailing_amount: Option<f64>,
    #[serde(default)]
    pub trailing_percentage: Option<bool>,
    #[serde(default)]
    pub trigger_price: Option<f64>,
    #[serde(default)]
    pub trigger_touched: Option<bool>,
    #[serde(default)]
    pub group_order_manager: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Symbol {
    #[serde(default)]
    pub value: String,
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub permtick: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderEvent {
    #[serde(default)]
    pub algorithm_id: Option<String>,
    #[serde(default)]
    pub symbol: Option<String>,
    #[serde(default)]
    pub symbol_value: Option<String>,
    #[serde(default)]
    pub symbol_permtick: Option<String>,
    #[serde(default)]
    pub order_id: i64,
    #[serde(default)]
    pub order_event_id: i64,
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub order_fee_amount: Option<f64>,
    #[serde(default)]
    pub order_fee_currency: Option<String>,
    #[serde(default)]
    pub fill_price: f64,
    #[serde(default)]
    pub fill_price_currency: Option<String>,
    #[serde(default)]
    pub fill_quantity: f64,
    #[serde(default)]
    pub direction: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub is_assignment: Option<bool>,
    #[serde(default)]
    pub stop_price: Option<f64>,
    #[serde(default)]
    pub limit_price: Option<f64>,
    #[serde(default)]
    pub quantity: f64,
    #[serde(default)]
    pub time: Option<Value>, // can be datetime string or unix int
    #[serde(default)]
    pub is_in_the_money: Option<bool>,
}

// Insights are in backtest_insights.rs
