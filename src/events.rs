use chrono::{DateTime, Utc};

#[derive(Clone)]
pub enum Event {
    BankTransactionIssued(BankTransactionIssuedPayload),
    PaymentAuthorized(PaymentAuthorizedPayload),
    PaymentCollected(PaymentCollectedPayload),
    ProductOrdered(ProductOrderedPayload),
}

#[derive(Clone)]
pub struct BankTransactionIssuedPayload {
    pub transaction_id: String,
    pub amount: f64,
    pub occurred_on: DateTime<Utc>,
}

#[derive(Clone)]
pub struct PaymentAuthorizedPayload {
    pub order_id: String,
    pub payment_id: String,
    pub amount: f64,
    pub occurred_on: DateTime<Utc>,
}

#[derive(Clone)]
pub struct PaymentCollectedPayload {
    pub order_id: String,
    pub transaction_id: String,
    pub amount: f64,
    pub occurred_on: DateTime<Utc>,
}

#[derive(Clone)]
pub struct ProductOrderedPayload {
    pub order_id: String,
    pub amount: f64,
    pub guarantees: Vec<Guarantee>,
    pub occurred_on: DateTime<Utc>,
}

#[derive(Clone)]
pub struct Guarantee {
    pub guarantee_type: String,
    pub price: f64,
}
