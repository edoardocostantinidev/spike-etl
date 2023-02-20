use chrono::{DateTime, Utc};

#[derive(Clone)]
pub enum Event {
    BankTransactionIssued(BankTransactionIssuedPayload),
    PaymentAuthorized(PaymentAuthorizedPayload),
    PaymentCollected(PaymentCollectedPayload),
    ProductOrdered(ProductOrderedPayload),
}

#[derive(Clone)]
pub enum EventType {
    Issuance,
    Cancellation,
    Interruption,
}

impl std::fmt::Display for EventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventType::Issuance => f.write_str("issuance"),
            EventType::Cancellation => f.write_str("cancellation"),
            EventType::Interruption => f.write_str("interruption"),
        }
    }
}

#[derive(Clone)]
pub enum InstallmentType {
    Yearly,
    BiYearly,
    Monthly,
}

impl std::fmt::Display for InstallmentType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InstallmentType::Yearly => f.write_str("yearly"),
            InstallmentType::BiYearly => f.write_str("bi_yearly"),
            InstallmentType::Monthly => f.write_str("monthly"),
        }
    }
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
    pub payment_id: String,
    pub transaction_id: String,
    pub amount: f64,
    pub occurred_on: DateTime<Utc>,
}

#[derive(Clone)]
pub struct ProductOrderedPayload {
    pub order_id: String,
    pub amount: f64,
    pub event_type: EventType,
    pub installment_type: InstallmentType,
    pub guarantees: Vec<Guarantee>,
    pub occurred_on: DateTime<Utc>,
    pub insurance_code: String,
}

#[derive(Clone)]
pub struct Guarantee {
    pub guarantee_type: String,
    pub price: f64,
}
