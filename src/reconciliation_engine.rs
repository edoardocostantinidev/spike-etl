type Client = PooledConnection<PostgresConnectionManager<NoTls>>;

use postgres::NoTls;
use r2d2_postgres::{r2d2::PooledConnection, PostgresConnectionManager};

use crate::events::{
    BankTransactionIssuedPayload, Event, PaymentAuthorizedPayload, PaymentCollectedPayload,
    ProductOrderedPayload,
};

pub struct ReconciliationEngine {}

impl ReconciliationEngine {
    pub fn new() -> Self {
        Self {}
    }

    pub fn reconcile(&self, event: Event) -> Result<(), String> {
        let mut client = crate::pool::POOL.get().unwrap();
        let result = match event {
            Event::BankTransactionIssued(payload) => {
                save_bank_transaction_issued(&mut client, payload.clone())?;
                let query = "
                SELECT bt.transaction_id, po.order_id, bt.amount, po.amount
                FROM bank_transactions bt, payment_collections pc, product_orders po, payment_authorizations pa 
                WHERE bt.transaction_id = $1
                AND bt.transaction_id = pc.transaction_id
                AND pc.payment_id = pa.payment_id
                AND pa.order_id = po.order_id";
                client.query(query, &[&payload.transaction_id])
            }
            Event::PaymentAuthorized(payload) => {
                save_payment_authorized(&mut client, payload.clone())?;
                let query = "
                SELECT bt.transaction_id, po.order_id, bt.amount, po.amount
                FROM bank_transactions bt, payment_collections pc, product_orders po, payment_authorizations pa 
                WHERE pa.payment_id = $1
                AND bt.transaction_id = pc.transaction_id
                AND pc.payment_id = pa.payment_id
                AND pa.order_id = po.order_id";
                client.query(query, &[&payload.payment_id])
            }
            Event::PaymentCollected(payload) => {
                save_payment_collected(&mut client, payload.clone())?;
                let query = "
                SELECT bt.transaction_id, po.order_id, bt.amount, po.amount
                FROM bank_transactions bt, payment_collections pc, product_orders po, payment_authorizations pa 
                WHERE pc.payment_id = $1
                AND bt.transaction_id = pc.transaction_id
                AND pc.payment_id = pa.payment_id
                AND pa.order_id = po.order_id";
                client.query(query, &[&payload.payment_id])
            }
            Event::ProductOrdered(payload) => {
                save_product_ordered(&mut client, payload.clone())?;
                let query = "
                SELECT bt.transaction_id, po.order_id, bt.amount, po.amount
                FROM bank_transactions bt, payment_collections pc, product_orders po, payment_authorizations pa 
                WHERE po.order_id = $1
                AND bt.transaction_id = pc.transaction_id
                AND pc.payment_id = pa.payment_id
                AND pa.order_id = po.order_id";
                client.query(query, &[&payload.order_id])
            }
        };

        if let Some(x) = result.unwrap().get(0) {
            let (t_id, o_id, collected_amount, ordered_amount): (String, String, f64, f64) =
                (x.get(0), x.get(1), x.get(2), x.get(3));
            client
                .execute(
                    r"UPDATE bank_transactions SET ordered_amount = (ordered_amount + $2) WHERE transaction_id = $1",
                    &[&t_id,&ordered_amount],
                )
                .map_err(|e| e.to_string())?;
            client
                .execute(
                    r"UPDATE product_orders SET collected_amount = $2 WHERE order_id = $1",
                    &[&o_id, &collected_amount],
                )
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }
}

fn save_bank_transaction_issued(
    client: &mut Client,
    payload: BankTransactionIssuedPayload,
) -> Result<(), String> {
    client
        .execute(
            r"INSERT INTO bank_transactions (transaction_id, amount,occurred_on) VALUES($1,$2,$3)",
            &[
                &payload.transaction_id,
                &payload.amount,
                &payload.occurred_on.to_string(),
            ],
        )
        .map(|_| ())
        .map_err(|e| e.to_string())
}

fn save_product_ordered(client: &mut Client, payload: ProductOrderedPayload) -> Result<(), String> {
    client.execute(r"
     INSERT INTO product_orders (order_id, amount,occurred_on, event_type, installment_type, insurance_code) 
     VALUES($1,$2,$3,$4,$5,$6)
     ", &[
        &payload.order_id,
        &payload.amount,
        &payload.occurred_on.to_string(),
        &payload.event_type.to_string(),
        &payload.installment_type.to_string(),
        &payload.insurance_code,
     ])
    .map_err(|e| e.to_string())
    .map(|_| ())
}

fn save_payment_collected(
    client: &mut Client,
    payload: PaymentCollectedPayload,
) -> Result<(), String> {
    client
        .execute(
            r"
    INSERT INTO payment_collections (payment_id, transaction_id,amount,occurred_on) 
    VALUES($1,$2,$3,$4)
    ",
            &[
                &payload.payment_id,
                &payload.transaction_id,
                &payload.amount,
                &payload.occurred_on.to_string(),
            ],
        )
        .map_err(|e| e.to_string())
        .map(|_| ())
}

fn save_payment_authorized(
    client: &mut Client,
    payload: PaymentAuthorizedPayload,
) -> Result<(), String> {
    client
        .execute(
            r"
    INSERT INTO payment_authorizations (payment_id, order_id,amount,occurred_on) 
    VALUES($1,$2,$3,$4)",
            &[
                &payload.payment_id,
                &payload.order_id,
                &payload.amount,
                &payload.occurred_on.to_string(),
            ],
        )
        .map_err(|e| e.to_string())
        .map(|_| ())
}
