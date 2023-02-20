use sqlite::{Connection, State, Value};

use crate::events::{
    BankTransactionIssuedPayload, Event, PaymentAuthorizedPayload, PaymentCollectedPayload,
    ProductOrderedPayload,
};

pub struct ReconciliationEngine<'a> {
    pub connection: &'a sqlite::Connection,
}

impl<'a> ReconciliationEngine<'a> {
    pub fn new(connection: &'a sqlite::Connection) -> Self {
        Self { connection }
    }

    pub fn reconcile(&self, event: Event) -> Result<(), String> {
        let mut statement = match event {
            Event::BankTransactionIssued(payload) => {
                save_bank_transaction_issued(self.connection, payload.clone())?;
                let query = "
                SELECT bt.transaction_id, po.order_id, pc.payment_id 
                FROM bank_transactions bt, payment_collections pc, product_orders po, payment_authorizations pa 
                WHERE bt.transaction_id = :id
                AND bt.transaction_id = pc.transaction_id
                AND pc.payment_id = pa.payment_id
                AND pa.order_id = po.order_id";
                let mut statement = self.connection.prepare(query).unwrap();
                statement
                    .bind::<(_, Value)>((":id", payload.transaction_id.into()))
                    .unwrap();
                statement
            }
            Event::PaymentAuthorized(payload) => {
                save_payment_authorized(self.connection, payload.clone())?;
                let query = "
                SELECT bt.transaction_id, po.order_id, pc.payment_id 
                FROM bank_transactions bt, payment_collections pc, product_orders po, payment_authorizations pa 
                WHERE pa.payment_id = :id
                AND bt.transaction_id = pc.transaction_id
                AND pc.payment_id = pa.payment_id
                AND pa.order_id = po.order_id";
                let mut statement = self.connection.prepare(query).unwrap();
                statement
                    .bind::<(_, Value)>((":id", payload.payment_id.into()))
                    .unwrap();
                statement
            }
            Event::PaymentCollected(payload) => {
                save_payment_collected(self.connection, payload.clone())?;
                let query = "
                SELECT bt.transaction_id, po.order_id, pc.payment_id 
                FROM bank_transactions bt, payment_collections pc, product_orders po, payment_authorizations pa 
                WHERE pc.payment_id = :id
                AND bt.transaction_id = pc.transaction_id
                AND pc.payment_id = pa.payment_id
                AND pa.order_id = po.order_id";
                let mut statement = self.connection.prepare(query).unwrap();
                statement
                    .bind::<(_, Value)>((":id", payload.payment_id.into()))
                    .unwrap();
                statement
            }
            Event::ProductOrdered(payload) => {
                save_product_ordered(self.connection, payload.clone())?;
                let query = "
                SELECT bt.transaction_id, po.order_id, pc.payment_id 
                FROM bank_transactions bt, payment_collections pc, product_orders po, payment_authorizations pa 
                WHERE po.order_id = :id
                AND bt.transaction_id = pc.transaction_id
                AND pc.payment_id = pa.payment_id
                AND pa.order_id = po.order_id";
                let mut statement = self.connection.prepare(query).unwrap();
                statement
                    .bind::<(_, Value)>((":id", payload.order_id.into()))
                    .unwrap();
                statement
            }
        };

        if let Ok(State::Row) = statement.next() {
            let (t_id, o_id, _p_id): (String, String, String) = (
                statement.read::<String, usize>(0).unwrap().into(),
                statement.read::<String, usize>(1).unwrap().into(),
                statement.read::<String, usize>(2).unwrap().into(),
            );
            //if amounts concile
            let mut statement = self
                .connection
                .prepare(r"UPDATE bank_transactions SET reconciled = 1 WHERE transaction_id = :id")
                .unwrap();
            statement
                .bind::<(_, Value)>((":id", t_id.into()))
                .map_err(|e| e.message.unwrap_or_default())?;
            statement
                .next()
                .map_err(|e| e.message.unwrap_or_default())?;

            let mut statement = self
                .connection
                .prepare(r"UPDATE product_orders SET reconciled = 1 WHERE order_id = :id")
                .unwrap();
            statement
                .bind::<(_, Value)>((":id", o_id.into()))
                .map_err(|e| e.message.unwrap_or_default())?;
            statement
                .next()
                .map_err(|e| e.message.unwrap_or_default())?;
        }
        Ok(())
    }
}

fn save_bank_transaction_issued(
    conn: &Connection,
    payload: BankTransactionIssuedPayload,
) -> Result<(), String> {
    let mut s = conn.prepare(r"INSERT INTO bank_transactions (transaction_id, amount,occurred_on) VALUES(:id, :amount, :occurred_on)")
    .unwrap();
    s.bind::<&[(_, Value)]>(&[
        (":id", payload.transaction_id.into()),
        (":amount", payload.amount.into()),
        (":occurred_on", payload.occurred_on.to_string().into()),
    ])
    .map_err(|e| e.message.unwrap_or_default())?;
    s.next().map_err(|e| e.to_string()).map(|_| ())
}

fn save_product_ordered(conn: &Connection, payload: ProductOrderedPayload) -> Result<(), String> {
    let mut s = conn.prepare(r"INSERT INTO product_orders (order_id, amount,occurred_on, event_type, installment_type, insurance_code) VALUES(:id, :amount, :occurred_on, :event_type, :installment_type, :insurance_code)")
    .unwrap();
    s.bind::<&[(_, Value)]>(&[
        (":id", payload.order_id.into()),
        (":amount", payload.amount.into()),
        (":occurred_on", payload.occurred_on.to_string().into()),
        (":event_type", payload.event_type.to_string().into()),
        (
            ":installment_type",
            payload.installment_type.to_string().into(),
        ),
        (":insurance_code", payload.insurance_code.to_string().into()),
    ])
    .map_err(|e| e.message.unwrap_or_default())?;
    s.next().map_err(|e| e.to_string()).map(|_| ())
}

fn save_payment_collected(
    conn: &Connection,
    payload: PaymentCollectedPayload,
) -> Result<(), String> {
    let mut s = conn.prepare(r"INSERT INTO payment_collections (payment_id, transaction_id,amount,occurred_on) VALUES(:payment_id, :transaction_id, :amount, :occurred_on)")
    .unwrap();
    s.bind::<&[(_, Value)]>(&[
        (":payment_id", payload.payment_id.into()),
        (":transaction_id", payload.transaction_id.into()),
        (":amount", payload.amount.into()),
        (":occurred_on", payload.occurred_on.to_string().into()),
    ])
    .map_err(|e| e.message.unwrap_or_default())?;
    s.next().map_err(|e| e.to_string()).map(|_| ())
}

fn save_payment_authorized(
    conn: &Connection,
    payload: PaymentAuthorizedPayload,
) -> Result<(), String> {
    let mut s = conn.prepare(r"INSERT INTO payment_authorizations (payment_id, order_id,amount,occurred_on) VALUES(:payment_id, :order_id, :amount, :occurred_on)")
    .unwrap();
    s.bind::<&[(_, Value)]>(&[
        (":payment_id", payload.payment_id.into()),
        (":order_id", payload.order_id.into()),
        (":amount", payload.amount.into()),
        (":occurred_on", payload.occurred_on.to_string().into()),
    ])
    .map_err(|e| e.message.unwrap_or_default())?;
    s.next().map_err(|e| e.to_string()).map(|_| ())
}
