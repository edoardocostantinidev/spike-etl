type Client = PooledConnection<PostgresConnectionManager<NoTls>>;

use postgres::{NoTls, Row, Transaction};
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

    pub fn reconcile(&self, event: Event) -> Result<(), postgres::Error> {
        let mut client = crate::pool::POOL.get().unwrap();
        match event {
            Event::BankTransactionIssued(payload) => {
                save_bank_transaction_issued(&mut client, payload.clone())?;
                reconciliate_bank_transaction_issued(&mut client, payload.clone())?;
            }
            Event::PaymentAuthorized(payload) => {
                save_payment_authorized(&mut client, payload.clone())?;
                reconciliate_payment_authorized(&mut client, payload.clone())?;
            }
            Event::PaymentCollected(payload) => {
                save_payment_collected(&mut client, payload.clone())?;
                reconciliate_payment_collected(&mut client, payload)?;
            }
            Event::ProductOrdered(payload) => {
                save_product_ordered(&mut client, payload.clone())?;
                reconciliate_product_ordered(&mut client, payload)?;
            }
        };

        Ok(())
    }
}

fn reconciliate_bank_transaction_issued(
    client: &mut Client,
    payload: BankTransactionIssuedPayload,
) -> Result<(), postgres::Error> {
    let mut t = client.transaction().unwrap();
    let rows = t
        .query(
            r"SELECT transaction_id, order_id, payment_id 
        FROM relations 
        WHERE transaction_id=$1
        AND order_id IS NOT NULL
        AND payment_id IS NOT NULL",
            &[&payload.transaction_id],
        )
        .unwrap();

    do_reconcile(&mut t, rows)?;
    t.commit()
}

fn reconciliate_product_ordered(
    client: &mut Client,
    payload: ProductOrderedPayload,
) -> Result<(), postgres::Error> {
    let mut t = client.transaction().unwrap();
    let rows = t
        .query(
            r"SELECT transaction_id, order_id, payment_id 
        FROM relations 
        WHERE order_id=$1
        AND transaction_id IS NOT NULL
        AND payment_id IS NOT NULL",
            &[&payload.order_id],
        )
        .unwrap();
    do_reconcile(&mut t, rows)?;
    t.commit()
}

fn reconciliate_payment_authorized(
    client: &mut Client,
    payload: PaymentAuthorizedPayload,
) -> Result<(), postgres::Error> {
    let mut t = client.transaction().unwrap();
    let rows = t
        .query(
            r"SELECT r.transaction_id, r.order_id, r.payment_id 
        FROM relations r, product_orders po
        WHERE r.order_id=$1
        AND po.order_id=$1
        AND transaction_id IS NOT NULL
        AND payment_id=$2",
            &[&payload.order_id, &payload.payment_id],
        )
        .unwrap();
    do_reconcile(&mut t, rows)?;
    t.commit()
}

fn reconciliate_payment_collected(
    client: &mut Client,
    payload: PaymentCollectedPayload,
) -> Result<(), postgres::Error> {
    let mut t = client.transaction().unwrap();
    let rows = t
        .query(
            r"SELECT r.transaction_id, r.order_id, r.payment_id 
        FROM relations r, bank_transactions bt
        WHERE r.transaction_id=$1
        AND bt.transaction_id=$1
        AND r.order_id IS NOT NULL
        AND r.payment_id=$2",
            &[&payload.transaction_id, &payload.payment_id],
        )
        .unwrap();
    do_reconcile(&mut t, rows)?;
    t.commit()
}

fn do_reconcile(t: &mut Transaction, rows: Vec<Row>) -> Result<(), postgres::Error> {
    rows.into_iter()
        .map(|x| (x.get(0), x.get(1), x.get(2)))
        .for_each(|(t_id, o_id, p_id): (String, String, String)| {
            let _ = t.query(
                r"UPDATE bank_transactions
        SET ordered_amount = ordered_amount + (
            SELECT po.amount 
            FROM product_orders po
            WHERE po.order_id=$1
        ) 
        WHERE transaction_id=$2",
                &[&o_id, &t_id],
            );
            let _ = t.query(
                r"UPDATE product_orders
        SET collected_amount = collected_amount + (
            SELECT pc.amount 
            FROM payment_collections pc 
            WHERE pc.payment_id=$1
        )
        WHERE order_id=$2",
                &[&p_id, &o_id],
            );
        });
    Ok(())
}

fn save_bank_transaction_issued(
    client: &mut Client,
    payload: BankTransactionIssuedPayload,
) -> Result<(), postgres::Error> {
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
}

fn save_product_ordered(
    client: &mut Client,
    payload: ProductOrderedPayload,
) -> Result<(), postgres::Error> {
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
    .map(|_| ())
}

fn save_payment_collected(
    client: &mut Client,
    payload: PaymentCollectedPayload,
) -> Result<(), postgres::Error> {
    let mut t = client.transaction().unwrap();
    t.execute(
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
    .map(|_| ())?;

    let r1 = t
        .query(
            "SELECT * FROM relations WHERE transaction_id=$1 AND payment_id IS NULL",
            &[&payload.transaction_id],
        )
        .unwrap();

    if r1.len() > 0 {
        // ho almeno un transaction id corrispondente con payment id nullo
        t.execute(
            r"UPDATE relations SET payment_id=$1 WHERE transaction_id=$2",
            &[&payload.payment_id, &payload.transaction_id],
        )?;
    } else {
        let r2 = t
            .query(
                "SELECT * FROM relations WHERE payment_id=$1 AND transaction_id IS NULL",
                &[&payload.payment_id],
            )
            .unwrap();

        if r2.len() > 0 {
            // ho almeno un payment id corrispondente con transaction id nullo
            t.execute(
                r"UPDATE relations SET transaction_id=$1 WHERE payment_id=$2",
                &[&payload.transaction_id, &payload.payment_id],
            )?;
        } else {
            t.execute(
                r"
                INSERT INTO relations (transaction_id, payment_id) VALUES ($1,$2)",
                &[&payload.transaction_id, &payload.payment_id],
            )
            .map(|_| ())?;
        }
    }
    t.commit()
}

fn save_payment_authorized(
    client: &mut Client,
    payload: PaymentAuthorizedPayload,
) -> Result<(), postgres::Error> {
    let mut t = client.transaction().unwrap();
    t.execute(
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
    .map(|_| ())?;

    let r1 = t
        .query(
            "SELECT * FROM relations WHERE order_id=$1 AND payment_id IS NULL",
            &[&payload.order_id],
        )
        .unwrap();

    if r1.len() > 0 {
        t.execute(
            r"UPDATE relations SET payment_id=$1 WHERE order_id=$2",
            &[&payload.payment_id, &payload.order_id],
        )
        .map(|_| ())?;
    } else {
        let r2 = t
            .query(
                "SELECT * FROM relations WHERE payment_id=$1 AND order_id IS NULL",
                &[&payload.payment_id],
            )
            .unwrap();

        if r2.len() > 0 {
            t.execute(
                r"UPDATE relations SET order_id=$1 WHERE payment_id=$2",
                &[&payload.order_id, &payload.payment_id],
            )
            .map(|_| ())?;
        } else {
            t.execute(
                r"
                INSERT INTO relations (order_id, payment_id) VALUES ($1,$2)",
                &[&payload.order_id, &payload.payment_id],
            )
            .map(|_| ())?;
        }
    }
    t.commit()
}
