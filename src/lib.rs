pub mod event_handler;
pub mod events;
pub mod projectors;
pub mod reconciliation_engine;

#[cfg(test)]
mod tests {
    use sqlite::Connection;

    use crate::event_handler::*;
    use crate::events::*;
    use std::str::FromStr;
    use std::sync::Mutex;

    fn reset_db(connection: &Connection) {
        connection
            .execute(
                r"
        DROP TABLE IF EXISTS total_ordered;
        DROP TABLE IF EXISTS bank_transactions;
        DROP TABLE IF EXISTS payment_authorizations;
        DROP TABLE IF EXISTS payment_collections;
        DROP TABLE IF EXISTS product_orders;
        
        CREATE TABLE total_ordered (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            amount float,
            occurred_on text
        );
        
        CREATE TABLE bank_transactions (
            transaction_id text PRIMARY KEY,
            amount float,
            occurred_on text
        );

        CREATE TABLE payment_authorizations (
            payment_id text,
            order_id text,
            amount float,
            occurred_on text,
            PRIMARY KEY (order_id, payment_id)
        );

        CREATE TABLE payment_collections (
            payment_id text,
            transaction_id text,
            amount float,
            occurred_on text,
            PRIMARY KEY (transaction_id, payment_id)
        );

        CREATE TABLE product_orders (
            order_id text PRIMARY KEY,
            amount float,
            occurred_on text
        );
        ",
            )
            .unwrap();
    }

    #[test]
    fn happy_path_reconciliation_engine() {
        let conn = sqlite::open(":memory:").unwrap();
        reset_db(&conn);
        let events = [
            Event::ProductOrdered(ProductOrderedPayload {
                amount: 100.0,
                order_id: "ord_1".to_owned(),
                guarantees: vec![],
                occurred_on: chrono::DateTime::from_str("2023-02-20T10:00:00.000Z").unwrap(),
            }),
            Event::PaymentAuthorized(PaymentAuthorizedPayload {
                amount: 100.0,
                order_id: "ord_1".to_owned(),
                payment_id: "payment_1".to_owned(),
                occurred_on: chrono::DateTime::from_str("2023-02-20T10:00:01.000Z").unwrap(),
            }),
            Event::PaymentCollected(PaymentCollectedPayload {
                amount: 100.0,
                order_id: "ord_1".to_owned(),
                transaction_id: "tran_1".to_owned(),
                occurred_on: chrono::DateTime::from_str("2023-02-20T10:00:00.000Z").unwrap(),
            }),
            Event::BankTransactionIssued(BankTransactionIssuedPayload {
                amount: 100.0,
                transaction_id: "tran_1".to_owned(),
                occurred_on: chrono::DateTime::from_str("2023-02-20T10:00:01.000Z").unwrap(),
            }),
        ];

        //push to event handler
        let event_handler = EventHandler::new(&conn);
        let handler_result = events
            .into_iter()
            .map(|e| event_handler.accept(e))
            .collect::<Result<Vec<_>, _>>();

        conn.prepare(r"SELECT * from total_ordered")
            .unwrap()
            .into_iter()
            .for_each(|d| {
                dbg!(d.unwrap());
            });

        let mut s = conn
            .prepare(r"SELECT SUM(amount) from total_ordered")
            .unwrap();
        let _ = s.next();
        let actual_total_ordered: f64 = s.read(0).unwrap();

        //assert that we project correctly
        assert!(handler_result.is_ok());
        assert_eq!(actual_total_ordered, 100.0);
    }
}
