pub mod event_handler;
pub mod events;
pub mod pool;
pub mod projectors;
pub mod reconciliation_engine;
#[cfg(test)]
mod tests {
    use postgres::types::FromSql;
    use postgres::Client;
    use std::fmt::Debug;
    use std::str::FromStr;

    use crate::event_handler::*;
    use crate::events::*;
    use crate::pool::Pool;

    fn reset_db(client: &mut Client) {
        let queries = r"
        DROP TABLE IF EXISTS total_ordered;
        DROP TABLE IF EXISTS total_authorized;
        DROP TABLE IF EXISTS total_collected;
        DROP TABLE IF EXISTS bank_transactions;
        DROP TABLE IF EXISTS payment_authorizations;
        DROP TABLE IF EXISTS payment_collections;
        DROP TABLE IF EXISTS product_orders;
        
        CREATE TABLE total_ordered (
            id SERIAL PRIMARY KEY,
            amount double precision,
            occurred_on text
        );

        CREATE TABLE total_authorized (
            id SERIAL PRIMARY KEY,
            amount double precision,
            occurred_on text
        );

        CREATE TABLE total_collected (
            id SERIAL PRIMARY KEY,
            amount double precision,
            occurred_on text
        );
        
        CREATE TABLE bank_transactions (
            transaction_id text PRIMARY KEY,
            amount double precision,
            occurred_on text,
            reconciled int4 default 0
        );

        CREATE TABLE payment_authorizations (
            payment_id text,
            order_id text,
            amount double precision,
            occurred_on text,
            PRIMARY KEY (order_id, payment_id)
        );

        CREATE TABLE payment_collections (
            payment_id text,
            transaction_id text,
            amount double precision,
            occurred_on text,
            PRIMARY KEY (transaction_id, payment_id)
        );

        CREATE TABLE product_orders (
            order_id text PRIMARY KEY,
            amount double precision,
            occurred_on text,
            reconciled int4 default 0,
            insurance_code text,
            installment_type text,
            event_type text
        );";

        queries.split(";").filter(|s| !s.is_empty()).for_each(|q| {
            client.execute(q, &[]).map(|_| ()).unwrap();
        });
    }

    #[test]
    fn happy_path_reconciliation_engine() {
        let client = &mut Pool::get_client();
        reset_db(client);
        let events = [
            Event::ProductOrdered(ProductOrderedPayload {
                amount: 100.0,
                order_id: "ord_1".to_owned(),
                guarantees: vec![],
                occurred_on: chrono::DateTime::from_str("2023-02-20T10:00:00.000Z").unwrap(),
                event_type: EventType::Issuance,
                installment_type: InstallmentType::Yearly,
                insurance_code: "PRP123".to_string(),
            }),
            Event::PaymentAuthorized(PaymentAuthorizedPayload {
                amount: 100.0,
                order_id: "ord_1".to_owned(),
                payment_id: "pay_1".to_owned(),
                occurred_on: chrono::DateTime::from_str("2023-02-20T10:00:01.000Z").unwrap(),
            }),
            Event::PaymentCollected(PaymentCollectedPayload {
                amount: 100.0,
                payment_id: "pay_1".to_owned(),
                transaction_id: "tran_1".to_owned(),
                occurred_on: chrono::DateTime::from_str("2023-02-20T10:00:00.000Z").unwrap(),
            }),
            Event::BankTransactionIssued(BankTransactionIssuedPayload {
                amount: 100.0,
                transaction_id: "tran_1".to_owned(),
                occurred_on: chrono::DateTime::from_str("2023-02-20T10:00:01.000Z").unwrap(),
            }),
        ];

        let event_handler = EventHandler::new();
        let handler_result = events
            .into_iter()
            .map(|e| event_handler.accept(e))
            .collect::<Result<Vec<_>, _>>();

        dbg!(&handler_result);
        assert!(handler_result.is_ok());

        let s = client
            .query(r"SELECT SUM(amount) from total_ordered", &[])
            .unwrap();

        let actual_total_ordered: f64 = s.get(0).unwrap().get(0);

        assert_eq!(
            actual_total_ordered, 100.0,
            "expecting the sum of all ordered events to be 100"
        );

        let s = client
            .query(r"SELECT SUM(amount) from total_authorized", &[])
            .unwrap();

        let actual_total_authorized: f64 = s.get(0).unwrap().get(0);

        assert_eq!(
            actual_total_authorized, 100.0,
            "expecting the sum of all authorized events to be 100"
        );

        let s = client
            .query(r"SELECT SUM(amount) from total_collected", &[])
            .unwrap();

        let actual_total_collected: f64 = s.get(0).unwrap().get(0);

        assert_eq!(
            actual_total_collected, 100.0 as f64,
            "expecting the sum of all collected events to be 100"
        );

        assert_query(
            client,
            r"SELECT COUNT(order_id) from product_orders where reconciled = 0",
            0 as i64,
        );

        assert_query(
            client,
            r"SELECT COUNT(transaction_id) from bank_transactions where reconciled = 0",
            0 as i64,
        );

        assert_query(
            client,
            r"SELECT COUNT(order_id) from product_orders where reconciled = 1",
            1 as i64,
        );

        assert_query(
            client,
            r"SELECT COUNT(transaction_id) from bank_transactions where reconciled = 1",
            1 as i64,
        );
    }

    #[test]
    fn events_type_not_reconciled() {
        let client = &mut Pool::get_client();
        reset_db(client);
        let events = [
            Event::ProductOrdered(ProductOrderedPayload {
                amount: 100.0,
                order_id: "ord_1".to_owned(),
                guarantees: vec![],
                occurred_on: chrono::DateTime::from_str("2023-02-20T10:00:00.000Z").unwrap(),
                event_type: EventType::Issuance,
                installment_type: InstallmentType::Yearly,
                insurance_code: "PRP1".to_owned(),
            }),
            Event::ProductOrdered(ProductOrderedPayload {
                amount: 200.0,
                order_id: "ord_2".to_owned(),
                guarantees: vec![],
                occurred_on: chrono::DateTime::from_str("2023-02-20T10:00:00.000Z").unwrap(),
                event_type: EventType::Interruption,
                installment_type: InstallmentType::BiYearly,
                insurance_code: "PRP2".to_owned(),
            }),
            Event::ProductOrdered(ProductOrderedPayload {
                amount: 300.0,
                order_id: "ord_3".to_owned(),
                guarantees: vec![],
                occurred_on: chrono::DateTime::from_str("2023-02-20T10:00:00.000Z").unwrap(),
                event_type: EventType::Interruption,
                installment_type: InstallmentType::BiYearly,
                insurance_code: "PRP3".to_owned(),
            }),
        ];

        let event_handler = EventHandler::new();
        let handler_result = events
            .into_iter()
            .map(|e| event_handler.accept(e))
            .collect::<Result<Vec<_>, _>>();
        assert!(handler_result.is_ok());

        assert_query(
            client,
            r"SELECT COUNT(*) FROM product_orders WHERE event_type='issuance' AND reconciled=0",
            1 as i64,
        );

        assert_query(
            client,
            r"SELECT SUM(amount) FROM product_orders WHERE event_type='issuance' AND reconciled=0",
            100 as i64,
        );

        assert_query(
            client,
            r"SELECT COUNT(*) FROM product_orders WHERE event_type='interruption' AND reconciled=0",
            2 as i64,
        );
        assert_query(
            client,
            r"SELECT SUM(amount) FROM product_orders WHERE event_type='interruption' AND reconciled=0",
            500 as i64,
        );

        assert_query(
            client,
            r"SELECT insurance_code FROM product_orders WHERE event_type='interruption' AND reconciled=0",
            "PRP2".to_string(),
        );
    }

    fn assert_query<T>(client: &mut Client, query: &str, value: T)
    where
        T: for<'a> FromSql<'a> + Eq + Debug,
    {
        let s = client.query(query, &[]).unwrap();
        let res: T = s.get(0).unwrap().get(0);
        assert_eq!(res, value, "expected {query} to return {:?}", value);
    }
}
