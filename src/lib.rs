pub mod event_handler;
pub mod events;
pub mod pool;
pub mod projectors;
pub mod reconciliation_engine;
#[cfg(test)]
mod tests {
    use postgres::types::FromSql;

    use postgres::NoTls;
    use r2d2_postgres::r2d2::PooledConnection;
    use r2d2_postgres::PostgresConnectionManager;
    use std::fmt::Debug;
    use std::str::FromStr;

    use crate::event_handler::*;
    use crate::events::*;
    type Client = PooledConnection<PostgresConnectionManager<NoTls>>;

    #[test]
    fn happy_path_reconciliation_engine() {
        let mut client = crate::pool::POOL.get().unwrap();
        crate::pool::reset_db(&mut client);
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

        assert!(handler_result.is_ok());

        client.query(r"SELECT * from relations", &[]).unwrap();

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
            &mut client,
            r"SELECT COUNT(order_id) from product_orders where  collected_amount <> amount",
            0 as i64,
        );

        assert_query(
            &mut client,
            r"SELECT COUNT(transaction_id) from bank_transactions where ordered_amount <> amount",
            0 as i64,
        );

        assert_query(
            &mut client,
            r"SELECT COUNT(order_id) from product_orders where collected_amount = amount",
            1 as i64,
        );

        assert_query(
            &mut client,
            r"SELECT COUNT(transaction_id) from bank_transactions where ordered_amount = amount",
            1 as i64,
        );
    }

    #[test]
    fn events_type_not_reconciled() {
        let mut client = crate::pool::POOL.get().unwrap();
        crate::pool::reset_db(&mut client);
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
            &mut client,
            r"SELECT COUNT(*) FROM product_orders WHERE event_type='issuance' AND collected_amount <> amount",
            1 as i64,
        );

        assert_query(
            &mut client,
            r"SELECT CAST(SUM(amount) as int8) FROM product_orders WHERE event_type='issuance' AND collected_amount <> amount",
            100 as i64,
        );

        assert_query(
            &mut client,
            r"SELECT COUNT(*) FROM product_orders WHERE event_type='interruption' AND collected_amount <> amount",
            2 as i64,
        );
        assert_query(
            &mut client,
            r"SELECT CAST(SUM(amount) as int8) FROM product_orders WHERE event_type='interruption' AND collected_amount <> amount",
            500 as i64,
        );

        assert_query(
            &mut client,
            r"SELECT insurance_code FROM product_orders WHERE event_type='interruption' AND collected_amount <> amount",
            "PRP2".to_string(),
        )
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
