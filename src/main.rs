use postgres::Client;
use rand::Rng;
use spike_costacando::{
    event_handler::EventHandler,
    events::{
        BankTransactionIssuedPayload, PaymentAuthorizedPayload, PaymentCollectedPayload,
        ProductOrderedPayload,
    },
    pool::Pool,
};
use std::vec;

fn main() -> Result<(), String> {
    println!("AOO stoppepartì!");
    for num in [10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000] {
        let num_of_events_to_handle: usize = num;
        let client = &mut Pool::get_client();
        reset_db(client);
        let handler = EventHandler::new();
        let mut events: Vec<spike_costacando::events::Event> = vec![];
        println!("Generating events...");
        for _i in 0..num_of_events_to_handle {
            events.append(&mut generate_random_events(num_of_events_to_handle));
        }
        println!("Generated events!\nHandling events...");
        let before = std::time::SystemTime::now();
        events.into_iter().for_each(|e| handler.accept(e).unwrap());
        let after = std::time::SystemTime::elapsed(&before).unwrap().as_millis();
        println!("{after}ms spent to handle {num_of_events_to_handle} events");
    }
    Ok(())
}

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
            amount float,
            occurred_on text
        );

        CREATE TABLE total_authorized (
            id SERIAL PRIMARY KEY,
            amount float,
            occurred_on text
        );

        CREATE TABLE total_collected (
            id SERIAL PRIMARY KEY,
            amount float,
            occurred_on text
        );
        
        CREATE TABLE bank_transactions (
            transaction_id text PRIMARY KEY,
            amount float,
            occurred_on text,
            reconciled int default 0
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
            occurred_on text,
            reconciled int default 0,
            insurance_code text,
            installment_type text,
            event_type text
        );";
    queries.split(";").filter(|s| !s.is_empty()).for_each(|q| {
        println!("Executing {q}");
        client.execute(q, &[]).map(|_| ()).unwrap();
        println!("Executed {q}");
    });
}

fn generate_random_events(num_of_events_to_handle: usize) -> Vec<spike_costacando::events::Event> {
    let mut rng = rand::thread_rng();
    rng.gen_range(0..num_of_events_to_handle);
    let random_number: usize = rng.gen();
    vec![
        spike_costacando::events::Event::BankTransactionIssued(BankTransactionIssuedPayload {
            transaction_id: format!("t_{random_number}"),
            amount: 100.0,
            occurred_on: chrono::Utc::now(),
        }),
        spike_costacando::events::Event::PaymentAuthorized(PaymentAuthorizedPayload {
            order_id: format!("o_{random_number}"),
            payment_id: format!("p_{random_number}"),
            amount: 100.0,
            occurred_on: chrono::Utc::now(),
        }),
        spike_costacando::events::Event::PaymentCollected(PaymentCollectedPayload {
            payment_id: format!("p_{random_number}"),
            transaction_id: format!("t_{random_number}"),
            amount: 100.0,
            occurred_on: chrono::Utc::now(),
        }),
        spike_costacando::events::Event::ProductOrdered(ProductOrderedPayload {
            amount: 100.0,
            occurred_on: chrono::Utc::now(),
            order_id: format!("p_{random_number}"),
            event_type: spike_costacando::events::EventType::Issuance,
            installment_type: spike_costacando::events::InstallmentType::Monthly,
            guarantees: vec![],
            insurance_code: format!("PRP{random_number}"),
        }),
    ]
}
