/// This example demonstrates how to use JWT/Bearer token authentication with CouchDB.
///
/// Prerequisites:
/// - CouchDB must be configured to accept JWT authentication
/// - You need a valid JWT token from your auth service
///
/// CouchDB JWT configuration typically involves:
/// 1. Setting up the `[jwt_auth]` section in CouchDB config
/// 2. Configuring the JWT secret or public key
/// 3. Setting the required claims (e.g., `sub` for username)
///
/// For more info, see: https://docs.couchdb.org/en/stable/api/server/authn.html#jwt-authentication
use serde_json::{Value, json};
use std::error::Error;

const DB_HOST: &str = "http://localhost:5984";
const TEST_DB: &str = "test_jwt_db";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("Connecting with JWT Bearer token...");

    // Get JWT token from environment variable or your auth service
    let jwt_token = std::env::var("COUCHDB_JWT_TOKEN")
        .expect("COUCHDB_JWT_TOKEN environment variable must be set");

    // Create client with JWT bearer token authentication
    let client = couch_rs::Client::new_with_bearer_token(DB_HOST, &jwt_token)?;

    // Verify connection by checking CouchDB status
    match client.check_status().await {
        Ok(status) => {
            println!("✓ Connected to CouchDB");
            println!("  Version: {}", status.version);
            println!("  Vendor: {}", status.vendor.name);
        }
        Err(e) => {
            eprintln!("✗ Failed to connect: {e}");
            return Err(e.into());
        }
    }

    // List existing databases
    let dbs = client.list_dbs().await?;
    println!("\nExisting databases:");
    for db in &dbs {
        println!("  - {db}");
    }

    // Create or get a database
    let db = client.db(TEST_DB).await?;
    println!("\n✓ Database '{TEST_DB}' ready");

    // Insert a test document
    let test_doc = json!({
        "message": "Hello from JWT-authenticated client!",
        "type": "test_document"
    });

    match db.create(&test_doc).await {
        Ok(response) => {
            println!("\n✓ Document created:");
            println!("  ID: {}", response.id);
            println!("  Rev: {}", response.rev);
        }
        Err(e) => {
            eprintln!("✗ Failed to create document: {e}");
        }
    }

    println!("\nAll operations completed successfully!");
    Ok(())
}
