// Debug script to check QueryInfo parsing differences
use winccua_pgwire_protocol::sql_handler::SqlHandler;
use winccua_pgwire_protocol::tables::SqlResult;

fn main() {
    let query1 = "SELECT * FROM tagvalues WHERE tag_name LIKE '%::PV%'";
    let query2 = "SELECT COUNT(*) FROM tagvalues WHERE tag_name LIKE '%::PV%'";
    
    println!("=== Query 1 ===");
    match SqlHandler::parse_query(query1) {
        Ok(SqlResult::Query(info)) => println!("{:#?}", info),
        Ok(SqlResult::SetStatement(s)) => println!("Set: {}", s),
        Err(e) => println!("Error: {}", e),
    }
    
    println!("\n=== Query 2 ===");
    match SqlHandler::parse_query(query2) {
        Ok(SqlResult::Query(info)) => println!("{:#?}", info),
        Ok(SqlResult::SetStatement(s)) => println!("Set: {}", s),
        Err(e) => println!("Error: {}", e),
    }
}