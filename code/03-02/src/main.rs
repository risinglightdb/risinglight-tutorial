//! A simple interactive shell of the database.

use risinglight_03_02::storage::StorageOptions;
use risinglight_03_02::Database;
use rustyline::error::ReadlineError;
use rustyline::Editor;

fn main() {
    env_logger::init();

    let db = Database::new(StorageOptions {
        base_path: "risinglight.db".into(),
    });

    let mut rl = Editor::<()>::new();
    loop {
        match rl.readline("> ") {
            Ok(line) => {
                rl.add_history_entry(line.as_str());
                let ret = db.run(&line);
                match ret {
                    Ok(chunks) => {
                        for chunk in chunks {
                            println!("{}", chunk);
                        }
                    }
                    Err(err) => println!("{}", err),
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("Interrupted");
            }
            Err(ReadlineError::Eof) => {
                println!("Exited");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
}
