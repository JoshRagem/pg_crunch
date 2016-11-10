extern crate pg_crunch;

use std::io;
use pg_crunch::scanner;

fn main() {
    let mut done: bool = false;
    let mut state: pg_crunch::scanner::CrunchState = scanner::init_state();
    while !done {
        let mut input: String = String::new();
        match io::stdin().read_line(&mut input) {
            Ok(0) => {
                done = true;
            }
            Ok(_) => {
                state = scanner::process_line(input, state);
            }
            Err(error) => println!("error: {}", error),
        }
    }
}
