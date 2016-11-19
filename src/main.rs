extern crate pg_crunch;

use pg_crunch::scanner::CrunchState;
use std::io;
use std::io::prelude::*;

fn main() {
    let mut state = CrunchState::new();
    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        match line {
            Ok(line) => state = state.process_line(line),
            Err(error) => println!("error: {}", error),
        }
    }
}
